use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use meridian_core::{
    crdt::registry::{CrdtOp, VersionedOp},
    crdt::{
        Crdt,
        clock::{HybridLogicalClock, now_ms},
        lwwregister::{LwwDelta, LwwOp, LwwRegister},
    },
    protocol::ClientMsg,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_bytes::ByteBuf;
use tokio::sync::watch;

use crate::{codec, error::ClientError, op_queue::OpQueue, transport::Transport};

#[cfg(feature = "crypto")]
use crate::crypto::{AesGcmKey, ClientKeypair, aes_gcm::is_encrypted_value};

#[allow(dead_code)]
pub(crate) struct LwwRegisterInner {
    state: Mutex<LwwRegister>,
    watch_tx: watch::Sender<Option<serde_json::Value>>,
    crdt_id: String,
    client_id: u64,
    transport: Arc<dyn Transport>,
    op_queue: Arc<OpQueue>,
    #[cfg(feature = "crypto")]
    pub(crate) signing_keypair: Option<Arc<ClientKeypair>>,
    #[cfg(feature = "crypto")]
    pub(crate) encrypt_key: Option<Arc<AesGcmKey>>,
    #[cfg(feature = "crypto")]
    pub(crate) decrypt_key: Option<Arc<AesGcmKey>>,
}

/// Live handle to a LwwRegister CRDT. Generic over the stored value type `T`.
#[derive(Clone)]
pub struct LwwRegisterHandle<T> {
    pub(crate) inner: Arc<LwwRegisterInner>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned + Serialize + Send + Sync + 'static> LwwRegisterHandle<T> {
    pub(crate) fn from_inner(inner: Arc<LwwRegisterInner>) -> Self {
        Self { inner, _marker: PhantomData }
    }

    pub(crate) fn into_inner(self) -> Arc<LwwRegisterInner> {
        self.inner
    }

    #[cfg_attr(feature = "crypto", allow(dead_code))]
    pub(crate) fn new(
        crdt_id: String,
        client_id: u64,
        transport: Arc<dyn Transport>,
        op_queue: Arc<OpQueue>,
    ) -> Self {
        let (watch_tx, _) = watch::channel(None);
        Self {
            inner: Arc::new(LwwRegisterInner {
                state: Mutex::new(LwwRegister::default()),
                watch_tx,
                crdt_id,
                client_id,
                transport,
                op_queue,
                #[cfg(feature = "crypto")]
                signing_keypair: None,
                #[cfg(feature = "crypto")]
                encrypt_key: None,
                #[cfg(feature = "crypto")]
                decrypt_key: None,
            }),
            _marker: PhantomData,
        }
    }

    #[cfg(feature = "crypto")]
    pub(crate) fn new_with_crypto(
        crdt_id: String,
        client_id: u64,
        transport: Arc<dyn Transport>,
        op_queue: Arc<OpQueue>,
        signing_keypair: Option<Arc<ClientKeypair>>,
        encrypt_key: Option<Arc<AesGcmKey>>,
        decrypt_key: Option<Arc<AesGcmKey>>,
    ) -> Self {
        let (watch_tx, _) = watch::channel(None);
        Self {
            inner: Arc::new(LwwRegisterInner {
                state: Mutex::new(LwwRegister::default()),
                watch_tx,
                crdt_id,
                client_id,
                transport,
                op_queue,
                signing_keypair,
                encrypt_key,
                decrypt_key,
            }),
            _marker: PhantomData,
        }
    }

    /// Current value, deserialized to `T`. Returns `None` if never written.
    pub fn value(&self) -> Result<Option<T>, ClientError> {
        let raw = self.inner.watch_tx.borrow().clone();
        match raw {
            None => Ok(None),
            Some(v) => serde_json::from_value(v).map(Some).map_err(ClientError::Json),
        }
    }

    /// Watch for raw JSON value changes.
    pub fn watch_raw(&self) -> watch::Receiver<Option<serde_json::Value>> {
        self.inner.watch_tx.subscribe()
    }

    /// Register a callback invoked on each value change.
    #[must_use = "dropping the JoinHandle cancels the subscription"]
    pub fn on_change<F>(&self, f: F) -> tokio::task::JoinHandle<()>
    where
        F: Fn(Option<serde_json::Value>) + Send + 'static,
    {
        let mut rx = self.inner.watch_tx.subscribe();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                f(rx.borrow().clone());
            }
        })
    }

    /// Set the register value. Encrypts and signs when configured.
    pub async fn set(&self, value: T) -> Result<(), ClientError> {
        let json_val = serde_json::to_value(&value)?;
        let hlc = HybridLogicalClock {
            wall_ms: now_ms(),
            logical: 0,
            node_id: self.inner.client_id,
        };

        // Optionally encrypt the wire value.
        #[cfg(feature = "crypto")]
        let wire_val = if let Some(key) = &self.inner.encrypt_key {
            serde_json::to_value(key.encrypt(&json_val)?).map_err(ClientError::Json)?
        } else {
            json_val.clone()
        };
        #[cfg(not(feature = "crypto"))]
        let wire_val = json_val.clone();

        let op = LwwOp {
            value: wire_val,
            hlc,
            author: self.inner.client_id,
        };

        // Apply locally with plaintext so watch_tx carries decrypted data.
        {
            let local_op = LwwOp { value: json_val, hlc, author: self.inner.client_id };
            let mut state = self.inner.state.lock().expect("lww lock poisoned");
            state.apply(local_op)?;
            let new_val = state.value().value;
            let _ = self.inner.watch_tx.send_if_modified(|v| {
                if *v != new_val { *v = new_val; true } else { false }
            });
        }

        let versioned = VersionedOp::new(CrdtOp::LwwRegister(op));
        let op_bytes = codec::encode_op(&versioned)?;

        // Optionally sign op_bytes.
        #[cfg(feature = "crypto")]
        let sig = self
            .inner
            .signing_keypair
            .as_ref()
            .map(|kp| ByteBuf::from(kp.sign(&op_bytes).to_vec()));
        #[cfg(not(feature = "crypto"))]
        let sig: Option<ByteBuf> = None;

        self.inner
            .transport
            .send(ClientMsg::Op {
                crdt_id: self.inner.crdt_id.clone(),
                op_bytes,
                ttl_ms: None,
                client_seq: None,
                sig,
            })
            .await
    }

    pub(crate) fn apply_delta(&self, delta_bytes: &[u8]) -> Result<(), ClientError> {
        let delta: LwwDelta = codec::decode_delta(delta_bytes)?;
        let mut state = self.inner.state.lock().expect("lww lock poisoned");
        state.merge_delta(delta);
        let new_val = state.value().value;
        #[cfg(feature = "crypto")]
        let mut new_val = new_val;

        // Optionally decrypt incoming value.
        #[cfg(feature = "crypto")]
        if let (Some(key), Some(v)) = (&self.inner.decrypt_key, &new_val) {
            if is_encrypted_value(v) {
                match key.decrypt_value(v) {
                    Ok(dec) => new_val = Some(dec),
                    Err(e) => {
                        tracing::warn!("lww decrypt failed for {}: {e}", self.inner.crdt_id);
                    }
                }
            }
        }

        let _ = self.inner.watch_tx.send_if_modified(|v| {
            if *v != new_val { *v = new_val; true } else { false }
        });
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn crdt_id(&self) -> &str {
        &self.inner.crdt_id
    }
}
