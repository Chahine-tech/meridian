use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use meridian_core::{
    crdt::registry::{CrdtOp, VersionedOp},
    crdt::{
        Crdt,
        clock::{HybridLogicalClock, now_ms},
        presence::{Presence, PresenceDelta, PresenceOp},
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
pub(crate) struct PresenceInner {
    state: Mutex<Presence>,
    watch_tx: watch::Sender<Vec<(u64, serde_json::Value)>>,
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

/// Live handle to a Presence CRDT. Generic over the data type `T`.
#[derive(Clone)]
pub struct PresenceHandle<T> {
    inner: Arc<PresenceInner>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned + Serialize + Send + Sync + 'static> PresenceHandle<T> {
    pub(crate) fn from_inner(inner: Arc<PresenceInner>) -> Self {
        Self { inner, _marker: PhantomData }
    }

    pub(crate) fn into_inner(self) -> Arc<PresenceInner> {
        self.inner
    }

    #[cfg_attr(feature = "crypto", allow(dead_code))]
    pub(crate) fn new(
        crdt_id: String,
        client_id: u64,
        transport: Arc<dyn Transport>,
        op_queue: Arc<OpQueue>,
    ) -> Self {
        let (watch_tx, _) = watch::channel(vec![]);
        Self {
            inner: Arc::new(PresenceInner {
                state: Mutex::new(Presence::default()),
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
        let (watch_tx, _) = watch::channel(vec![]);
        Self {
            inner: Arc::new(PresenceInner {
                state: Mutex::new(Presence::default()),
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

    /// Currently online clients: `(client_id, data)` pairs.
    pub fn online(&self) -> Result<Vec<(u64, T)>, ClientError> {
        let raw = self.inner.watch_tx.borrow().clone();
        raw.into_iter()
            .map(|(id, v)| {
                serde_json::from_value(v)
                    .map(|data| (id, data))
                    .map_err(ClientError::Json)
            })
            .collect()
    }

    pub fn watch_raw(&self) -> watch::Receiver<Vec<(u64, serde_json::Value)>> {
        self.inner.watch_tx.subscribe()
    }

    #[must_use = "dropping the JoinHandle cancels the subscription"]
    pub fn on_change<F>(&self, f: F) -> tokio::task::JoinHandle<()>
    where
        F: Fn(Vec<(u64, serde_json::Value)>) + Send + 'static,
    {
        let mut rx = self.inner.watch_tx.subscribe();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                f(rx.borrow().clone());
            }
        })
    }

    /// Announce presence with `data` and a `ttl_ms` heartbeat window.
    pub async fn heartbeat(&self, data: T, ttl_ms: u64) -> Result<(), ClientError> {
        let json_data = serde_json::to_value(&data)?;

        #[cfg(feature = "crypto")]
        let wire_data = if let Some(key) = &self.inner.encrypt_key {
            serde_json::to_value(key.encrypt(&json_data)?).map_err(ClientError::Json)?
        } else {
            json_data.clone()
        };
        #[cfg(not(feature = "crypto"))]
        let wire_data = json_data;

        let hlc = HybridLogicalClock {
            wall_ms: now_ms(),
            logical: 0,
            node_id: self.inner.client_id,
        };
        let op = PresenceOp::Heartbeat {
            client_id: self.inner.client_id,
            data: wire_data,
            hlc,
            ttl_ms,
        };
        self.apply_and_send(op).await
    }

    /// Explicitly mark this client as offline.
    pub async fn leave(&self) -> Result<(), ClientError> {
        let hlc = HybridLogicalClock {
            wall_ms: now_ms(),
            logical: 0,
            node_id: self.inner.client_id,
        };
        let op = PresenceOp::Leave {
            client_id: self.inner.client_id,
            hlc,
        };
        self.apply_and_send(op).await
    }

    async fn apply_and_send(&self, op: PresenceOp) -> Result<(), ClientError> {
        {
            let mut state = self.inner.state.lock().expect("presence lock poisoned");
            state.apply(op.clone())?;
            self.notify_change(&state);
        }
        let versioned = VersionedOp::new(CrdtOp::Presence(op));
        let op_bytes = codec::encode_op(&versioned)?;

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

    fn notify_change(&self, state: &Presence) {
        let now = now_ms();
        let new_val: Vec<(u64, serde_json::Value)> = state
            .entries
            .iter()
            .filter(|(_, e)| e.is_alive(now))
            .map(|(&id, e)| (id, (*e.data).clone()))
            .collect();
        let _ = self.inner.watch_tx.send_if_modified(|v| {
            if *v != new_val { *v = new_val; true } else { false }
        });
    }

    pub(crate) fn apply_delta(&self, delta_bytes: &[u8]) -> Result<(), ClientError> {
        let delta: PresenceDelta = codec::decode_delta(delta_bytes)?;
        let mut state = self.inner.state.lock().expect("presence lock poisoned");
        state.merge_delta(delta);

        // Decrypt presence entry data if needed.
        #[cfg(feature = "crypto")]
        if let Some(key) = &self.inner.decrypt_key {
            for entry in state.entries.values_mut() {
                if is_encrypted_value(&entry.data) {
                    match key.decrypt_value(&entry.data) {
                        Ok(dec) => entry.data = std::sync::Arc::new(dec),
                        Err(e) => {
                            tracing::warn!(
                                "presence decrypt failed for {}: {e}",
                                self.inner.crdt_id
                            );
                        }
                    }
                }
            }
        }

        self.notify_change(&state);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn crdt_id(&self) -> &str {
        &self.inner.crdt_id
    }
}
