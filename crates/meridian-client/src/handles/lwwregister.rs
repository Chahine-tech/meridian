use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use meridian_core::{
    crdt::{
        clock::{now_ms, HybridLogicalClock},
        lwwregister::{LwwDelta, LwwOp, LwwRegister},
        Crdt,
    },
    crdt::registry::{CrdtOp, VersionedOp},
    protocol::ClientMsg,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::watch;

use crate::{codec, error::ClientError, op_queue::OpQueue, transport::Transport};

#[allow(dead_code)]
pub(crate) struct LwwRegisterInner {
    state:     Mutex<LwwRegister>,
    watch_tx:  watch::Sender<Option<serde_json::Value>>,
    crdt_id:   String,
    client_id: u64,
    transport: Arc<dyn Transport>,
    op_queue:  Arc<OpQueue>,
}

/// Live handle to a LwwRegister CRDT. Generic over the stored value type `T`.
#[derive(Clone)]
pub struct LwwRegisterHandle<T> {
    inner:   Arc<LwwRegisterInner>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned + Serialize + Send + Sync + 'static> LwwRegisterHandle<T> {
    pub(crate) fn from_inner(inner: Arc<LwwRegisterInner>) -> Self {
        Self { inner, _marker: PhantomData }
    }

    pub(crate) fn into_inner(self) -> Arc<LwwRegisterInner> {
        self.inner
    }

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

    /// Set the register value. Uses current wall clock as HLC timestamp.
    pub async fn set(&self, value: T) -> Result<(), ClientError> {
        let json_val = serde_json::to_value(&value)?;
        let hlc = HybridLogicalClock {
            wall_ms: now_ms(),
            logical: 0,
            node_id: self.inner.client_id,
        };
        let op = LwwOp { value: json_val, hlc, author: self.inner.client_id };

        {
            let mut state = self.inner.state.lock().expect("lww lock poisoned");
            state.apply(op.clone())?;
            let new_val = state.value().value;
            let _ = self.inner.watch_tx.send_if_modified(|v| {
                if *v != new_val { *v = new_val; true } else { false }
            });
        }

        let versioned = VersionedOp::new(CrdtOp::LwwRegister(op));
        let op_bytes = codec::encode_op(&versioned)?;
        self.inner.transport.send(ClientMsg::Op {
            crdt_id: self.inner.crdt_id.clone(),
            op_bytes,
            ttl_ms: None,
            client_seq: None,
        }).await
    }

    pub(crate) fn apply_delta(&self, delta_bytes: &[u8]) -> Result<(), ClientError> {
        let delta: LwwDelta = codec::decode_delta(delta_bytes)?;
        let mut state = self.inner.state.lock().expect("lww lock poisoned");
        state.merge_delta(delta);
        let new_val = state.value().value;
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
