use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use meridian_core::{
    crdt::{
        orset::{ORSet, ORSetDelta, ORSetOp},
        Crdt,
    },
    crdt::registry::{CrdtOp, VersionedOp},
    protocol::ClientMsg,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::watch;
use uuid::Uuid;

use crate::{codec, error::ClientError, op_queue::OpQueue, transport::Transport};

#[allow(dead_code)]
pub(crate) struct ORSetInner {
    state:     Mutex<ORSet>,
    watch_tx:  watch::Sender<Vec<serde_json::Value>>,
    crdt_id:   String,
    client_id: u64,
    local_seq: std::sync::atomic::AtomicU32,
    transport: Arc<dyn Transport>,
    op_queue:  Arc<OpQueue>,
}

/// Live handle to an ORSet CRDT. Generic over the element type `T`.
#[derive(Clone)]
pub struct ORSetHandle<T> {
    inner:   Arc<ORSetInner>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned + Serialize + Send + Sync + 'static> ORSetHandle<T> {
    pub(crate) fn from_inner(inner: Arc<ORSetInner>) -> Self {
        Self { inner, _marker: PhantomData }
    }

    pub(crate) fn into_inner(self) -> Arc<ORSetInner> {
        self.inner
    }

    pub(crate) fn new(
        crdt_id: String,
        client_id: u64,
        transport: Arc<dyn Transport>,
        op_queue: Arc<OpQueue>,
    ) -> Self {
        let (watch_tx, _) = watch::channel(vec![]);
        Self {
            inner: Arc::new(ORSetInner {
                state: Mutex::new(ORSet::default()),
                watch_tx,
                crdt_id,
                client_id,
                local_seq: std::sync::atomic::AtomicU32::new(0),
                transport,
                op_queue,
            }),
            _marker: PhantomData,
        }
    }

    /// Current elements, deserialized to `T`.
    pub fn elements(&self) -> Result<Vec<T>, ClientError> {
        let raw = self.inner.watch_tx.borrow().clone();
        raw.into_iter()
            .map(|v| serde_json::from_value(v).map_err(ClientError::Json))
            .collect()
    }

    pub fn watch_raw(&self) -> watch::Receiver<Vec<serde_json::Value>> {
        self.inner.watch_tx.subscribe()
    }

    #[must_use = "dropping the JoinHandle cancels the subscription"]
    pub fn on_change<F>(&self, f: F) -> tokio::task::JoinHandle<()>
    where
        F: Fn(Vec<serde_json::Value>) + Send + 'static,
    {
        let mut rx = self.inner.watch_tx.subscribe();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                f(rx.borrow().clone());
            }
        })
    }

    /// Add an element to the set.
    pub async fn add(&self, value: T) -> Result<(), ClientError>
    where
        T: Serialize,
    {
        let json_val = serde_json::to_value(&value)?;
        let tag = Uuid::new_v4();
        let seq = self.inner.local_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let op = ORSetOp::Add {
            element: json_val,
            tag,
            node_id: self.inner.client_id,
            seq,
        };
        self.apply_and_send(op).await
    }

    /// Remove an element from the set (add-wins on concurrent add).
    pub async fn remove(&self, value: &T) -> Result<(), ClientError>
    where
        T: Serialize,
    {
        let json_val = serde_json::to_value(value)?;
        let element_key = json_val.to_string();
        let known_tags = {
            let state = self.inner.state.lock().expect("orset lock poisoned");
            state.entries.get(&element_key).cloned().unwrap_or_default()
        };
        let op = ORSetOp::Remove { element: json_val, known_tags };
        self.apply_and_send(op).await
    }

    async fn apply_and_send(&self, op: ORSetOp) -> Result<(), ClientError> {
        {
            let mut state = self.inner.state.lock().expect("orset lock poisoned");
            state.apply(op.clone())?;
            let new_elements = state.value().elements;
            let _ = self.inner.watch_tx.send_if_modified(|v| {
                if *v != new_elements { *v = new_elements; true } else { false }
            });
        }
        let versioned = VersionedOp::new(CrdtOp::ORSet(op));
        let op_bytes = codec::encode_op(&versioned)?;
        self.inner.transport.send(ClientMsg::Op {
            crdt_id: self.inner.crdt_id.clone(),
            op_bytes,
            ttl_ms: None,
            client_seq: None,
        }).await
    }

    pub(crate) fn apply_delta(&self, delta_bytes: &[u8]) -> Result<(), ClientError> {
        let delta: ORSetDelta = codec::decode_delta(delta_bytes)?;
        let mut state = self.inner.state.lock().expect("orset lock poisoned");
        state.merge_delta(delta);
        let new_elements = state.value().elements;
        let _ = self.inner.watch_tx.send_if_modified(|v| {
            if *v != new_elements { *v = new_elements; true } else { false }
        });
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn crdt_id(&self) -> &str {
        &self.inner.crdt_id
    }
}
