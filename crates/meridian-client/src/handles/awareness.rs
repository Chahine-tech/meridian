use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use meridian_core::protocol::ClientMsg;
use serde::{de::DeserializeOwned, Serialize};
use serde_bytes::ByteBuf;
use tokio::sync::watch;

use crate::{codec, error::ClientError, op_queue::OpQueue, transport::Transport};

#[allow(dead_code)]
pub(crate) struct AwarenessInner {
    /// Peers: (client_id, raw_data). Does not include self.
    peers:     Mutex<Vec<(u64, serde_json::Value)>>,
    watch_tx:  watch::Sender<Vec<(u64, serde_json::Value)>>,
    key:       String,
    transport: Arc<dyn Transport>,
    op_queue:  Arc<OpQueue>,
}

/// Handle for ephemeral awareness broadcasts (cursors, selections, "is typing").
/// Not persisted — fire-and-forget fan-out.
#[derive(Clone)]
pub struct AwarenessHandle<T> {
    inner:   Arc<AwarenessInner>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned + Serialize + Send + Sync + 'static> AwarenessHandle<T> {
    pub(crate) fn from_inner(inner: Arc<AwarenessInner>) -> Self {
        Self { inner, _marker: PhantomData }
    }

    pub(crate) fn into_inner(self) -> Arc<AwarenessInner> {
        self.inner
    }

    pub(crate) fn new(
        key: String,
        transport: Arc<dyn Transport>,
        op_queue: Arc<OpQueue>,
    ) -> Self {
        let (watch_tx, _) = watch::channel(vec![]);
        Self {
            inner: Arc::new(AwarenessInner {
                peers: Mutex::new(vec![]),
                watch_tx,
                key,
                transport,
                op_queue,
            }),
            _marker: PhantomData,
        }
    }

    /// Broadcast an ephemeral update to all other subscribers.
    pub async fn update(&self, data: T) -> Result<(), ClientError>
    where
        T: Serialize,
    {
        let json_data = serde_json::to_value(&data)?;
        let raw_bytes = rmp_serde::encode::to_vec_named(&json_data).map_err(ClientError::Encode)?;
        let msg = ClientMsg::AwarenessUpdate {
            key: self.inner.key.clone(),
            data: ByteBuf::from(raw_bytes),
        };
        self.inner.transport.send(msg).await
    }

    /// Current peer states (does not include self).
    pub fn peers(&self) -> Result<Vec<(u64, T)>, ClientError> {
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

    /// Called by the dispatch loop when an `AwarenessBroadcast` arrives.
    pub(crate) fn apply_broadcast(&self, client_id: u64, data_bytes: &[u8]) -> Result<(), ClientError> {
        let json_data: serde_json::Value = codec::decode_delta(data_bytes)?;
        let mut peers = self.inner.peers.lock().expect("awareness lock poisoned");
        // Update or insert peer entry
        if let Some(entry) = peers.iter_mut().find(|(id, _)| *id == client_id) {
            entry.1 = json_data;
        } else {
            peers.push((client_id, json_data));
        }
        let new_val = peers.clone();
        drop(peers);
        let _ = self.inner.watch_tx.send_if_modified(|v| {
            if *v != new_val { *v = new_val; true } else { false }
        });
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn key(&self) -> &str {
        &self.inner.key
    }
}
