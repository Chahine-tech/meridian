use std::sync::{Arc, Mutex};

use meridian_core::{
    crdt::{
        gcounter::{GCounter, GCounterDelta, GCounterOp},
        Crdt,
    },
    protocol::ClientMsg,
    crdt::registry::{CrdtOp, VersionedOp},
};
use tokio::sync::watch;

use crate::{codec, error::ClientError, op_queue::OpQueue, transport::Transport};

#[allow(dead_code)]
pub(crate) struct GCounterInner {
    state:     Mutex<GCounter>,
    watch_tx:  watch::Sender<u64>,
    crdt_id:   String,
    client_id: u64,
    transport: Arc<dyn Transport>,
    op_queue:  Arc<OpQueue>,
}

/// Live handle to a GCounter CRDT. Cloneable and `Send + Sync`.
///
/// Multiple clones share the same inner state — safe to hold in separate tasks
/// (e.g. one render task, one SDK task in a Ratatui app).
#[derive(Clone)]
pub struct GCounterHandle(Arc<GCounterInner>);

impl GCounterHandle {
    pub(crate) fn new(
        crdt_id: String,
        client_id: u64,
        transport: Arc<dyn Transport>,
        op_queue: Arc<OpQueue>,
    ) -> Self {
        let state = GCounter::default();
        let total = state.value().total;
        let (watch_tx, _) = watch::channel(total);
        Self(Arc::new(GCounterInner {
            state: Mutex::new(state),
            watch_tx,
            crdt_id,
            client_id,
            transport,
            op_queue,
        }))
    }

    /// Current total across all clients.
    pub fn value(&self) -> u64 {
        *self.0.watch_tx.borrow()
    }

    /// Subscribe to value changes. The receiver always holds the latest value.
    pub fn watch(&self) -> watch::Receiver<u64> {
        self.0.watch_tx.subscribe()
    }

    /// Register a callback invoked whenever the value changes.
    /// Returns a `JoinHandle` — dropping it cancels the subscription.
    pub fn on_change<F>(&self, f: F) -> tokio::task::JoinHandle<()>
    where
        F: Fn(u64) + Send + 'static,
    {
        let mut rx = self.0.watch_tx.subscribe();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                f(*rx.borrow());
            }
        })
    }

    /// Increment the counter by `amount`. Applies optimistically and sends to server.
    pub async fn increment(&self, amount: u64) -> Result<(), ClientError> {
        let op = GCounterOp { client_id: self.0.client_id, amount };

        // Optimistic local apply
        {
            let mut state = self.0.state.lock().expect("gcounter lock poisoned");
            if let Some(_delta) = state.apply(op.clone())? {
                let new_total = state.value().total;
                let _ = self.0.watch_tx.send_if_modified(|v| {
                    if *v != new_total { *v = new_total; true } else { false }
                });
            }
        }

        let versioned = VersionedOp::new(CrdtOp::GCounter(op));
        let op_bytes = codec::encode_op(&versioned)?;
        let msg = ClientMsg::Op {
            crdt_id: self.0.crdt_id.clone(),
            op_bytes,
            ttl_ms: None,
            client_seq: None,
        };
        self.0.transport.send(msg).await
    }

    /// Apply a delta received from the server. Called by the dispatch loop.
    pub(crate) fn apply_delta(&self, delta_bytes: &[u8]) -> Result<(), ClientError> {
        let delta: GCounterDelta = codec::decode_delta(delta_bytes)?;
        let mut state = self.0.state.lock().expect("gcounter lock poisoned");
        state.merge_delta(delta);
        let new_total = state.value().total;
        let _ = self.0.watch_tx.send_if_modified(|v| {
            if *v != new_total { *v = new_total; true } else { false }
        });
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn crdt_id(&self) -> &str {
        &self.0.crdt_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::fake::FakeTransport;

    fn make_handle() -> (GCounterHandle, crate::transport::fake::FakeHandle) {
        let (transport, handle) = FakeTransport::new();
        let op_queue = Arc::new(OpQueue::new());
        let h = GCounterHandle::new("gc:test".into(), 1, transport, op_queue);
        (h, handle)
    }

    #[tokio::test]
    async fn increment_updates_value() {
        let (h, _fh) = make_handle();
        assert_eq!(h.value(), 0);
        h.increment(5).await.unwrap();
        assert_eq!(h.value(), 5);
    }

    #[tokio::test]
    async fn increment_sends_op() {
        let (h, fh) = make_handle();
        h.increment(3).await.unwrap();
        let sent = fh.sent();
        assert_eq!(sent.len(), 1);
        assert!(matches!(&sent[0], ClientMsg::Op { crdt_id, .. } if crdt_id == "gc:test"));
    }

    #[tokio::test]
    async fn apply_delta_merges() {
        let (h, _fh) = make_handle();
        // Simulate server delta: another client incremented by 10
        let remote_delta = GCounterDelta {
            counters: [(2u64, 10u64)].into_iter().collect(),
        };
        let bytes = rmp_serde::encode::to_vec_named(&remote_delta).unwrap();
        h.apply_delta(&bytes).unwrap();
        assert_eq!(h.value(), 10);
    }

    #[tokio::test]
    async fn on_change_fires() {
        let (h, _fh) = make_handle();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = std::sync::Mutex::new(Some(tx));
        let _guard = h.on_change(move |v| {
            if let Some(sender) = tx.lock().unwrap().take() {
                let _ = sender.send(v);
            }
        });
        h.increment(7).await.unwrap();
        let received = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            async { rx.await.ok() }
        ).await;
        assert!(matches!(received, Ok(Some(7))));
    }
}
