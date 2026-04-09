use std::sync::{Arc, Mutex};

use meridian_core::{
    crdt::{
        pncounter::{PNCounter, PNCounterDelta, PNCounterOp},
        Crdt,
    },
    crdt::registry::{CrdtOp, VersionedOp},
    protocol::ClientMsg,
};
use tokio::sync::watch;

use crate::{codec, error::ClientError, op_queue::OpQueue, transport::Transport};

#[allow(dead_code)]
pub(crate) struct PNCounterInner {
    state:     Mutex<PNCounter>,
    watch_tx:  watch::Sender<i64>,
    crdt_id:   String,
    client_id: u64,
    transport: Arc<dyn Transport>,
    op_queue:  Arc<OpQueue>,
}

#[derive(Clone)]
pub struct PNCounterHandle(Arc<PNCounterInner>);

impl PNCounterHandle {
    pub(crate) fn new(
        crdt_id: String,
        client_id: u64,
        transport: Arc<dyn Transport>,
        op_queue: Arc<OpQueue>,
    ) -> Self {
        let state = PNCounter::default();
        let val = state.value().value;
        let (watch_tx, _) = watch::channel(val);
        Self(Arc::new(PNCounterInner {
            state: Mutex::new(state),
            watch_tx,
            crdt_id,
            client_id,
            transport,
            op_queue,
        }))
    }

    pub fn value(&self) -> i64 {
        *self.0.watch_tx.borrow()
    }

    pub fn watch(&self) -> watch::Receiver<i64> {
        self.0.watch_tx.subscribe()
    }

    #[must_use = "dropping the JoinHandle cancels the subscription"]
    pub fn on_change<F>(&self, f: F) -> tokio::task::JoinHandle<()>
    where
        F: Fn(i64) + Send + 'static,
    {
        let mut rx = self.0.watch_tx.subscribe();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                f(*rx.borrow());
            }
        })
    }

    pub async fn increment(&self, amount: u64) -> Result<(), ClientError> {
        self.apply_and_send(PNCounterOp::Increment { client_id: self.0.client_id, amount }).await
    }

    pub async fn decrement(&self, amount: u64) -> Result<(), ClientError> {
        self.apply_and_send(PNCounterOp::Decrement { client_id: self.0.client_id, amount }).await
    }

    async fn apply_and_send(&self, op: PNCounterOp) -> Result<(), ClientError> {
        {
            let mut state = self.0.state.lock().expect("pncounter lock poisoned");
            if state.apply(op.clone())?.is_some() {
                let new_val = state.value().value;
                let _ = self.0.watch_tx.send_if_modified(|v| {
                    if *v != new_val { *v = new_val; true } else { false }
                });
            }
        }
        let versioned = VersionedOp::new(CrdtOp::PNCounter(op));
        let op_bytes = codec::encode_op(&versioned)?;
        self.0.transport.send(ClientMsg::Op {
            crdt_id: self.0.crdt_id.clone(),
            op_bytes,
            ttl_ms: None,
            client_seq: None,
        }).await
    }

    pub(crate) fn apply_delta(&self, delta_bytes: &[u8]) -> Result<(), ClientError> {
        let delta: PNCounterDelta = codec::decode_delta(delta_bytes)?;
        let mut state = self.0.state.lock().expect("pncounter lock poisoned");
        state.merge_delta(delta);
        let new_val = state.value().value;
        let _ = self.0.watch_tx.send_if_modified(|v| {
            if *v != new_val { *v = new_val; true } else { false }
        });
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn crdt_id(&self) -> &str {
        &self.0.crdt_id
    }
}
