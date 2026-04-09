use std::sync::{Arc, Mutex};

use meridian_core::{
    crdt::{
        clock::{now_ms, HybridLogicalClock},
        rga::{Rga, RgaDelta, RgaOp},
        Crdt,
    },
    crdt::registry::{CrdtOp, VersionedOp},
    protocol::ClientMsg,
};
use tokio::sync::watch;

use crate::{codec, error::ClientError, op_queue::OpQueue, transport::Transport};

#[allow(dead_code)]
pub(crate) struct RgaInner {
    state:     Mutex<Rga>,
    watch_tx:  watch::Sender<String>,
    crdt_id:   String,
    client_id: u64,
    transport: Arc<dyn Transport>,
    op_queue:  Arc<OpQueue>,
}

/// Live handle to an RGA collaborative text CRDT.
#[derive(Clone)]
pub struct RgaHandle(Arc<RgaInner>);

impl RgaHandle {
    pub(crate) fn new(
        crdt_id: String,
        client_id: u64,
        transport: Arc<dyn Transport>,
        op_queue: Arc<OpQueue>,
    ) -> Self {
        let (watch_tx, _) = watch::channel(String::new());
        Self(Arc::new(RgaInner {
            state: Mutex::new(Rga::default()),
            watch_tx,
            crdt_id,
            client_id,
            transport,
            op_queue,
        }))
    }

    /// Current text content.
    pub fn value(&self) -> String {
        self.0.watch_tx.borrow().clone()
    }

    pub fn watch(&self) -> watch::Receiver<String> {
        self.0.watch_tx.subscribe()
    }

    #[must_use = "dropping the JoinHandle cancels the subscription"]
    pub fn on_change<F>(&self, f: F) -> tokio::task::JoinHandle<()>
    where
        F: Fn(String) + Send + 'static,
    {
        let mut rx = self.0.watch_tx.subscribe();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                f(rx.borrow().clone());
            }
        })
    }

    /// Insert `content` at character position `pos` (0-indexed).
    pub async fn insert(&self, pos: usize, content: &str) -> Result<(), ClientError> {
        let ops = {
            let state = self.0.state.lock().expect("rga lock poisoned");
            // Collect visible nodes to find insertion origin
            let visible: Vec<_> = state.nodes.iter().filter(|n| !n.deleted).collect();
            let origin_id = if pos == 0 {
                None
            } else {
                visible.get(pos.saturating_sub(1)).map(|n| n.id)
            };
            drop(state);

            let mut ops = Vec::with_capacity(content.chars().count());
            let mut prev_origin = origin_id;
            let wall = now_ms();
            for (i, ch) in content.chars().enumerate() {
                let id = HybridLogicalClock {
                    wall_ms: wall,
                    logical: i as u16,
                    node_id: self.0.client_id,
                };
                ops.push(RgaOp::Insert { id, origin_id: prev_origin, content: ch });
                prev_origin = Some(id);
            }
            ops
        };

        {
            let mut state = self.0.state.lock().expect("rga lock poisoned");
            for op in &ops {
                state.apply(op.clone())?;
            }
            let new_text = state.value().text;
            let _ = self.0.watch_tx.send_if_modified(|v| {
                if *v != new_text { *v = new_text; true } else { false }
            });
        }

        // Send each char as a separate op (keeps ops atomic and orderable)
        for op in ops {
            let versioned = VersionedOp::new(CrdtOp::RGA(op));
            let op_bytes = codec::encode_op(&versioned)?;
            self.0.transport.send(ClientMsg::Op {
                crdt_id: self.0.crdt_id.clone(),
                op_bytes,
                ttl_ms: None,
                client_seq: None,
            }).await?;
        }
        Ok(())
    }

    /// Delete `len` characters starting at position `pos`.
    pub async fn delete(&self, pos: usize, len: usize) -> Result<(), ClientError> {
        let ops: Vec<RgaOp> = {
            let state = self.0.state.lock().expect("rga lock poisoned");
            let visible: Vec<_> = state.nodes.iter().filter(|n| !n.deleted).collect();
            visible.iter().skip(pos).take(len)
                .map(|n| RgaOp::Delete { id: n.id })
                .collect()
        };

        {
            let mut state = self.0.state.lock().expect("rga lock poisoned");
            for op in &ops {
                state.apply(op.clone())?;
            }
            let new_text = state.value().text;
            let _ = self.0.watch_tx.send_if_modified(|v| {
                if *v != new_text { *v = new_text; true } else { false }
            });
        }

        for op in ops {
            let versioned = VersionedOp::new(CrdtOp::RGA(op));
            let op_bytes = codec::encode_op(&versioned)?;
            self.0.transport.send(ClientMsg::Op {
                crdt_id: self.0.crdt_id.clone(),
                op_bytes,
                ttl_ms: None,
                client_seq: None,
            }).await?;
        }
        Ok(())
    }

    pub(crate) fn apply_delta(&self, delta_bytes: &[u8]) -> Result<(), ClientError> {
        let delta: RgaDelta = codec::decode_delta(delta_bytes)?;
        let mut state = self.0.state.lock().expect("rga lock poisoned");
        state.merge_delta(delta);
        let new_text = state.value().text;
        let _ = self.0.watch_tx.send_if_modified(|v| {
            if *v != new_text { *v = new_text; true } else { false }
        });
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn crdt_id(&self) -> &str {
        &self.0.crdt_id
    }
}
