use std::sync::{Arc, Mutex};

use meridian_core::{
    crdt::{
        clock::{now_ms, HybridLogicalClock},
        tree::{DiscardedMove, TreeCrdt, TreeDelta, TreeNodeValue, TreeOp},
        Crdt,
    },
    crdt::registry::{CrdtOp, VersionedOp},
    protocol::ClientMsg,
};
use tokio::sync::watch;

use crate::{codec, error::ClientError, op_queue::OpQueue, transport::Transport};

#[allow(dead_code)]
pub(crate) struct TreeInner {
    state:     Mutex<TreeCrdt>,
    watch_tx:  watch::Sender<Vec<TreeNodeValue>>,
    crdt_id:   String,
    client_id: u64,
    transport: Arc<dyn Transport>,
    op_queue:  Arc<OpQueue>,
}

/// Live handle to a Tree CRDT (Kleppmann et al. move-operation algorithm).
#[derive(Clone)]
pub struct TreeHandle(Arc<TreeInner>);

impl TreeHandle {
    pub(crate) fn new(
        crdt_id: String,
        client_id: u64,
        transport: Arc<dyn Transport>,
        op_queue: Arc<OpQueue>,
    ) -> Self {
        let (watch_tx, _) = watch::channel(vec![]);
        Self(Arc::new(TreeInner {
            state: Mutex::new(TreeCrdt::default()),
            watch_tx,
            crdt_id,
            client_id,
            transport,
            op_queue,
        }))
    }

    /// Current tree roots (recursive structure).
    pub fn value(&self) -> Vec<TreeNodeValue> {
        self.0.watch_tx.borrow().clone()
    }

    pub fn watch(&self) -> watch::Receiver<Vec<TreeNodeValue>> {
        self.0.watch_tx.subscribe()
    }

    pub fn on_change<F>(&self, f: F) -> tokio::task::JoinHandle<()>
    where
        F: Fn(Vec<TreeNodeValue>) + Send + 'static,
    {
        let mut rx = self.0.watch_tx.subscribe();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                f(rx.borrow().clone());
            }
        })
    }

    /// Add a new node. Returns the node's HLC id.
    pub async fn add_node(
        &self,
        parent_id: Option<HybridLogicalClock>,
        position: &str,
        value: &str,
    ) -> Result<HybridLogicalClock, ClientError> {
        let id = HybridLogicalClock {
            wall_ms: now_ms(),
            logical: 0,
            node_id: self.0.client_id,
        };
        let op = TreeOp::AddNode {
            id,
            parent_id,
            position: position.to_owned(),
            value: value.to_owned(),
        };
        self.apply_and_send(op).await?;
        Ok(id)
    }

    /// Move a node to a new parent and position.
    pub async fn move_node(
        &self,
        node_id: HybridLogicalClock,
        new_parent_id: Option<HybridLogicalClock>,
        new_position: &str,
    ) -> Result<(), ClientError> {
        let op_id = HybridLogicalClock {
            wall_ms: now_ms(),
            logical: 0,
            node_id: self.0.client_id,
        };
        let op = TreeOp::MoveNode {
            op_id,
            node_id,
            new_parent_id,
            new_position: new_position.to_owned(),
        };
        self.apply_and_send(op).await
    }

    /// Update a node's value (LWW).
    pub async fn update_node(
        &self,
        id: HybridLogicalClock,
        value: &str,
    ) -> Result<(), ClientError> {
        let updated_at = HybridLogicalClock {
            wall_ms: now_ms(),
            logical: 0,
            node_id: self.0.client_id,
        };
        let op = TreeOp::UpdateNode {
            id,
            value: value.to_owned(),
            updated_at,
        };
        self.apply_and_send(op).await
    }

    /// Delete a node (tombstone — children are preserved).
    pub async fn delete_node(&self, id: HybridLogicalClock) -> Result<(), ClientError> {
        self.apply_and_send(TreeOp::DeleteNode { id }).await
    }

    async fn apply_and_send(&self, op: TreeOp) -> Result<(), ClientError> {
        {
            let mut state = self.0.state.lock().expect("tree lock poisoned");
            state.apply(op.clone())?;
            self.notify(&state);
        }
        let versioned = VersionedOp::new(CrdtOp::Tree(op));
        let op_bytes = codec::encode_op(&versioned)?;
        self.0.transport.send(ClientMsg::Op {
            crdt_id: self.0.crdt_id.clone(),
            op_bytes,
            ttl_ms: None,
            client_seq: None,
        }).await
    }

    fn notify(&self, state: &TreeCrdt) {
        let roots = state.value().roots;
        let _ = self.0.watch_tx.send_if_modified(|v| {
            // Simple length check to avoid deep comparison on every delta
            if v.len() != roots.len() { *v = roots; return true; }
            *v = roots;
            true
        });
    }

    /// Called by the dispatch loop for incoming deltas.
    pub(crate) fn apply_delta(&self, delta_bytes: &[u8]) -> Result<Vec<DiscardedMove>, ClientError> {
        let delta: TreeDelta = codec::decode_delta(delta_bytes)?;
        let discarded = delta.discarded_moves.clone();
        let mut state = self.0.state.lock().expect("tree lock poisoned");
        state.merge_delta(delta);
        self.notify(&state);
        Ok(discarded)
    }

    #[allow(dead_code)]
    pub(crate) fn crdt_id(&self) -> &str {
        &self.0.crdt_id
    }
}
