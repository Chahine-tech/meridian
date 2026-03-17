/// Server-side implementation of `AntiEntropyApplier` for `meridian-cluster`.
///
/// Bridges the cluster crate (which knows only raw bytes) with the server's
/// CRDT registry (`apply_op`, `CrdtOp`, `CrdtValue`).
#[cfg(any(feature = "cluster", feature = "cluster-http"))]
pub mod anti_entropy {
    use std::sync::Arc;

    use meridian_cluster::AntiEntropyApplier;

    use crate::{
        crdt::registry::{apply_op, CrdtOp, CrdtValue},
        storage::CrdtStore,
    };

    /// Wraps a `CrdtStore` and implements `AntiEntropyApplier`.
    ///
    /// Anti-entropy runs as a single background task per node, so concurrent
    /// access is not a concern here. `get` + `put` is safe in this context.
    /// Concurrent writes from multiple clients go through the HTTP/WS handlers
    /// which use `merge_put` for atomic read-merge-write on shared Postgres.
    pub struct StoreApplier<S> {
        store: Arc<S>,
    }

    impl<S: CrdtStore> StoreApplier<S> {
        pub fn new(store: Arc<S>) -> Self {
            Self { store }
        }
    }

    impl<S: CrdtStore> AntiEntropyApplier for StoreApplier<S> {
        async fn apply_wal_op(
            &self,
            namespace: &str,
            crdt_id: &str,
            op_bytes: Vec<u8>,
        ) -> Result<Option<Vec<u8>>, String> {
            let op: CrdtOp = rmp_serde::decode::from_slice(&op_bytes)
                .map_err(|e| format!("decode CrdtOp: {e}"))?;

            let crdt_type = op.crdt_type();
            let mut crdt: CrdtValue = self
                .store
                .get(namespace, crdt_id)
                .await
                .map_err(|e| format!("store.get: {e}"))?
                .unwrap_or_else(|| CrdtValue::new(crdt_type));

            let delta_bytes = apply_op(&mut crdt, op)
                .map_err(|e| format!("apply_op: {e}"))?;

            if delta_bytes.is_some() {
                self.store
                    .put(namespace, crdt_id, &crdt)
                    .await
                    .map_err(|e| format!("store.put: {e}"))?;
            }

            Ok(delta_bytes)
        }
    }
}
