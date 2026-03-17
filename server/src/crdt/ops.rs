use thiserror::Error;

use crate::{
    crdt::registry::{apply_op, CrdtOp, CrdtValue},
    storage::{CrdtStore, StorageError},
};

// ---------------------------------------------------------------------------
// ApplyError
// ---------------------------------------------------------------------------

/// Error returned by [`apply_op_atomic`].
///
/// Separates storage failures (infrastructure) from CRDT logic failures
/// (bad op, type mismatch) so callers can map each to the right HTTP status.
#[derive(Debug, Error)]
pub enum ApplyError {
    /// The op could not be applied (wrong type, invalid state, etc.).
    #[error("crdt error: {0}")]
    Crdt(#[from] crate::crdt::CrdtError),

    /// The underlying store failed.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

// ---------------------------------------------------------------------------
// apply_op_atomic
// ---------------------------------------------------------------------------

/// Load a CRDT, apply `op`, and persist the result atomically.
///
/// Uses [`Store::merge_put_with`] so that on shared PostgreSQL the
/// read-merge-write is protected by a `SELECT FOR UPDATE` transaction,
/// preventing lost updates from concurrent writes across nodes.
///
/// Returns `Ok(Some(delta_bytes))` if the op produced a delta,
/// `Ok(None)` if it was a no-op, or `Err` on storage/CRDT failure.
pub async fn apply_op_atomic<S: CrdtStore>(
    store: &S,
    ns: &str,
    crdt_id: &str,
    op: CrdtOp,
) -> Result<Option<Vec<u8>>, ApplyError> {
    let crdt_type = op.crdt_type();

    let delta = store
        .merge_put_with(ns, crdt_id, CrdtValue::new(crdt_type), |existing, default| {
            let mut crdt = existing.unwrap_or(default);
            let delta = apply_op(&mut crdt, op);
            (crdt, delta)
        })
        .await
        .map_err(ApplyError::Storage)?
        .map_err(ApplyError::Crdt)?;

    Ok(delta)
}
