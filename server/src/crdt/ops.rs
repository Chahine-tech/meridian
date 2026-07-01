use meridian_core::protocol::ConflictKind;
use thiserror::Error;

use crate::{
    crdt::{
        HybridLogicalClock,
        lwwregister::LwwOp,
        registry::{CrdtOp, CrdtType, CrdtValue, apply_op},
    },
    metrics,
    storage::{CrdtStore, StorageError},
};

// ApplyError

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

// apply_op_atomic

/// Load a CRDT, apply `op`, and persist the result atomically.
///
/// Uses [`Store::merge_put_with_expiry`] so that on shared PostgreSQL the
/// read-merge-write is protected by a `SELECT FOR UPDATE` transaction,
/// preventing lost updates from concurrent writes across nodes.
///
/// `ttl_ms` — if `Some(n)`, the CRDT entry will be marked for deletion by
/// the GC task after `n` milliseconds from now.  Pass `None` for permanent
/// entries (the default).
///
/// Returns `Ok((delta_bytes, conflict))`:
/// - `delta_bytes` — serialised delta to broadcast, or `None` if the op was a no-op.
/// - `conflict`    — present when the op was superseded or structurally rejected.
///
/// Callers should forward the conflict (if any) privately to the originating
/// connection via [`ServerMsg::Conflict`] without broadcasting it.
pub async fn apply_op_atomic<S: CrdtStore>(
    store: &S,
    ns: &str,
    crdt_id: &str,
    op: CrdtOp,
    ttl_ms: Option<u64>,
) -> Result<(Option<Vec<u8>>, Option<ConflictKind>), ApplyError> {
    let crdt_type = op.crdt_type();

    // Compute expires_at_ms once so the storage layer doesn't need to know
    // about wall-clock time.
    let expires_at_ms = ttl_ms.map(|ms| crate::crdt::clock::now_ms().saturating_add(ms));

    let t = metrics::merge_start();
    let (delta, conflict) = store
        .merge_put_with_expiry(
            ns,
            crdt_id,
            CrdtValue::new(crdt_type),
            expires_at_ms,
            |existing, default| {
                let mut crdt = existing.unwrap_or(default);
                let result = apply_op(&mut crdt, op);
                (crdt, result)
            },
        )
        .await
        .map_err(ApplyError::Storage)?
        .map_err(ApplyError::Crdt)?;
    metrics::record_merge_duration(ns, t.elapsed());

    if delta.is_some() {
        metrics::record_wal_entry();
    }

    Ok((delta, conflict))
}

// apply_undo_lww_atomic

/// Outcome of [`apply_undo_lww_atomic`].
pub enum UndoLwwResult {
    /// Undo was applied; `delta_bytes` should be broadcast to all subscribers.
    Applied(Vec<u8>),
    /// Undo was skipped — the register was already overwritten by a concurrent
    /// write. This is not an error; it is the intended behaviour.
    Skipped(String),
}

/// Undo a specific `LwwRegister::Set` op, server-side validated.
///
/// Loads the register and checks whether `target_hlc` is still the winning
/// entry. If so, applies a fresh `LwwOp` with `restore_value` (or JSON null
/// if the register was empty before the undone write) stamped with the
/// current server wall clock so it wins over `target_hlc`.
///
/// If another client has since overwritten the register (`entry.hlc ≠ target_hlc`),
/// the undo is skipped — the server returns `Skipped` so the caller can send
/// `ServerMsg::UndoSkipped` back to the client. This is the Kleppmann & Stewen
/// PaPoC 2024 property: undo commutes with concurrent remote writes.
///
/// `requester_client_id` — the `client_id` from the WebSocket connection's token.
///   Used as the `author` on the restore op so the entry can be attributed.
pub async fn apply_undo_lww_atomic<S: CrdtStore>(
    store: &S,
    ns: &str,
    crdt_id: &str,
    target_hlc: HybridLogicalClock,
    restore_value: Option<serde_json::Value>,
    requester_client_id: u64,
) -> Result<UndoLwwResult, ApplyError> {
    let now_ms = crate::crdt::clock::now_ms();

    let result: Result<UndoLwwResult, String> = store
        .merge_put_with(
            ns,
            crdt_id,
            CrdtValue::new(CrdtType::LwwRegister),
            |existing, default| {
                let mut crdt = existing.unwrap_or(default);

                let CrdtValue::LwwRegister(ref mut reg) = crdt else {
                    return (crdt, Err("crdt is not a lwwregister".to_string()));
                };

                let is_current = reg.entry.as_ref().is_some_and(|e| e.hlc == target_hlc);
                if !is_current {
                    return (
                        crdt,
                        Ok(UndoLwwResult::Skipped(
                            "entry has been overwritten by a concurrent write".to_string(),
                        )),
                    );
                }

                // The restore HLC must beat target_hlc in LWW ordering.
                // Use max(now_ms, target_hlc.wall_ms) + 1 to guarantee it wins
                // even when the client's op had a clock slightly ahead of the server.
                let restore_wall_ms = now_ms.max(target_hlc.wall_ms).saturating_add(1);
                let restore_op = CrdtOp::LwwRegister(LwwOp {
                    value: restore_value.unwrap_or(serde_json::Value::Null),
                    hlc: HybridLogicalClock {
                        wall_ms: restore_wall_ms,
                        logical: 0,
                        node_id: requester_client_id,
                    },
                    author: requester_client_id,
                });

                match apply_op(&mut crdt, restore_op) {
                    Err(e) => (crdt, Err(e.to_string())),
                    Ok((Some(delta_bytes), _)) => {
                        (crdt, Ok(UndoLwwResult::Applied(delta_bytes)))
                    }
                    Ok((None, _)) => (
                        crdt,
                        Ok(UndoLwwResult::Skipped(
                            "restore value is identical to current".to_string(),
                        )),
                    ),
                }
            },
        )
        .await
        .map_err(ApplyError::Storage)?;

    result.map_err(|e| ApplyError::Crdt(crate::crdt::CrdtError::InvalidOp(e)))
}
