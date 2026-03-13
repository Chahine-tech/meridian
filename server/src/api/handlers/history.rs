use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    auth::ClaimsExt,
    crdt::registry::CrdtOp,
};

use super::AppStateExt;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct HistoryEntry {
    /// WAL sequence number — stable, monotonically increasing.
    pub seq: u64,
    /// Unix timestamp (ms) when the op was applied.
    pub timestamp_ms: u64,
    /// The decoded operation (JSON).
    pub op: serde_json::Value,
}

#[derive(Serialize)]
pub struct HistoryResponse {
    pub crdt_id: String,
    pub entries: Vec<HistoryEntry>,
    /// Sequence number to use as `since_seq` in the next request for pagination.
    /// `None` if there are no more entries.
    pub next_seq: Option<u64>,
}

// ---------------------------------------------------------------------------
// Query params
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    /// Only return entries with seq >= since_seq. Default: 0 (all entries).
    pub since_seq: Option<u64>,
    /// Maximum number of entries to return. Default: 50, max: 500.
    pub limit: Option<usize>,
}

// ---------------------------------------------------------------------------
// GET /v1/namespaces/:ns/crdts/:id/history
// ---------------------------------------------------------------------------

/// Returns the operation history for a CRDT from the WAL.
///
/// Entries are returned in chronological order (lowest seq first).
/// Use `since_seq` + `next_seq` for pagination.
///
/// Note: the WAL is compacted periodically — entries older than the last
/// compaction checkpoint are no longer available. For long-term audit logs,
/// consider persisting WAL entries externally before they are compacted.
#[instrument(skip(state, claims))]
pub async fn get_history<S: AppStateExt>(
    Path((ns, id)): Path<(String, String)>,
    Query(q): Query<HistoryQuery>,
    ClaimsExt(claims): ClaimsExt,
    State(state): State<S>,
) -> Response {
    if claims.namespace != ns || !claims.can_read_key(&id) {
        return StatusCode::FORBIDDEN.into_response();
    }

    let since_seq = q.since_seq.unwrap_or(0);
    let limit = q.limit.unwrap_or(50).min(500);

    let wal = state.wal();
    let all_entries = match wal.replay_from(since_seq) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error = %e, "WAL replay failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Filter to entries for this specific crdt_id in this namespace.
    let filtered: Vec<_> = all_entries
        .into_iter()
        .filter(|e| e.namespace == ns && e.crdt_id == id)
        .collect();

    let has_more = filtered.len() > limit;
    let page: Vec<_> = filtered.into_iter().take(limit).collect();
    let next_seq = if has_more {
        page.last().map(|e| e.seq + 1)
    } else {
        None
    };

    let entries: Vec<HistoryEntry> = page
        .into_iter()
        .map(|e| {
            // Decode op bytes to JSON for readability.
            let op_json = rmp_serde::decode::from_slice::<CrdtOp>(&e.op_bytes)
                .ok()
                .and_then(|op| serde_json::to_value(op).ok())
                .unwrap_or(serde_json::Value::Null);

            HistoryEntry {
                seq: e.seq,
                timestamp_ms: e.timestamp_ms,
                op: op_json,
            }
        })
        .collect();

    Json(HistoryResponse {
        crdt_id: id,
        entries,
        next_seq,
    })
    .into_response()
}
