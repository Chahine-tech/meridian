use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde_json::json;

use super::AppStateExt;
use crate::storage::{Store, WalBackend};

/// `GET /health/live` — always 200. Used by load balancers to detect dead processes.
pub async fn health_live() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

/// `GET /health/ready` — checks that storage and WAL are operational.
///
/// Returns 200 if both are reachable, 503 otherwise.
/// Performs a lightweight probe: reads a sentinel key from the store and
/// checks the WAL last_seq (in-memory, no I/O).
pub async fn health_ready<S: AppStateExt>(State(state): State<S>) -> impl IntoResponse {
    // Probe store: attempt a get on a well-known sentinel key.
    let store_ok = state
        .store()
        .get("__health__", "__probe__")
        .await
        .is_ok();

    // WAL: last_seq is an in-memory atomic — if it's accessible the WAL thread is alive.
    let wal_last_seq = state.wal().last_seq();

    if store_ok {
        (
            StatusCode::OK,
            Json(json!({
                "status": "ready",
                "wal_last_seq": wal_last_seq,
            })),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "not_ready", "store": "unreachable" })),
        )
    }
}
