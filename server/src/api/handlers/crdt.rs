use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::Deserialize;
use tracing::instrument;

use crate::{
    auth::ClaimsExt,
    crdt::{
        clock::now_ms,
        ops::{apply_op_atomic, ApplyError},
        registry::CrdtOp,
        VectorClock,
    },
    metrics,
    storage::Store,
    webhooks::WebhookEvent,
};

use super::AppStateExt;

// ---------------------------------------------------------------------------
// GET /v1/namespaces/:ns/crdts/:id
// ---------------------------------------------------------------------------

/// Returns the current JSON value of a CRDT.
#[instrument(skip(state, claims))]
pub async fn get_crdt<S: AppStateExt>(
    Path((ns, id)): Path<(String, String)>,
    ClaimsExt(claims): ClaimsExt,
    State(state): State<S>,
) -> Response {
    if claims.namespace != ns || !claims.can_read_key(&id) {
        return StatusCode::FORBIDDEN.into_response();
    }

    match state.store().get(&ns, &id).await {
        Ok(Some(crdt)) => Json(crdt.to_json_value()).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "store.get failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// POST /v1/namespaces/:ns/crdts/:id/ops
// ---------------------------------------------------------------------------

/// Applies a msgpack-encoded `CrdtOp` to a CRDT.
/// Returns the resulting delta as msgpack bytes (Content-Type: application/msgpack).
/// Returns 204 No Content if the op was a no-op.
#[instrument(skip(state, claims, body))]
pub async fn post_op<S: AppStateExt>(
    Path((ns, id)): Path<(String, String)>,
    ClaimsExt(claims): ClaimsExt,
    State(state): State<S>,
    body: axum::body::Bytes,
) -> Response {
    if claims.namespace != ns {
        return StatusCode::FORBIDDEN.into_response();
    }

    let op: CrdtOp = match rmp_serde::decode::from_slice(&body) {
        Ok(o) => o,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "malformed op", "detail": e.to_string() })),
            )
                .into_response();
        }
    };

    // For V1 tokens: key-level glob check. For V2: op-level mask check (subsumes key check).
    if !claims.can_write_key_op(&id, op.op_mask()) {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "op_not_permitted", "detail": "token does not allow this operation on this key" })),
        )
            .into_response();
    }

    // Validate HLC clock drift for LwwRegister ops (piège critique)
    if let CrdtOp::LwwRegister(ref lww_op) = op {
        let now = now_ms();
        let drift = now.abs_diff(lww_op.hlc.wall_ms);
        if drift > 30_000 {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "clock_drift",
                    "detail": format!("HLC drift of {drift}ms exceeds 30s limit")
                })),
            )
                .into_response();
        }
    }

    // HTTP API does not expose TTL — pass None (permanent entry).
    let delta_bytes = match apply_op_atomic(state.store(), &ns, &id, op, None).await {
        Ok(d) => d,
        Err(ApplyError::Crdt(e)) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "apply_failed", "detail": e.to_string() })),
            )
                .into_response();
        }
        Err(ApplyError::Storage(e)) => {
            tracing::error!(error = %e, "store failed during apply");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if let Some(ref bytes) = delta_bytes {

        metrics::record_op(&ns, &id, "http");

        if let Some(dispatcher) = state.webhooks() {
            dispatcher.send(WebhookEvent {
                ns: ns.clone(),
                crdt_id: id.clone(),
                source: "http".into(),
                timestamp_ms: now_ms(),
            });
        }

        // Fan-out delta to WebSocket subscribers
        let msg = std::sync::Arc::new(crate::api::ws::ServerMsg::Delta {
            crdt_id: id.clone(),
            delta_bytes: bytes.clone().into(),
        });
        state.subscriptions().publish(&ns, msg);

        // Fan-out delta to peer nodes via cluster transport (best-effort).
        #[cfg(any(feature = "cluster", feature = "cluster-http"))]
        if let Some(cluster) = state.cluster() {
            cluster.on_delta(&ns, &id, bytes::Bytes::from(bytes.clone())).await;
        }

        axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/msgpack")
            .body(axum::body::Body::from(bytes.clone()))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    } else {
        StatusCode::NO_CONTENT.into_response()
    }
}

// ---------------------------------------------------------------------------
// GET /v1/namespaces/:ns/crdts/:id/sync?since=<vc_base64>
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SyncQuery {
    /// Base64url-encoded msgpack VectorClock. If absent, returns full state delta.
    pub since: Option<String>,
}

/// Returns a msgpack delta since the given VectorClock checkpoint.
#[instrument(skip(state, claims))]
pub async fn get_sync<S: AppStateExt>(
    Path((ns, id)): Path<(String, String)>,
    Query(q): Query<SyncQuery>,
    ClaimsExt(claims): ClaimsExt,
    State(state): State<S>,
) -> Response {
    if claims.namespace != ns || !claims.can_read_key(&id) {
        return StatusCode::FORBIDDEN.into_response();
    }

    let since = match q.since {
        None => VectorClock::new(),
        Some(ref b64) => {
            let bytes = match URL_SAFE_NO_PAD.decode(b64) {
                Ok(b) => b,
                Err(_) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({ "error": "invalid since parameter" })),
                    )
                        .into_response();
                }
            };
            match rmp_serde::decode::from_slice::<VectorClock>(&bytes) {
                Ok(vc) => vc,
                Err(_) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({ "error": "malformed vector clock" })),
                    )
                        .into_response();
                }
            }
        }
    };

    let crdt = match state.store().get(&ns, &id).await {
        Ok(Some(c)) => c,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "store.get failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    match crdt.delta_since_msgpack(&since) {
        Ok(Some(delta_bytes)) => axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/msgpack")
            .body(axum::body::Body::from(delta_bytes))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "delta_since failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
