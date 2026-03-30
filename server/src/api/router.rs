use std::sync::Arc;

use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::auth::AuthState;

use super::handlers::{
    crdt::{get_crdt, get_sync, post_op},
    health::{health_live, health_ready},
    history::get_history,
    metrics::get_metrics,
    query::post_query,
    sse::get_sse,
    tokens::{issue_token, token_me},
    AppStateExt,
};
use super::ws::ws_upgrade_handler;
use crate::auth::auth_middleware;

/// Build the full axum `Router`.
///
/// Routes:
/// ```text
/// GET  /v1/namespaces/:ns/crdts/:id           -> get_crdt
/// POST /v1/namespaces/:ns/crdts/:id/ops       -> post_op
/// GET  /v1/namespaces/:ns/crdts/:id/sync      -> get_sync
/// GET  /v1/namespaces/:ns/crdts/:id/history   -> get_history
/// GET  /v1/namespaces/:ns/crdts/:id/events    -> get_sse (SSE stream)
/// POST /v1/namespaces/:ns/query               -> post_query
/// POST /v1/namespaces/:ns/tokens              -> issue_token
/// GET  /v1/namespaces/:ns/tokens/me           -> token_me
/// GET  /v1/namespaces/:ns/connect             -> ws_upgrade_handler
/// GET  /metrics                               -> get_metrics (no auth)
/// GET  /health/live                           -> health_live (no auth, always 200)
/// GET  /health/ready                          -> health_ready (no auth, checks store+WAL)
/// ```
pub fn build_router<S>(state: S, auth_state: Arc<AuthState>) -> Router
where
    S: AppStateExt + crate::api::ws::WsState<S = <S as AppStateExt>::S>,
{
    let protected = Router::new()
        .route("/v1/namespaces/{ns}/crdts/{id}", get(get_crdt::<S>))
        .route("/v1/namespaces/{ns}/crdts/{id}/ops", post(post_op::<S>))
        .route("/v1/namespaces/{ns}/crdts/{id}/sync", get(get_sync::<S>))
        .route("/v1/namespaces/{ns}/crdts/{id}/history", get(get_history::<S>))
        .route("/v1/namespaces/{ns}/crdts/{id}/events", get(get_sse::<S>))
        .route("/v1/namespaces/{ns}/query", post(post_query::<S>))
        .route("/v1/namespaces/{ns}/tokens", post(issue_token::<S>))
        .route("/v1/namespaces/{ns}/tokens/me", get(token_me::<S>))
        .route("/v1/namespaces/{ns}/connect", get(ws_upgrade_handler::<S>))
        .layer(middleware::from_fn_with_state(auth_state, auth_middleware))
        .with_state(state.clone());

    let unprotected = Router::new()
        .route("/health/ready", get(health_ready::<S>))
        .with_state(state);

    Router::new()
        .merge(protected)
        .merge(unprotected)
        .route("/metrics", get(get_metrics))
        .route("/health/live", get(health_live))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}
