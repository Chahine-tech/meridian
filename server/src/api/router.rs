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
    history::get_history,
    metrics::get_metrics,
    tokens::issue_token,
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
/// POST /v1/namespaces/:ns/tokens              -> issue_token
/// GET  /v1/namespaces/:ns/connect             -> ws_upgrade_handler
/// GET  /metrics                               -> get_metrics (no auth)
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
        .route("/v1/namespaces/{ns}/tokens", post(issue_token::<S>))
        .route("/v1/namespaces/{ns}/connect", get(ws_upgrade_handler::<S>))
        .layer(middleware::from_fn_with_state(auth_state, auth_middleware))
        .with_state(state);

    Router::new()
        .merge(protected)
        .route("/metrics", get(get_metrics))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}
