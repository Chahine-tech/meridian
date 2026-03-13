use std::sync::Arc;

use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::auth::{auth_middleware, TokenSigner};

use super::handlers::{
    crdt::{get_crdt, get_sync, post_op},
    history::get_history,
    tokens::issue_token,
    AppStateExt,
};
use super::ws::ws_upgrade_handler;

/// Build the full axum `Router`.
///
/// Routes:
/// ```text
/// GET  /v1/namespaces/:ns/crdts/:id           -> get_crdt
/// POST /v1/namespaces/:ns/crdts/:id/ops       -> post_op
/// GET  /v1/namespaces/:ns/crdts/:id/sync      -> get_sync
/// POST /v1/namespaces/:ns/tokens              -> issue_token
/// GET  /v1/namespaces/:ns/connect             -> ws_upgrade_handler
/// ```
pub fn build_router<S>(state: S) -> Router
where
    S: AppStateExt + crate::api::ws::WsState<S = <S as AppStateExt>::S>,
{
    let signer: Arc<TokenSigner> = state.signer().clone();

    Router::new()
        // CRDT REST
        .route("/v1/namespaces/{ns}/crdts/{id}", get(get_crdt::<S>))
        .route("/v1/namespaces/{ns}/crdts/{id}/ops", post(post_op::<S>))
        .route("/v1/namespaces/{ns}/crdts/{id}/sync", get(get_sync::<S>))
        .route("/v1/namespaces/{ns}/crdts/{id}/history", get(get_history::<S>))
        // Token issuance (admin only)
        .route("/v1/namespaces/{ns}/tokens", post(issue_token::<S>))
        // WebSocket
        .route("/v1/namespaces/{ns}/connect", get(ws_upgrade_handler::<S>))
        // Auth middleware (runs on all routes)
        .layer(middleware::from_fn_with_state(signer, auth_middleware))
        // Observability
        .layer(TraceLayer::new_for_http())
        // CORS (permissive for dev — tighten in production via config)
        .layer(CorsLayer::permissive())
        .with_state(state)
}
