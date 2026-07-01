use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::json;

use crate::auth::ClaimsExt;

use super::AppStateExt;

/// `DELETE /v1/namespaces/:ns/sessions/:client_id`
///
/// Immediately closes the WebSocket connection of the target client.
/// Requires the caller to hold a token with `write` access to the namespace.
/// Returns 200 if the session was found and revoked, 404 if the client is not connected.
pub async fn revoke_session<S: AppStateExt>(
    Path((ns, client_id_str)): Path<(String, String)>,
    ClaimsExt(claims): ClaimsExt,
    State(state): State<S>,
) -> impl IntoResponse {
    if claims.namespace != ns || !claims.is_admin() {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": "forbidden" }))).into_response();
    }

    let Ok(client_id) = client_id_str.parse::<u64>() else {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "invalid client_id" }))).into_response();
    };

    if state.session_registry().revoke(&ns, client_id) {
        (StatusCode::OK, Json(json!({ "revoked": true, "client_id": client_id }))).into_response()
    } else {
        (StatusCode::NOT_FOUND, Json(json!({ "error": "session not found", "client_id": client_id }))).into_response()
    }
}
