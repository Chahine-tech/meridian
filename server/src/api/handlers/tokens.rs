use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Deserialize;

use crate::auth::{ClaimsExt, Permissions, TokenClaims};

use super::AppStateExt;

// ---------------------------------------------------------------------------
// POST /v1/namespaces/:ns/tokens
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct IssueTokenRequest {
    pub client_id: u64,
    /// TTL in milliseconds. Default: 1 hour.
    pub ttl_ms: Option<u64>,
    pub permissions: Option<PermissionsDto>,
}

#[derive(Deserialize)]
pub struct PermissionsDto {
    pub read: bool,
    pub write: bool,
    pub admin: bool,
}

/// Issue a new token for `client_id` in the caller's namespace.
/// Requires `admin` permission on the issuing token.
pub async fn issue_token<S: AppStateExt>(
    ClaimsExt(claims): ClaimsExt,
    State(state): State<S>,
    Json(req): Json<IssueTokenRequest>,
) -> Response {
    if !claims.is_admin() {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "admin permission required" })),
        )
            .into_response();
    }

    let perms = req.permissions.map(|p| Permissions {
        read: p.read,
        write: p.write,
        admin: p.admin,
    }).unwrap_or_else(Permissions::read_write);

    let new_claims = TokenClaims::new(
        &claims.namespace,
        req.client_id,
        req.ttl_ms.unwrap_or(3_600_000),
        perms,
    );

    match state.signer().sign(&new_claims) {
        Ok(token) => Json(serde_json::json!({ "token": token })).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "token signing failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
