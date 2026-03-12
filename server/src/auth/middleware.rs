use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use serde_json::json;
use tracing::warn;

use super::{claims::TokenClaims, error::AuthError, signer::TokenSigner};

// ---------------------------------------------------------------------------
// Error response helper
// ---------------------------------------------------------------------------

fn auth_error_response(err: AuthError) -> Response {
    let (status, code, message) = match &err {
        AuthError::MissingToken => (StatusCode::UNAUTHORIZED, "missing_token", err.to_string()),
        AuthError::InvalidFormat => (StatusCode::UNAUTHORIZED, "invalid_format", err.to_string()),
        AuthError::SignatureInvalid => (StatusCode::UNAUTHORIZED, "invalid_signature", err.to_string()),
        AuthError::Expired { .. } => (StatusCode::UNAUTHORIZED, "token_expired", err.to_string()),
        AuthError::InsufficientPermissions { .. } => {
            (StatusCode::FORBIDDEN, "insufficient_permissions", err.to_string())
        }
        _ => (StatusCode::UNAUTHORIZED, "auth_error", err.to_string()),
    };

    (status, Json(json!({ "error": code, "message": message }))).into_response()
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

/// Axum middleware that validates a Meridian token on every request.
///
/// Token lookup order:
/// 1. `Authorization: Bearer <token>` header
/// 2. `?token=<token>` query parameter (for WebSocket handshakes)
///
/// On success: inserts `TokenClaims` into request extensions.
/// On failure: returns a structured JSON 401/403.
pub async fn auth_middleware(
    State(signer): State<Arc<TokenSigner>>,
    mut req: Request,
    next: Next,
) -> Response {
    let token = extract_token(&req);

    match token {
        None => auth_error_response(AuthError::MissingToken),
        Some(t) => match signer.verify(t) {
            Err(e) => {
                warn!(error = %e, "auth rejected");
                auth_error_response(e)
            }
            Ok(claims) => {
                req.extensions_mut().insert(claims);
                next.run(req).await
            }
        },
    }
}

/// Extract the raw token string from the request (does not allocate if not found).
fn extract_token(req: &Request) -> Option<&str> {
    // 1. Authorization header
    if let Some(auth) = req.headers().get(header::AUTHORIZATION)
        && let Ok(s) = auth.to_str()
        && let Some(token) = s.strip_prefix("Bearer ")
    {
        return Some(token);
    }

    // 2. ?token= query param
    if let Some(query) = req.uri().query() {
        for pair in query.split('&') {
            if let Some(val) = pair.strip_prefix("token=") {
                return Some(val);
            }
        }
    }

    None
}

// ---------------------------------------------------------------------------
// Extractor: pull TokenClaims from extensions
// ---------------------------------------------------------------------------

/// Axum extractor that retrieves `TokenClaims` inserted by the middleware.
/// Use this in handlers: `async fn handler(claims: ClaimsExt, ...) -> ...`
pub struct ClaimsExt(pub TokenClaims);

impl<S> axum::extract::FromRequestParts<S> for ClaimsExt
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<TokenClaims>()
            .cloned()
            .map(ClaimsExt)
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({ "error": "unauthenticated" })),
                )
            })
    }
}
