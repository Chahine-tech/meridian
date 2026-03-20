use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use serde_json::json;
use tracing::warn;

use super::{AuthError, TokenClaims, TokenSigner};
use crate::rate_limit::RateLimiter;

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

/// Combined auth + rate-limit state threaded through the middleware.
#[derive(Clone)]
pub struct AuthState {
    pub signer: Arc<TokenSigner>,
    pub rate_limiter: Arc<RateLimiter>,
}

/// Axum middleware that validates a Meridian token on every request,
/// then enforces per-token rate limiting (100 req/s sliding window).
///
/// Token lookup order:
/// 1. `Authorization: Bearer <token>` header
/// 2. `?token=<token>` query parameter (for WebSocket handshakes)
///
/// On success: inserts `TokenClaims` into request extensions.
/// On failure: returns a structured JSON 401/403/429.
pub async fn auth_middleware(
    State(auth_state): State<Arc<AuthState>>,
    mut req: Request,
    next: Next,
) -> Response {
    let token = extract_token(&req);

    match token {
        None => auth_error_response(AuthError::MissingToken),
        Some(t) => match auth_state.signer.verify(t) {
            Err(e) => {
                warn!(error = %e, "auth rejected");
                auth_error_response(e)
            }
            Ok(claims) => {
                let rate_key = format!("{}:{}", claims.namespace, claims.client_id);
                if !auth_state.rate_limiter.check(&rate_key) {
                    return (
                        StatusCode::TOO_MANY_REQUESTS,
                        Json(json!({ "error": "rate_limited", "message": "too many requests" })),
                    )
                        .into_response();
                }
                req.extensions_mut().insert(claims);
                next.run(req).await
            }
        },
    }
}

fn extract_token(req: &Request) -> Option<&str> {
    if let Some(auth) = req.headers().get(header::AUTHORIZATION)
        && let Ok(s) = auth.to_str()
        && let Some(token) = s.strip_prefix("Bearer ")
    {
        return Some(token);
    }

    if let Some(query) = req.uri().query() {
        for pair in query.split('&') {
            if let Some(val) = pair.strip_prefix("token=") {
                return Some(val);
            }
        }
    }

    None
}

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
