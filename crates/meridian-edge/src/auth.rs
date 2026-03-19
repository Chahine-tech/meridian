use meridian_core::auth::{AuthError, TokenClaims, TokenSigner};
use worker::{Env, Request};

/// Extract the Bearer token from the Authorization header or `?token=` query param.
fn extract_bearer(req: &Request) -> Result<String, AuthError> {
    // Try Authorization header first
    if let Some(header) = req.headers().get("Authorization").ok().flatten() {
        return header
            .strip_prefix("Bearer ")
            .map(|t| t.to_owned())
            .ok_or(AuthError::MissingToken);
    }

    // Fallback: query param (WebSocket upgrades can't set custom headers in browsers)
    req.url()
        .ok()
        .and_then(|u| {
            u.query_pairs()
                .find(|(k, _)| k == "token")
                .map(|(_, v)| v.into_owned())
        })
        .ok_or(AuthError::MissingToken)
}

/// Validate the request's Bearer token using the signing key from the Worker env.
///
/// Returns the decoded `TokenClaims` on success, or an `AuthError` on failure.
pub fn validate(req: &Request, env: &Env) -> Result<TokenClaims, AuthError> {
    let signing_key = env
        .secret("MERIDIAN_SIGNING_KEY")
        .map_err(|_| AuthError::MissingToken)?
        .to_string();

    let signer = TokenSigner::from_hex(&signing_key)
        .map_err(|e| AuthError::Signing(e.to_string()))?;

    let token = extract_bearer(req)?;
    signer.verify(&token)
}

/// Build a JSON error response for auth failures.
pub fn auth_error_response(err: &AuthError) -> worker::Response {
    let (code, message) = match err {
        AuthError::MissingToken => (401, "missing or malformed Authorization header"),
        AuthError::InvalidFormat => (401, "token format invalid"),
        AuthError::SignatureInvalid => (401, "signature invalid"),
        AuthError::Expired { .. } => (401, "token expired"),
        AuthError::InsufficientPermissions { .. } => (403, "insufficient permissions"),
        _ => (401, "authentication failed"),
    };

    let body = serde_json::json!({ "error": message, "code": code }).to_string();
    worker::Response::error(body, code).unwrap_or_else(|_| worker::Response::error("error", 500).unwrap())
}
