//! Auth helpers: token parsing and permission checks.
//!
//! Note: This module does NOT verify token signatures — the client does not
//! have the server's signing key. Use `TokenSigner::verify` server-side.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use meridian_core::auth::{TokenClaims, glob_match};

pub use meridian_core::auth::{OpMask, PermEntry, Permissions, PermissionsV1, PermissionsV2};

use crate::error::ClientError;

/// Decode a Meridian token and return its claims **without** verifying the signature.
///
/// Useful client-side for reading `client_id`, `expires_at`, and `permissions`
/// from the token the server issued to you.
pub fn parse_token(token: &str) -> Result<TokenClaims, ClientError> {
    let payload_b64 = token
        .split('.')
        .next()
        .ok_or_else(|| ClientError::Crypto("invalid token format".into()))?;
    let payload = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|e: base64::DecodeError| ClientError::Crypto(e.to_string()))?;
    rmp_serde::decode::from_slice(&payload).map_err(ClientError::Decode)
}

/// Returns `true` if the token's permissions allow reading `crdt_id`.
pub fn can_read(claims: &TokenClaims, crdt_id: &str) -> bool {
    match &claims.permissions {
        Permissions::V1(v1) => v1.read.iter().any(|p| glob_match(p, crdt_id)),
        Permissions::V2(v2) => v2.r.iter().any(|e| glob_match(&e.p, crdt_id)),
    }
}

/// Returns `true` if the token's permissions allow writing `crdt_id` with `op_mask`.
///
/// `op_mask` is a bitmask — use constants from [`op_masks`] or build with `|`.
pub fn can_write(claims: &TokenClaims, crdt_id: &str, op_mask: OpMask) -> bool {
    match &claims.permissions {
        Permissions::V1(v1) => v1.write.iter().any(|p| glob_match(p, crdt_id)),
        Permissions::V2(v2) => v2.w.iter().any(|e| {
            glob_match(&e.p, crdt_id) && (e.o & op_mask) == op_mask
        }),
    }
}

/// Returns the remaining time-to-live of the token in milliseconds, or `0` if expired.
pub fn token_ttl_ms(claims: &TokenClaims) -> u64 {
    let now = meridian_core::crdt::clock::now_ms();
    claims.expires_at.saturating_sub(now)
}

/// Re-export op mask constants for `can_write`.
pub mod masks {
    pub use meridian_core::auth::op_masks::*;
}

#[cfg(test)]
mod tests {
    use super::*;
    use meridian_core::auth::{TokenSigner, claims::Permissions};

    fn make_token(perms: Permissions) -> (TokenClaims, String) {
        let signer = TokenSigner::generate();
        let claims = TokenClaims::new("room-1", 42, 60_000, perms);
        let token = signer.sign(&claims).unwrap();
        (claims, token)
    }

    #[test]
    fn parse_roundtrip() {
        let (original, token) = make_token(Permissions::read_write());
        let parsed = parse_token(&token).unwrap();
        assert_eq!(parsed.namespace, original.namespace);
        assert_eq!(parsed.client_id, original.client_id);
    }

    #[test]
    fn can_read_v1() {
        let (claims, _) = make_token(Permissions::read_write());
        assert!(can_read(&claims, "any-crdt"));
    }

    #[test]
    fn ttl_positive_for_fresh_token() {
        let (claims, _) = make_token(Permissions::read_write());
        assert!(token_ttl_ms(&claims) > 0);
    }
}
