use serde::{Deserialize, Serialize};

use crate::crdt::clock::now_ms;

// ---------------------------------------------------------------------------
// Permissions
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Permissions {
    pub read: bool,
    pub write: bool,
    pub admin: bool,
}

impl Permissions {
    pub fn read_only() -> Self {
        Self { read: true, write: false, admin: false }
    }

    pub fn read_write() -> Self {
        Self { read: true, write: true, admin: false }
    }

    pub fn admin() -> Self {
        Self { read: true, write: true, admin: true }
    }
}

impl std::fmt::Display for Permissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if self.read  { parts.push("read"); }
        if self.write { parts.push("write"); }
        if self.admin { parts.push("admin"); }
        f.write_str(&parts.join("+"))
    }
}

// ---------------------------------------------------------------------------
// TokenClaims
// ---------------------------------------------------------------------------

/// Payload embedded in a signed token.
///
/// Serialized with msgpack (compact binary) before signing.
/// Wire format: `base64url(msgpack(claims)) + "." + base64url(ed25519_sig)`
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenClaims {
    /// Namespace this token grants access to.
    pub namespace: String,
    /// Opaque client identifier (used as HLC node_id + author in CRDTs).
    pub client_id: u64,
    /// Expiry: Unix timestamp in milliseconds.
    pub expires_at: u64,
    pub permissions: Permissions,
}

impl TokenClaims {
    pub fn new(
        namespace: impl Into<String>,
        client_id: u64,
        ttl_ms: u64,
        permissions: Permissions,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            client_id,
            expires_at: now_ms().saturating_add(ttl_ms),
            permissions,
        }
    }

    pub fn is_expired(&self) -> bool {
        now_ms() >= self.expires_at
    }

    pub fn can_read(&self) -> bool {
        self.permissions.read
    }

    pub fn can_write(&self) -> bool {
        self.permissions.write
    }

    pub fn is_admin(&self) -> bool {
        self.permissions.admin
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permissions_display() {
        assert_eq!(Permissions::read_only().to_string(), "read");
        assert_eq!(Permissions::read_write().to_string(), "read+write");
        assert_eq!(Permissions::admin().to_string(), "read+write+admin");
    }

    #[test]
    fn claims_not_expired_immediately() {
        let claims = TokenClaims::new("ns", 1, 60_000, Permissions::read_write());
        assert!(!claims.is_expired());
        assert!(claims.can_read());
        assert!(claims.can_write());
        assert!(!claims.is_admin());
    }

    #[test]
    fn claims_expired_in_the_past() {
        let claims = TokenClaims {
            namespace: "ns".into(),
            client_id: 1,
            expires_at: 1, // epoch + 1ms — definitely expired
            permissions: Permissions::read_write(),
        };
        assert!(claims.is_expired());
    }

    #[test]
    fn msgpack_roundtrip() {
        let claims = TokenClaims::new("my-room", 42, 3_600_000, Permissions::admin());
        let bytes = rmp_serde::encode::to_vec_named(&claims).unwrap();
        let decoded: TokenClaims = rmp_serde::decode::from_slice(&bytes).unwrap();
        assert_eq!(claims, decoded);
    }
}
