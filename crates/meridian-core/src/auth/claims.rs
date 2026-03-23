use serde::{Deserialize, Serialize};

use crate::crdt::clock::now_ms;

/// Returns true if `pattern` matches `value`.
///
/// Supports a single `*` wildcard that matches any sequence of characters
/// (including empty). Multiple `*` work too (simple recursive impl).
///
/// Examples:
///   `"*"`         matches everything
///   `"gc:*"`      matches `"gc:views"`, `"gc:downloads"`
///   `"or:cart-*"` matches `"or:cart-42"` but not `"or:tags"`
///   `"gc:views"`  matches only `"gc:views"` exactly
pub fn glob_match(pattern: &str, value: &str) -> bool {
    // Fast paths
    if pattern == "*" {
        return true;
    }
    match pattern.find('*') {
        None => pattern == value,
        Some(star_pos) => {
            let prefix = &pattern[..star_pos];
            let suffix = &pattern[star_pos + 1..];
            // value must start with prefix
            if !value.starts_with(prefix) {
                return false;
            }
            let rest = &value[prefix.len()..];
            // suffix must match the tail; recurse to handle multiple `*`
            if suffix.is_empty() {
                return true;
            }
            // Try every possible position where suffix could start in rest.
            for i in 0..=rest.len() {
                if glob_match(suffix, &rest[i..]) {
                    return true;
                }
            }
            false
        }
    }
}

/// Key-scoped permissions.
///
/// Each list contains glob patterns matched against the CRDT key (crdt_id).
/// `["*"]` grants access to all keys (equivalent to the old `read: true`).
///
/// Examples:
/// ```json
/// { "read": ["*"], "write": ["or:cart-42", "gc:views"], "admin": false }
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Permissions {
    /// Glob patterns for keys this token may read. `["*"]` = all.
    pub read: Vec<String>,
    /// Glob patterns for keys this token may write. `["*"]` = all.
    pub write: Vec<String>,
    pub admin: bool,
}

impl Permissions {
    pub fn read_only() -> Self {
        Self { read: vec!["*".into()], write: vec![], admin: false }
    }

    pub fn read_write() -> Self {
        Self { read: vec!["*".into()], write: vec!["*".into()], admin: false }
    }

    pub fn admin() -> Self {
        Self { read: vec!["*".into()], write: vec!["*".into()], admin: true }
    }

    /// Returns true if the token may read `crdt_id`.
    pub fn can_read_key(&self, crdt_id: &str) -> bool {
        self.read.iter().any(|p| glob_match(p, crdt_id))
    }

    /// Returns true if the token may write `crdt_id`.
    pub fn can_write_key(&self, crdt_id: &str) -> bool {
        self.write.iter().any(|p| glob_match(p, crdt_id))
    }
}

impl std::fmt::Display for Permissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if !self.read.is_empty()  { parts.push(format!("read:{}", self.read.join(","))); }
        if !self.write.is_empty() { parts.push(format!("write:{}", self.write.join(","))); }
        if self.admin { parts.push("admin".into()); }
        f.write_str(&parts.join("+"))
    }
}

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

    /// Returns true if the token has read access to any key (namespace-level check).
    pub fn can_read(&self) -> bool {
        !self.permissions.read.is_empty()
    }

    /// Returns true if the token has write access to any key (namespace-level check).
    pub fn can_write(&self) -> bool {
        !self.permissions.write.is_empty()
    }

    /// Returns true if the token may read the specific CRDT key.
    pub fn can_read_key(&self, crdt_id: &str) -> bool {
        self.permissions.can_read_key(crdt_id)
    }

    /// Returns true if the token may write the specific CRDT key.
    pub fn can_write_key(&self, crdt_id: &str) -> bool {
        self.permissions.can_write_key(crdt_id)
    }

    pub fn is_admin(&self) -> bool {
        self.permissions.admin
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_match_wildcard() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
        assert!(glob_match("gc:*", "gc:views"));
        assert!(glob_match("gc:*", "gc:downloads"));
        assert!(!glob_match("gc:*", "or:tags"));
        assert!(glob_match("or:cart-*", "or:cart-42"));
        assert!(!glob_match("or:cart-*", "or:tags"));
        assert!(glob_match("gc:views", "gc:views"));
        assert!(!glob_match("gc:views", "gc:downloads"));
    }

    #[test]
    fn permissions_scoped() {
        let p = Permissions {
            read: vec!["gc:*".into(), "lw:title".into()],
            write: vec!["or:cart-42".into()],
            admin: false,
        };
        assert!(p.can_read_key("gc:views"));
        assert!(p.can_read_key("lw:title"));
        assert!(!p.can_read_key("or:tags"));
        assert!(p.can_write_key("or:cart-42"));
        assert!(!p.can_write_key("or:tags"));
        assert!(!p.can_write_key("gc:views"));
    }

    #[test]
    fn permissions_wildcard_full_access() {
        let p = Permissions::read_write();
        assert!(p.can_read_key("anything"));
        assert!(p.can_write_key("anything"));
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
    fn claims_key_scoped() {
        let perms = Permissions {
            read: vec!["gc:*".into()],
            write: vec!["gc:views".into()],
            admin: false,
        };
        let claims = TokenClaims::new("ns", 1, 60_000, perms);
        assert!(claims.can_read_key("gc:views"));
        assert!(!claims.can_read_key("or:tags"));
        assert!(claims.can_write_key("gc:views"));
        assert!(!claims.can_write_key("gc:downloads"));
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
