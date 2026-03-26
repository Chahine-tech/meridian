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

// ---------------------------------------------------------------------------
// Op-level permission masks (V2)
// ---------------------------------------------------------------------------

/// Bitmask of allowed operations on a specific CRDT key.
///
/// `0` or `0xFFFF` (or absent) means all operations are allowed.
pub type OpMask = u16;

/// Op-mask constants — combined with `|` to allow multiple ops.
pub mod op_masks {
    use super::OpMask;

    // GCounter
    pub const GC_INCREMENT: OpMask = 0b0000_0001;

    // PNCounter
    pub const PN_INCREMENT: OpMask = 0b0000_0001;
    pub const PN_DECREMENT: OpMask = 0b0000_0010;

    // ORSet
    pub const OR_ADD:    OpMask = 0b0000_0001;
    pub const OR_REMOVE: OpMask = 0b0000_0010;

    // LWW Register
    pub const LWW_SET: OpMask = 0b0000_0001;

    // Presence
    pub const PRESENCE_UPDATE: OpMask = 0b0000_0001;

    // RGA
    pub const RGA_INSERT: OpMask = 0b0000_0001;
    pub const RGA_DELETE: OpMask = 0b0000_0010;

    // Tree
    pub const TREE_ADD:    OpMask = 0b0000_0001;
    pub const TREE_MOVE:   OpMask = 0b0000_0010;
    pub const TREE_UPDATE: OpMask = 0b0000_0100;
    pub const TREE_DELETE: OpMask = 0b0000_1000;

    // CRDTMap — delegates to inner CRDT ops; at the map level, any write is one bit
    pub const MAP_WRITE: OpMask = 0b0000_0001;

    /// Allow all operations (sentinel — `0` means "no restriction").
    /// Also accepted: `0xFFFF` for compatibility.
    pub const ALL: OpMask = 0;
}

// ---------------------------------------------------------------------------
// V2 permission rule
// ---------------------------------------------------------------------------

/// A single permission rule in a V2 token.
///
/// Short field names keep the serialised token compact (saves ~30 bytes/rule).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PermEntry {
    /// Glob pattern matched against `crdt_id` (e.g. `"gc:*"`, `"*"`).
    pub p: String,
    /// Operation mask — which ops are allowed on matching keys.
    /// Absent or `0xFFFF` means all ops.
    #[serde(default, skip_serializing_if = "is_all_mask")]
    pub o: OpMask,
    /// Expiry for this rule — Unix timestamp in milliseconds.
    /// Absent = no per-rule expiry (token-level `expires_at` still applies).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e: Option<u64>,
}

fn is_all_mask(mask: &OpMask) -> bool {
    *mask == 0 || *mask == 0xFFFF
}

impl PermEntry {
    pub fn new(pattern: impl Into<String>) -> Self {
        Self { p: pattern.into(), o: op_masks::ALL, e: None }
    }

    pub fn with_mask(mut self, mask: OpMask) -> Self {
        self.o = mask;
        self
    }

    pub fn with_expiry(mut self, expires_at_ms: u64) -> Self {
        self.e = Some(expires_at_ms);
        self
    }

    /// Returns true if this rule matches `crdt_id`, has not expired, and
    /// the operation is permitted by the op mask.
    pub fn matches(&self, crdt_id: &str, op_mask: OpMask, now_ms: u64) -> bool {
        // TTL gate
        if let Some(exp) = self.e {
            if now_ms >= exp {
                return false;
            }
        }
        // Pattern gate
        if !glob_match(&self.p, crdt_id) {
            return false;
        }
        // Op-mask gate (0 or 0xFFFF = allow everything)
        if is_all_mask(&self.o) {
            return true;
        }
        op_mask & self.o != 0
    }
}

// ---------------------------------------------------------------------------
// V1 permissions (backward compat)
// ---------------------------------------------------------------------------

/// V1 key-scoped permissions (glob lists).
///
/// Each list contains glob patterns matched against the CRDT key (crdt_id).
/// `["*"]` grants access to all keys (equivalent to the old `read: true`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PermissionsV1 {
    pub read: Vec<String>,
    pub write: Vec<String>,
    pub admin: bool,
}

impl PermissionsV1 {
    pub fn can_read_key(&self, crdt_id: &str) -> bool {
        self.read.iter().any(|p| glob_match(p, crdt_id))
    }

    pub fn can_write_key(&self, crdt_id: &str) -> bool {
        self.write.iter().any(|p| glob_match(p, crdt_id))
    }
}

// ---------------------------------------------------------------------------
// V2 permissions
// ---------------------------------------------------------------------------

/// V2 fine-grained permissions with per-rule op masks and TTLs.
///
/// Wire discriminant: `"v": 2` (absent in V1 tokens).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PermissionsV2 {
    /// Schema version — always 2.
    pub v: u8,
    /// Read rules — first match wins.
    pub r: Vec<PermEntry>,
    /// Write rules — first match wins.
    pub w: Vec<PermEntry>,
    pub admin: bool,
    /// Optional per-token rate limit override (requests per second).
    /// Absent = server default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rl: Option<u32>,
}

impl PermissionsV2 {
    pub fn can_read_key(&self, crdt_id: &str) -> bool {
        let now = now_ms();
        self.r.iter().any(|rule| rule.matches(crdt_id, op_masks::ALL, now))
    }

    /// Check write access with op-level granularity.
    pub fn can_write_key_op(&self, crdt_id: &str, op_mask: OpMask) -> bool {
        let now = now_ms();
        self.w.iter().any(|rule| rule.matches(crdt_id, op_mask, now))
    }

    pub fn can_write_key(&self, crdt_id: &str) -> bool {
        let now = now_ms();
        self.w.iter().any(|rule| rule.matches(crdt_id, op_masks::ALL, now))
    }

    pub fn rate_limit(&self) -> Option<u32> {
        self.rl
    }
}

// ---------------------------------------------------------------------------
// Untagged enum — backward-compatible union
// ---------------------------------------------------------------------------

/// Token permissions — either V1 (legacy) or V2 (fine-grained).
///
/// Serde `untagged` tries V2 first (it has a `v` field); falls back to V1.
/// Old V1 tokens continue to work without migration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Permissions {
    V2(PermissionsV2),
    V1(PermissionsV1),
}

impl Permissions {
    // --- Convenience constructors (V1) ---

    pub fn read_only() -> Self {
        Self::V1(PermissionsV1 { read: vec!["*".into()], write: vec![], admin: false })
    }

    pub fn read_write() -> Self {
        Self::V1(PermissionsV1 { read: vec!["*".into()], write: vec!["*".into()], admin: false })
    }

    pub fn admin() -> Self {
        Self::V1(PermissionsV1 { read: vec!["*".into()], write: vec!["*".into()], admin: true })
    }

    // --- Capability checks ---

    pub fn can_read_key(&self, crdt_id: &str) -> bool {
        match self {
            Self::V1(p) => p.can_read_key(crdt_id),
            Self::V2(p) => p.can_read_key(crdt_id),
        }
    }

    pub fn can_write_key(&self, crdt_id: &str) -> bool {
        match self {
            Self::V1(p) => p.can_write_key(crdt_id),
            Self::V2(p) => p.can_write_key(crdt_id),
        }
    }

    /// Check write access with op-level granularity (V2 only; V1 falls back to key-level check).
    pub fn can_write_key_op(&self, crdt_id: &str, op_mask: OpMask) -> bool {
        match self {
            Self::V1(p) => p.can_write_key(crdt_id),
            Self::V2(p) => p.can_write_key_op(crdt_id, op_mask),
        }
    }

    pub fn is_admin(&self) -> bool {
        match self {
            Self::V1(p) => p.admin,
            Self::V2(p) => p.admin,
        }
    }

    /// Returns the per-token rate limit override (requests/sec), if any.
    pub fn rate_limit(&self) -> Option<u32> {
        match self {
            Self::V1(_) => None,
            Self::V2(p) => p.rate_limit(),
        }
    }
}

impl std::fmt::Display for Permissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::V1(p) => {
                let mut parts = Vec::new();
                if !p.read.is_empty()  { parts.push(format!("read:{}", p.read.join(","))); }
                if !p.write.is_empty() { parts.push(format!("write:{}", p.write.join(","))); }
                if p.admin { parts.push("admin".into()); }
                f.write_str(&parts.join("+"))
            }
            Self::V2(p) => {
                write!(f, "v2+r:{}+w:{}", p.r.len(), p.w.len())
            }
        }
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
        match &self.permissions {
            Permissions::V1(p) => !p.read.is_empty(),
            Permissions::V2(p) => !p.r.is_empty(),
        }
    }

    /// Returns true if the token has write access to any key (namespace-level check).
    pub fn can_write(&self) -> bool {
        match &self.permissions {
            Permissions::V1(p) => !p.write.is_empty(),
            Permissions::V2(p) => !p.w.is_empty(),
        }
    }

    /// Returns true if the token may read the specific CRDT key.
    pub fn can_read_key(&self, crdt_id: &str) -> bool {
        self.permissions.can_read_key(crdt_id)
    }

    /// Returns true if the token may write the specific CRDT key.
    pub fn can_write_key(&self, crdt_id: &str) -> bool {
        self.permissions.can_write_key(crdt_id)
    }

    /// Returns true if the token may apply the specific op to `crdt_id`.
    pub fn can_write_key_op(&self, crdt_id: &str, op_mask: OpMask) -> bool {
        self.permissions.can_write_key_op(crdt_id, op_mask)
    }

    pub fn is_admin(&self) -> bool {
        self.permissions.is_admin()
    }

    /// Returns a per-token rate-limit override if present (V2 only).
    pub fn rate_limit(&self) -> Option<u32> {
        self.permissions.rate_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::{op_masks, *};

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
    fn permissions_v1_scoped() {
        let p = Permissions::V1(PermissionsV1 {
            read: vec!["gc:*".into(), "lw:title".into()],
            write: vec!["or:cart-42".into()],
            admin: false,
        });
        assert!(p.can_read_key("gc:views"));
        assert!(p.can_read_key("lw:title"));
        assert!(!p.can_read_key("or:tags"));
        assert!(p.can_write_key("or:cart-42"));
        assert!(!p.can_write_key("or:tags"));
        assert!(!p.can_write_key("gc:views"));
    }

    #[test]
    fn permissions_v1_wildcard_full_access() {
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
    fn claims_key_scoped_v1() {
        let perms = Permissions::V1(PermissionsV1 {
            read: vec!["gc:*".into()],
            write: vec!["gc:views".into()],
            admin: false,
        });
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
    fn msgpack_roundtrip_v1() {
        let claims = TokenClaims::new("my-room", 42, 3_600_000, Permissions::admin());
        let bytes = rmp_serde::encode::to_vec_named(&claims).unwrap();
        let decoded: TokenClaims = rmp_serde::decode::from_slice(&bytes).unwrap();
        assert_eq!(claims, decoded);
    }

    #[test]
    fn msgpack_roundtrip_v2() {
        let perms = Permissions::V2(PermissionsV2 {
            v: 2,
            r: vec![PermEntry::new("*")],
            w: vec![
                PermEntry::new("gc:*").with_mask(op_masks::GC_INCREMENT),
                PermEntry::new("or:cart").with_mask(op_masks::OR_ADD | op_masks::OR_REMOVE),
            ],
            admin: false,
            rl: Some(50),
        });
        let claims = TokenClaims::new("room", 7, 3_600_000, perms);
        let bytes = rmp_serde::encode::to_vec_named(&claims).unwrap();
        let decoded: TokenClaims = rmp_serde::decode::from_slice(&bytes).unwrap();
        assert_eq!(claims, decoded);
    }

    #[test]
    fn v2_op_mask_enforced() {
        let perms = Permissions::V2(PermissionsV2 {
            v: 2,
            r: vec![PermEntry::new("*")],
            w: vec![PermEntry::new("gc:*").with_mask(op_masks::GC_INCREMENT)],
            admin: false,
            rl: None,
        });
        // Increment allowed
        assert!(perms.can_write_key_op("gc:views", op_masks::GC_INCREMENT));
        // No PN_DECREMENT bit on gc:* rule — denied
        assert!(!perms.can_write_key_op("gc:views", op_masks::PN_DECREMENT));
    }

    #[test]
    fn v2_per_rule_expiry() {
        let expired_ms = 1u64; // definitely in the past
        let perms = Permissions::V2(PermissionsV2 {
            v: 2,
            r: vec![PermEntry::new("*")],
            w: vec![PermEntry::new("gc:*").with_expiry(expired_ms)],
            admin: false,
            rl: None,
        });
        // gc:* write rule is expired → denied
        assert!(!perms.can_write_key("gc:views"));
    }

    #[test]
    fn v2_rate_limit() {
        let perms = Permissions::V2(PermissionsV2 {
            v: 2,
            r: vec![PermEntry::new("*")],
            w: vec![PermEntry::new("*")],
            admin: false,
            rl: Some(200),
        });
        let claims = TokenClaims::new("ns", 1, 60_000, perms);
        assert_eq!(claims.rate_limit(), Some(200));
    }

    #[test]
    fn v1_token_still_deserializes() {
        // Simulate an old V1 token payload encoded with msgpack
        let old = TokenClaims {
            namespace: "ns".into(),
            client_id: 1,
            expires_at: u64::MAX,
            permissions: Permissions::V1(PermissionsV1 {
                read: vec!["*".into()],
                write: vec!["*".into()],
                admin: false,
            }),
        };
        let bytes = rmp_serde::encode::to_vec_named(&old).unwrap();
        let decoded: TokenClaims = rmp_serde::decode::from_slice(&bytes).unwrap();
        assert!(decoded.can_read_key("anything"));
        assert!(decoded.can_write_key("anything"));
    }
}
