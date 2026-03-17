/// Unique identifier for a cluster node.
///
/// Derived from `MERIDIAN_NODE_ID` env var (u64) or hashed from hostname+port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NodeId(pub u64);

impl NodeId {
    pub fn from_env_or_hostname(port: u16) -> Self {
        if let Ok(val) = std::env::var("MERIDIAN_NODE_ID")
            && let Ok(id) = val.parse::<u64>()
        {
            return Self(id);
        }

        Self(fnv1a_hash(hostname().as_bytes(), port))
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| {
            // gethostname fallback via /proc/sys/kernel/hostname on Linux
            std::fs::read_to_string("/proc/sys/kernel/hostname")
                .map(|s| s.trim().to_owned())
        })
        .unwrap_or_else(|_| "localhost".to_owned())
}

/// FNV-1a 64-bit hash — stable across Rust versions and platforms.
///
/// Unlike `DefaultHasher`, FNV-1a is specified by a fixed algorithm and
/// produces identical results regardless of Rust version or target architecture.
/// This ensures two nodes compiled separately produce the same NodeId for
/// the same hostname+port combination.
fn fnv1a_hash(input: &[u8], port: u16) -> u64 {
    const OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
    const PRIME: u64 = 1_099_511_628_211;

    let mut hash = OFFSET_BASIS;
    for byte in input {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    // Mix in the port
    for byte in port.to_le_bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic() {
        let a = fnv1a_hash(b"node-a", 3000);
        let b = fnv1a_hash(b"node-a", 3000);
        assert_eq!(a, b);
    }

    #[test]
    fn different_inputs_produce_different_hashes() {
        let a = fnv1a_hash(b"node-a", 3000);
        let b = fnv1a_hash(b"node-b", 3000);
        let c = fnv1a_hash(b"node-a", 3001);
        assert_ne!(a, b);
        assert_ne!(a, c);
    }
}
