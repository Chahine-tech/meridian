use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

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

        let hostname = hostname();
        let mut hasher = DefaultHasher::new();
        hostname.hash(&mut hasher);
        port.hash(&mut hasher);
        Self(hasher.finish())
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
