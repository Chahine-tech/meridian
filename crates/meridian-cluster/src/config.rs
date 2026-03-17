use std::time::Duration;

use url::Url;

use crate::node_id::NodeId;

/// Cluster configuration loaded from environment variables.
pub struct ClusterConfig {
    /// This node's unique ID.
    pub node_id: NodeId,

    /// Static peer addresses (used by HTTP transport and anti-entropy pull).
    /// Loaded from `MERIDIAN_PEERS` env var as comma-separated URLs.
    /// Example: `http://node-a:3000,http://node-b:3000`
    pub peers: Vec<Url>,

    /// Redis URL for Pub/Sub transport.
    /// Reuses `REDIS_URL` env var (same as storage backend).
    pub redis_url: Option<String>,

    /// How often anti-entropy runs. Default: 30s.
    pub anti_entropy_interval: Duration,
}

impl ClusterConfig {
    /// Returns `None` if clustering is not configured
    /// (no `MERIDIAN_PEERS` and no `REDIS_URL`).
    pub fn from_env(bind_port: u16) -> Option<Self> {
        let peers = parse_peers();
        let redis_url = std::env::var("REDIS_URL").ok();

        if peers.is_empty() && redis_url.is_none() {
            return None;
        }

        let node_id = NodeId::from_env_or_hostname(bind_port);
        let anti_entropy_interval = std::env::var("MERIDIAN_ANTI_ENTROPY_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(30));

        Some(Self {
            node_id,
            peers,
            redis_url,
            anti_entropy_interval,
        })
    }
}

fn parse_peers() -> Vec<Url> {
    std::env::var("MERIDIAN_PEERS")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .filter_map(|s| {
            let trimmed = s.trim();
            match Url::parse(trimmed) {
                Ok(url) => Some(url),
                Err(e) => {
                    tracing::warn!(peer = trimmed, error = %e, "ignoring invalid peer URL in MERIDIAN_PEERS");
                    None
                }
            }
        })
        .collect()
}
