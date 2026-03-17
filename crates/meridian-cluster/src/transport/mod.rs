use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use crate::{error::Result, node_id::NodeId};

// ---------------------------------------------------------------------------
// DeltaEnvelope — wire format for cross-node delta messages
// ---------------------------------------------------------------------------

/// A delta broadcast from one node to all peers.
///
/// The `delta_bytes` are the raw msgpack-encoded delta — the same bytes
/// that would be sent in a `ServerMsg::Delta` to a WebSocket client.
/// No re-encoding needed on the receiver side.
#[must_use]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaEnvelope {
    /// The node that produced this delta.
    pub origin_node_id: NodeId,
    /// Namespace the CRDT belongs to.
    pub namespace: String,
    /// CRDT identifier within the namespace.
    pub crdt_id: String,
    /// Raw msgpack delta bytes (opaque to the transport layer).
    #[serde(with = "serde_bytes")]
    pub delta_bytes: Vec<u8>,
}

impl DeltaEnvelope {
    pub fn new(origin: NodeId, namespace: &str, crdt_id: &str, delta_bytes: Bytes) -> Self {
        Self {
            origin_node_id: origin,
            namespace: namespace.to_owned(),
            crdt_id: crdt_id.to_owned(),
            delta_bytes: delta_bytes.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// ClusterTransport — abstraction over the inter-node messaging layer
// ---------------------------------------------------------------------------

/// Transport layer for broadcasting deltas between cluster nodes.
///
/// Implementations must be cheaply cloneable (use `Arc` internally).
#[async_trait]
pub trait ClusterTransport: Send + Sync + 'static {
    /// Broadcast a delta to all other nodes in the cluster.
    ///
    /// This is fire-and-forget from the caller's perspective.
    /// Transport errors are logged but not propagated — anti-entropy
    /// will recover any missed deltas within one gossip interval.
    async fn broadcast_delta(&self, envelope: DeltaEnvelope) -> Result<()>;

    /// Subscribe to incoming deltas from other nodes.
    ///
    /// Returns a stream that yields one `DeltaEnvelope` per received message.
    /// The stream runs until the transport is dropped or the cancel signal fires.
    fn subscribe_deltas(&self) -> BoxStream<'static, DeltaEnvelope>;
}

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------

#[cfg(feature = "transport-redis")]
pub mod redis_pubsub;

#[cfg(feature = "transport-http")]
pub mod http_push;
