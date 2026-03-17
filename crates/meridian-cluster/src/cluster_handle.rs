use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};

use meridian_storage::WalBackend;

use crate::{
    anti_entropy::{run_anti_entropy, AntiEntropyApplier},
    config::ClusterConfig,
    node_id::NodeId,
    transport::{ClusterTransport, DeltaEnvelope},
};

// ---------------------------------------------------------------------------
// SubscriptionManager re-export shim
// ---------------------------------------------------------------------------
// ClusterHandle needs to call `subscriptions.publish()` but lives in its own
// crate with no dependency on `meridian-server`. We use a trait to break the
// cycle: the server implements `LocalBroadcast` for its `SubscriptionManager`.

/// Minimal interface for the node-local WebSocket broadcast hub.
pub trait LocalBroadcast: Send + Sync + 'static {
    fn publish_delta(&self, namespace: &str, crdt_id: &str, delta_bytes: Bytes);
}

// ---------------------------------------------------------------------------
// ClusterHandle
// ---------------------------------------------------------------------------

/// The single entry point for cluster operations on a running node.
///
/// - Call [`on_delta`] from HTTP/WS handlers after a successful op.
/// - Call [`spawn_receiver`] once at startup to forward remote deltas
///   to the local `SubscriptionManager`.
/// - Call [`spawn_anti_entropy`] once at startup for convergence recovery.
pub struct ClusterHandle {
    transport: Arc<dyn ClusterTransport>,
    node_id: NodeId,
    pub config: Arc<ClusterConfig>,
}

impl ClusterHandle {
    pub fn new(config: ClusterConfig, transport: Arc<dyn ClusterTransport>) -> Self {
        Self {
            node_id: config.node_id,
            transport,
            config: Arc::new(config),
        }
    }

    /// Called from HTTP/WS handlers after applying an op and persisting.
    ///
    /// Broadcasts the delta to all other nodes via the transport layer.
    /// Errors are logged and dropped — anti-entropy recovers missed deltas.
    #[instrument(skip(self, delta_bytes), fields(node_id = %self.node_id, ns, crdt_id))]
    pub async fn on_delta(&self, ns: &str, crdt_id: &str, delta_bytes: Bytes) {
        let envelope = DeltaEnvelope::new(self.node_id, ns, crdt_id, delta_bytes);

        if let Err(e) = self.transport.broadcast_delta(envelope).await {
            warn!(error = %e, ns, crdt_id, "failed to broadcast delta to cluster — anti-entropy will recover");
        } else {
            debug!(ns, crdt_id, "delta broadcast to cluster");
        }
    }

    /// Spawns a background task that reads from the transport's delta stream
    /// and forwards each received envelope to the local broadcast hub.
    ///
    /// Must be called once at server startup.
    pub fn spawn_receiver<B: LocalBroadcast>(
        &self,
        broadcast: Arc<B>,
        cancel: CancellationToken,
    ) {
        let mut stream = self.transport.subscribe_deltas();
        let node_id = self.node_id;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        debug!(node_id = %node_id, "cluster receiver shutting down");
                        break;
                    }
                    maybe = stream.next() => {
                        let Some(envelope) = maybe else { break };
                        debug!(
                            from = %envelope.origin_node_id,
                            ns = %envelope.namespace,
                            crdt_id = %envelope.crdt_id,
                            "received cluster delta"
                        );
                        broadcast.publish_delta(
                            &envelope.namespace,
                            &envelope.crdt_id,
                            Bytes::from(envelope.delta_bytes),
                        );
                    }
                }
            }
        });
    }

    /// Spawns the anti-entropy background task.
    ///
    /// Replays WAL entries periodically and re-broadcasts any deltas that
    /// peers may have missed due to transport failures or node restarts.
    ///
    /// Must be called once at server startup, after `spawn_receiver`.
    pub fn spawn_anti_entropy<W, A, B>(
        &self,
        wal: Arc<W>,
        applier: Arc<A>,
        broadcast: Arc<B>,
        cancel: CancellationToken,
    ) where
        W: WalBackend,
        A: AntiEntropyApplier,
        B: LocalBroadcast,
    {
        let transport = Arc::clone(&self.transport);
        let node_id = self.node_id;
        let config = Arc::clone(&self.config);

        tokio::spawn(run_anti_entropy(
            wal,
            applier,
            broadcast,
            transport,
            node_id,
            config,
            cancel,
        ));
    }
}
