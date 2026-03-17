use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use meridian_storage::WalBackend;

use crate::{
    cluster_handle::LocalBroadcast,
    config::ClusterConfig,
    transport::{ClusterTransport, DeltaEnvelope},
    node_id::NodeId,
};

// ---------------------------------------------------------------------------
// AntiEntropyApplier — server-side callback to re-apply a WAL op
// ---------------------------------------------------------------------------

/// Callback trait implemented by the server to re-apply a raw WAL op.
///
/// `meridian-cluster` cannot depend on the CRDT registry (that would create
/// a circular dependency). Instead, the server provides an implementation
/// that calls `apply_op`, persists the result, and returns the delta bytes.
///
/// Returns `Ok(Some(delta_bytes))` if the op produced a new delta,
/// `Ok(None)` if it was a no-op (already applied), `Err` on failure.
pub trait AntiEntropyApplier: Send + Sync + 'static {
    fn apply_wal_op(
        &self,
        namespace: &str,
        crdt_id: &str,
        op_bytes: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, String>> + Send;
}

// ---------------------------------------------------------------------------
// run_anti_entropy — background task
// ---------------------------------------------------------------------------

/// Periodically replays recent WAL entries and re-publishes any deltas that
/// peers may have missed due to Redis disconnections or node restarts.
///
/// Strategy:
/// 1. Record `start_seq = wal.last_seq()` at startup.
/// 2. Every `interval`:
///    a. Replay WAL from `last_replayed + 1` to current `last_seq`.
///    b. For each entry: re-apply op via `applier` (no-op if already applied).
///    c. Broadcast resulting delta via transport + publish locally.
///    d. Advance `last_replayed`.
///
/// This ensures that after a Redis outage, the next anti-entropy tick
/// will re-broadcast all deltas that were written during the outage.
pub async fn run_anti_entropy<W, A, B>(
    wal: Arc<W>,
    applier: Arc<A>,
    broadcast: Arc<B>,
    transport: Arc<dyn ClusterTransport>,
    node_id: NodeId,
    config: Arc<ClusterConfig>,
    cancel: CancellationToken,
) where
    W: WalBackend,
    A: AntiEntropyApplier,
    B: LocalBroadcast,
{
    // Start from the current WAL head — we don't replay history from before
    // this node started; peers that were running already have those entries.
    let mut last_replayed = wal.last_seq();
    let interval = config.anti_entropy_interval;

    info!(
        node_id = %node_id,
        interval_secs = interval.as_secs(),
        start_seq = last_replayed,
        "anti-entropy task started"
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!(node_id = %node_id, "anti-entropy task shutting down");
                break;
            }
            _ = tokio::time::sleep(interval) => {}
        }

        let current_last = wal.last_seq();
        if current_last <= last_replayed {
            debug!(node_id = %node_id, seq = last_replayed, "anti-entropy: nothing new");
            continue;
        }

        let from = last_replayed + 1;
        debug!(node_id = %node_id, from, to = current_last, "anti-entropy: replaying WAL entries");

        match wal.replay_from(from).await {
            Err(e) => {
                warn!(error = %e, "anti-entropy: WAL replay failed");
                continue;
            }
            Ok(entries) => {
                let count = entries.len();
                for entry in entries {
                    match applier.apply_wal_op(&entry.namespace, &entry.crdt_id, entry.op_bytes).await {
                        Ok(Some(delta_bytes)) => {
                            // Publish locally (to WS clients on this node).
                            broadcast.publish_delta(
                                &entry.namespace,
                                &entry.crdt_id,
                                bytes::Bytes::from(delta_bytes.clone()),
                            );

                            // Broadcast to peers via transport (best-effort).
                            let envelope = DeltaEnvelope::new(
                                node_id,
                                &entry.namespace,
                                &entry.crdt_id,
                                bytes::Bytes::from(delta_bytes),
                            );
                            if let Err(e) = transport.broadcast_delta(envelope).await {
                                warn!(
                                    error = %e,
                                    ns = %entry.namespace,
                                    crdt_id = %entry.crdt_id,
                                    "anti-entropy: broadcast failed (will retry next tick)"
                                );
                            }
                        }
                        Ok(None) => {
                            // Already applied — no delta produced (idempotent).
                            debug!(
                                ns = %entry.namespace,
                                crdt_id = %entry.crdt_id,
                                seq = entry.seq,
                                "anti-entropy: op was no-op (already applied)"
                            );
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                ns = %entry.namespace,
                                crdt_id = %entry.crdt_id,
                                seq = entry.seq,
                                "anti-entropy: apply_wal_op failed"
                            );
                        }
                    }
                }

                last_replayed = current_last;
                if count > 0 {
                    info!(
                        node_id = %node_id,
                        replayed = count,
                        up_to_seq = current_last,
                        "anti-entropy: tick complete"
                    );
                }
            }
        }
    }
}
