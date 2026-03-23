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

#[cfg(feature = "transport-http")]
use crate::transport::http_push::HttpPushTransport;

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

/// Periodically pulls WAL entries from each peer and applies any ops this
/// node missed while it was down or partitioned.
///
/// This is the complement to push anti-entropy: instead of broadcasting our
/// WAL to peers, we pull their WAL entries and apply them locally.
///
/// On startup, `peer_seq` is seeded from `wal.checkpoint_seq()` — everything
/// before the checkpoint was already compacted locally, so there is no need
/// to re-pull it. This avoids replaying the full peer WAL history after a
/// node restart while remaining correct (ops after the checkpoint that were
/// missed will still be pulled).
///
/// Only available with the `transport-http` feature — Redis Pub/Sub is
/// stateless and doesn't support WAL pull.
#[cfg(feature = "transport-http")]
pub async fn run_pull_anti_entropy<W, A, B>(
    wal: Arc<W>,
    transport: Arc<HttpPushTransport>,
    applier: Arc<A>,
    broadcast: Arc<B>,
    node_id: NodeId,
    config: Arc<ClusterConfig>,
    cancel: CancellationToken,
) where
    W: WalBackend,
    A: AntiEntropyApplier,
    B: LocalBroadcast,
{
    use std::collections::HashMap;

    let interval = config.anti_entropy_interval;

    // Seed from the local WAL checkpoint — ops before this seq were already
    // compacted locally, so we know we applied them. This prevents a full
    // WAL replay from each peer on every node restart.
    let checkpoint = wal.checkpoint_seq();
    let mut peer_seq: HashMap<String, u64> = HashMap::new();
    for peer in transport.peers() {
        peer_seq.insert(peer.to_string(), checkpoint);
    }

    info!(
        node_id = %node_id,
        peers = transport.peers().len(),
        "pull anti-entropy task started"
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!(node_id = %node_id, "pull anti-entropy task shutting down");
                break;
            }
            _ = tokio::time::sleep(interval) => {}
        }

        for peer in transport.peers() {
            let from_seq = peer_seq.get(peer.as_str()).copied().unwrap_or(0);

            let entries = match transport.pull_wal_from(peer, from_seq).await {
                Ok(e) => e,
                Err(e) => {
                    warn!(peer = %peer, error = %e, "pull anti-entropy: failed to pull WAL from peer");
                    continue;
                }
            };

            if entries.is_empty() {
                continue;
            }

            let last_seq = entries.last().map(|e| e.seq).unwrap_or(from_seq);
            let count = entries.len();

            for entry in entries {
                match applier.apply_wal_op(&entry.namespace, &entry.crdt_id, entry.op_bytes).await {
                    Ok(Some(delta_bytes)) => {
                        broadcast.publish_delta(
                            &entry.namespace,
                            &entry.crdt_id,
                            bytes::Bytes::from(delta_bytes),
                        );
                    }
                    Ok(None) => {
                        debug!(
                            ns = %entry.namespace,
                            crdt_id = %entry.crdt_id,
                            seq = entry.seq,
                            "pull anti-entropy: op already applied"
                        );
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            ns = %entry.namespace,
                            crdt_id = %entry.crdt_id,
                            "pull anti-entropy: apply_wal_op failed"
                        );
                    }
                }
            }

            peer_seq.insert(peer.to_string(), last_seq);
            info!(
                node_id = %node_id,
                peer = %peer,
                pulled = count,
                up_to_seq = last_seq,
                "pull anti-entropy: tick complete"
            );
        }
    }
}

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
