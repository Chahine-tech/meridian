use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

use crate::storage::{CrdtStore, WalBackend};

/// Compact the WAL every `COMPACT_INTERVAL`.
///
/// ## Strategy
///
/// The store is already the "snapshot" — every `Store::put` writes the full CRDT state.
/// So compaction is straightforward:
/// 1. Flush the store to disk (ensures all CRDT states are durable).
/// 2. Truncate WAL entries up to the current last_seq.
///
/// After a restart, the server loads CRDT state directly from the store (already up-to-date)
/// and only needs to replay WAL entries written *after* the last compaction point.
/// With periodic compaction the WAL stays bounded regardless of write volume.
///
/// ## Safety
///
/// We flush the store *before* truncating the WAL. If the process crashes between the two
/// steps, the WAL entries are replayed harmlessly (CRDT merge is idempotent).
const COMPACT_INTERVAL: Duration = Duration::from_secs(60);

#[instrument(skip(store, wal, cancel))]
pub async fn run_wal_compactor<S: CrdtStore, W: WalBackend>(
    store: Arc<S>,
    wal: Arc<W>,
    cancel: CancellationToken,
) {
    let mut interval = time::interval(COMPACT_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("wal_compactor shutting down — final compaction");
                compact(&*store, &*wal).await;
                break;
            }
            _ = interval.tick() => {
                let pending = wal.last_seq().saturating_sub(wal.checkpoint_seq());
                if pending > 0 {
                    debug!(pending, "wal_compactor: running compaction");
                    compact(&*store, &*wal).await;
                } else {
                    debug!("wal_compactor: WAL already compact");
                }
            }
        }
    }
}

async fn compact<S: CrdtStore, W: WalBackend>(store: &S, wal: &W) {
    if let Err(e) = store.flush().await {
        error!(error = %e, "wal_compactor: flush failed — skipping truncation");
        return;
    }

    let up_to = wal.last_seq();
    if up_to == 0 {
        return;
    }

    match wal.truncate_before(up_to).await {
        Ok(()) => {
            if let Err(e) = wal.set_checkpoint_seq(up_to).await {
                error!(error = %e, "wal_compactor: set_checkpoint_seq failed");
            } else {
                info!(up_to, "wal_compactor: WAL compacted");
            }
        }
        Err(e) => error!(error = %e, "wal_compactor: truncate failed"),
    }
}
