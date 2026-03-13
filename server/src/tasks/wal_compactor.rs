use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

use crate::storage::SledStore;

/// Compact the WAL every `COMPACT_INTERVAL` or when it exceeds `COMPACT_THRESHOLD` entries.
///
/// ## Strategy
///
/// sled is already the "snapshot" — every `Store::put` writes the full CRDT state.
/// So compaction is straightforward:
/// 1. Flush sled to disk (ensures all CRDT states are durable).
/// 2. Truncate WAL entries up to the current last_seq.
///
/// After a restart, the server loads CRDT state directly from sled (already up-to-date)
/// and only needs to replay WAL entries written *after* the last compaction point.
/// With periodic compaction the WAL stays bounded regardless of write volume.
///
/// ## Safety
///
/// We flush sled *before* truncating the WAL. If the process crashes between the two
/// steps, the WAL entries are replayed harmlessly (CRDT merge is idempotent).
const COMPACT_INTERVAL: Duration = Duration::from_secs(60);

#[instrument(skip(store, cancel))]
pub async fn run_wal_compactor(store: Arc<SledStore>, cancel: CancellationToken) {
    let mut interval = time::interval(COMPACT_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("wal_compactor shutting down — final compaction");
                compact(&store).await;
                break;
            }
            _ = interval.tick() => {
                let pending = store.wal.last_seq().saturating_sub(store.wal.checkpoint_seq());
                if pending > 0 {
                    debug!(pending, "wal_compactor: running compaction");
                    compact(&store).await;
                } else {
                    debug!("wal_compactor: WAL already compact");
                }
            }
        }
    }
}

async fn compact(store: &SledStore) {
    // Step 1: flush sled — all CRDT states are now durable on disk.
    if let Err(e) = store.flush().await {
        error!(error = %e, "wal_compactor: flush failed — skipping truncation");
        return;
    }

    // Step 2: snapshot the current WAL tail (everything up to this point is
    // covered by the sled state we just flushed).
    let up_to = store.wal.last_seq();

    // Step 3: truncate WAL entries up to (but not including) up_to.
    // We keep the last entry so next_seq arithmetic stays correct.
    if up_to == 0 {
        return;
    }

    match store.wal.truncate_before(up_to) {
        Ok(()) => {
            store.wal.set_checkpoint_seq(up_to);
            info!(up_to, "wal_compactor: WAL compacted");
        }
        Err(e) => error!(error = %e, "wal_compactor: truncate failed"),
    }
}
