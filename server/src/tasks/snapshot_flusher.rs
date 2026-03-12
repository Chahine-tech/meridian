use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument};

use crate::storage::SledStore;

const FLUSH_INTERVAL: Duration = Duration::from_secs(60);

/// Background task: flushes sled's write buffer to disk every 60 seconds.
///
/// sled batches writes in memory; this task ensures they reach disk even
/// when write volume is low. On shutdown the `CancellationToken` fires a
/// final flush before exit.
#[instrument(skip(store, cancel))]
pub async fn run_snapshot_flusher(store: Arc<SledStore>, cancel: CancellationToken) {
    let mut interval = time::interval(FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("snapshot_flusher shutting down — final flush");
                if let Err(e) = store.flush().await {
                    error!(error = %e, "final flush failed");
                }
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = store.flush().await {
                    error!(error = %e, "periodic flush failed");
                }
                debug!("snapshot_flusher: flushed");
            }
        }
    }
}
