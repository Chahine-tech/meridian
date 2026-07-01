use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

use crate::{
    crdt::{ClientRegistry, clock::now_ms, registry::CrdtValue},
    storage::{CrdtStore, Result},
};

/// How often the compactor runs.
const COMPACT_INTERVAL: Duration = Duration::from_secs(30 * 60); // 30 minutes

/// Tombstones older than this are eligible for removal (offline-client grace window).
/// Must be larger than your maximum expected client offline duration.
const DEFAULT_GRACE_MS: u64 = 24 * 60 * 60 * 1_000; // 24 hours

/// Background task: compact RGA and Tree CRDT tombstones every 30 minutes.
///
/// ## GC strategy
///
/// **Without a `ClientRegistry`** (no connected clients): falls back to the
/// classic time-based `compact_before(now - grace_ms)` — identical to the old
/// behaviour.
///
/// **With a `ClientRegistry`** (at least one connected, synced client): uses the
/// DottedDB-inspired `compact_safe(floor_vc, grace_ms)` strategy:
///
/// - `floor_vc` is the component-wise minimum VectorClock over all registered
///   clients. It encodes "what every online client has already seen".
/// - A tombstone is only removed when `floor_vc` shows that every connected
///   client has already observed the corresponding insert, AND the deletion is
///   old enough (`deleted_at_ms + grace_ms <= now`) to cover clients that were
///   briefly offline.
///
/// This eliminates a whole class of "phantom resurrection" bugs that the
/// time-only approach could produce when a client returns from a long offline
/// period with stale data.
///
/// ## Safety
///
/// Tombstones with `deleted_at_ms == 0` are never GC'd — their age is unknown.
#[instrument(skip(store, registry, cancel))]
pub async fn run_crdt_compactor<S: CrdtStore>(
    store: Arc<S>,
    registry: Arc<ClientRegistry>,
    cancel: CancellationToken,
) {
    let grace_ms = std::env::var("MERIDIAN_CRDT_GC_GRACE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_GRACE_MS);

    info!(grace_ms, "crdt_compactor started (DottedDB-precise GC)");

    let mut interval = time::interval(COMPACT_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("crdt_compactor shutting down");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = compact_tick(&*store, &registry, grace_ms).await {
                    error!(error = %e, "crdt_compactor tick failed");
                }
            }
        }
    }
}

async fn compact_tick<S: CrdtStore>(
    store: &S,
    registry: &ClientRegistry,
    grace_ms: u64,
) -> Result<()> {
    let all = store.scan_prefix("").await?;
    let mut total_tombstones = 0usize;
    let mut total_move_records = 0usize;
    let mut compacted = 0usize;

    for (key, mut crdt) in all {
        if !matches!(crdt, CrdtValue::RGA(_) | CrdtValue::Tree(_)) {
            continue;
        }

        let Some((ns, crdt_id)) = key.split_once('/') else {
            continue;
        };

        let stats = match registry.floor_vc(ns) {
            Some(floor_vc) => {
                // DottedDB path: only GC tombstones all connected clients have seen.
                crdt.compact_safe(&floor_vc, grace_ms)
            }
            None => {
                // No registered clients for this namespace — classic time-based GC.
                let threshold = now_ms().saturating_sub(grace_ms);
                crdt.compact_before(threshold)
            }
        };

        if stats.tombstones_removed == 0 && stats.move_records_removed == 0 {
            continue;
        }

        if let Err(e) = store.put(ns, crdt_id, &crdt).await {
            error!(key, error = %e, "crdt_compactor: store.put failed");
            continue;
        }

        total_tombstones += stats.tombstones_removed;
        total_move_records += stats.move_records_removed;
        compacted += 1;

        debug!(
            key,
            tombstones = stats.tombstones_removed,
            move_records = stats.move_records_removed,
            "crdt_compactor: compacted"
        );
    }

    let online_clients = registry.total_clients();
    if compacted > 0 {
        info!(
            crdts = compacted,
            tombstones = total_tombstones,
            move_records = total_move_records,
            online_clients,
            "crdt_compactor: cycle complete"
        );
    } else {
        debug!(online_clients, "crdt_compactor: nothing to compact");
    }

    Ok(())
}
