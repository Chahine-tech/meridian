use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

use crate::{
    crdt::{clock::now_ms, registry::CrdtValue},
    storage::{CrdtStore, Result},
};

/// How often the compactor runs.
const COMPACT_INTERVAL: Duration = Duration::from_secs(30 * 60); // 30 minutes

/// Tombstones older than this are eligible for removal.
/// Must be larger than your maximum expected client offline duration.
const DEFAULT_GRACE_MS: u64 = 24 * 60 * 60 * 1_000; // 24 hours

/// Background task: compact RGA and Tree CRDT tombstones every 30 minutes.
///
/// Tombstones are only removed when they are:
/// 1. Structurally safe — no live node references them as a parent/origin.
/// 2. Time-safe — `deleted_at_ms > 0` and older than `CRDT_GC_GRACE_MS`
///    (default 24 h, overridable via env var).
///
/// Tombstones with `deleted_at_ms == 0` (written before this version) are
/// never removed — their age is unknown.
#[instrument(skip(store, cancel))]
pub async fn run_crdt_compactor<S: CrdtStore>(store: Arc<S>, cancel: CancellationToken) {
    let grace_ms = std::env::var("MERIDIAN_CRDT_GC_GRACE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_GRACE_MS);

    info!(grace_ms, "crdt_compactor started");

    let mut interval = time::interval(COMPACT_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("crdt_compactor shutting down");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = compact_tick(&*store, grace_ms).await {
                    error!(error = %e, "crdt_compactor tick failed");
                }
            }
        }
    }
}

async fn compact_tick<S: CrdtStore>(store: &S, grace_ms: u64) -> Result<()> {
    let threshold = now_ms().saturating_sub(grace_ms);

    let all = store.scan_prefix("").await?;
    let mut total_tombstones = 0usize;
    let mut total_move_records = 0usize;
    let mut compacted = 0usize;

    for (key, mut crdt) in all {
        let is_compactable = matches!(crdt, CrdtValue::RGA(_) | CrdtValue::Tree(_));
        if !is_compactable {
            continue;
        }

        let stats = crdt.compact_before(threshold);
        if stats.tombstones_removed == 0 && stats.move_records_removed == 0 {
            continue;
        }

        let Some((ns, crdt_id)) = key.split_once('/') else {
            continue;
        };

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

    if compacted > 0 {
        info!(
            crdts = compacted,
            tombstones = total_tombstones,
            move_records = total_move_records,
            "crdt_compactor: cycle complete"
        );
    } else {
        debug!("crdt_compactor: nothing to compact");
    }

    Ok(())
}
