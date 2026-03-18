use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, warn};

use crate::{
    api::ws::SubscriptionManager,
    crdt::{clock::now_ms, registry::CrdtValue},
    storage::{CrdtStore, Result},
};

const GC_INTERVAL: Duration = Duration::from_secs(5);

/// Background task: runs two GC sweeps every 5 seconds.
///
/// 1. **Presence GC** — for each Presence CRDT: load, call `.gc(now_ms())`,
///    persist if changed, publish tombstone delta to subscribers.
///
/// 2. **TTL GC** — delete any CRDT entry whose `expires_at_ms` has passed
///    via `Store::delete_expired`.  On PostgreSQL this is a single indexed
///    DELETE; for in-memory / sled backends it is a no-op.
///
/// The GC task holds no in-memory locks across store calls — each cycle
/// is a fresh read-modify-write.
#[instrument(skip(store, subscriptions, cancel))]
pub async fn run_presence_gc<S: CrdtStore>(
    store: Arc<S>,
    subscriptions: Arc<SubscriptionManager>,
    cancel: CancellationToken,
) {
    let mut interval = time::interval(GC_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("presence_gc shutting down");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = gc_tick(&*store, &subscriptions).await {
                    error!(error = %e, "presence_gc tick failed");
                }
            }
        }
    }
}

async fn gc_tick<S: CrdtStore>(
    store: &S,
    subscriptions: &SubscriptionManager,
) -> Result<()> {
    let now = now_ms();

    // --- Pass 1: Presence CRDT entry expiry (removes individual entries) ----
    let all = store.scan_prefix("").await?;
    for (key, mut crdt) in all {
        let CrdtValue::Presence(ref mut presence) = crdt else {
            continue;
        };

        let gc_delta = presence.gc(now);
        if gc_delta.is_none() {
            continue;
        }

        let Some((ns, crdt_id)) = key.split_once('/') else {
            continue;
        };

        if let Err(e) = store.put(ns, crdt_id, &crdt).await {
            error!(key, error = %e, "gc: store.put failed");
            continue;
        }

        if let Ok(delta_bytes) = rmp_serde::encode::to_vec_named(&gc_delta) {
            subscriptions.publish(ns, Arc::new(crate::api::ws::ServerMsg::Delta {
                crdt_id: crdt_id.to_owned(),
                delta_bytes: delta_bytes.into(),
            }));
        }

        debug!(key, "gc: pruned expired presence entries");
    }

    // --- Pass 2: TTL-based CRDT expiry (deletes whole CRDT rows) -----------
    match store.delete_expired(now).await {
        Ok(deleted) => {
            for (ns, crdt_id) in deleted {
                debug!(ns, crdt_id, "gc: deleted TTL-expired CRDT");
                // Subscribers don't need a delta for a fully deleted CRDT —
                // they will simply stop receiving future updates.  Logging is
                // sufficient for now.
            }
        }
        Err(e) => {
            warn!(error = %e, "gc: delete_expired failed");
        }
    }

    Ok(())
}
