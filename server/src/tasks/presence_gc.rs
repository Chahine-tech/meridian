use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument};

use crate::{
    api::ws::SubscriptionManager,
    crdt::{clock::now_ms, registry::CrdtValue},
    storage::{CrdtStore, Result},
};

const GC_INTERVAL: Duration = Duration::from_secs(5);

/// Background task: prunes expired Presence entries every 5 seconds.
///
/// For each Presence CRDT in all namespaces:
/// 1. Load from store
/// 2. Call `.gc(now_ms())` → returns a delta of removed entries
/// 3. If changed: persist updated state + publish delta to subscribers
///
/// The GC task holds no in-memory locks across store calls — each cycle
/// is a fresh read-modify-write. The CAS-like safety comes from sled's
/// single-writer guarantee per key at the storage level.
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

async fn gc_tick<S: CrdtStore>(store: &S, subscriptions: &SubscriptionManager) -> Result<()> {
    let now = now_ms();
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

    Ok(())
}
