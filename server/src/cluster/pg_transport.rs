// PostgresNotifyTransport — ClusterTransport backed by PostgreSQL NOTIFY/LISTEN.
//
// Two payload kinds share the same channel ("meridian_ops"):
//
//   kind = "delta"  — emitted by broadcast_delta() (node-to-node replication).
//     { "kind": "delta", "origin": "<hex node_id>", "ns": "...", "crdt_id": "...", "d": "<b64 delta_bytes>" }
//     delta_bytes = msgpack-encoded typed delta (GCounterDelta, LwwDelta, …).
//     Forwarded directly to local WebSocket subscribers via publish_delta().
//
//   kind = "state"  — emitted by the meridian_notify_trigger() pgrx trigger.
//     { "kind": "state", "ns": "...", "crdt_id": "...", "d": "<b64 CrdtValue msgpack>" }
//     d = the full msgpack-encoded CrdtValue of the row after the write.
//     The server merges this state into its own store and derives the typed delta
//     before forwarding to subscribers — so clients always receive correct deltas.
//
// Large payloads (> ~7 800 bytes) are silently dropped — use logical
// replication (Phase 4) for large RGA / Tree documents.
//
// Anti-entropy (reconnect resync):
//
// pg_notify does NOT buffer missed notifications across disconnections.
// To recover state lost during a listener outage, the transport calls an
// optional SQL function registered by the user:
//
//   meridian.resync_fn(namespace TEXT) → TABLE(crdt_id TEXT, state BYTEA)
//
// If that function exists in the database, the transport calls it once per
// namespace after every reconnection, merging all returned states via the
// normal PgStateApplier path.  This is opt-in: if the function is absent,
// the behaviour is unchanged (best-effort delivery only).
//
// Example registration:
//   CREATE OR REPLACE FUNCTION meridian.resync_fn(ns TEXT)
//   RETURNS TABLE(crdt_id TEXT, state BYTEA) AS $$
//     SELECT 'gc:' || id::TEXT, views
//     FROM   articles
//     WHERE  views IS NOT NULL;
//   $$ LANGUAGE sql STABLE;

use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use bytes::Bytes;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgListener, PgPool};
use tokio::sync::broadcast;
use tracing::{debug, error, instrument, warn};

use meridian_cluster::{
    error::{ClusterError, Result},
    node_id::NodeId,
    transport::{ClusterTransport, DeltaEnvelope},
    LocalBroadcast,
};

use crate::{
    crdt::{Crdt, registry::CrdtValue, VectorClock},
    storage::CrdtStore,
};

const NOTIFY_CHANNEL: &str = "meridian_ops";
const MAX_NOTIFY_BYTES: usize = 7_800;

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PgPayload<'a> {
    /// Node-to-node delta broadcast (origin present).
    Delta {
        origin: &'a str,
        ns: &'a str,
        crdt_id: &'a str,
        d: &'a str,
    },
    /// Trigger-emitted full state (no origin — comes from Postgres itself).
    State {
        ns: &'a str,
        crdt_id: &'a str,
        d: &'a str,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PgPayloadOwned {
    Delta { origin: String, ns: String, crdt_id: String, d: String },
    State { ns: String, crdt_id: String, d: String },
}

/// Implemented by the server to merge an incoming Postgres state snapshot.
///
/// `meridian-cluster` can't depend on the CRDT registry, so we inject this
/// as a trait object — same pattern as `AntiEntropyApplier`.
pub trait PgStateApplier: Send + Sync + 'static {
    /// Merge `state_bytes` (msgpack CrdtValue) into the local store.
    /// Returns the typed delta bytes to forward to WS subscribers, or None
    /// if the incoming state added nothing new.
    fn merge_pg_state(
        &self,
        namespace: String,
        crdt_id: String,
        state_bytes: Vec<u8>,
    ) -> impl std::future::Future<Output = Option<Vec<u8>>> + Send;
}

/// Server-side implementation of `PgStateApplier`.
pub struct StorePgApplier<S> {
    store: Arc<S>,
    broadcast: Arc<dyn LocalBroadcast>,
}

impl<S: CrdtStore> StorePgApplier<S> {
    pub fn new(store: Arc<S>, broadcast: Arc<dyn LocalBroadcast>) -> Self {
        Self { store, broadcast }
    }
}

impl<S: CrdtStore> PgStateApplier for StorePgApplier<S> {
    async fn merge_pg_state(
        &self,
        namespace: String,
        crdt_id: String,
        state_bytes: Vec<u8>,
    ) -> Option<Vec<u8>> {
        // Deserialize the full CrdtValue from the trigger payload.
        let incoming: CrdtValue = match rmp_serde::decode::from_slice(&state_bytes) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, ns = %namespace, crdt_id = %crdt_id, "failed to decode pg state");
                return None;
            }
        };

        let crdt_type = incoming.crdt_type();
        let empty_vc = VectorClock::new();

        // Merge into the store: read existing, merge incoming, write back,
        // return the typed delta (delta_since empty vc = "everything new").
        let ns_clone = namespace.clone();
        let id_clone = crdt_id.clone();
        let delta: Option<Vec<u8>> = self.store
            .merge_put_with(
                &namespace,
                &crdt_id,
                CrdtValue::new(crdt_type),
                move |existing, default| {
                    let mut crdt = existing.unwrap_or(default);
                    // Detect type mismatch before merging — log and skip rather
                    // than silently discarding the incoming state.
                    if std::mem::discriminant(&crdt) != std::mem::discriminant(&incoming) {
                        error!(
                            ns = %ns_clone,
                            crdt_id = %id_clone,
                            stored_type = ?std::mem::discriminant(&crdt),
                            incoming_type = ?std::mem::discriminant(&incoming),
                            "pg state type mismatch — incoming state discarded"
                        );
                        let delta_bytes = crdt.delta_since_msgpack(&empty_vc).ok().flatten();
                        return (crdt, delta_bytes);
                    }
                    // merge() is defined per-variant — dispatch manually.
                    merge_crdt_value(&mut crdt, &incoming);
                    let delta_bytes = crdt
                        .delta_since_msgpack(&empty_vc)
                        .ok()
                        .flatten();
                    (crdt, delta_bytes)
                },
            )
            .await
            .ok()
            .flatten();

        if let Some(ref bytes) = delta {
            self.broadcast.publish_delta(&namespace, &crdt_id, Bytes::copy_from_slice(bytes));
        }

        delta
    }
}

/// Dispatch `merge` for each CrdtValue variant.
/// CrdtValue itself doesn't expose a `merge(&other)` method — the `Crdt`
/// trait's `merge` takes a concrete type. We unwrap both sides.
fn merge_crdt_value(target: &mut CrdtValue, source: &CrdtValue) {
    use CrdtValue::*;
    match (target, source) {
        (GCounter(a),    GCounter(b))    => a.merge(b),
        (PNCounter(a),   PNCounter(b))   => a.merge(b),
        (ORSet(a),       ORSet(b))       => a.merge(b),
        (LwwRegister(a), LwwRegister(b)) => a.merge(b),
        (Presence(a),    Presence(b))    => a.merge(b),
        (CRDTMap(a),     CRDTMap(b))     => a.merge(b),
        (RGA(a),         RGA(b))         => a.merge(b),
        (Tree(a),        Tree(b))        => a.merge(b),
        // Type mismatch — skip (schema evolution / misconfiguration).
        // Logged at the call site where namespace/crdt_id are available.
        _ => {}
    }
}

/// Cluster transport backed by PostgreSQL NOTIFY/LISTEN.
///
/// Handles two payload kinds:
/// - `delta` — node-to-node replication (forwarded directly to WS subscribers)
/// - `state` — trigger-emitted full state (merged into store, delta derived)
#[derive(Clone)]
pub struct PostgresNotifyTransport {
    inner: Arc<Inner>,
}

struct Inner {
    pool: PgPool,
    node_id: NodeId,
    tx: broadcast::Sender<DeltaEnvelope>,
    /// Namespaces to resync after every listener reconnection.
    /// Populated via `register_resync_namespace`.
    resync_namespaces: tokio::sync::RwLock<Vec<String>>,
}

impl PostgresNotifyTransport {
    pub async fn new(pool: PgPool, node_id: NodeId) -> Result<Self> {
        let (tx, _) = broadcast::channel(1024);
        let transport = Self {
            inner: Arc::new(Inner {
                pool,
                node_id,
                tx,
                resync_namespaces: tokio::sync::RwLock::new(Vec::new()),
            }),
        };
        Ok(transport)
    }

    /// Register a namespace for anti-entropy resync after listener reconnections.
    ///
    /// For each registered namespace, the transport will call
    /// `meridian.resync_fn(namespace)` after every reconnection (if that
    /// function exists) and merge all returned states.  Call this once per
    /// namespace during server startup, before `spawn_listener`.
    pub async fn register_resync_namespace(&self, namespace: impl Into<String>) {
        self.inner.resync_namespaces.write().await.push(namespace.into());
    }

    /// Start the background WAL logical replication consumer.
    ///
    /// `connstr` must be a libpq connection string or URL pointing to the same
    /// Postgres instance.  The slot and publication are created automatically
    /// if they don't exist.
    ///
    /// This supplements `spawn_listener`: large payloads (> ~7 800 bytes) that
    /// are silently dropped by pg_notify are delivered reliably via the WAL
    /// stream instead.  Both paths feed the same idempotent `PgStateApplier`.
    pub fn spawn_wal_replication<A: PgStateApplier>(
        &self,
        connstr: String,
        slot_name: String,
        pub_name: String,
        applier: Arc<A>,
    ) {
        tokio::spawn(super::wal_replication::run(connstr, slot_name, pub_name, applier));
    }

    /// Start the background LISTEN loop.
    ///
    /// Must be called after construction.  Separated from `new()` so the
    /// caller can inject the `PgStateApplier` (which depends on the store,
    /// which is constructed after the transport in `server.rs`).
    pub fn spawn_listener<A: PgStateApplier>(&self, applier: Arc<A>) {
        let pool    = self.inner.pool.clone();
        let node_id = self.inner.node_id;
        let tx      = self.inner.tx.clone();
        let inner   = Arc::clone(&self.inner);

        tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_secs(1);
            const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);
            let mut attempt: u32 = 0;

            loop {
                match run_listener(&pool, node_id, &tx, &*applier).await {
                    Ok(()) => break,
                    Err(e) => {
                        if attempt == 0 {
                            // First failure is a hard startup error — log at ERROR.
                            error!(error = %e, "pg_notify listener failed to start");
                        } else {
                            warn!(
                                error = %e,
                                backoff_secs = backoff.as_secs(),
                                "pg_notify listener disconnected, reconnecting"
                            );
                        }
                        attempt += 1;
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);

                        // Resync missed state after reconnection.
                        let namespaces = inner.resync_namespaces.read().await.clone();
                        if !namespaces.is_empty() {
                            resync_namespaces(&pool, &namespaces, &*applier).await;
                        }
                    }
                }
            }
        });
    }
}

/// After a listener reconnection, call `meridian.resync_fn(namespace)` for
/// each registered namespace (if the function exists) and merge all returned
/// states via the normal applier path.
async fn resync_namespaces<A: PgStateApplier>(
    pool: &PgPool,
    namespaces: &[String],
    applier: &A,
) {
    // Check once whether the resync function exists.
    let fn_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'meridian' AND p.proname = 'resync_fn'
         )",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(false);

    if !fn_exists {
        return;
    }

    for namespace in namespaces {
        match sqlx::query_as::<_, (String, Vec<u8>)>(
            "SELECT crdt_id, state FROM meridian.resync_fn($1)",
        )
        .bind(namespace)
        .fetch_all(pool)
        .await
        {
            Ok(rows) => {
                let count = rows.len();
                for (crdt_id, state_bytes) in rows {
                    applier
                        .merge_pg_state(namespace.clone(), crdt_id, state_bytes)
                        .await;
                }
                if count > 0 {
                    tracing::info!(
                        namespace = %namespace,
                        rows = count,
                        "pg anti-entropy resync complete"
                    );
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    namespace = %namespace,
                    "pg anti-entropy resync failed"
                );
            }
        }
    }
}

async fn run_listener<A: PgStateApplier>(
    pool: &PgPool,
    node_id: NodeId,
    tx: &broadcast::Sender<DeltaEnvelope>,
    applier: &A,
) -> Result<()> {
    let mut listener = PgListener::connect_with(pool).await
        .map_err(|e| ClusterError::Transport(e.to_string()))?;

    listener.listen(NOTIFY_CHANNEL).await
        .map_err(|e| ClusterError::Transport(e.to_string()))?;

    // Pre-compute once — avoids a String allocation on every received notification.
    let own_origin = node_id.to_string();

    loop {
        let notif = listener.recv().await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        let parsed: PgPayloadOwned = match serde_json::from_str(notif.payload()) {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, raw = %notif.payload(), "failed to parse pg_notify payload");
                continue;
            }
        };

        match parsed {
            // Node-to-node delta — filter self, forward to local subscribers.
            PgPayloadOwned::Delta { origin, ns, crdt_id, d } => {
                if origin == own_origin {
                    debug!(node_id = %node_id, "dropping self-originating delta");
                    continue;
                }

                let delta_bytes = match B64.decode(&d) {
                    Ok(b) => b,
                    Err(e) => { warn!(error = %e, "bad base64 in delta payload"); continue; }
                };

                let origin_id = u64::from_str_radix(&origin, 16)
                    .map(NodeId)
                    .unwrap_or(node_id);

                // Ignore send error — no subscribers yet is fine.
                let _ = tx.send(DeltaEnvelope {
                    origin_node_id: origin_id,
                    namespace: ns,
                    crdt_id,
                    delta_bytes,
                });
            }

            // Trigger-emitted full state — merge into store, derive delta.
            PgPayloadOwned::State { ns, crdt_id, d } => {
                let state_bytes = match B64.decode(&d) {
                    Ok(b) => b,
                    Err(e) => { warn!(error = %e, "bad base64 in state payload"); continue; }
                };

                applier.merge_pg_state(ns, crdt_id, state_bytes).await;
            }
        }
    }
}

#[async_trait]
impl ClusterTransport for PostgresNotifyTransport {
    #[instrument(skip(self, envelope), fields(ns = %envelope.namespace, crdt_id = %envelope.crdt_id))]
    async fn broadcast_delta(&self, envelope: DeltaEnvelope) -> Result<()> {
        let d      = B64.encode(&envelope.delta_bytes);
        let origin = self.inner.node_id.to_string();

        let payload = serde_json::to_string(&PgPayload::Delta {
            origin: &origin,
            ns: &envelope.namespace,
            crdt_id: &envelope.crdt_id,
            d: &d,
        })
        .map_err(|e| ClusterError::Transport(format!("json encode: {e}")))?;

        if payload.len() > MAX_NOTIFY_BYTES {
            warn!(
                ns = %envelope.namespace,
                crdt_id = %envelope.crdt_id,
                size = payload.len(),
                "pg_notify payload too large — delta skipped"
            );
            return Ok(());
        }

        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(NOTIFY_CHANNEL)
            .bind(&payload)
            .execute(&self.inner.pool)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        debug!(channel = NOTIFY_CHANNEL, size = payload.len(), "delta published via pg_notify");
        Ok(())
    }

    fn subscribe_deltas(&self) -> BoxStream<'static, DeltaEnvelope> {
        let mut rx = self.inner.tx.subscribe();

        Box::pin(stream! {
            loop {
                match rx.recv().await {
                    Ok(envelope) => yield envelope,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(dropped = n, "pg_notify subscriber lagged — anti-entropy will recover");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }
}
