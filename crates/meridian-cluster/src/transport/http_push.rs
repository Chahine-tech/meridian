use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;
use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream};
use tokio::sync::broadcast;
use tracing::{debug, instrument, warn};
use url::Url;

use meridian_storage::{WalBackend, WalEntry};

use crate::{
    error::{ClusterError, Result},
    node_id::NodeId,
    transport::{ClusterTransport, DeltaEnvelope},
};

// ---------------------------------------------------------------------------
// HttpPushTransport
// ---------------------------------------------------------------------------

/// Type-erased WAL replay function stored inside `Inner`.
///
/// `WalBackend` uses `impl Future` return types (not dyn-compatible), so we
/// erase the concrete type at construction time and store a closure instead.
type WalReplayFn =
    dyn Fn(u64) -> BoxFuture<'static, meridian_storage::Result<Vec<WalEntry>>> + Send + Sync;

/// Cluster transport backed by direct HTTP POST to peer nodes.
///
/// Used when Redis is not available (PostgreSQL-only deployments).
///
/// Each node exposes a `POST /internal/cluster/delta` endpoint.
/// On broadcast: serialize `DeltaEnvelope` as msgpack, POST to each peer.
/// On subscribe: return a stream fed by the internal broadcast channel
/// which is populated when peers call *our* endpoint.
///
/// Peers are configured via `MERIDIAN_PEERS=http://node-a:3000,http://node-b:3000`.
/// The internal API is mounted separately — call [`HttpPushTransport::router`]
/// and serve it on the internal port (`MERIDIAN_INTERNAL_BIND`, default `:3001`).
#[derive(Clone)]
pub struct HttpPushTransport {
    inner: Arc<Inner>,
}

struct Inner {
    peers: Vec<Url>,
    node_id: NodeId,
    client: reqwest::Client,
    /// Incoming deltas from peers are forwarded here.
    tx: broadcast::Sender<DeltaEnvelope>,
    /// Type-erased WAL replay for pull anti-entropy (`GET /internal/cluster/wal`).
    wal_replay: Option<Box<WalReplayFn>>,
}

impl HttpPushTransport {
    pub fn new(peers: Vec<Url>, node_id: NodeId) -> Self {
        Self::build(peers, node_id, None)
    }

    /// Create a transport that also exposes this node's WAL for pull anti-entropy.
    pub fn with_wal<W: WalBackend>(peers: Vec<Url>, node_id: NodeId, wal: Arc<W>) -> Self {
        let replay: Box<WalReplayFn> = Box::new(move |from_seq| {
            let wal = Arc::clone(&wal);
            Box::pin(async move { wal.replay_from(from_seq).await })
        });
        Self::build(peers, node_id, Some(replay))
    }

    fn build(peers: Vec<Url>, node_id: NodeId, wal_replay: Option<Box<WalReplayFn>>) -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            inner: Arc::new(Inner {
                peers,
                node_id,
                client: reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(5))
                    .build()
                    .expect("failed to build reqwest client"),
                tx,
                wal_replay,
            }),
        }
    }

    /// Fetch WAL entries from a peer node starting at `from_seq`.
    ///
    /// Used by pull anti-entropy to catch up after a node restart.
    pub async fn pull_wal_from(&self, peer: &Url, from_seq: u64) -> Result<Vec<WalEntry>> {
        let url = peer
            .join(&format!("/internal/cluster/wal?from_seq={from_seq}"))
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        let resp = self
            .inner
            .client
            .get(url.as_str())
            .send()
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ClusterError::Transport(format!(
                "peer {} returned {} for WAL pull",
                peer,
                resp.status()
            )));
        }

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        let entries: Vec<WalEntry> =
            rmp_serde::decode::from_slice(&bytes).map_err(ClusterError::Deserialization)?;

        Ok(entries)
    }

    /// Returns the list of configured peers.
    pub fn peers(&self) -> &[Url] {
        &self.inner.peers
    }

    /// Returns an Axum router that peers use to push deltas to this node.
    ///
    /// Mount this on the internal bind address (e.g. `MERIDIAN_INTERNAL_BIND`):
    /// ```text
    /// let listener = TcpListener::bind("0.0.0.0:3001").await?;
    /// axum::serve(listener, transport.router()).await?;
    /// ```
    pub fn router(&self) -> Router {
        Router::new()
            .route("/internal/cluster/delta", post(handle_incoming_delta))
            .route("/internal/cluster/wal", get(handle_wal_pull))
            .with_state(Arc::clone(&self.inner))
    }
}

// ---------------------------------------------------------------------------
// Internal HTTP handler
// ---------------------------------------------------------------------------

async fn handle_incoming_delta(
    State(inner): State<Arc<Inner>>,
    body: Bytes,
) -> impl IntoResponse {
    let envelope: DeltaEnvelope = match rmp_serde::decode::from_slice(&body) {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "failed to decode incoming cluster delta");
            return StatusCode::BAD_REQUEST;
        }
    };

    if envelope.origin_node_id == inner.node_id {
        debug!("dropping self-originating delta (loop prevention)");
        return StatusCode::OK;
    }

    debug!(
        from = %envelope.origin_node_id,
        ns = %envelope.namespace,
        crdt_id = %envelope.crdt_id,
        "received cluster delta via HTTP"
    );

    // Lagged receivers are fine — anti-entropy recovers.
    let _ = inner.tx.send(envelope);
    StatusCode::OK
}

// ---------------------------------------------------------------------------
// WAL pull handler
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct WalQuery {
    from_seq: u64,
}

async fn handle_wal_pull(
    State(inner): State<Arc<Inner>>,
    Query(params): Query<WalQuery>,
) -> impl IntoResponse {
    let Some(replay) = &inner.wal_replay else {
        return (StatusCode::NOT_IMPLEMENTED, Bytes::new());
    };

    match replay(params.from_seq).await {
        Ok(entries) => match rmp_serde::encode::to_vec_named(&entries) {
            Ok(bytes) => (StatusCode::OK, Bytes::from(bytes)),
            Err(e) => {
                warn!(error = %e, "failed to encode WAL entries for pull response");
                (StatusCode::INTERNAL_SERVER_ERROR, Bytes::new())
            }
        },
        Err(e) => {
            warn!(error = %e, from_seq = params.from_seq, "WAL replay_from failed");
            (StatusCode::INTERNAL_SERVER_ERROR, Bytes::new())
        }
    }
}

// ---------------------------------------------------------------------------
// ClusterTransport impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ClusterTransport for HttpPushTransport {
    #[instrument(skip(self, envelope), fields(ns = %envelope.namespace, crdt_id = %envelope.crdt_id, peers = self.inner.peers.len()))]
    async fn broadcast_delta(&self, envelope: DeltaEnvelope) -> Result<()> {
        let payload = rmp_serde::encode::to_vec_named(&envelope)
            .map_err(ClusterError::Serialization)?;

        let mut failed = 0usize;
        for peer in &self.inner.peers {
            let url = peer
                .join("/internal/cluster/delta")
                .map_err(|e| ClusterError::Transport(e.to_string()))?;

            match self
                .inner
                .client
                .post(url.as_str())
                .header("Content-Type", "application/msgpack")
                .body(Bytes::copy_from_slice(&payload))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    debug!(peer = %peer, "delta pushed to peer");
                }
                Ok(resp) => {
                    warn!(peer = %peer, status = %resp.status(), "peer rejected delta");
                    failed += 1;
                }
                Err(e) => {
                    warn!(peer = %peer, error = %e, "failed to push delta to peer — anti-entropy will recover");
                    failed += 1;
                }
            }
        }

        if failed > 0 && failed == self.inner.peers.len() {
            // All peers failed — surface the error so the caller can log it.
            return Err(ClusterError::Transport(format!(
                "all {} peers unreachable",
                self.inner.peers.len()
            )));
        }

        Ok(())
    }

    fn subscribe_deltas(&self) -> BoxStream<'static, DeltaEnvelope> {
        let mut rx = self.inner.tx.subscribe();

        Box::pin(stream! {
            loop {
                match rx.recv().await {
                    Ok(envelope) => yield envelope,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(dropped = n, "HTTP cluster subscriber lagged — anti-entropy will recover");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }
}
