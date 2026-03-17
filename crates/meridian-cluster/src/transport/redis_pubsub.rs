use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use redis::{aio::MultiplexedConnection, Client};
use tokio::sync::broadcast;
use tracing::{debug, instrument, warn};

use crate::{
    error::{ClusterError, Result},
    node_id::NodeId,
    transport::{ClusterTransport, DeltaEnvelope},
};

// Redis channel prefix — one channel per namespace.
// Format: `meridian:delta:{namespace}`
const CHANNEL_PREFIX: &str = "meridian:delta:";

// ---------------------------------------------------------------------------
// RedisTransport
// ---------------------------------------------------------------------------

/// Cluster transport backed by Redis Pub/Sub.
///
/// Each namespace maps to a Redis channel `meridian:delta:{ns}`.
/// On publish: serialize `DeltaEnvelope` as msgpack, call `PUBLISH`.
/// On subscribe: `SUBSCRIBE` to `meridian:delta:*` (pattern), deserialize,
/// filter out messages originating from this node.
#[derive(Clone)]
pub struct RedisTransport {
    inner: Arc<Inner>,
}

struct Inner {
    client: Client,
    node_id: NodeId,
    /// Internal broadcast channel — the subscription loop forwards
    /// received envelopes here; `subscribe_deltas()` taps it.
    tx: broadcast::Sender<DeltaEnvelope>,
}

impl RedisTransport {
    /// Connect to Redis and spawn the background subscription loop.
    pub async fn new(url: &str, node_id: NodeId) -> Result<Self> {
        let client = Client::open(url)?;

        // Verify connectivity.
        let mut conn = client.get_multiplexed_tokio_connection().await?;
        redis::cmd("PING").query_async::<String>(&mut conn).await?;

        let (tx, _) = broadcast::channel(1024);

        let transport = Self {
            inner: Arc::new(Inner { client, node_id, tx }),
        };

        transport.spawn_subscriber_loop();

        Ok(transport)
    }

    fn spawn_subscriber_loop(&self) {
        let client = self.inner.client.clone();
        let node_id = self.inner.node_id;
        let tx = self.inner.tx.clone();

        tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_secs(1);
            const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

            loop {
                match run_subscriber(&client, node_id, &tx).await {
                    Ok(()) => break, // clean shutdown (tx dropped)
                    Err(e) => {
                        warn!(error = %e, backoff_secs = backoff.as_secs(), "cluster Redis subscriber disconnected, reconnecting");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    }
                }
            }
        });
    }
}

async fn run_subscriber(
    client: &Client,
    node_id: NodeId,
    tx: &broadcast::Sender<DeltaEnvelope>,
) -> Result<()> {
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.psubscribe(format!("{CHANNEL_PREFIX}*")).await?;

    let mut stream = pubsub.on_message();

    while let Some(msg) = stream.next().await {
        let payload: Vec<u8> = msg.get_payload()?;

        let envelope: DeltaEnvelope = match rmp_serde::decode::from_slice(&payload) {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "failed to decode cluster delta envelope");
                continue;
            }
        };

        // Drop messages from ourselves — we already published locally.
        if envelope.origin_node_id == node_id {
            debug!(node_id = %node_id, "dropping self-originating delta");
            continue;
        }

        // If no receivers, the message is dropped. That's fine —
        // anti-entropy will recover within one gossip interval.
        let _ = tx.send(envelope);
    }

    Ok(())
}

#[async_trait]
impl ClusterTransport for RedisTransport {
    #[instrument(skip(self, envelope), fields(ns = %envelope.namespace, crdt_id = %envelope.crdt_id))]
    async fn broadcast_delta(&self, envelope: DeltaEnvelope) -> Result<()> {
        let channel = format!("{CHANNEL_PREFIX}{}", envelope.namespace);
        let payload = rmp_serde::encode::to_vec_named(&envelope)
            .map_err(ClusterError::Serialization)?;

        let mut conn: MultiplexedConnection =
            self.inner.client.get_multiplexed_tokio_connection().await?;

        redis::cmd("PUBLISH")
            .arg(&channel)
            .arg(payload.as_slice())
            .query_async::<i64>(&mut conn)
            .await?;

        debug!(channel, "delta published to Redis");
        Ok(())
    }

    fn subscribe_deltas(&self) -> BoxStream<'static, DeltaEnvelope> {
        let mut rx = self.inner.tx.subscribe();

        Box::pin(stream! {
            loop {
                match rx.recv().await {
                    Ok(envelope) => yield envelope,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(dropped = n, "cluster subscriber lagged — some deltas lost, anti-entropy will recover");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }
}
