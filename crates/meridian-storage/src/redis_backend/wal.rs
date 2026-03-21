use std::sync::atomic::{AtomicU64, Ordering};

use redis::{aio::MultiplexedConnection, Client};
use tracing::instrument;

use crate::{
    error::Result,
    utils::now_ms,
    wal_backend::{WalBackend, WalEntry},
};

// ---------------------------------------------------------------------------
// RedisWal — Redis Streams backed Write-Ahead Log
// ---------------------------------------------------------------------------

/// Write-Ahead Log backed by Redis Streams (`XADD` / `XRANGE` / `XTRIM`).
///
/// ## Stream key
/// `meridian:wal` — single global stream; each entry carries `namespace`,
/// `crdt_id`, `op_bytes`, and `timestamp_ms` fields.
///
/// ## Sequence numbers
/// Redis Stream IDs have the form `{ms}-{seq_within_ms}`. We encode them as
/// `ms * 1_000 + seq_within_ms` — monotonic u64 values safe to compare and
/// store in atomics.
///
/// ## In-memory caches
/// `last_seq` and `checkpoint_seq` are cached as `AtomicU64` (same pattern
/// as `PgWal`) so hot-path reads avoid a Redis round-trip.
pub struct RedisWal {
    client: Client,
    last_seq: AtomicU64,
    checkpoint_seq: AtomicU64,
}

const WAL_STREAM_KEY: &str = "meridian:wal";
const CHECKPOINT_KEY: &str = "meridian:wal:checkpoint";

impl RedisWal {
    /// Connect and restore `last_seq` / `checkpoint_seq` from Redis.
    pub async fn new(url: &str) -> Result<Self> {
        let client = Client::open(url)?;
        let mut conn = client.get_multiplexed_tokio_connection().await?;

        // Last entry in the stream — its ID encodes last_seq.
        let last_seq = {
            let raw: redis::Value = redis::cmd("XREVRANGE")
                .arg(WAL_STREAM_KEY)
                .arg("+")
                .arg("-")
                .arg("COUNT")
                .arg(1u64)
                .query_async(&mut conn)
                .await?;

            match raw {
                redis::Value::Array(items) if !items.is_empty() => {
                    if let redis::Value::Array(parts) = &items[0] {
                        if let Some(redis::Value::BulkString(id_bytes)) = parts.first() {
                            let id = String::from_utf8_lossy(id_bytes);
                            stream_id_to_seq(&id)
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                }
                _ => 0,
            }
        };

        // Checkpoint — stored as a plain u64 string.
        let checkpoint_seq: u64 = {
            let raw: Option<String> = redis::cmd("GET")
                .arg(CHECKPOINT_KEY)
                .query_async(&mut conn)
                .await?;
            raw.and_then(|s| s.parse().ok()).unwrap_or(0)
        };

        Ok(Self {
            client,
            last_seq: AtomicU64::new(last_seq),
            checkpoint_seq: AtomicU64::new(checkpoint_seq),
        })
    }

    async fn conn(&self) -> Result<MultiplexedConnection> {
        Ok(self.client.get_multiplexed_tokio_connection().await?)
    }
}

// ---------------------------------------------------------------------------
// WalBackend impl
// ---------------------------------------------------------------------------

impl WalBackend for RedisWal {
    #[instrument(skip(self, op_bytes))]
    async fn append(&self, namespace: &str, crdt_id: &str, op_bytes: Vec<u8>) -> Result<u64> {
        let mut conn = self.conn().await?;
        let timestamp_ms = now_ms();

        let id: String = redis::cmd("XADD")
            .arg(WAL_STREAM_KEY)
            .arg("*")
            .arg("namespace")
            .arg(namespace)
            .arg("crdt_id")
            .arg(crdt_id)
            .arg("op_bytes")
            .arg(op_bytes.as_slice())
            .arg("timestamp_ms")
            .arg(timestamp_ms)
            .query_async(&mut conn)
            .await?;

        let seq = stream_id_to_seq(&id);
        self.last_seq.fetch_max(seq, Ordering::Relaxed);
        Ok(seq)
    }

    async fn replay_from(&self, from_seq: u64) -> Result<Vec<WalEntry>> {
        let mut conn = self.conn().await?;
        xrange(&mut conn, &seq_to_stream_id(from_seq), "+").await
    }

    async fn replay_until(&self, from_seq: u64, until_ms: u64) -> Result<Vec<WalEntry>> {
        let mut conn = self.conn().await?;
        let end = format!("{until_ms}-999");
        xrange(&mut conn, &seq_to_stream_id(from_seq), &end).await
    }

    async fn truncate_before(&self, before_seq: u64) -> Result<()> {
        let mut conn = self.conn().await?;
        redis::cmd("XTRIM")
            .arg(WAL_STREAM_KEY)
            .arg("MINID")
            .arg(seq_to_stream_id(before_seq))
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    fn last_seq(&self) -> u64 {
        self.last_seq.load(Ordering::Relaxed)
    }

    fn checkpoint_seq(&self) -> u64 {
        self.checkpoint_seq.load(Ordering::Relaxed)
    }

    async fn set_checkpoint_seq(&self, seq: u64) -> Result<()> {
        let mut conn = self.conn().await?;
        redis::cmd("SET")
            .arg(CHECKPOINT_KEY)
            .arg(seq)
            .query_async::<()>(&mut conn)
            .await?;
        self.checkpoint_seq.store(seq, Ordering::Relaxed);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn stream_id_to_seq(id: &str) -> u64 {
    let mut parts = id.splitn(2, '-');
    let ms: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let sub: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    ms * 1_000 + sub
}

fn seq_to_stream_id(seq: u64) -> String {
    format!("{}-{}", seq / 1_000, seq % 1_000)
}

async fn xrange(conn: &mut MultiplexedConnection, start: &str, end: &str) -> Result<Vec<WalEntry>> {
    let raw: redis::Value = redis::cmd("XRANGE")
        .arg(WAL_STREAM_KEY)
        .arg(start)
        .arg(end)
        .query_async(conn)
        .await?;

    let mut entries = Vec::new();

    if let redis::Value::Array(items) = raw {
        for item in items {
            if let redis::Value::Array(parts) = item {
                if parts.len() < 2 {
                    continue;
                }
                let id = match &parts[0] {
                    redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                    _ => continue,
                };
                let seq = stream_id_to_seq(&id);

                if let redis::Value::Array(fields) = &parts[1] {
                    let map = fields_to_map(fields);
                    let namespace = map.get("namespace").cloned().unwrap_or_default();
                    let crdt_id = map.get("crdt_id").cloned().unwrap_or_default();
                    let op_bytes = map
                        .get("op_bytes")
                        .map(|s| s.as_bytes().to_vec())
                        .unwrap_or_default();
                    let timestamp_ms: u64 = map
                        .get("timestamp_ms")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    entries.push(WalEntry { seq, namespace, crdt_id, op_bytes, timestamp_ms });
                }
            }
        }
    }

    Ok(entries)
}

fn fields_to_map(fields: &[redis::Value]) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    let mut iter = fields.iter();
    while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
        let key = match k {
            redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
            _ => continue,
        };
        let val = match v {
            redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
            _ => continue,
        };
        map.insert(key, val);
    }
    map
}
