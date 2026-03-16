use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    error::{Result, StorageError},
    store::Store,
};

// ---------------------------------------------------------------------------
// RedisStore
// ---------------------------------------------------------------------------

/// CRDT snapshot storage backed by Redis.
///
/// ## Key format
/// `meridian:{namespace}:{crdt_id}` → msgpack-encoded CRDT value
///
/// ## Scan
/// Uses cursor-based `SCAN` with a glob pattern — non-blocking, O(N) over
/// matching keys. For large keyspaces consider partitioning by namespace.
pub struct RedisStore {
    client: Client,
}

impl RedisStore {
    pub async fn new(url: &str) -> Result<Self> {
        let client = Client::open(url)?;
        Ok(Self { client })
    }

    async fn conn(&self) -> Result<MultiplexedConnection> {
        Ok(self.client.get_multiplexed_tokio_connection().await?)
    }

    fn redis_key(ns: &str, id: &str) -> String {
        format!("meridian:{ns}:{id}")
    }

    fn scan_pattern(ns: &str) -> String {
        format!("meridian:{ns}:*")
    }
}

// ---------------------------------------------------------------------------
// Store impl
// ---------------------------------------------------------------------------

impl<V> Store<V> for RedisStore
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    #[instrument(skip(self), fields(ns, id))]
    async fn get(&self, ns: &str, id: &str) -> Result<Option<V>> {
        let mut conn = self.conn().await?;
        let key = Self::redis_key(ns, id);
        let bytes: Option<Vec<u8>> = conn.get(&key).await?;

        match bytes {
            None => Ok(None),
            Some(b) => {
                let value = rmp_serde::decode::from_slice::<V>(&b)
                    .map_err(StorageError::Deserialization)?;
                Ok(Some(value))
            }
        }
    }

    #[instrument(skip(self, value), fields(ns, id))]
    async fn put(&self, ns: &str, id: &str, value: &V) -> Result<()> {
        let mut conn = self.conn().await?;
        let key = Self::redis_key(ns, id);
        let bytes =
            rmp_serde::encode::to_vec_named(value).map_err(StorageError::Serialization)?;
        conn.set::<_, Vec<u8>, ()>(&key, bytes).await?;
        Ok(())
    }

    #[instrument(skip(self), fields(ns, id))]
    async fn delete(&self, ns: &str, id: &str) -> Result<()> {
        let mut conn = self.conn().await?;
        let key = Self::redis_key(ns, id);
        conn.del::<_, ()>(&key).await?;
        Ok(())
    }

    #[instrument(skip(self), fields(prefix))]
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, V)>> {
        let mut conn = self.conn().await?;
        let pattern = Self::scan_pattern(prefix);

        // Cursor-based SCAN — collects all matching keys without blocking.
        let mut cursor: u64 = 0;
        let mut keys: Vec<String> = Vec::new();
        loop {
            let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100u64)
                .query_async(&mut conn)
                .await?;
            keys.extend(batch);
            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let bytes: Option<Vec<u8>> = conn.get(&key).await?;
            if let Some(b) = bytes {
                let value = rmp_serde::decode::from_slice::<V>(&b)
                    .map_err(StorageError::Deserialization)?;
                // key format: "meridian:{ns}:{id}" → "{ns}/{id}"
                let stripped = key
                    .strip_prefix("meridian:")
                    .unwrap_or(&key)
                    .replacen(':', "/", 1);
                results.push((stripped, value));
            }
        }

        Ok(results)
    }
}
