use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{
    error::{Result, StorageError},
    store::Store,
};

// ---------------------------------------------------------------------------
// SledStore
// ---------------------------------------------------------------------------

/// Persistent CRDT storage backed by sled.
///
/// ## Key encoding
/// `"{namespace}/{crdt_id}"` as UTF-8 bytes. Both components must be
/// `[a-z0-9_-]{1,64}` (enforced by the namespace module, not here).
///
/// ## Thread safety
/// `sled::Db` is internally an `Arc` — `SledStore` is `Clone + Send + Sync`.
///
/// ## sled ↔ tokio bridge
/// All sled calls are wrapped in `tokio::task::spawn_blocking` to avoid
/// blocking the async executor.
pub struct SledStore {
    db: sled::Db,
}

impl SledStore {
    /// Open (or create) a sled database at `path`.
    pub fn open(path: &str) -> Result<Self> {
        let db = sled::open(path)?;
        debug!(path, "SledStore opened");
        Ok(Self { db })
    }

    /// Open an ephemeral in-memory database (for tests).
    pub fn open_temporary() -> Result<Self> {
        let db = sled::Config::new().temporary(true).open()?;
        Ok(Self { db })
    }

    /// Expose the underlying sled `Db` so callers can construct backend-specific
    /// components (e.g. `SledWal::new(store.db())`).
    pub fn db(&self) -> &sled::Db {
        &self.db
    }

    fn make_key(ns: &str, id: &str) -> Vec<u8> {
        format!("{ns}/{id}").into_bytes()
    }

    fn parse_key(key: &[u8]) -> Result<String> {
        std::str::from_utf8(key)
            .map(|s| s.to_owned())
            .map_err(|_| StorageError::InvalidKey("non-UTF8 key".into()))
    }
}

// ---------------------------------------------------------------------------
// Store impl
// ---------------------------------------------------------------------------

impl<V> Store<V> for SledStore
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    #[instrument(skip(self), fields(ns, id))]
    async fn get(&self, ns: &str, id: &str) -> Result<Option<V>> {
        let key = Self::make_key(ns, id);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || match db.get(&key)? {
            None => Ok(None),
            Some(bytes) => {
                let value = rmp_serde::decode::from_slice::<V>(&bytes)
                    .map_err(StorageError::Deserialization)?;
                Ok(Some(value))
            }
        })
        .await
        .map_err(|_| StorageError::TaskJoin)?
    }

    #[instrument(skip(self, value), fields(ns, id))]
    async fn put(&self, ns: &str, id: &str, value: &V) -> Result<()> {
        let key = Self::make_key(ns, id);
        let bytes = rmp_serde::encode::to_vec_named(value).map_err(StorageError::Serialization)?;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.insert(key, bytes)?;
            Ok(())
        })
        .await
        .map_err(|_| StorageError::TaskJoin)?
    }

    #[instrument(skip(self), fields(ns, id))]
    async fn delete(&self, ns: &str, id: &str) -> Result<()> {
        let key = Self::make_key(ns, id);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.remove(key)?;
            Ok(())
        })
        .await
        .map_err(|_| StorageError::TaskJoin)?
    }

    #[instrument(skip(self), fields(prefix))]
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, V)>> {
        let prefix_bytes = prefix.as_bytes().to_vec();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut results = Vec::new();
            for kv in db.scan_prefix(&prefix_bytes) {
                let (k, v) = kv?;
                let key_str = Self::parse_key(&k)?;
                if key_str.starts_with('_') {
                    continue;
                }
                let value = rmp_serde::decode::from_slice::<V>(&v)
                    .map_err(StorageError::Deserialization)?;
                results.push((key_str, value));
            }
            Ok(results)
        })
        .await
        .map_err(|_| StorageError::TaskJoin)?
    }

    async fn flush(&self) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.flush().map_err(StorageError::Sled).map(|_| ()))
            .await
            .map_err(|_| StorageError::TaskJoin)?
    }
}
