use std::sync::Arc;

use tracing::{debug, instrument};

use crate::crdt::registry::CrdtValue;

use super::{
    error::{Result, StorageError},
    store::Store,
    wal::Wal,
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
/// blocking the async executor (Rule: never block in async code).
pub struct SledStore {
    db: sled::Db,
    pub wal: Arc<Wal>,
}

impl SledStore {
    /// Open (or create) a sled database at `path`.
    pub fn open(path: &str) -> Result<Self> {
        let db = sled::open(path)?;
        let wal = Arc::new(Wal::new(&db)?);
        debug!(path, "SledStore opened");
        Ok(Self { db, wal })
    }

    /// Open an ephemeral in-memory database (for tests).
    pub fn open_temporary() -> Result<Self> {
        let db = sled::Config::new().temporary(true).open()?;
        let wal = Arc::new(Wal::new(&db)?);
        Ok(Self { db, wal })
    }

    /// Flush all pending writes to disk.
    pub async fn flush(&self) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.flush().map_err(StorageError::Sled).map(|_| ()))
            .await
            .map_err(|_| StorageError::TaskJoin)?
    }

    // -- key helpers --

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

impl Store for SledStore {
    #[instrument(skip(self), fields(ns, id))]
    async fn get(&self, ns: &str, id: &str) -> Result<Option<CrdtValue>> {
        let key = Self::make_key(ns, id);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            match db.get(&key)? {
                None => Ok(None),
                Some(bytes) => {
                    let value = CrdtValue::from_msgpack(&bytes)?;
                    Ok(Some(value))
                }
            }
        })
        .await
        .map_err(|_| StorageError::TaskJoin)?
    }

    #[instrument(skip(self, value), fields(ns, id))]
    async fn put(&self, ns: &str, id: &str, value: &CrdtValue) -> Result<()> {
        let key = Self::make_key(ns, id);
        let bytes = value.to_msgpack()?;
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
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, CrdtValue)>> {
        let prefix_bytes = prefix.as_bytes().to_vec();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut results = Vec::new();
            for kv in db.scan_prefix(&prefix_bytes) {
                let (k, v) = kv?;
                // Skip internal trees (WAL, meta)
                let key_str = Self::parse_key(&k)?;
                if key_str.starts_with('_') {
                    continue;
                }
                let value = CrdtValue::from_msgpack(&v)?;
                results.push((key_str, value));
            }
            Ok(results)
        })
        .await
        .map_err(|_| StorageError::TaskJoin)?
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::{
        gcounter::GCounterOp,
        registry::{apply_op, CrdtOp, CrdtType},
    };

    fn store() -> SledStore {
        SledStore::open_temporary().unwrap()
    }

    #[tokio::test]
    async fn put_and_get_roundtrip() {
        let s = store();
        let mut v = CrdtValue::new(CrdtType::GCounter);
        apply_op(&mut v, CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 42 })).unwrap();

        s.put("ns", "counter", &v).await.unwrap();
        let loaded = s.get("ns", "counter").await.unwrap().unwrap();
        assert_eq!(v.to_json_value(), loaded.to_json_value());
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let s = store();
        assert!(s.get("ns", "ghost").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_removes_entry() {
        let s = store();
        let v = CrdtValue::new(CrdtType::GCounter);
        s.put("ns", "x", &v).await.unwrap();
        s.delete("ns", "x").await.unwrap();
        assert!(s.get("ns", "x").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn scan_prefix_returns_matching_keys() {
        let s = store();
        let v1 = CrdtValue::new(CrdtType::GCounter);
        let v2 = CrdtValue::new(CrdtType::PNCounter);
        let v3 = CrdtValue::new(CrdtType::ORSet);

        s.put("alpha", "a", &v1).await.unwrap();
        s.put("alpha", "b", &v2).await.unwrap();
        s.put("beta", "c", &v3).await.unwrap();

        let results = s.scan_prefix("alpha/").await.unwrap();
        assert_eq!(results.len(), 2);
        let keys: Vec<&str> = results.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"alpha/a"));
        assert!(keys.contains(&"alpha/b"));
    }

    #[tokio::test]
    async fn put_overwrites_existing() {
        let s = store();
        let mut v = CrdtValue::new(CrdtType::GCounter);
        s.put("ns", "c", &v).await.unwrap();

        apply_op(&mut v, CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 10 })).unwrap();
        s.put("ns", "c", &v).await.unwrap();

        let loaded = s.get("ns", "c").await.unwrap().unwrap();
        assert_eq!(v.to_json_value(), loaded.to_json_value());
    }
}
