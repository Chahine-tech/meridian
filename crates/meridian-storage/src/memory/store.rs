use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    error::{Result, StorageError},
    store::Store,
};

/// Default maximum number of entries for `MemoryStore`.
/// Prevents unbounded memory growth in test/dev environments.
pub const DEFAULT_MAX_ENTRIES: usize = 10_000;

/// In-process CRDT storage backed by a `BTreeMap`.
///
/// Useful for tests, edge/WASM environments, and development. Not persistent.
///
/// `max_entries` caps the number of stored CRDTs to prevent unbounded memory
/// growth. Defaults to [`DEFAULT_MAX_ENTRIES`]. `put()` returns
/// [`StorageError::CapacityExceeded`] when the limit is reached and the key
/// does not already exist (updates to existing keys are always allowed).
pub struct MemoryStore<V> {
    data: RwLock<BTreeMap<String, V>>,
    max_entries: usize,
}

impl<V> MemoryStore<V> {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
            max_entries: DEFAULT_MAX_ENTRIES,
        }
    }

    /// Create a store with a custom entry cap.
    pub fn with_capacity(max_entries: usize) -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
            max_entries,
        }
    }

    fn make_key(ns: &str, id: &str) -> String {
        format!("{ns}/{id}")
    }
}

impl<V> Default for MemoryStore<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> Store<V> for MemoryStore<V>
where
    V: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
{
    async fn get(&self, ns: &str, id: &str) -> Result<Option<V>> {
        let data = self.data.read().await;
        Ok(data.get(&Self::make_key(ns, id)).cloned())
    }

    async fn put(&self, ns: &str, id: &str, value: &V) -> Result<()> {
        let mut data = self.data.write().await;
        let key = Self::make_key(ns, id);
        // Allow updates to existing keys unconditionally.
        // Only enforce the cap for new insertions.
        if !data.contains_key(&key) && data.len() >= self.max_entries {
            return Err(StorageError::CapacityExceeded {
                max: self.max_entries,
                current: data.len(),
            });
        }
        data.insert(key, value.clone());
        Ok(())
    }

    async fn delete(&self, ns: &str, id: &str) -> Result<()> {
        let mut data = self.data.write().await;
        data.remove(&Self::make_key(ns, id));
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, V)>> {
        let data = self.data.read().await;
        let results = data
            .range(prefix.to_owned()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(results)
    }
}
