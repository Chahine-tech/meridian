use std::collections::BTreeMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::{
    error::{Result, StorageError},
    store::Store,
};

// ---------------------------------------------------------------------------
// MemoryStore
// ---------------------------------------------------------------------------

/// In-process CRDT storage backed by a `BTreeMap`.
///
/// Useful for tests, edge/WASM environments, and development. Not persistent.
pub struct MemoryStore<V> {
    data: RwLock<BTreeMap<String, V>>,
}

impl<V> MemoryStore<V> {
    pub fn new() -> Self {
        Self { data: RwLock::new(BTreeMap::new()) }
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
        let key = Self::make_key(ns, id);
        let data = self.data.read().map_err(|_| StorageError::LockPoisoned)?;
        Ok(data.get(&key).cloned())
    }

    async fn put(&self, ns: &str, id: &str, value: &V) -> Result<()> {
        let key = Self::make_key(ns, id);
        let mut data = self.data.write().map_err(|_| StorageError::LockPoisoned)?;
        data.insert(key, value.clone());
        Ok(())
    }

    async fn delete(&self, ns: &str, id: &str) -> Result<()> {
        let key = Self::make_key(ns, id);
        let mut data = self.data.write().map_err(|_| StorageError::LockPoisoned)?;
        data.remove(&key);
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, V)>> {
        let data = self.data.read().map_err(|_| StorageError::LockPoisoned)?;
        let results = data
            .range(prefix.to_owned()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(results)
    }
}
