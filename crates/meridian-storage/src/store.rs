use serde::{Deserialize, Serialize};

use super::error::Result;

/// Abstraction over persistent CRDT storage.
///
/// `V` is the CRDT value type — callers supply their own (e.g. `CrdtValue`
/// from `meridian-server`). `V` must be serializable/deserializable via
/// msgpack so backends can encode it without knowing the concrete type.
///
/// Implementations must be `Send + Sync` (used behind `Arc` in `AppState`).
///
/// Key space: `{namespace}/{crdt_id}` — both components are validated
/// to be `[a-z0-9_-]{1,64}` before reaching the store.
pub trait Store<V>: Send + Sync + 'static
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    /// Load a CRDT by (namespace, id). Returns `None` if not found.
    fn get(
        &self,
        ns: &str,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<V>>> + Send;

    /// Persist a CRDT (full state snapshot, overwrites previous).
    fn put(
        &self,
        ns: &str,
        id: &str,
        value: &V,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Delete a CRDT entry.
    fn delete(
        &self,
        ns: &str,
        id: &str,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Scan all CRDTs whose key starts with `prefix` (e.g. `"my_namespace/"`).
    /// Returns `(full_key, value)` pairs.
    fn scan_prefix(
        &self,
        prefix: &str,
    ) -> impl std::future::Future<Output = Result<Vec<(String, V)>>> + Send;

    /// Flush pending writes to durable storage. No-op for backends that
    /// don't buffer writes (e.g. `MemoryStore`).
    fn flush(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}
