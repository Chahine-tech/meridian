use crate::crdt::registry::CrdtValue;

use super::error::Result;

/// Abstraction over persistent CRDT storage.
///
/// Implementations must be `Send + Sync` (used behind `Arc` in `AppState`).
/// All methods are `async` for abstraction; the concrete `SledStore` bridges
/// sled's sync API via `tokio::task::spawn_blocking`.
///
/// Key space: `{namespace}/{crdt_id}` — both components are validated
/// to be `[a-z0-9_-]{1,64}` before reaching the store.
pub trait Store: Send + Sync + 'static {
    /// Load a CRDT by (namespace, id). Returns `None` if not found.
    fn get(
        &self,
        ns: &str,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<CrdtValue>>> + Send;

    /// Persist a CRDT (full state snapshot, overwrites previous).
    fn put(
        &self,
        ns: &str,
        id: &str,
        value: &CrdtValue,
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
    ) -> impl std::future::Future<Output = Result<Vec<(String, CrdtValue)>>> + Send;
}
