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

    /// Atomic read-merge-write: load the current value, apply `merge_fn(existing, new)`,
    /// and persist the result in a single atomic operation.
    ///
    /// This is the safe way to update a shared CRDT when multiple nodes or
    /// writers may write to the same key concurrently (e.g. shared PostgreSQL).
    /// The default implementation is a non-atomic read-then-write — backends
    /// that support transactions (e.g. `PgStore`) override this with a
    /// `SELECT FOR UPDATE` inside a transaction.
    ///
    /// Use this instead of `put()` whenever concurrent writes are possible.
    /// If you need to extract a value computed during the merge (e.g. a delta),
    /// use [`merge_put_with`](Store::merge_put_with) instead.
    fn merge_put<F>(
        &self,
        ns: &str,
        id: &str,
        new_value: V,
        merge_fn: F,
    ) -> impl std::future::Future<Output = Result<()>> + Send
    where
        F: FnOnce(Option<V>, V) -> V + Send,
    {
        async move {
            let existing = self.get(ns, id).await?;
            let merged = merge_fn(existing, new_value);
            self.put(ns, id, &merged).await
        }
    }

    /// Delete all entries whose TTL has expired (i.e. `expires_at_ms < before_ms`).
    ///
    /// Returns the `(namespace, crdt_id)` pairs that were removed so the caller
    /// can broadcast tombstone deltas via the subscription manager.
    ///
    /// The default implementation is a no-op (returns an empty list). Backends
    /// that store an indexed `expires_at_ms` column (e.g. `PgStore`) should
    /// override this with a single efficient DELETE query.
    fn delete_expired(
        &self,
        before_ms: u64,
    ) -> impl std::future::Future<Output = Result<Vec<(String, String)>>> + Send {
        let _ = before_ms;
        async { Ok(vec![]) }
    }

    /// Like [`merge_put`](Store::merge_put), but the closure also returns a
    /// side-output `R` (e.g. computed delta bytes) alongside the merged value.
    ///
    /// The closure signature is `FnOnce(existing: Option<V>, new: V) -> (merged: V, output: R)`.
    /// The merged value is persisted; `R` is returned to the caller.
    ///
    /// Backends that override `merge_put` for atomicity (e.g. `PgStore`) should
    /// also override this method so the same transaction guarantee applies.
    fn merge_put_with<F, R>(
        &self,
        ns: &str,
        id: &str,
        new_value: V,
        merge_fn: F,
    ) -> impl std::future::Future<Output = Result<R>> + Send
    where
        F: FnOnce(Option<V>, V) -> (V, R) + Send,
        R: Send,
    {
        async move {
            let existing = self.get(ns, id).await?;
            let (merged, result) = merge_fn(existing, new_value);
            self.put(ns, id, &merged).await?;
            Ok(result)
        }
    }

    /// Flush pending writes to durable storage. No-op for backends that
    /// don't buffer writes (e.g. `MemoryStore`).
    fn flush(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Like [`merge_put_with`](Store::merge_put_with) but also sets an optional
    /// `expires_at_ms` column for TTL-based GC.
    ///
    /// The default implementation ignores `expires_at_ms` (safe for backends
    /// that don't support it). `PgStore` overrides this to persist the expiry
    /// timestamp in the dedicated indexed column.
    fn merge_put_with_expiry<F, R>(
        &self,
        ns: &str,
        id: &str,
        new_value: V,
        expires_at_ms: Option<u64>,
        merge_fn: F,
    ) -> impl std::future::Future<Output = Result<R>> + Send
    where
        F: FnOnce(Option<V>, V) -> (V, R) + Send,
        R: Send,
    {
        // Default: ignore `expires_at_ms`, delegate to the standard path.
        let _ = expires_at_ms;
        self.merge_put_with(ns, id, new_value, merge_fn)
    }
}
