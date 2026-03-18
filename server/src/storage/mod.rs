// Re-export the storage abstraction layer from the dedicated crate.
// Server-internal code uses `crate::storage::*` unchanged.
pub use meridian_storage::{Result, Store, StorageError, WalBackend, WalEntry};

#[cfg(feature = "storage-sled")]
pub use meridian_storage::{SledStore, SledWal};

#[cfg(feature = "storage-memory")]
pub use meridian_storage::{MemoryStore, NoopWal};

#[cfg(feature = "storage-postgres")]
pub use meridian_storage::{PgStore, PgWal};

// ---------------------------------------------------------------------------
// CrdtStore — Store<CrdtValue> convenience bound
// ---------------------------------------------------------------------------
// Blanket-implemented for any `T: Store<CrdtValue>`.
// Files that call Store methods must also import `crate::storage::Store`
// to bring the trait methods into scope (standard Rust trait import rules).

use crate::crdt::registry::CrdtValue;

pub trait CrdtStore: Store<CrdtValue> {}
impl<T: Store<CrdtValue>> CrdtStore for T {}

// `delete_expired` is a default method on `Store<V>` (in meridian-storage).
// `CrdtStore` inherits it via `Store<CrdtValue>` — no extra trait needed.
