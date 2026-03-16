pub mod error;
pub mod store;
pub mod wal_backend;
pub(crate) mod utils;

#[cfg(feature = "storage-sled")]
pub mod sled;

#[cfg(feature = "storage-memory")]
pub mod memory;

#[cfg(feature = "storage-postgres")]
pub mod postgres;

// Re-exports for convenience
pub use error::{Result, StorageError};
pub use store::Store;
pub use wal_backend::{WalBackend, WalEntry};

#[cfg(feature = "storage-sled")]
pub use sled::{SledStore, SledWal};

#[cfg(feature = "storage-memory")]
pub use memory::{MemoryStore, NoopWal};

#[cfg(feature = "storage-postgres")]
pub use postgres::{PgStore, PgWal};
