use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[cfg(feature = "storage-sled")]
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),

    #[cfg(feature = "storage-postgres")]
    #[error("postgres error: {0}")]
    Postgres(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),

    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    /// Codec error from a higher-level layer (e.g. CRDT deserialization).
    #[error("codec error: {0}")]
    Codec(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("background task panicked")]
    TaskJoin,

    #[error("key not found: {0}")]
    NotFound(String),

    #[error("invalid key format: {0}")]
    InvalidKey(String),

    #[error("internal lock poisoned")]
    LockPoisoned,
}

pub type Result<T> = std::result::Result<T, StorageError>;
