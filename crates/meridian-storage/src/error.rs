use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[cfg(feature = "storage-sled")]
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),

    #[cfg(feature = "storage-postgres")]
    #[error("postgres error: {0}")]
    Postgres(#[from] sqlx::Error),

    #[cfg(feature = "storage-redis")]
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

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

    #[error("store capacity exceeded: max {max} entries, currently {current}")]
    CapacityExceeded { max: usize, current: usize },

    #[cfg(feature = "wal-archive-s3")]
    #[error("S3 error: {0}")]
    S3(Box<aws_sdk_s3::Error>),

    #[cfg(feature = "wal-archive-s3")]
    #[error("S3 archive upload failed: {message}")]
    S3Upload { message: String },

    #[cfg(feature = "wal-archive-s3")]
    #[error("S3 archive restore failed: {message}")]
    S3Restore { message: String },
}

pub type Result<T> = std::result::Result<T, StorageError>;

#[cfg(feature = "wal-archive-s3")]
impl From<aws_sdk_s3::Error> for StorageError {
    fn from(e: aws_sdk_s3::Error) -> Self {
        StorageError::S3(Box::new(e))
    }
}
