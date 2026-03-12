use thiserror::Error;

use crate::crdt::CrdtError;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),

    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    #[error("crdt codec error: {0}")]
    Crdt(#[from] CrdtError),

    #[error("background task panicked")]
    TaskJoin,

    #[error("key not found: {0}")]
    NotFound(String),

    #[error("invalid key format: {0}")]
    InvalidKey(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;
