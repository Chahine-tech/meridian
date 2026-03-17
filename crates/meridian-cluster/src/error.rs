use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClusterError {
    #[cfg(feature = "transport-redis")]
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),

    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    #[error("invalid config: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, ClusterError>;
