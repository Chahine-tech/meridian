use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("codec error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),

    #[error("decode error: {0}")]
    Decode(#[from] rmp_serde::decode::Error),

    #[error("transport closed")]
    TransportClosed,

    #[error("not connected")]
    NotConnected,

    #[error("server error {code}: {message}")]
    Server { code: u16, message: String },

    #[error("crdt error: {0}")]
    Crdt(#[from] meridian_core::crdt::CrdtError),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("websocket error: {0}")]
    #[cfg(feature = "ws")]
    WebSocket(#[from] Box<tokio_tungstenite::tungstenite::Error>),
}
