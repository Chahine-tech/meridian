use async_trait::async_trait;
use meridian_core::protocol::{ClientMsg, ServerMsg};
use tokio::sync::{broadcast, watch};

use crate::error::ClientError;

pub mod fake;
#[cfg(feature = "ws")]
pub mod ws;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Closing,
}

/// Abstraction over the WebSocket transport.
/// Inject `FakeTransport` in tests, `WsTransport` in production.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send a message to the server. Queues it if not connected.
    async fn send(&self, msg: ClientMsg) -> Result<(), ClientError>;

    /// Subscribe to incoming server messages.
    /// The broadcast channel holds the last 256 messages.
    fn subscribe_incoming(&self) -> broadcast::Receiver<ServerMsg>;

    /// Watch the connection state for reconnect events.
    fn state_watch(&self) -> watch::Receiver<ConnectionState>;

    /// Gracefully close the connection.
    async fn close(&self);
}
