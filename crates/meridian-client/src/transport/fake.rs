/// In-memory fake transport for unit tests.
///
/// Usage:
/// ```rust,no_run
/// # use meridian_client::transport::fake::FakeTransport;
/// let (transport, handle) = FakeTransport::new();
/// // inject a server message and inspect sent messages
/// let sent = handle.sent();
/// ```
use async_trait::async_trait;
use meridian_core::protocol::{ClientMsg, ServerMsg};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, watch};

use super::{ConnectionState, Transport};
use crate::error::ClientError;

pub struct FakeHandle {
    sent: Arc<Mutex<Vec<ClientMsg>>>,
    inbox_tx: broadcast::Sender<ServerMsg>,
    state_tx: watch::Sender<ConnectionState>,
}

impl FakeHandle {
    /// Inject a server message as if the server sent it.
    pub fn inject(&self, msg: ServerMsg) {
        let _ = self.inbox_tx.send(msg);
    }

    /// Returns all messages sent by the client so far.
    pub fn sent(&self) -> Vec<ClientMsg> {
        self.sent.lock().expect("sent lock poisoned").clone()
    }

    /// Simulate a disconnect followed by reconnect.
    pub fn reconnect(&self) {
        let _ = self.state_tx.send(ConnectionState::Disconnected);
        let _ = self.state_tx.send(ConnectionState::Connected);
    }

    /// Set connection state directly.
    pub fn set_state(&self, state: ConnectionState) {
        let _ = self.state_tx.send(state);
    }
}

pub struct FakeTransport {
    sent: Arc<Mutex<Vec<ClientMsg>>>,
    inbox_tx: broadcast::Sender<ServerMsg>,
    state_tx: Arc<watch::Sender<ConnectionState>>,
    state_rx: watch::Receiver<ConnectionState>,
}

impl FakeTransport {
    pub fn new() -> (Arc<Self>, FakeHandle) {
        let (inbox_tx, _) = broadcast::channel(256);
        let (state_tx, state_rx) = watch::channel(ConnectionState::Connected);
        let sent = Arc::new(Mutex::new(Vec::new()));

        let transport = Arc::new(Self {
            sent: Arc::clone(&sent),
            inbox_tx: inbox_tx.clone(),
            state_tx: Arc::new(state_tx.clone()),
            state_rx,
        });

        let handle = FakeHandle {
            sent,
            inbox_tx,
            state_tx,
        };

        (transport, handle)
    }
}

impl Default for FakeTransport {
    fn default() -> Self {
        let (inbox_tx, _) = broadcast::channel(256);
        let (state_tx, state_rx) = watch::channel(ConnectionState::Connected);
        Self {
            sent: Arc::new(Mutex::new(Vec::new())),
            inbox_tx,
            state_tx: Arc::new(state_tx),
            state_rx,
        }
    }
}

#[async_trait]
impl Transport for FakeTransport {
    async fn send(&self, msg: ClientMsg) -> Result<(), ClientError> {
        self.sent.lock().expect("sent lock poisoned").push(msg);
        Ok(())
    }

    fn subscribe_incoming(&self) -> broadcast::Receiver<ServerMsg> {
        self.inbox_tx.subscribe()
    }

    fn state_watch(&self) -> watch::Receiver<ConnectionState> {
        self.state_rx.clone()
    }

    async fn close(&self) {
        let _ = self.state_tx.send(ConnectionState::Closing);
    }
}
