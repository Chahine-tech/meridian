use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use meridian_core::protocol::{ClientMsg, ServerMsg};
use tokio::sync::{broadcast, watch, Mutex};
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
};
use tracing::{debug, info, warn};

use super::{ConnectionState, Transport};
use crate::{codec, error::ClientError, op_queue::OpQueue};

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

pub struct WsTransport {
    inbox_tx:   broadcast::Sender<ServerMsg>,
    outbox_tx:  tokio::sync::mpsc::UnboundedSender<ClientMsg>,
    state_tx:   Arc<watch::Sender<ConnectionState>>,
    state_rx:   watch::Receiver<ConnectionState>,
    op_queue:   Arc<OpQueue>,
    // Held to prevent close until explicitly called
    _close_tx:  tokio::sync::oneshot::Sender<()>,
}

impl WsTransport {
    /// Connect to the Meridian WebSocket endpoint.
    ///
    /// `url`       — base server URL, e.g. `"ws://localhost:3000"`
    /// `namespace` — Meridian namespace
    /// `token`     — auth token string
    pub async fn connect(
        url: &str,
        namespace: &str,
        token: &str,
    ) -> Result<Arc<Self>, ClientError> {
        let ws_url = format!("{url}/v1/namespaces/{namespace}/connect?token={token}");

        let (inbox_tx, _) = broadcast::channel(256);
        let (outbox_tx, outbox_rx) = tokio::sync::mpsc::unbounded_channel();
        let (state_tx, state_rx) = watch::channel(ConnectionState::Connecting);
        let (close_tx, close_rx) = tokio::sync::oneshot::channel::<()>();
        let op_queue = Arc::new(OpQueue::new());

        let transport = Arc::new(Self {
            inbox_tx: inbox_tx.clone(),
            outbox_tx,
            state_tx: Arc::new(state_tx),
            state_rx,
            op_queue: Arc::clone(&op_queue),
            _close_tx: close_tx,
        });

        // Spawn the connection loop
        let state_tx_clone = Arc::clone(&transport.state_tx);
        let outbox_rx = Arc::new(Mutex::new(outbox_rx));
        tokio::spawn(connection_loop(
            ws_url,
            inbox_tx,
            outbox_rx,
            state_tx_clone,
            op_queue,
            close_rx,
        ));

        Ok(transport)
    }
}

async fn connection_loop(
    url: String,
    inbox_tx: broadcast::Sender<ServerMsg>,
    outbox_rx: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<ClientMsg>>>,
    state_tx: Arc<watch::Sender<ConnectionState>>,
    op_queue: Arc<OpQueue>,
    mut close_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut backoff = INITIAL_BACKOFF;

    loop {
        let _ = state_tx.send(ConnectionState::Connecting);

        match connect_async(&url).await {
            Ok((ws, _)) => {
                backoff = INITIAL_BACKOFF;
                let _ = state_tx.send(ConnectionState::Connected);
                info!(url = %url, "WebSocket connected");

                // Drain pending ops from queue on reconnect
                let pending = op_queue.drain();
                if !pending.is_empty() {
                    debug!(count = pending.len(), "replaying queued ops after reconnect");
                }

                let (mut sink, mut stream) = ws.split();

                // Send pending ops first
                for msg in pending {
                    match codec::encode(&msg) {
                        Ok(bytes) => {
                            if let Err(e) = sink.send(Message::Binary(bytes.into())).await {
                                warn!(error = %e, "failed to replay queued op");
                                break;
                            }
                        }
                        Err(e) => warn!(error = %e, "failed to encode queued op"),
                    }
                }

                // Outbox drain loop
                let sink = Arc::new(Mutex::new(sink));
                let sink_clone = Arc::clone(&sink);
                let mut rx = outbox_rx.lock().await;

                loop {
                    tokio::select! {
                        // Outgoing message
                        Some(msg) = rx.recv() => {
                            match codec::encode(&msg) {
                                Ok(bytes) => {
                                    let mut s = sink_clone.lock().await;
                                    if let Err(e) = s.send(Message::Binary(bytes.into())).await {
                                        warn!(error = %e, "send failed — reconnecting");
                                        op_queue.push(msg);
                                        break;
                                    }
                                }
                                Err(e) => warn!(error = %e, "encode failed"),
                            }
                        }

                        // Incoming message
                        msg = stream.next() => {
                            match msg {
                                Some(Ok(Message::Binary(bytes))) => {
                                    match codec::decode(&bytes) {
                                        Ok(server_msg) => { let _ = inbox_tx.send(server_msg); }
                                        Err(e) => warn!(error = %e, "decode failed"),
                                    }
                                }
                                Some(Ok(Message::Close(_))) | None => {
                                    info!("WebSocket closed by server — reconnecting");
                                    break;
                                }
                                Some(Err(e)) => {
                                    warn!(error = %e, "WebSocket error — reconnecting");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        // Close signal
                        _ = &mut close_rx => {
                            let _ = state_tx.send(ConnectionState::Closing);
                            let _ = sink.lock().await.close().await;
                            return;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, backoff_ms = backoff.as_millis(), "connection failed — retrying");
            }
        }

        let _ = state_tx.send(ConnectionState::Disconnected);

        // Jitter: ±25%
        let jitter_factor = 0.75 + rand::random::<f64>() * 0.5;
        let sleep_ms = (backoff.as_millis() as f64 * jitter_factor) as u64;
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

#[async_trait]
impl Transport for WsTransport {
    async fn send(&self, msg: ClientMsg) -> Result<(), ClientError> {
        if *self.state_tx.borrow() != ConnectionState::Connected {
            self.op_queue.push(msg);
            return Ok(());
        }
        self.outbox_tx.send(msg).map_err(|_| ClientError::TransportClosed)
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
