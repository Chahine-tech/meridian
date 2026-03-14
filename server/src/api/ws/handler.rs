use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::{IntoResponse, Response},
};
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, info, warn};

use crate::{
    auth::ClaimsExt,
    crdt::registry::{apply_op, CrdtOp, CrdtType, CrdtValue},
    metrics,
    storage::Store,
};

use super::{
    protocol::{ClientMsg, ServerMsg},
    subscription::SubscriptionManager,
};

// ---------------------------------------------------------------------------
// AppState subset (injected via State — avoids circular dependency on AppState)
// ---------------------------------------------------------------------------

/// Minimal state needed by the WS handler.
/// The concrete `AppState` implements this interface.
pub trait WsState: Clone + Send + Sync + 'static {
    type S: Store;
    fn store(&self) -> &Self::S;
    fn subscriptions(&self) -> &Arc<SubscriptionManager>;
}

use crate::auth::claims::TokenClaims;

// ---------------------------------------------------------------------------
// WebSocket upgrade handler
// ---------------------------------------------------------------------------

/// Upgrades an HTTP request to a WebSocket connection.
///
/// URL: `GET /v1/namespaces/:ns/connect`
/// Auth: token extracted by middleware, injected as `ClaimsExt`.
///
/// The connection is rejected with a 403 if the token doesn't grant `read`
/// permission on the requested namespace.
pub async fn ws_upgrade_handler<S>(
    ws: WebSocketUpgrade,
    Path(ns): Path<String>,
    ClaimsExt(claims): ClaimsExt,
    State(state): State<S>,
) -> Response
where
    S: WsState,
{
    if claims.namespace != ns || !claims.can_read() {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }

    let client_id = claims.client_id;
    info!(ns, client_id, "WebSocket upgrade");

    metrics::ws_connected();
    ws.on_upgrade(move |socket| handle_socket(socket, ns, client_id, claims, state))
}

// ---------------------------------------------------------------------------
// Per-connection loop
// ---------------------------------------------------------------------------

async fn handle_socket<S: WsState>(mut socket: WebSocket, ns: String, client_id: u64, claims: TokenClaims, state: S) {
    // Subscribe to namespace broadcast channel.
    let mut broadcast_rx = state.subscriptions().subscribe(&ns);

    let mut op_seq: u64 = 0;

    loop {
        tokio::select! {
            // Incoming message from this client
            maybe_msg = socket.recv() => {
                match maybe_msg {
                    None => {
                        debug!(ns, client_id, "WebSocket closed by client");
                        break;
                    }
                    Some(Err(e)) => {
                        warn!(ns, client_id, error = %e, "WebSocket recv error");
                        break;
                    }
                    Some(Ok(msg)) => {
                        if !handle_client_message(&mut socket, &state, &ns, client_id, &claims, msg, &mut op_seq).await {
                            break;
                        }
                    }
                }
            }

            // Incoming broadcast from another client in the same namespace
            broadcast_result = broadcast_rx.recv() => {
                match broadcast_result {
                    Ok(server_msg) => {
                        if let Ok(bytes) = server_msg.to_msgpack()
                            && socket.send(Message::Binary(bytes.into())).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        warn!(ns, client_id, skipped = n, "broadcast lagged — disconnecting");
                        let err = ServerMsg::Error {
                            code: 1008,
                            message: format!("lagged: missed {n} messages"),
                        };
                        if let Ok(bytes) = err.to_msgpack() {
                            let _ = socket.send(Message::Binary(bytes.into())).await;
                        }
                        break;
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        }
    }

    metrics::ws_disconnected();
    debug!(ns, client_id, "WebSocket handler exiting");
}

/// Returns false if the connection should be closed.
async fn handle_client_message<S: WsState>(
    socket: &mut WebSocket,
    state: &S,
    ns: &str,
    client_id: u64,
    claims: &TokenClaims,
    msg: Message,
    seq: &mut u64,
) -> bool {
    let bytes = match msg {
        Message::Binary(b) => b,
        Message::Close(_) => return false,
        Message::Ping(p) => {
            let _ = socket.send(Message::Pong(p)).await;
            return true;
        }
        _ => return true, // ignore text frames
    };

    let client_msg = match ClientMsg::from_msgpack(&bytes) {
        Ok(m) => m,
        Err(e) => {
            warn!(error = %e, "malformed client message");
            send_error(socket, 400, "malformed message").await;
            return true;
        }
    };

    match client_msg {
        ClientMsg::Subscribe { crdt_id } => {
            debug!(ns, client_id, crdt_id, "subscribed");
            // No explicit per-crdt subscribe needed — all deltas in ns are broadcast together.
            let ack = ServerMsg::Ack { seq: *seq };
            if let Ok(b) = ack.to_msgpack() {
                let _ = socket.send(Message::Binary(b.into())).await;
            }
        }

        ClientMsg::Op { crdt_id, op_bytes } => {
            if !claims.can_write_key(&crdt_id) {
                send_error(socket, 403, "insufficient permissions for key").await;
                return true;
            }

            let op: CrdtOp = match rmp_serde::decode::from_slice(&op_bytes) {
                Ok(o) => o,
                Err(e) => {
                    warn!(error = %e, "malformed CrdtOp");
                    send_error(socket, 400, "malformed op").await;
                    return true;
                }
            };

            match apply_and_persist(state, ns, &crdt_id, op).await {
                Ok(Some(delta_bytes)) => {
                    *seq += 1;
                    metrics::record_op(ns, &crdt_id, "ws");
                    // Broadcast delta to all namespace subscribers (including this client).
                    let delta_msg = Arc::new(ServerMsg::Delta {
                        crdt_id: crdt_id.clone(),
                        delta_bytes,
                    });
                    state.subscriptions().publish(ns, delta_msg);

                    let ack = ServerMsg::Ack { seq: *seq };
                    if let Ok(b) = ack.to_msgpack() {
                        let _ = socket.send(Message::Binary(b.into())).await;
                    }
                }
                Ok(None) => {
                    // Op was a no-op (stale / idempotent) — still ack.
                    let ack = ServerMsg::Ack { seq: *seq };
                    if let Ok(b) = ack.to_msgpack() {
                        let _ = socket.send(Message::Binary(b.into())).await;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "apply_op failed");
                    send_error(socket, 500, "internal error").await;
                }
            }
        }

        ClientMsg::Sync { crdt_id, since_vc } => {
            if !claims.can_read_key(&crdt_id) {
                send_error(socket, 403, "insufficient permissions for key").await;
                return true;
            }

            let vc = match rmp_serde::decode::from_slice(&since_vc) {
                Ok(v) => v,
                Err(_) => {
                    send_error(socket, 400, "malformed vector clock").await;
                    return true;
                }
            };

            match state.store().get(ns, &crdt_id).await {
                Ok(Some(crdt)) => {
                    if let Ok(Some(delta_bytes)) = crdt.delta_since_msgpack(&vc) {
                        let msg = ServerMsg::Delta { crdt_id, delta_bytes };
                        if let Ok(b) = msg.to_msgpack() {
                            let _ = socket.send(Message::Binary(b.into())).await;
                        }
                    } else {
                        // Up to date — send empty ack
                        let ack = ServerMsg::Ack { seq: *seq };
                        if let Ok(b) = ack.to_msgpack() {
                            let _ = socket.send(Message::Binary(b.into())).await;
                        }
                    }
                }
                Ok(None) => send_error(socket, 404, "crdt not found").await,
                Err(e) => {
                    warn!(error = %e, "store.get failed");
                    send_error(socket, 500, "internal error").await;
                }
            }
        }
    }

    true
}

/// Load CRDT (or create if new), apply op, persist.
async fn apply_and_persist<S: WsState>(
    state: &S,
    ns: &str,
    crdt_id: &str,
    op: CrdtOp,
) -> Result<Option<Vec<u8>>, crate::storage::StorageError> {
    let crdt_type: CrdtType = op.crdt_type();
    let mut crdt = state
        .store()
        .get(ns, crdt_id)
        .await?
        .unwrap_or_else(|| CrdtValue::new(crdt_type));

    let delta_bytes = apply_op(&mut crdt, op)
        .map_err(crate::storage::StorageError::Crdt)?;

    if delta_bytes.is_some() {
        state.store().put(ns, crdt_id, &crdt).await?;
    }

    Ok(delta_bytes)
}

async fn send_error(socket: &mut WebSocket, code: u16, message: &str) {
    let err = ServerMsg::Error { code, message: message.to_owned() };
    if let Ok(b) = err.to_msgpack() {
        let _ = socket.send(Message::Binary(b.into())).await;
    }
}
