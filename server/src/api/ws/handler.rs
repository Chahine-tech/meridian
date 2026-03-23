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
    crdt::{
        clock::now_ms,
        ops::{apply_op_atomic, ApplyError},
        registry::CrdtOp,
    },
    metrics,
    storage::{CrdtStore, Store},
    webhooks::{WebhookDispatcher, WebhookEvent},
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
    type S: CrdtStore;
    fn store(&self) -> &Self::S;
    fn subscriptions(&self) -> &Arc<SubscriptionManager>;
    fn webhooks(&self) -> Option<&WebhookDispatcher>;

    #[cfg(any(feature = "cluster", feature = "cluster-http"))]
    fn cluster(&self) -> Option<&Arc<meridian_cluster::ClusterHandle>> {
        None
    }
}

use crate::auth::TokenClaims;

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
            let ack = ServerMsg::Ack { seq: *seq, client_seq: None };
            if let Ok(b) = ack.to_msgpack() {
                let _ = socket.send(Message::Binary(b.into())).await;
            }
        }

        ClientMsg::Op { crdt_id, op_bytes, ttl_ms, client_seq } => {
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

            match apply_op_atomic(state.store(), ns, &crdt_id, op, ttl_ms).await {
                Ok(Some(delta_bytes)) => {
                    *seq += 1;
                    metrics::record_op(ns, &crdt_id, "ws");

                    if let Some(dispatcher) = state.webhooks() {
                        dispatcher.send(WebhookEvent {
                            ns: ns.to_owned(),
                            crdt_id: crdt_id.clone(),
                            source: "ws".into(),
                            timestamp_ms: now_ms(),
                        });
                    }

                    // Broadcast delta to all namespace subscribers (including this client).
                    let delta_msg = Arc::new(ServerMsg::Delta {
                        crdt_id: crdt_id.clone(),
                        delta_bytes: delta_bytes.clone().into(),
                    });
                    state.subscriptions().publish(ns, delta_msg);

                    // Fan-out delta to peer nodes via cluster transport (best-effort).
                    #[cfg(any(feature = "cluster", feature = "cluster-http"))]
                    if let Some(cluster) = state.cluster() {
                        cluster.on_delta(ns, &crdt_id, bytes::Bytes::from(delta_bytes)).await;
                    }

                    let ack = ServerMsg::Ack { seq: *seq, client_seq };
                    if let Ok(b) = ack.to_msgpack() {
                        let _ = socket.send(Message::Binary(b.into())).await;
                    }
                }
                Ok(None) => {
                    // Op was a no-op (stale / idempotent) — still ack.
                    let ack = ServerMsg::Ack { seq: *seq, client_seq };
                    if let Ok(b) = ack.to_msgpack() {
                        let _ = socket.send(Message::Binary(b.into())).await;
                    }
                }
                Err(ApplyError::Crdt(e)) => {
                    warn!(error = %e, "crdt op rejected");
                    send_error(socket, 400, &e.to_string()).await;
                }
                Err(ApplyError::Storage(e)) => {
                    warn!(error = %e, "store failed during apply");
                    send_error(socket, 500, "internal error").await;
                }
            }
        }

        ClientMsg::AwarenessUpdate { key, data } => {
            // Stateless fan-out — no persistence, no ack.
            let broadcast_msg = Arc::new(ServerMsg::AwarenessBroadcast {
                client_id: 0,
                key,
                data,
            });
            state.subscriptions().publish(ns, broadcast_msg);
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
                        let msg = ServerMsg::Delta { crdt_id, delta_bytes: delta_bytes.into() };
                        if let Ok(b) = msg.to_msgpack() {
                            let _ = socket.send(Message::Binary(b.into())).await;
                        }
                    } else {
                        // Up to date — send empty ack
                        let ack = ServerMsg::Ack { seq: *seq, client_seq: None };
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


async fn send_error(socket: &mut WebSocket, code: u16, message: &str) {
    let err = ServerMsg::Error { code, message: message.to_owned() };
    if let Ok(b) = err.to_msgpack() {
        let _ = socket.send(Message::Binary(b.into())).await;
    }
}
