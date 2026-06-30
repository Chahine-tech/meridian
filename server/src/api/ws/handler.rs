use std::sync::Arc;

use axum::{
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{IntoResponse, Response},
};
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, info, warn};

use meridian_core::protocol::BatchItem;

use meridian_core::query::{AggregateOp, WhereClause, infer_crdt_type};

use crate::{
    api::handlers::{ExecuteQueryError, query::execute_query},
    auth::ClaimsExt,
    crdt::{
        HybridLogicalClock,
        clock::now_ms,
        ops::{ApplyError, UndoLwwResult, apply_op_atomic, apply_undo_lww_atomic},
        registry::{CrdtOp, validate_clock_drift},
    },
    metrics,
    storage::{CrdtStore, Store},
    webhooks::{WebhookDispatcher, WebhookEvent},
};

use super::{
    protocol::{ClientMsg, ServerMsg},
    query_registry::QueryRegistry,
    subscription::SubscriptionManager,
};

/// Verify an Ed25519 signature over `op_bytes` using the 32-byte public key
/// stored in the token claims. Returns `true` if the signature is valid.
fn verify_op_sig(pubkey_bytes: &[u8], op_bytes: &[u8], sig_bytes: &[u8]) -> bool {
    use ed25519_dalek::{Signature, VerifyingKey};
    let Ok(pk_arr): Result<[u8; 32], _> = pubkey_bytes.try_into() else {
        return false;
    };
    let Ok(vk) = VerifyingKey::from_bytes(&pk_arr) else {
        return false;
    };
    let Ok(sig_arr): Result<[u8; 64], _> = sig_bytes.try_into() else {
        return false;
    };
    let sig = Signature::from_bytes(&sig_arr);
    vk.verify_strict(op_bytes, &sig).is_ok()
}

// AppState subset (injected via State — avoids circular dependency on AppState)

/// Minimal state needed by the WS handler.
/// The concrete `AppState` implements this interface.
pub trait WsState: Clone + Send + Sync + 'static {
    type S: CrdtStore;
    fn store(&self) -> &Self::S;
    fn subscriptions(&self) -> &Arc<SubscriptionManager>;
    fn client_registry(&self) -> &Arc<crate::crdt::ClientRegistry>;
    fn webhooks(&self) -> Option<&WebhookDispatcher>;

    #[cfg(any(feature = "cluster", feature = "cluster-http", feature = "pg-sync"))]
    fn cluster(&self) -> Option<&Arc<meridian_cluster::ClusterHandle>> {
        None
    }
}

use crate::auth::TokenClaims;

// WebSocket upgrade handler

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

// Per-connection loop

async fn handle_socket<S: WsState>(
    mut socket: WebSocket,
    ns: String,
    client_id: u64,
    claims: TokenClaims,
    state: S,
) {
    // Subscribe to namespace broadcast channel.
    let mut broadcast_rx = state.subscriptions().subscribe(&ns);

    // The client starts unregistered from the ClientRegistry — it will be added
    // on its first Sync, once we know what it has seen. This prevents new
    // connections that never sync from blocking GC indefinitely.

    let mut op_seq: u64 = 0;
    let mut query_registry = QueryRegistry::default();

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
                        if !handle_client_message(&mut socket, &state, &ns, client_id, &claims, msg, &mut op_seq, &mut query_registry).await {
                            break;
                        }
                    }
                }
            }

            // Incoming broadcast from another client in the same namespace
            broadcast_result = broadcast_rx.recv() => {
                match broadcast_result {
                    Ok(server_msg) => {
                        // Forward the delta to the client.
                        if let Ok(bytes) = server_msg.to_msgpack()
                            && socket.send(Message::Binary(bytes.into())).await.is_err()
                        {
                            break;
                        }

                        // Re-execute any live queries that match the changed CRDT.
                        if !query_registry.is_empty()
                            && let ServerMsg::Delta { crdt_id, .. } = server_msg.as_ref()
                        {
                            let bare_id = crdt_id.trim_start_matches(&format!("{ns}/"));
                            // Infer the CRDT type from the ID prefix so we can skip
                            // queries that have an incompatible type filter.
                            let changed_type = infer_crdt_type(bare_id)
                                .unwrap_or(crate::crdt::registry::CrdtType::GCounter);
                            let matching: Vec<(String, _)> = query_registry
                                .matching(bare_id, changed_type)
                                .map(|(id, payload)| (id.to_owned(), payload.clone()))
                                .collect();

                            for (query_id, payload) in matching {
                                push_live_query_result(
                                    &mut socket,
                                    state.store(),
                                    &ns,
                                    &query_id,
                                    &payload,
                                )
                                .await;
                            }
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

    // Remove this client from the GC registry so it no longer blocks tombstone removal.
    state.client_registry().unregister(&ns, client_id);

    metrics::ws_disconnected();
    debug!(ns, client_id, "WebSocket handler exiting");
}

/// Execute a live query and push the result to the socket.
async fn push_live_query_result<S: CrdtStore>(
    socket: &mut WebSocket,
    store: &S,
    ns: &str,
    query_id: &str,
    payload: &meridian_core::protocol::LiveQueryPayload,
) {
    let filter: Option<WhereClause> = payload.filter.as_ref().map(|f| WhereClause {
        contains: f.contains.clone(),
        updated_after: f.updated_after,
    });

    let agg_op: AggregateOp = match payload.aggregate.parse() {
        Ok(op) => op,
        Err(_) => {
            warn!(query_id, aggregate = %payload.aggregate, "live query: unknown aggregate op");
            return;
        }
    };

    match execute_query(
        store,
        ns,
        &payload.from,
        payload.crdt_type.as_deref(),
        agg_op,
        filter.as_ref(),
    )
    .await
    {
        Ok(outcome) => {
            let msg = ServerMsg::QueryResult {
                query_id: query_id.to_owned(),
                value: outcome.value,
                matched: outcome.matched,
            };
            if let Ok(b) = msg.to_msgpack() {
                let _ = socket.send(Message::Binary(b.into())).await;
            }
        }
        Err(e) => {
            let detail = match &e {
                ExecuteQueryError::UnknownCrdtType(s) => s.as_str(),
                ExecuteQueryError::IncompatibleAggregate { aggregate, .. } => aggregate.as_str(),
                ExecuteQueryError::Storage(s) => s.as_str(),
            };
            warn!(query_id, error = detail, "live query execution failed");
        }
    }
}

/// Returns false if the connection should be closed.
#[allow(clippy::too_many_arguments)]
async fn handle_client_message<S: WsState>(
    socket: &mut WebSocket,
    state: &S,
    ns: &str,
    client_id: u64,
    claims: &TokenClaims,
    msg: Message,
    seq: &mut u64,
    query_registry: &mut QueryRegistry,
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
            let ack = ServerMsg::Ack {
                seq: *seq,
                client_seq: None,
            };
            if let Ok(b) = ack.to_msgpack() {
                let _ = socket.send(Message::Binary(b.into())).await;
            }
        }

        ClientMsg::Op {
            crdt_id,
            op_bytes,
            ttl_ms,
            client_seq,
            sig,
        } => {
            let op: CrdtOp = match rmp_serde::decode::from_slice(&op_bytes) {
                Ok(o) => o,
                Err(e) => {
                    warn!(error = %e, "malformed CrdtOp");
                    send_error(socket, 400, "malformed op").await;
                    return true;
                }
            };

            // BFT signature verification: if the token embeds a client public key,
            // every op must carry a valid Ed25519 signature over op_bytes.
            if let Some(pubkey) = &claims.client_pubkey {
                match &sig {
                    Some(sig_bytes)
                        if verify_op_sig(pubkey.as_ref(), &op_bytes, sig_bytes.as_ref()) => {}
                    Some(_) => {
                        warn!(client_id, crdt_id, "invalid BFT op signature");
                        send_error(socket, 401, "invalid op signature").await;
                        return true;
                    }
                    None => {
                        warn!(client_id, crdt_id, "missing BFT op signature");
                        send_error(socket, 401, "op signature required").await;
                        return true;
                    }
                }
            }

            // Single permission check: V1 uses key-level glob, V2 uses op-level mask.
            if !claims.can_write_key_op(&crdt_id, op.op_mask()) {
                send_error(socket, 403, "op not permitted by token").await;
                return true;
            }

            match apply_op_atomic(state.store(), ns, &crdt_id, op, ttl_ms).await {
                Ok((Some(delta_bytes), conflict)) => {
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
                        cluster
                            .on_delta(ns, &crdt_id, bytes::Bytes::from(delta_bytes))
                            .await;
                    }

                    // Privately notify the sender if their op produced a conflict
                    // (e.g. Tree move cycle detected alongside the delta).
                    if let Some(kind) = conflict {
                        let msg = ServerMsg::Conflict { crdt_id: crdt_id.clone(), kind };
                        if let Ok(b) = msg.to_msgpack() {
                            let _ = socket.send(Message::Binary(b.into())).await;
                        }
                    }

                    let ack = ServerMsg::Ack { seq: *seq, client_seq };
                    if let Ok(b) = ack.to_msgpack() {
                        let _ = socket.send(Message::Binary(b.into())).await;
                    }
                }
                Ok((None, conflict)) => {
                    // Op was a no-op (stale / idempotent) — ack and optionally notify conflict.
                    if let Some(kind) = conflict {
                        let msg = ServerMsg::Conflict { crdt_id: crdt_id.clone(), kind };
                        if let Ok(b) = msg.to_msgpack() {
                            let _ = socket.send(Message::Binary(b.into())).await;
                        }
                    }
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

        ClientMsg::BatchOp { ops, client_seq } => {
            if ops.is_empty() {
                send_error(socket, 400, "batch must not be empty").await;
                return true;
            }

            let server_now = now_ms();

            struct Parsed {
                crdt_id: String,
                op: CrdtOp,
                ttl_ms: Option<u64>,
            }
            let mut parsed: Vec<Parsed> = Vec::with_capacity(ops.len());
            for BatchItem {
                crdt_id,
                op_bytes,
                ttl_ms,
                sig,
            } in ops
            {
                // BFT signature check per-item in the batch.
                if let Some(pubkey) = &claims.client_pubkey {
                    match &sig {
                        Some(sig_bytes)
                            if verify_op_sig(pubkey.as_ref(), &op_bytes, sig_bytes.as_ref()) => {}
                        Some(_) => {
                            warn!(client_id, crdt_id, "invalid BFT sig in batch");
                            send_error(
                                socket,
                                401,
                                &format!("batch: invalid op signature for {crdt_id}"),
                            )
                            .await;
                            return true;
                        }
                        None => {
                            warn!(client_id, crdt_id, "missing BFT sig in batch");
                            send_error(
                                socket,
                                401,
                                &format!("batch: op signature required for {crdt_id}"),
                            )
                            .await;
                            return true;
                        }
                    }
                }

                let op: CrdtOp = match rmp_serde::decode::from_slice(&op_bytes) {
                    Ok(o) => o,
                    Err(e) => {
                        warn!(error = %e, crdt_id, "batch: malformed CrdtOp");
                        send_error(socket, 400, &format!("batch: malformed op for {crdt_id}"))
                            .await;
                        return true;
                    }
                };
                if let Err(e) = validate_clock_drift(&op, server_now) {
                    send_error(socket, 400, &format!("batch: {crdt_id}: {e}")).await;
                    return true;
                }
                // Single permission check: V1 uses key-level glob, V2 uses op-level mask.
                if !claims.can_write_key_op(&crdt_id, op.op_mask()) {
                    send_error(
                        socket,
                        403,
                        &format!("batch: op not permitted on {crdt_id}"),
                    )
                    .await;
                    return true;
                }
                parsed.push(Parsed {
                    crdt_id,
                    op,
                    ttl_ms,
                });
            }

            let mut delta_count = 0usize;
            let mut broadcast_msgs: Vec<Arc<ServerMsg>> = Vec::new();
            #[cfg(any(feature = "cluster", feature = "cluster-http"))]
            let mut cluster_deltas: Vec<(String, bytes::Bytes)> = Vec::new();

            let mut conflict_msgs: Vec<ServerMsg> = Vec::new();

            for item in parsed {
                match apply_op_atomic(state.store(), ns, &item.crdt_id, item.op, item.ttl_ms).await
                {
                    Ok((Some(delta_bytes), conflict)) => {
                        *seq += 1;
                        delta_count += 1;
                        metrics::record_op(ns, &item.crdt_id, "ws_batch");

                        if let Some(dispatcher) = state.webhooks() {
                            dispatcher.send(WebhookEvent {
                                ns: ns.to_owned(),
                                crdt_id: item.crdt_id.clone(),
                                source: "ws_batch".into(),
                                timestamp_ms: server_now,
                            });
                        }

                        #[cfg(any(feature = "cluster", feature = "cluster-http"))]
                        cluster_deltas.push((
                            item.crdt_id.clone(),
                            bytes::Bytes::from(delta_bytes.clone()),
                        ));

                        broadcast_msgs.push(Arc::new(ServerMsg::Delta {
                            crdt_id: item.crdt_id.clone(),
                            delta_bytes: delta_bytes.into(),
                        }));

                        if let Some(kind) = conflict {
                            conflict_msgs.push(ServerMsg::Conflict { crdt_id: item.crdt_id, kind });
                        }
                    }
                    Ok((None, conflict)) => {
                        // no-op — still record conflict if present
                        if let Some(kind) = conflict {
                            conflict_msgs.push(ServerMsg::Conflict { crdt_id: item.crdt_id, kind });
                        }
                    }
                    Err(ApplyError::Crdt(e)) => {
                        warn!(error = %e, crdt_id = item.crdt_id, "batch: crdt op rejected");
                        send_error(socket, 400, &e.to_string()).await;
                        return true;
                    }
                    Err(ApplyError::Storage(e)) => {
                        warn!(error = %e, crdt_id = item.crdt_id, "batch: store failed");
                        send_error(socket, 500, "internal error").await;
                        return true;
                    }
                }
            }

            for msg in broadcast_msgs {
                state.subscriptions().publish(ns, msg);
            }

            #[cfg(any(feature = "cluster", feature = "cluster-http"))]
            if let Some(cluster) = state.cluster() {
                for (crdt_id, delta) in cluster_deltas {
                    cluster.on_delta(ns, &crdt_id, delta).await;
                }
            }

            // Send conflict notifications privately to this client.
            for msg in conflict_msgs {
                if let Ok(b) = msg.to_msgpack() {
                    let _ = socket.send(Message::Binary(b.into())).await;
                }
            }

            let ack = ServerMsg::BatchAck {
                seq: *seq,
                count: delta_count,
                client_seq,
            };
            if let Ok(b) = ack.to_msgpack() {
                let _ = socket.send(Message::Binary(b.into())).await;
            }
        }

        ClientMsg::AwarenessUpdate { key, data } => {
            // Stateless fan-out — no persistence, no ack.
            let broadcast_msg = Arc::new(ServerMsg::AwarenessBroadcast {
                client_id,
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

            let vc: crate::crdt::VectorClock = match rmp_serde::decode::from_slice(&since_vc) {
                Ok(v) => v,
                Err(_) => {
                    send_error(socket, 400, "malformed vector clock").await;
                    return true;
                }
            };

            // Register (or advance) this client's observed VC in the GC registry.
            // We do this on every Sync so the floor_vc stays accurate.
            state.client_registry().update_vc(ns, client_id, &vc);

            match state.store().get(ns, &crdt_id).await {
                Ok(Some(crdt)) => {
                    if let Ok(Some(delta_bytes)) = crdt.delta_since_msgpack(&vc) {
                        let msg = ServerMsg::Delta {
                            crdt_id,
                            delta_bytes: delta_bytes.into(),
                        };
                        if let Ok(b) = msg.to_msgpack() {
                            let _ = socket.send(Message::Binary(b.into())).await;
                        }
                    } else {
                        // Up to date — send empty ack
                        let ack = ServerMsg::Ack {
                            seq: *seq,
                            client_seq: None,
                        };
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

        ClientMsg::SubscribeQuery { query_id, query } => {
            debug!(ns, client_id, query_id, "live query subscribe");
            // Register the query and immediately push a first result.
            query_registry.insert(query_id.clone(), query.clone());
            push_live_query_result(socket, state.store(), ns, &query_id, &query).await;
        }

        ClientMsg::UnsubscribeQuery { query_id } => {
            debug!(ns, client_id, query_id, "live query unsubscribe");
            query_registry.remove(&query_id);
        }

        ClientMsg::UndoLww {
            crdt_id,
            target_hlc,
            restore_entry,
        } => {
            // Undo a LwwRegister Set requires the same write permission as a normal Set.
            if !claims.can_write_key_op(
                &crdt_id,
                meridian_core::auth::claims::op_masks::LWW_SET,
            ) {
                send_error(socket, 403, "insufficient permissions for key").await;
                return true;
            }

            let target_hlc_val: HybridLogicalClock = match rmp_serde::decode::from_slice(&target_hlc) {
                Ok(h) => h,
                Err(_) => {
                    send_error(socket, 400, "malformed target_hlc").await;
                    return true;
                }
            };

            // restore_entry is msgpack-encoded `Option<serde_json::Value>`:
            // `null` (None) means the register was empty before the undone write.
            let restore_value: Option<serde_json::Value> = match rmp_serde::decode::from_slice(&restore_entry) {
                Ok(v) => v,
                Err(_) => {
                    send_error(socket, 400, "malformed restore_entry").await;
                    return true;
                }
            };

            match apply_undo_lww_atomic(
                state.store(),
                ns,
                &crdt_id,
                target_hlc_val,
                restore_value,
                client_id,
            )
            .await
            {
                Ok(UndoLwwResult::Applied(delta_bytes)) => {
                    metrics::record_op(ns, &crdt_id, "ws_undo_lww");

                    // Broadcast the restore delta to all namespace subscribers.
                    let delta_msg = Arc::new(ServerMsg::Delta {
                        crdt_id: crdt_id.clone(),
                        delta_bytes: delta_bytes.into(),
                    });
                    state.subscriptions().publish(ns, delta_msg);

                    let ack = ServerMsg::UndoAck { crdt_id };
                    if let Ok(b) = ack.to_msgpack() {
                        let _ = socket.send(Message::Binary(b.into())).await;
                    }
                }
                Ok(UndoLwwResult::Skipped(reason)) => {
                    debug!(ns, client_id, crdt_id, reason, "UndoLww skipped");
                    let msg = ServerMsg::UndoSkipped { crdt_id, reason };
                    if let Ok(b) = msg.to_msgpack() {
                        let _ = socket.send(Message::Binary(b.into())).await;
                    }
                }
                Err(ApplyError::Crdt(e)) => {
                    warn!(error = %e, "undo_lww crdt error");
                    send_error(socket, 400, &e.to_string()).await;
                }
                Err(ApplyError::Storage(e)) => {
                    warn!(error = %e, "undo_lww store error");
                    send_error(socket, 500, "internal error").await;
                }
            }
        }
    }

    true
}

async fn send_error(socket: &mut WebSocket, code: u16, message: &str) {
    let err = ServerMsg::Error {
        code,
        message: message.to_owned(),
    };
    if let Ok(b) = err.to_msgpack() {
        let _ = socket.send(Message::Binary(b.into())).await;
    }
}

#[cfg(test)]
mod tests {
    use super::verify_op_sig;
    use ed25519_dalek::{Signature, Signer, SigningKey};

    fn make_key() -> SigningKey {
        SigningKey::from_bytes(&[0x42u8; 32])
    }

    #[test]
    fn valid_signature_accepted() {
        let sk = make_key();
        let msg = b"op_bytes_payload";
        let sig: Signature = sk.sign(msg);
        let vk = sk.verifying_key();
        assert!(verify_op_sig(vk.as_bytes(), msg, sig.to_bytes().as_ref()));
    }

    #[test]
    fn tampered_message_rejected() {
        let sk = make_key();
        let msg = b"op_bytes_payload";
        let sig: Signature = sk.sign(msg);
        let vk = sk.verifying_key();
        assert!(!verify_op_sig(vk.as_bytes(), b"tampered_payload", sig.to_bytes().as_ref()));
    }

    #[test]
    fn wrong_key_rejected() {
        let sk = make_key();
        let other_sk = SigningKey::from_bytes(&[0x99u8; 32]);
        let msg = b"op_bytes_payload";
        let sig: Signature = sk.sign(msg);
        let wrong_vk = other_sk.verifying_key();
        assert!(!verify_op_sig(wrong_vk.as_bytes(), msg, sig.to_bytes().as_ref()));
    }

    #[test]
    fn bad_pubkey_length_rejected() {
        assert!(!verify_op_sig(&[0u8; 16], b"msg", &[0u8; 64]));
    }

    #[test]
    fn bad_sig_length_rejected() {
        let sk = make_key();
        let vk = sk.verifying_key();
        assert!(!verify_op_sig(vk.as_bytes(), b"msg", &[0u8; 32]));
    }
}
