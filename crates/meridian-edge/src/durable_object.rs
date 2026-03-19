use std::collections::HashMap;

use meridian_core::{
    crdt::registry::{apply_op, CrdtOp, CrdtValue},
    protocol::{ClientMsg, ServerMsg},
};
use serde::{Deserialize, Serialize};
use worker::{durable_object, Env, Request, Response, Result, State, WebSocket, WebSocketPair};
use worker::durable::DurableObject;

/// Newtype wrapper so `Vec<u8>` serializes as a base64 string via serde_json,
/// which is what the DO KV storage uses internally.
#[derive(Serialize, Deserialize)]
struct Bytes(#[serde(with = "serde_bytes")] Vec<u8>);

/// Per-namespace Durable Object.
///
/// One instance per namespace:
/// - Stores CRDT snapshots in DO KV storage (msgpack-encoded)
/// - Manages WebSocket connections via the Hibernation API
/// - Fan-outs CRDT deltas and awareness broadcasts
#[durable_object]
pub struct NsObject {
    state: State,
    #[allow(dead_code)]
    env: Env,
}

impl DurableObject for NsObject {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
        let path = url.path();

        // WebSocket upgrade: triggered either by the original client request
        // (forwarded directly from the main worker) or by an internal `/ws` path.
        let is_ws_upgrade = req
            .headers()
            .get("Upgrade")
            .ok()
            .flatten()
            .map(|v| v.eq_ignore_ascii_case("websocket"))
            .unwrap_or(false);

        if is_ws_upgrade || path.ends_with("/ws") || path.ends_with("/connect") {
            self.handle_websocket(req).await
        } else if path.ends_with("/op") {
            self.handle_op(req).await
        } else if path.contains("/get/") {
            self.handle_get(req).await
        } else {
            Response::error("not found", 404)
        }
    }

    async fn websocket_message(
        &self,
        ws: WebSocket,
        msg: worker::WebSocketIncomingMessage,
    ) -> Result<()> {
        let bytes = match msg {
            worker::WebSocketIncomingMessage::Binary(b) => b,
            worker::WebSocketIncomingMessage::String(_) => return Ok(()),
        };

        let client_msg = match ClientMsg::from_msgpack(&bytes) {
            Ok(m) => m,
            Err(_) => {
                let err = ServerMsg::Error { code: 400, message: "malformed message".into() };
                if let Ok(b) = err.to_msgpack() {
                    let _ = ws.send_with_bytes(&b);
                }
                return Ok(());
            }
        };

        match client_msg {
            ClientMsg::Subscribe { crdt_id } => {
                if let Ok(Some(value)) = self.load_crdt(&crdt_id).await {
                    let empty_vc = meridian_core::crdt::VectorClock::default();
                    if let Ok(Some(delta_bytes)) = value.delta_since_msgpack(&empty_vc) {
                        let msg = ServerMsg::Delta {
                            crdt_id,
                            delta_bytes: delta_bytes.into(),
                        };
                        if let Ok(b) = msg.to_msgpack() {
                            let _ = ws.send_with_bytes(&b);
                        }
                    }
                }
            }
            ClientMsg::Op { crdt_id, op_bytes, ttl_ms } => {
                let op: CrdtOp = match rmp_serde::decode::from_slice(&op_bytes) {
                    Ok(o) => o,
                    Err(_) => return Ok(()),
                };

                let crdt_type = op.crdt_type();
                let mut value = self
                    .load_crdt(&crdt_id)
                    .await
                    .unwrap_or(None)
                    .unwrap_or_else(|| CrdtValue::new(crdt_type));

                let delta_bytes = match apply_op(&mut value, op) {
                    Ok(d) => d,
                    Err(_) => return Ok(()),
                };

                let _ = self.save_crdt(&crdt_id, &value).await;

                if let Some(ms) = ttl_ms {
                    let now = js_sys::Date::now() as u64;
                    let exp_key = format!("crdt:{crdt_id}:exp");
                    let _ = self.state.storage().put(&exp_key, now.saturating_add(ms)).await;
                }

                if let Some(ref delta) = delta_bytes {
                    let msg = ServerMsg::Delta {
                        crdt_id,
                        delta_bytes: delta.clone().into(),
                    };
                    if let Ok(msg_bytes) = msg.to_msgpack() {
                        for other in self.state.get_websockets() {
                            let _ = other.send_with_bytes(&msg_bytes);
                        }
                    }
                }
            }
            ClientMsg::AwarenessUpdate { key, data } => {
                let broadcast = ServerMsg::AwarenessBroadcast { client_id: 0, key, data };
                if let Ok(b) = broadcast.to_msgpack() {
                    for other in self.state.get_websockets() {
                        let _ = other.send_with_bytes(&b);
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    async fn websocket_close(
        &self,
        _ws: WebSocket,
        _code: usize,
        _reason: String,
        _was_clean: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn websocket_error(&self, _ws: WebSocket, _error: worker::Error) -> Result<()> {
        Ok(())
    }
}

impl NsObject {
    /// Load a CRDT from DO KV storage.
    async fn load_crdt(&self, crdt_id: &str) -> Result<Option<CrdtValue>> {
        let key = format!("crdt:{crdt_id}");
        let stored: Option<Bytes> = self.state.storage().get(&key).await?;
        match stored {
            None => Ok(None),
            Some(Bytes(b)) => CrdtValue::from_msgpack(&b)
                .map(Some)
                .map_err(|e| worker::Error::RustError(e.to_string())),
        }
    }

    /// Persist a CRDT snapshot to DO KV storage.
    async fn save_crdt(&self, crdt_id: &str, value: &CrdtValue) -> Result<()> {
        let key = format!("crdt:{crdt_id}");
        let data = value
            .to_msgpack()
            .map_err(|e| worker::Error::RustError(e.to_string()))?;
        self.state.storage().put(&key, Bytes(data)).await
    }

    /// WebSocket upgrade — attach client to DO via Hibernation API.
    async fn handle_websocket(&self, _req: Request) -> Result<Response> {
        let pair = WebSocketPair::new()?;
        let server = pair.server;
        let client = pair.client;

        self.state.accept_web_socket(&server);

        Response::from_websocket(client)
    }

    /// Apply a CRDT op and broadcast the delta to all connected clients.
    async fn handle_op(&self, mut req: Request) -> Result<Response> {
        let body = req.bytes().await?;

        let url = req.url()?;
        let params: HashMap<String, String> = url.query_pairs().into_owned().collect();
        let crdt_id = params.get("crdt_id").cloned().unwrap_or_default();
        let ttl_ms: Option<u64> = params.get("ttl_ms").and_then(|v| v.parse().ok());

        let op: CrdtOp = rmp_serde::decode::from_slice(&body)
            .map_err(|e| worker::Error::RustError(format!("decode op: {e}")))?;

        let crdt_type = op.crdt_type();
        let mut value = self
            .load_crdt(&crdt_id)
            .await?
            .unwrap_or_else(|| CrdtValue::new(crdt_type));

        let delta_bytes = apply_op(&mut value, op)
            .map_err(|e| worker::Error::RustError(e.to_string()))?;

        self.save_crdt(&crdt_id, &value).await?;

        // Store TTL expiry separately if requested
        if let Some(ms) = ttl_ms {
            let now = js_sys::Date::now() as u64;
            let exp_key = format!("crdt:{crdt_id}:exp");
            self.state.storage().put(&exp_key, now.saturating_add(ms)).await?;
        }

        // Broadcast delta to all connected WS clients
        if let Some(ref delta) = delta_bytes {
            let msg = ServerMsg::Delta {
                crdt_id: crdt_id.clone(),
                delta_bytes: delta.clone().into(),
            };
            if let Ok(msg_bytes) = msg.to_msgpack() {
                for ws in self.state.get_websockets() {
                    let _ = ws.send_with_bytes(&msg_bytes);
                }
            }
        }

        match delta_bytes {
            Some(bytes) => {
                let mut resp = Response::from_bytes(bytes)?;
                resp.headers_mut()
                    .set("Content-Type", "application/msgpack")?;
                Ok(resp)
            }
            None => Response::empty().map(|r| r.with_status(204)),
        }
    }

    /// Return CRDT state as JSON.
    async fn handle_get(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
        let crdt_id = url.path().split("/get/").nth(1).unwrap_or("");

        match self.load_crdt(crdt_id).await? {
            Some(value) => Response::from_json(&value.to_json_value()),
            None => Response::error("not found", 404),
        }
    }
}
