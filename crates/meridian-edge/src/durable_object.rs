use std::collections::HashMap;

use base64::Engine as _;
use meridian_core::{
    crdt::{registry::{apply_op, CrdtOp, CrdtValue}, VectorClock},
    protocol::{ClientMsg, ServerMsg},
};
use serde::{Deserialize, Serialize};
use worker::{durable_object, Env, Request, Response, Result, State, WebSocket, WebSocketPair};
use worker::durable::DurableObject;

use crate::wal::WalEntry;

/// A registered webhook for a namespace.
#[derive(Clone, Serialize, Deserialize)]
pub struct Webhook {
    pub id: String,
    pub url: String,
    /// Optional secret — sent as `X-Meridian-Secret` header.
    pub secret: Option<String>,
    /// Filter: only fire for these crdt_ids. Empty = fire for all.
    pub crdt_ids: Vec<String>,
}

/// Payload sent to webhook URLs on every op.
#[derive(Serialize)]
struct WebhookPayload<'a> {
    namespace: &'a str,
    crdt_id: &'a str,
    seq: u64,
    timestamp_ms: u64,
}

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
        } else if path.ends_with("/wal") {
            self.handle_wal(req).await
        } else if path.ends_with("/sync") {
            self.handle_sync(req).await
        } else if path.ends_with("/webhooks") {
            match req.method() {
                worker::Method::Get => self.handle_list_webhooks().await,
                worker::Method::Post => self.handle_register_webhook(req).await,
                _ => Response::error("method not allowed", 405),
            }
        } else if path.contains("/webhooks/") {
            match req.method() {
                worker::Method::Delete => self.handle_delete_webhook(req).await,
                _ => Response::error("method not allowed", 405),
            }
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
            ClientMsg::Op { crdt_id, op_bytes, ttl_ms, client_seq } => {
                // Rate limit: 500 ops per minute per namespace (WS path, higher limit)
                if !self.check_rate_limit(500, 60_000).await {
                    let err = ServerMsg::Error { code: 429, message: "rate limit exceeded".into() };
                    if let Ok(b) = err.to_msgpack() {
                        let _ = ws.send_with_bytes(&b);
                    }
                    return Ok(());
                }

                let op: CrdtOp = match rmp_serde::decode::from_slice(&op_bytes) {
                    Ok(o) => o,
                    Err(_) => return Ok(()),
                };

                // Write-ahead: persist the op before applying the snapshot.
                let _ = self.append_wal(&crdt_id, &op_bytes).await;
                self.touch_activity().await;

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
                        crdt_id: crdt_id.clone(),
                        delta_bytes: delta.clone().into(),
                    };
                    if let Ok(msg_bytes) = msg.to_msgpack() {
                        for other in self.state.get_websockets() {
                            let _ = other.send_with_bytes(&msg_bytes);
                        }
                    }
                }

                // Fire webhooks (best-effort, non-blocking)
                let seq: u64 = self.state.storage().get("wal:seq").await.ok().flatten().unwrap_or(0);
                let _ = self.fire_webhooks(&crdt_id, seq).await;

                // Acknowledge the op to the sender with the WAL seq and echoed client_seq.
                let ack = ServerMsg::Ack { seq, client_seq };
                if let Ok(b) = ack.to_msgpack() {
                    let _ = ws.send_with_bytes(&b);
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

    async fn alarm(&self) -> Result<Response> {
        let now_ms = js_sys::Date::now() as u64;

        // --- Namespace TTL: delete if inactive for > NAMESPACE_TTL_MS ---
        const NAMESPACE_TTL_MS: u64 = 30 * 24 * 3600 * 1000; // 30 days
        let last_activity: Option<u64> = self.state.storage().get("ns:last_activity").await.ok().flatten();
        if let Some(last) = last_activity
            && now_ms.saturating_sub(last) > NAMESPACE_TTL_MS
        {
            let _ = self.state.storage().delete_all().await;
            return Response::ok("expired");
        }

        // --- WAL compaction: remove entries older than 7 days ---
        let cutoff_ms = now_ms - 7 * 24 * 3600 * 1000;
        let _ = self.compact_wal_before(cutoff_ms).await;

        // Reschedule in 24h (set_alarm takes i64 ms since epoch)
        let next: i64 = (now_ms as i64) + 24 * 3600 * 1000;
        let _ = self.state.storage().set_alarm(next).await;

        Response::ok("compacted")
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

    /// Sliding-window rate limiter: max `limit` ops per `window_ms`.
    /// Returns `true` if the request is allowed, `false` if throttled.
    /// Uses a per-minute bucket key so old counts expire naturally.
    async fn check_rate_limit(&self, limit: u64, window_ms: u64) -> bool {
        let now_ms = js_sys::Date::now() as u64;
        let bucket = now_ms / window_ms;
        let key = format!("rl:{bucket}");

        let count: u64 = self.state.storage().get(&key).await.ok().flatten().unwrap_or(0u64);
        if count >= limit {
            return false;
        }
        let _ = self.state.storage().put(&key, count + 1).await;
        true
    }

    /// Update `ns:last_activity` to now. Called on every client interaction.
    async fn touch_activity(&self) {
        let now_ms = js_sys::Date::now() as u64;
        let _ = self.state.storage().put("ns:last_activity", now_ms).await;
    }

    /// WebSocket upgrade — attach client to DO via Hibernation API.
    async fn handle_websocket(&self, _req: Request) -> Result<Response> {
        let pair = WebSocketPair::new()?;
        let server = pair.server;
        let client = pair.client;

        self.state.accept_web_socket(&server);
        self.touch_activity().await;

        Response::from_websocket(client)
    }

    /// Apply a CRDT op and broadcast the delta to all connected clients.
    async fn handle_op(&self, mut req: Request) -> Result<Response> {
        // Rate limit: 200 ops per minute per namespace (HTTP path)
        if !self.check_rate_limit(200, 60_000).await {
            return Response::error("rate limit exceeded", 429);
        }

        let body = req.bytes().await?;

        let url = req.url()?;
        let params: HashMap<String, String> = url.query_pairs().into_owned().collect();
        let crdt_id = params.get("crdt_id").cloned().unwrap_or_default();
        let ttl_ms: Option<u64> = params.get("ttl_ms").and_then(|v| v.parse().ok());

        let op: CrdtOp = rmp_serde::decode::from_slice(&body)
            .map_err(|e| worker::Error::RustError(format!("decode op: {e}")))?;

        // Write-ahead: persist the op before applying the snapshot.
        let _ = self.append_wal(&crdt_id, &body).await;
        self.touch_activity().await;

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

        // Fire webhooks (best-effort, non-blocking)
        let seq: u64 = self.state.storage().get("wal:seq").await.ok().flatten().unwrap_or(0);
        let _ = self.fire_webhooks(&crdt_id, seq).await;

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

    // -----------------------------------------------------------------------
    // Webhook management
    // -----------------------------------------------------------------------

    /// Load all registered webhooks from KV.
    async fn load_webhooks(&self) -> Result<Vec<Webhook>> {
        let opts = worker::durable::ListOptions::new().prefix("webhook:");
        let map = self.state.storage().list_with_options(opts).await?;
        let mut hooks = Vec::new();

        let iter = match js_sys::try_iter(&map).ok().flatten() {
            Some(i) => i,
            None => return Ok(hooks),
        };

        for item in iter {
            let item = item.map_err(|e| worker::Error::RustError(format!("{e:?}")))?;
            let pair = js_sys::Array::from(&item);
            let raw = pair.get(1);
            let s = raw.as_string().unwrap_or_else(|| {
                js_sys::JSON::stringify(&raw).ok().and_then(|s| s.as_string()).unwrap_or_default()
            });
            if let Ok(hook) = serde_json::from_str::<Webhook>(&s) {
                hooks.push(hook);
            }
        }

        Ok(hooks)
    }

    /// GET /webhooks — list registered webhooks (admin only).
    async fn handle_list_webhooks(&self) -> Result<Response> {
        let hooks = self.load_webhooks().await?;
        Response::from_json(&hooks)
    }

    /// POST /webhooks — register a new webhook.
    async fn handle_register_webhook(&self, mut req: Request) -> Result<Response> {
        #[derive(Deserialize)]
        struct RegisterBody {
            url: String,
            secret: Option<String>,
            #[serde(default)]
            crdt_ids: Vec<String>,
        }

        let body: RegisterBody = req.json().await
            .map_err(|_| worker::Error::RustError("invalid body".into()))?;

        let id = uuid::Uuid::new_v4().to_string();
        let hook = Webhook { id: id.clone(), url: body.url, secret: body.secret, crdt_ids: body.crdt_ids };
        let key = format!("webhook:{id}");
        let val = serde_json::to_string(&hook)
            .map_err(|e| worker::Error::RustError(e.to_string()))?;
        self.state.storage().put(&key, val).await?;

        Response::from_json(&hook)
    }

    /// DELETE /webhooks/:id — remove a webhook.
    async fn handle_delete_webhook(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
        let path = url.path();
        let id = path.split("/webhooks/").nth(1).unwrap_or("").to_owned();
        if id.is_empty() {
            return Response::error("missing webhook id", 400);
        }
        let key = format!("webhook:{id}");
        self.state.storage().delete(&key).await?;
        Response::ok("deleted")
    }

    /// Fire all matching webhooks for a crdt_id op — best effort, errors ignored.
    async fn fire_webhooks(&self, crdt_id: &str, seq: u64) -> Result<()> {
        let hooks = self.load_webhooks().await?;
        if hooks.is_empty() {
            return Ok(());
        }

        let payload = WebhookPayload {
            namespace: "",  // namespace not stored in DO, callers don't need it here
            crdt_id,
            seq,
            timestamp_ms: js_sys::Date::now() as u64,
        };
        let body = serde_json::to_string(&payload)
            .map_err(|e| worker::Error::RustError(e.to_string()))?;

        for hook in hooks {
            if !hook.crdt_ids.is_empty() && !hook.crdt_ids.iter().any(|id| id == crdt_id || id == "*") {
                continue;
            }

            let mut init = worker::RequestInit::new();
            init.with_method(worker::Method::Post);
            let headers = worker::Headers::new();
            let _ = headers.set("Content-Type", "application/json");
            if let Some(ref secret) = hook.secret {
                let _ = headers.set("X-Meridian-Secret", secret);
            }
            init.with_headers(headers);
            init.with_body(Some(body.clone().into()));

            if let Ok(req) = Request::new_with_init(&hook.url, &init) {
                // Fire and forget — don't await the result
                let _ = worker::Fetch::Request(req).send().await;
            }
        }

        Ok(())
    }

    /// Delete WAL entries older than `cutoff_ms` and update `wal:checkpoint`.
    async fn compact_wal_before(&self, cutoff_ms: u64) -> Result<()> {
        let opts = worker::durable::ListOptions::new().prefix("wal:");
        let map = self.state.storage().list_with_options(opts).await?;

        let iter = match js_sys::try_iter(&map).ok().flatten() {
            Some(i) => i,
            None => return Ok(()),
        };

        let mut last_deleted_seq: u64 = 0;
        let mut keys_to_delete: Vec<String> = Vec::new();

        for item in iter {
            let item = item.map_err(|e| worker::Error::RustError(format!("{e:?}")))?;
            let pair = js_sys::Array::from(&item);
            let key = pair.get(0).as_string().unwrap_or_default();
            // Skip metadata keys (wal:seq, wal:checkpoint) — only process hex-seq keys
            if key == "wal:seq" || key == "wal:checkpoint" {
                continue;
            }
            let raw_val = pair.get(1);
            let val = raw_val.as_string().unwrap_or_else(|| {
                js_sys::JSON::stringify(&raw_val)
                    .ok()
                    .and_then(|s| s.as_string())
                    .unwrap_or_default()
            });
            let raw: Bytes = match serde_json::from_str(&val) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let entry: WalEntry = match rmp_serde::decode::from_slice(&raw.0) {
                Ok(e) => e,
                Err(_) => continue,
            };
            if entry.timestamp_ms < cutoff_ms {
                keys_to_delete.push(key);
                last_deleted_seq = entry.seq;
            } else {
                break; // entries are in seq order ≈ time order
            }
        }

        for key in keys_to_delete {
            let _ = self.state.storage().delete(&key).await;
        }

        if last_deleted_seq > 0 {
            let _ = self.state.storage().put("wal:checkpoint", last_deleted_seq).await;
        }

        Ok(())
    }

    /// Append an op to the WAL. Returns the assigned sequence number.
    /// Write-ahead: called before applying the op to the snapshot.
    async fn append_wal(&self, crdt_id: &str, op_bytes: &[u8]) -> Result<u64> {
        let seq: u64 = self.state.storage().get("wal:seq").await?.unwrap_or(0u64);
        let next_seq = seq + 1;

        let entry = WalEntry {
            seq: next_seq,
            crdt_id: crdt_id.to_owned(),
            op_bytes: op_bytes.to_vec(),
            timestamp_ms: js_sys::Date::now() as u64,
        };

        let key = format!("wal:{:016x}", next_seq);
        let data = rmp_serde::encode::to_vec_named(&entry)
            .map_err(|e| worker::Error::RustError(e.to_string()))?;

        self.state.storage().put(&key, Bytes(data)).await?;
        self.state.storage().put("wal:seq", next_seq).await?;

        // Schedule the first compaction alarm when the first op is written
        if next_seq == 1 {
            let next_alarm: i64 = (js_sys::Date::now() as i64) + 24 * 3600 * 1000;
            let _ = self.state.storage().set_alarm(next_alarm).await;
        }

        Ok(next_seq)
    }

    /// Replay WAL entries with seq >= from_seq, optionally bounded by until_ms.
    async fn replay_wal(&self, from_seq: u64, until_ms: Option<u64>) -> Result<Vec<WalEntry>> {
        let start_key = format!("wal:{:016x}", from_seq);
        let opts = worker::durable::ListOptions::new()
            .prefix("wal:")
            .start(&start_key);

        let map = self.state.storage().list_with_options(opts).await?;
        let mut entries = Vec::new();

        let iter = js_sys::try_iter(&map)
            .ok()
            .flatten()
            .ok_or_else(|| worker::Error::RustError("wal map not iterable".into()))?;

        for item in iter {
            let item = item.map_err(|e| worker::Error::RustError(format!("{e:?}")))?;
            let pair = js_sys::Array::from(&item);
            let val = pair.get(1);
            // In production CF DO KV, values are returned as JSON strings.
            // In miniflare (tests), they may be returned as JS objects — stringify both.
            let bytes = val.as_string().unwrap_or_else(|| {
                js_sys::JSON::stringify(&val)
                    .ok()
                    .and_then(|s| s.as_string())
                    .unwrap_or_default()
            });
            if bytes.is_empty() {
                continue;
            }
            // Values are stored as Bytes (base64 via serde_bytes / serde_json)
            let raw: Bytes = match serde_json::from_str(&bytes) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let entry: WalEntry = match rmp_serde::decode::from_slice(&raw.0) {
                Ok(e) => e,
                Err(_) => continue,
            };

            if let Some(ms) = until_ms
                && entry.timestamp_ms > ms
            {
                break;
            }
            entries.push(entry);
        }

        Ok(entries)
    }

    /// GET /wal?from_seq=0[&until_ms=...]
    async fn handle_wal(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
        let params: HashMap<String, String> = url.query_pairs().into_owned().collect();
        let from_seq: u64 = params.get("from_seq").and_then(|v| v.parse().ok()).unwrap_or(0);
        let until_ms: Option<u64> = params.get("until_ms").and_then(|v| v.parse().ok());

        let checkpoint_seq: u64 = self.state.storage()
            .get("wal:checkpoint").await.ok().flatten().unwrap_or(0u64);
        let entries = self.replay_wal(from_seq, until_ms).await?;

        #[derive(Serialize)]
        struct WalResponse {
            checkpoint_seq: u64,
            entries: Vec<WalEntry>,
        }

        Response::from_json(&WalResponse { checkpoint_seq, entries })
    }

    /// GET /sync?crdt_id=<id>[&since=<base64url-msgpack-vc>]
    /// Returns a msgpack delta since the given VectorClock, or full state if absent.
    async fn handle_sync(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
        let params: HashMap<String, String> = url.query_pairs().into_owned().collect();
        let crdt_id = params.get("crdt_id").cloned().unwrap_or_default();

        let since = match params.get("since") {
            None => VectorClock::default(),
            Some(b64) => {
                let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(b64)
                    .map_err(|e| worker::Error::RustError(format!("invalid since: {e}")))?;
                rmp_serde::decode::from_slice::<VectorClock>(&bytes)
                    .map_err(|e| worker::Error::RustError(format!("malformed vector clock: {e}")))?
            }
        };

        match self.load_crdt(&crdt_id).await? {
            None => Response::error("not found", 404),
            Some(value) => match value.delta_since_msgpack(&since) {
                Ok(Some(bytes)) => {
                    let mut r = Response::from_bytes(bytes)?;
                    r.headers_mut().set("Content-Type", "application/msgpack")?;
                    Ok(r)
                }
                Ok(None) => Response::empty().map(|r| r.with_status(204)),
                Err(e) => Response::error(e.to_string(), 500),
            },
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
