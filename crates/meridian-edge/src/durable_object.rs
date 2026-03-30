use std::collections::HashMap;

use base64::Engine as _;
use meridian_core::{
    crdt::{registry::{apply_op, validate_clock_drift, CrdtOp, CrdtValue}, VectorClock},
    protocol::{BatchItem, ClientMsg, ServerMsg},
    query::{execute_query_on_values, AggregateOp, WhereClause},
};
use serde::{Deserialize, Serialize};
use worker::{durable_object, Env, Request, Response, Result, State, WebSocket, WebSocketPair};
use worker::durable::DurableObject;

use crate::wal::WalEntry;

/// Data attached to each WebSocket via the Hibernation API.
///
/// Persisted across hibernation cycles — stored as a msgpack blob in the WS
/// attachment slot. Gives the DO access to `client_id` and a stable
/// per-connection `conn_id` (UUID) without re-parsing the token on every message.
#[derive(Clone, Serialize, Deserialize)]
struct WsAttachment {
    client_id: u64,
    /// Stable UUID assigned at handshake — used as the KV key prefix for live
    /// query subscriptions (`lq:{conn_id}:{query_id}`).
    conn_id: String,
}

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

/// A failed webhook delivery queued for retry.
#[derive(Clone, Serialize, Deserialize)]
struct WebhookDlqEntry {
    /// Unique key identifying this entry (used as KV key).
    id: String,
    hook_url: String,
    hook_secret: Option<String>,
    /// JSON-serialized `WebhookPayload`.
    payload_json: String,
    /// Number of delivery attempts so far (0 = never tried successfully).
    attempts: u32,
    /// Wall-clock ms of the next allowed retry.
    retry_after_ms: u64,
}

/// Backoff delays for webhook retries (ms).
/// After attempt 1 fails → wait 5 min; after 2 → 30 min; after 3 → 2 h, then drop.
const WEBHOOK_RETRY_DELAYS_MS: [u64; 3] = [
    5 * 60 * 1000,
    30 * 60 * 1000,
    2 * 60 * 60 * 1000,
];
const WEBHOOK_MAX_ATTEMPTS: u32 = 3;

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

        // Persist namespace on first request so fire_webhooks can read it later.
        // Path format: /{ns}/op, /{ns}/ws, etc. — namespace is the first segment.
        if self.state.storage().get::<String>("ns:name").await.ok().flatten().is_none() {
            let ns = path.trim_start_matches('/').split('/').next().unwrap_or("");
            if !ns.is_empty() {
                let _ = self.state.storage().put("ns:name", ns.to_owned()).await;
            }
        }

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
        } else if path.ends_with("/history") {
            self.handle_history(req).await
        } else if path.ends_with("/sync") {
            self.handle_sync(req).await
        } else if path.ends_with("/query") {
            self.handle_query(req).await
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

                // Reject ops with client HLC timestamps too far from server wall time.
                let now_ms = js_sys::Date::now() as u64;
                if let Err(e) = validate_clock_drift(&op, now_ms) {
                    let err = ServerMsg::Error { code: 400, message: e.to_string() };
                    if let Ok(b) = err.to_msgpack() {
                        let _ = ws.send_with_bytes(&b);
                    }
                    return Ok(());
                }

                // Write-ahead: persist the op before applying the snapshot.
                if self.append_wal(&crdt_id, &op_bytes).await.is_err() {
                    let err = ServerMsg::Error { code: 503, message: "wal write failed".into() };
                    if let Ok(b) = err.to_msgpack() { let _ = ws.send_with_bytes(&b); }
                    return Ok(());
                }
                self.touch_activity().await;

                let crdt_type = op.crdt_type();
                let mut value = self
                    .load_crdt(&crdt_id)
                    .await
                    .unwrap_or(None)
                    .unwrap_or_else(|| CrdtValue::new(crdt_type));

                let delta_bytes = match apply_op(&mut value, op) {
                    Ok(d) => d,
                    Err(e) => {
                        let err = ServerMsg::Error { code: 400, message: e.to_string() };
                        if let Ok(b) = err.to_msgpack() { let _ = ws.send_with_bytes(&b); }
                        return Ok(());
                    }
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

                // Push live query results to all matching subscribers.
                let _ = self.push_live_queries(&crdt_id).await;

                // Acknowledge the op to the sender with the WAL seq and echoed client_seq.
                let ack = ServerMsg::Ack { seq, client_seq };
                if let Ok(b) = ack.to_msgpack() {
                    let _ = ws.send_with_bytes(&b);
                }
            }
            ClientMsg::BatchOp { ops, client_seq } => {
                // Rate limit: count the batch as one request.
                if !self.check_rate_limit(500, 60_000).await {
                    let err = ServerMsg::Error { code: 429, message: "rate limit exceeded".into() };
                    if let Ok(b) = err.to_msgpack() {
                        let _ = ws.send_with_bytes(&b);
                    }
                    return Ok(());
                }

                if ops.is_empty() {
                    let err = ServerMsg::Error { code: 400, message: "batch must not be empty".into() };
                    if let Ok(b) = err.to_msgpack() {
                        let _ = ws.send_with_bytes(&b);
                    }
                    return Ok(());
                }

                let now_ms = js_sys::Date::now() as u64;

                // --- Phase 1: decode + validate all ops before touching any state ---
                struct ParsedItem {
                    crdt_id: String,
                    op: CrdtOp,
                    op_bytes: Vec<u8>,
                    ttl_ms: Option<u64>,
                }
                let mut parsed: Vec<ParsedItem> = Vec::with_capacity(ops.len());
                for BatchItem { crdt_id, op_bytes, ttl_ms } in ops {
                    let op: CrdtOp = match rmp_serde::decode::from_slice(&op_bytes) {
                        Ok(o) => o,
                        Err(e) => {
                            let err = ServerMsg::Error { code: 400, message: format!("batch: decode error for {crdt_id}: {e}") };
                            if let Ok(b) = err.to_msgpack() { let _ = ws.send_with_bytes(&b); }
                            return Ok(());
                        }
                    };
                    if let Err(e) = validate_clock_drift(&op, now_ms) {
                        let err = ServerMsg::Error { code: 400, message: format!("batch: {crdt_id}: {e}") };
                        if let Ok(b) = err.to_msgpack() { let _ = ws.send_with_bytes(&b); }
                        return Ok(());
                    }
                    parsed.push(ParsedItem { crdt_id, op, op_bytes: op_bytes.into_vec(), ttl_ms });
                }

                // --- Phase 2: write one WAL entry per op ---
                // Each op is stored individually so anti-entropy and point-in-time
                // recovery can replay them one by one using the standard path.
                // Client-side atomicity (all-or-nothing visibility) is provided by
                // the grouped fan-out in Phase 4, not by the WAL.
                for item in &parsed {
                    if self.append_wal(&item.crdt_id, &item.op_bytes).await.is_err() {
                        let err = ServerMsg::Error { code: 503, message: "wal write failed".into() };
                        if let Ok(b) = err.to_msgpack() { let _ = ws.send_with_bytes(&b); }
                        return Ok(());
                    }
                }
                self.touch_activity().await;

                // --- Phase 3: apply all ops and collect deltas ---
                let mut delta_count = 0usize;
                let mut all_msg_bytes: Vec<Vec<u8>> = Vec::new();
                for item in &parsed {
                    let crdt_type = item.op.crdt_type();
                    let mut value = self
                        .load_crdt(&item.crdt_id)
                        .await
                        .unwrap_or(None)
                        .unwrap_or_else(|| CrdtValue::new(crdt_type));

                    if let Ok(Some(delta)) = apply_op(&mut value, item.op.clone()) {
                        let _ = self.save_crdt(&item.crdt_id, &value).await;
                        delta_count += 1;
                        let msg = ServerMsg::Delta {
                            crdt_id: item.crdt_id.clone(),
                            delta_bytes: delta.into(),
                        };
                        if let Ok(b) = msg.to_msgpack() {
                            all_msg_bytes.push(b);
                        }
                    } else {
                        let _ = self.save_crdt(&item.crdt_id, &value).await;
                    }

                    if let Some(ms) = item.ttl_ms {
                        let exp_key = format!("crdt:{}:exp", item.crdt_id);
                        let _ = self.state.storage().put(&exp_key, now_ms.saturating_add(ms)).await;
                    }
                }

                // --- Phase 4: fan-out all deltas together to every connected client ---
                let all_sockets = self.state.get_websockets();
                for msg_bytes in &all_msg_bytes {
                    for sock in &all_sockets {
                        let _ = sock.send_with_bytes(msg_bytes);
                    }
                }

                let seq: u64 = self.state.storage().get("wal:seq").await.ok().flatten().unwrap_or(0);
                let ack = ServerMsg::BatchAck { seq, count: delta_count, client_seq };
                if let Ok(b) = ack.to_msgpack() {
                    let _ = ws.send_with_bytes(&b);
                }

                // Push live query results for each changed CRDT.
                for item in &parsed {
                    let _ = self.push_live_queries(&item.crdt_id).await;
                }
            }
            ClientMsg::AwarenessUpdate { key, data } => {
                let client_id: u64 = ws.deserialize_attachment::<WsAttachment>()
                    .ok().flatten().map(|a| a.client_id).unwrap_or(0);
                let broadcast = ServerMsg::AwarenessBroadcast { client_id, key, data };
                if let Ok(b) = broadcast.to_msgpack() {
                    for other in self.state.get_websockets() {
                        let _ = other.send_with_bytes(&b);
                    }
                }
            }
            ClientMsg::SubscribeQuery { query_id, query } => {
                let attachment: Option<WsAttachment> =
                    ws.deserialize_attachment().ok().flatten();
                let conn_id = attachment.map(|a| a.conn_id).unwrap_or_default();

                // Persist subscription to KV.
                let key = format!("lq:{conn_id}:{query_id}");
                let payload_bytes = rmp_serde::encode::to_vec_named(&query)
                    .unwrap_or_default();
                let _ = self.state.storage().put(&key, Bytes(payload_bytes)).await;

                // Push initial result immediately.
                if let Ok(result) = self.run_live_query(&query).await {
                    let msg = ServerMsg::QueryResult {
                        query_id,
                        value: result.value,
                        matched: result.matched,
                    };
                    if let Ok(b) = msg.to_msgpack() {
                        let _ = ws.send_with_bytes(&b);
                    }
                }
            }

            ClientMsg::UnsubscribeQuery { query_id } => {
                let attachment: Option<WsAttachment> =
                    ws.deserialize_attachment().ok().flatten();
                let conn_id = attachment.map(|a| a.conn_id).unwrap_or_default();
                let key = format!("lq:{conn_id}:{query_id}");
                let _ = self.state.storage().delete(&key).await;
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
        // compact_wal_before reads wal:min_peer_seq (the lowest from_seq any peer
        // requested since the previous alarm) to avoid deleting entries a lagging
        // peer still needs. After compaction we reset it so the next window starts
        // fresh — decommissioned peers no longer block compaction after one cycle.
        let cutoff_ms = now_ms - 7 * 24 * 3600 * 1000;
        let _ = self.compact_wal_before(cutoff_ms).await;
        // Reset min_peer_seq for the next 24h window.
        let _ = self.state.storage().put("wal:min_peer_seq", u64::MAX).await;

        // --- CRDT snapshot compaction: strip tombstones and stale move_log entries ---
        // Safe to run after WAL compaction: checkpoint_seq is now updated,
        // meaning all connected clients have acknowledged those ops.
        let _ = self.compact_crdt_snapshots().await;

        // --- Webhook DLQ retry: re-attempt failed webhook deliveries ---
        let _ = self.retry_dlq_webhooks().await;

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
    ///
    /// Uses a two-bucket weighted approximation of a true sliding window:
    /// - Bucket N  = ops in the current fixed window
    /// - Bucket N-1 = ops in the previous fixed window
    ///
    /// Estimated count = prev_count × (1 - elapsed/window) + cur_count
    ///
    /// This prevents the fixed-window burst attack (2× limit at boundary)
    /// while remaining stateless beyond two small KV keys.
    async fn check_rate_limit(&self, limit: u64, window_ms: u64) -> bool {
        let now_ms = js_sys::Date::now() as u64;
        let bucket = now_ms / window_ms;
        let elapsed_in_bucket = now_ms % window_ms; // ms since current bucket started

        let cur_key  = format!("rl:{bucket}");
        let prev_key = format!("rl:{}", bucket.saturating_sub(1));

        let cur_count: u64  = self.state.storage().get(&cur_key).await.ok().flatten().unwrap_or(0u64);
        let prev_count: u64 = self.state.storage().get(&prev_key).await.ok().flatten().unwrap_or(0u64);

        // Weight the previous bucket by the fraction of the window not yet consumed.
        // Integer arithmetic: multiply first to avoid float.
        let prev_weight_num = window_ms - elapsed_in_bucket; // numerator
        let estimated = prev_count.saturating_mul(prev_weight_num) / window_ms + cur_count;

        if estimated >= limit {
            return false;
        }
        let _ = self.state.storage().put(&cur_key, cur_count + 1).await;
        true
    }

    /// Update `ns:last_activity` to now. Called on every client interaction.
    async fn touch_activity(&self) {
        let now_ms = js_sys::Date::now() as u64;
        let _ = self.state.storage().put("ns:last_activity", now_ms).await;
    }

    /// WebSocket upgrade — attach client to DO via Hibernation API.
    ///
    /// Stores the token's `client_id` as a WS attachment so that
    /// `websocket_message` can inject it into `AwarenessUpdate` broadcasts
    /// without re-parsing the token on every message.
    async fn handle_websocket(&self, req: Request) -> Result<Response> {
        let pair = WebSocketPair::new()?;
        let server = pair.server;
        let client = pair.client;

        // Extract client_id from the token and assign a stable conn_id.
        // Falls back to client_id=0 if the token can't be parsed (shouldn't
        // happen — the main worker already validated it before forwarding).
        let client_id: u64 = crate::auth::validate(&req, &self.env)
            .map(|c| c.client_id)
            .unwrap_or(0);
        let attachment = WsAttachment {
            client_id,
            conn_id: uuid::Uuid::new_v4().to_string(),
        };
        let _ = server.serialize_attachment(&attachment);

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

        // Reject ops with client HLC timestamps too far from server wall time.
        let now_ms = js_sys::Date::now() as u64;
        validate_clock_drift(&op, now_ms)
            .map_err(|e| worker::Error::RustError(e.to_string()))?;

        // Write-ahead: persist the op before applying the snapshot.
        self.append_wal(&crdt_id, &body).await?;
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

        // Push live query results to all matching subscribers.
        let _ = self.push_live_queries(&crdt_id).await;

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

        let now_ms = js_sys::Date::now() as u64;
        let ns: String = self.state.storage().get("ns:name").await.ok().flatten().unwrap_or_default();
        let payload = WebhookPayload {
            namespace: &ns,
            crdt_id,
            seq,
            timestamp_ms: now_ms,
        };
        let payload_json = serde_json::to_string(&payload)
            .map_err(|e| worker::Error::RustError(e.to_string()))?;

        for hook in hooks {
            if !hook.crdt_ids.is_empty() && !hook.crdt_ids.iter().any(|id| id == crdt_id || id == "*") {
                continue;
            }

            let delivered = self.send_webhook(&hook.url, hook.secret.as_deref(), &payload_json).await;
            if !delivered {
                // Queue for retry — store in KV with a unique key.
                let entry_id = format!("wdlq:{seq}:{}", hook.id);
                let entry = WebhookDlqEntry {
                    id: entry_id.clone(),
                    hook_url: hook.url.clone(),
                    hook_secret: hook.secret.clone(),
                    payload_json: payload_json.clone(),
                    attempts: 1,
                    retry_after_ms: now_ms + WEBHOOK_RETRY_DELAYS_MS[0],
                };
                if let Ok(json) = serde_json::to_string(&entry) {
                    let _ = self.state.storage().put(&entry_id, json).await;
                }
            }
        }

        Ok(())
    }

    /// Send a single webhook HTTP POST. Returns true on success (2xx), false otherwise.
    async fn send_webhook(&self, url: &str, secret: Option<&str>, body: &str) -> bool {
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Post);
        let headers = worker::Headers::new();
        let _ = headers.set("Content-Type", "application/json");
        if let Some(s) = secret {
            let _ = headers.set("X-Meridian-Secret", s);
        }
        init.with_headers(headers);
        init.with_body(Some(body.to_owned().into()));

        match Request::new_with_init(url, &init) {
            Err(_) => false,
            Ok(req) => match worker::Fetch::Request(req).send().await {
                Err(_) => false,
                Ok(resp) => resp.status_code() >= 200 && resp.status_code() < 300,
            },
        }
    }

    /// Retry failed webhook deliveries from the dead letter queue.
    /// Called by the alarm handler. Drops entries that have exceeded max attempts.
    async fn retry_dlq_webhooks(&self) -> Result<()> {
        let now_ms = js_sys::Date::now() as u64;
        let opts = worker::durable::ListOptions::new().prefix("wdlq:");
        let map = self.state.storage().list_with_options(opts).await?;

        let iter = match js_sys::try_iter(&map).ok().flatten() {
            Some(i) => i,
            None => return Ok(()),
        };

        for item in iter {
            let item = item.map_err(|e| worker::Error::RustError(format!("{e:?}")))?;
            let pair = js_sys::Array::from(&item);
            let key = pair.get(0).as_string().unwrap_or_default();
            let raw = pair.get(1).as_string().unwrap_or_default();
            let mut entry: WebhookDlqEntry = match serde_json::from_str(&raw) {
                Ok(e) => e,
                Err(_) => { let _ = self.state.storage().delete(&key).await; continue; }
            };

            // Not yet due for retry.
            if entry.retry_after_ms > now_ms {
                continue;
            }

            let delivered = self.send_webhook(&entry.hook_url, entry.hook_secret.as_deref(), &entry.payload_json).await;
            entry.attempts += 1;

            if delivered || entry.attempts > WEBHOOK_MAX_ATTEMPTS {
                // Success or exhausted — remove from DLQ.
                let _ = self.state.storage().delete(&key).await;
            } else {
                // Schedule next retry with exponential backoff.
                let delay_idx = (entry.attempts as usize - 1).min(WEBHOOK_RETRY_DELAYS_MS.len() - 1);
                entry.retry_after_ms = now_ms + WEBHOOK_RETRY_DELAYS_MS[delay_idx];
                if let Ok(json) = serde_json::to_string(&entry) {
                    let _ = self.state.storage().put(&key, json).await;
                }
            }
        }

        Ok(())
    }

    /// Compact all CRDT snapshots in this namespace: strip RGA tombstones and
    /// truncate Tree move_log entries that are no longer needed for merge replay.
    ///
    /// Iterates all `crdt:*` keys in DO KV, deserializes each snapshot, calls
    /// `CrdtValue::compact()`, and writes the compacted snapshot back. No-op for
    /// CRDT types that have no accumulated garbage (counters, ORSet, etc.).
    async fn compact_crdt_snapshots(&self) -> Result<()> {
        use meridian_core::crdt::registry::CrdtValue;

        let now_ms = js_sys::Date::now() as u64;

        let opts = worker::durable::ListOptions::new().prefix("crdt:");
        let map = self.state.storage().list_with_options(opts).await?;

        let iter = match js_sys::try_iter(&map).ok().flatten() {
            Some(i) => i,
            None => return Ok(()),
        };

        for item in iter {
            let item = item.map_err(|e| worker::Error::RustError(format!("{e:?}")))?;
            let pair = js_sys::Array::from(&item);
            let key = pair.get(0).as_string().unwrap_or_default();

            // Skip TTL expiry keys (crdt:<id>:exp) — not CRDT snapshots.
            if key.ends_with(":exp") {
                continue;
            }

            let raw_val = pair.get(1);
            let val = raw_val.as_string().unwrap_or_else(|| {
                js_sys::JSON::stringify(&raw_val)
                    .ok()
                    .and_then(|s| s.as_string())
                    .unwrap_or_default()
            });
            let stored: Bytes = match serde_json::from_str(&val) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let mut crdt: CrdtValue = match CrdtValue::from_msgpack(&stored.0) {
                Ok(v) => v,
                Err(_) => continue,
            };

            // For Presence CRDTs: run GC to evict expired entries and broadcast
            // removals to connected WebSocket clients so they update immediately.
            if let CrdtValue::Presence(ref mut presence) = crdt
                && let Some(gc_delta) = presence.gc(now_ms)
            {
                if let Ok(delta_bytes) = rmp_serde::encode::to_vec_named(&gc_delta) {
                    let crdt_id = key.trim_start_matches("crdt:");
                    let msg = ServerMsg::Delta {
                        crdt_id: crdt_id.to_string(),
                        delta_bytes: delta_bytes.into(),
                    };
                    if let Ok(msg_bytes) = msg.to_msgpack() {
                        for ws in self.state.get_websockets() {
                            let _ = ws.send_with_bytes(&msg_bytes);
                        }
                    }
                }
                // Persist the compacted snapshot.
                if let Ok(data) = crdt.to_msgpack() {
                    let _ = self.state.storage().put(&key, Bytes(data)).await;
                }
                continue;
            }

            // For RGA / Tree CRDTs: strip tombstones and stale move_log entries.
            let stats = crdt.compact();
            if (stats.tombstones_removed > 0 || stats.move_records_removed > 0)
                && let Ok(data) = crdt.to_msgpack()
            {
                let _ = self.state.storage().put(&key, Bytes(data)).await;
            }
        }

        Ok(())
    }

    /// Delete WAL entries older than `cutoff_ms` and update `wal:checkpoint`.
    ///
    /// Convergence-safe compaction: never deletes entries that any peer may
    /// still need. `wal:min_peer_seq` tracks the lowest `from_seq` ever
    /// requested by a peer via `GET /wal`. Entries at or above that seq are
    /// retained regardless of age, ensuring a reconnecting peer can always
    /// catch up by re-requesting from its last known position.
    async fn compact_wal_before(&self, cutoff_ms: u64) -> Result<()> {
        // Load the lowest seq any peer has ever requested from this DO.
        // u64::MAX means no peer has ever pulled WAL → fall back to pure time-based compaction.
        let min_peer_seq: u64 = self.state.storage()
            .get("wal:min_peer_seq").await.ok().flatten().unwrap_or(u64::MAX);

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
            // Skip metadata keys (wal:seq, wal:checkpoint, wal:min_peer_seq)
            if key == "wal:seq" || key == "wal:checkpoint" || key == "wal:min_peer_seq" {
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
            // Time guard: only consider entries old enough to be compacted.
            if entry.timestamp_ms >= cutoff_ms {
                break; // entries are in seq order ≈ time order
            }
            // Convergence guard: never compact entries a peer might still request.
            // A peer requesting from_seq=N needs all entries with seq >= N.
            // So entries with seq < min_peer_seq are safe to delete.
            if entry.seq >= min_peer_seq {
                continue;
            }
            keys_to_delete.push(key);
            last_deleted_seq = entry.seq;
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

        // Track the minimum seq any active peer has requested during this compaction
        // window. compact_wal_before reads this to avoid deleting entries a peer
        // still needs. We only update when from_seq > 0 (seq=0 means "from the
        // beginning" and is not a meaningful lower bound for compaction).
        //
        // wal:min_peer_seq is reset to u64::MAX at the start of each alarm cycle
        // so stale / decommissioned peers cannot block compaction indefinitely.
        if from_seq > 0 {
            let current_min: u64 = self.state.storage()
                .get("wal:min_peer_seq").await.ok().flatten().unwrap_or(u64::MAX);
            if from_seq < current_min {
                let _ = self.state.storage().put("wal:min_peer_seq", from_seq).await;
            }
        }

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

    /// GET /history?crdt_id=<id>[&since_seq=<n>][&limit=<n>]
    /// Returns op history for a specific CRDT, paginated by seq.
    async fn handle_history(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
        let params: HashMap<String, String> = url.query_pairs().into_owned().collect();
        let crdt_id = params.get("crdt_id").cloned().unwrap_or_default();
        let since_seq: u64 = params.get("since_seq").and_then(|v| v.parse().ok()).unwrap_or(0);
        let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(50).min(500);

        let all_entries = self.replay_wal(since_seq, None).await?;

        let filtered: Vec<_> = all_entries
            .into_iter()
            .filter(|e| e.crdt_id == crdt_id)
            .collect();

        let has_more = filtered.len() > limit;
        let page: Vec<_> = filtered.into_iter().take(limit).collect();
        let next_seq = if has_more { page.last().map(|e| e.seq + 1) } else { None };

        #[derive(Serialize)]
        struct HistoryEntry {
            seq: u64,
            timestamp_ms: u64,
            op: serde_json::Value,
        }

        #[derive(Serialize)]
        struct HistoryResponse {
            crdt_id: String,
            entries: Vec<HistoryEntry>,
            next_seq: Option<u64>,
        }

        let entries: Vec<HistoryEntry> = page
            .into_iter()
            .map(|e| {
                let op = rmp_serde::decode::from_slice::<meridian_core::crdt::registry::CrdtOp>(&e.op_bytes)
                    .ok()
                    .and_then(|op| serde_json::to_value(op).ok())
                    .unwrap_or(serde_json::Value::Null);
                HistoryEntry { seq: e.seq, timestamp_ms: e.timestamp_ms, op }
            })
            .collect();

        Response::from_json(&HistoryResponse { crdt_id, entries, next_seq })
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

    /// POST /query — execute a one-shot query against DO KV storage.
    ///
    /// Body is JSON: `{ "from": "gc:*", "aggregate": "sum", "where": { ... } }`
    /// Returns JSON: `{ "value": ..., "matched": N, "scanned": N }`
    async fn handle_query(&self, mut req: Request) -> Result<Response> {
        let payload: meridian_core::protocol::LiveQueryPayload = req.json().await
            .map_err(|_| worker::Error::RustError("invalid query body".into()))?;

        match self.run_live_query(&payload).await {
            Ok(outcome) => {
                #[derive(serde::Serialize)]
                struct QueryResponse {
                    value: serde_json::Value,
                    matched: usize,
                    scanned: usize,
                }
                Response::from_json(&QueryResponse {
                    value: outcome.value,
                    matched: outcome.matched,
                    scanned: outcome.scanned,
                })
            }
            Err(e) => Response::error(e.to_string(), 400),
        }
    }

    /// Execute a live query by scanning all CRDT snapshots in DO KV.
    ///
    /// Loads all `crdt:*` keys (excluding `:exp` metadata), deserializes them,
    /// then delegates to the pure `execute_query_on_values` core function.
    async fn run_live_query(
        &self,
        payload: &meridian_core::protocol::LiveQueryPayload,
    ) -> std::result::Result<meridian_core::query::QueryOutcome, String> {
        use std::str::FromStr as _;

        let aggregate_op = AggregateOp::from_str(&payload.aggregate)
            .map_err(|_| format!("unknown aggregate: {}", payload.aggregate))?;

        // Convert LiveQueryFilter → WhereClause (same semantics, different type
        // because LiveQueryPayload lives in the protocol layer, WhereClause in query).
        let where_clause: Option<WhereClause> = payload.filter.as_ref().map(|f| WhereClause {
            contains: f.contains.clone(),
            updated_after: f.updated_after,
        });

        let opts = worker::durable::ListOptions::new().prefix("crdt:");
        let map = self
            .state
            .storage()
            .list_with_options(opts)
            .await
            .map_err(|e| e.to_string())?;

        let iter = match js_sys::try_iter(&map).ok().flatten() {
            Some(i) => i,
            None => {
                return execute_query_on_values(
                    vec![],
                    &payload.from,
                    payload.crdt_type.as_deref(),
                    aggregate_op,
                    where_clause.as_ref(),
                )
                .map_err(map_query_error);
            }
        };

        let mut pairs: Vec<(String, meridian_core::crdt::registry::CrdtValue)> = Vec::new();

        for item in iter {
            let item = item.map_err(|e| format!("{e:?}"))?;
            let pair = js_sys::Array::from(&item);
            let key = pair.get(0).as_string().unwrap_or_default();

            // Skip TTL expiry keys.
            if key.ends_with(":exp") {
                continue;
            }

            let crdt_id = key.trim_start_matches("crdt:").to_owned();
            let raw_val = pair.get(1);
            let val_str = raw_val.as_string().unwrap_or_else(|| {
                js_sys::JSON::stringify(&raw_val)
                    .ok()
                    .and_then(|s| s.as_string())
                    .unwrap_or_default()
            });
            let stored: Bytes = match serde_json::from_str(&val_str) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let crdt_value = match meridian_core::crdt::registry::CrdtValue::from_msgpack(&stored.0) {
                Ok(v) => v,
                Err(_) => continue,
            };
            pairs.push((crdt_id, crdt_value));
        }

        execute_query_on_values(
            pairs,
            &payload.from,
            payload.crdt_type.as_deref(),
            aggregate_op,
            where_clause.as_ref(),
        )
        .map_err(map_query_error)
    }

    /// After every op, re-evaluate all live queries whose glob matches `changed_crdt_id`
    /// and push updated `ServerMsg::QueryResult` to the relevant WebSocket connections.
    ///
    /// Live query subscriptions are stored as `lq:{conn_id}:{query_id}` → msgpack
    /// `LiveQueryPayload` in DO KV. Each active WS connection tagged with the same
    /// `conn_id` receives the refreshed result.
    async fn push_live_queries(&self, changed_crdt_id: &str) -> Result<()> {
        let opts = worker::durable::ListOptions::new().prefix("lq:");
        let map = self.state.storage().list_with_options(opts).await?;

        let iter = match js_sys::try_iter(&map).ok().flatten() {
            Some(i) => i,
            None => return Ok(()),
        };

        // Build a map from conn_id → WebSocket for fast lookup.
        let all_sockets = self.state.get_websockets();
        let mut conn_to_ws: std::collections::HashMap<String, &WebSocket> = std::collections::HashMap::new();
        for ws in &all_sockets {
            if let Ok(Some(attachment)) = ws.deserialize_attachment::<WsAttachment>() {
                conn_to_ws.insert(attachment.conn_id, ws);
            }
        }

        for item in iter {
            let item = item.map_err(|e| worker::Error::RustError(format!("{e:?}")))?;
            let pair = js_sys::Array::from(&item);
            let key = pair.get(0).as_string().unwrap_or_default();

            // Key format: lq:{conn_id}:{query_id}
            // Strip the "lq:" prefix, then split at the first ':' to get conn_id.
            let rest = key.trim_start_matches("lq:");
            let (conn_id, query_id) = match rest.find(':') {
                Some(pos) => (&rest[..pos], &rest[pos + 1..]),
                None => continue,
            };

            let raw_val = pair.get(1);
            let val_str = raw_val.as_string().unwrap_or_else(|| {
                js_sys::JSON::stringify(&raw_val)
                    .ok()
                    .and_then(|s| s.as_string())
                    .unwrap_or_default()
            });
            let stored: Bytes = match serde_json::from_str(&val_str) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let payload: meridian_core::protocol::LiveQueryPayload =
                match rmp_serde::decode::from_slice(&stored.0) {
                    Ok(p) => p,
                    Err(_) => continue,
                };

            // Only re-evaluate queries that could be affected by this change.
            if !meridian_core::auth::glob_match(&payload.from, changed_crdt_id) {
                continue;
            }

            // Run the query and send the result to the matching WebSocket.
            if let Some(ws) = conn_to_ws.get(conn_id)
                && let Ok(outcome) = self.run_live_query(&payload).await
            {
                let msg = ServerMsg::QueryResult {
                    query_id: query_id.to_string(),
                    value: outcome.value,
                    matched: outcome.matched,
                };
                if let Ok(b) = msg.to_msgpack() {
                    let _ = ws.send_with_bytes(&b);
                }
            }
        }

        Ok(())
    }
}

fn map_query_error(e: meridian_core::query::QueryError) -> String {
    match e {
        meridian_core::query::QueryError::UnknownCrdtType(s) =>
            format!("unknown crdt type: {s}"),
        meridian_core::query::QueryError::IncompatibleAggregate { aggregate, crdt_type } =>
            format!("aggregate '{aggregate}' is not supported for crdt type '{crdt_type}'"),
    }
}
