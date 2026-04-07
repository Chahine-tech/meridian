use std::sync::Arc;

use dashmap::DashMap;
use meridian_core::{
    crdt::clock::VectorClock,
    protocol::{ClientMsg, LiveQueryPayload, ServerMsg},
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::{
    error::ClientError,
    handles::{
        awareness::AwarenessInner,
        lwwregister::LwwRegisterInner,
        orset::ORSetInner,
        presence::PresenceInner,
        AwarenessHandle, GCounterHandle, LwwRegisterHandle, ORSetHandle, PNCounterHandle,
        PresenceHandle, RgaHandle, TreeHandle,
    },
    op_queue::OpQueue,
    transport::{ConnectionState, Transport},
};

/// Result type for live query pushes.
#[derive(Debug, Clone)]
pub struct LiveQueryResult {
    pub query_id: String,
    pub value: serde_json::Value,
    pub matched: usize,
}

/// Meridian Rust client. Wraps a WebSocket transport and exposes typed CRDT handles.
///
/// All handles returned by `gcounter()`, `orset()`, etc. share the same inner
/// state via `Arc` — cloning a handle is cheap and safe across tasks.
pub struct MeridianClient {
    pub namespace:  String,
    pub client_id:  u64,
    transport:      Arc<dyn Transport>,
    op_queue:       Arc<OpQueue>,

    // Handle caches — one Arc<Inner> per crdt_id
    gc_handles:   DashMap<String, GCounterHandle>,
    pn_handles:   DashMap<String, PNCounterHandle>,
    // Generic handles: store Arc<Inner> directly; typed handles are reconstructed via from_inner()
    or_handles:   DashMap<String, Arc<ORSetInner>>,
    lw_handles:   DashMap<String, Arc<LwwRegisterInner>>,
    pr_handles:   DashMap<String, Arc<PresenceInner>>,
    aw_handles:   DashMap<String, Arc<AwarenessInner>>,
    rga_handles:  DashMap<String, RgaHandle>,
    tree_handles: DashMap<String, TreeHandle>,

    // Subscription tracking for re-subscribe on reconnect
    subscriptions: DashMap<String, VectorClock>,

    // Live query registry: query_id → channel sender
    live_queries:  DashMap<String, mpsc::Sender<LiveQueryResult>>,
    lq_counter:    std::sync::atomic::AtomicU64,
}

impl MeridianClient {
    /// Connect to a Meridian server over WebSocket.
    ///
    /// `url`       — WebSocket base URL, e.g. `"ws://localhost:3000"`
    /// `namespace` — Meridian namespace (room / document identifier)
    /// `token`     — auth token string
    #[cfg(feature = "ws")]
    pub async fn connect(
        url: &str,
        namespace: &str,
        token: &str,
    ) -> Result<Arc<Self>, ClientError> {
        let transport = crate::transport::ws::WsTransport::connect(url, namespace, token).await?;
        Ok(Self::from_transport(namespace, 0, transport))
    }

    /// Create a client from an existing transport. Used in tests with `FakeTransport`.
    pub fn from_transport(
        namespace: &str,
        client_id: u64,
        transport: Arc<dyn Transport>,
    ) -> Arc<Self> {
        let op_queue = Arc::new(OpQueue::new());
        let client = Arc::new(Self {
            namespace: namespace.to_owned(),
            client_id,
            transport: Arc::clone(&transport),
            op_queue,
            gc_handles:    DashMap::new(),
            pn_handles:    DashMap::new(),
            or_handles:    DashMap::new(),
            lw_handles:    DashMap::new(),
            pr_handles:    DashMap::new(),
            aw_handles:    DashMap::new(),
            rga_handles:   DashMap::new(),
            tree_handles:  DashMap::new(),
            subscriptions: DashMap::new(),
            live_queries:  DashMap::new(),
            lq_counter:    std::sync::atomic::AtomicU64::new(0),
        });
        Self::spawn_dispatch(Arc::clone(&client));
        client
    }

    fn spawn_dispatch(client: Arc<Self>) {
        let mut inbox = client.transport.subscribe_incoming();
        let mut state_rx = client.transport.state_watch();
        let client_clone = Arc::clone(&client);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Incoming server message
                    result = inbox.recv() => {
                        match result {
                            Ok(msg) => client_clone.handle_server_msg(msg),
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                warn!(dropped = n, "dispatch: inbox lagged, some messages dropped");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                debug!("dispatch: inbox closed, exiting");
                                break;
                            }
                        }
                    }

                    // Connection state change
                    _ = state_rx.changed() => {
                        let state = state_rx.borrow().clone();
                        if state == ConnectionState::Connected {
                            client_clone.on_reconnect().await;
                        }
                    }
                }
            }
        });
    }

    fn handle_server_msg(&self, msg: ServerMsg) {
        match msg {
            ServerMsg::Delta { crdt_id, delta_bytes } => {
                self.apply_delta(&crdt_id, &delta_bytes);
            }
            ServerMsg::AwarenessBroadcast { client_id, key, data } => {
                if let Some(inner) = self.aw_handles.get(&key) {
                    let h = AwarenessHandle::<serde_json::Value>::from_inner(Arc::clone(&*inner));
                    if let Err(e) = h.apply_broadcast(client_id, &data) {
                        warn!(error = %e, key = %key, "awareness apply failed");
                    }
                }
            }
            ServerMsg::QueryResult { query_id, value, matched } => {
                if let Some(tx) = self.live_queries.get(&query_id) {
                    let result = LiveQueryResult { query_id: query_id.clone(), value, matched };
                    let _ = tx.try_send(result);
                }
            }
            ServerMsg::Error { code, message } => {
                warn!(code, message = %message, "server error");
            }
            ServerMsg::Ack { .. } | ServerMsg::BatchAck { .. } => {
                // Latency tracking could be added here
            }
        }
    }

    fn apply_delta(&self, crdt_id: &str, delta_bytes: &[u8]) {
        if let Some(h) = self.gc_handles.get(crdt_id) {
            if let Err(e) = h.apply_delta(delta_bytes) {
                warn!(error = %e, crdt_id, "gcounter delta failed");
            }
            return;
        }
        if let Some(h) = self.pn_handles.get(crdt_id) {
            if let Err(e) = h.apply_delta(delta_bytes) {
                warn!(error = %e, crdt_id, "pncounter delta failed");
            }
            return;
        }
        if let Some(inner) = self.or_handles.get(crdt_id) {
            let h = ORSetHandle::<serde_json::Value>::from_inner(Arc::clone(&*inner));
            if let Err(e) = h.apply_delta(delta_bytes) {
                warn!(error = %e, crdt_id, "orset delta failed");
            }
            return;
        }
        if let Some(inner) = self.lw_handles.get(crdt_id) {
            let h = LwwRegisterHandle::<serde_json::Value>::from_inner(Arc::clone(&*inner));
            if let Err(e) = h.apply_delta(delta_bytes) {
                warn!(error = %e, crdt_id, "lww delta failed");
            }
            return;
        }
        if let Some(inner) = self.pr_handles.get(crdt_id) {
            let h = PresenceHandle::<serde_json::Value>::from_inner(Arc::clone(&*inner));
            if let Err(e) = h.apply_delta(delta_bytes) {
                warn!(error = %e, crdt_id, "presence delta failed");
            }
            return;
        }
        if let Some(h) = self.rga_handles.get(crdt_id) {
            if let Err(e) = h.apply_delta(delta_bytes) {
                warn!(error = %e, crdt_id, "rga delta failed");
            }
            return;
        }
        if let Some(h) = self.tree_handles.get(crdt_id) {
            if let Err(e) = h.apply_delta(delta_bytes) {
                warn!(error = %e, crdt_id, "tree delta failed");
            }
            return;
        }
        debug!(crdt_id, "delta received for unsubscribed CRDT");
    }

    async fn on_reconnect(&self) {
        debug!("transport reconnected — re-subscribing {} CRDTs", self.subscriptions.len());

        // Re-subscribe to all tracked CRDTs
        for entry in self.subscriptions.iter() {
            let crdt_id = entry.key().clone();
            let vc = entry.value().clone();
            let since_vc = match rmp_serde::encode::to_vec_named(&vc) {
                Ok(b) => serde_bytes::ByteBuf::from(b),
                Err(e) => {
                    warn!(error = %e, crdt_id, "failed to encode vc for re-sync");
                    continue;
                }
            };
            let _ = self.transport.send(ClientMsg::Subscribe { crdt_id: crdt_id.clone() }).await;
            let _ = self.transport.send(ClientMsg::Sync { crdt_id, since_vc }).await;
        }

        // Re-register live queries
        for entry in self.live_queries.iter() {
            let _ = entry; // Re-subscribe logic would go here
        }

        // Drain op queue
        for msg in self.op_queue.drain() {
            let _ = self.transport.send(msg).await;
        }
    }

    fn subscribe_crdt(&self, crdt_id: &str) {
        self.subscriptions.entry(crdt_id.to_owned()).or_default();
        let transport = Arc::clone(&self.transport);
        let id = crdt_id.to_owned();
        tokio::spawn(async move {
            let _ = transport.send(ClientMsg::Subscribe { crdt_id: id }).await;
        });
    }

    // ── Handle accessors ──────────────────────────────────────────────────────

    /// Get or create a GCounterHandle.
    pub fn gcounter(&self, crdt_id: &str) -> GCounterHandle {
        let h = self.gc_handles
            .entry(crdt_id.to_owned())
            .or_insert_with(|| GCounterHandle::new(
                crdt_id.to_owned(),
                self.client_id,
                Arc::clone(&self.transport),
                Arc::clone(&self.op_queue),
            ))
            .clone();
        self.subscribe_crdt(crdt_id);
        h
    }

    pub fn pncounter(&self, crdt_id: &str) -> PNCounterHandle {
        let h = self.pn_handles
            .entry(crdt_id.to_owned())
            .or_insert_with(|| PNCounterHandle::new(
                crdt_id.to_owned(),
                self.client_id,
                Arc::clone(&self.transport),
                Arc::clone(&self.op_queue),
            ))
            .clone();
        self.subscribe_crdt(crdt_id);
        h
    }

    pub fn orset<T: DeserializeOwned + Serialize + Send + Sync + 'static>(
        &self,
        crdt_id: &str,
    ) -> ORSetHandle<T> {
        let inner = self.or_handles
            .entry(crdt_id.to_owned())
            .or_insert_with(|| {
                ORSetHandle::<serde_json::Value>::new(
                    crdt_id.to_owned(),
                    self.client_id,
                    Arc::clone(&self.transport),
                    Arc::clone(&self.op_queue),
                ).into_inner()
            })
            .clone();
        self.subscribe_crdt(crdt_id);
        ORSetHandle::from_inner(inner)
    }

    pub fn lwwregister<T: DeserializeOwned + Serialize + Send + Sync + 'static>(
        &self,
        crdt_id: &str,
    ) -> LwwRegisterHandle<T> {
        let inner = self.lw_handles
            .entry(crdt_id.to_owned())
            .or_insert_with(|| {
                LwwRegisterHandle::<serde_json::Value>::new(
                    crdt_id.to_owned(),
                    self.client_id,
                    Arc::clone(&self.transport),
                    Arc::clone(&self.op_queue),
                ).into_inner()
            })
            .clone();
        self.subscribe_crdt(crdt_id);
        LwwRegisterHandle::from_inner(inner)
    }

    pub fn presence<T: DeserializeOwned + Serialize + Send + Sync + 'static>(
        &self,
        crdt_id: &str,
    ) -> PresenceHandle<T> {
        let inner = self.pr_handles
            .entry(crdt_id.to_owned())
            .or_insert_with(|| {
                PresenceHandle::<serde_json::Value>::new(
                    crdt_id.to_owned(),
                    self.client_id,
                    Arc::clone(&self.transport),
                    Arc::clone(&self.op_queue),
                ).into_inner()
            })
            .clone();
        self.subscribe_crdt(crdt_id);
        PresenceHandle::from_inner(inner)
    }

    pub fn awareness<T: DeserializeOwned + Serialize + Send + Sync + 'static>(
        &self,
        key: &str,
    ) -> AwarenessHandle<T> {
        let inner = self.aw_handles
            .entry(key.to_owned())
            .or_insert_with(|| {
                AwarenessHandle::<serde_json::Value>::new(
                    key.to_owned(),
                    Arc::clone(&self.transport),
                    Arc::clone(&self.op_queue),
                ).into_inner()
            })
            .clone();
        AwarenessHandle::from_inner(inner)
    }

    pub fn rga(&self, crdt_id: &str) -> RgaHandle {
        let h = self.rga_handles
            .entry(crdt_id.to_owned())
            .or_insert_with(|| RgaHandle::new(
                crdt_id.to_owned(),
                self.client_id,
                Arc::clone(&self.transport),
                Arc::clone(&self.op_queue),
            ))
            .clone();
        self.subscribe_crdt(crdt_id);
        h
    }

    pub fn tree(&self, crdt_id: &str) -> TreeHandle {
        let h = self.tree_handles
            .entry(crdt_id.to_owned())
            .or_insert_with(|| TreeHandle::new(
                crdt_id.to_owned(),
                self.client_id,
                Arc::clone(&self.transport),
                Arc::clone(&self.op_queue),
            ))
            .clone();
        self.subscribe_crdt(crdt_id);
        h
    }

    /// Subscribe to a live cross-CRDT query.
    /// Returns a `Receiver` that gets a push on each matching CRDT change.
    pub fn live_query(
        &self,
        pattern: &str,
        aggregate: &str,
    ) -> mpsc::Receiver<LiveQueryResult> {
        let seq = self.lq_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let query_id = format!("lq-{}-{seq}", self.client_id);
        let (tx, rx) = mpsc::channel(64);
        self.live_queries.insert(query_id.clone(), tx);

        let msg = ClientMsg::SubscribeQuery {
            query_id,
            query: LiveQueryPayload {
                from: pattern.to_owned(),
                crdt_type: None,
                aggregate: aggregate.to_owned(),
                filter: None,
            },
        };
        let transport = Arc::clone(&self.transport);
        tokio::spawn(async move { let _ = transport.send(msg).await; });
        rx
    }

    /// Watch the transport connection state.
    pub fn connection_state(&self) -> tokio::sync::watch::Receiver<ConnectionState> {
        self.transport.state_watch()
    }

    pub fn pending_op_count(&self) -> usize {
        self.op_queue.len()
    }

    pub async fn close(&self) {
        self.transport.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::fake::FakeTransport;

    fn make_client() -> (Arc<MeridianClient>, crate::transport::fake::FakeHandle) {
        let (transport, handle) = FakeTransport::new();
        let client = MeridianClient::from_transport("test-ns", 1, transport);
        (client, handle)
    }

    #[tokio::test]
    async fn gcounter_increment_sends_op() {
        let (client, fh) = make_client();
        let score = client.gcounter("gc:score");
        score.increment(5).await.unwrap();
        let sent = fh.sent();
        // Subscribe + Op
        assert!(sent.iter().any(|m| matches!(m, ClientMsg::Op { crdt_id, .. } if crdt_id == "gc:score")));
    }

    #[tokio::test]
    async fn same_crdt_id_returns_shared_handle() {
        let (client, _) = make_client();
        let h1 = client.gcounter("gc:shared");
        let h2 = client.gcounter("gc:shared");
        // Both handles share the same Arc — increment via h1 visible in h2
        h1.increment(10).await.unwrap();
        assert_eq!(h2.value(), 10);
    }

    #[tokio::test]
    async fn delta_dispatch_updates_handle() {
        let (client, fh) = make_client();
        let score = client.gcounter("gc:dispatch");

        // Inject a delta from the server
        let delta = meridian_core::crdt::gcounter::GCounterDelta {
            counters: [(99u64, 42u64)].into_iter().collect(),
        };
        let delta_bytes = rmp_serde::encode::to_vec_named(&delta).unwrap();
        fh.inject(ServerMsg::Delta {
            crdt_id: "gc:dispatch".into(),
            delta_bytes: serde_bytes::ByteBuf::from(delta_bytes),
        });

        // Give the dispatch loop time to process
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(score.value(), 42);
    }

    #[tokio::test]
    async fn reconnect_resubscribes() {
        let (client, fh) = make_client();
        let _score = client.gcounter("gc:resub");

        // Clear initial Subscribe
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let _ = fh.sent();

        // Simulate reconnect
        fh.reconnect();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let sent = fh.sent();
        assert!(sent.iter().any(|m| matches!(m, ClientMsg::Subscribe { crdt_id } if crdt_id == "gc:resub")));
    }
}
