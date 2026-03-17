/// Integration tests for anti-entropy gossip.
///
/// Uses in-memory fakes for WalBackend, AntiEntropyApplier, and LocalBroadcast
/// so the test has no external dependencies.
///
/// Time is controlled via `tokio::time::pause` + `advance` — no real sleeps.
#[cfg(feature = "transport-http")]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        Mutex,
    };
    use std::time::Duration;

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use tokio_util::sync::CancellationToken;

    use meridian_cluster::{
        AntiEntropyApplier, ClusterConfig, ClusterTransport, DeltaEnvelope,
        LocalBroadcast, NodeId, Result as ClusterResult, run_anti_entropy,
    };
    use meridian_storage::{WalBackend, WalEntry};

    // -------------------------------------------------------------------------
    // Fakes
    // -------------------------------------------------------------------------

    struct FakeWal {
        entries: Vec<WalEntry>,
        last: AtomicU64,
    }

    impl FakeWal {
        fn new(entries: Vec<WalEntry>) -> Self {
            let last = entries.last().map(|e| e.seq).unwrap_or(0);
            Self { entries, last: AtomicU64::new(last) }
        }

        fn with_last_zero(entries: Vec<WalEntry>) -> Self {
            Self { entries, last: AtomicU64::new(0) }
        }
    }

    impl WalBackend for FakeWal {
        async fn append(&self, _: &str, _: &str, _: Vec<u8>) -> meridian_storage::Result<u64> {
            unimplemented!()
        }
        async fn replay_from(&self, from_seq: u64) -> meridian_storage::Result<Vec<WalEntry>> {
            Ok(self.entries.iter().filter(|e| e.seq >= from_seq).cloned().collect())
        }
        async fn truncate_before(&self, _: u64) -> meridian_storage::Result<()> { Ok(()) }
        fn last_seq(&self) -> u64 { self.last.load(Ordering::Relaxed) }
        fn checkpoint_seq(&self) -> u64 { 0 }
        async fn set_checkpoint_seq(&self, _: u64) -> meridian_storage::Result<()> { Ok(()) }
    }

    struct EchoApplier {
        /// std::sync::Mutex — safe to use in sync context (publish_delta trait method).
        calls: Mutex<Vec<(String, String)>>,
        response: Option<Vec<u8>>,
    }

    impl EchoApplier {
        fn returning(delta: &[u8]) -> Self {
            Self { calls: Mutex::new(vec![]), response: Some(delta.to_vec()) }
        }
        fn noop() -> Self {
            Self { calls: Mutex::new(vec![]), response: None }
        }
        fn call_count(&self) -> usize { self.calls.lock().unwrap().len() }
    }

    impl AntiEntropyApplier for EchoApplier {
        async fn apply_wal_op(
            &self, namespace: &str, crdt_id: &str, _op_bytes: Vec<u8>,
        ) -> Result<Option<Vec<u8>>, String> {
            self.calls.lock().unwrap().push((namespace.to_owned(), crdt_id.to_owned()));
            Ok(self.response.clone())
        }
    }

    struct RecordingBroadcast {
        /// std::sync::Mutex — publish_delta is a sync trait method; no async context here.
        published: Mutex<Vec<(String, String, Vec<u8>)>>,
    }

    impl RecordingBroadcast {
        fn new() -> Self { Self { published: Mutex::new(vec![]) } }
        fn count(&self) -> usize { self.published.lock().unwrap().len() }
    }

    impl LocalBroadcast for RecordingBroadcast {
        fn publish_delta(&self, namespace: &str, crdt_id: &str, delta_bytes: Bytes) {
            self.published.lock().unwrap().push((
                namespace.to_owned(),
                crdt_id.to_owned(),
                delta_bytes.to_vec(),
            ));
        }
    }

    struct RecordingTransport {
        sent: Mutex<Vec<DeltaEnvelope>>,
    }

    impl RecordingTransport {
        fn new() -> Arc<Self> { Arc::new(Self { sent: Mutex::new(vec![]) }) }
        fn count(&self) -> usize { self.sent.lock().unwrap().len() }
    }

    #[async_trait::async_trait]
    impl ClusterTransport for RecordingTransport {
        async fn broadcast_delta(&self, envelope: DeltaEnvelope) -> ClusterResult<()> {
            self.sent.lock().unwrap().push(envelope);
            Ok(())
        }
        fn subscribe_deltas(&self) -> BoxStream<'static, DeltaEnvelope> {
            Box::pin(futures::stream::pending())
        }
    }

    fn config(interval_ms: u64) -> Arc<ClusterConfig> {
        Arc::new(ClusterConfig {
            node_id: NodeId(1),
            peers: vec![],
            redis_url: None,
            anti_entropy_interval: Duration::from_millis(interval_ms),
        })
    }

    fn entry(seq: u64, ns: &str, crdt_id: &str) -> WalEntry {
        WalEntry {
            seq,
            namespace: ns.to_owned(),
            crdt_id: crdt_id.to_owned(),
            op_bytes: b"op".to_vec(),
            timestamp_ms: 0,
        }
    }

    fn spawn(
        wal: Arc<FakeWal>,
        applier: Arc<EchoApplier>,
        broadcast: Arc<RecordingBroadcast>,
        transport: Arc<RecordingTransport>,
        cfg: Arc<ClusterConfig>,
    ) -> CancellationToken {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let transport_dyn: Arc<dyn ClusterTransport> = transport;
        tokio::spawn(async move {
            run_anti_entropy(wal, applier, broadcast, transport_dyn, NodeId(1), cfg, cancel_clone).await;
        });
        cancel
    }

    // -------------------------------------------------------------------------
    // Test: WAL entries replayed → applier called, deltas published + broadcast
    // -------------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn replays_wal_and_broadcasts() {
        let wal = Arc::new(FakeWal::with_last_zero(vec![
            entry(1, "ns-a", "crdt-1"),
            entry(2, "ns-a", "crdt-2"),
        ]));

        let applier = Arc::new(EchoApplier::returning(b"delta"));
        let broadcast = Arc::new(RecordingBroadcast::new());
        let transport = RecordingTransport::new();

        let cancel = spawn(
            Arc::clone(&wal),
            Arc::clone(&applier),
            Arc::clone(&broadcast),
            Arc::clone(&transport),
            config(50),
        );

        // Yield so anti-entropy records last_replayed = 0 before we advance the WAL.
        tokio::task::yield_now().await;
        wal.last.store(2, Ordering::Relaxed);

        // Advance mock time past one anti-entropy interval and yield to let it run.
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        cancel.cancel();

        assert_eq!(applier.call_count(), 2, "applier called for each entry");
        assert_eq!(broadcast.count(), 2, "2 deltas published locally");
        assert_eq!(transport.count(), 2, "2 deltas broadcast to peers");
    }

    // -------------------------------------------------------------------------
    // Test: no-op applier → nothing published or broadcast
    // -------------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn noop_op_not_published() {
        let wal = Arc::new(FakeWal::with_last_zero(vec![entry(1, "ns", "crdt")]));
        let applier = Arc::new(EchoApplier::noop());
        let broadcast = Arc::new(RecordingBroadcast::new());
        let transport = RecordingTransport::new();

        let cancel = spawn(
            Arc::clone(&wal),
            Arc::clone(&applier),
            Arc::clone(&broadcast),
            Arc::clone(&transport),
            config(50),
        );

        tokio::task::yield_now().await;
        wal.last.store(1, Ordering::Relaxed);

        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        cancel.cancel();

        assert_eq!(applier.call_count(), 1, "applier still called");
        assert_eq!(broadcast.count(), 0, "no-op → nothing published");
        assert_eq!(transport.count(), 0, "no-op → nothing broadcast");
    }

    // -------------------------------------------------------------------------
    // Test: WAL not advanced → no replay at all
    // -------------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn no_replay_when_wal_unchanged() {
        let wal = Arc::new(FakeWal::new(vec![]));
        let applier = Arc::new(EchoApplier::returning(b"d"));
        let broadcast = Arc::new(RecordingBroadcast::new());
        let transport = RecordingTransport::new();

        let cancel = spawn(
            Arc::clone(&wal),
            Arc::clone(&applier),
            Arc::clone(&broadcast),
            Arc::clone(&transport),
            config(50),
        );

        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        cancel.cancel();

        assert_eq!(applier.call_count(), 0, "no replay when WAL unchanged");
        assert_eq!(broadcast.count(), 0);
        assert_eq!(transport.count(), 0);
    }
}
