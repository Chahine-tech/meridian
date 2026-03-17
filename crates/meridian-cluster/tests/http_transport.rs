/// Integration tests for `HttpPushTransport`.
///
/// Tests spin up real Axum listeners on random loopback ports — no Redis needed.
///
/// Pattern for each test:
///   1. Bind N listeners on 127.0.0.1:0 (random available ports).
///   2. Create N transports, each pointing at the others as peers.
///   3. Serve the internal router on each listener.
///   4. Subscribe / broadcast / assert.
#[cfg(feature = "transport-http")]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::StreamExt;
    use tokio::net::TcpListener;
    use url::Url;

    use meridian_cluster::{ClusterTransport, DeltaEnvelope, HttpPushTransport, NodeId};

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    async fn bind() -> (TcpListener, Url) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
        (listener, url)
    }

    fn make_transport(peers: Vec<Url>, id: u64) -> Arc<HttpPushTransport> {
        Arc::new(HttpPushTransport::new(peers, NodeId(id)))
    }

    fn make_envelope(origin: u64, ns: &str, crdt_id: &str, payload: &[u8]) -> DeltaEnvelope {
        DeltaEnvelope::new(NodeId(origin), ns, crdt_id, Bytes::copy_from_slice(payload))
    }

    fn serve(listener: TcpListener, transport: Arc<HttpPushTransport>) {
        let router = transport.router();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
    }

    // -------------------------------------------------------------------------
    // Test: A broadcasts → B receives
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn broadcast_received_by_peer() {
        let (listener_a, url_a) = bind().await;
        let (listener_b, url_b) = bind().await;

        let transport_a = make_transport(vec![url_b.clone()], 1);
        let transport_b = make_transport(vec![url_a.clone()], 2);

        serve(listener_a, Arc::clone(&transport_a));
        serve(listener_b, Arc::clone(&transport_b));

        let mut stream_b = transport_b.subscribe_deltas();

        tokio::time::sleep(Duration::from_millis(50)).await;

        let envelope = make_envelope(1, "my-ns", "counter-1", b"delta-payload");
        transport_a.broadcast_delta(envelope).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(2), stream_b.next())
            .await
            .expect("timed out waiting for delta on B")
            .expect("stream ended unexpectedly");

        assert_eq!(received.namespace, "my-ns");
        assert_eq!(received.crdt_id, "counter-1");
        assert_eq!(received.delta_bytes, b"delta-payload");
        assert_eq!(received.origin_node_id, NodeId(1));
    }

    // -------------------------------------------------------------------------
    // Test: self-originating messages are dropped (loop prevention)
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn self_message_is_dropped() {
        let (listener_b, url_b) = bind().await;
        let transport_b = make_transport(vec![], 2);

        serve(listener_b, Arc::clone(&transport_b));

        let mut stream_b = transport_b.subscribe_deltas();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Manually POST a delta that claims to originate from node 2 (itself).
        let self_envelope = make_envelope(2, "ns", "crdt", b"self-delta");
        let payload = rmp_serde::encode::to_vec_named(&self_envelope).unwrap();

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{url_b}internal/cluster/delta"))
            .header("Content-Type", "application/msgpack")
            .body(payload)
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "handler should return 200 even for self-messages");

        // Nothing should arrive on the stream.
        let result = tokio::time::timeout(Duration::from_millis(200), stream_b.next()).await;
        assert!(result.is_err(), "self-originating delta should be silently dropped");
    }

    // -------------------------------------------------------------------------
    // Test: 3-node fan-out — A broadcasts, both B and C receive
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn three_node_fanout() {
        let (listener_a, _url_a) = bind().await;
        let (listener_b, url_b) = bind().await;
        let (listener_c, url_c) = bind().await;

        // A sends to B and C; B and C have no peers (they only receive here)
        let transport_a = make_transport(vec![url_b.clone(), url_c.clone()], 10);
        let transport_b = make_transport(vec![], 11);
        let transport_c = make_transport(vec![], 12);

        serve(listener_a, Arc::clone(&transport_a));
        serve(listener_b, Arc::clone(&transport_b));
        serve(listener_c, Arc::clone(&transport_c));

        let mut stream_b = transport_b.subscribe_deltas();
        let mut stream_c = transport_c.subscribe_deltas();

        tokio::time::sleep(Duration::from_millis(50)).await;

        let envelope = make_envelope(10, "shared-ns", "orset-1", b"fanout-delta");
        transport_a.broadcast_delta(envelope).await.unwrap();

        let recv_b = tokio::time::timeout(Duration::from_secs(2), stream_b.next())
            .await
            .expect("B: timed out waiting for delta")
            .expect("B: stream ended");

        let recv_c = tokio::time::timeout(Duration::from_secs(2), stream_c.next())
            .await
            .expect("C: timed out waiting for delta")
            .expect("C: stream ended");

        assert_eq!(recv_b.namespace, "shared-ns");
        assert_eq!(recv_b.delta_bytes, b"fanout-delta");
        assert_eq!(recv_c.namespace, "shared-ns");
        assert_eq!(recv_c.delta_bytes, b"fanout-delta");
    }

    // -------------------------------------------------------------------------
    // Test: bad payload returns 400
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn bad_payload_returns_400() {
        let (listener, url) = bind().await;
        let transport = make_transport(vec![], 99);

        serve(listener, transport);

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{url}internal/cluster/delta"))
            .header("Content-Type", "application/msgpack")
            .body(b"not-valid-msgpack".to_vec())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status().as_u16(), 400);
    }

    // -------------------------------------------------------------------------
    // Test: unreachable peer returns error but doesn't panic
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn unreachable_peer_returns_error() {
        // Point at a port that has no listener
        let dead_url = Url::parse("http://127.0.0.1:1").unwrap();
        let transport = make_transport(vec![dead_url], 50);

        let envelope = make_envelope(50, "ns", "crdt", b"bytes");
        let result = transport.broadcast_delta(envelope).await;

        assert!(result.is_err(), "should return error when all peers are unreachable");
    }
}
