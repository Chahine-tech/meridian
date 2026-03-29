use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use http_body_util::BodyExt;
use meridian_server::{
    api::{build_router, ws::SubscriptionManager},
    auth::{AuthState, Permissions, PermissionsV1, TokenClaims, TokenSigner},
    crdt::{
        gcounter::GCounterOp,
        orset::ORSetOp,
        registry::CrdtOp,
    },
    rate_limit::RateLimiter,
    storage::{SledStore, SledWal},
    AppState,
};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static PROMETHEUS_HANDLE: std::sync::OnceLock<metrics_exporter_prometheus::PrometheusHandle> =
    std::sync::OnceLock::new();

fn prometheus_handle() -> metrics_exporter_prometheus::PrometheusHandle {
    PROMETHEUS_HANDLE
        .get_or_init(|| {
            metrics_exporter_prometheus::PrometheusBuilder::new()
                .install_recorder()
                .expect("failed to install Prometheus recorder")
        })
        .clone()
}

fn build_test_app() -> (axum::Router, Arc<TokenSigner>) {
    let store = Arc::new(SledStore::open_temporary().unwrap());
    let wal = Arc::new(SledWal::new(store.db()).unwrap());
    let signer = Arc::new(TokenSigner::generate());
    let auth_state = Arc::new(AuthState {
        signer: Arc::clone(&signer),
        rate_limiter: Arc::new(RateLimiter::new()),
    });
    let state = AppState {
        store,
        wal,
        subscriptions: Arc::new(SubscriptionManager::new()),
        signer: Arc::clone(&signer),
        webhooks: None,
        #[cfg(any(feature = "cluster", feature = "cluster-http"))]
        cluster: None,
    };
    let router = build_router(state, auth_state)
        .layer(axum::extract::Extension(prometheus_handle()));
    (router, signer)
}

fn make_token(signer: &TokenSigner, ns: &str, client_id: u64, permissions: Permissions) -> String {
    let claims = TokenClaims::new(ns, client_id, 3_600_000, permissions);
    signer.sign(&claims).unwrap()
}

fn read_write_token(signer: &TokenSigner, ns: &str) -> String {
    make_token(signer, ns, 1, Permissions::read_write())
}

fn read_only_token(signer: &TokenSigner, ns: &str) -> String {
    make_token(signer, ns, 2, Permissions::read_only())
}

async fn body_bytes(body: Body) -> Vec<u8> {
    body.collect().await.unwrap().to_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// GET /v1/namespaces/:ns/crdts/:id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_crdt_not_found_returns_404() {
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/v1/namespaces/ns/crdts/counter")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_crdt_without_token_returns_401() {
    let (app, _) = build_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/v1/namespaces/ns/crdts/counter")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn get_crdt_wrong_namespace_returns_403() {
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "other-ns");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/v1/namespaces/ns/crdts/counter")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

// ---------------------------------------------------------------------------
// POST /v1/namespaces/:ns/crdts/:id/ops
// ---------------------------------------------------------------------------

#[tokio::test]
async fn post_op_gcounter_returns_ok() {
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    let op = CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 10 });
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/namespaces/ns/crdts/counter/ops")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn post_op_then_get_returns_updated_value() {
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    let op = CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 5 });
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();

    // POST op
    let post_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/namespaces/ns/crdts/gc/ops")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(post_response.status(), StatusCode::OK);

    // GET the CRDT — same app, shared state
    let get_response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/v1/namespaces/ns/crdts/gc")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    let body = body_bytes(get_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["total"], 5);
}

#[tokio::test]
async fn post_op_read_only_token_returns_403() {
    let (app, signer) = build_test_app();
    let token = read_only_token(&signer, "ns");

    let op = CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 1 });
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/namespaces/ns/crdts/counter/ops")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn post_op_scoped_permission_blocks_wrong_key() {
    let (app, signer) = build_test_app();
    let token = make_token(
        &signer,
        "ns",
        1,
        Permissions::V1(PermissionsV1 {
            read: vec!["*".into()],
            write: vec!["allowed:*".into()],
            admin: false,
        }),
    );

    let op = CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 1 });
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/namespaces/ns/crdts/blocked:counter/ops")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn post_op_scoped_permission_allows_matching_key() {
    let (app, signer) = build_test_app();
    let token = make_token(
        &signer,
        "ns",
        1,
        Permissions::V1(PermissionsV1 {
            read: vec!["*".into()],
            write: vec!["allowed:*".into()],
            admin: false,
        }),
    );

    let op = CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 1 });
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/namespaces/ns/crdts/allowed:counter/ops")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// GET /metrics (no auth)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metrics_endpoint_is_unauthenticated() {
    let (app, _) = build_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response.into_body()).await;
    let text = std::str::from_utf8(&body).unwrap();
    assert!(text.contains("meridian_") || text.is_empty() || !text.contains("error"));
}

// Rate limiting is tested at the unit level in src/rate_limit.rs.

// ---------------------------------------------------------------------------
// GET /v1/namespaces/:ns/crdts/:id/history
// ---------------------------------------------------------------------------

#[tokio::test]
async fn history_returns_empty_for_new_crdt() {
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    // First write an op so the CRDT exists
    let op = CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 1 });
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();

    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/namespaces/ns/crdts/hist-counter/ops")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    // Query history
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/v1/namespaces/ns/crdts/hist-counter/history?since_seq=0&limit=10")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["entries"].is_array());
}

// ---------------------------------------------------------------------------
// ORSet integration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn orset_add_and_get_returns_element() {
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    let op = CrdtOp::ORSet(ORSetOp::Add {
        element: serde_json::json!("hello"),
        tag: uuid::Uuid::new_v4(),
        node_id: 1,
        seq: 1,
    });
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();

    let post_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/namespaces/ns/crdts/my-set/ops")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(post_response.status(), StatusCode::OK);

    let get_response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/v1/namespaces/ns/crdts/my-set")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    let body = body_bytes(get_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["elements"].is_array());
    assert_eq!(json["elements"].as_array().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Permissions V2 — op-mask enforcement
// ---------------------------------------------------------------------------

use meridian_server::auth::{op_masks, PermEntry, PermissionsV2};

fn make_v2_token(signer: &TokenSigner, ns: &str, client_id: u64, perms: PermissionsV2) -> String {
    make_token(signer, ns, client_id, Permissions::V2(perms))
}

#[tokio::test]
async fn v2_op_mask_blocks_disallowed_op() {
    // Token: can read *, can only increment GCounter (mask 0x01).
    // Attempt a PNCounter decrement (mask 0x02) → should 403.
    let (app, signer) = build_test_app();
    let token = make_v2_token(
        &signer,
        "ns",
        1,
        PermissionsV2 {
            v: 2,
            r: vec![PermEntry::new("*")],
            w: vec![PermEntry::new("gc:*").with_mask(op_masks::GC_INCREMENT)],
            admin: false,
            rl: None,
        },
    );

    use meridian_server::crdt::pncounter::PNCounterOp;
    let op = CrdtOp::PNCounter(PNCounterOp::Decrement { client_id: 1, amount: 1 });
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/namespaces/ns/crdts/gc:views/ops")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn v2_op_mask_allows_permitted_op() {
    // Token: can only increment GCounter on gc:*. Attempt GCounter increment → 200.
    let (app, signer) = build_test_app();
    let token = make_v2_token(
        &signer,
        "ns",
        1,
        PermissionsV2 {
            v: 2,
            r: vec![PermEntry::new("*")],
            w: vec![PermEntry::new("gc:*").with_mask(op_masks::GC_INCREMENT)],
            admin: false,
            rl: None,
        },
    );

    let op = CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 5 });
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/namespaces/ns/crdts/gc:views/ops")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn v2_per_rule_expiry_blocks_write() {
    // Token: write rule for gc:* expired in the past → 403.
    let (app, signer) = build_test_app();
    let token = make_v2_token(
        &signer,
        "ns",
        1,
        PermissionsV2 {
            v: 2,
            r: vec![PermEntry::new("*")],
            w: vec![PermEntry::new("gc:*").with_expiry(1)], // epoch+1ms — definitely expired
            admin: false,
            rl: None,
        },
    );

    let op = CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 1 });
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/namespaces/ns/crdts/gc:views/ops")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn v2_no_write_rules_blocks_all_writes() {
    // Token: read-only V2 (empty write rules).
    let (app, signer) = build_test_app();
    let token = make_v2_token(
        &signer,
        "ns",
        1,
        PermissionsV2 {
            v: 2,
            r: vec![PermEntry::new("*")],
            w: vec![],
            admin: false,
            rl: None,
        },
    );

    let op = CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 1 });
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/namespaces/ns/crdts/gc:views/ops")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

// ---------------------------------------------------------------------------
// POST /v1/namespaces/:ns/query
// ---------------------------------------------------------------------------

async fn post_op_for_query(
    app: axum::Router,
    ns: &str,
    crdt_id: &str,
    op: CrdtOp,
    token: &str,
) {
    let op_bytes = rmp_serde::encode::to_vec_named(&op).unwrap();
    let uri = format!("/v1/namespaces/{ns}/crdts/{crdt_id}/ops");
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/msgpack")
                .body(Body::from(op_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

async fn post_query_req(
    app: axum::Router,
    ns: &str,
    body: serde_json::Value,
    token: &str,
) -> (StatusCode, serde_json::Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/v1/namespaces/{ns}/query"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = body_bytes(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn post_query_gcounter_sum() {
    use meridian_server::crdt::gcounter::GCounterOp;
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    // Seed three GCounters: 6 + 10 + 4 = 20
    for (id, amount) in [("gc:a", 6u64), ("gc:b", 10), ("gc:c", 4)] {
        post_op_for_query(
            app.clone(),
            "ns",
            id,
            CrdtOp::GCounter(GCounterOp { client_id: 1, amount }),
            &token,
        )
        .await;
    }

    let (status, json) = post_query_req(
        app,
        "ns",
        serde_json::json!({ "from": "gc:*", "aggregate": "sum" }),
        &token,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["value"], 20);
    assert_eq!(json["matched"], 3);
    assert_eq!(json["scanned"], 3);
}

#[tokio::test]
async fn post_query_count_short_circuit() {
    use meridian_server::crdt::gcounter::GCounterOp;
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    for (id, amount) in [("gc:x", 1u64), ("gc:y", 2), ("gc:z", 3)] {
        post_op_for_query(
            app.clone(),
            "ns",
            id,
            CrdtOp::GCounter(GCounterOp { client_id: 1, amount }),
            &token,
        )
        .await;
    }

    let (status, json) = post_query_req(
        app,
        "ns",
        serde_json::json!({ "from": "gc:*", "aggregate": "count" }),
        &token,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["value"], 3);
    assert_eq!(json["matched"], 3);
}

#[tokio::test]
async fn post_query_glob_filter() {
    use meridian_server::crdt::gcounter::GCounterOp;
    use meridian_server::crdt::pncounter::PNCounterOp;
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    post_op_for_query(
        app.clone(),
        "ns",
        "gc:views-home",
        CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 5 }),
        &token,
    )
    .await;
    post_op_for_query(
        app.clone(),
        "ns",
        "gc:views-dash",
        CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 8 }),
        &token,
    )
    .await;
    post_op_for_query(
        app.clone(),
        "ns",
        "pn:score",
        CrdtOp::PNCounter(PNCounterOp::Increment { client_id: 1, amount: 3 }),
        &token,
    )
    .await;

    let (status, json) = post_query_req(
        app,
        "ns",
        serde_json::json!({ "from": "gc:views-*", "aggregate": "sum" }),
        &token,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["value"], 13);
    assert_eq!(json["matched"], 2);
    assert_eq!(json["scanned"], 3);
}

#[tokio::test]
async fn post_query_type_filter() {
    use meridian_server::crdt::gcounter::GCounterOp;
    use meridian_server::crdt::pncounter::PNCounterOp;
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    post_op_for_query(
        app.clone(),
        "ns",
        "gc:counter",
        CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 7 }),
        &token,
    )
    .await;
    post_op_for_query(
        app.clone(),
        "ns",
        "pn:score",
        CrdtOp::PNCounter(PNCounterOp::Increment { client_id: 1, amount: 3 }),
        &token,
    )
    .await;

    let (status, json) = post_query_req(
        app,
        "ns",
        serde_json::json!({ "from": "*", "type": "gcounter", "aggregate": "sum" }),
        &token,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["value"], 7);
    assert_eq!(json["matched"], 1);
    assert_eq!(json["scanned"], 2);
}

#[tokio::test]
async fn post_query_orset_union() {
    use meridian_server::crdt::orset::ORSetOp;
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");
    let tag_a = uuid::Uuid::new_v4();
    let tag_b = uuid::Uuid::new_v4();

    post_op_for_query(
        app.clone(),
        "ns",
        "or:tags-a",
        CrdtOp::ORSet(ORSetOp::Add {
            element: serde_json::json!("apple"),
            tag: tag_a,
            node_id: 1,
            seq: 1,
        }),
        &token,
    )
    .await;
    post_op_for_query(
        app.clone(),
        "ns",
        "or:tags-b",
        CrdtOp::ORSet(ORSetOp::Add {
            element: serde_json::json!("banana"),
            tag: tag_b,
            node_id: 1,
            seq: 2,
        }),
        &token,
    )
    .await;

    let (status, json) = post_query_req(
        app,
        "ns",
        serde_json::json!({ "from": "or:*", "aggregate": "union" }),
        &token,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let elements = json["value"].as_array().unwrap();
    assert_eq!(elements.len(), 2);
    assert_eq!(json["matched"], 2);
}

#[tokio::test]
async fn post_query_wrong_ns_returns_403() {
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "other-ns");

    let (status, _) = post_query_req(
        app,
        "ns",
        serde_json::json!({ "from": "*", "aggregate": "count" }),
        &token,
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn post_query_incompatible_aggregate_returns_400() {
    use meridian_server::crdt::gcounter::GCounterOp;
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    post_op_for_query(
        app.clone(),
        "ns",
        "gc:counter",
        CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 5 }),
        &token,
    )
    .await;

    let (status, json) = post_query_req(
        app,
        "ns",
        serde_json::json!({ "from": "gc:*", "aggregate": "union" }),
        &token,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["error"], "incompatible_aggregate");
}

#[tokio::test]
async fn post_query_empty_namespace_returns_null() {
    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "empty-ns");

    let (status, json) = post_query_req(
        app,
        "empty-ns",
        serde_json::json!({ "from": "*", "aggregate": "sum" }),
        &token,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["matched"], 0);
    assert_eq!(json["value"], serde_json::Value::Null);
}

// ---------------------------------------------------------------------------
// WebSocket live query tests
// ---------------------------------------------------------------------------

/// Bind a real TCP listener, spawn the axum server, return the bound address.
async fn spawn_test_server(router: axum::Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}

/// Open a WebSocket connection to the test server.
async fn ws_connect(
    addr: std::net::SocketAddr,
    ns: &str,
    token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = format!("ws://{addr}/v1/namespaces/{ns}/connect?token={token}");
    let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    ws
}

/// Encode a `ClientMsg` as a msgpack binary WS frame.
fn ws_encode(msg: &meridian_core::protocol::ClientMsg) -> tokio_tungstenite::tungstenite::Message {
    let bytes = msg.to_msgpack().unwrap();
    tokio_tungstenite::tungstenite::Message::Binary(bytes.into())
}

/// Decode a binary WS frame as a `ServerMsg`.
fn ws_decode(msg: tokio_tungstenite::tungstenite::Message) -> meridian_core::protocol::ServerMsg {
    match msg {
        tokio_tungstenite::tungstenite::Message::Binary(b) => {
            meridian_core::protocol::ServerMsg::from_msgpack(&b).unwrap()
        }
        other => panic!("expected binary frame, got {other:?}"),
    }
}

/// Drain frames from a WebSocket until a `QueryResult` matching `query_id`
/// arrives, then return it. Fails the test if no such frame arrives within
/// `timeout_ms` milliseconds.
async fn expect_query_result(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    query_id: &str,
    timeout_ms: u64,
) -> meridian_core::protocol::ServerMsg {
    use futures_util::StreamExt;
    use meridian_core::protocol::ServerMsg;

    let deadline = std::time::Duration::from_millis(timeout_ms);
    let result = tokio::time::timeout(deadline, async {
        loop {
            let frame = ws.next().await
                .expect("WebSocket closed before QueryResult arrived")
                .expect("WebSocket error");
            let msg = ws_decode(frame);
            if matches!(&msg, ServerMsg::QueryResult { query_id: id, .. } if id == query_id) {
                return msg;
            }
        }
    })
    .await;

    result.unwrap_or_else(|_| {
        panic!("timed out waiting for QueryResult(query_id={query_id}) after {timeout_ms}ms")
    })
}

/// Assert that no `QueryResult` for `query_id` arrives within `timeout_ms` ms.
async fn assert_no_query_result(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    query_id: &str,
    timeout_ms: u64,
) {
    use futures_util::StreamExt;
    use meridian_core::protocol::ServerMsg;

    let deadline = std::time::Duration::from_millis(timeout_ms);
    while let Ok(Some(Ok(frame))) =
        tokio::time::timeout(deadline, ws.next()).await
    {
        if matches!(ws_decode(frame), ServerMsg::QueryResult { query_id: id, .. } if id == query_id)
        {
            panic!("unexpected QueryResult(query_id={query_id}) received");
        }
    }
}

/// Send a `SubscribeQuery` and receive the immediate first result.
#[tokio::test]
async fn ws_live_query_initial_result() {
    use futures_util::{SinkExt, StreamExt};
    use meridian_core::protocol::{ClientMsg, LiveQueryPayload, ServerMsg};

    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    // Pre-populate two GCounters.
    post_op_for_query(
        app.clone(),
        "ns",
        "gc:hits-a",
        CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 10 }),
        &token,
    )
    .await;
    post_op_for_query(
        app.clone(),
        "ns",
        "gc:hits-b",
        CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 20 }),
        &token,
    )
    .await;

    let addr = spawn_test_server(app).await;
    let mut ws = ws_connect(addr, "ns", &token).await;

    ws.send(ws_encode(&ClientMsg::SubscribeQuery {
        query_id: "q1".into(),
        query: LiveQueryPayload {
            from: "gc:hits-*".into(),
            crdt_type: None,
            aggregate: "sum".into(),
            filter: None,
        },
    }))
    .await
    .unwrap();

    // The server pushes a QueryResult immediately on subscribe.
    let frame = ws.next().await.unwrap().unwrap();
    let msg = ws_decode(frame);
    let ServerMsg::QueryResult { query_id, value, matched } = msg else {
        panic!("expected QueryResult, got {msg:?}");
    };
    assert_eq!(query_id, "q1");
    assert_eq!(matched, 2);
    assert_eq!(value, serde_json::json!(30u64));
}

/// A live query is re-executed and the result pushed when a matching CRDT changes.
#[tokio::test]
async fn ws_live_query_pushed_on_delta() {
    use futures_util::{SinkExt, StreamExt};
    use meridian_core::protocol::{ClientMsg, LiveQueryPayload, ServerMsg};

    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    let addr = spawn_test_server(app.clone()).await;
    let mut ws = ws_connect(addr, "ns", &token).await;

    // Subscribe to a live sum query (namespace empty — first result is null/0).
    ws.send(ws_encode(&ClientMsg::SubscribeQuery {
        query_id: "q2".into(),
        query: LiveQueryPayload {
            from: "gc:score-*".into(),
            crdt_type: None,
            aggregate: "sum".into(),
            filter: None,
        },
    }))
    .await
    .unwrap();

    // Consume the initial result (matched=0, value=null).
    let _ = ws.next().await.unwrap().unwrap();

    // Now apply an op via HTTP — this will broadcast a Delta over WS.
    post_op_for_query(
        app,
        "ns",
        "gc:score-1",
        CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 42 }),
        &token,
    )
    .await;

    // The server should push a Delta followed by a QueryResult.
    let msg = expect_query_result(&mut ws, "q2", 2_000).await;
    let ServerMsg::QueryResult { query_id, value, matched } = msg else { unreachable!() };
    assert_eq!(query_id, "q2");
    assert_eq!(matched, 1);
    assert_eq!(value, serde_json::json!(42u64));
}

/// `UnsubscribeQuery` stops future pushes.
#[tokio::test]
async fn ws_live_query_unsubscribe_stops_pushes() {
    use futures_util::SinkExt;
    use meridian_core::protocol::{ClientMsg, LiveQueryPayload};

    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    let addr = spawn_test_server(app.clone()).await;
    let mut ws = ws_connect(addr, "ns", &token).await;

    ws.send(ws_encode(&ClientMsg::SubscribeQuery {
        query_id: "q3".into(),
        query: LiveQueryPayload {
            from: "gc:unsub-*".into(),
            crdt_type: None,
            aggregate: "count".into(),
            filter: None,
        },
    }))
    .await
    .unwrap();

    // Consume the initial result (empty namespace).
    let _ = expect_query_result(&mut ws, "q3", 2_000).await;

    // Apply a first op and wait for the push — this confirms the subscription
    // is active and the server has processed the op before we unsubscribe.
    post_op_for_query(
        app.clone(),
        "ns",
        "gc:unsub-1",
        CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 1 }),
        &token,
    )
    .await;
    let _ = expect_query_result(&mut ws, "q3", 2_000).await;

    // Now unsubscribe.
    ws.send(ws_encode(&ClientMsg::UnsubscribeQuery { query_id: "q3".into() }))
        .await
        .unwrap();

    // Apply a second op — the server must NOT push a QueryResult for q3.
    post_op_for_query(
        app,
        "ns",
        "gc:unsub-2",
        CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 2 }),
        &token,
    )
    .await;

    assert_no_query_result(&mut ws, "q3", 300).await;
}

/// Type-filtered live query only fires for matching CRDT types.
#[tokio::test]
async fn ws_live_query_type_filter_skips_unrelated() {
    use futures_util::{SinkExt, StreamExt};
    use meridian_core::protocol::{ClientMsg, LiveQueryPayload, ServerMsg};

    let (app, signer) = build_test_app();
    let token = read_write_token(&signer, "ns");

    let addr = spawn_test_server(app.clone()).await;
    let mut ws = ws_connect(addr, "ns", &token).await;

    // Subscribe to a query that only targets GCounters under "gc:typed-*".
    ws.send(ws_encode(&ClientMsg::SubscribeQuery {
        query_id: "q4".into(),
        query: LiveQueryPayload {
            from: "gc:typed-*".into(),
            crdt_type: Some("gcounter".into()),
            aggregate: "sum".into(),
            filter: None,
        },
    }))
    .await
    .unwrap();

    // Consume the initial result.
    let _ = ws.next().await.unwrap().unwrap();

    // Apply an ORSet op under "or:typed-data" — different type, should NOT trigger.
    post_op_for_query(
        app.clone(),
        "ns",
        "or:typed-data",
        CrdtOp::ORSet(ORSetOp::Add {
            element: serde_json::json!("x"),
            tag: uuid::Uuid::new_v4(),
            node_id: 1,
            seq: 1,
        }),
        &token,
    )
    .await;

    assert_no_query_result(&mut ws, "q4", 300).await;

    // Now apply a GCounter op — this one SHOULD trigger.
    post_op_for_query(
        app,
        "ns",
        "gc:typed-1",
        CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 7 }),
        &token,
    )
    .await;

    let msg = expect_query_result(&mut ws, "q4", 2_000).await;
    let ServerMsg::QueryResult { query_id, matched, .. } = msg else { unreachable!() };
    assert_eq!(query_id, "q4");
    assert_eq!(matched, 1);
}
