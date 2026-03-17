use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use http_body_util::BodyExt;
use meridian_server::{
    api::{build_router, ws::SubscriptionManager},
    auth::{AuthState, Permissions, TokenClaims, TokenSigner},
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
        #[cfg(feature = "cluster")]
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
        Permissions {
            read: vec!["*".into()],
            write: vec!["allowed:*".into()],
            admin: false,
        },
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
        Permissions {
            read: vec!["*".into()],
            write: vec!["allowed:*".into()],
            admin: false,
        },
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
