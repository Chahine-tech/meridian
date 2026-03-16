//! Integration tests for PgStore and PgWal.
//!
//! These tests require Docker (used by testcontainers to spin up a real
//! Postgres instance). They are gated behind `--features storage-postgres`
//! and are skipped automatically when the feature is absent.

#![cfg(feature = "storage-postgres")]

use meridian_storage::{
    postgres::{PgStore, PgWal},
    store::Store,
    wal_backend::WalBackend,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestValue {
    data: String,
    count: u32,
}

async fn setup() -> (
    sqlx::PgPool,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default().start().await.expect("docker must be running");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("failed to connect to test postgres");
    (pool, container)
}

// ---------------------------------------------------------------------------
// PgStore tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_store_get_missing_returns_none() {
    let (pool, _container) = setup().await;
    PgStore::migrate(&pool).await.unwrap();
    let store = PgStore::new(pool);
    let result: Option<TestValue> = store.get("ns", "missing").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn pg_store_put_and_get_roundtrip() {
    let (pool, _container) = setup().await;
    PgStore::migrate(&pool).await.unwrap();
    let store = PgStore::new(pool);

    let value = TestValue { data: "hello".into(), count: 42 };
    store.put("ns", "id1", &value).await.unwrap();
    let retrieved: Option<TestValue> = store.get("ns", "id1").await.unwrap();
    assert_eq!(retrieved, Some(value));
}

#[tokio::test]
async fn pg_store_put_overwrites() {
    let (pool, _container) = setup().await;
    PgStore::migrate(&pool).await.unwrap();
    let store = PgStore::new(pool);

    let v1 = TestValue { data: "first".into(), count: 1 };
    let v2 = TestValue { data: "second".into(), count: 2 };
    store.put("ns", "id1", &v1).await.unwrap();
    store.put("ns", "id1", &v2).await.unwrap();
    let retrieved: Option<TestValue> = store.get("ns", "id1").await.unwrap();
    assert_eq!(retrieved, Some(v2));
}

#[tokio::test]
async fn pg_store_delete_removes_entry() {
    let (pool, _container) = setup().await;
    PgStore::migrate(&pool).await.unwrap();
    let store = PgStore::new(pool);

    let value = TestValue { data: "to delete".into(), count: 0 };
    store.put("ns", "id1", &value).await.unwrap();
    store.delete("ns", "id1").await.unwrap();
    let retrieved: Option<TestValue> = store.get("ns", "id1").await.unwrap();
    assert!(retrieved.is_none());
}

#[tokio::test]
async fn pg_store_scan_prefix_returns_namespace_entries() {
    let (pool, _container) = setup().await;
    PgStore::migrate(&pool).await.unwrap();
    let store = PgStore::new(pool);

    let v1 = TestValue { data: "a".into(), count: 1 };
    let v2 = TestValue { data: "b".into(), count: 2 };
    let other = TestValue { data: "other".into(), count: 3 };
    store.put("ns1", "id1", &v1).await.unwrap();
    store.put("ns1", "id2", &v2).await.unwrap();
    store.put("ns2", "id3", &other).await.unwrap();

    let results: Vec<(String, TestValue)> = store.scan_prefix("ns1").await.unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().any(|(k, _)| k == "ns1/id1"));
    assert!(results.iter().any(|(k, _)| k == "ns1/id2"));
}

// ---------------------------------------------------------------------------
// PgWal tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_wal_append_and_replay() {
    let (pool, _container) = setup().await;
    PgWal::migrate(&pool).await.unwrap();
    let wal = PgWal::new(pool).await.unwrap();

    let seq1 = wal.append("ns", "crdt1", vec![1, 2, 3]).await.unwrap();
    let seq2 = wal.append("ns", "crdt1", vec![4, 5, 6]).await.unwrap();
    assert!(seq2 > seq1);

    let entries = wal.replay_from(seq1).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].op_bytes, vec![1, 2, 3]);
    assert_eq!(entries[1].op_bytes, vec![4, 5, 6]);
}

#[tokio::test]
async fn pg_wal_truncate_before() {
    let (pool, _container) = setup().await;
    PgWal::migrate(&pool).await.unwrap();
    let wal = PgWal::new(pool).await.unwrap();

    let seq1 = wal.append("ns", "crdt1", vec![1]).await.unwrap();
    let seq2 = wal.append("ns", "crdt1", vec![2]).await.unwrap();
    let _seq3 = wal.append("ns", "crdt1", vec![3]).await.unwrap();

    wal.truncate_before(seq2).await.unwrap();

    let entries = wal.replay_from(0).await.unwrap();
    assert!(entries.iter().all(|e| e.seq >= seq2));
    assert!(!entries.iter().any(|e| e.seq == seq1));
}

#[tokio::test]
async fn pg_wal_checkpoint_persists() {
    let (pool, _container) = setup().await;
    PgWal::migrate(&pool).await.unwrap();
    let wal = PgWal::new(pool.clone()).await.unwrap();

    wal.append("ns", "crdt1", vec![1]).await.unwrap();
    wal.set_checkpoint_seq(42).await.unwrap();
    assert_eq!(wal.checkpoint_seq(), 42);

    // Re-open — checkpoint must survive
    let wal2 = PgWal::new(pool).await.unwrap();
    assert_eq!(wal2.checkpoint_seq(), 42);
}

#[tokio::test]
async fn pg_wal_last_seq_resumes_after_reopen() {
    let (pool, _container) = setup().await;
    PgWal::migrate(&pool).await.unwrap();
    let wal = PgWal::new(pool.clone()).await.unwrap();

    let seq = wal.append("ns", "crdt1", vec![99]).await.unwrap();

    let wal2 = PgWal::new(pool).await.unwrap();
    assert_eq!(wal2.last_seq(), seq);
}
