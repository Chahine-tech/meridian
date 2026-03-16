//! Integration tests for RedisStore and RedisWal.
//!
//! These tests require Docker (used by testcontainers to spin up a real
//! Redis instance). They are gated behind `--features storage-redis`.

#![cfg(feature = "storage-redis")]

use meridian_storage::{
    redis_backend::{RedisStore, RedisWal},
    store::Store,
    wal_backend::WalBackend,
};
use serde::{Deserialize, Serialize};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestValue {
    data: String,
    count: u32,
}

async fn setup() -> (String, testcontainers::ContainerAsync<Redis>) {
    let container = Redis::default().start().await.expect("docker must be running");
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let url = format!("redis://127.0.0.1:{port}");
    (url, container)
}

// ---------------------------------------------------------------------------
// RedisStore tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn redis_store_get_missing_returns_none() {
    let (url, _container) = setup().await;
    let store = RedisStore::new(&url).await.unwrap();
    let result: Option<TestValue> = store.get("ns", "missing").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn redis_store_put_and_get_roundtrip() {
    let (url, _container) = setup().await;
    let store = RedisStore::new(&url).await.unwrap();

    let value = TestValue { data: "hello".into(), count: 42 };
    store.put("ns", "id1", &value).await.unwrap();
    let retrieved: Option<TestValue> = store.get("ns", "id1").await.unwrap();
    assert_eq!(retrieved, Some(value));
}

#[tokio::test]
async fn redis_store_put_overwrites() {
    let (url, _container) = setup().await;
    let store = RedisStore::new(&url).await.unwrap();

    let v1 = TestValue { data: "first".into(), count: 1 };
    let v2 = TestValue { data: "second".into(), count: 2 };
    store.put("ns", "id1", &v1).await.unwrap();
    store.put("ns", "id1", &v2).await.unwrap();
    let retrieved: Option<TestValue> = store.get("ns", "id1").await.unwrap();
    assert_eq!(retrieved, Some(v2));
}

#[tokio::test]
async fn redis_store_delete_removes_entry() {
    let (url, _container) = setup().await;
    let store = RedisStore::new(&url).await.unwrap();

    let value = TestValue { data: "to delete".into(), count: 0 };
    store.put("ns", "id1", &value).await.unwrap();
    store.delete("ns", "id1").await.unwrap();
    let result: Option<TestValue> = store.get("ns", "id1").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn redis_store_scan_prefix_returns_namespace_entries() {
    let (url, _container) = setup().await;
    let store = RedisStore::new(&url).await.unwrap();

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
// RedisWal tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn redis_wal_append_and_replay() {
    let (url, _container) = setup().await;
    let wal = RedisWal::new(&url).await.unwrap();

    let seq1 = wal.append("ns", "crdt1", vec![1, 2, 3]).await.unwrap();
    let seq2 = wal.append("ns", "crdt1", vec![4, 5, 6]).await.unwrap();
    assert!(seq2 > seq1);

    let entries = wal.replay_from(seq1).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].op_bytes, vec![1, 2, 3]);
    assert_eq!(entries[1].op_bytes, vec![4, 5, 6]);
}

#[tokio::test]
async fn redis_wal_truncate_before() {
    let (url, _container) = setup().await;
    let wal = RedisWal::new(&url).await.unwrap();

    let seq1 = wal.append("ns", "crdt1", vec![1]).await.unwrap();
    let seq2 = wal.append("ns", "crdt1", vec![2]).await.unwrap();
    let _seq3 = wal.append("ns", "crdt1", vec![3]).await.unwrap();

    wal.truncate_before(seq2).await.unwrap();

    let entries = wal.replay_from(0).await.unwrap();
    assert!(entries.iter().all(|e| e.seq >= seq2));
    assert!(!entries.iter().any(|e| e.seq == seq1));
}

#[tokio::test]
async fn redis_wal_last_seq_updated_on_append() {
    let (url, _container) = setup().await;
    let wal = RedisWal::new(&url).await.unwrap();

    let seq = wal.append("ns", "crdt1", vec![99]).await.unwrap();
    assert_eq!(wal.last_seq(), seq);
}

#[tokio::test]
async fn redis_wal_last_seq_resumes_after_reopen() {
    let (url, _container) = setup().await;
    let wal = RedisWal::new(&url).await.unwrap();
    let seq = wal.append("ns", "crdt1", vec![42]).await.unwrap();

    let wal2 = RedisWal::new(&url).await.unwrap();
    assert_eq!(wal2.last_seq(), seq);
}

#[tokio::test]
async fn redis_wal_checkpoint_persists() {
    let (url, _container) = setup().await;
    let wal = RedisWal::new(&url).await.unwrap();

    wal.append("ns", "crdt1", vec![1]).await.unwrap();
    wal.set_checkpoint_seq(42).await.unwrap();
    assert_eq!(wal.checkpoint_seq(), 42);

    let wal2 = RedisWal::new(&url).await.unwrap();
    assert_eq!(wal2.checkpoint_seq(), 42);
}
