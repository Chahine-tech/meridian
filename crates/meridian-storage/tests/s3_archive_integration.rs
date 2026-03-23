//! Integration tests for S3ArchivedWal using LocalStack via testcontainers.
//!
//! Run with:
//!   cargo test -p meridian-storage \
//!     --features wal-archive-s3,storage-memory \
//!     --test s3_archive_integration
//!
//! Requires Docker.

#![cfg(feature = "wal-archive-s3")]

use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Credentials, Client};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;

use meridian_storage::{
    memory::NoopWal,
    s3_archive::{S3ArchiveConfig, S3ArchivedWal},
    wal_backend::WalBackend,
};

const BUCKET: &str = "wal-test";

async fn start_localstack() -> (testcontainers::ContainerAsync<LocalStack>, String) {
    let container = LocalStack::default()
        .start()
        .await
        .expect("failed to start LocalStack container");
    let port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("failed to get LocalStack port");
    (container, format!("http://127.0.0.1:{port}"))
}

async fn raw_s3_client(endpoint: &str) -> Client {
    let creds = Credentials::new("fake", "fake", None, None, "test");
    let cfg = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    Client::new(&cfg)
}

async fn create_bucket(endpoint: &str) {
    raw_s3_client(endpoint)
        .await
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("failed to create bucket");
}

fn make_config(endpoint: &str) -> S3ArchiveConfig {
    S3ArchiveConfig {
        bucket: BUCKET.into(),
        key_prefix: "wal/".into(),
        region: "us-east-1".into(),
        endpoint_url: Some(endpoint.to_owned()),
        segment_size: 3,
    }
}

fn set_fake_credentials() {
    // Safety: tests run in a single-threaded tokio runtime by default.
    // These env vars are only read once during S3 client init inside this test process.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "fake");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "fake");
    }
}

/// A fresh `S3ArchivedWal` pointed at an empty bucket should restore successfully (no-op).
#[tokio::test]
async fn restore_from_empty_bucket_is_noop() {
    let (_c, endpoint) = start_localstack().await;
    create_bucket(&endpoint).await;
    set_fake_credentials();

    S3ArchivedWal::new(NoopWal::default(), make_config(&endpoint))
        .await
        .expect("new() should succeed on empty bucket");
}

/// After 3 appends (= segment_size) the upload path is exercised against real LocalStack S3.
/// NoopWal returns no replay entries so the PUT is skipped, but the threshold logic
/// and S3 connectivity run without error.
#[tokio::test]
async fn segment_threshold_does_not_error() {
    let (_c, endpoint) = start_localstack().await;
    create_bucket(&endpoint).await;
    set_fake_credentials();

    let wal: S3ArchivedWal<NoopWal> =
        S3ArchivedWal::new(NoopWal::default(), make_config(&endpoint))
            .await
            .unwrap();

    wal.append("ns", "c1", vec![1]).await.unwrap();
    wal.append("ns", "c1", vec![2]).await.unwrap();
    // Third append crosses segment_size=3 and triggers upload (no-op for NoopWal)
    wal.append("ns", "c1", vec![3]).await.unwrap();
}

/// `truncate_before` must flush pending entries to S3 and then delegate without error.
#[tokio::test]
async fn truncate_before_does_not_error() {
    let (_c, endpoint) = start_localstack().await;
    create_bucket(&endpoint).await;
    set_fake_credentials();

    let wal: S3ArchivedWal<NoopWal> =
        S3ArchivedWal::new(NoopWal::default(), make_config(&endpoint))
            .await
            .unwrap();

    wal.append("ns", "c1", vec![42]).await.unwrap();
    wal.truncate_before(2).await.expect("truncate_before must not fail");
}

/// replay_from delegates to inner WAL — confirms the S3 wrapper is transparent.
#[tokio::test]
async fn replay_delegates_to_inner() {
    let (_c, endpoint) = start_localstack().await;
    create_bucket(&endpoint).await;
    set_fake_credentials();

    let wal: S3ArchivedWal<NoopWal> =
        S3ArchivedWal::new(NoopWal::default(), make_config(&endpoint))
            .await
            .unwrap();

    let entries = wal.replay_from(0).await.unwrap();
    assert!(entries.is_empty(), "NoopWal always returns empty replay");
}
