use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use crate::error::{Result, StorageError};
use super::{config::S3ArchiveConfig, segment::SegmentKey};

/// Abstraction over S3-compatible object storage operations needed for WAL archiving.
/// Kept as a sealed trait so only crate-internal types implement it.
pub(crate) trait ArchiveClient: Send + Sync + 'static {
    fn upload_segment(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn list_segments(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> impl std::future::Future<Output = Result<Vec<SegmentKey>>> + Send;

    fn download_segment(
        &self,
        bucket: &str,
        key: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send;
}

/// Production S3 client backed by `aws-sdk-s3`.
#[derive(Clone)]
pub struct S3ArchiveClient {
    inner: Client,
}

impl S3ArchiveClient {
    pub async fn from_config(config: &S3ArchiveConfig) -> Result<Self> {
        let mut loader = aws_config::from_env().region(
            aws_sdk_s3::config::Region::new(config.region.clone()),
        );
        if let Some(ref endpoint) = config.endpoint_url {
            loader = loader.endpoint_url(endpoint.clone());
        }
        let sdk_config = loader.load().await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config);
        // R2 and MinIO require path-style addressing.
        if config.endpoint_url.is_some() {
            s3_config = s3_config.force_path_style(true);
        }
        Ok(Self { inner: Client::from_conf(s3_config.build()) })
    }
}

impl ArchiveClient for S3ArchiveClient {
    async fn upload_segment(&self, bucket: &str, key: &str, data: Vec<u8>) -> Result<()> {
        self.inner
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| StorageError::S3Upload { message: e.to_string() })?;
        Ok(())
    }

    async fn list_segments(&self, bucket: &str, prefix: &str) -> Result<Vec<SegmentKey>> {
        let mut keys = Vec::new();
        let mut continuation: Option<String> = None;

        loop {
            let mut req = self.inner
                .list_objects_v2()
                .bucket(bucket)
                .prefix(prefix);
            if let Some(ref token) = continuation {
                req = req.continuation_token(token.clone());
            }
            let resp = req.send().await.map_err(|e| StorageError::S3Upload { message: e.to_string() })?;

            for obj in resp.contents() {
                if let Some(k) = obj.key()
                    && let Some(seg) = SegmentKey::from_object_key(k, prefix)
                {
                    keys.push(seg);
                }
            }

            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(str::to_owned);
            } else {
                break;
            }
        }

        // Ensure segments are in seq order (S3 returns lexicographic order which matches
        // our zero-padded key format, but sort explicitly for safety).
        keys.sort_by_key(|k| k.seq_start);
        Ok(keys)
    }

    async fn download_segment(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let resp = self.inner
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| StorageError::S3Restore { message: e.to_string() })?;
        let bytes = resp.body.collect().await
            .map_err(|e| StorageError::S3Restore { message: e.to_string() })?;
        Ok(bytes.into_bytes().to_vec())
    }
}

/// In-memory archive client for unit tests — no Docker required.
#[cfg(test)]
pub(crate) struct InMemoryArchiveClient {
    store: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

#[cfg(test)]
impl InMemoryArchiveClient {
    pub(crate) fn new() -> Self {
        Self { store: std::sync::Mutex::new(std::collections::HashMap::new()) }
    }

    pub(crate) fn object_count(&self) -> usize {
        self.store.lock().unwrap().len()
    }
}

#[cfg(test)]
impl ArchiveClient for InMemoryArchiveClient {
    async fn upload_segment(&self, _bucket: &str, key: &str, data: Vec<u8>) -> Result<()> {
        self.store.lock().unwrap().insert(key.to_owned(), data);
        Ok(())
    }

    async fn list_segments(&self, _bucket: &str, prefix: &str) -> Result<Vec<SegmentKey>> {
        let store = self.store.lock().unwrap();
        let mut keys: Vec<SegmentKey> = store
            .keys()
            .filter_map(|k| SegmentKey::from_object_key(k, prefix))
            .collect();
        keys.sort_by_key(|k| k.seq_start);
        Ok(keys)
    }

    async fn download_segment(&self, _bucket: &str, key: &str) -> Result<Vec<u8>> {
        self.store.lock().unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| StorageError::S3Restore { message: format!("key not found: {key}") })
    }
}
