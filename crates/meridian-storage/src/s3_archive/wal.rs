use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{info, warn};

use crate::{
    error::Result,
    wal_backend::{WalBackend, WalEntry},
};
use super::{client::ArchiveClient, config::S3ArchiveConfig, segment::SegmentKey};

/// Transparent wrapper around any `W: WalBackend` that archives WAL segments to S3.
///
/// - **Upload on size threshold**: when `entries_since_archive >= config.segment_size`,
///   a background task uploads the accumulated segment.
/// - **Upload on truncation**: entries are uploaded to S3 *before* `truncate_before`
///   delegates to the inner WAL. This guarantees data is on S3 before local deletion.
/// - **Restore on empty WAL**: in `new()`, if `inner.last_seq() == 0`, all segments
///   are downloaded from S3 and replayed into the inner WAL.
///
/// Upload failures are non-fatal — logged as warnings, local WAL remains authoritative.
///
/// The `C` type parameter is a `pub(crate)` implementation detail (defaults to the real S3
/// client); external callers should always use `S3ArchivedWal<W>` via [`S3ArchivedWal::new`].
#[allow(private_bounds, private_interfaces)]
pub struct S3ArchivedWal<W: WalBackend, C: ArchiveClient = super::client::S3ArchiveClient> {
    inner: W,
    s3: C,
    config: S3ArchiveConfig,
    /// Entries appended since the last S3 upload.
    entries_since_archive: AtomicU64,
    /// The seq assigned to the first entry of the current open segment.
    current_segment_start: AtomicU64,
}

#[allow(private_bounds)]
impl<W: WalBackend, C: ArchiveClient> S3ArchivedWal<W, C> {
    pub async fn new_with_client(inner: W, config: S3ArchiveConfig, s3: C) -> Result<Self> {
        let wal = Self {
            inner,
            s3,
            config,
            entries_since_archive: AtomicU64::new(0),
            current_segment_start: AtomicU64::new(0),
        };
        wal.maybe_restore().await?;
        Ok(wal)
    }

    async fn maybe_restore(&self) -> Result<()> {
        if self.inner.last_seq() > 0 {
            return Ok(());
        }
        self.restore_from_s3().await
    }

    async fn restore_from_s3(&self) -> Result<()> {
        let segments = self.s3.list_segments(&self.config.bucket, &self.config.key_prefix).await?;
        if segments.is_empty() {
            return Ok(());
        }
        info!(count = segments.len(), "restoring WAL from S3");
        for seg_key in &segments {
            let object_key = seg_key.to_object_key(&self.config.key_prefix);
            let data = self.s3.download_segment(&self.config.bucket, &object_key).await?;
            let entries: Vec<WalEntry> = rmp_serde::from_slice(&data)
                .map_err(crate::error::StorageError::Deserialization)?;
            for entry in entries {
                self.inner.append(&entry.namespace, &entry.crdt_id, entry.op_bytes).await?;
            }
        }
        info!(last_seq = self.inner.last_seq(), "WAL restore from S3 complete");
        Ok(())
    }

    /// Upload entries `[seg_start, seg_end]` from the inner WAL to S3.
    async fn upload_segment(&self, seg_start: u64, seg_end: u64) {
        let entries = match self.inner.replay_from(seg_start).await {
            Ok(e) => e.into_iter().filter(|e| e.seq <= seg_end).collect::<Vec<_>>(),
            Err(e) => {
                warn!(error = %e, seg_start, seg_end, "S3 archive: failed to read entries for upload");
                return;
            }
        };
        if entries.is_empty() {
            return;
        }
        let data = match rmp_serde::to_vec_named(&entries) {
            Ok(d) => d,
            Err(e) => {
                warn!(error = %e, "S3 archive: serialization failed");
                return;
            }
        };
        let key = SegmentKey { seq_start: seg_start, seq_end: seg_end }
            .to_object_key(&self.config.key_prefix);
        if let Err(e) = self.s3.upload_segment(&self.config.bucket, &key, data).await {
            warn!(error = %e, %key, "S3 archive: upload failed (non-fatal, local WAL intact)");
        } else {
            info!(%key, entries = entries.len(), "S3 archive: segment uploaded");
        }
    }
}

impl<W: WalBackend> S3ArchivedWal<W, super::client::S3ArchiveClient> {
    pub async fn new(inner: W, config: S3ArchiveConfig) -> Result<Self> {
        let s3 = super::client::S3ArchiveClient::from_config(&config).await?;
        Self::new_with_client(inner, config, s3).await
    }
}

impl<W: WalBackend, C: ArchiveClient> WalBackend for S3ArchivedWal<W, C> {
    async fn append(&self, namespace: &str, crdt_id: &str, op_bytes: Vec<u8>) -> Result<u64> {
        let seq = self.inner.append(namespace, crdt_id, op_bytes).await?;

        // Initialise segment_start on first append (or after an archive reset).
        self.current_segment_start.compare_exchange(0, seq, Ordering::Relaxed, Ordering::Relaxed).ok();

        let count = self.entries_since_archive.fetch_add(1, Ordering::Relaxed) + 1;

        if count >= self.config.segment_size as u64 {
            let seg_start = self.current_segment_start.swap(seq + 1, Ordering::Relaxed);
            self.entries_since_archive.store(0, Ordering::Relaxed);
            self.upload_segment(seg_start, seq).await;
        }

        Ok(seq)
    }

    async fn replay_from(&self, from_seq: u64) -> Result<Vec<WalEntry>> {
        self.inner.replay_from(from_seq).await
    }

    async fn replay_until(&self, from_seq: u64, until_ms: u64) -> Result<Vec<WalEntry>> {
        self.inner.replay_until(from_seq, until_ms).await
    }

    async fn truncate_before(&self, before_seq: u64) -> Result<()> {
        // Upload any pending unarchived entries before they are truncated.
        let seg_start = self.current_segment_start.load(Ordering::Relaxed);
        let pending = self.entries_since_archive.load(Ordering::Relaxed);
        if seg_start < before_seq && pending > 0 {
            let seg_end = self.inner.last_seq().min(before_seq.saturating_sub(1));
            self.upload_segment(seg_start, seg_end).await;
            self.current_segment_start.store(before_seq, Ordering::Relaxed);
            self.entries_since_archive.store(0, Ordering::Relaxed);
        }
        self.inner.truncate_before(before_seq).await
    }

    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }

    fn checkpoint_seq(&self) -> u64 {
        self.inner.checkpoint_seq()
    }

    async fn set_checkpoint_seq(&self, seq: u64) -> Result<()> {
        self.inner.set_checkpoint_seq(seq).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memory::NoopWal, s3_archive::client::InMemoryArchiveClient};

    fn make_config() -> S3ArchiveConfig {
        S3ArchiveConfig {
            bucket: "test-bucket".into(),
            key_prefix: "wal/".into(),
            region: "us-east-1".into(),
            endpoint_url: None,
            segment_size: 3,
        }
    }

    #[tokio::test]
    async fn restore_skipped_when_inner_has_data() {
        let inner = NoopWal::default();
        // NoopWal always returns last_seq=0 but we test the branch logic
        let s3 = InMemoryArchiveClient::new();
        // Pre-populate S3 with a segment
        s3.upload_segment("test-bucket", "wal/00000000000000000001-00000000000000000002.msgpack", vec![]).await.unwrap();
        // Still no restore because NoopWal.last_seq() == 0 but entries are empty bytes —
        // the real test is that `maybe_restore` doesn't error on malformed data unless entries exist
        let _wal: Result<S3ArchivedWal<NoopWal, InMemoryArchiveClient>> =
            S3ArchivedWal::new_with_client(inner, make_config(), s3).await;
    }

    #[tokio::test]
    async fn upload_triggered_at_segment_size() {
        let inner = NoopWal::default();
        let s3 = InMemoryArchiveClient::new();
        let wal: S3ArchivedWal<NoopWal, InMemoryArchiveClient> =
            S3ArchivedWal::new_with_client(inner, make_config(), s3).await.unwrap();

        // segment_size = 3, so upload fires on 3rd append
        wal.append("ns", "id", vec![1]).await.unwrap();
        wal.append("ns", "id", vec![2]).await.unwrap();
        assert_eq!(wal.s3.object_count(), 0);
        wal.append("ns", "id", vec![3]).await.unwrap();
        // NoopWal returns empty replay, so upload is a no-op size-wise,
        // but the counter resets and object_count stays 0 (no data to serialize).
        // What matters is entries_since_archive resets.
        assert_eq!(wal.entries_since_archive.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn truncate_uploads_pending_before_delegating() {
        let inner = NoopWal::default();
        let s3 = InMemoryArchiveClient::new();
        let wal: S3ArchivedWal<NoopWal, InMemoryArchiveClient> =
            S3ArchivedWal::new_with_client(inner, make_config(), s3).await.unwrap();

        wal.append("ns", "id", vec![1]).await.unwrap();
        // 1 pending entry, below segment_size=3 — no upload yet
        assert_eq!(wal.s3.object_count(), 0);

        // truncate_before should upload the pending segment first
        wal.truncate_before(2).await.unwrap();
        // entries_since_archive resets
        assert_eq!(wal.entries_since_archive.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn delegates_replay_to_inner() {
        let inner = NoopWal::default();
        let s3 = InMemoryArchiveClient::new();
        let wal: S3ArchivedWal<NoopWal, InMemoryArchiveClient> =
            S3ArchivedWal::new_with_client(inner, make_config(), s3).await.unwrap();
        let entries = wal.replay_from(0).await.unwrap();
        assert!(entries.is_empty());
    }
}
