use serde::{Deserialize, Serialize};

use super::error::Result;

/// A single operation appended to the Write-Ahead Log.
///
/// Sequence numbers are monotonically increasing u64 values assigned by the
/// backend. They are opaque to callers — only relative ordering matters.
#[must_use]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub seq: u64,
    pub namespace: String,
    pub crdt_id: String,
    /// msgpack-encoded `CrdtOp` — opaque bytes, avoids coupling WAL to CRDT types.
    pub op_bytes: Vec<u8>,
    pub timestamp_ms: u64,
}

/// Abstraction over Write-Ahead Log storage.
pub trait WalBackend: Send + Sync + 'static {
    /// Append an operation entry. Returns the assigned sequence number.
    fn append(
        &self,
        namespace: &str,
        crdt_id: &str,
        op_bytes: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<u64>> + Send;

    /// Replay all entries with `seq >= from_seq`.
    fn replay_from(
        &self,
        from_seq: u64,
    ) -> impl std::future::Future<Output = Result<Vec<WalEntry>>> + Send;

    /// Replay all entries with `seq >= from_seq` and `timestamp_ms <= until_ms`.
    ///
    /// Used for point-in-time recovery: replay the WAL up to a specific wall-clock
    /// timestamp to reconstruct CRDT state as it was at that moment.
    ///
    /// Default impl filters the result of `replay_from` in memory — backends
    /// can override with a more efficient storage-side filter.
    fn replay_until(
        &self,
        from_seq: u64,
        until_ms: u64,
    ) -> impl std::future::Future<Output = Result<Vec<WalEntry>>> + Send {
        async move {
            let entries = self.replay_from(from_seq).await?;
            Ok(entries.into_iter().filter(|e| e.timestamp_ms <= until_ms).collect())
        }
    }

    /// Remove all entries with `seq < before_seq`.
    fn truncate_before(
        &self,
        before_seq: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// The highest sequence number ever appended (0 if empty).
    fn last_seq(&self) -> u64;

    /// The last compacted sequence number — entries before this have been removed.
    fn checkpoint_seq(&self) -> u64;

    /// Persist the new checkpoint sequence number.
    fn set_checkpoint_seq(
        &self,
        seq: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}
