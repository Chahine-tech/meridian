use std::sync::Mutex;

use crate::{
    error::{Result, StorageError},
    wal_backend::{WalBackend, WalEntry},
};

// ---------------------------------------------------------------------------
// NoopWal
// ---------------------------------------------------------------------------

/// No-op WAL — discards all entries. Used with `MemoryStore` in tests and
/// edge/WASM environments where WAL persistence is not needed.
pub struct NoopWal {
    next_seq: Mutex<u64>,
}

impl NoopWal {
    pub fn new() -> Self {
        Self { next_seq: Mutex::new(0) }
    }
}

impl Default for NoopWal {
    fn default() -> Self {
        Self::new()
    }
}

impl WalBackend for NoopWal {
    async fn append(&self, _namespace: &str, _crdt_id: &str, _op_bytes: Vec<u8>) -> Result<u64> {
        let mut guard = self.next_seq.lock().map_err(|_| StorageError::LockPoisoned)?;
        let seq = *guard;
        *guard = seq + 1;
        Ok(seq)
    }

    async fn replay_from(&self, _from_seq: u64) -> Result<Vec<WalEntry>> {
        Ok(vec![])
    }

    async fn replay_until(&self, _from_seq: u64, _until_ms: u64) -> Result<Vec<WalEntry>> {
        Ok(vec![])
    }

    async fn truncate_before(&self, _before_seq: u64) -> Result<()> {
        Ok(())
    }

    fn last_seq(&self) -> u64 {
        self.next_seq.lock().map(|g| g.saturating_sub(1)).unwrap_or(0)
    }

    fn checkpoint_seq(&self) -> u64 {
        0
    }

    async fn set_checkpoint_seq(&self, _seq: u64) -> Result<()> {
        Ok(())
    }
}
