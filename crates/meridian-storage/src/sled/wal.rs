use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{debug, warn};

use crate::{
    error::Result,
    utils::now_ms,
    wal_backend::{WalBackend, WalEntry},
};

/// Append-only Write-Ahead Log backed by a dedicated sled tree (`_wal`).
///
/// ## Key format
/// `u64::to_be_bytes(seq)` — big-endian ensures lexicographic order == seq order.
///
/// ## Crash recovery protocol
/// On startup: load the state snapshot from the main store, then replay WAL
/// entries with `seq > last_snapshot_seq`. After replay, truncate stale entries.
pub struct SledWal {
    tree: sled::Tree,
    next_seq: AtomicU64,
    checkpoint_seq: AtomicU64,
}

impl SledWal {
    const TREE_NAME: &'static str = "_wal";

    pub fn new(db: &sled::Db) -> Result<Self> {
        let tree = db.open_tree(Self::TREE_NAME)?;

        let last = tree
            .iter()
            .keys()
            .next_back()
            .transpose()?
            .and_then(|k| Self::key_to_seq(&k))
            .unwrap_or(0);

        let checkpoint = tree
            .get(b"_checkpoint")?
            .and_then(|v| v.as_ref().try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0);

        debug!(last_seq = last, checkpoint_seq = checkpoint, "WAL opened");

        Ok(Self {
            tree,
            next_seq: AtomicU64::new(last + 1),
            checkpoint_seq: AtomicU64::new(checkpoint),
        })
    }

    fn seq_to_key(seq: u64) -> [u8; 8] {
        seq.to_be_bytes()
    }

    fn key_to_seq(key: &[u8]) -> Option<u64> {
        let arr: [u8; 8] = key.try_into().ok()?;
        Some(u64::from_be_bytes(arr))
    }
}

impl WalBackend for SledWal {
    async fn append(&self, namespace: &str, crdt_id: &str, op_bytes: Vec<u8>) -> Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);

        let entry = WalEntry {
            seq,
            namespace: namespace.to_owned(),
            crdt_id: crdt_id.to_owned(),
            op_bytes,
            timestamp_ms: now_ms(),
        };

        let value = rmp_serde::encode::to_vec_named(&entry)?;
        self.tree.insert(Self::seq_to_key(seq), value)?;

        Ok(seq)
    }

    async fn replay_from(&self, from_seq: u64) -> Result<Vec<WalEntry>> {
        let start = Self::seq_to_key(from_seq);
        let mut entries = Vec::new();

        for kv in self.tree.range(start..) {
            let (k, v) = kv?;
            // Skip non-seq keys (e.g. b"_checkpoint") — they sort before u64 keys.
            if Self::key_to_seq(&k).is_none() {
                continue;
            }
            match rmp_serde::decode::from_slice::<WalEntry>(&v) {
                Ok(entry) => entries.push(entry),
                Err(e) => warn!(error = %e, "skipping corrupt WAL entry"),
            }
        }

        Ok(entries)
    }

    async fn replay_until(&self, from_seq: u64, until_ms: u64) -> Result<Vec<WalEntry>> {
        let start = Self::seq_to_key(from_seq);
        let mut entries = Vec::new();

        for kv in self.tree.range(start..) {
            let (k, v) = kv?;
            if Self::key_to_seq(&k).is_none() {
                continue;
            }
            match rmp_serde::decode::from_slice::<WalEntry>(&v) {
                Ok(entry) if entry.timestamp_ms <= until_ms => entries.push(entry),
                Ok(_) => break, // entries are ordered by seq ≈ time
                Err(e) => warn!(error = %e, "skipping corrupt WAL entry"),
            }
        }

        Ok(entries)
    }

    async fn truncate_before(&self, before_seq: u64) -> Result<()> {
        let end = Self::seq_to_key(before_seq);
        for kv in self.tree.range(..end) {
            let (k, _) = kv?;
            self.tree.remove(k)?;
        }
        self.checkpoint_seq.fetch_max(before_seq, Ordering::Relaxed);
        Ok(())
    }

    fn last_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Relaxed).saturating_sub(1)
    }

    fn checkpoint_seq(&self) -> u64 {
        self.checkpoint_seq.load(Ordering::Relaxed)
    }

    async fn set_checkpoint_seq(&self, seq: u64) -> Result<()> {
        self.checkpoint_seq.store(seq, Ordering::Relaxed);
        self.tree.insert(b"_checkpoint", &seq.to_be_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> sled::Db {
        sled::Config::new().temporary(true).open().unwrap()
    }

    #[tokio::test]
    async fn append_and_replay() {
        let db = temp_db();
        let wal = SledWal::new(&db).unwrap();

        let seq0 = wal.append("ns", "counter", vec![1, 2, 3]).await.unwrap();
        let seq1 = wal.append("ns", "counter", vec![4, 5, 6]).await.unwrap();

        assert!(seq1 > seq0);

        let entries = wal.replay_from(0).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].op_bytes, vec![1, 2, 3]);
        assert_eq!(entries[1].op_bytes, vec![4, 5, 6]);
    }

    #[tokio::test]
    async fn replay_from_seq() {
        let db = temp_db();
        let wal = SledWal::new(&db).unwrap();

        wal.append("ns", "a", vec![0]).await.unwrap();
        let seq1 = wal.append("ns", "b", vec![1]).await.unwrap();
        wal.append("ns", "c", vec![2]).await.unwrap();

        let entries = wal.replay_from(seq1).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].op_bytes, vec![1]);
    }

    #[tokio::test]
    async fn truncate_removes_old_entries() {
        let db = temp_db();
        let wal = SledWal::new(&db).unwrap();

        let seq0 = wal.append("ns", "a", vec![0]).await.unwrap();
        let seq1 = wal.append("ns", "b", vec![1]).await.unwrap();
        wal.append("ns", "c", vec![2]).await.unwrap();

        wal.truncate_before(seq1).await.unwrap();

        let all = wal.replay_from(0).await.unwrap();
        assert!(!all.iter().any(|e| e.seq == seq0));
        assert!(all.iter().any(|e| e.seq == seq1));
    }

    #[tokio::test]
    async fn survives_reopen() {
        let db = temp_db();
        {
            let wal = SledWal::new(&db).unwrap();
            wal.append("ns", "x", vec![42]).await.unwrap();
        }
        let wal2 = SledWal::new(&db).unwrap();
        let entries = wal2.replay_from(0).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].op_bytes, vec![42]);
        let seq = wal2.append("ns", "x", vec![99]).await.unwrap();
        assert_eq!(seq, entries[0].seq + 1);
    }
}
