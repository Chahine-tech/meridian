use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::error::Result;

// ---------------------------------------------------------------------------
// WAL entry
// ---------------------------------------------------------------------------

/// A single operation appended to the Write-Ahead Log.
///
/// Sequence numbers are monotonically increasing u64 values.
/// They are stored as big-endian bytes so sled's lexicographic scan
/// returns entries in insertion order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub seq: u64,
    pub namespace: String,
    pub crdt_id: String,
    /// msgpack-encoded `CrdtOp` — opaque bytes to avoid coupling WAL to CRDT types.
    pub op_bytes: Vec<u8>,
    pub timestamp_ms: u64,
}

// ---------------------------------------------------------------------------
// WAL
// ---------------------------------------------------------------------------

/// Append-only Write-Ahead Log backed by a dedicated sled tree (`_wal`).
///
/// ## Key format
/// `u64::to_be_bytes(seq)` — big-endian ensures lexicographic order == seq order.
///
/// ## Crash recovery protocol
/// On startup: load the state snapshot from the main store, then replay WAL
/// entries with `seq > last_snapshot_seq`. After replay, truncate stale entries.
pub struct Wal {
    tree: sled::Tree,
    /// Monotonic counter; wrapped in std::sync::Mutex because the lock is
    /// never held across an `.await` point.
    next_seq: Mutex<u64>,
}

impl Wal {
    const TREE_NAME: &'static str = "_wal";

    pub fn new(db: &sled::Db) -> Result<Self> {
        let tree = db.open_tree(Self::TREE_NAME)?;

        // Resume from the highest existing sequence number.
        let last = tree
            .iter()
            .keys()
            .next_back()
            .transpose()?
            .map(|k| Self::key_to_seq(&k))
            .unwrap_or(0);

        debug!(last_seq = last, "WAL opened");

        Ok(Self {
            tree,
            next_seq: Mutex::new(last + 1),
        })
    }

    /// Append an entry. Returns the assigned sequence number.
    pub fn append(&self, namespace: &str, crdt_id: &str, op_bytes: Vec<u8>) -> Result<u64> {
        let seq = {
            let mut guard = self.next_seq.lock().expect("WAL mutex poisoned");
            let s = *guard;
            *guard = s + 1;
            s
        };

        let entry = WalEntry {
            seq,
            namespace: namespace.to_owned(),
            crdt_id: crdt_id.to_owned(),
            op_bytes,
            timestamp_ms: crate::crdt::clock::now_ms(),
        };

        let value = rmp_serde::encode::to_vec_named(&entry)?;
        self.tree.insert(Self::seq_to_key(seq), value)?;

        Ok(seq)
    }

    /// Replay all entries with `seq >= from_seq`.
    pub fn replay_from(&self, from_seq: u64) -> Result<Vec<WalEntry>> {
        let start = Self::seq_to_key(from_seq);
        let mut entries = Vec::new();

        for kv in self.tree.range(start..) {
            let (_, v) = kv?;
            match rmp_serde::decode::from_slice::<WalEntry>(&v) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    warn!(error = %e, "skipping corrupt WAL entry");
                }
            }
        }

        Ok(entries)
    }

    /// Remove all entries with `seq < before_seq` (inclusive trim).
    pub fn truncate_before(&self, before_seq: u64) -> Result<()> {
        let end = Self::seq_to_key(before_seq);
        for kv in self.tree.range(..end) {
            let (k, _) = kv?;
            self.tree.remove(k)?;
        }
        Ok(())
    }

    /// The highest sequence number ever appended (0 if empty).
    pub fn last_seq(&self) -> u64 {
        self.next_seq
            .lock()
            .expect("WAL mutex poisoned")
            .saturating_sub(1)
    }

    // -- helpers --

    fn seq_to_key(seq: u64) -> [u8; 8] {
        seq.to_be_bytes()
    }

    fn key_to_seq(key: &[u8]) -> u64 {
        let arr: [u8; 8] = key.try_into().unwrap_or([0u8; 8]);
        u64::from_be_bytes(arr)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> sled::Db {
        sled::Config::new().temporary(true).open().unwrap()
    }

    #[test]
    fn append_and_replay() {
        let db = temp_db();
        let wal = Wal::new(&db).unwrap();

        let seq0 = wal.append("ns", "counter", vec![1, 2, 3]).unwrap();
        let seq1 = wal.append("ns", "counter", vec![4, 5, 6]).unwrap();

        assert!(seq1 > seq0);

        let entries = wal.replay_from(0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].op_bytes, vec![1, 2, 3]);
        assert_eq!(entries[1].op_bytes, vec![4, 5, 6]);
    }

    #[test]
    fn replay_from_seq() {
        let db = temp_db();
        let wal = Wal::new(&db).unwrap();

        wal.append("ns", "a", vec![0]).unwrap();
        let seq1 = wal.append("ns", "b", vec![1]).unwrap();
        wal.append("ns", "c", vec![2]).unwrap();

        let entries = wal.replay_from(seq1).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].op_bytes, vec![1]);
    }

    #[test]
    fn truncate_removes_old_entries() {
        let db = temp_db();
        let wal = Wal::new(&db).unwrap();

        let seq0 = wal.append("ns", "a", vec![0]).unwrap();
        let seq1 = wal.append("ns", "b", vec![1]).unwrap();
        wal.append("ns", "c", vec![2]).unwrap();

        // Truncate entries before seq1 (i.e. remove seq0 only)
        wal.truncate_before(seq1).unwrap();

        let all = wal.replay_from(0).unwrap();
        assert!(!all.iter().any(|e| e.seq == seq0));
        assert!(all.iter().any(|e| e.seq == seq1));
    }

    #[test]
    fn survives_reopen() {
        let db = temp_db();
        {
            let wal = Wal::new(&db).unwrap();
            wal.append("ns", "x", vec![42]).unwrap();
        }
        // Reopen with same db
        let wal2 = Wal::new(&db).unwrap();
        let entries = wal2.replay_from(0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].op_bytes, vec![42]);
        // next_seq should continue from where we left off
        let seq = wal2.append("ns", "x", vec![99]).unwrap();
        assert_eq!(seq, entries[0].seq + 1);
    }
}
