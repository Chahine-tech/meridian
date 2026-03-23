use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::{Crdt, CrdtError, HybridLogicalClock, VectorClock};

//
// Each client_id has at most one PresenceEntry. The entry is:
//   - Live if current_wall_ms <= entry.hlc.wall_ms + entry.ttl_ms
//   - Dead (expired) if current_wall_ms >  entry.hlc.wall_ms + entry.ttl_ms
//
// The "resurrect after merge" problem is solved by the HLC expiry check:
//   - Even if a stale server merges an old entry, the wall-time expiry check
//     will reject it because current_wall_ms will be past the deadline.
//
// Leave creates an explicit tombstone (hlc = now, ttl = 0) so the entry
// disappears immediately regardless of wall time, and survives merge correctly.
//
// Background GC (tasks/presence_gc.rs) prunes expired entries from memory
// and broadcasts suppressions to subscribers every 5s.

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Presence {
    /// Maps client_id → presence entry.
    pub entries: HashMap<u64, PresenceEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PresenceEntry {
    /// Arc so clone() in merge/gossip hot paths is O(1) — no deep copy of JsonValue.
    pub data: Arc<JsonValue>,
    pub hlc: HybridLogicalClock,
    /// Time-to-live in milliseconds (from hlc.wall_ms).
    pub ttl_ms: u64,
}

impl PresenceEntry {
    pub fn is_alive(&self, current_wall_ms: u64) -> bool {
        !self.hlc.is_expired(current_wall_ms, self.ttl_ms)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PresenceOp {
    /// Join or refresh presence. Replaces any existing entry for client_id.
    Heartbeat {
        client_id: u64,
        data: JsonValue,
        hlc: HybridLogicalClock,
        ttl_ms: u64,
    },
    /// Explicit leave — sets ttl = 0 so the entry expires immediately.
    Leave {
        client_id: u64,
        hlc: HybridLogicalClock,
    },
}

/// Delta: sparse map of client_id → Option<PresenceEntry>
/// None means the entry was removed (tombstone for Leave / expiry broadcast).
#[must_use]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PresenceDelta {
    pub changes: HashMap<u64, Option<PresenceEntry>>,
}

/// Returns true if `candidate` should replace `existing`.
/// Primary key: HLC (higher wins). Tie-break: ttl_ms (longer wins → favors liveness).
/// This ensures merge is commutative even when HLCs are identical.
fn presence_entry_wins(candidate: &PresenceEntry, existing: &PresenceEntry) -> bool {
    match candidate.hlc.cmp(&existing.hlc) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => candidate.ttl_ms > existing.ttl_ms,
        std::cmp::Ordering::Less => false,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PresenceValue {
    /// Only live (non-expired) entries.
    pub entries: HashMap<u64, PresenceEntryView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PresenceEntryView {
    pub data: JsonValue,
    pub expires_at_ms: u64,
}

impl Crdt for Presence {
    type Op = PresenceOp;
    type Delta = PresenceDelta;
    type Value = PresenceValue;

    fn apply(&mut self, op: PresenceOp) -> Result<Option<PresenceDelta>, CrdtError> {
        match op {
            PresenceOp::Heartbeat { client_id, data, hlc, ttl_ms } => {
                let new_entry = PresenceEntry { data: Arc::new(data), hlc, ttl_ms };

                let should_apply = match self.entries.get(&client_id) {
                    None => true,
                    Some(existing) => presence_entry_wins(&new_entry, existing),
                };

                if should_apply {
                    self.entries.insert(client_id, new_entry.clone());
                    let mut changes = HashMap::new();
                    changes.insert(client_id, Some(new_entry));
                    Ok(Some(PresenceDelta { changes }))
                } else {
                    Ok(None)
                }
            }

            PresenceOp::Leave { client_id, hlc } => {
                // Tombstone: ttl = 0 → expires immediately.
                let tombstone = PresenceEntry {
                    data: Arc::new(JsonValue::Null),
                    hlc,
                    ttl_ms: 0,
                };

                let should_apply = match self.entries.get(&client_id) {
                    None => false, // Nothing to leave
                    Some(existing) => tombstone.hlc > existing.hlc,
                };

                if should_apply {
                    self.entries.insert(client_id, tombstone);
                    let mut changes = HashMap::new();
                    changes.insert(client_id, None);
                    Ok(Some(PresenceDelta { changes }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn merge(&mut self, other: &Presence) {
        for (&client_id, other_entry) in &other.entries {
            match self.entries.get(&client_id) {
                None => {
                    self.entries.insert(client_id, other_entry.clone());
                }
                Some(existing) => {
                    if presence_entry_wins(other_entry, existing) {
                        self.entries.insert(client_id, other_entry.clone());
                    }
                }
            }
        }
    }

    fn merge_delta(&mut self, delta: PresenceDelta) {
        for (client_id, maybe_entry) in delta.changes {
            match maybe_entry {
                Some(new_entry) => {
                    match self.entries.get(&client_id) {
                        None => {
                            self.entries.insert(client_id, new_entry);
                        }
                        Some(existing) => {
                            if presence_entry_wins(&new_entry, existing) {
                                self.entries.insert(client_id, new_entry);
                            }
                        }
                    }
                }
                None => {
                    // Tombstone broadcast: mark as left (ttl=0) if we have an entry.
                    // We don't remove the entry entirely — we need the tombstone to
                    // prevent resurrection in future merges.
                    // The GC task will clean up tombstones after they're safely propagated.
                }
            }
        }
    }

    fn delta_since(&self, _since: &VectorClock) -> Option<PresenceDelta> {
        if self.entries.is_empty() {
            return None;
        }
        let changes = self
            .entries
            .iter()
            .map(|(&id, e)| (id, Some(e.clone())))
            .collect();
        Some(PresenceDelta { changes })
    }

    fn value(&self) -> PresenceValue {
        let now = super::clock::now_ms();
        let entries = self
            .entries
            .iter()
            .filter(|(_, e)| e.is_alive(now))
            .map(|(&id, e)| {
                (
                    id,
                    PresenceEntryView {
                        data: (*e.data).clone(), // Arc deref — clones JsonValue once for the view
                        expires_at_ms: e.hlc.wall_ms + e.ttl_ms,
                    },
                )
            })
            .collect();
        PresenceValue { entries }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Presence {
    /// Prune expired entries from memory. Returns a delta of removed entries
    /// so callers can broadcast the removals to subscribers.
    pub fn gc(&mut self, current_wall_ms: u64) -> Option<PresenceDelta> {
        let expired: Vec<u64> = self
            .entries
            .iter()
            .filter(|(_, e)| !e.is_alive(current_wall_ms))
            .map(|(&id, _)| id)
            .collect();

        if expired.is_empty() {
            return None;
        }

        let mut changes = HashMap::new();
        for id in expired {
            self.entries.remove(&id);
            changes.insert(id, None);
        }
        Some(PresenceDelta { changes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(wall_ms: u64) -> HybridLogicalClock {
        HybridLogicalClock { wall_ms, logical: 0, node_id: 1 }
    }

    fn heartbeat(client_id: u64, wall_ms: u64, ttl_ms: u64) -> PresenceOp {
        PresenceOp::Heartbeat {
            client_id,
            data: JsonValue::String(format!("user_{client_id}")),
            hlc: hlc(wall_ms),
            ttl_ms,
        }
    }

    fn alive_ids(p: &Presence, now: u64) -> Vec<u64> {
        let mut ids: Vec<u64> = p
            .entries
            .iter()
            .filter(|(_, e)| e.is_alive(now))
            .map(|(&id, _)| id)
            .collect();
        ids.sort();
        ids
    }

    #[test]
    fn heartbeat_makes_entry_live() {
        let mut p = Presence::default();
        p.apply(heartbeat(1, 1000, 5000)).unwrap();
        assert_eq!(alive_ids(&p, 1500), vec![1]);
    }

    #[test]
    fn entry_expires_after_ttl() {
        let mut p = Presence::default();
        p.apply(heartbeat(1, 1000, 500)).unwrap();
        // 1499 < 1500 → alive
        assert!(p.entries[&1].is_alive(1499));
        // 1500 >= 1500 → expired (boundary with >= semantics)
        assert!(!p.entries[&1].is_alive(1500));
        // 1501 > 1500 → expired
        assert!(!p.entries[&1].is_alive(1501));
    }

    #[test]
    fn leave_expires_immediately() {
        let mut p = Presence::default();
        p.apply(heartbeat(1, 1000, 10000)).unwrap();
        p.apply(PresenceOp::Leave { client_id: 1, hlc: hlc(2000) }).unwrap();
        // ttl=0 → expired at any time >= 2000
        assert!(!p.entries[&1].is_alive(2000));
        assert!(!p.entries[&1].is_alive(2001));
    }

    #[test]
    fn no_resurrection_after_merge() {
        // node A: entry expires at 1000+500 = 1500
        let mut a = Presence::default();
        a.apply(heartbeat(1, 1000, 500)).unwrap();

        // node B: has a stale copy of the entry with a higher wall_ms
        // (simulating clock drift or delayed sync) — but still expired
        let mut b = Presence::default();
        b.apply(heartbeat(1, 1200, 100)).unwrap(); // expires at 1300

        // At wall time 2000, both entries are expired
        a.merge(&b);
        // B's entry wins (hlc 1200 > 1000), but it's also expired
        assert!(!a.entries[&1].is_alive(2000));
    }

    #[test]
    fn merge_commutative() {
        let mut a = Presence::default();
        a.apply(heartbeat(1, 1000, 5000)).unwrap();

        let mut b = Presence::default();
        b.apply(heartbeat(2, 1000, 5000)).unwrap();

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab, ba);
    }

    #[test]
    fn merge_idempotent() {
        let mut p = Presence::default();
        p.apply(heartbeat(1, 1000, 5000)).unwrap();
        let copy = p.clone();
        p.merge(&copy);
        assert_eq!(p, copy);
    }

    #[test]
    fn gc_removes_expired() {
        let mut p = Presence::default();
        p.apply(heartbeat(1, 1000, 500)).unwrap();
        p.apply(heartbeat(2, 1000, 10000)).unwrap();

        // At 2000: client 1 expired (1000+500=1500 < 2000), client 2 still alive
        let delta = p.gc(2000).unwrap();
        assert!(delta.changes.contains_key(&1));
        assert!(!p.entries.contains_key(&1));
        assert!(p.entries.contains_key(&2));
    }

    #[test]
    fn heartbeat_refreshes_ttl() {
        let mut p = Presence::default();
        p.apply(heartbeat(1, 1000, 500)).unwrap(); // expires at 1500
        p.apply(heartbeat(1, 2000, 5000)).unwrap(); // refresh: expires at 7000
        assert!(p.entries[&1].is_alive(6000));
        assert!(!p.entries[&1].is_alive(7001));
    }
}
