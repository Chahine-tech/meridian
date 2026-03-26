use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::{Crdt, CrdtError, HybridLogicalClock, VectorClock};

//
// A single-value cell. The write with the highest HLC wins.
// Tie-break on `author` (u64) makes the merge totally ordered and deterministic
// even when two clients write at the exact same HLC timestamp.
//
// Security: server MUST validate that client_hlc.wall_ms is within 30s of
// server wall time before accepting a Write op. This prevents a rogue client
// from "winning" indefinitely into the future.

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct LwwRegister {
    pub entry: Option<LwwEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LwwEntry {
    pub value: JsonValue,
    pub hlc: HybridLogicalClock,
    pub author: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LwwOp {
    pub value: JsonValue,
    pub hlc: HybridLogicalClock,
    pub author: u64,
}

/// Delta is just the winning entry (or None if cleared).
#[must_use]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LwwDelta {
    pub entry: Option<LwwEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LwwValue {
    pub value: Option<JsonValue>,
    pub updated_at_ms: Option<u64>,
    pub author: Option<u64>,
}

/// Returns true if `a` beats `b` in LWW order.
/// Primary key: HLC (higher wins). Tie-break 1: author (higher wins).
/// Tie-break 2: value serialized as JSON bytes (lexicographic) — makes merge
/// totally ordered and commutative even when two writers share the same HLC and author.
/// Serialized once per call to avoid redundant allocations on large values.
fn wins_over(
    a_hlc: HybridLogicalClock, a_author: u64, a_value: &serde_json::Value,
    b_hlc: HybridLogicalClock, b_author: u64, b_value: &serde_json::Value,
) -> bool {
    match a_hlc.cmp(&b_hlc) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => match a_author.cmp(&b_author) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => {
                let a_bytes = serde_json::to_vec(a_value).unwrap_or_default();
                let b_bytes = serde_json::to_vec(b_value).unwrap_or_default();
                a_bytes > b_bytes
            }
        },
    }
}

impl Crdt for LwwRegister {
    type Op = LwwOp;
    type Delta = LwwDelta;
    type Value = LwwValue;

    fn apply(&mut self, op: LwwOp) -> Result<Option<LwwDelta>, CrdtError> {
        let new_entry = LwwEntry {
            value: op.value,
            hlc: op.hlc,
            author: op.author,
        };

        let should_apply = match &self.entry {
            None => true,
            Some(existing) => wins_over(new_entry.hlc, new_entry.author, &new_entry.value, existing.hlc, existing.author, &existing.value),
        };

        if should_apply {
            // Store first, then borrow for the delta — avoids a second clone.
            self.entry = Some(new_entry);
            Ok(Some(LwwDelta { entry: self.entry.clone() }))
        } else {
            Ok(None) // Stale write — no state change
        }
    }

    fn merge(&mut self, other: &LwwRegister) {
        match (&self.entry, &other.entry) {
            (_, None) => {}
            (None, Some(o)) => self.entry = Some(o.clone()),
            (Some(s), Some(o)) => {
                if wins_over(o.hlc, o.author, &o.value, s.hlc, s.author, &s.value) {
                    self.entry = Some(o.clone());
                }
            }
        }
    }

    fn merge_delta(&mut self, delta: LwwDelta) {
        let tmp = LwwRegister { entry: delta.entry };
        self.merge(&tmp);
    }

    fn delta_since(&self, since: &VectorClock) -> Option<LwwDelta> {
        // For LWWRegister, the vc encodes the author_id → logical version seen.
        // We include our entry if the caller hasn't seen this author/version combo.
        match &self.entry {
            None => None,
            Some(e) => {
                let seen_version = since.get(e.author);
                // Encode the HLC logical counter as the "version" for comparison.
                // If the caller has seen at least this logical count from this author,
                // they already have this entry.
                if (e.hlc.logical as u32) > seen_version {
                    Some(LwwDelta { entry: Some(e.clone()) })
                } else {
                    None
                }
            }
        }
    }

    fn value(&self) -> LwwValue {
        match &self.entry {
            None => LwwValue { value: None, updated_at_ms: None, author: None },
            Some(e) => LwwValue {
                value: Some(e.value.clone()),
                updated_at_ms: Some(e.hlc.wall_ms),
                author: Some(e.author),
            },
        }
    }

    fn is_empty(&self) -> bool {
        self.entry.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(wall_ms: u64, logical: u16, node_id: u64) -> HybridLogicalClock {
        HybridLogicalClock { wall_ms, logical, node_id }
    }

    fn write(wall_ms: u64, logical: u16, author: u64, val: &str) -> LwwOp {
        LwwOp {
            value: JsonValue::String(val.to_string()),
            hlc: hlc(wall_ms, logical, author),
            author,
        }
    }

    #[test]
    fn later_write_wins() {
        let mut r = LwwRegister::default();
        r.apply(write(100, 0, 1, "first")).unwrap();
        r.apply(write(200, 0, 1, "second")).unwrap();
        assert_eq!(r.value().value, Some(JsonValue::String("second".into())));
    }

    #[test]
    fn earlier_write_rejected() {
        let mut r = LwwRegister::default();
        r.apply(write(200, 0, 1, "second")).unwrap();
        let delta = r.apply(write(100, 0, 1, "first")).unwrap();
        assert!(delta.is_none());
        assert_eq!(r.value().value, Some(JsonValue::String("second".into())));
    }

    #[test]
    fn tie_break_on_author() {
        let mut r = LwwRegister::default();
        r.apply(write(100, 0, 1, "from_client_1")).unwrap();

        let mut r2 = LwwRegister::default();
        r2.apply(write(100, 0, 2, "from_client_2")).unwrap(); // same HLC, higher author

        r.merge(&r2);
        // client 2 has higher author id → wins
        assert_eq!(r.value().value, Some(JsonValue::String("from_client_2".into())));
    }

    #[test]
    fn merge_commutative() {
        let mut a = LwwRegister::default();
        a.apply(write(100, 0, 1, "a")).unwrap();

        let mut b = LwwRegister::default();
        b.apply(write(200, 0, 2, "b")).unwrap();

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab, ba);
    }

    #[test]
    fn merge_idempotent() {
        let mut r = LwwRegister::default();
        r.apply(write(100, 0, 1, "value")).unwrap();
        let copy = r.clone();
        r.merge(&copy);
        assert_eq!(r, copy);
    }

    #[test]
    fn merge_associative() {
        let mut a = LwwRegister::default();
        a.apply(write(100, 0, 1, "a")).unwrap();
        let mut b = LwwRegister::default();
        b.apply(write(200, 0, 2, "b")).unwrap();
        let mut c = LwwRegister::default();
        c.apply(write(150, 0, 3, "c")).unwrap();

        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(ab_c, a_bc);
    }
}
