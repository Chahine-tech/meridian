use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{Crdt, CrdtError, VectorClock};

// ---------------------------------------------------------------------------
// GCounter — Grow-Only Counter
// ---------------------------------------------------------------------------
//
// State:  BTreeMap<client_id, count>
// Merge:  max(self[i], other[i]) for all i
// Value:  sum of all slots
//
// Lattice order: a ≤ b  iff  ∀i, a[i] ≤ b[i]
// The merge is the least upper bound (join) in this lattice.
//
// Each client only ever increments its own slot → no coordination needed.

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GCounter {
    /// Maps client_id → cumulative increment from that client.
    pub counters: BTreeMap<u64, u64>,
}

// ---------------------------------------------------------------------------
// Op + Delta types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GCounterOp {
    pub client_id: u64,
    pub amount: u64,
}

/// Delta = the subset of counters that changed.
/// Represented as a sparse map (same shape as the full state).
#[must_use]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GCounterDelta {
    pub counters: BTreeMap<u64, u64>,
}

// ---------------------------------------------------------------------------
// Value
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GCounterValue {
    pub total: u64,
    /// Per-client breakdown (client_id as string for JSON compat).
    pub counters: BTreeMap<u64, u64>,
}

// ---------------------------------------------------------------------------
// Crdt impl
// ---------------------------------------------------------------------------

impl Crdt for GCounter {
    type Op = GCounterOp;
    type Delta = GCounterDelta;
    type Value = GCounterValue;

    fn apply(&mut self, op: GCounterOp) -> Result<Option<GCounterDelta>, CrdtError> {
        if op.amount == 0 {
            return Ok(None);
        }
        let entry = self.counters.entry(op.client_id).or_insert(0);
        *entry = entry.saturating_add(op.amount);

        let mut delta_counters = BTreeMap::new();
        delta_counters.insert(op.client_id, *entry);
        Ok(Some(GCounterDelta { counters: delta_counters }))
    }

    fn merge(&mut self, other: &GCounter) {
        for (&client_id, &count) in &other.counters {
            let entry = self.counters.entry(client_id).or_insert(0);
            if count > *entry {
                *entry = count;
            }
        }
    }

    fn merge_delta(&mut self, delta: GCounterDelta) {
        for (client_id, count) in delta.counters {
            let entry = self.counters.entry(client_id).or_insert(0);
            if count > *entry {
                *entry = count;
            }
        }
    }

    fn delta_since(&self, since: &VectorClock) -> Option<GCounterDelta> {
        // For GCounter we use the VectorClock as a proxy: each client_id maps to
        // a version in the clock. If the clock has a version for client_id, we
        // include the counter only if it's strictly greater (client has incremented
        // since that version was recorded).
        //
        // Simpler heuristic: include all entries the caller hasn't seen.
        // The caller encodes "seen" as vc.get(client_id) > 0 iff they've had
        // at least one sync. We include entries whose count > 0 and whose
        // vc entry is 0 (never seen) OR whose stored count differs.
        //
        // For GCounter specifically: since we can't know which count corresponds
        // to which vc version without extra bookkeeping, we use a conservative
        // approach — include all entries not dominated by the vc.
        // (A vc entry of N means the receiver has seen at least N increments from
        // that client, encoded as the raw counter value at last sync.)
        let changed: BTreeMap<u64, u64> = self
            .counters
            .iter()
            .filter(|(client_id, count)| {
                // Include if the receiver hasn't seen this count yet.
                // We use the vc entry as the "last known count" for that client.
                **count > since.get(**client_id) as u64
            })
            .map(|(&k, &v)| (k, v))
            .collect();

        if changed.is_empty() {
            None
        } else {
            Some(GCounterDelta { counters: changed })
        }
    }

    fn value(&self) -> GCounterValue {
        let total = self.counters.values().sum();
        GCounterValue {
            total,
            counters: self.counters.clone(),
        }
    }

    fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make(entries: &[(u64, u64)]) -> GCounter {
        let mut g = GCounter::default();
        for &(id, amt) in entries {
            g.apply(GCounterOp { client_id: id, amount: amt }).unwrap();
        }
        g
    }

    #[test]
    fn increment_and_value() {
        let g = make(&[(1, 5), (2, 3)]);
        assert_eq!(g.value().total, 8);
    }

    #[test]
    fn merge_takes_max() {
        let mut a = make(&[(1, 10)]);
        let b = make(&[(1, 5), (2, 7)]);
        a.merge(&b);
        assert_eq!(a.value().total, 17); // max(10,5) + 7
    }

    #[test]
    fn merge_idempotent() {
        let mut a = make(&[(1, 10), (2, 5)]);
        let b = a.clone();
        a.merge(&b);
        assert_eq!(a, b);
    }

    #[test]
    fn merge_commutative() {
        let a = make(&[(1, 3), (2, 7)]);
        let b = make(&[(1, 6), (3, 2)]);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab, ba);
    }

    #[test]
    fn merge_associative() {
        let a = make(&[(1, 1)]);
        let b = make(&[(2, 2)]);
        let c = make(&[(3, 3)]);

        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut a_bc = a.clone();
        let mut bc = b.clone();
        bc.merge(&c);
        a_bc.merge(&bc);

        assert_eq!(ab_c, a_bc);
    }

    #[test]
    fn delta_apply() {
        let mut g = make(&[(1, 5)]);
        let delta = g.apply(GCounterOp { client_id: 1, amount: 3 }).unwrap().unwrap();
        assert_eq!(delta.counters[&1], 8);
    }

    #[test]
    fn zero_amount_no_delta() {
        let mut g = GCounter::default();
        let delta = g.apply(GCounterOp { client_id: 1, amount: 0 }).unwrap();
        assert!(delta.is_none());
    }
}
