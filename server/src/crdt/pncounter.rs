use serde::{Deserialize, Serialize};

use super::{
    gcounter::{GCounter, GCounterDelta, GCounterOp},
    Crdt, CrdtError, VectorClock,
};

// ---------------------------------------------------------------------------
// PNCounter — Positive-Negative Counter
// ---------------------------------------------------------------------------
//
// Composed of two GCounters: P (increments) and N (decrements).
// value() = sum(P) - sum(N)  — can be negative, do NOT clamp.
//
// Applications that need non-negative semantics (e.g. inventory stock)
// must enforce that constraint at the application layer.

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PNCounter {
    pub pos: GCounter,
    pub neg: GCounter,
}

// ---------------------------------------------------------------------------
// Op + Delta
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PNCounterOp {
    Increment { client_id: u64, amount: u64 },
    Decrement { client_id: u64, amount: u64 },
}

#[must_use]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PNCounterDelta {
    pub pos: Option<GCounterDelta>,
    pub neg: Option<GCounterDelta>,
}

// ---------------------------------------------------------------------------
// Value
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PNCounterValue {
    /// Signed value: sum(pos) - sum(neg). May be negative.
    pub value: i64,
}

// ---------------------------------------------------------------------------
// Crdt impl
// ---------------------------------------------------------------------------

impl Crdt for PNCounter {
    type Op = PNCounterOp;
    type Delta = PNCounterDelta;
    type Value = PNCounterValue;

    fn apply(&mut self, op: PNCounterOp) -> Result<Option<PNCounterDelta>, CrdtError> {
        match op {
            PNCounterOp::Increment { client_id, amount } => {
                let delta = self.pos.apply(GCounterOp { client_id, amount })?;
                Ok(delta.map(|d| PNCounterDelta { pos: Some(d), neg: None }))
            }
            PNCounterOp::Decrement { client_id, amount } => {
                let delta = self.neg.apply(GCounterOp { client_id, amount })?;
                Ok(delta.map(|d| PNCounterDelta { pos: None, neg: Some(d) }))
            }
        }
    }

    fn merge(&mut self, other: &PNCounter) {
        self.pos.merge(&other.pos);
        self.neg.merge(&other.neg);
    }

    fn merge_delta(&mut self, delta: PNCounterDelta) {
        if let Some(d) = delta.pos {
            self.pos.merge_delta(d);
        }
        if let Some(d) = delta.neg {
            self.neg.merge_delta(d);
        }
    }

    fn delta_since(&self, since: &VectorClock) -> Option<PNCounterDelta> {
        let pos = self.pos.delta_since(since);
        let neg = self.neg.delta_since(since);
        if pos.is_none() && neg.is_none() {
            None
        } else {
            Some(PNCounterDelta { pos, neg })
        }
    }

    fn value(&self) -> PNCounterValue {
        let p = self.pos.value().total as i64;
        let n = self.neg.value().total as i64;
        PNCounterValue { value: p - n }
    }

    fn is_empty(&self) -> bool {
        self.pos.is_empty() && self.neg.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make(ops: &[PNCounterOp]) -> PNCounter {
        let mut c = PNCounter::default();
        for op in ops {
            c.apply(op.clone()).unwrap();
        }
        c
    }

    #[test]
    fn increment_decrement() {
        let c = make(&[
            PNCounterOp::Increment { client_id: 1, amount: 10 },
            PNCounterOp::Decrement { client_id: 1, amount: 3 },
        ]);
        assert_eq!(c.value().value, 7);
    }

    #[test]
    fn can_go_negative() {
        let c = make(&[
            PNCounterOp::Increment { client_id: 1, amount: 2 },
            PNCounterOp::Decrement { client_id: 1, amount: 5 },
        ]);
        assert_eq!(c.value().value, -3);
    }

    #[test]
    fn merge_commutative() {
        let a = make(&[PNCounterOp::Increment { client_id: 1, amount: 10 }]);
        let b = make(&[PNCounterOp::Decrement { client_id: 2, amount: 3 }]);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.value().value, ba.value().value);
        assert_eq!(ab, ba);
    }

    #[test]
    fn merge_idempotent() {
        let mut a = make(&[
            PNCounterOp::Increment { client_id: 1, amount: 5 },
            PNCounterOp::Decrement { client_id: 1, amount: 2 },
        ]);
        let b = a.clone();
        a.merge(&b);
        assert_eq!(a, b);
    }

    #[test]
    fn merge_associative() {
        let a = make(&[PNCounterOp::Increment { client_id: 1, amount: 1 }]);
        let b = make(&[PNCounterOp::Decrement { client_id: 2, amount: 2 }]);
        let c = make(&[PNCounterOp::Increment { client_id: 3, amount: 3 }]);

        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(ab_c, a_bc);
    }

    #[test]
    fn concurrent_inc_dec_both_reflected() {
        // Two clients: one increments, one decrements concurrently.
        let mut a = PNCounter::default();
        a.apply(PNCounterOp::Increment { client_id: 1, amount: 10 }).unwrap();

        let mut b = PNCounter::default();
        b.apply(PNCounterOp::Decrement { client_id: 2, amount: 4 }).unwrap();

        a.merge(&b);
        assert_eq!(a.value().value, 6);
    }
}
