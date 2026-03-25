use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{
    gcounter::{GCounter, GCounterDelta, GCounterOp},
    lwwregister::{LwwDelta, LwwOp, LwwRegister},
    orset::{ORSet, ORSetDelta, ORSetOp},
    pncounter::{PNCounter, PNCounterDelta, PNCounterOp},
    presence::{Presence, PresenceDelta, PresenceOp},
    registry::CrdtType,
    rga::{Rga, RgaDelta, RgaOp},
    tree::{TreeCrdt, TreeDelta, TreeOp},
    Crdt, CrdtError, VectorClock,
};

//
// Each key maps to exactly one CRDT leaf type (GCounter, PNCounter, ORSet,
// LwwRegister, or Presence). The type is fixed at creation time: applying an
// op of the wrong type to an existing key is an InvalidOp error.
//
// Merge semantics: per-key lattice join. Keys are permanent (no deletion).
// Two replicas with the same key but different types: self wins (no panic).
//
// Nesting is intentionally disallowed: CrdtInnerOp and CrdtMapInnerValue
// exclude the CRDTMap variant to prevent infinite type recursion without Box.

#[must_use]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum CrdtValueDelta {
    GCounter(GCounterDelta),
    PNCounter(PNCounterDelta),
    ORSet(ORSetDelta),
    LwwRegister(LwwDelta),
    Presence(PresenceDelta),
    RGA(RgaDelta),
    Tree(TreeDelta),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CrdtInnerOp {
    GCounter(GCounterOp),
    PNCounter(PNCounterOp),
    ORSet(ORSetOp),
    LwwRegister(LwwOp),
    Presence(PresenceOp),
    RGA(RgaOp),
    Tree(TreeOp),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CRDTMapOp {
    pub key: String,
    pub crdt_type: CrdtType,
    pub op: CrdtInnerOp,
}

#[must_use]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CRDTMapDelta {
    pub deltas: HashMap<String, CrdtValueDelta>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum CrdtMapInnerValue {
    GCounter(GCounter),
    PNCounter(PNCounter),
    ORSet(ORSet),
    LwwRegister(LwwRegister),
    Presence(Presence),
    RGA(Rga),
    Tree(TreeCrdt),
}

impl CrdtMapInnerValue {
    pub fn new(crdt_type: CrdtType) -> Self {
        match crdt_type {
            CrdtType::GCounter    => Self::GCounter(GCounter::default()),
            CrdtType::PNCounter   => Self::PNCounter(PNCounter::default()),
            CrdtType::ORSet       => Self::ORSet(ORSet::default()),
            CrdtType::LwwRegister => Self::LwwRegister(LwwRegister::default()),
            CrdtType::Presence    => Self::Presence(Presence::default()),
            CrdtType::Rga         => Self::RGA(Rga::default()),
            CrdtType::Tree        => Self::Tree(TreeCrdt::default()),
            CrdtType::CRDTMap     => unreachable!("CRDTMap cannot be nested"),
        }
    }

    pub fn crdt_type(&self) -> CrdtType {
        match self {
            Self::GCounter(_)    => CrdtType::GCounter,
            Self::PNCounter(_)   => CrdtType::PNCounter,
            Self::ORSet(_)       => CrdtType::ORSet,
            Self::LwwRegister(_) => CrdtType::LwwRegister,
            Self::Presence(_)    => CrdtType::Presence,
            Self::RGA(_)         => CrdtType::Rga,
            Self::Tree(_)        => CrdtType::Tree,
        }
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            Self::GCounter(v)    => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::PNCounter(v)   => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::ORSet(v)       => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::LwwRegister(v) => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::Presence(v)    => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::RGA(v)         => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::Tree(v)        => serde_json::to_value(v.value()).unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CRDTMap {
    pub entries: HashMap<String, CrdtMapInnerValue>,
}

fn apply_inner(
    inner: &mut CrdtMapInnerValue,
    op: CrdtInnerOp,
) -> Result<Option<CrdtValueDelta>, CrdtError> {
    match (inner, op) {
        (CrdtMapInnerValue::GCounter(v),    CrdtInnerOp::GCounter(o))    => Ok(v.apply(o)?.map(CrdtValueDelta::GCounter)),
        (CrdtMapInnerValue::PNCounter(v),   CrdtInnerOp::PNCounter(o))   => Ok(v.apply(o)?.map(CrdtValueDelta::PNCounter)),
        (CrdtMapInnerValue::ORSet(v),       CrdtInnerOp::ORSet(o))       => Ok(v.apply(o)?.map(CrdtValueDelta::ORSet)),
        (CrdtMapInnerValue::LwwRegister(v), CrdtInnerOp::LwwRegister(o)) => Ok(v.apply(o)?.map(CrdtValueDelta::LwwRegister)),
        (CrdtMapInnerValue::Presence(v),    CrdtInnerOp::Presence(o))    => Ok(v.apply(o)?.map(CrdtValueDelta::Presence)),
        (CrdtMapInnerValue::RGA(v),         CrdtInnerOp::RGA(o))         => Ok(v.apply(o)?.map(CrdtValueDelta::RGA)),
        (CrdtMapInnerValue::Tree(v),        CrdtInnerOp::Tree(o))        => Ok(v.apply(o)?.map(CrdtValueDelta::Tree)),
        _ => Err(CrdtError::InvalidOp("inner op type does not match inner crdt type".into())),
    }
}

fn merge_inner(a: &mut CrdtMapInnerValue, b: &CrdtMapInnerValue) {
    match (a, b) {
        (CrdtMapInnerValue::GCounter(x),    CrdtMapInnerValue::GCounter(y))    => x.merge(y),
        (CrdtMapInnerValue::PNCounter(x),   CrdtMapInnerValue::PNCounter(y))   => x.merge(y),
        (CrdtMapInnerValue::ORSet(x),       CrdtMapInnerValue::ORSet(y))       => x.merge(y),
        (CrdtMapInnerValue::LwwRegister(x), CrdtMapInnerValue::LwwRegister(y)) => x.merge(y),
        (CrdtMapInnerValue::Presence(x),    CrdtMapInnerValue::Presence(y))    => x.merge(y),
        (CrdtMapInnerValue::RGA(x),         CrdtMapInnerValue::RGA(y))         => x.merge(y),
        (CrdtMapInnerValue::Tree(x),        CrdtMapInnerValue::Tree(y))        => x.merge(y),
        _ => {} // type mismatch — self wins
    }
}

fn merge_delta_inner(inner: &mut CrdtMapInnerValue, delta: CrdtValueDelta) {
    match (inner, delta) {
        (CrdtMapInnerValue::GCounter(v),    CrdtValueDelta::GCounter(d))    => v.merge_delta(d),
        (CrdtMapInnerValue::PNCounter(v),   CrdtValueDelta::PNCounter(d))   => v.merge_delta(d),
        (CrdtMapInnerValue::ORSet(v),       CrdtValueDelta::ORSet(d))       => v.merge_delta(d),
        (CrdtMapInnerValue::LwwRegister(v), CrdtValueDelta::LwwRegister(d)) => v.merge_delta(d),
        (CrdtMapInnerValue::Presence(v),    CrdtValueDelta::Presence(d))    => v.merge_delta(d),
        (CrdtMapInnerValue::RGA(v),         CrdtValueDelta::RGA(d))         => v.merge_delta(d),
        (CrdtMapInnerValue::Tree(v),        CrdtValueDelta::Tree(d))        => v.merge_delta(d),
        _ => {} // type mismatch — no-op
    }
}

fn inner_from_delta(delta: CrdtValueDelta) -> CrdtMapInnerValue {
    match delta {
        CrdtValueDelta::GCounter(d) => {
            let mut v = GCounter::default();
            v.merge_delta(d);
            CrdtMapInnerValue::GCounter(v)
        }
        CrdtValueDelta::PNCounter(d) => {
            let mut v = PNCounter::default();
            v.merge_delta(d);
            CrdtMapInnerValue::PNCounter(v)
        }
        CrdtValueDelta::ORSet(d) => {
            let mut v = ORSet::default();
            v.merge_delta(d);
            CrdtMapInnerValue::ORSet(v)
        }
        CrdtValueDelta::LwwRegister(d) => {
            let mut v = LwwRegister::default();
            v.merge_delta(d);
            CrdtMapInnerValue::LwwRegister(v)
        }
        CrdtValueDelta::Presence(d) => {
            let mut v = Presence::default();
            v.merge_delta(d);
            CrdtMapInnerValue::Presence(v)
        }
        CrdtValueDelta::RGA(d) => {
            let mut v = Rga::default();
            v.merge_delta(d);
            CrdtMapInnerValue::RGA(v)
        }
        CrdtValueDelta::Tree(d) => {
            let mut v = TreeCrdt::default();
            v.merge_delta(d);
            CrdtMapInnerValue::Tree(v)
        }
    }
}

fn delta_since_inner(inner: &CrdtMapInnerValue, since: &VectorClock) -> Option<CrdtValueDelta> {
    match inner {
        CrdtMapInnerValue::GCounter(v)    => v.delta_since(since).map(CrdtValueDelta::GCounter),
        CrdtMapInnerValue::PNCounter(v)   => v.delta_since(since).map(CrdtValueDelta::PNCounter),
        CrdtMapInnerValue::ORSet(v)       => v.delta_since(since).map(CrdtValueDelta::ORSet),
        CrdtMapInnerValue::LwwRegister(v) => v.delta_since(since).map(CrdtValueDelta::LwwRegister),
        CrdtMapInnerValue::Presence(v)    => v.delta_since(since).map(CrdtValueDelta::Presence),
        CrdtMapInnerValue::RGA(v)         => v.delta_since(since).map(CrdtValueDelta::RGA),
        CrdtMapInnerValue::Tree(v)        => v.delta_since(since).map(CrdtValueDelta::Tree),
    }
}

pub type CrdtMapValue = HashMap<String, serde_json::Value>;

impl Crdt for CRDTMap {
    type Op    = CRDTMapOp;
    type Delta = CRDTMapDelta;
    type Value = CrdtMapValue;

    fn apply(&mut self, op: CRDTMapOp) -> Result<Option<CRDTMapDelta>, CrdtError> {
        match self.entries.get_mut(&op.key) {
            Some(inner) => {
                if inner.crdt_type() != op.crdt_type {
                    return Err(CrdtError::InvalidOp(format!(
                        "key '{}' is type {:?}, op is type {:?}",
                        op.key,
                        inner.crdt_type(),
                        op.crdt_type,
                    )));
                }
                let maybe_delta = apply_inner(inner, op.op)?;
                Ok(maybe_delta.map(|d| {
                    CRDTMapDelta { deltas: [(op.key, d)].into_iter().collect() }
                }))
            }
            None => {
                let mut inner = CrdtMapInnerValue::new(op.crdt_type);
                let maybe_delta = apply_inner(&mut inner, op.op)?;
                self.entries.insert(op.key.clone(), inner);
                Ok(maybe_delta.map(|d| {
                    CRDTMapDelta { deltas: [(op.key, d)].into_iter().collect() }
                }))
            }
        }
    }

    fn merge(&mut self, other: &CRDTMap) {
        for (key, other_inner) in &other.entries {
            match self.entries.get_mut(key) {
                None => {
                    self.entries.insert(key.clone(), other_inner.clone());
                }
                Some(self_inner) => {
                    merge_inner(self_inner, other_inner);
                }
            }
        }
    }

    fn merge_delta(&mut self, delta: CRDTMapDelta) {
        for (key, value_delta) in delta.deltas {
            match self.entries.get_mut(&key) {
                None => {
                    let inner = inner_from_delta(value_delta);
                    self.entries.insert(key, inner);
                }
                Some(inner) => {
                    merge_delta_inner(inner, value_delta);
                }
            }
        }
    }

    fn delta_since(&self, since: &VectorClock) -> Option<CRDTMapDelta> {
        let deltas: HashMap<String, CrdtValueDelta> = self
            .entries
            .iter()
            .filter_map(|(key, inner)| {
                delta_since_inner(inner, since).map(|d| (key.clone(), d))
            })
            .collect();

        if deltas.is_empty() {
            None
        } else {
            Some(CRDTMapDelta { deltas })
        }
    }

    fn value(&self) -> CrdtMapValue {
        self.entries
            .iter()
            .map(|(key, inner)| (key.clone(), inner.to_json_value()))
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::gcounter::GCounterOp;
    use crate::crdt::pncounter::PNCounterOp;

    fn gcounter_op(key: &str, client_id: u64, amount: u64) -> CRDTMapOp {
        CRDTMapOp {
            key: key.to_string(),
            crdt_type: CrdtType::GCounter,
            op: CrdtInnerOp::GCounter(GCounterOp { client_id, amount }),
        }
    }

    fn pncounter_op_inc(key: &str, client_id: u64, amount: u64) -> CRDTMapOp {
        CRDTMapOp {
            key: key.to_string(),
            crdt_type: CrdtType::PNCounter,
            op: CrdtInnerOp::PNCounter(PNCounterOp::Increment { client_id, amount }),
        }
    }

    #[test]
    fn apply_and_value() {
        let mut m = CRDTMap::default();
        m.apply(gcounter_op("score", 1, 10)).unwrap();
        m.apply(gcounter_op("score", 1, 5)).unwrap();
        let val = m.value();
        assert!(val.contains_key("score"));
    }

    #[test]
    fn type_mismatch_returns_error() {
        let mut m = CRDTMap::default();
        m.apply(gcounter_op("x", 1, 1)).unwrap();
        let result = m.apply(pncounter_op_inc("x", 1, 1));
        assert!(result.is_err());
        match result.unwrap_err() {
            CrdtError::InvalidOp(msg) => assert!(msg.contains("'x'")),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn merge_commutative() {
        let mut a = CRDTMap::default();
        a.apply(gcounter_op("x", 1, 10)).unwrap();

        let mut b = CRDTMap::default();
        b.apply(gcounter_op("y", 2, 20)).unwrap();

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn merge_same_key_calls_inner_merge() {
        let mut a = CRDTMap::default();
        a.apply(gcounter_op("hits", 1, 10)).unwrap();

        let mut b = CRDTMap::default();
        b.apply(gcounter_op("hits", 2, 20)).unwrap();

        a.merge(&b);

        // GCounter merge = component-wise max; total = 10 + 20 = 30
        let val = a.value();
        let hits = val.get("hits").unwrap();
        assert_eq!(hits["total"], 30);
    }

    #[test]
    fn merge_idempotent() {
        let mut m = CRDTMap::default();
        m.apply(gcounter_op("k", 1, 5)).unwrap();
        let copy = m.clone();
        m.merge(&copy);
        assert_eq!(m, copy);
    }

    #[test]
    fn merge_associative() {
        let mut a = CRDTMap::default();
        a.apply(gcounter_op("a", 1, 1)).unwrap();
        let mut b = CRDTMap::default();
        b.apply(gcounter_op("b", 2, 2)).unwrap();
        let mut c = CRDTMap::default();
        c.apply(gcounter_op("c", 3, 3)).unwrap();

        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(ab_c.value(), a_bc.value());
    }

    #[test]
    fn delta_roundtrip() {
        let mut m = CRDTMap::default();
        m.apply(gcounter_op("score", 1, 42)).unwrap();

        let vc = VectorClock::default();
        let delta = m.delta_since(&vc).expect("should have delta");

        let mut m2 = CRDTMap::default();
        m2.merge_delta(delta);

        assert_eq!(m.value(), m2.value());
    }

    #[test]
    fn merge_delta_for_unknown_key() {
        let mut a = CRDTMap::default();
        a.apply(gcounter_op("x", 1, 7)).unwrap();

        let vc = VectorClock::default();
        let delta = a.delta_since(&vc).unwrap();

        let mut b = CRDTMap::default();
        b.merge_delta(delta);

        assert_eq!(b.value(), a.value());
    }

    #[test]
    fn is_empty_on_default() {
        assert!(CRDTMap::default().is_empty());
    }

    #[test]
    fn not_empty_after_apply() {
        let mut m = CRDTMap::default();
        m.apply(gcounter_op("k", 1, 1)).unwrap();
        assert!(!m.is_empty());
    }

    #[test]
    fn new_key_absent_before_apply() {
        let mut m = CRDTMap::default();
        assert!(!m.value().contains_key("k"));
        m.apply(gcounter_op("k", 1, 1)).unwrap();
        assert!(m.value().contains_key("k"));
    }
}
