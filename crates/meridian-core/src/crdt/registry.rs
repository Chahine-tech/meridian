use serde::{Deserialize, Serialize};

use super::{
    crdtmap::{CRDTMap, CRDTMapOp},
    gcounter::{GCounter, GCounterOp},
    lwwregister::{LwwOp, LwwRegister},
    orset::{ORSet, ORSetOp},
    pncounter::{PNCounter, PNCounterOp},
    presence::{Presence, PresenceOp},
    Crdt, CrdtError, VectorClock,
};

// ---------------------------------------------------------------------------
// CrdtType — discriminant byte (matches wire protocol tag)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum CrdtType {
    GCounter    = 0x01,
    PNCounter   = 0x02,
    ORSet       = 0x03,
    LwwRegister = 0x04,
    Presence    = 0x05,
    CRDTMap     = 0x06,
}

impl CrdtType {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::GCounter),
            0x02 => Some(Self::PNCounter),
            0x03 => Some(Self::ORSet),
            0x04 => Some(Self::LwwRegister),
            0x05 => Some(Self::Presence),
            0x06 => Some(Self::CRDTMap),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GCounter    => "gcounter",
            Self::PNCounter   => "pncounter",
            Self::ORSet       => "orset",
            Self::LwwRegister => "lwwregister",
            Self::Presence    => "presence",
            Self::CRDTMap     => "crdtmap",
        }
    }
}

impl std::fmt::Display for CrdtType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for CrdtType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "gcounter"    => Ok(Self::GCounter),
            "pncounter"   => Ok(Self::PNCounter),
            "orset"       => Ok(Self::ORSet),
            "lwwregister" => Ok(Self::LwwRegister),
            "presence"    => Ok(Self::Presence),
            "crdtmap"     => Ok(Self::CRDTMap),
            other         => Err(format!("unknown crdt type: {other}")),
        }
    }
}

// ---------------------------------------------------------------------------
// CrdtValue — enum wrapping all 5 types (used in storage)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CrdtValue {
    GCounter(GCounter),
    PNCounter(PNCounter),
    ORSet(ORSet),
    LwwRegister(LwwRegister),
    Presence(Presence),
    CRDTMap(CRDTMap),
}

impl CrdtValue {
    pub fn crdt_type(&self) -> CrdtType {
        match self {
            Self::GCounter(_)    => CrdtType::GCounter,
            Self::PNCounter(_)   => CrdtType::PNCounter,
            Self::ORSet(_)       => CrdtType::ORSet,
            Self::LwwRegister(_) => CrdtType::LwwRegister,
            Self::Presence(_)    => CrdtType::Presence,
            Self::CRDTMap(_)     => CrdtType::CRDTMap,
        }
    }

    pub fn new(crdt_type: CrdtType) -> Self {
        match crdt_type {
            CrdtType::GCounter    => Self::GCounter(GCounter::default()),
            CrdtType::PNCounter   => Self::PNCounter(PNCounter::default()),
            CrdtType::ORSet       => Self::ORSet(ORSet::default()),
            CrdtType::LwwRegister => Self::LwwRegister(LwwRegister::default()),
            CrdtType::Presence    => Self::Presence(Presence::default()),
            CrdtType::CRDTMap     => Self::CRDTMap(CRDTMap::default()),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::GCounter(v)    => v.is_empty(),
            Self::PNCounter(v)   => v.is_empty(),
            Self::ORSet(v)       => v.is_empty(),
            Self::LwwRegister(v) => v.is_empty(),
            Self::Presence(v)    => v.is_empty(),
            Self::CRDTMap(v)     => v.is_empty(),
        }
    }

    /// Serialize to JSON value for HTTP responses.
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            Self::GCounter(v)    => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::PNCounter(v)   => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::ORSet(v)       => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::LwwRegister(v) => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::Presence(v)    => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::CRDTMap(v)     => serde_json::to_value(v.value()).unwrap_or_default(),
        }
    }

    /// Serialize full state to msgpack bytes (for sled storage).
    pub fn to_msgpack(&self) -> Result<Vec<u8>, CrdtError> {
        rmp_serde::encode::to_vec_named(self).map_err(CrdtError::Serialization)
    }

    /// Deserialize from msgpack bytes.
    pub fn from_msgpack(bytes: &[u8]) -> Result<Self, CrdtError> {
        rmp_serde::decode::from_slice(bytes).map_err(CrdtError::Deserialization)
    }

    /// Compute delta since `vc` and serialize to msgpack.
    pub fn delta_since_msgpack(&self, vc: &VectorClock) -> Result<Option<Vec<u8>>, CrdtError> {
        let delta = match self {
            Self::GCounter(v)    => v.delta_since(vc).map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::PNCounter(v)   => v.delta_since(vc).map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::ORSet(v)       => v.delta_since(vc).map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::LwwRegister(v) => v.delta_since(vc).map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::Presence(v)    => v.delta_since(vc).map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::CRDTMap(v)     => v.delta_since(vc).map(|d| rmp_serde::encode::to_vec_named(&d)),
        };
        match delta {
            None => Ok(None),
            Some(Ok(bytes)) => Ok(Some(bytes)),
            Some(Err(e)) => Err(CrdtError::Serialization(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// CrdtOp — enum wrapping all 5 op types (used in WAL + WebSocket protocol)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CrdtOp {
    GCounter(GCounterOp),
    PNCounter(PNCounterOp),
    ORSet(ORSetOp),
    LwwRegister(LwwOp),
    Presence(PresenceOp),
    CRDTMap(CRDTMapOp),
}

impl CrdtOp {
    pub fn crdt_type(&self) -> CrdtType {
        match self {
            Self::GCounter(_)    => CrdtType::GCounter,
            Self::PNCounter(_)   => CrdtType::PNCounter,
            Self::ORSet(_)       => CrdtType::ORSet,
            Self::LwwRegister(_) => CrdtType::LwwRegister,
            Self::Presence(_)    => CrdtType::Presence,
            Self::CRDTMap(_)     => CrdtType::CRDTMap,
        }
    }
}

/// Apply a CrdtOp to a CrdtValue. Returns a serialized delta on success.
pub fn apply_op(value: &mut CrdtValue, op: CrdtOp) -> Result<Option<Vec<u8>>, CrdtError> {
    macro_rules! apply_and_serialize {
        ($crdt:expr, $op:expr) => {{
            let delta = $crdt.apply($op)?;
            match delta {
                None => Ok(None),
                Some(d) => Ok(Some(rmp_serde::encode::to_vec_named(&d)?)),
            }
        }};
    }

    match (value, op) {
        (CrdtValue::GCounter(v),    CrdtOp::GCounter(op))    => apply_and_serialize!(v, op),
        (CrdtValue::PNCounter(v),   CrdtOp::PNCounter(op))   => apply_and_serialize!(v, op),
        (CrdtValue::ORSet(v),       CrdtOp::ORSet(op))       => apply_and_serialize!(v, op),
        (CrdtValue::LwwRegister(v), CrdtOp::LwwRegister(op)) => apply_and_serialize!(v, op),
        (CrdtValue::Presence(v),    CrdtOp::Presence(op))    => apply_and_serialize!(v, op),
        (CrdtValue::CRDTMap(v),     CrdtOp::CRDTMap(op))     => apply_and_serialize!(v, op),
        _ => Err(CrdtError::InvalidOp("op type does not match crdt type".into())),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::gcounter::GCounterOp;

    #[test]
    fn crdt_type_roundtrip() {
        for t in [
            CrdtType::GCounter,
            CrdtType::PNCounter,
            CrdtType::ORSet,
            CrdtType::LwwRegister,
            CrdtType::Presence,
            CrdtType::CRDTMap,
        ] {
            let s = t.as_str();
            let parsed: CrdtType = s.parse().unwrap();
            assert_eq!(parsed, t);
            assert_eq!(CrdtType::from_u8(t as u8), Some(t));
        }
    }

    #[test]
    fn apply_op_gcounter() {
        let mut v = CrdtValue::new(CrdtType::GCounter);
        let op = CrdtOp::GCounter(GCounterOp { client_id: 1, amount: 5 });
        let delta_bytes = apply_op(&mut v, op).unwrap().unwrap();
        assert!(!delta_bytes.is_empty());
    }

    #[test]
    fn apply_op_type_mismatch_is_error() {
        let mut v = CrdtValue::new(CrdtType::GCounter);
        let op = CrdtOp::PNCounter(PNCounterOp::Increment { client_id: 1, amount: 1 });
        assert!(apply_op(&mut v, op).is_err());
    }

    #[test]
    fn msgpack_roundtrip() {
        let mut v = CrdtValue::new(CrdtType::GCounter);
        apply_op(&mut v, CrdtOp::GCounter(GCounterOp { client_id: 42, amount: 100 })).unwrap();
        let bytes = v.to_msgpack().unwrap();
        let v2 = CrdtValue::from_msgpack(&bytes).unwrap();
        assert_eq!(v.to_json_value(), v2.to_json_value());
    }
}
