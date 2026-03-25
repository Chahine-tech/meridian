use serde::{Deserialize, Serialize};

use super::{
    crdtmap::{CRDTMap, CRDTMapOp},
    gcounter::{GCounter, GCounterOp},
    lwwregister::{LwwOp, LwwRegister},
    orset::{ORSet, ORSetOp},
    pncounter::{PNCounter, PNCounterOp},
    presence::{Presence, PresenceOp},
    rga::{Rga, RgaOp},
    tree::{TreeCrdt, TreeOp},
    Crdt, CrdtError, VectorClock,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum CrdtType {
    GCounter    = 0x01,
    PNCounter   = 0x02,
    ORSet       = 0x03,
    LwwRegister = 0x04,
    Presence    = 0x05,
    CRDTMap     = 0x06,
    Rga         = 0x07,
    Tree        = 0x08,
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
            0x07 => Some(Self::Rga),
            0x08 => Some(Self::Tree),
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
            Self::Rga         => "rga",
            Self::Tree        => "tree",
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
            "rga"         => Ok(Self::Rga),
            "tree"        => Ok(Self::Tree),
            other         => Err(format!("unknown crdt type: {other}")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CrdtValue {
    GCounter(GCounter),
    PNCounter(PNCounter),
    ORSet(ORSet),
    LwwRegister(LwwRegister),
    Presence(Presence),
    CRDTMap(CRDTMap),
    RGA(Rga),
    Tree(TreeCrdt),
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
            Self::RGA(_)         => CrdtType::Rga,
            Self::Tree(_)        => CrdtType::Tree,
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
            CrdtType::Rga         => Self::RGA(Rga::default()),
            CrdtType::Tree        => Self::Tree(TreeCrdt::default()),
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
            Self::RGA(v)         => v.is_empty(),
            Self::Tree(v)        => v.is_empty(),
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
            Self::RGA(v)         => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::Tree(v)        => serde_json::to_value(v.value()).unwrap_or_default(),
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
            Self::RGA(v)         => v.delta_since(vc).map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::Tree(v)        => v.delta_since(vc).map(|d| rmp_serde::encode::to_vec_named(&d)),
        };
        match delta {
            None => Ok(None),
            Some(Ok(bytes)) => Ok(Some(bytes)),
            Some(Err(e)) => Err(CrdtError::Serialization(e)),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CrdtOp {
    GCounter(GCounterOp),
    PNCounter(PNCounterOp),
    ORSet(ORSetOp),
    LwwRegister(LwwOp),
    Presence(PresenceOp),
    CRDTMap(CRDTMapOp),
    RGA(RgaOp),
    Tree(TreeOp),
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
            Self::RGA(_)         => CrdtType::Rga,
            Self::Tree(_)        => CrdtType::Tree,
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
        (CrdtValue::RGA(v),         CrdtOp::RGA(op))         => apply_and_serialize!(v, op),
        (CrdtValue::Tree(v),        CrdtOp::Tree(op))        => apply_and_serialize!(v, op),
        _ => Err(CrdtError::InvalidOp("op type does not match crdt type".into())),
    }
}

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
