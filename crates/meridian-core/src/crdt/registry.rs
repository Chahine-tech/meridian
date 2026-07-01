use serde::{Deserialize, Serialize};
use tracing::{debug, instrument, warn};

use crate::protocol::ConflictKind;

use super::{
    Crdt, CrdtError, HybridLogicalClock, VectorClock,
    crdtmap::{CRDTMap, CRDTMapOp},
    gcounter::{GCounter, GCounterOp},
    lwwregister::{LwwOp, LwwRegister},
    orset::{ORSet, ORSetOp},
    pncounter::{PNCounter, PNCounterOp},
    presence::{Presence, PresenceOp},
    rga::{Rga, RgaOp},
    tree::{TreeCrdt, TreeOp},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum CrdtType {
    GCounter = 0x01,
    PNCounter = 0x02,
    ORSet = 0x03,
    LwwRegister = 0x04,
    Presence = 0x05,
    CRDTMap = 0x06,
    Rga = 0x07,
    Tree = 0x08,
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
            Self::GCounter => "gcounter",
            Self::PNCounter => "pncounter",
            Self::ORSet => "orset",
            Self::LwwRegister => "lwwregister",
            Self::Presence => "presence",
            Self::CRDTMap => "crdtmap",
            Self::Rga => "rga",
            Self::Tree => "tree",
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
            "gcounter" => Ok(Self::GCounter),
            "pncounter" => Ok(Self::PNCounter),
            "orset" => Ok(Self::ORSet),
            "lwwregister" => Ok(Self::LwwRegister),
            "presence" => Ok(Self::Presence),
            "crdtmap" => Ok(Self::CRDTMap),
            "rga" => Ok(Self::Rga),
            "tree" => Ok(Self::Tree),
            other => Err(format!("unknown crdt type: {other}")),
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

/// Stats returned by `CrdtValue::compact()`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CompactStats {
    /// Number of tombstoned RGA nodes or deleted Tree nodes removed.
    pub tombstones_removed: usize,
    /// Number of Tree move_log records removed.
    pub move_records_removed: usize,
}

impl CrdtValue {
    pub fn crdt_type(&self) -> CrdtType {
        match self {
            Self::GCounter(_) => CrdtType::GCounter,
            Self::PNCounter(_) => CrdtType::PNCounter,
            Self::ORSet(_) => CrdtType::ORSet,
            Self::LwwRegister(_) => CrdtType::LwwRegister,
            Self::Presence(_) => CrdtType::Presence,
            Self::CRDTMap(_) => CrdtType::CRDTMap,
            Self::RGA(_) => CrdtType::Rga,
            Self::Tree(_) => CrdtType::Tree,
        }
    }

    pub fn new(crdt_type: CrdtType) -> Self {
        match crdt_type {
            CrdtType::GCounter => Self::GCounter(GCounter::default()),
            CrdtType::PNCounter => Self::PNCounter(PNCounter::default()),
            CrdtType::ORSet => Self::ORSet(ORSet::default()),
            CrdtType::LwwRegister => Self::LwwRegister(LwwRegister::default()),
            CrdtType::Presence => Self::Presence(Presence::default()),
            CrdtType::CRDTMap => Self::CRDTMap(CRDTMap::default()),
            CrdtType::Rga => Self::RGA(Rga::default()),
            CrdtType::Tree => Self::Tree(TreeCrdt::default()),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::GCounter(v) => v.is_empty(),
            Self::PNCounter(v) => v.is_empty(),
            Self::ORSet(v) => v.is_empty(),
            Self::LwwRegister(v) => v.is_empty(),
            Self::Presence(v) => v.is_empty(),
            Self::CRDTMap(v) => v.is_empty(),
            Self::RGA(v) => v.is_empty(),
            Self::Tree(v) => v.is_empty(),
        }
    }

    /// Serialize to JSON value for HTTP responses.
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            Self::GCounter(v) => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::PNCounter(v) => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::ORSet(v) => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::LwwRegister(v) => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::Presence(v) => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::CRDTMap(v) => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::RGA(v) => serde_json::to_value(v.value()).unwrap_or_default(),
            Self::Tree(v) => serde_json::to_value(v.value()).unwrap_or_default(),
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

    /// Compact internal state to reclaim memory from tombstones and stale log entries.
    ///
    /// Only `RGA` and `Tree` types perform meaningful work. All other CRDT types
    /// return zero stats (they have no unbounded accumulated state).
    ///
    /// `threshold_wall_ms` is the wall-clock cutoff: tombstones deleted before
    /// this time are eligible for removal. Pass `now_ms() - grace_ms` where
    /// `grace_ms` is your maximum expected client offline duration (e.g. 24 h).
    /// Tombstones with an unknown deletion time (`deleted_at_ms == 0`) are
    /// always preserved regardless of `threshold_wall_ms`.
    pub fn compact_before(&mut self, threshold_wall_ms: u64) -> CompactStats {
        match self {
            Self::RGA(v) => {
                let removed = v.compact_before(threshold_wall_ms);
                CompactStats {
                    tombstones_removed: removed,
                    move_records_removed: 0,
                }
            }
            Self::Tree(v) => {
                let (nodes, records) = v.compact_before(threshold_wall_ms);
                CompactStats {
                    tombstones_removed: nodes,
                    move_records_removed: records,
                }
            }
            _ => CompactStats::default(),
        }
    }

    /// DottedDB-inspired precise GC: remove tombstones that every registered client
    /// has observed AND that are old enough for offline clients to have reconnected.
    ///
    /// `floor_vc` is the component-wise minimum VectorClock over all currently
    /// connected clients (from [`ClientRegistry::floor_vc`]). A tombstone whose
    /// creator's version is dominated by `floor_vc` is eligible once
    /// `grace_ms` ms have elapsed since the deletion.
    ///
    /// Returns the same `CompactStats` as `compact_before`.
    pub fn compact_safe(&mut self, floor_vc: &VectorClock, grace_ms: u64) -> CompactStats {
        match self {
            Self::RGA(v) => CompactStats {
                tombstones_removed: v.compact_safe(floor_vc, grace_ms),
                move_records_removed: 0,
            },
            Self::Tree(v) => {
                let (nodes, records) = v.compact_safe(floor_vc, grace_ms);
                CompactStats {
                    tombstones_removed: nodes,
                    move_records_removed: records,
                }
            }
            _ => CompactStats::default(),
        }
    }

    /// Compute delta since `vc` and serialize to msgpack.
    pub fn delta_since_msgpack(&self, vc: &VectorClock) -> Result<Option<Vec<u8>>, CrdtError> {
        let delta = match self {
            Self::GCounter(v) => v
                .delta_since(vc)
                .map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::PNCounter(v) => v
                .delta_since(vc)
                .map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::ORSet(v) => v
                .delta_since(vc)
                .map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::LwwRegister(v) => v
                .delta_since(vc)
                .map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::Presence(v) => v
                .delta_since(vc)
                .map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::CRDTMap(v) => v
                .delta_since(vc)
                .map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::RGA(v) => v
                .delta_since(vc)
                .map(|d| rmp_serde::encode::to_vec_named(&d)),
            Self::Tree(v) => v
                .delta_since(vc)
                .map(|d| rmp_serde::encode::to_vec_named(&d)),
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
            Self::GCounter(_) => CrdtType::GCounter,
            Self::PNCounter(_) => CrdtType::PNCounter,
            Self::ORSet(_) => CrdtType::ORSet,
            Self::LwwRegister(_) => CrdtType::LwwRegister,
            Self::Presence(_) => CrdtType::Presence,
            Self::CRDTMap(_) => CrdtType::CRDTMap,
            Self::RGA(_) => CrdtType::Rga,
            Self::Tree(_) => CrdtType::Tree,
        }
    }

    /// Returns the op-level permission mask for this op.
    ///
    /// Used by the granular permissions system (V2 tokens) to check whether
    /// a token is allowed to perform this specific operation on a CRDT key.
    pub fn op_mask(&self) -> crate::auth::claims::OpMask {
        use crate::auth::claims::op_masks;
        use crate::crdt::{orset::ORSetOp, pncounter::PNCounterOp, rga::RgaOp, tree::TreeOp};
        match self {
            Self::GCounter(_) => op_masks::GC_INCREMENT,
            Self::PNCounter(op) => match op {
                PNCounterOp::Increment { .. } => op_masks::PN_INCREMENT,
                PNCounterOp::Decrement { .. } => op_masks::PN_DECREMENT,
            },
            Self::ORSet(op) => match op {
                ORSetOp::Add { .. } => op_masks::OR_ADD,
                ORSetOp::Remove { .. } => op_masks::OR_REMOVE,
            },
            Self::LwwRegister(_) => op_masks::LWW_SET,
            Self::Presence(_) => op_masks::PRESENCE_UPDATE,
            Self::CRDTMap(_) => op_masks::MAP_WRITE,
            Self::RGA(op) => match op {
                RgaOp::Insert { .. } => op_masks::RGA_INSERT,
                RgaOp::Delete { .. } => op_masks::RGA_DELETE,
            },
            Self::Tree(op) => match op {
                TreeOp::AddNode { .. } => op_masks::TREE_ADD,
                TreeOp::MoveNode { .. } => op_masks::TREE_MOVE,
                TreeOp::UpdateNode { .. } => op_masks::TREE_UPDATE,
                TreeOp::DeleteNode { .. } => op_masks::TREE_DELETE,
            },
        }
    }
}

/// Maximum allowed clock drift between a client HLC and the server wall clock.
/// Ops with `|client_wall_ms - server_now_ms| > CLOCK_DRIFT_TOLERANCE_MS` are rejected
/// to prevent rogue clients from injecting far-future timestamps and winning all LWW merges.
pub const CLOCK_DRIFT_TOLERANCE_MS: u64 = 30_000; // 30 seconds

/// Validate that the client-supplied HLC wall time is within the allowed drift window.
///
/// Returns `Err(CrdtError::InvalidOp)` if the op should be rejected.
/// Only ops that carry a client HLC (`LwwRegister`, `Presence`) are checked;
/// all other op types are accepted unconditionally.
#[instrument(skip(op), fields(op_type = op.crdt_type().as_str()))]
pub fn validate_clock_drift(op: &CrdtOp, server_now_ms: u64) -> Result<(), CrdtError> {
    let client_wall_ms: Option<u64> = match op {
        CrdtOp::LwwRegister(o) => Some(o.hlc.wall_ms),
        CrdtOp::Presence(PresenceOp::Heartbeat { hlc, .. }) => Some(hlc.wall_ms),
        CrdtOp::Presence(PresenceOp::Leave { hlc, .. }) => Some(hlc.wall_ms),
        _ => None,
    };

    if let Some(client_ms) = client_wall_ms {
        let drift = client_ms.abs_diff(server_now_ms);
        if drift > CLOCK_DRIFT_TOLERANCE_MS {
            warn!(
                client_wall_ms = client_ms,
                server_now_ms,
                drift_ms = drift,
                max_ms = CLOCK_DRIFT_TOLERANCE_MS,
                "clock drift too large — op rejected"
            );
            return Err(CrdtError::InvalidOp(format!(
                "clock drift too large: client_wall_ms={client_ms} server_now_ms={server_now_ms} drift={drift}ms (max {}ms)",
                CLOCK_DRIFT_TOLERANCE_MS
            )));
        }
        debug!(
            client_wall_ms = client_ms,
            server_now_ms,
            drift_ms = drift,
            "clock drift ok"
        );
    }

    Ok(())
}

/// Current wire format version for `VersionedOp`.
/// Increment this constant when making breaking changes to the op encoding.
pub const OP_VERSION: u8 = 1;

/// Wire-format envelope for a `CrdtOp` that carries a schema version number.
///
/// Allows future receivers to detect when they receive an op encoded with a
/// newer format they don't understand, rather than silently misinterpreting it.
///
/// Backwards-compatible: `version` defaults to `0` when absent (old clients),
/// so legacy msgpack payloads without this field still deserialize correctly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedOp {
    /// Schema version of the enclosed `op`. `0` = legacy (no version field).
    #[serde(default)]
    pub version: u8,
    /// The actual CRDT operation.
    pub op: CrdtOp,
}

impl VersionedOp {
    /// Wrap a `CrdtOp` with the current protocol version.
    pub fn new(op: CrdtOp) -> Self {
        Self {
            version: OP_VERSION,
            op,
        }
    }

    /// Decode from msgpack bytes, accepting both versioned and legacy payloads.
    ///
    /// Returns `Err` if the version is newer than what this build understands,
    /// preventing silent data corruption from future format changes.
    pub fn from_msgpack(bytes: &[u8]) -> Result<Self, CrdtError> {
        let v: Self = rmp_serde::decode::from_slice(bytes).map_err(CrdtError::Deserialization)?;
        if v.version > OP_VERSION {
            return Err(CrdtError::InvalidOp(format!(
                "op version {} is newer than supported version {}; upgrade the server",
                v.version, OP_VERSION
            )));
        }
        Ok(v)
    }

    /// Encode to msgpack bytes with the current protocol version.
    pub fn to_msgpack(&self) -> Result<Vec<u8>, CrdtError> {
        rmp_serde::encode::to_vec_named(self).map_err(CrdtError::Serialization)
    }
}

/// Apply a CrdtOp to a CrdtValue.
///
/// Returns `(delta_bytes, conflict)`:
/// - `delta_bytes` — serialised delta to broadcast, or `None` if the op was a no-op.
/// - `conflict`    — present when the op was superseded or structurally rejected
///   (LWW write lost, Tree move created cycle). Callers should forward this
///   privately to the originating connection via [`ServerMsg::Conflict`].
#[instrument(skip(value, op), fields(crdt_type = value.crdt_type().as_str(), op_type = op.crdt_type().as_str()))]
pub fn apply_op(
    value: &mut CrdtValue,
    op: CrdtOp,
) -> Result<(Option<Vec<u8>>, Option<ConflictKind>), CrdtError> {
    macro_rules! apply_and_serialize {
        ($crdt:expr, $op:expr) => {{
            let delta = $crdt.apply($op)?;
            match delta {
                None => Ok((None, None)),
                Some(d) => Ok((Some(rmp_serde::encode::to_vec_named(&d)?), None)),
            }
        }};
    }

    let hlc_str = |h: HybridLogicalClock| {
        format!("{}:{}:{}", h.wall_ms, u32::from(h.logical), h.node_id)
    };

    let result = match (value, op) {
        (CrdtValue::GCounter(v), CrdtOp::GCounter(op)) => apply_and_serialize!(v, op),
        (CrdtValue::PNCounter(v), CrdtOp::PNCounter(op)) => apply_and_serialize!(v, op),
        (CrdtValue::ORSet(v), CrdtOp::ORSet(op)) => apply_and_serialize!(v, op),
        (CrdtValue::Presence(v), CrdtOp::Presence(op)) => apply_and_serialize!(v, op),
        (CrdtValue::CRDTMap(v), CrdtOp::CRDTMap(op)) => apply_and_serialize!(v, op),
        (CrdtValue::RGA(v), CrdtOp::RGA(op)) => apply_and_serialize!(v, op),

        (CrdtValue::LwwRegister(v), CrdtOp::LwwRegister(op)) => {
            let op_author = op.author;
            let delta = v.apply(op)?;
            if let Some(d) = delta {
                Ok((Some(rmp_serde::encode::to_vec_named(&d)?), None))
            } else {
                // Op was a stale write — conflict if a different client's entry won.
                let conflict = v.entry.as_ref().and_then(|e| {
                    if e.author != op_author {
                        Some(ConflictKind::LwwOverwritten {
                            winning_client_id: e.author,
                            winning_ts_ms: e.hlc.wall_ms,
                        })
                    } else {
                        None
                    }
                });
                Ok((None, conflict))
            }
        }

        (CrdtValue::Tree(v), CrdtOp::Tree(op)) => {
            let delta = v.apply(op)?;
            if let Some(d) = delta {
                let conflict = d.discarded_moves.first().map(|disc| ConflictKind::TreeMoveCycle {
                    node_id: hlc_str(disc.node_id),
                    attempted_parent_id: disc.attempted_parent_id.map(hlc_str),
                });
                Ok((Some(rmp_serde::encode::to_vec_named(&d)?), conflict))
            } else {
                Ok((None, None))
            }
        }

        _ => Err(CrdtError::InvalidOp("op type does not match crdt type".into())),
    };
    match &result {
        Ok((Some(_), None)) => debug!("op applied, delta produced"),
        Ok((Some(_), Some(_))) => debug!("op applied, delta produced with conflict"),
        Ok((None, Some(_))) => debug!("op applied, no-op with conflict"),
        Ok((None, None)) => debug!("op applied, no-op (already seen)"),
        Err(e) => warn!(error = %e, "op rejected"),
    }
    result
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
        let op = CrdtOp::GCounter(GCounterOp {
            client_id: 1,
            amount: 5,
        });
        let (delta_bytes, conflict) = apply_op(&mut v, op).unwrap();
        assert!(delta_bytes.is_some());
        assert!(!delta_bytes.unwrap().is_empty());
        assert!(conflict.is_none());
    }

    #[test]
    fn apply_op_type_mismatch_is_error() {
        let mut v = CrdtValue::new(CrdtType::GCounter);
        let op = CrdtOp::PNCounter(PNCounterOp::Increment {
            client_id: 1,
            amount: 1,
        });
        assert!(apply_op(&mut v, op).is_err());
    }

    #[test]
    fn msgpack_roundtrip() {
        let mut v = CrdtValue::new(CrdtType::GCounter);
        let _ = apply_op(
            &mut v,
            CrdtOp::GCounter(GCounterOp {
                client_id: 42,
                amount: 100,
            }),
        )
        .unwrap();
        let bytes = v.to_msgpack().unwrap();
        let v2 = CrdtValue::from_msgpack(&bytes).unwrap();
        assert_eq!(v.to_json_value(), v2.to_json_value());
    }

    // --- clock drift validation ---

    fn lww_op(wall_ms: u64) -> CrdtOp {
        use crate::crdt::{HybridLogicalClock, lwwregister::LwwOp};
        CrdtOp::LwwRegister(LwwOp {
            value: serde_json::Value::String("v".into()),
            hlc: HybridLogicalClock {
                wall_ms,
                logical: 0,
                node_id: 1,
            },
            author: 1,
        })
    }

    fn presence_heartbeat_op(wall_ms: u64) -> CrdtOp {
        use crate::crdt::{HybridLogicalClock, presence::PresenceOp};
        CrdtOp::Presence(PresenceOp::Heartbeat {
            client_id: 1,
            data: serde_json::Value::Null,
            hlc: HybridLogicalClock {
                wall_ms,
                logical: 0,
                node_id: 1,
            },
            ttl_ms: 5000,
        })
    }

    #[test]
    fn clock_drift_accepts_op_within_tolerance() {
        let server_now = 100_000u64;
        // Exactly at tolerance boundary — should pass.
        assert!(
            validate_clock_drift(&lww_op(server_now + CLOCK_DRIFT_TOLERANCE_MS), server_now)
                .is_ok()
        );
        assert!(
            validate_clock_drift(&lww_op(server_now - CLOCK_DRIFT_TOLERANCE_MS), server_now)
                .is_ok()
        );
    }

    #[test]
    fn clock_drift_rejects_future_timestamp() {
        let server_now = 100_000u64;
        let future = server_now + CLOCK_DRIFT_TOLERANCE_MS + 1;
        assert!(validate_clock_drift(&lww_op(future), server_now).is_err());
    }

    #[test]
    fn clock_drift_rejects_past_timestamp() {
        let server_now = 100_000u64;
        let past = server_now - CLOCK_DRIFT_TOLERANCE_MS - 1;
        assert!(validate_clock_drift(&lww_op(past), server_now).is_err());
    }

    #[test]
    fn clock_drift_validates_presence_heartbeat() {
        let server_now = 100_000u64;
        let future = server_now + CLOCK_DRIFT_TOLERANCE_MS + 1;
        assert!(validate_clock_drift(&presence_heartbeat_op(future), server_now).is_err());
        assert!(validate_clock_drift(&presence_heartbeat_op(server_now), server_now).is_ok());
    }

    #[test]
    fn clock_drift_skips_non_hlc_ops() {
        // GCounter ops carry no HLC — must always pass regardless of server_now.
        let op = CrdtOp::GCounter(GCounterOp {
            client_id: 1,
            amount: 1,
        });
        assert!(validate_clock_drift(&op, 0).is_ok());
        assert!(validate_clock_drift(&op, u64::MAX).is_ok());
    }

    // --- VersionedOp ---

    #[test]
    fn versioned_op_roundtrip() {
        let op = CrdtOp::GCounter(GCounterOp {
            client_id: 7,
            amount: 3,
        });
        let versioned = VersionedOp::new(op);
        assert_eq!(versioned.version, OP_VERSION);
        let bytes = versioned.to_msgpack().unwrap();
        let decoded = VersionedOp::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.version, OP_VERSION);
        assert!(matches!(
            decoded.op,
            CrdtOp::GCounter(GCounterOp {
                client_id: 7,
                amount: 3
            })
        ));
    }

    #[test]
    fn versioned_op_rejects_future_version() {
        // Simulate receiving a payload from a newer server.
        let future = VersionedOp {
            version: OP_VERSION + 1,
            op: CrdtOp::GCounter(GCounterOp {
                client_id: 1,
                amount: 1,
            }),
        };
        let bytes = rmp_serde::encode::to_vec_named(&future).unwrap();
        assert!(VersionedOp::from_msgpack(&bytes).is_err());
    }

    #[test]
    fn versioned_op_accepts_legacy_version_zero() {
        // A payload with version=0 (old client, no version field) must deserialize.
        let legacy = VersionedOp {
            version: 0,
            op: CrdtOp::GCounter(GCounterOp {
                client_id: 1,
                amount: 1,
            }),
        };
        let bytes = rmp_serde::encode::to_vec_named(&legacy).unwrap();
        let decoded = VersionedOp::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.version, 0);
    }
}
