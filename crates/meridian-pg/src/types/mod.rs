pub mod gcounter;
pub mod lwwreg;
pub mod orset;
pub mod pncounter;
pub mod rga;
pub mod tree;

use meridian_core::crdt::registry::{CrdtType, CrdtValue};

pub(super) fn decode_crdt(bytes: &[u8]) -> Option<CrdtValue> {
    CrdtValue::from_msgpack(bytes).ok()
}

pub(super) fn decode_or_default(bytes: Option<&[u8]>, crdt_type: CrdtType) -> CrdtValue {
    bytes
        .and_then(|b| CrdtValue::from_msgpack(b).ok())
        .unwrap_or_else(|| CrdtValue::new(crdt_type))
}

pub(super) fn encode_crdt(crdt: &CrdtValue) -> Vec<u8> {
    match crdt.to_msgpack() {
        Ok(bytes) => bytes,
        Err(e) => {
            pgrx::warning!("meridian: failed to serialize CRDT state: {e}");
            Vec::new()
        }
    }
}
