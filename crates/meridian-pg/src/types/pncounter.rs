use pgrx::prelude::*;

use meridian_core::crdt::{pncounter::PNCounterOp, registry::{CrdtType, CrdtValue}, Crdt};

use super::{decode_crdt, decode_or_default, encode_crdt};

/// Increment (positive direction) a PNCounter.
///
/// ```sql
/// UPDATE articles SET likes = meridian.pncounter_increment(likes, 1, 42);
/// ```
#[pg_extern(schema = "meridian")]
pub fn pncounter_increment(state: Option<&[u8]>, amount: i64, client_id: i64) -> Vec<u8> {
    let mut crdt = decode_or_default(state, CrdtType::PNCounter);
    if let CrdtValue::PNCounter(ref mut p) = crdt {
        if let Err(e) = p.apply(PNCounterOp::Increment {
            client_id: client_id as u64,
            amount: amount.unsigned_abs(),
        }) {
            pgrx::warning!("meridian.pncounter_increment: apply failed: {e}");
        }
    }
    encode_crdt(&crdt)
}

/// Decrement (negative direction) a PNCounter.
///
/// ```sql
/// UPDATE articles SET likes = meridian.pncounter_decrement(likes, 1, 42);
/// ```
#[pg_extern(schema = "meridian")]
pub fn pncounter_decrement(state: Option<&[u8]>, amount: i64, client_id: i64) -> Vec<u8> {
    let mut crdt = decode_or_default(state, CrdtType::PNCounter);
    if let CrdtValue::PNCounter(ref mut p) = crdt {
        if let Err(e) = p.apply(PNCounterOp::Decrement {
            client_id: client_id as u64,
            amount: amount.unsigned_abs(),
        }) {
            pgrx::warning!("meridian.pncounter_decrement: apply failed: {e}");
        }
    }
    encode_crdt(&crdt)
}

/// Return the signed value (pos - neg) of a PNCounter as BIGINT.
#[pg_extern(schema = "meridian")]
pub fn pncounter_value(state: &[u8]) -> i64 {
    decode_crdt(state)
        .and_then(|c| {
            if let CrdtValue::PNCounter(p) = c {
                Some(p.value().value)
            } else {
                None
            }
        })
        .unwrap_or(0)
}

/// Merge two PNCounter states (lattice join). Used by the aggregate.
#[pg_extern(schema = "meridian")]
pub fn pncounter_merge(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut av = match CrdtValue::from_msgpack(a) {
        Ok(v) => v,
        Err(_) => return a.to_vec(),
    };
    let bv = match CrdtValue::from_msgpack(b) {
        Ok(v) => v,
        Err(_) => return a.to_vec(),
    };
    if let (CrdtValue::PNCounter(ap), CrdtValue::PNCounter(bp)) = (&mut av, &bv) {
        ap.merge(bp);
    }
    encode_crdt(&av)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_pncounter_roundtrip() {
        let state = crate::types::pncounter::pncounter_increment(None, 10, 1);
        let state = crate::types::pncounter::pncounter_decrement(Some(&state), 3, 1);
        assert_eq!(crate::types::pncounter::pncounter_value(&state), 7);
    }

    #[pg_test]
    fn test_pncounter_negative() {
        let state = crate::types::pncounter::pncounter_increment(None, 2, 1);
        let state = crate::types::pncounter::pncounter_decrement(Some(&state), 5, 1);
        assert_eq!(crate::types::pncounter::pncounter_value(&state), -3);
    }

    #[pg_test]
    fn test_pncounter_merge_commutative() {
        let a = crate::types::pncounter::pncounter_increment(None, 10, 1);
        let b = crate::types::pncounter::pncounter_decrement(None, 3, 2);
        let ab = crate::types::pncounter::pncounter_merge(&a, &b);
        let ba = crate::types::pncounter::pncounter_merge(&b, &a);
        assert_eq!(
            crate::types::pncounter::pncounter_value(&ab),
            crate::types::pncounter::pncounter_value(&ba)
        );
        assert_eq!(crate::types::pncounter::pncounter_value(&ab), 7);
    }

    #[pg_test]
    fn test_pncounter_merge_idempotent() {
        let a = crate::types::pncounter::pncounter_increment(None, 5, 1);
        let aa = crate::types::pncounter::pncounter_merge(&a, &a);
        assert_eq!(
            crate::types::pncounter::pncounter_value(&a),
            crate::types::pncounter::pncounter_value(&aa)
        );
    }
}
