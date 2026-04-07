use pgrx::prelude::*;

use meridian_core::crdt::{gcounter::GCounterOp, registry::{CrdtType, CrdtValue}, Crdt};

use super::{decode_crdt, decode_or_default, encode_crdt};

/// Increment a GCounter by `amount` for `client_id`. Returns the updated BYTEA state.
///
/// ```sql
/// UPDATE articles SET views = meridian.gcounter_increment(views, 1, 42)
/// WHERE id = 'article-1';
/// ```
#[pg_extern(schema = "meridian")]
pub fn gcounter_increment(state: Option<&[u8]>, amount: i64, client_id: i64) -> Vec<u8> {
    let mut crdt = decode_or_default(state, CrdtType::GCounter);
    if let CrdtValue::GCounter(ref mut g) = crdt {
        if let Err(e) = g.apply(GCounterOp {
            client_id: client_id as u64,
            amount: amount.unsigned_abs(),
        }) {
            pgrx::warning!("meridian.gcounter_increment: apply failed: {e}");
        }
    }
    encode_crdt(&crdt)
}

/// Return the total value of a GCounter as BIGINT.
#[pg_extern(schema = "meridian")]
pub fn gcounter_value(state: &[u8]) -> i64 {
    decode_crdt(state)
        .and_then(|c| {
            if let CrdtValue::GCounter(g) = c {
                Some(g.value().total as i64)
            } else {
                None
            }
        })
        .unwrap_or(0)
}

/// Merge two GCounter states (lattice join). Used by the aggregate.
#[pg_extern(schema = "meridian")]
pub fn gcounter_merge(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut av = match CrdtValue::from_msgpack(a) {
        Ok(v) => v,
        Err(_) => return a.to_vec(),
    };
    let bv = match CrdtValue::from_msgpack(b) {
        Ok(v) => v,
        Err(_) => return a.to_vec(),
    };
    if let (CrdtValue::GCounter(ag), CrdtValue::GCounter(bg)) = (&mut av, &bv) {
        ag.merge(bg);
    }
    encode_crdt(&av)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_gcounter_roundtrip() {
        let state = crate::types::gcounter::gcounter_increment(None, 5, 1);
        let state = crate::types::gcounter::gcounter_increment(Some(&state), 3, 2);
        assert_eq!(crate::types::gcounter::gcounter_value(&state), 8);
    }

    #[pg_test]
    fn test_gcounter_merge_commutative() {
        let a = crate::types::gcounter::gcounter_increment(None, 5, 1);
        let b = crate::types::gcounter::gcounter_increment(None, 3, 2);
        let ab = crate::types::gcounter::gcounter_merge(&a, &b);
        let ba = crate::types::gcounter::gcounter_merge(&b, &a);
        assert_eq!(
            crate::types::gcounter::gcounter_value(&ab),
            crate::types::gcounter::gcounter_value(&ba)
        );
        assert_eq!(crate::types::gcounter::gcounter_value(&ab), 8);
    }

    #[pg_test]
    fn test_gcounter_merge_idempotent() {
        let a = crate::types::gcounter::gcounter_increment(None, 7, 1);
        let aa = crate::types::gcounter::gcounter_merge(&a, &a);
        assert_eq!(
            crate::types::gcounter::gcounter_value(&a),
            crate::types::gcounter::gcounter_value(&aa)
        );
    }

    #[pg_test]
    fn test_gcounter_merge_associative() {
        let a = crate::types::gcounter::gcounter_increment(None, 1, 1);
        let b = crate::types::gcounter::gcounter_increment(None, 2, 2);
        let c = crate::types::gcounter::gcounter_increment(None, 3, 3);
        let left = crate::types::gcounter::gcounter_merge(
            &crate::types::gcounter::gcounter_merge(&a, &b),
            &c,
        );
        let right = crate::types::gcounter::gcounter_merge(
            &a,
            &crate::types::gcounter::gcounter_merge(&b, &c),
        );
        assert_eq!(
            crate::types::gcounter::gcounter_value(&left),
            crate::types::gcounter::gcounter_value(&right)
        );
        assert_eq!(crate::types::gcounter::gcounter_value(&left), 6);
    }

    #[pg_test]
    fn test_gcounter_concurrent_same_client() {
        let a = crate::types::gcounter::gcounter_increment(None, 5, 1);
        let b = crate::types::gcounter::gcounter_increment(None, 3, 1);
        let merged = crate::types::gcounter::gcounter_merge(&a, &b);
        assert_eq!(crate::types::gcounter::gcounter_value(&merged), 5);
    }

    #[pg_test]
    fn test_gcounter_value_on_empty() {
        assert_eq!(crate::types::gcounter::gcounter_value(&[]), 0);
    }
}
