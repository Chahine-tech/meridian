use pgrx::prelude::*;

use meridian_core::crdt::{
    lwwregister::LwwOp,
    registry::{CrdtType, CrdtValue},
    Crdt, HybridLogicalClock,
};
use serde_json::Value as JsonValue;

use super::{decode_crdt, decode_or_default, encode_crdt};

/// Set the value of a LwwRegister. The write with the highest HLC wins.
///
/// `value` is a JSON string. `wall_ms` should be `EXTRACT(EPOCH FROM now()) * 1000`.
///
/// ```sql
/// UPDATE articles
/// SET title = meridian.lww_set(title, '"Hello"', EXTRACT(EPOCH FROM now()) * 1000, 42);
/// ```
#[pg_extern(schema = "meridian")]
pub fn lww_set(state: Option<&[u8]>, value: &str, wall_ms: i64, author: i64) -> Vec<u8> {
    let mut crdt = decode_or_default(state, CrdtType::LwwRegister);
    if let CrdtValue::LwwRegister(ref mut r) = crdt {
        let json: JsonValue =
            serde_json::from_str(value).unwrap_or(JsonValue::String(value.to_string()));

        let wall = wall_ms as u64;

        // Build the HLC for this write.  We only advance the logical counter
        // when the new wall time equals (or is below) the existing one, so that
        // a stale wall_ms results in a lower HLC and is correctly ignored by apply().
        let hlc = match &r.entry {
            Some(existing) if wall >= existing.hlc.wall_ms => {
                let logical = if wall == existing.hlc.wall_ms {
                    existing.hlc.logical + 1
                } else {
                    0
                };
                HybridLogicalClock { wall_ms: wall, logical, node_id: author as u64 }
            }
            Some(_) => {
                // wall_ms is older than current — will be rejected by apply()
                HybridLogicalClock { wall_ms: wall, logical: 0, node_id: author as u64 }
            }
            None => HybridLogicalClock {
                wall_ms: wall,
                logical: 0,
                node_id: author as u64,
            },
        };

        let hlc = HybridLogicalClock { node_id: author as u64, ..hlc };

        if let Err(e) = r.apply(LwwOp { value: json, hlc, author: author as u64 }) {
            pgrx::warning!("meridian.lww_set: apply failed: {e}");
        }
    }
    encode_crdt(&crdt)
}

/// Return the current value of a LwwRegister as a JSON string, or NULL.
///
/// ```sql
/// SELECT meridian.lww_value(title) FROM articles;  -- → '"Hello"'
/// ```
#[pg_extern(schema = "meridian")]
pub fn lww_value(state: &[u8]) -> Option<String> {
    decode_crdt(state).and_then(|c| {
        if let CrdtValue::LwwRegister(r) = c {
            r.value().value.map(|v| v.to_string())
        } else {
            None
        }
    })
}

/// Return the last-write timestamp (ms since epoch) of a LwwRegister, or NULL.
#[pg_extern(schema = "meridian")]
pub fn lww_updated_at_ms(state: &[u8]) -> Option<i64> {
    decode_crdt(state).and_then(|c| {
        if let CrdtValue::LwwRegister(r) = c {
            r.value().updated_at_ms.map(|ms| ms as i64)
        } else {
            None
        }
    })
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_lww_set_get() {
        let state = crate::types::lwwreg::lww_set(None, r#""hello""#, 1_000_000, 1);
        let val = crate::types::lwwreg::lww_value(&state);
        assert_eq!(val, Some(r#""hello""#.to_string()));
    }

    #[pg_test]
    fn test_lww_last_write_wins() {
        let state = crate::types::lwwreg::lww_set(None, r#""first""#, 1_000, 1);
        let state = crate::types::lwwreg::lww_set(Some(&state), r#""second""#, 2_000, 2);
        assert_eq!(
            crate::types::lwwreg::lww_value(&state),
            Some(r#""second""#.to_string())
        );
    }

    #[pg_test]
    fn test_lww_stale_write_ignored() {
        let state = crate::types::lwwreg::lww_set(None, r#""new""#, 2_000, 1);
        let state = crate::types::lwwreg::lww_set(Some(&state), r#""old""#, 1_000, 2);
        assert_eq!(
            crate::types::lwwreg::lww_value(&state),
            Some(r#""new""#.to_string())
        );
    }

    #[pg_test]
    fn test_lww_same_wall_ms_monotonic() {
        let state = crate::types::lwwreg::lww_set(None, r#""first""#, 1_000, 1);
        let state = crate::types::lwwreg::lww_set(Some(&state), r#""second""#, 1_000, 2);
        assert_eq!(
            crate::types::lwwreg::lww_value(&state),
            Some(r#""second""#.to_string())
        );
    }

    #[pg_test]
    fn test_lww_updated_at_ms() {
        let state = crate::types::lwwreg::lww_set(None, r#""v""#, 42_000, 1);
        assert_eq!(crate::types::lwwreg::lww_updated_at_ms(&state), Some(42_000));
    }
}
