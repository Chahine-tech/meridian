use pgrx::prelude::*;
use uuid::Uuid;

use meridian_core::crdt::{orset::ORSetOp, registry::{CrdtType, CrdtValue}, Crdt};
use serde_json::Value as JsonValue;

use super::{decode_crdt, decode_or_default, encode_crdt};

/// Add a JSON element to an ORSet.
///
/// `element` must be a valid JSON scalar or shallow array/object (depth ≤ 1).
///
/// ```sql
/// UPDATE articles SET tags = meridian.orset_add(tags, '"rust"', 1, 1);
/// ```
#[pg_extern(schema = "meridian")]
pub fn orset_add(state: Option<&[u8]>, element: &str, node_id: i64, seq: i32) -> Vec<u8> {
    let mut crdt = decode_or_default(state, CrdtType::ORSet);
    if let CrdtValue::ORSet(ref mut s) = crdt {
        let json: JsonValue =
            serde_json::from_str(element).unwrap_or(JsonValue::String(element.to_string()));
        if let Err(e) = s.apply(ORSetOp::Add {
            element: json,
            tag: Uuid::new_v4(),
            node_id: node_id as u64,
            seq: seq as u32,
        }) {
            pgrx::warning!("meridian.orset_add: apply failed: {e}");
        }
    }
    encode_crdt(&crdt)
}

/// Remove a JSON element from an ORSet (add-wins: concurrent adds survive).
///
/// ```sql
/// UPDATE articles SET tags = meridian.orset_remove(tags, '"rust"');
/// ```
#[pg_extern(schema = "meridian")]
pub fn orset_remove(state: Option<&[u8]>, element: &str) -> Vec<u8> {
    let mut crdt = decode_or_default(state, CrdtType::ORSet);
    if let CrdtValue::ORSet(ref mut s) = crdt {
        let json: JsonValue =
            serde_json::from_str(element).unwrap_or(JsonValue::String(element.to_string()));
        let element_key = json.to_string();
        let known_tags = s.entries.get(&element_key).cloned().unwrap_or_default();
        if let Err(e) = s.apply(ORSetOp::Remove { element: json, known_tags }) {
            pgrx::warning!("meridian.orset_remove: apply failed: {e}");
        }
    }
    encode_crdt(&crdt)
}

/// Return elements of an ORSet as a JSON array string.
///
/// ```sql
/// SELECT meridian.orset_elements(tags) FROM articles;  -- → '["rust","go"]'
/// ```
#[pg_extern(schema = "meridian")]
pub fn orset_elements(state: &[u8]) -> String {
    let elements = decode_crdt(state)
        .and_then(|c| {
            if let CrdtValue::ORSet(s) = c {
                Some(s.value())
            } else {
                None
            }
        })
        .map(|v| {
            let mut elems = v.elements;
            elems.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
            elems
        })
        .unwrap_or_default();
    serde_json::to_string(&elements).unwrap_or_else(|_| "[]".to_string())
}

/// Merge two ORSet states (lattice join). Used by the aggregate.
#[pg_extern(schema = "meridian")]
pub fn orset_merge(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut av = match CrdtValue::from_msgpack(a) {
        Ok(v) => v,
        Err(_) => return a.to_vec(),
    };
    let bv = match CrdtValue::from_msgpack(b) {
        Ok(v) => v,
        Err(_) => return a.to_vec(),
    };
    if let (CrdtValue::ORSet(ao), CrdtValue::ORSet(bo)) = (&mut av, &bv) {
        ao.merge(bo);
    }
    encode_crdt(&av)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_orset_add_remove() {
        let state = crate::types::orset::orset_add(None, r#""apple""#, 1, 1);
        let state = crate::types::orset::orset_add(Some(&state), r#""banana""#, 1, 2);
        let state = crate::types::orset::orset_remove(Some(&state), r#""apple""#);
        let elements = crate::types::orset::orset_elements(&state);
        assert!(elements.contains("banana"));
        assert!(!elements.contains("apple"));
    }

    #[pg_test]
    fn test_orset_remove_nonexistent() {
        let state = crate::types::orset::orset_add(None, r#""apple""#, 1, 1);
        let state = crate::types::orset::orset_remove(Some(&state), r#""ghost""#);
        let elements = crate::types::orset::orset_elements(&state);
        assert!(elements.contains("apple"));
    }

    #[pg_test]
    fn test_orset_add_wins() {
        let a = crate::types::orset::orset_add(None, r#""x""#, 1, 1);
        let b = crate::types::orset::orset_add(None, r#""x""#, 2, 1);
        let b = crate::types::orset::orset_remove(Some(&b), r#""x""#);
        let merged = crate::types::orset::orset_merge(&a, &b);
        let elements = crate::types::orset::orset_elements(&merged);
        assert!(elements.contains("x"), "add-wins: concurrent add should survive remove");
    }

    #[pg_test]
    fn test_orset_merge_commutative() {
        let a = crate::types::orset::orset_add(None, r#""a""#, 1, 1);
        let b = crate::types::orset::orset_add(None, r#""b""#, 2, 1);
        let ab = crate::types::orset::orset_merge(&a, &b);
        let ba = crate::types::orset::orset_merge(&b, &a);
        assert_eq!(
            crate::types::orset::orset_elements(&ab),
            crate::types::orset::orset_elements(&ba)
        );
    }

    #[pg_test]
    fn test_orset_merge_idempotent() {
        let a = crate::types::orset::orset_add(None, r#""x""#, 1, 1);
        let aa = crate::types::orset::orset_merge(&a, &a);
        assert_eq!(
            crate::types::orset::orset_elements(&a),
            crate::types::orset::orset_elements(&aa)
        );
    }

    #[pg_test]
    fn test_orset_invalid_json_fallback() {
        let state = crate::types::orset::orset_add(None, "not json {{{", 1, 1);
        let elements = crate::types::orset::orset_elements(&state);
        assert!(elements.contains("not json {{{"));
    }
}
