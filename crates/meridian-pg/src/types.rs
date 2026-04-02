// CRDT types exposed as Postgres types (stored as BYTEA / msgpack-encoded CrdtValue).
//
// Each Postgres type is a thin wrapper around `CrdtValue` serialized with rmp-serde.
// Functions follow the naming convention:
//   meridian.<crdt>_<operation>  e.g. meridian.gcounter_increment
//
// The in/out functions implement the mandatory Postgres type I/O protocol using a
// hex-encoded representation (compatible with `encode(col, 'hex')` in SQL).

use pgrx::prelude::*;

use meridian_core::crdt::{
    gcounter::GCounterOp,
    lwwregister::LwwOp,
    orset::ORSetOp,
    pncounter::PNCounterOp,
    registry::{CrdtType, CrdtValue},
    HybridLogicalClock,
};
use serde_json::Value as JsonValue;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn decode_crdt(bytes: &[u8]) -> Option<CrdtValue> {
    CrdtValue::from_msgpack(bytes).ok()
}

fn decode_or_default(bytes: Option<&[u8]>, crdt_type: CrdtType) -> CrdtValue {
    bytes
        .and_then(|b| CrdtValue::from_msgpack(b).ok())
        .unwrap_or_else(|| CrdtValue::new(crdt_type))
}

fn encode_crdt(crdt: &CrdtValue) -> Vec<u8> {
    match crdt.to_msgpack() {
        Ok(bytes) => bytes,
        Err(e) => {
            pgrx::warning!("meridian: failed to serialize CRDT state: {e}");
            Vec::new()
        }
    }
}

// ---------------------------------------------------------------------------
// GCounter
// ---------------------------------------------------------------------------

/// Increment a GCounter.  Returns the updated BYTEA state.
///
/// ```sql
/// UPDATE t SET views = meridian.gcounter_increment(views, 1, <client_id>);
/// ```
#[pg_extern(schema = "meridian")]
fn gcounter_increment(state: Option<&[u8]>, amount: i64, client_id: i64) -> Vec<u8> {
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

/// Return the total value of a GCounter.
#[pg_extern(schema = "meridian")]
fn gcounter_value(state: &[u8]) -> i64 {
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

/// Merge two GCounter states (lattice join).  Useful in aggregate queries.
#[pg_extern(schema = "meridian")]
fn gcounter_merge(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut av = match CrdtValue::from_msgpack(a) {
        Ok(v) => v,
        Err(_) => return a.to_vec(),
    };
    let bv = match CrdtValue::from_msgpack(b) {
        Ok(v) => v,
        Err(_) => return a.to_vec(),
    };
    if let (CrdtValue::GCounter(ref mut ag), CrdtValue::GCounter(ref bg)) = (&mut av, &bv) {
        ag.merge(bg);
    }
    encode_crdt(&av)
}

// ---------------------------------------------------------------------------
// PNCounter
// ---------------------------------------------------------------------------

/// Increment (positive direction) a PNCounter.
#[pg_extern(schema = "meridian")]
fn pncounter_increment(state: Option<&[u8]>, amount: i64, client_id: i64) -> Vec<u8> {
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
#[pg_extern(schema = "meridian")]
fn pncounter_decrement(state: Option<&[u8]>, amount: i64, client_id: i64) -> Vec<u8> {
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

/// Return the signed value of a PNCounter.
#[pg_extern(schema = "meridian")]
fn pncounter_value(state: &[u8]) -> i64 {
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

// ---------------------------------------------------------------------------
// ORSet
// ---------------------------------------------------------------------------

/// Add a JSON element to an ORSet.
///
/// `element` must be a valid JSON scalar or shallow array/object (depth ≤ 1).
#[pg_extern(schema = "meridian")]
fn orset_add(state: Option<&[u8]>, element: &str, node_id: i64, seq: i32) -> Vec<u8> {
    let mut crdt = decode_or_default(state, CrdtType::ORSet);
    if let CrdtValue::ORSet(ref mut s) = crdt {
        let json: JsonValue = serde_json::from_str(element).unwrap_or(JsonValue::String(element.to_string()));
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
#[pg_extern(schema = "meridian")]
fn orset_remove(state: Option<&[u8]>, element: &str) -> Vec<u8> {
    let mut crdt = decode_or_default(state, CrdtType::ORSet);
    if let CrdtValue::ORSet(ref mut s) = crdt {
        let json: JsonValue = serde_json::from_str(element).unwrap_or(JsonValue::String(element.to_string()));
        // Collect known tags for the element at remove-time (add-wins semantics).
        let element_key = json.to_string();
        let known_tags = s.entries.get(&element_key).cloned().unwrap_or_default();
        if let Err(e) = s.apply(ORSetOp::Remove { element: json, known_tags }) {
            pgrx::warning!("meridian.orset_remove: apply failed: {e}");
        }
    }
    encode_crdt(&crdt)
}

/// Return elements of an ORSet as a JSON array string.
#[pg_extern(schema = "meridian")]
fn orset_elements(state: &[u8]) -> String {
    let elements = decode_crdt(state)
        .and_then(|c| {
            if let CrdtValue::ORSet(s) = c {
                Some(s.value())
            } else {
                None
            }
        })
        .map(|v| v.elements)
        .unwrap_or_default();
    serde_json::to_string(&elements).unwrap_or_else(|_| "[]".to_string())
}

// ---------------------------------------------------------------------------
// LwwRegister
// ---------------------------------------------------------------------------

/// Set the value of a LwwRegister.
///
/// `value` is a JSON string. `wall_ms` should be `EXTRACT(EPOCH FROM now()) * 1000`.
/// The write with the highest HLC wins — tie-break on `author`.
///
/// The `logical` counter is derived from the existing entry's HLC so that two
/// writes at the same `wall_ms` from different `author`s are ordered correctly:
/// the new write gets `logical = existing.logical + 1` when the wall time matches,
/// ensuring strict monotonicity regardless of which client_id wins the tie.
#[pg_extern(schema = "meridian")]
fn lww_set(state: Option<&[u8]>, value: &str, wall_ms: i64, author: i64) -> Vec<u8> {
    let mut crdt = decode_or_default(state, CrdtType::LwwRegister);
    if let CrdtValue::LwwRegister(ref mut r) = crdt {
        let json: JsonValue = serde_json::from_str(value)
            .unwrap_or(JsonValue::String(value.to_string()));

        let wall = wall_ms as u64;

        // Build the HLC by ticking from the existing entry (if any).
        // This ensures logical monotonicity within the same millisecond.
        let hlc = match &r.entry {
            Some(existing) => {
                let mut base = existing.hlc;
                base.tick(wall)
            }
            None => HybridLogicalClock {
                wall_ms: wall,
                logical: 0,
                node_id: author as u64,
            },
        };

        // Override node_id with the caller's author so the total order includes it.
        let hlc = HybridLogicalClock { node_id: author as u64, ..hlc };

        if let Err(e) = r.apply(LwwOp { value: json, hlc, author: author as u64 }) {
            pgrx::warning!("meridian.lww_set: apply failed: {e}");
        }
    }
    encode_crdt(&crdt)
}

/// Return the current value of a LwwRegister as a JSON string, or NULL.
#[pg_extern(schema = "meridian")]
fn lww_value(state: &[u8]) -> Option<String> {
    decode_crdt(state).and_then(|c| {
        if let CrdtValue::LwwRegister(r) = c {
            r.value().value.map(|v| v.to_string())
        } else {
            None
        }
    })
}

/// Return the last-write timestamp (ms) of a LwwRegister, or NULL.
#[pg_extern(schema = "meridian")]
fn lww_updated_at_ms(state: &[u8]) -> Option<i64> {
    decode_crdt(state).and_then(|c| {
        if let CrdtValue::LwwRegister(r) = c {
            r.value().updated_at_ms.map(|ms| ms as i64)
        } else {
            None
        }
    })
}

// ---------------------------------------------------------------------------
// RGA (Replicated Growable Array) — collaborative text
// ---------------------------------------------------------------------------
//
// Writing to RGA requires HLC timestamps and conflict-resolution logic that
// is impractical to express in SQL. RGA columns should be written exclusively
// via WebSocket clients or the Meridian HTTP API.
//
// The read functions below let existing SQL tooling observe the current text.

/// Return the current text of an RGA column.
#[pg_extern(schema = "meridian")]
fn rga_text(state: &[u8]) -> String {
    decode_crdt(state)
        .and_then(|c| {
            if let CrdtValue::RGA(r) = c {
                Some(r.value().text)
            } else {
                None
            }
        })
        .unwrap_or_default()
}

/// Return the character count (visible chars only, no tombstones) of an RGA column.
#[pg_extern(schema = "meridian")]
fn rga_len(state: &[u8]) -> i64 {
    decode_crdt(state)
        .and_then(|c| {
            if let CrdtValue::RGA(r) = c {
                Some(r.value().len as i64)
            } else {
                None
            }
        })
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tree CRDT — hierarchical tree
// ---------------------------------------------------------------------------
//
// Same limitation as RGA: writes require HLC-stamped ops.  Use the read
// functions for SQL-side access (reporting, full-text search, etc.).

/// Return the tree as a JSON string (array of root nodes with nested children).
#[pg_extern(schema = "meridian")]
fn tree_json(state: &[u8]) -> String {
    decode_crdt(state)
        .and_then(|c| {
            if let CrdtValue::Tree(t) = c {
                serde_json::to_string(&t.value()).ok()
            } else {
                None
            }
        })
        .unwrap_or_else(|| r#"{"roots":[]}"#.to_string())
}

/// Return the number of live (non-deleted) nodes in a Tree column.
#[pg_extern(schema = "meridian")]
fn tree_node_count(state: &[u8]) -> i64 {
    decode_crdt(state)
        .and_then(|c| {
            if let CrdtValue::Tree(t) = c {
                Some(t.nodes.iter().filter(|n| !n.deleted).count() as i64)
            } else {
                None
            }
        })
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Generic helpers
// ---------------------------------------------------------------------------

/// Return a JSON representation of any CRDT column.
///
/// ```sql
/// SELECT meridian.crdt_json(views) FROM articles;
/// ```
#[pg_extern(schema = "meridian")]
fn crdt_json(state: &[u8]) -> String {
    decode_crdt(state)
        .map(|c| c.to_json_value().to_string())
        .unwrap_or_else(|| "null".to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg-test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_gcounter_roundtrip() {
        let state = crate::types::gcounter_increment(None, 5, 1);
        let state = crate::types::gcounter_increment(Some(&state), 3, 2);
        assert_eq!(crate::types::gcounter_value(&state), 8);
    }

    #[pg_test]
    fn test_pncounter_roundtrip() {
        let state = crate::types::pncounter_increment(None, 10, 1);
        let state = crate::types::pncounter_decrement(Some(&state), 3, 1);
        assert_eq!(crate::types::pncounter_value(&state), 7);
    }

    #[pg_test]
    fn test_orset_add_remove() {
        let state = crate::types::orset_add(None, r#""apple""#, 1, 1);
        let state = crate::types::orset_add(Some(&state), r#""banana""#, 1, 2);
        let state = crate::types::orset_remove(Some(&state), r#""apple""#);
        let elements = crate::types::orset_elements(&state);
        assert!(elements.contains("banana"));
        assert!(!elements.contains("apple"));
    }

    #[pg_test]
    fn test_lww_set_get() {
        let state = crate::types::lww_set(None, r#""hello""#, 1_000_000, 1);
        let val = crate::types::lww_value(&state);
        assert_eq!(val, Some(r#""hello""#.to_string()));
    }
}
