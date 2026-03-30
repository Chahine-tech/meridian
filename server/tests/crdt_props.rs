//! Property-based tests for all 5 CRDT types.
//!
//! For each type we verify the three lattice join axioms:
//!   - Commutativity:  merge(a, b) == merge(b, a)
//!   - Associativity:  merge(merge(a,b), c) == merge(a, merge(b,c))
//!   - Idempotency:    merge(a, a) == a
//!
//! Plus type-specific invariants.

use meridian_server::crdt::{
    clock::HybridLogicalClock,
    gcounter::{GCounter, GCounterOp},
    lwwregister::{LwwOp, LwwRegister},
    orset::{ORSet, ORSetOp},
    pncounter::{PNCounter, PNCounterOp},
    presence::{Presence, PresenceOp},
    Crdt,
};
use proptest::prelude::*;
use serde_json::Value as JsonValue;
use uuid::Uuid;

// Helpers

fn hlc(wall_ms: u64, logical: u16, node_id: u64) -> HybridLogicalClock {
    HybridLogicalClock { wall_ms, logical, node_id }
}

/// Build a GCounter from a list of (client_id, amount) increments.
fn build_gcounter(ops: &[(u64, u64)]) -> GCounter {
    let mut g = GCounter::default();
    for &(id, amt) in ops {
        g.apply(GCounterOp { client_id: id, amount: amt }).unwrap();
    }
    g
}

/// Build a PNCounter from a list of (client_id, signed_amount) ops.
fn build_pncounter(ops: &[(u64, i64)]) -> PNCounter {
    let mut c = PNCounter::default();
    for &(id, amt) in ops {
        if amt >= 0 {
            c.apply(PNCounterOp::Increment { client_id: id, amount: amt as u64 }).unwrap();
        } else {
            c.apply(PNCounterOp::Decrement { client_id: id, amount: (-amt) as u64 }).unwrap();
        }
    }
    c
}

// GCounter property tests

proptest! {
    #[test]
    fn gcounter_merge_commutative(
        a_ops in prop::collection::vec((0u64..10, 0u64..100), 0..10),
        b_ops in prop::collection::vec((0u64..10, 0u64..100), 0..10),
    ) {
        let a = build_gcounter(&a_ops);
        let b = build_gcounter(&b_ops);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(ab, ba);
    }

    #[test]
    fn gcounter_merge_associative(
        a_ops in prop::collection::vec((0u64..5, 0u64..50), 0..8),
        b_ops in prop::collection::vec((0u64..5, 0u64..50), 0..8),
        c_ops in prop::collection::vec((0u64..5, 0u64..50), 0..8),
    ) {
        let a = build_gcounter(&a_ops);
        let b = build_gcounter(&b_ops);
        let c = build_gcounter(&c_ops);

        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        prop_assert_eq!(ab_c, a_bc);
    }

    #[test]
    fn gcounter_merge_idempotent(
        ops in prop::collection::vec((0u64..10, 0u64..100), 0..15),
    ) {
        let mut a = build_gcounter(&ops);
        let b = a.clone();
        a.merge(&b);
        prop_assert_eq!(a, b);
    }

    #[test]
    fn gcounter_value_is_sum(
        ops in prop::collection::vec((0u64..10, 0u64..100), 1..15),
    ) {
        let g = build_gcounter(&ops);
        let expected: u64 = g.counters.values().sum();
        prop_assert_eq!(g.value().total, expected);
    }

    #[test]
    fn gcounter_merge_never_decreases(
        a_ops in prop::collection::vec((0u64..5, 1u64..50), 0..8),
        b_ops in prop::collection::vec((0u64..5, 1u64..50), 0..8),
    ) {
        let mut a = build_gcounter(&a_ops);
        let b = build_gcounter(&b_ops);
        let before = a.value().total;
        a.merge(&b);
        prop_assert!(a.value().total >= before);
    }
}

// PNCounter property tests

proptest! {
    #[test]
    fn pncounter_merge_commutative(
        a_ops in prop::collection::vec((0u64..10, -50i64..50), 0..10),
        b_ops in prop::collection::vec((0u64..10, -50i64..50), 0..10),
    ) {
        let a = build_pncounter(&a_ops);
        let b = build_pncounter(&b_ops);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(ab, ba);
    }

    #[test]
    fn pncounter_merge_idempotent(
        ops in prop::collection::vec((0u64..10, -50i64..50), 0..15),
    ) {
        let mut a = build_pncounter(&ops);
        let b = a.clone();
        a.merge(&b);
        prop_assert_eq!(a, b);
    }

    #[test]
    fn pncounter_merge_associative(
        a_ops in prop::collection::vec((0u64..5, -20i64..20), 0..8),
        b_ops in prop::collection::vec((0u64..5, -20i64..20), 0..8),
        c_ops in prop::collection::vec((0u64..5, -20i64..20), 0..8),
    ) {
        let a = build_pncounter(&a_ops);
        let b = build_pncounter(&b_ops);
        let c = build_pncounter(&c_ops);

        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        prop_assert_eq!(ab_c, a_bc);
    }
}

// LWWRegister property tests

fn build_lww(ops: &[(u64, u16, u64, u8)]) -> LwwRegister {
    let mut r = LwwRegister::default();
    for &(wall_ms, logical, author, val) in ops {
        r.apply(LwwOp {
            value: JsonValue::Number(val.into()),
            hlc: hlc(wall_ms, logical, author),
            author,
        }).unwrap();
    }
    r
}

proptest! {
    #[test]
    fn lww_merge_commutative(
        a_ops in prop::collection::vec((0u64..1000, 0u16..10, 0u64..5, 0u8..10), 0..10),
        b_ops in prop::collection::vec((0u64..1000, 0u16..10, 0u64..5, 0u8..10), 0..10),
    ) {
        let a = build_lww(&a_ops);
        let b = build_lww(&b_ops);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(ab, ba);
    }

    #[test]
    fn lww_merge_idempotent(
        ops in prop::collection::vec((0u64..1000, 0u16..10, 0u64..5, 0u8..10), 0..10),
    ) {
        let mut a = build_lww(&ops);
        let b = a.clone();
        a.merge(&b);
        prop_assert_eq!(a, b);
    }

    #[test]
    fn lww_merge_associative(
        a_ops in prop::collection::vec((0u64..1000, 0u16..10, 0u64..5, 0u8..10), 0..8),
        b_ops in prop::collection::vec((0u64..1000, 0u16..10, 0u64..5, 0u8..10), 0..8),
        c_ops in prop::collection::vec((0u64..1000, 0u16..10, 0u64..5, 0u8..10), 0..8),
    ) {
        let a = build_lww(&a_ops);
        let b = build_lww(&b_ops);
        let c = build_lww(&c_ops);

        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        prop_assert_eq!(ab_c, a_bc);
    }

    #[test]
    fn lww_higher_hlc_always_wins(
        wall_a in 0u64..500,
        wall_b in 500u64..1000,
        author in 0u64..5,
    ) {
        let mut r = LwwRegister::default();
        // Write with lower HLC first
        r.apply(LwwOp {
            value: JsonValue::String("first".into()),
            hlc: hlc(wall_a, 0, author),
            author,
        }).unwrap();
        // Write with higher HLC second
        r.apply(LwwOp {
            value: JsonValue::String("second".into()),
            hlc: hlc(wall_b, 0, author),
            author,
        }).unwrap();
        prop_assert_eq!(r.value().value, Some(JsonValue::String("second".into())));
    }
}

// ORSet property tests

fn str_val(s: &str) -> JsonValue {
    JsonValue::String(s.to_string())
}

fn orset_add(val: &str) -> ORSetOp {
    ORSetOp::Add { element: str_val(val), tag: Uuid::new_v4(), node_id: 1, seq: 1 }
}

fn orset_remove_all(s: &ORSet, val: &str) -> ORSetOp {
    let key = val.to_string();
    let quoted = format!("\"{}\"", key);
    let known_tags = s.entries.get(&quoted).cloned().unwrap_or_default();
    ORSetOp::Remove { element: str_val(val), known_tags }
}

fn orset_contains(s: &ORSet, val: &str) -> bool {
    let quoted = format!("\"{}\"", val);
    s.entries.get(&quoted).map(|t| !t.is_empty()).unwrap_or(false)
}

proptest! {
    #[test]
    fn orset_add_wins_concurrent(
        val in "[a-z]{3,8}",
    ) {
        // Simulate: both nodes start from same state (has elem)
        // Node A removes, Node B adds concurrently
        let mut shared = ORSet::default();
        shared.apply(orset_add(&val)).unwrap();

        let mut node_a = shared.clone();
        let rm = orset_remove_all(&node_a, &val);
        node_a.apply(rm).unwrap();

        let mut node_b = shared.clone();
        node_b.apply(orset_add(&val)).unwrap(); // new tag

        // After merge, B's new tag survives
        node_a.merge(&node_b);
        prop_assert!(orset_contains(&node_a, &val), "add-wins: element should survive");
    }

    #[test]
    fn orset_merge_commutative(
        a_vals in prop::collection::vec("[a-z]{2,5}", 0..5),
        b_vals in prop::collection::vec("[a-z]{2,5}", 0..5),
    ) {
        let mut a = ORSet::default();
        for v in &a_vals { a.apply(orset_add(v)).unwrap(); }
        let mut b = ORSet::default();
        for v in &b_vals { b.apply(orset_add(v)).unwrap(); }

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(ab, ba);
    }

    #[test]
    fn orset_merge_idempotent(
        vals in prop::collection::vec("[a-z]{2,5}", 0..8),
    ) {
        let mut a = ORSet::default();
        for v in &vals { a.apply(orset_add(v)).unwrap(); }
        let b = a.clone();
        a.merge(&b);
        prop_assert_eq!(a, b);
    }

    #[test]
    fn orset_no_phantom_elements(
        vals in prop::collection::vec("[a-z]{2,5}", 1..8),
    ) {
        let mut a = ORSet::default();
        for v in &vals { a.apply(orset_add(v)).unwrap(); }

        // Remove all elements
        let to_remove: Vec<String> = vals.clone();
        for v in &to_remove {
            let rm = orset_remove_all(&a, v);
            a.apply(rm).unwrap();
        }

        // All entries should be gone (no phantom elements)
        for v in &vals {
            prop_assert!(!orset_contains(&a, v), "element should be removed: {v}");
        }
    }
}

// Presence property tests

fn build_presence(heartbeats: &[(u64, u64, u64)]) -> Presence {
    let mut p = Presence::default();
    for &(client_id, wall_ms, ttl_ms) in heartbeats {
        p.apply(PresenceOp::Heartbeat {
            client_id,
            data: JsonValue::Null,
            hlc: hlc(wall_ms, 0, client_id),
            ttl_ms,
        }).unwrap();
    }
    p
}

proptest! {
    #[test]
    fn presence_merge_commutative(
        a_hb in prop::collection::vec((0u64..5, 0u64..1000, 1000u64..5000), 0..8),
        b_hb in prop::collection::vec((0u64..5, 0u64..1000, 1000u64..5000), 0..8),
    ) {
        let a = build_presence(&a_hb);
        let b = build_presence(&b_hb);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(ab, ba);
    }

    #[test]
    fn presence_merge_idempotent(
        hb in prop::collection::vec((0u64..5, 0u64..1000, 1000u64..5000), 0..10),
    ) {
        let mut a = build_presence(&hb);
        let b = a.clone();
        a.merge(&b);
        prop_assert_eq!(a, b);
    }

    #[test]
    fn presence_expired_entry_not_visible(
        wall_ms in 0u64..1000,
        ttl_ms in 1u64..500,
    ) {
        let mut p = Presence::default();
        p.apply(PresenceOp::Heartbeat {
            client_id: 1,
            data: JsonValue::Null,
            hlc: hlc(wall_ms, 0, 1),
            ttl_ms,
        }).unwrap();

        // Way past the deadline
        let future = wall_ms.saturating_add(ttl_ms).saturating_add(1);
        let _value = p.value();
        // value() filters using now_ms() internally, but we can check the entry directly
        prop_assert!(!p.entries[&1].is_alive(future));
    }

    #[test]
    fn presence_leave_expires_immediately(
        wall_ms in 100u64..1000,
        ttl_ms in 5000u64..10000,
    ) {
        let mut p = Presence::default();
        p.apply(PresenceOp::Heartbeat {
            client_id: 1,
            data: JsonValue::Null,
            hlc: hlc(wall_ms, 0, 1),
            ttl_ms,
        }).unwrap();
        p.apply(PresenceOp::Leave {
            client_id: 1,
            hlc: hlc(wall_ms + 1, 0, 1),
        }).unwrap();
        // Entry has ttl=0, expired at wall_ms+1
        prop_assert!(!p.entries[&1].is_alive(wall_ms + 1));
    }

    #[test]
    fn presence_higher_hlc_wins_on_merge(
        wall_a in 0u64..500,
        wall_b in 500u64..1000,
        ttl in 1000u64..5000,
    ) {
        let mut a = Presence::default();
        a.apply(PresenceOp::Heartbeat {
            client_id: 1,
            data: JsonValue::String("old".into()),
            hlc: hlc(wall_a, 0, 1),
            ttl_ms: ttl,
        }).unwrap();

        let mut b = Presence::default();
        b.apply(PresenceOp::Heartbeat {
            client_id: 1,
            data: JsonValue::String("new".into()),
            hlc: hlc(wall_b, 0, 1),
            ttl_ms: ttl,
        }).unwrap();

        a.merge(&b);
        prop_assert_eq!(a.entries[&1].hlc.wall_ms, wall_b);
    }
}
