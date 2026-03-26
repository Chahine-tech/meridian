/// Property-based tests for CRDT merge laws.
///
/// Every CRDT must satisfy three algebraic properties to guarantee eventual
/// consistency across replicas:
///
///   - **Commutativity**:  merge(a, b) == merge(b, a)
///   - **Associativity**:  merge(merge(a, b), c) == merge(a, merge(b, c))
///   - **Idempotency**:    merge(a, a) == a
///
/// These tests use `proptest` to verify the properties hold for arbitrary
/// sequences of ops, not just the hand-crafted cases in unit tests.
use proptest::prelude::*;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use meridian_core::crdt::{
    gcounter::{GCounter, GCounterOp},
    lwwregister::{LwwOp, LwwRegister},
    orset::{ORSet, ORSetOp},
    pncounter::{PNCounter, PNCounterOp},
    rga::{Rga, RgaOp},
    Crdt, HybridLogicalClock,
};

// ---------------------------------------------------------------------------
// Arbitrary generators
// ---------------------------------------------------------------------------

fn arb_json_scalar() -> impl Strategy<Value = JsonValue> {
    prop_oneof![
        any::<i64>().prop_map(|n| JsonValue::Number(n.into())),
        "[a-z]{1,8}".prop_map(JsonValue::String),
        any::<bool>().prop_map(JsonValue::Bool),
    ]
}

// ---------------------------------------------------------------------------
// GCounter
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn gcounter_merge_commutative(
        ops_a in prop::collection::vec((1u64..4, 1u64..100), 0..8),
        ops_b in prop::collection::vec((1u64..4, 1u64..100), 0..8),
    ) {
        let mut a = GCounter::default();
        for (client_id, amount) in &ops_a {
            a.apply(GCounterOp { client_id: *client_id, amount: *amount }).unwrap();
        }
        let mut b = GCounter::default();
        for (client_id, amount) in &ops_b {
            b.apply(GCounterOp { client_id: *client_id, amount: *amount }).unwrap();
        }

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);

        prop_assert_eq!(ab.value().total, ba.value().total);
    }

    #[test]
    fn gcounter_merge_idempotent(
        ops in prop::collection::vec((1u64..4, 1u64..100), 0..8),
    ) {
        let mut a = GCounter::default();
        for (client_id, amount) in &ops {
            a.apply(GCounterOp { client_id: *client_id, amount: *amount }).unwrap();
        }
        let copy = a.clone();
        a.merge(&copy);
        prop_assert_eq!(a.value().total, copy.value().total);
    }

    #[test]
    fn gcounter_merge_associative(
        ops_a in prop::collection::vec((1u64..4, 1u64..50), 0..5),
        ops_b in prop::collection::vec((1u64..4, 1u64..50), 0..5),
        ops_c in prop::collection::vec((1u64..4, 1u64..50), 0..5),
    ) {
        let mut a = GCounter::default();
        for (cid, amt) in &ops_a { a.apply(GCounterOp { client_id: *cid, amount: *amt }).unwrap(); }
        let mut b = GCounter::default();
        for (cid, amt) in &ops_b { b.apply(GCounterOp { client_id: *cid, amount: *amt }).unwrap(); }
        let mut c = GCounter::default();
        for (cid, amt) in &ops_c { c.apply(GCounterOp { client_id: *cid, amount: *amt }).unwrap(); }

        let mut ab_c = a.clone(); ab_c.merge(&b); ab_c.merge(&c);
        let mut bc = b.clone(); bc.merge(&c);
        let mut a_bc = a.clone(); a_bc.merge(&bc);

        prop_assert_eq!(ab_c.value().total, a_bc.value().total);
    }

    #[test]
    fn gcounter_monotone(
        ops in prop::collection::vec((1u64..4, 1u64..100), 1..10),
    ) {
        let mut g = GCounter::default();
        let mut prev = g.value().total;
        for (cid, amt) in ops {
            g.apply(GCounterOp { client_id: cid, amount: amt }).unwrap();
            let next = g.value().total;
            prop_assert!(next >= prev, "GCounter must be monotone: {next} < {prev}");
            prev = next;
        }
    }
}

// ---------------------------------------------------------------------------
// PNCounter
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum PNOp { Inc(u64, u64), Dec(u64, u64) }

fn arb_pn_op() -> impl Strategy<Value = PNOp> {
    prop_oneof![
        (1u64..4, 1u64..50).prop_map(|(cid, amt)| PNOp::Inc(cid, amt)),
        (1u64..4, 1u64..50).prop_map(|(cid, amt)| PNOp::Dec(cid, amt)),
    ]
}

fn apply_pn_ops(ops: &[PNOp]) -> PNCounter {
    let mut c = PNCounter::default();
    for op in ops {
        match op {
            PNOp::Inc(cid, amt) => { c.apply(PNCounterOp::Increment { client_id: *cid, amount: *amt }).unwrap(); }
            PNOp::Dec(cid, amt) => { c.apply(PNCounterOp::Decrement { client_id: *cid, amount: *amt }).unwrap(); }
        }
    }
    c
}

proptest! {
    #[test]
    fn pncounter_merge_commutative(
        ops_a in prop::collection::vec(arb_pn_op(), 0..8),
        ops_b in prop::collection::vec(arb_pn_op(), 0..8),
    ) {
        let a = apply_pn_ops(&ops_a);
        let b = apply_pn_ops(&ops_b);

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);

        prop_assert_eq!(ab.value().value, ba.value().value);
    }

    #[test]
    fn pncounter_merge_idempotent(
        ops in prop::collection::vec(arb_pn_op(), 0..8),
    ) {
        let a = apply_pn_ops(&ops);
        let mut b = a.clone();
        b.merge(&a);
        prop_assert_eq!(a.value().value, b.value().value);
    }
}

// ---------------------------------------------------------------------------
// LwwRegister
// ---------------------------------------------------------------------------

fn apply_lww_ops(ops: &[(u64, u64, u16, u64, String)]) -> LwwRegister {
    let mut r = LwwRegister::default();
    for (wall_ms, node_id, logical, author, val) in ops {
        r.apply(LwwOp {
            value: JsonValue::String(val.clone()),
            hlc: HybridLogicalClock { wall_ms: *wall_ms, logical: *logical, node_id: *node_id },
            author: *author,
        }).unwrap();
    }
    r
}

proptest! {
    #[test]
    fn lww_merge_commutative(
        ops_a in prop::collection::vec((1u64..1000, 1u64..4, 0u16..4, 1u64..4, "[a-z]{1,4}"), 0..6),
        ops_b in prop::collection::vec((1u64..1000, 1u64..4, 0u16..4, 1u64..4, "[a-z]{1,4}"), 0..6),
    ) {
        let a = apply_lww_ops(&ops_a);
        let b = apply_lww_ops(&ops_b);

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);

        prop_assert_eq!(ab.value().value, ba.value().value,
            "LWW merge must be commutative");
    }

    #[test]
    fn lww_merge_idempotent(
        ops in prop::collection::vec((1u64..1000, 1u64..4, 0u16..4, 1u64..4, "[a-z]{1,4}"), 0..6),
    ) {
        let a = apply_lww_ops(&ops);
        let mut b = a.clone();
        b.merge(&a);
        prop_assert_eq!(a.value().value, b.value().value);
    }

    #[test]
    fn lww_merge_associative(
        ops_a in prop::collection::vec((1u64..1000, 1u64..4, 0u16..4, 1u64..4, "[a-z]{1,4}"), 0..4),
        ops_b in prop::collection::vec((1u64..1000, 1u64..4, 0u16..4, 1u64..4, "[a-z]{1,4}"), 0..4),
        ops_c in prop::collection::vec((1u64..1000, 1u64..4, 0u16..4, 1u64..4, "[a-z]{1,4}"), 0..4),
    ) {
        let a = apply_lww_ops(&ops_a);
        let b = apply_lww_ops(&ops_b);
        let c = apply_lww_ops(&ops_c);

        let mut ab_c = a.clone(); ab_c.merge(&b); ab_c.merge(&c);
        let mut bc = b.clone(); bc.merge(&c);
        let mut a_bc = a.clone(); a_bc.merge(&bc);

        prop_assert_eq!(ab_c.value().value, a_bc.value().value);
    }
}

// ---------------------------------------------------------------------------
// ORSet
// ---------------------------------------------------------------------------

fn arb_orset_element() -> impl Strategy<Value = JsonValue> {
    // Restricted to scalars — depth > 1 is rejected by apply()
    arb_json_scalar()
}

proptest! {
    #[test]
    fn orset_merge_commutative(
        adds_a in prop::collection::vec(arb_orset_element(), 0..6),
        adds_b in prop::collection::vec(arb_orset_element(), 0..6),
    ) {
        let mut a = ORSet::default();
        for el in &adds_a {
            let _ = a.apply(ORSetOp::Add { element: el.clone(), tag: Uuid::new_v4(), node_id: 1, seq: 1 });
        }
        let mut b = ORSet::default();
        for el in &adds_b {
            let _ = b.apply(ORSetOp::Add { element: el.clone(), tag: Uuid::new_v4(), node_id: 1, seq: 1 });
        }

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);

        // Compare sorted element lists (HashMap iteration order is unspecified).
        let mut ab_els: Vec<String> = ab.value().elements.iter().map(|v| v.to_string()).collect();
        let mut ba_els: Vec<String> = ba.value().elements.iter().map(|v| v.to_string()).collect();
        ab_els.sort(); ba_els.sort();
        prop_assert_eq!(ab_els, ba_els, "ORSet merge must be commutative");
    }

    #[test]
    fn orset_merge_idempotent(
        adds in prop::collection::vec(arb_orset_element(), 0..6),
    ) {
        let mut a = ORSet::default();
        for el in &adds {
            let _ = a.apply(ORSetOp::Add { element: el.clone(), tag: Uuid::new_v4(), node_id: 1, seq: 1 });
        }
        let copy = a.clone();
        a.merge(&copy);

        let mut els: Vec<String> = a.value().elements.iter().map(|v| v.to_string()).collect();
        let mut copy_els: Vec<String> = copy.value().elements.iter().map(|v| v.to_string()).collect();
        els.sort(); copy_els.sort();
        prop_assert_eq!(els, copy_els);
    }

    #[test]
    fn orset_add_wins_under_concurrent_remove(
        element in arb_json_scalar(),
    ) {
        // Classic add-wins scenario: concurrent add and remove should leave element present.
        let mut base = ORSet::default();
        let initial_tag = Uuid::new_v4();
        base.apply(ORSetOp::Add { element: element.clone(), tag: initial_tag, node_id: 1, seq: 1 }).unwrap();

        let mut node_a = base.clone(); // will remove
        let mut node_b = base.clone(); // will add concurrently

        // A removes (knows about initial_tag only)
        let key = element.to_string();
        let known = base.entries.get(&key).cloned().unwrap_or_default();
        node_a.apply(ORSetOp::Remove { element: element.clone(), known_tags: known }).unwrap();

        // B adds with a fresh tag (concurrent — A doesn't know about it)
        node_b.apply(ORSetOp::Add { element: element.clone(), tag: Uuid::new_v4(), node_id: 2, seq: 1 }).unwrap();

        // Merge: B's new tag survives A's remove
        node_a.merge(&node_b);
        let present = !node_a.entries.get(&key).map(|t| t.is_empty()).unwrap_or(true);
        prop_assert!(present, "add-wins: element must be present after concurrent add+remove");
    }

    #[test]
    fn orset_depth_validation_rejects_nested(
        inner in arb_json_scalar(),
    ) {
        let mut s = ORSet::default();
        let nested = JsonValue::Array(vec![JsonValue::Array(vec![inner])]);
        prop_assert!(s.apply(ORSetOp::Add { element: nested, tag: Uuid::new_v4(), node_id: 1, seq: 1 }).is_err(),
            "nested arrays must be rejected by apply()");
    }
}

// ---------------------------------------------------------------------------
// RGA — text collaborative editing
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn rga_insert_order_stable(
        chars in prop::collection::vec(prop::char::range('a', 'z'), 1..15),
    ) {
        // Insert chars sequentially on a single node — order must match insertion order.
        let mut rga = Rga::default();
        for (i, ch) in chars.iter().enumerate() {
            let prev_id = if i == 0 { None } else {
                rga.nodes.get(i - 1).map(|n| n.id)
            };
            rga.apply(RgaOp::Insert {
                id: HybridLogicalClock { wall_ms: i as u64 + 1, logical: 0, node_id: 1 },
                origin_id: prev_id,
                content: *ch,
            }).unwrap();
        }
        let result = rga.value().text;
        let expected: String = chars.iter().collect();
        prop_assert_eq!(result, expected, "sequential inserts must preserve order");
    }

    #[test]
    fn rga_merge_idempotent(
        chars in prop::collection::vec(prop::char::range('a', 'z'), 0..10),
    ) {
        let mut rga = Rga::default();
        for (i, ch) in chars.iter().enumerate() {
            rga.apply(RgaOp::Insert {
                id: HybridLogicalClock { wall_ms: i as u64 + 1, logical: 0, node_id: 1 },
                origin_id: None,
                content: *ch,
            }).unwrap();
        }
        let copy = rga.clone();
        rga.merge(&copy);
        prop_assert_eq!(rga.value().text, copy.value().text,
            "RGA merge must be idempotent");
    }

    #[test]
    fn rga_delete_removes_char(
        chars in prop::collection::vec(prop::char::range('a', 'z'), 1..10),
        del_idx in any::<prop::sample::Index>(),
    ) {
        let mut rga = Rga::default();
        for (i, ch) in chars.iter().enumerate() {
            rga.apply(RgaOp::Insert {
                id: HybridLogicalClock { wall_ms: i as u64 + 1, logical: 0, node_id: 1 },
                origin_id: None,
                content: *ch,
            }).unwrap();
        }

        let live_ids: Vec<_> = rga.nodes.iter().filter(|n| !n.deleted).map(|n| n.id).collect();
        let live_count = live_ids.len();
        let target_id = *del_idx.get(&live_ids);
        rga.apply(RgaOp::Delete { id: target_id }).unwrap();

        let remaining: usize = rga.nodes.iter().filter(|n| !n.deleted).count();
        prop_assert_eq!(remaining, live_count - 1, "delete must remove exactly one char");
    }
}
