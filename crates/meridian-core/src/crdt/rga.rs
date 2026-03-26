use serde::{Deserialize, Serialize};

use super::{Crdt, CrdtError, HybridLogicalClock, VectorClock};

//
// RGA (Replicated Growable Array) — collaborative text editing.
//
// Each character has a unique HybridLogicalClock ID. Insertions reference
// the predecessor character's ID (left-origin). Deletions use tombstones
// so the sequence structure is never disturbed.
//
// Concurrent inserts after the same origin are ordered by ID descending:
// the node with the higher HLC is placed leftmost. Since HLC is a total
// order, all replicas converge to the same sequence.
//
// Tombstones are never GC'd — this keeps the implementation simple and
// correct. For documents with very high deletion rates, a compaction pass
// could be added later without changing the protocol.

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RgaNode {
    pub id: HybridLogicalClock,
    /// ID of the left neighbour at insert time. None = insert at head.
    pub origin_id: Option<HybridLogicalClock>,
    pub content: char,
    pub deleted: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Rga {
    /// Document sequence in display order, tombstones included.
    pub nodes: Vec<RgaNode>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RgaOp {
    Insert {
        id: HybridLogicalClock,
        origin_id: Option<HybridLogicalClock>,
        content: char,
    },
    Delete {
        id: HybridLogicalClock,
    },
}

#[must_use]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RgaDelta {
    pub ops: Vec<RgaOp>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RgaValue {
    pub text: String,
    pub len: usize,
}

impl Rga {
    /// Remove tombstoned nodes that are safe to garbage-collect.
    ///
    /// A tombstone is safe to remove when no live node references it as its
    /// `origin_id` (i.e. no future insert can land "after" the deleted node).
    /// Removing tombstones that are still referenced as origins would break
    /// the insertion position algorithm for clients that haven't yet received
    /// concurrent inserts anchored to those origins.
    ///
    /// This should only be called after confirming that all connected clients
    /// have acknowledged the corresponding WAL entries (i.e. after WAL
    /// compaction, when `checkpoint_seq` covers the tombstone's seq).
    ///
    /// Returns the number of tombstones removed.
    pub fn compact(&mut self) -> usize {
        // A tombstone is safe to GC when no *live* node uses its id as an
        // `origin_id`. The `origin_id` field is only consulted when placing a
        // *new* insert — tombstoned nodes can never serve as the origin of a
        // future insert, so their own origin_id chains do not matter.
        //
        // Rule: keep a tombstone iff some live node has `origin_id == tombstone.id`.
        let live_origin_ids: std::collections::BTreeSet<HybridLogicalClock> = self
            .nodes
            .iter()
            .filter(|n| !n.deleted)
            .filter_map(|n| n.origin_id)
            .collect();

        let before = self.nodes.len();
        self.nodes.retain(|n| {
            // Keep if: live, OR a live node references it as its origin.
            !n.deleted || live_origin_ids.contains(&n.id)
        });
        before - self.nodes.len()
    }

    /// Find the insertion index for a node with the given origin_id and id.
    ///
    /// Algorithm:
    /// 1. Find the origin node (or position 0 if origin_id is None).
    /// 2. Scan rightward past any concurrent siblings that should come before
    ///    us: a sibling comes before us if it shares the same origin AND has
    ///    a strictly greater ID (higher HLC wins → placed leftmost).
    /// 3. Insert at the first position that is not such a sibling.
    fn insert_position(&self, origin_id: Option<HybridLogicalClock>, id: HybridLogicalClock) -> usize {
        // Start scanning after the origin node (or from the beginning).
        let start = match origin_id {
            None => 0,
            Some(oid) => self
                .nodes
                .iter()
                .position(|n| n.id == oid)
                .map(|p| p + 1)
                .unwrap_or(self.nodes.len()),
        };

        let mut pos = start;
        while pos < self.nodes.len() {
            let sibling = &self.nodes[pos];
            // A sibling belongs before us if it has the same origin and a
            // greater ID. We stop as soon as we find a node that belongs
            // after us (different origin or lower/equal ID).
            if sibling.origin_id == origin_id && sibling.id > id {
                pos += 1;
            } else {
                break;
            }
        }
        pos
    }
}

impl Crdt for Rga {
    type Op = RgaOp;
    type Delta = RgaDelta;
    type Value = RgaValue;

    fn apply(&mut self, op: RgaOp) -> Result<Option<RgaDelta>, CrdtError> {
        match op {
            RgaOp::Insert { id, origin_id, content } => {
                // Idempotency: skip if we already have this node.
                if self.nodes.iter().any(|n| n.id == id) {
                    return Ok(None);
                }
                let pos = self.insert_position(origin_id, id);
                self.nodes.insert(pos, RgaNode { id, origin_id, content, deleted: false });
                Ok(Some(RgaDelta { ops: vec![RgaOp::Insert { id, origin_id, content }] }))
            }

            RgaOp::Delete { id } => {
                match self.nodes.iter_mut().find(|n| n.id == id) {
                    None => Ok(None), // unknown node — no-op
                    Some(node) if node.deleted => Ok(None), // already deleted — idempotent
                    Some(node) => {
                        node.deleted = true;
                        Ok(Some(RgaDelta { ops: vec![RgaOp::Delete { id }] }))
                    }
                }
            }
        }
    }

    fn merge(&mut self, other: &Rga) {
        for node in &other.nodes {
            match self.nodes.iter_mut().find(|n| n.id == node.id) {
                Some(existing) => {
                    // Propagate tombstone: delete wins.
                    if node.deleted {
                        existing.deleted = true;
                    }
                }
                None => {
                    // New node — insert at the correct position.
                    let pos = self.insert_position(node.origin_id, node.id);
                    self.nodes.insert(pos, node.clone());
                }
            }
        }
    }

    fn merge_delta(&mut self, delta: RgaDelta) {
        for op in delta.ops {
            // Ignore errors — already-applied ops are no-ops by design.
            let _ = self.apply(op);
        }
    }

    fn delta_since(&self, since: &VectorClock) -> Option<RgaDelta> {
        // Include a node if the client has not yet seen it.
        // For inserts: the node's node_id (author) has a logical counter we
        // track via VectorClock. If vc[node_id] < node.id.logical, unseen.
        // Deletions are included whenever the node itself is unseen OR when
        // we know the node was deleted but the client may not have seen it yet
        // (conservative: always emit delete ops for tombstones the client may
        // have seen the insert for but not the delete).
        let ops: Vec<RgaOp> = self
            .nodes
            .iter()
            .flat_map(|node| {
                let seen_logical = since.get(node.id.node_id);
                let mut out = Vec::new();

                // Emit insert if client hasn't seen this node.
                if seen_logical < u32::from(node.id.logical) {
                    out.push(RgaOp::Insert {
                        id: node.id,
                        origin_id: node.origin_id,
                        content: node.content,
                    });
                }

                // Always emit delete for tombstoned nodes, regardless of seen_logical.
                //
                // The delete op may have been authored by a *different* client than
                // the insert (different node_id), so the insert's VectorClock entry
                // gives no information about whether the client has seen the delete.
                // Emitting conservatively is safe: Delete is idempotent on the receiver.
                if node.deleted {
                    out.push(RgaOp::Delete { id: node.id });
                }

                out
            })
            .collect();

        if ops.is_empty() { None } else { Some(RgaDelta { ops }) }
    }

    fn value(&self) -> RgaValue {
        let text: String = self
            .nodes
            .iter()
            .filter(|n| !n.deleted)
            .map(|n| n.content)
            .collect();
        let len = text.chars().count();
        RgaValue { text, len }
    }

    fn is_empty(&self) -> bool {
        self.nodes.iter().all(|n| n.deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(wall_ms: u64, logical: u16, node_id: u64) -> HybridLogicalClock {
        HybridLogicalClock { wall_ms, logical, node_id }
    }

    fn insert(rga: &mut Rga, id: HybridLogicalClock, origin_id: Option<HybridLogicalClock>, ch: char) {
        rga.apply(RgaOp::Insert { id, origin_id, content: ch }).unwrap();
    }

    fn delete(rga: &mut Rga, id: HybridLogicalClock) {
        rga.apply(RgaOp::Delete { id }).unwrap();
    }

    fn text(rga: &Rga) -> String {
        rga.value().text
    }

    #[test]
    fn insert_at_head() {
        let mut r = Rga::default();
        insert(&mut r, hlc(1, 1, 1), None, 'H');
        assert_eq!(text(&r), "H");
    }

    #[test]
    fn insert_sequential() {
        let mut r = Rga::default();
        let id_h = hlc(1, 1, 1);
        let id_i = hlc(1, 2, 1);
        let id_bang = hlc(1, 3, 1);
        insert(&mut r, id_h, None, 'H');
        insert(&mut r, id_i, Some(id_h), 'i');
        insert(&mut r, id_bang, Some(id_i), '!');
        assert_eq!(text(&r), "Hi!");
    }

    #[test]
    fn delete_char() {
        let mut r = Rga::default();
        let id_h = hlc(1, 1, 1);
        let id_i = hlc(1, 2, 1);
        insert(&mut r, id_h, None, 'H');
        insert(&mut r, id_i, Some(id_h), 'i');
        delete(&mut r, id_h);
        assert_eq!(text(&r), "i");
    }

    #[test]
    fn concurrent_insert_same_position_converges() {
        // Two replicas start empty.
        // Node 1 inserts 'A' at head (higher HLC → comes first).
        // Node 2 inserts 'B' at head (lower HLC → comes second).
        // Both replicas must converge to "AB".

        let id_a = hlc(100, 1, 1); // node 1 — higher HLC
        let id_b = hlc(100, 0, 2); // node 2 — lower HLC

        let mut r1 = Rga::default();
        insert(&mut r1, id_a, None, 'A');

        let mut r2 = Rga::default();
        insert(&mut r2, id_b, None, 'B');

        // Merge in both directions.
        let mut r1_merged = r1.clone();
        r1_merged.merge(&r2);

        let mut r2_merged = r2.clone();
        r2_merged.merge(&r1);

        assert_eq!(text(&r1_merged), text(&r2_merged), "replicas must converge");
        assert_eq!(text(&r1_merged), "AB", "higher HLC comes first");
    }

    #[test]
    fn merge_idempotent() {
        let mut r = Rga::default();
        let id = hlc(1, 1, 1);
        insert(&mut r, id, None, 'X');
        let copy = r.clone();
        r.merge(&copy);
        assert_eq!(text(&r), "X");
        assert_eq!(r.nodes.len(), 1);
    }

    #[test]
    fn merge_tombstone_propagates() {
        let id = hlc(1, 1, 1);
        let mut r1 = Rga::default();
        insert(&mut r1, id, None, 'X');

        let mut r2 = r1.clone();
        delete(&mut r2, id);
        assert!(r2.is_empty());

        r1.merge(&r2);
        assert!(r1.is_empty(), "tombstone must propagate on merge");
    }

    #[test]
    fn apply_insert_idempotent() {
        let mut r = Rga::default();
        let id = hlc(1, 1, 1);
        insert(&mut r, id, None, 'Z');
        let delta = r.apply(RgaOp::Insert { id, origin_id: None, content: 'Z' }).unwrap();
        assert!(delta.is_none(), "re-applying same insert must be a no-op");
        assert_eq!(r.nodes.len(), 1);
    }

    #[test]
    fn delta_since_empty_vc() {
        let mut r = Rga::default();
        insert(&mut r, hlc(1, 1, 1), None, 'A');
        insert(&mut r, hlc(1, 2, 1), Some(hlc(1, 1, 1)), 'B');

        let vc = VectorClock::new();
        let delta = r.delta_since(&vc).unwrap();
        assert_eq!(delta.ops.len(), 2);
    }

    #[test]
    fn is_empty_all_tombstones() {
        let mut r = Rga::default();
        let id = hlc(1, 1, 1);
        insert(&mut r, id, None, 'Q');
        assert!(!r.is_empty());
        delete(&mut r, id);
        assert!(r.is_empty());
    }

    #[test]
    fn delete_unknown_node_is_noop() {
        let mut r = Rga::default();
        let delta = r.apply(RgaOp::Delete { id: hlc(99, 99, 99) }).unwrap();
        assert!(delta.is_none());
    }

    #[test]
    fn msgpack_roundtrip() {
        let mut r = Rga::default();
        let id1 = hlc(1, 1, 1);
        let id2 = hlc(1, 2, 1);
        insert(&mut r, id1, None, 'H');
        insert(&mut r, id2, Some(id1), 'i');

        let bytes = rmp_serde::encode::to_vec_named(&r).unwrap();
        let r2: Rga = rmp_serde::decode::from_slice(&bytes).unwrap();
        assert_eq!(r, r2);
    }

    #[test]
    fn delta_since_emits_delete_for_tombstone_even_if_insert_was_seen() {
        // Node 1 inserts 'A' (seen by client). Node 2 deletes 'A' (not seen by client).
        // delta_since must include the Delete op even though the client's VC shows
        // it has seen node 1's inserts up to that logical counter.
        let id_a = hlc(1, 1, 1); // authored by node 1

        let mut server = Rga::default();
        insert(&mut server, id_a, None, 'A');
        // Delete authored by node 2 — different node_id.
        server.apply(RgaOp::Delete { id: id_a }).unwrap();

        // Client has seen node 1's insert (logical=1) but nothing from node 2.
        let mut vc = VectorClock::new();
        vc.increment(1); // seen node_id=1, logical=1

        let delta = server.delta_since(&vc).unwrap();
        let has_delete = delta.ops.iter().any(|op| matches!(op, RgaOp::Delete { id } if *id == id_a));
        assert!(has_delete, "delta_since must emit Delete even when insert was already seen by client");
    }

    #[test]
    fn compact_removes_unreferenced_tombstones() {
        let mut r = Rga::default();
        let id_a = hlc(1, 1, 1);
        let id_b = hlc(1, 2, 1);
        let id_c = hlc(1, 3, 1);
        insert(&mut r, id_a, None, 'A');
        insert(&mut r, id_b, Some(id_a), 'B');
        insert(&mut r, id_c, Some(id_b), 'C');

        // Delete A — no live node uses A as origin (B uses A, but B is live)
        // Actually B uses A as origin, so A must be kept.
        delete(&mut r, id_a);
        let removed = r.compact();
        // A is still referenced by B as origin — must NOT be GC'd
        assert_eq!(removed, 0, "A is still origin of B, must be kept");
        assert_eq!(r.nodes.len(), 3);

        // Now delete B too — B was the last node referencing A as origin.
        delete(&mut r, id_b);
        let removed = r.compact();
        // Now A and B are both tombstones. C uses B as origin, so B is kept.
        // A is referenced by nothing live — can be removed.
        // B is still referenced by C — must be kept.
        assert_eq!(removed, 1, "A (unreferenced tombstone) must be GC'd");
        assert_eq!(text(&r), "C");

        // Delete C — now B is referenced by nothing.
        delete(&mut r, id_c);
        let removed = r.compact();
        // B and C are tombstones, neither referenced.
        assert_eq!(removed, 2, "B and C (unreferenced tombstones) must be GC'd");
        assert_eq!(r.nodes.len(), 0);
    }

    #[test]
    fn compact_preserves_text_correctness() {
        let mut r = Rga::default();
        // Build: H-e-l-l-o
        let ids: Vec<_> = (1u16..=5).map(|i| hlc(1, i, 1)).collect();
        insert(&mut r, ids[0], None, 'H');
        insert(&mut r, ids[1], Some(ids[0]), 'e');
        insert(&mut r, ids[2], Some(ids[1]), 'l');
        insert(&mut r, ids[3], Some(ids[2]), 'l');
        insert(&mut r, ids[4], Some(ids[3]), 'o');

        // Delete 'e' and first 'l'
        delete(&mut r, ids[1]);
        delete(&mut r, ids[2]);

        assert_eq!(text(&r), "Hlo");
        let _ = r.compact();
        // Text must be identical after compaction
        assert_eq!(text(&r), "Hlo");
    }

    #[test]
    fn compact_noop_when_no_tombstones() {
        let mut r = Rga::default();
        insert(&mut r, hlc(1, 1, 1), None, 'X');
        insert(&mut r, hlc(1, 2, 1), Some(hlc(1, 1, 1)), 'Y');
        let removed = r.compact();
        assert_eq!(removed, 0);
        assert_eq!(r.nodes.len(), 2);
    }
}
