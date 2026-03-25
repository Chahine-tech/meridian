use serde::{Deserialize, Serialize};

use super::{Crdt, CrdtError, HybridLogicalClock, VectorClock};

//
// TreeCRDT — hierarchical tree with convergent move semantics.
//
// Based on Kleppmann et al. (2021) "A highly-available move operation for
// replicated trees". Each node has a unique HLC id, a parent pointer, a
// fractional-index position string for sibling ordering, and a string value.
//
// Key design decisions:
// - Tombstones are never GC'd (same as RGA). Children of deleted nodes are
//   preserved; the subtree becomes invisible but is restored if the delete is
//   later "undone" via a concurrent move.
// - MoveNode uses a separate op_id so a node can be moved many times; each
//   move has its own causal identity in the move log.
// - Concurrent moves are ordered by op_id. A move that would create a cycle
//   (i.e. moving a node under its own descendant) is silently discarded.
// - UpdateNode uses last-write-wins on the value field via updated_at HLC.
// - Sibling order: lexicographic on position string, ties broken by id descending
//   (higher HLC = leftmost), matching the RGA tiebreak convention.

/// A single tree node. Tombstones are kept forever.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreeNode {
    pub id: HybridLogicalClock,
    /// None = direct child of the implicit root.
    pub parent_id: Option<HybridLogicalClock>,
    /// Fractional index string for sibling ordering, e.g. "a0", "a0V", "b0".
    /// The caller supplies this; the SDK computes it locally.
    pub position: String,
    pub value: String,
    pub deleted: bool,
    /// HLC of the last accepted UpdateNode op. Used for LWW on value.
    pub updated_at: HybridLogicalClock,
}

/// One entry in the ordered move log — needed to detect cycles during merge.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MoveRecord {
    /// Unique ID of this move op (a fresh HLC tick, not the node id).
    pub op_id: HybridLogicalClock,
    pub node_id: HybridLogicalClock,
    pub new_parent_id: Option<HybridLogicalClock>,
    pub new_position: String,
    /// Parent state before this move — kept so we can undo during merge replay.
    pub old_parent_id: Option<HybridLogicalClock>,
    pub old_position: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreeCrdt {
    pub nodes: Vec<TreeNode>,
    /// All MoveNode ops ever applied, sorted ascending by op_id.
    pub move_log: Vec<MoveRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TreeOp {
    AddNode {
        id: HybridLogicalClock,
        parent_id: Option<HybridLogicalClock>,
        position: String,
        value: String,
    },
    MoveNode {
        /// Fresh HLC for this op — NOT the node id.
        op_id: HybridLogicalClock,
        node_id: HybridLogicalClock,
        new_parent_id: Option<HybridLogicalClock>,
        new_position: String,
    },
    UpdateNode {
        id: HybridLogicalClock,
        value: String,
        /// LWW timestamp for this update.
        updated_at: HybridLogicalClock,
    },
    DeleteNode {
        id: HybridLogicalClock,
    },
}

#[must_use]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreeDelta {
    pub ops: Vec<TreeOp>,
}

/// Recursive tree node as delivered to clients.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeNodeValue {
    /// HLC serialized as "wall_ms:logical:node_id" — avoids JS bigint issues.
    pub id: String,
    pub value: String,
    pub children: Vec<TreeNodeValue>,
}

/// The root-level value returned by `TreeCrdt::value()`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeValue {
    pub roots: Vec<TreeNodeValue>,
}

impl TreeCrdt {
    /// Returns true if `ancestor_id` is an ancestor of `node_id` in the
    /// current (possibly partially updated) parent-pointer state.
    /// Cycles in the parent graph are guarded against with a visited set.
    fn is_ancestor(&self, mut node_id: HybridLogicalClock, ancestor_id: HybridLogicalClock) -> bool {
        let mut visited = std::collections::BTreeSet::new();
        loop {
            if node_id == ancestor_id {
                return true;
            }
            if !visited.insert(node_id) {
                // Guard against corrupt cycles in state (should not happen in
                // practice, but keeps the function total).
                return false;
            }
            match self.nodes.iter().find(|n| n.id == node_id) {
                Some(n) => match n.parent_id {
                    Some(pid) => node_id = pid,
                    None => return false,
                },
                None => return false,
            }
        }
    }

    /// Serialize an HLC to the wire string format used in TreeNodeValue.
    fn hlc_to_str(hlc: HybridLogicalClock) -> String {
        format!("{}:{}:{}", hlc.wall_ms, u32::from(hlc.logical), hlc.node_id)
    }

    /// Build the recursive value for one node and its visible descendants.
    fn build_node_value(&self, node: &TreeNode) -> TreeNodeValue {
        let mut children: Vec<&TreeNode> = self
            .nodes
            .iter()
            .filter(|n| !n.deleted && n.parent_id == Some(node.id))
            .collect();

        // Sort by (position asc, id desc) — lexicographic position, ties broken by HLC descending.
        children.sort_by(|a, b| {
            a.position
                .cmp(&b.position)
                .then_with(|| b.id.cmp(&a.id))
        });

        TreeNodeValue {
            id: Self::hlc_to_str(node.id),
            value: node.value.clone(),
            children: children.iter().map(|c| self.build_node_value(c)).collect(),
        }
    }

    /// Insert a MoveRecord into move_log, keeping it sorted by op_id ascending.
    fn insert_move_record(&mut self, record: MoveRecord) {
        let idx = self.move_log.partition_point(|r| r.op_id < record.op_id);
        self.move_log.insert(idx, record);
    }
}

impl Crdt for TreeCrdt {
    type Op = TreeOp;
    type Delta = TreeDelta;
    type Value = TreeValue;

    fn apply(&mut self, op: TreeOp) -> Result<Option<TreeDelta>, CrdtError> {
        match op {
            TreeOp::AddNode { id, parent_id, ref position, ref value } => {
                // Idempotent: skip if already present.
                if self.nodes.iter().any(|n| n.id == id) {
                    return Ok(None);
                }
                self.nodes.push(TreeNode {
                    id,
                    parent_id,
                    position: position.clone(),
                    value: value.clone(),
                    deleted: false,
                    updated_at: id,
                });
                Ok(Some(TreeDelta { ops: vec![op] }))
            }

            TreeOp::MoveNode { op_id, node_id, new_parent_id, ref new_position } => {
                // Idempotent: skip if this exact move op was already applied.
                if self.move_log.iter().any(|r| r.op_id == op_id) {
                    return Ok(None);
                }
                // Node must exist to move it.
                let node_idx = match self.nodes.iter().position(|n| n.id == node_id) {
                    Some(i) => i,
                    None => return Ok(None),
                };
                // Cycle prevention: refuse if new_parent is a descendant of node_id.
                if new_parent_id.is_some_and(|new_pid| new_pid == node_id || self.is_ancestor(new_pid, node_id)) {
                    return Ok(None);
                }
                let old_parent_id = self.nodes[node_idx].parent_id;
                let old_position = self.nodes[node_idx].position.clone();

                self.nodes[node_idx].parent_id = new_parent_id;
                self.nodes[node_idx].position = new_position.clone();

                self.insert_move_record(MoveRecord {
                    op_id,
                    node_id,
                    new_parent_id,
                    new_position: new_position.clone(),
                    old_parent_id,
                    old_position,
                });

                Ok(Some(TreeDelta { ops: vec![op] }))
            }

            TreeOp::UpdateNode { id, ref value, updated_at } => {
                match self.nodes.iter_mut().find(|n| n.id == id) {
                    None => Ok(None),
                    Some(node) => {
                        // LWW: only accept if this update is strictly newer.
                        if updated_at <= node.updated_at {
                            return Ok(None);
                        }
                        node.value = value.clone();
                        node.updated_at = updated_at;
                        Ok(Some(TreeDelta { ops: vec![op] }))
                    }
                }
            }

            TreeOp::DeleteNode { id } => {
                match self.nodes.iter_mut().find(|n| n.id == id) {
                    None => Ok(None),
                    Some(node) if node.deleted => Ok(None),
                    Some(node) => {
                        node.deleted = true;
                        Ok(Some(TreeDelta { ops: vec![TreeOp::DeleteNode { id }] }))
                    }
                }
            }
        }
    }

    fn merge(&mut self, other: &TreeCrdt) {
        // Step 1: merge node set (excluding parent pointers — those come from move log replay).
        for node in &other.nodes {
            match self.nodes.iter_mut().find(|n| n.id == node.id) {
                Some(existing) => {
                    // Tombstone propagates.
                    if node.deleted {
                        existing.deleted = true;
                    }
                    // LWW on value.
                    if node.updated_at > existing.updated_at {
                        existing.value = node.value.clone();
                        existing.updated_at = node.updated_at;
                    }
                }
                None => {
                    self.nodes.push(node.clone());
                }
            }
        }

        // Step 2: build the combined move log (self + other), sorted by op_id.
        let mut combined_log: Vec<MoveRecord> = self.move_log.clone();
        for record in &other.move_log {
            if !combined_log.iter().any(|r| r.op_id == record.op_id) {
                let idx = combined_log.partition_point(|r| r.op_id < record.op_id);
                combined_log.insert(idx, record.clone());
            }
        }

        if combined_log == self.move_log {
            // No new move ops — parent pointers are already correct.
            return;
        }

        // Step 3: reset all parent pointers to the original AddNode state, then
        // replay the combined log in op_id order. This ensures all replicas
        // converge to the same tree regardless of the order they received moves.
        //
        // "Original state" = the parent_id that came from the AddNode op,
        // which we recover by resetting to the AddNode's parent (stored in the
        // first MoveRecord's old_parent_id if it exists, else the node's
        // current parent when no moves have been applied). Since we keep
        // AddNode parent_id in the node struct at creation time, and moves
        // only update parent_id, we need to reconstruct it.
        //
        // Simplest correct approach: reset to AddNode parent (None for
        // root-level, or the parent given at creation). We approximate this by
        // rewinding to the old_parent_id of the earliest MoveRecord per node,
        // then replaying from there.

        // Build a map: node_id -> original parent_id (before any moves).
        let mut original_parents: std::collections::BTreeMap<HybridLogicalClock, Option<HybridLogicalClock>> = std::collections::BTreeMap::new();
        for node in &self.nodes {
            original_parents.insert(node.id, node.parent_id);
        }
        // Walk the combined log forward to find the oldest record per node.
        // The old_parent_id of the first move on each node is the AddNode parent.
        let mut first_move_seen: std::collections::BTreeSet<HybridLogicalClock> = std::collections::BTreeSet::new();
        for record in &combined_log {
            if first_move_seen.insert(record.node_id) {
                original_parents.insert(record.node_id, record.old_parent_id);
            }
        }

        // Reset parent pointers to original AddNode state.
        for node in &mut self.nodes {
            if let Some(orig) = original_parents.get(&node.id) {
                node.parent_id = *orig;
            }
        }

        // Clear move log — we will rebuild it via replay.
        self.move_log.clear();

        // Replay all moves in op_id order with cycle detection.
        for record in &combined_log {
            let _ = self.apply(TreeOp::MoveNode {
                op_id: record.op_id,
                node_id: record.node_id,
                new_parent_id: record.new_parent_id,
                new_position: record.new_position.clone(),
            });
        }
    }

    fn merge_delta(&mut self, delta: TreeDelta) {
        for op in delta.ops {
            let _ = self.apply(op);
        }
    }

    fn delta_since(&self, since: &VectorClock) -> Option<TreeDelta> {
        let mut ops: Vec<TreeOp> = Vec::new();

        for node in &self.nodes {
            let seen = since.get(node.id.node_id);
            let logical = u32::from(node.id.logical);

            if seen < logical {
                ops.push(TreeOp::AddNode {
                    id: node.id,
                    parent_id: node.parent_id,
                    position: node.position.clone(),
                    value: node.value.clone(),
                });
                if node.deleted {
                    ops.push(TreeOp::DeleteNode { id: node.id });
                }
            }
        }

        // Include unseen move ops.
        for record in &self.move_log {
            let seen = since.get(record.op_id.node_id);
            let logical = u32::from(record.op_id.logical);
            if seen < logical {
                ops.push(TreeOp::MoveNode {
                    op_id: record.op_id,
                    node_id: record.node_id,
                    new_parent_id: record.new_parent_id,
                    new_position: record.new_position.clone(),
                });
            }
        }

        if ops.is_empty() { None } else { Some(TreeDelta { ops }) }
    }

    fn value(&self) -> TreeValue {
        // Collect root nodes (parent_id == None, not deleted).
        let mut roots: Vec<&TreeNode> = self
            .nodes
            .iter()
            .filter(|n| !n.deleted && n.parent_id.is_none())
            .collect();

        roots.sort_by(|a, b| {
            a.position
                .cmp(&b.position)
                .then_with(|| b.id.cmp(&a.id))
        });

        TreeValue {
            roots: roots.iter().map(|n| self.build_node_value(n)).collect(),
        }
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

    fn add(tree: &mut TreeCrdt, id: HybridLogicalClock, parent_id: Option<HybridLogicalClock>, pos: &str, val: &str) {
        tree.apply(TreeOp::AddNode {
            id,
            parent_id,
            position: pos.to_string(),
            value: val.to_string(),
        })
        .unwrap();
    }

    fn root_ids(tree: &TreeCrdt) -> Vec<String> {
        tree.value().roots.iter().map(|n| n.id.clone()).collect()
    }

    #[test]
    fn add_node_idempotent() {
        let mut t = TreeCrdt::default();
        let id = hlc(1, 1, 1);
        add(&mut t, id, None, "a0", "Root");
        let delta = t.apply(TreeOp::AddNode {
            id,
            parent_id: None,
            position: "a0".into(),
            value: "Root".into(),
        }).unwrap();
        assert!(delta.is_none(), "duplicate add must be a no-op");
        assert_eq!(t.nodes.len(), 1);
    }

    #[test]
    fn add_children_ordered() {
        let mut t = TreeCrdt::default();
        let parent = hlc(1, 1, 1);
        let c1 = hlc(1, 2, 1);
        let c2 = hlc(1, 3, 1);
        let c3 = hlc(1, 4, 1);
        add(&mut t, parent, None, "a0", "Parent");
        add(&mut t, c1, Some(parent), "b0", "C");
        add(&mut t, c2, Some(parent), "a0", "A");
        add(&mut t, c3, Some(parent), "a0", "B"); // same position as c2, higher id → comes first

        let children = &t.value().roots[0].children;
        assert_eq!(children.len(), 3);
        // a0 entries first (c3 before c2 by id desc), then b0
        assert_eq!(children[0].value, "B");
        assert_eq!(children[1].value, "A");
        assert_eq!(children[2].value, "C");
    }

    #[test]
    fn delete_node_tombstone() {
        let mut t = TreeCrdt::default();
        let id = hlc(1, 1, 1);
        add(&mut t, id, None, "a0", "Root");
        t.apply(TreeOp::DeleteNode { id }).unwrap();

        assert!(t.is_empty());
        assert_eq!(t.nodes.len(), 1); // tombstone kept
    }

    #[test]
    fn delete_preserves_children() {
        let mut t = TreeCrdt::default();
        let parent = hlc(1, 1, 1);
        let child = hlc(1, 2, 1);
        add(&mut t, parent, None, "a0", "Parent");
        add(&mut t, child, Some(parent), "a0", "Child");

        t.apply(TreeOp::DeleteNode { id: parent }).unwrap();

        // Child still exists but parent is deleted → child's parent_id is
        // still Some(parent). value() hides both since parent is deleted
        // and child is orphaned (parent not in visible tree).
        let v = t.value();
        assert!(v.roots.is_empty(), "deleted parent must not appear");
    }

    #[test]
    fn move_node_basic() {
        let mut t = TreeCrdt::default();
        let r1 = hlc(1, 1, 1);
        let r2 = hlc(1, 2, 1);
        let child = hlc(1, 3, 1);
        add(&mut t, r1, None, "a0", "R1");
        add(&mut t, r2, None, "b0", "R2");
        add(&mut t, child, Some(r1), "a0", "Child");

        let op_id = hlc(2, 1, 1);
        t.apply(TreeOp::MoveNode {
            op_id,
            node_id: child,
            new_parent_id: Some(r2),
            new_position: "a0".into(),
        })
        .unwrap();

        let v = t.value();
        assert_eq!(v.roots[0].children.len(), 0, "r1 should have no children");
        assert_eq!(v.roots[1].children.len(), 1, "r2 should have child");
        assert_eq!(v.roots[1].children[0].value, "Child");
    }

    #[test]
    fn move_node_cycle_prevention() {
        let mut t = TreeCrdt::default();
        let root = hlc(1, 1, 1);
        let child = hlc(1, 2, 1);
        let grandchild = hlc(1, 3, 1);
        add(&mut t, root, None, "a0", "Root");
        add(&mut t, child, Some(root), "a0", "Child");
        add(&mut t, grandchild, Some(child), "a0", "Grandchild");

        // Try to move root under grandchild — would create a cycle.
        let op_id = hlc(2, 1, 1);
        let delta = t.apply(TreeOp::MoveNode {
            op_id,
            node_id: root,
            new_parent_id: Some(grandchild),
            new_position: "a0".into(),
        }).unwrap();

        assert!(delta.is_none(), "cycle-creating move must be discarded");
        // root is still at the top level
        assert_eq!(t.value().roots.len(), 1);
        assert_eq!(t.value().roots[0].value, "Root");
    }

    #[test]
    fn move_node_self_cycle_prevention() {
        let mut t = TreeCrdt::default();
        let id = hlc(1, 1, 1);
        add(&mut t, id, None, "a0", "Node");

        let delta = t.apply(TreeOp::MoveNode {
            op_id: hlc(2, 1, 1),
            node_id: id,
            new_parent_id: Some(id), // move under itself
            new_position: "a0".into(),
        }).unwrap();
        assert!(delta.is_none());
    }

    #[test]
    fn concurrent_moves_converge() {
        // Two replicas start with the same state.
        // Replica 1 moves child under r2.
        // Replica 2 moves child under r3.
        // After merge both should have the same parent for child.
        let mut base = TreeCrdt::default();
        let r1 = hlc(1, 1, 1);
        let r2 = hlc(1, 2, 1);
        let r3 = hlc(1, 3, 1);
        let child = hlc(1, 4, 1);
        add(&mut base, r1, None, "a0", "R1");
        add(&mut base, r2, None, "b0", "R2");
        add(&mut base, r3, None, "c0", "R3");
        add(&mut base, child, Some(r1), "a0", "Child");

        let mut t1 = base.clone();
        let mut t2 = base.clone();

        // op1 (node 1, higher HLC) moves child to r2
        t1.apply(TreeOp::MoveNode {
            op_id: hlc(10, 1, 1),
            node_id: child,
            new_parent_id: Some(r2),
            new_position: "a0".into(),
        }).unwrap();

        // op2 (node 2, lower HLC) moves child to r3
        t2.apply(TreeOp::MoveNode {
            op_id: hlc(10, 1, 2),
            node_id: child,
            new_parent_id: Some(r3),
            new_position: "a0".into(),
        }).unwrap();

        let mut m1 = t1.clone();
        m1.merge(&t2);
        let mut m2 = t2.clone();
        m2.merge(&t1);

        // Both replicas must agree on where child ended up.
        let child_parent_m1 = m1.nodes.iter().find(|n| n.id == child).unwrap().parent_id;
        let child_parent_m2 = m2.nodes.iter().find(|n| n.id == child).unwrap().parent_id;
        assert_eq!(child_parent_m1, child_parent_m2, "concurrent moves must converge");
    }

    #[test]
    fn update_node_lww() {
        let mut t = TreeCrdt::default();
        let id = hlc(1, 1, 1);
        add(&mut t, id, None, "a0", "Original");

        // Newer update wins.
        t.apply(TreeOp::UpdateNode {
            id,
            value: "Updated".into(),
            updated_at: hlc(2, 1, 1),
        }).unwrap();
        assert_eq!(t.value().roots[0].value, "Updated");

        // Older update is ignored.
        let delta = t.apply(TreeOp::UpdateNode {
            id,
            value: "Stale".into(),
            updated_at: hlc(1, 2, 1),
        }).unwrap();
        assert!(delta.is_none(), "stale update must be ignored");
        assert_eq!(t.value().roots[0].value, "Updated");
    }

    #[test]
    fn delta_since_empty_vc() {
        let mut t = TreeCrdt::default();
        let root = hlc(1, 1, 1);
        let child = hlc(1, 2, 1);
        add(&mut t, root, None, "a0", "Root");
        add(&mut t, child, Some(root), "a0", "Child");

        let vc = VectorClock::new();
        let delta = t.delta_since(&vc).unwrap();
        // Two AddNode ops.
        assert_eq!(delta.ops.len(), 2);
    }

    #[test]
    fn delta_since_no_changes() {
        let mut t = TreeCrdt::default();
        let id = hlc(1, 1, 1);
        add(&mut t, id, None, "a0", "Root");

        // Build a VC that has seen this node (increment to match logical counter).
        let mut vc = VectorClock::new();
        for _ in 0..u32::from(id.logical) {
            vc.increment(id.node_id);
        }

        let delta = t.delta_since(&vc);
        assert!(delta.is_none(), "client is up-to-date, delta must be None");
    }

    #[test]
    fn msgpack_roundtrip() {
        let mut t = TreeCrdt::default();
        let root = hlc(1, 1, 1);
        let child = hlc(1, 2, 1);
        add(&mut t, root, None, "a0", "Root");
        add(&mut t, child, Some(root), "a0", "Child");
        t.apply(TreeOp::MoveNode {
            op_id: hlc(2, 1, 1),
            node_id: child,
            new_parent_id: None,
            new_position: "b0".into(),
        }).unwrap();

        let bytes = rmp_serde::encode::to_vec_named(&t).unwrap();
        let t2: TreeCrdt = rmp_serde::decode::from_slice(&bytes).unwrap();
        assert_eq!(t, t2);
    }
}
