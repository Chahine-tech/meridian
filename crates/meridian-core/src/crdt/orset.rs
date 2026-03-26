use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use super::{Crdt, CrdtError, VectorClock};

//
// State:  HashMap<element_key, HashSet<Uuid>>
//   - element_key: canonical JSON string of the element
//   - Each Uuid is an "add-tag" generated at add() time
//
// Semantics:
//   add(e)    → generate fresh UUID tag, store (e → {tag})
//   remove(e) → remove all currently-known tags for e
//   merge     → union of all tag sets per element
//
// Add-wins: if a concurrent add(e) and remove(e) race,
//   - The add generates a NEW uuid not known to the remove
//   - The remove only removes tags it knew about at remove-time
//   - After merge: new add-tag survives → add wins by construction
//
// Element type: serde_json::Value restricted to scalars and shallow arrays/objects
// (depth ≤ 2). Enforced here in apply() so it cannot be bypassed via WebSocket
// or anti-entropy, not just at the HTTP handler level.

/// Maximum allowed JSON nesting depth for ORSet elements.
/// Scalars = depth 0, `[1, 2]` = depth 1, `[[1]]` = depth 2 (rejected).
const MAX_ELEMENT_DEPTH: u32 = 1;

/// Returns the nesting depth of a JSON value (0 = scalar).
fn json_depth(v: &JsonValue) -> u32 {
    match v {
        JsonValue::Array(arr) => 1 + arr.iter().map(json_depth).max().unwrap_or(0),
        JsonValue::Object(map) => 1 + map.values().map(json_depth).max().unwrap_or(0),
        _ => 0,
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ORSet {
    /// element_key → set of live add-tags
    pub entries: HashMap<String, HashSet<Uuid>>,
    /// tag → (node_id, logical_seq) — used for delta_since filtering.
    /// Populated on Add; entries removed when the tag is removed.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub tag_versions: HashMap<Uuid, (u64, u32)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ORSetOp {
    /// Add an element. Tag is generated client-side.
    Add {
        element: JsonValue,
        tag: Uuid,
        /// Node (client) ID of the author — used for delta_since VectorClock tracking.
        #[serde(default)]
        node_id: u64,
        /// Logical sequence number from the author's local counter — used for delta_since.
        #[serde(default)]
        seq: u32,
    },
    /// Remove an element — carries snapshot of tags known at remove-time.
    Remove {
        element: JsonValue,
        /// The tags to remove. A freshly-added tag not in this set survives.
        known_tags: HashSet<Uuid>,
    },
}

/// Delta encodes both new adds and removes since last sync.
#[must_use]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ORSetDelta {
    /// element_key → tags to add
    pub adds: HashMap<String, HashSet<Uuid>>,
    /// element_key → tags to remove (tombstones)
    pub removes: HashMap<String, HashSet<Uuid>>,
    /// tag → (node_id, seq) version info for added tags — propagated for delta tracking.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub tag_versions: HashMap<Uuid, (u64, u32)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ORSetValue {
    /// Elements that have at least one live add-tag.
    pub elements: Vec<JsonValue>,
}

fn element_key(e: &JsonValue) -> String {
    // Canonical JSON serialization as map key.
    // serde_json produces deterministic output for scalars and arrays.
    e.to_string()
}

impl Crdt for ORSet {
    type Op = ORSetOp;
    type Delta = ORSetDelta;
    type Value = ORSetValue;

    fn apply(&mut self, op: ORSetOp) -> Result<Option<ORSetDelta>, CrdtError> {
        match op {
            ORSetOp::Add { element, tag, node_id, seq } => {
                if json_depth(&element) > MAX_ELEMENT_DEPTH {
                    return Err(CrdtError::InvalidOp(format!(
                        "ORSet element nesting depth {} exceeds maximum {}",
                        json_depth(&element),
                        MAX_ELEMENT_DEPTH
                    )));
                }
                let key = element_key(&element);
                let tags = self.entries.entry(key.clone()).or_default();
                if tags.contains(&tag) {
                    return Ok(None); // already present — idempotent
                }
                tags.insert(tag);
                // Track version for delta_since filtering.
                self.tag_versions.insert(tag, (node_id, seq));

                let mut adds = HashMap::new();
                let mut add_tags = HashSet::new();
                add_tags.insert(tag);
                adds.insert(key, add_tags);

                let mut tag_versions = HashMap::new();
                tag_versions.insert(tag, (node_id, seq));

                Ok(Some(ORSetDelta { adds, removes: HashMap::new(), tag_versions }))
            }

            ORSetOp::Remove { element, known_tags } => {
                let key = element_key(&element);
                let mut actually_removed = HashSet::new();

                if let Some(tags) = self.entries.get_mut(&key) {
                    for t in &known_tags {
                        if tags.remove(t) {
                            actually_removed.insert(*t);
                        }
                    }
                    if tags.is_empty() {
                        self.entries.remove(&key);
                    }
                }

                if actually_removed.is_empty() {
                    return Ok(None);
                }

                // Clean up tag_versions for removed tags.
                for t in &actually_removed {
                    self.tag_versions.remove(t);
                }

                let mut removes = HashMap::new();
                removes.insert(key, actually_removed);
                Ok(Some(ORSetDelta { adds: HashMap::new(), removes, tag_versions: HashMap::new() }))
            }
        }
    }

    fn merge(&mut self, other: &ORSet) {
        for (key, other_tags) in &other.entries {
            let my_tags = self.entries.entry(key.clone()).or_default();
            for tag in other_tags {
                my_tags.insert(*tag);
            }
        }
        // Merge tag_versions from the other replica.
        for (tag, version) in &other.tag_versions {
            self.tag_versions.entry(*tag).or_insert(*version);
        }
        // Remove entries that ended up empty (defensive — shouldn't happen in practice).
        self.entries.retain(|_, tags| !tags.is_empty());
    }

    fn merge_delta(&mut self, delta: ORSetDelta) {
        // Apply adds first, propagating version info.
        for (key, add_tags) in delta.adds {
            let tags = self.entries.entry(key).or_default();
            for tag in add_tags {
                tags.insert(tag);
            }
        }
        for (tag, version) in delta.tag_versions {
            self.tag_versions.entry(tag).or_insert(version);
        }
        // Then removes — clean up tag_versions too.
        for (key, remove_tags) in delta.removes {
            if let Some(tags) = self.entries.get_mut(&key) {
                for tag in &remove_tags {
                    tags.remove(tag);
                    self.tag_versions.remove(tag);
                }
                if tags.is_empty() {
                    self.entries.remove(&key);
                }
            }
        }
    }

    fn delta_since(&self, since: &VectorClock) -> Option<ORSetDelta> {
        // Include only tags the receiver hasn't seen yet, based on VectorClock.
        // A tag is "unseen" if its (node_id, seq) is not covered by `since`:
        //   since.get(node_id) < seq  →  receiver hasn't seen this add yet.
        //
        // Tags without version info (added by old clients before this field existed)
        // are always included conservatively.
        let mut adds: HashMap<String, HashSet<Uuid>> = HashMap::new();
        let mut tag_versions: HashMap<Uuid, (u64, u32)> = HashMap::new();

        for (key, tags) in &self.entries {
            let unseen: HashSet<Uuid> = tags
                .iter()
                .filter(|tag| {
                    match self.tag_versions.get(tag) {
                        // No version info — include conservatively (old wire format).
                        None => true,
                        Some(&(node_id, seq)) => since.get(node_id) < seq,
                    }
                })
                .copied()
                .collect();

            if !unseen.is_empty() {
                for tag in &unseen {
                    if let Some(&v) = self.tag_versions.get(tag) {
                        tag_versions.insert(*tag, v);
                    }
                }
                adds.insert(key.clone(), unseen);
            }
        }

        if adds.is_empty() {
            None
        } else {
            Some(ORSetDelta { adds, removes: HashMap::new(), tag_versions })
        }
    }

    fn value(&self) -> ORSetValue {
        let elements = self
            .entries
            .iter()
            .filter(|(_, tags)| !tags.is_empty())
            .filter_map(|(key, _)| serde_json::from_str(key).ok())
            .collect();
        ORSetValue { elements }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(_s: &ORSet, val: &str) -> ORSetOp {
        ORSetOp::Add {
            element: JsonValue::String(val.to_string()),
            tag: Uuid::new_v4(),
            node_id: 1,
            seq: 1,
        }
    }

    fn remove_all(s: &ORSet, val: &str) -> ORSetOp {
        let key = element_key(&JsonValue::String(val.to_string()));
        let known_tags = s.entries.get(&key).cloned().unwrap_or_default();
        ORSetOp::Remove {
            element: JsonValue::String(val.to_string()),
            known_tags,
        }
    }

    fn contains(s: &ORSet, val: &str) -> bool {
        let key = element_key(&JsonValue::String(val.to_string()));
        s.entries.get(&key).map(|t| !t.is_empty()).unwrap_or(false)
    }

    #[test]
    fn add_and_contains() {
        let mut s = ORSet::default();
        s.apply(add(&s, "apple")).unwrap();
        assert!(contains(&s, "apple"));
    }

    #[test]
    fn remove_after_add() {
        let mut s = ORSet::default();
        s.apply(add(&s, "apple")).unwrap();
        let rm = remove_all(&s, "apple");
        s.apply(rm).unwrap();
        assert!(!contains(&s, "apple"));
    }

    #[test]
    fn add_wins_concurrent() {
        // Simulate concurrent add on node B and remove on node A.
        let mut a = ORSet::default();
        let initial_add = add(&a, "apple");
        a.apply(initial_add).unwrap();

        // B gets the same initial state
        let mut b = a.clone();

        // A removes apple (knows about initial tag)
        let rm = remove_all(&a, "apple");
        a.apply(rm).unwrap();

        // B concurrently adds apple again (new UUID — not known to A's remove)
        b.apply(add(&b, "apple")).unwrap();

        // Merge: B's new tag is unknown to A's remove → survives
        a.merge(&b);
        assert!(contains(&a, "apple"), "add-wins: new tag should survive");
    }

    #[test]
    fn merge_commutative() {
        let mut a = ORSet::default();
        let mut b = ORSet::default();
        a.apply(add(&a, "x")).unwrap();
        b.apply(add(&b, "y")).unwrap();

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab, ba);
    }

    #[test]
    fn merge_idempotent() {
        let mut a = ORSet::default();
        a.apply(add(&a, "z")).unwrap();
        let b = a.clone();
        a.merge(&b);
        assert_eq!(a, b);
    }

    #[test]
    fn merge_associative() {
        let mut a = ORSet::default();
        let mut b = ORSet::default();
        let mut c = ORSet::default();
        a.apply(add(&a, "a")).unwrap();
        b.apply(add(&b, "b")).unwrap();
        c.apply(add(&c, "c")).unwrap();

        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(ab_c, a_bc);
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let mut s = ORSet::default();
        let delta = s.apply(ORSetOp::Remove {
            element: JsonValue::String("ghost".into()),
            known_tags: HashSet::new(),
        }).unwrap();
        assert!(delta.is_none());
    }

    // --- depth validation ---

    #[test]
    fn add_scalar_accepted() {
        let mut s = ORSet::default();
        assert!(s.apply(ORSetOp::Add { element: JsonValue::Number(42.into()), tag: Uuid::new_v4(), node_id: 1, seq: 1 }).is_ok());
        assert!(s.apply(ORSetOp::Add { element: JsonValue::String("ok".into()), tag: Uuid::new_v4(), node_id: 1, seq: 2 }).is_ok());
        assert!(s.apply(ORSetOp::Add { element: JsonValue::Bool(true), tag: Uuid::new_v4(), node_id: 1, seq: 3 }).is_ok());
    }

    #[test]
    fn add_shallow_array_accepted() {
        let mut s = ORSet::default();
        let arr = JsonValue::Array(vec![JsonValue::Number(1.into()), JsonValue::String("x".into())]);
        assert!(s.apply(ORSetOp::Add { element: arr, tag: Uuid::new_v4(), node_id: 1, seq: 1 }).is_ok());
    }

    #[test]
    fn add_nested_array_rejected() {
        let mut s = ORSet::default();
        let nested = JsonValue::Array(vec![JsonValue::Array(vec![JsonValue::Number(1.into())])]);
        let result = s.apply(ORSetOp::Add { element: nested, tag: Uuid::new_v4(), node_id: 1, seq: 1 });
        assert!(result.is_err());
    }

    #[test]
    fn add_nested_object_rejected() {
        let mut s = ORSet::default();
        let mut inner = serde_json::Map::new();
        inner.insert("x".into(), JsonValue::Number(1.into()));
        let mut outer = serde_json::Map::new();
        outer.insert("nested".into(), JsonValue::Object(inner));
        let result = s.apply(ORSetOp::Add { element: JsonValue::Object(outer), tag: Uuid::new_v4(), node_id: 1, seq: 1 });
        assert!(result.is_err());
    }

    // --- delta_since ---

    #[test]
    fn delta_since_returns_only_unseen_adds() {
        use crate::crdt::VectorClock;

        let mut s = ORSet::default();
        // Node 1 adds "apple" at seq=1, "banana" at seq=2
        s.apply(ORSetOp::Add { element: JsonValue::String("apple".into()), tag: Uuid::new_v4(), node_id: 1, seq: 1 }).unwrap();
        s.apply(ORSetOp::Add { element: JsonValue::String("banana".into()), tag: Uuid::new_v4(), node_id: 1, seq: 2 }).unwrap();

        // Receiver has already seen node_id=1 up to seq=1 — only "banana" (seq=2) is new.
        let mut seen = VectorClock::new();
        seen.entries.insert(1, 1);

        let delta = s.delta_since(&seen).unwrap();
        assert!(!delta.adds.contains_key(&element_key(&JsonValue::String("apple".into()))),
            "apple was already seen — should not be in delta");
        assert!(delta.adds.contains_key(&element_key(&JsonValue::String("banana".into()))),
            "banana is new — must be in delta");
    }

    #[test]
    fn delta_since_empty_when_all_seen() {
        use crate::crdt::VectorClock;

        let mut s = ORSet::default();
        s.apply(ORSetOp::Add { element: JsonValue::String("x".into()), tag: Uuid::new_v4(), node_id: 1, seq: 1 }).unwrap();

        let mut seen = VectorClock::new();
        seen.entries.insert(1, 1);

        assert!(s.delta_since(&seen).is_none(), "nothing new — delta should be None");
    }

    #[test]
    fn delta_since_full_state_when_vc_empty() {
        use crate::crdt::VectorClock;

        let mut s = ORSet::default();
        s.apply(ORSetOp::Add { element: JsonValue::String("a".into()), tag: Uuid::new_v4(), node_id: 1, seq: 1 }).unwrap();
        s.apply(ORSetOp::Add { element: JsonValue::String("b".into()), tag: Uuid::new_v4(), node_id: 2, seq: 1 }).unwrap();

        let seen = VectorClock::new(); // empty — receiver has seen nothing
        let delta = s.delta_since(&seen).unwrap();
        assert_eq!(delta.adds.len(), 2, "empty VC → both elements should be in delta");
    }
}
