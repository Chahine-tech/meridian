use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use super::{Crdt, CrdtError, VectorClock};

// ---------------------------------------------------------------------------
// ORSet — Observed-Remove Set
// ---------------------------------------------------------------------------
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
// Element type: serde_json::Value restricted to scalars and shallow arrays
// (depth ≤ 2, enforced on ingress by the HTTP handler, not here).

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ORSet {
    /// element_key → set of live add-tags
    pub entries: HashMap<String, HashSet<Uuid>>,
}

// ---------------------------------------------------------------------------
// Op + Delta
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ORSetOp {
    /// Add an element. Tag is generated client-side.
    Add {
        element: JsonValue,
        tag: Uuid,
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
}

// ---------------------------------------------------------------------------
// Value
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ORSetValue {
    /// Elements that have at least one live add-tag.
    pub elements: Vec<JsonValue>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn element_key(e: &JsonValue) -> String {
    // Canonical JSON serialization as map key.
    // serde_json produces deterministic output for scalars and arrays.
    e.to_string()
}

// ---------------------------------------------------------------------------
// Crdt impl
// ---------------------------------------------------------------------------

impl Crdt for ORSet {
    type Op = ORSetOp;
    type Delta = ORSetDelta;
    type Value = ORSetValue;

    fn apply(&mut self, op: ORSetOp) -> Result<Option<ORSetDelta>, CrdtError> {
        match op {
            ORSetOp::Add { element, tag } => {
                let key = element_key(&element);
                let tags = self.entries.entry(key.clone()).or_default();
                if tags.contains(&tag) {
                    return Ok(None); // already present — idempotent
                }
                tags.insert(tag);

                let mut adds = HashMap::new();
                let mut add_tags = HashSet::new();
                add_tags.insert(tag);
                adds.insert(key, add_tags);

                Ok(Some(ORSetDelta { adds, removes: HashMap::new() }))
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

                let mut removes = HashMap::new();
                removes.insert(key, actually_removed);
                Ok(Some(ORSetDelta { adds: HashMap::new(), removes }))
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
        // Remove entries that ended up empty (defensive — shouldn't happen in practice).
        self.entries.retain(|_, tags| !tags.is_empty());
    }

    fn merge_delta(&mut self, delta: ORSetDelta) {
        // Apply adds first
        for (key, add_tags) in delta.adds {
            let tags = self.entries.entry(key).or_default();
            for tag in add_tags {
                tags.insert(tag);
            }
        }
        // Then removes
        for (key, remove_tags) in delta.removes {
            if let Some(tags) = self.entries.get_mut(&key) {
                for tag in remove_tags {
                    tags.remove(&tag);
                }
                if tags.is_empty() {
                    self.entries.remove(&key);
                }
            }
        }
    }

    fn delta_since(&self, _since: &VectorClock) -> Option<ORSetDelta> {
        // Conservative: return full add-set as delta.
        // A more precise implementation would track per-tag timestamps.
        if self.entries.is_empty() {
            return None;
        }
        let adds: HashMap<String, HashSet<Uuid>> = self
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Some(ORSetDelta { adds, removes: HashMap::new() })
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn add(_s: &ORSet, val: &str) -> ORSetOp {
        ORSetOp::Add {
            element: JsonValue::String(val.to_string()),
            tag: Uuid::new_v4(),
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
}
