use pgrx::prelude::*;

use meridian_core::crdt::{registry::CrdtValue, Crdt};

use super::decode_crdt;

// Tree writes require HLC-stamped ops. Use the read functions for SQL-side
// access (reporting, search, etc.).

/// Return the tree as a JSON string (array of root nodes with nested children).
///
/// ```sql
/// SELECT meridian.tree_json(tree) FROM pages;
/// -- → '{"roots":[{"id":"...","value":"Home","children":[]}]}'
/// ```
#[pg_extern(schema = "meridian")]
pub fn tree_json(state: &[u8]) -> String {
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
pub fn tree_node_count(state: &[u8]) -> i64 {
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_tree_json_empty() {
        let json = crate::types::tree::tree_json(&[]);
        assert!(json.contains("roots"));
    }

    #[pg_test]
    fn test_tree_node_count_empty() {
        assert_eq!(crate::types::tree::tree_node_count(&[]), 0);
    }
}
