use pgrx::prelude::*;

use meridian_core::crdt::{registry::CrdtValue, Crdt};

use super::decode_crdt;

// RGA writes require HLC-stamped ops and conflict-resolution logic that is
// impractical to express in SQL. RGA columns should be written exclusively via
// WebSocket clients or the Meridian HTTP API.
//
// The read functions below let existing SQL tooling observe the current text
// (full-text search, reporting, etc.).

/// Return the current text content of an RGA column.
///
/// ```sql
/// SELECT meridian.rga_text(body) FROM docs;
/// ```
#[pg_extern(schema = "meridian")]
pub fn rga_text(state: &[u8]) -> String {
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

/// Return the character count (visible chars, tombstones excluded) of an RGA column.
#[pg_extern(schema = "meridian")]
pub fn rga_len(state: &[u8]) -> i64 {
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_rga_text_empty() {
        assert_eq!(crate::types::rga::rga_text(&[]), "");
    }

    #[pg_test]
    fn test_rga_len_empty() {
        assert_eq!(crate::types::rga::rga_len(&[]), 0);
    }
}
