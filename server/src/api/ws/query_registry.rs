use std::collections::HashMap;

use meridian_core::protocol::LiveQueryPayload;

use crate::crdt::registry::CrdtType;

/// Per-connection registry of live query subscriptions.
///
/// Stored inside the WebSocket handler task — no locks needed as it is
/// accessed from a single async task.
#[derive(Default)]
pub struct QueryRegistry {
    queries: HashMap<String, LiveQueryPayload>,
}

impl QueryRegistry {
    pub fn insert(&mut self, query_id: String, payload: LiveQueryPayload) {
        self.queries.insert(query_id, payload);
    }

    pub fn remove(&mut self, query_id: &str) {
        self.queries.remove(query_id);
    }

    /// Iterate over registered queries that could be affected by a change to
    /// `crdt_id` (without namespace prefix) of the given `crdt_type`.
    ///
    /// A query matches when:
    /// - its `from` glob matches `crdt_id`, AND
    /// - it has no explicit type filter, OR the type filter matches `crdt_type`.
    pub fn matching<'a>(
        &'a self,
        crdt_id: &'a str,
        crdt_type: CrdtType,
    ) -> impl Iterator<Item = (&'a str, &'a LiveQueryPayload)> {
        self.queries.iter().filter_map(move |(id, payload)| {
            if !crate::auth::glob_match(&payload.from, crdt_id) {
                return None;
            }
            // If the query has an explicit type filter, skip if it doesn't match.
            if payload.crdt_type.as_deref().is_some_and(|s| s.parse::<CrdtType>().ok() != Some(crdt_type)) {
                return None;
            }
            Some((id.as_str(), payload))
        })
    }

    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }
}
