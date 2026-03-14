/// Metric name constants — all counters/gauges used across the codebase.
pub const OPS_TOTAL: &str = "meridian_ops_total";
pub const WS_CONNECTIONS_ACTIVE: &str = "meridian_ws_connections_active";
pub const WAL_ENTRIES_TOTAL: &str = "meridian_wal_entries_total";

/// Label keys.
pub const LABEL_NS: &str = "ns";
pub const LABEL_CRDT_ID: &str = "crdt_id";
pub const LABEL_OP_TYPE: &str = "op_type";

/// Increment the ops counter for a given namespace, CRDT, and op type.
pub fn record_op(ns: &str, crdt_id: &str, op_type: &str) {
    metrics::counter!(
        OPS_TOTAL,
        LABEL_NS => ns.to_owned(),
        LABEL_CRDT_ID => crdt_id.to_owned(),
        LABEL_OP_TYPE => op_type.to_owned(),
    )
    .increment(1);
}

/// Adjust the active WebSocket connections gauge.
pub fn ws_connected() {
    metrics::gauge!(WS_CONNECTIONS_ACTIVE).increment(1.0);
}

pub fn ws_disconnected() {
    metrics::gauge!(WS_CONNECTIONS_ACTIVE).decrement(1.0);
}

/// Record a WAL entry written.
pub fn record_wal_entry() {
    metrics::counter!(WAL_ENTRIES_TOTAL).increment(1);
}
