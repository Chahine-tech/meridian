use std::time::Instant;

/// Metric name constants — all counters/gauges used across the codebase.
pub const OPS_TOTAL: &str = "meridian_ops_total";
pub const WS_CONNECTIONS_ACTIVE: &str = "meridian_ws_connections_active";
pub const WAL_ENTRIES_TOTAL: &str = "meridian_wal_entries_total";
pub const MERGE_DURATION: &str = "meridian_merge_duration_seconds";
pub const SUBSCRIPTIONS_ACTIVE: &str = "meridian_subscriptions_active";

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

/// Adjust the active WebSocket connections and subscriptions gauges.
pub fn ws_connected() {
    metrics::gauge!(WS_CONNECTIONS_ACTIVE).increment(1.0);
    metrics::gauge!(SUBSCRIPTIONS_ACTIVE).increment(1.0);
}

pub fn ws_disconnected() {
    metrics::gauge!(WS_CONNECTIONS_ACTIVE).decrement(1.0);
    metrics::gauge!(SUBSCRIPTIONS_ACTIVE).decrement(1.0);
}

/// Record a WAL entry written.
pub fn record_wal_entry() {
    metrics::counter!(WAL_ENTRIES_TOTAL).increment(1);
}

/// Record the duration of a merge/apply operation (seconds).
pub fn record_merge_duration(ns: &str, elapsed: std::time::Duration) {
    metrics::histogram!(
        MERGE_DURATION,
        LABEL_NS => ns.to_owned(),
    )
    .record(elapsed.as_secs_f64());
}

/// Helper: returns an `Instant` to pass to `record_merge_duration` after the op.
pub fn merge_start() -> Instant {
    Instant::now()
}

/// Adjust the subscriptions (active WS receivers) gauge by `delta`.
pub fn subscriptions_delta(delta: f64) {
    metrics::gauge!(SUBSCRIPTIONS_ACTIVE).increment(delta);
}
