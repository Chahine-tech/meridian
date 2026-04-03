// meridian_notify_trigger — generic row-level trigger that fires pg_notify
// whenever a CRDT column changes.
//
// Usage (SQL):
//   CREATE TRIGGER sync_views
//   AFTER INSERT OR UPDATE OF views ON articles
//   FOR EACH ROW
//   EXECUTE FUNCTION meridian.notify_trigger('my-namespace', 'gc', 'id', 'views');
//
// Arguments (TG_ARGV):
//   $1 — Meridian namespace
//   $2 — CRDT id prefix (e.g. "gc" → crdt_id = "gc:<row_id>")
//   $3 — primary-key column name (defaults to "id")
//   $4 — CRDT column name        (defaults to "views")
//
// Payload (JSON, on channel "meridian_ops"):
//   { "ns": "my-namespace", "crdt_id": "gc:42", "op_bytes": "<base64>" }
//
// Large deltas (> 7 800 bytes after base64) are dropped with a WARNING log.

use pgrx::prelude::*;

const NOTIFY_CHANNEL: &str = "meridian_ops";

/// Base64-encode bytes using the standard alphabet (no line breaks).
/// Avoids depending on pgrx::encode_base64 which is not part of pgrx 0.12's
/// public API — uses the `base64` crate directly instead.
fn base64_encode(bytes: &[u8]) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD.encode(bytes)
}
const MAX_PAYLOAD_BYTES: usize = 7_800;

/// Row-level AFTER trigger. Returns the NEW row unchanged.
#[pg_trigger]
fn notify_trigger<'a>(
    trigger: &'a pgrx::PgTrigger<'a>,
) -> Result<
    Option<PgHeapTuple<'a, impl WhoAllocated>>,
    pgrx::PgTriggerError,
> {
    // Only fire on INSERT / UPDATE — DELETE has no delta to broadcast.
    if trigger.event().fired_by_delete() {
        return Ok(trigger.old().map(|t| t.into_owned()));
    }

    let new_row = match trigger.new() {
        Some(r) => r,
        None => return Ok(None),
    };

    let args    = trigger.extra_args();
    let namespace = args.get(0).copied().unwrap_or("default");
    let id_prefix = args.get(1).copied().unwrap_or("crdt");
    let pk_col    = args.get(2).copied().unwrap_or("id");
    let crdt_col  = args.get(3).copied().unwrap_or("data");

    // Read primary key — try TEXT, then BIGINT, INTEGER, UUID in order.
    let row_id: Option<String> = new_row
        .get_by_name::<String>(pk_col).unwrap_or(None)
        .or_else(|| new_row.get_by_name::<i64>(pk_col).unwrap_or(None).map(|n| n.to_string()))
        .or_else(|| new_row.get_by_name::<i32>(pk_col).unwrap_or(None).map(|n| n.to_string()))
        .or_else(|| new_row.get_by_name::<uuid::Uuid>(pk_col).unwrap_or(None).map(|u| u.to_string()));

    if row_id.is_none() {
        pgrx::warning!(
            "meridian_notify_trigger: could not read primary key column {:?} \
             (ns={}, prefix={}) — skipping notify",
            pk_col, namespace, id_prefix
        );
    }

    let crdt_bytes: Option<Vec<u8>> = new_row.get_by_name::<Vec<u8>>(crdt_col).unwrap_or(None);

    if crdt_bytes.is_none() {
        pgrx::warning!(
            "meridian_notify_trigger: CRDT column {:?} is NULL or not BYTEA \
             (ns={}, prefix={}) — skipping notify",
            crdt_col, namespace, id_prefix
        );
    }

    if let (Some(row_id), Some(crdt_bytes)) = (row_id, crdt_bytes) {
        let crdt_id = format!("{id_prefix}:{row_id}");

        // kind="state" tells the server this is a full CrdtValue snapshot,
        // not a node-to-node delta.  The server merges it and derives the
        // typed delta before forwarding to WebSocket clients.
        let payload = serde_json::json!({
            "kind":    "state",
            "ns":      namespace,
            "crdt_id": crdt_id,
            "d":       base64_encode(&crdt_bytes),
        })
        .to_string();

        if payload.len() <= MAX_PAYLOAD_BYTES {
            // Use parameterised SPI query — no string interpolation of user data.
            if let Err(e) = Spi::run_with_args(
                "SELECT pg_notify($1, $2)",
                Some(vec![
                    (PgOid::BuiltIn(PgBuiltInOids::TEXTOID), NOTIFY_CHANNEL.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::TEXTOID), payload.into_datum()),
                ]),
            ) {
                pgrx::warning!(
                    "meridian_notify_trigger: pg_notify failed for crdt_id={:?}: {e}",
                    crdt_id
                );
            }
        } else {
            pgrx::warning!(
                "meridian_notify_trigger: payload too large ({} bytes > {} limit) for \
                 crdt_id={:?} — notify skipped. Use logical replication for large documents.",
                payload.len(), MAX_PAYLOAD_BYTES, crdt_id
            );
        }
    }

    Ok(trigger.new().map(|t| t.into_owned()))
}
