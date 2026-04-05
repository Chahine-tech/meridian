// meridian_notify_trigger — row-level trigger that fires pg_notify on CRDT column changes.
//
// Usage (SQL):
//   CREATE TRIGGER sync_articles
//   AFTER INSERT OR UPDATE OF views, likes ON articles
//   FOR EACH ROW
//   EXECUTE FUNCTION meridian.notify_trigger(
//       'articles-ns',  -- $1: Meridian namespace
//       'gc',           -- $2: crdt_id prefix  → "gc:<pk>"
//       'id',           -- $3: primary-key column
//       'views',        -- $4..N: CRDT columns to watch (variadiac)
//       'likes'
//   );
//
// Payload format (channel "meridian_ops"):
//   { "kind": "state", "ns": "<ns>", "crdt_id": "<prefix>:<pk>:<col>", "d": "<base64 msgpack>" }
//
// One pg_notify is emitted per changed CRDT column per row.
// Payloads > 7 800 bytes are skipped with a WARNING (pg_notify limit ~8 KB).
// Use logical replication (meridian.wal_consumer) for large RGA / Tree documents.

use pgrx::prelude::*;

const NOTIFY_CHANNEL: &str = "meridian_ops";
const MAX_PAYLOAD_BYTES: usize = 7_800;

fn base64_encode(bytes: &[u8]) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD.encode(bytes)
}

fn run_notify(channel: &str, payload: &str) -> Result<(), pgrx::spi::SpiError> {
    let escaped = payload.replace('\'', "''");
    Spi::run(&format!("SELECT pg_notify('{channel}', '{escaped}')"))
}

// ---------------------------------------------------------------------------
// Trigger function (placed in the meridian schema)
// ---------------------------------------------------------------------------

#[pg_schema]
mod meridian {
    use pgrx::prelude::*;

    /// Row-level AFTER trigger. Returns the NEW row unchanged.
    #[pg_trigger]
    pub fn notify_trigger<'a>(
        trigger: &'a pgrx::PgTrigger<'a>,
    ) -> Result<
        Option<PgHeapTuple<'a, impl WhoAllocated>>,
        pgrx::PgTriggerError,
    > {
        if trigger.event().fired_by_delete() {
            return Ok(trigger.old().map(|t| t.into_owned()));
        }

        let new_row = match trigger.new() {
            Some(r) => r,
            None => return Ok(None),
        };

        let args: Vec<String> = trigger.extra_args().unwrap_or_default();

        // TG_ARGV: $1=namespace  $2=prefix  $3=pk_col  $4..N=crdt_cols
        let namespace = args.get(0).map(String::as_str).unwrap_or("default");
        let id_prefix = args.get(1).map(String::as_str).unwrap_or("crdt");
        let pk_col    = args.get(2).map(String::as_str).unwrap_or("id");
        let crdt_cols: Vec<&str> = if args.len() > 3 {
            args[3..].iter().map(String::as_str).collect()
        } else {
            vec!["data"]
        };

        // Read primary key — try TEXT, BIGINT, INTEGER in order.
        let row_id: Option<String> = new_row
            .get_by_name::<String>(pk_col).unwrap_or(None)
            .or_else(|| new_row.get_by_name::<i64>(pk_col).unwrap_or(None).map(|n| n.to_string()))
            .or_else(|| new_row.get_by_name::<i32>(pk_col).unwrap_or(None).map(|n| n.to_string()));

        let row_id = match row_id {
            Some(id) => id,
            None => {
                pgrx::warning!(
                    "meridian_notify_trigger: could not read pk column {:?} (ns={}) — skipping",
                    pk_col, namespace
                );
                return Ok(trigger.new().map(|t| t.into_owned()));
            }
        };

        for col in &crdt_cols {
            let new_bytes: Option<Vec<u8>> = new_row
                .get_by_name::<&[u8]>(col)
                .unwrap_or(None)
                .map(|b| b.to_vec());

            let new_bytes = match new_bytes {
                Some(b) if !b.is_empty() => b,
                _ => continue,
            };

            // Skip unchanged columns on UPDATE
            if trigger.event().fired_by_update() {
                if let Some(old_row) = trigger.old() {
                    let old_bytes: Vec<u8> = old_row
                        .get_by_name::<&[u8]>(col)
                        .unwrap_or(None)
                        .map(|b| b.to_vec())
                        .unwrap_or_default();
                    if old_bytes == new_bytes {
                        continue;
                    }
                }
            }

            let crdt_id = format!("{id_prefix}:{row_id}:{col}");
            let payload = serde_json::json!({
                "kind":    "state",
                "ns":      namespace,
                "crdt_id": crdt_id,
                "d":       crate::trigger::base64_encode(&new_bytes),
            })
            .to_string();

            if payload.len() > crate::trigger::MAX_PAYLOAD_BYTES {
                pgrx::warning!(
                    "meridian_notify_trigger: payload too large ({} bytes > {} limit) for \
                     crdt_id={:?} — skipped. Use logical replication for large documents.",
                    payload.len(), crate::trigger::MAX_PAYLOAD_BYTES, crdt_id
                );
                continue;
            }

            if let Err(e) = crate::trigger::run_notify(crate::trigger::NOTIFY_CHANNEL, &payload) {
                pgrx::warning!(
                    "meridian_notify_trigger: pg_notify failed for crdt_id={:?}: {e}",
                    crdt_id
                );
            }
        }

        Ok(trigger.new().map(|t| t.into_owned()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn setup_trigger_table() {
        Spi::run(
            "CREATE TEMP TABLE articles (
                id    TEXT PRIMARY KEY,
                views BYTEA,
                likes BYTEA
            )",
        ).unwrap();
        Spi::run("INSERT INTO articles (id) VALUES ('a1')").unwrap();
        Spi::run(
            "CREATE TRIGGER meridian_sync
             AFTER INSERT OR UPDATE ON articles
             FOR EACH ROW EXECUTE FUNCTION meridian.notify_trigger(
                 'test-ns', 'gc', 'id', 'views', 'likes'
             )",
        ).unwrap();
        Spi::run("LISTEN meridian_ops").unwrap();
    }

    fn bytes_to_hex_literal(b: &[u8]) -> String {
        let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
        format!("'\\x{hex}'::bytea")
    }

    /// Trigger fires after INSERT without error and row is intact.
    #[pg_test]
    fn test_trigger_insert_no_error() {
        setup_trigger_table();
        let views = crate::types::gcounter::gcounter_increment(None, 5, 1);
        let lit = bytes_to_hex_literal(&views);
        Spi::run(&format!("INSERT INTO articles (id, views) VALUES ('a2', {lit})")).unwrap();
        let total: i64 = Spi::get_one(
            "SELECT meridian.gcounter_value(views) FROM articles WHERE id = 'a2'",
        ).unwrap().unwrap();
        assert_eq!(total, 5);
    }

    /// Trigger fires after UPDATE and doesn't corrupt the row.
    #[pg_test]
    fn test_trigger_update_no_error() {
        setup_trigger_table();
        let views = crate::types::gcounter::gcounter_increment(None, 10, 1);
        let lit = bytes_to_hex_literal(&views);
        Spi::run(&format!("UPDATE articles SET views = {lit} WHERE id = 'a1'")).unwrap();
        let total: i64 = Spi::get_one(
            "SELECT meridian.gcounter_value(views) FROM articles WHERE id = 'a1'",
        ).unwrap().unwrap();
        assert_eq!(total, 10);
    }

    /// Trigger handles multiple CRDT columns in one UPDATE.
    #[pg_test]
    fn test_trigger_multi_column_update() {
        setup_trigger_table();
        let views = crate::types::gcounter::gcounter_increment(None, 3, 1);
        let likes = crate::types::pncounter::pncounter_increment(None, 7, 1);
        let vlit = bytes_to_hex_literal(&views);
        let llit = bytes_to_hex_literal(&likes);
        Spi::run(&format!(
            "UPDATE articles SET views = {vlit}, likes = {llit} WHERE id = 'a1'"
        )).unwrap();
        let v: i64 = Spi::get_one(
            "SELECT meridian.gcounter_value(views) FROM articles WHERE id = 'a1'",
        ).unwrap().unwrap();
        let l: i64 = Spi::get_one(
            "SELECT meridian.pncounter_value(likes) FROM articles WHERE id = 'a1'",
        ).unwrap().unwrap();
        assert_eq!(v, 3);
        assert_eq!(l, 7);
    }
}
