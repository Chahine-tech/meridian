/**
 * Helpers for mapping Postgres table rows to Meridian CRDT identifiers.
 *
 * When using `meridian-pg` (the Postgres extension), each CRDT column in a
 * table maps to a Meridian `crdt_id` of the form `<prefix>:<row_id>`.
 * The trigger is configured with the same prefix:
 *
 * ```sql
 * CREATE TRIGGER sync_views
 * AFTER INSERT OR UPDATE OF views ON articles
 * FOR EACH ROW
 * EXECUTE FUNCTION meridian.notify_trigger('my-ns', 'gc', 'id', 'views');
 * --                                                   ^^^^
 * --                                           this prefix
 * ```
 *
 * Use `pgCrdtKey` to build the matching crdt_id on the client side:
 *
 * ```ts
 * import { pgCrdtKey } from "meridian-sdk/pg";
 *
 * const views = client.gcounter(pgCrdtKey("gc", "article-1"));
 * // → gcounter subscribed to crdt_id = "gc:article-1"
 * ```
 */

/**
 * Build the CRDT identifier that `meridian-pg` uses for a given row.
 *
 * @param prefix  The `crdt_id` prefix set in the trigger (2nd TG_ARGV argument).
 * @param rowId   The primary key value of the row.
 * @returns       `"<prefix>:<rowId>"` — the crdt_id used by the Meridian server.
 *
 * @example
 * ```ts
 * // Trigger: EXECUTE FUNCTION meridian.notify_trigger('shop', 'gc', 'id', 'views')
 * const views = client.gcounter(pgCrdtKey("gc", productId));
 * ```
 */
export function pgCrdtKey(prefix: string, rowId: string | number): string {
  return `${prefix}:${rowId}`;
}

/**
 * Build multiple CRDT keys for a row that has several CRDT columns.
 *
 * @param rowId    The primary key value of the row.
 * @param columns  Map of column name → crdt_id prefix (as configured in the trigger).
 * @returns        Map of column name → crdt_id.
 *
 * @example
 * ```ts
 * const keys = pgCrdtKeys("article-1", { views: "gc", likes: "pn", tags: "or" });
 * const views = client.gcounter(keys.views);
 * const likes = client.pncounter(keys.likes);
 * const tags  = client.orset(keys.tags);
 * ```
 */
export function pgCrdtKeys<K extends string>(
  rowId: string | number,
  columns: Record<K, string>,
): Record<K, string> {
  return Object.fromEntries(
    Object.entries(columns).map(([col, prefix]) => [col, pgCrdtKey(prefix as string, rowId)]),
  ) as Record<K, string>;
}
