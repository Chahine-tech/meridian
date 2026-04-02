-- meridian--0.1.0.sql
-- Extension DDL scaffold.  pgrx generates most function definitions
-- automatically from #[pg_extern] annotations.  This file handles:
--   • Schema creation
--   • Trigger function wrapper
--   • Aggregate definitions (gcounter_sum, pncounter_sum)
--   • Convenience views / helper comments

-- Namespace for all Meridian objects.
CREATE SCHEMA IF NOT EXISTS meridian;

-- -------------------------------------------------------------------------
-- Aggregate: gcounter_merge_agg — merges all GCounter BYTEA states
--   (intermediate result, still BYTEA)
-- Aggregate: gcounter_total — returns the final BIGINT total directly
--
-- SELECT meridian.gcounter_total(views) FROM articles;
-- -------------------------------------------------------------------------
CREATE AGGREGATE meridian.gcounter_merge_agg(bytea) (
    SFUNC    = meridian.gcounter_merge,
    STYPE    = bytea,
    INITCOND = ''
);

CREATE AGGREGATE meridian.gcounter_total(bytea) (
    SFUNC    = meridian.gcounter_merge,
    STYPE    = bytea,
    FINALFUNC = meridian.gcounter_value,
    INITCOND = ''
);

-- -------------------------------------------------------------------------
-- Trigger wrapper — exposes the Rust #[pg_trigger] as a SQL trigger function.
--
-- Usage:
--   CREATE TRIGGER sync_views
--   AFTER INSERT OR UPDATE OF views ON articles
--   FOR EACH ROW
--   EXECUTE FUNCTION meridian.notify_trigger('my-ns', 'gc', 'id', 'views');
-- -------------------------------------------------------------------------
-- (pgrx automatically generates the CREATE FUNCTION for notify_trigger from
-- the #[pg_trigger] annotation — no manual declaration needed here.)

-- -------------------------------------------------------------------------
-- Example: collaborative articles table
-- (commented out — uncomment to test during development)
-- -------------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS meridian_example_articles (
--     id      TEXT PRIMARY KEY,
--     title   TEXT NOT NULL DEFAULT '',
--     views   BYTEA,   -- gcounter
--     likes   BYTEA,   -- pncounter
--     tags    BYTEA,   -- orset
--     summary BYTEA    -- lwwregister
-- );
--
-- CREATE TRIGGER meridian_sync_articles
-- AFTER INSERT OR UPDATE ON meridian_example_articles
-- FOR EACH ROW
-- EXECUTE FUNCTION meridian.notify_trigger(
--     'articles-ns',  -- namespace
--     'gc',           -- crdt_id prefix
--     'id',           -- pk column
--     'views'         -- crdt column
-- );
