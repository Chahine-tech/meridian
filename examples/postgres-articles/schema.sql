-- schema.sql — Articles table with CRDT columns + Meridian sync trigger
--
-- Run: psql $DATABASE_URL -f schema.sql

CREATE EXTENSION IF NOT EXISTS meridian_pg;

CREATE TABLE IF NOT EXISTS articles (
    id       TEXT PRIMARY KEY,
    views    BYTEA,   -- GCounter  : grow-only view counter
    likes    BYTEA,   -- PNCounter : up/down vote counter
    tags     BYTEA,   -- ORSet     : tag set (add-wins)
    headline BYTEA    -- LwwRegister : last-write-wins title
    -- body is managed exclusively via the SDK (RGA collaborative text)
);

-- Sync trigger: fires AFTER INSERT OR UPDATE, emits one pg_notify per changed column.
-- crdt_id format: "article:<id>:<col>"  e.g. "article:rust-intro:views"
CREATE OR REPLACE TRIGGER meridian_sync
AFTER INSERT OR UPDATE ON articles
FOR EACH ROW EXECUTE FUNCTION meridian.notify_trigger(
    'articles',   -- namespace
    'article',    -- crdt_id prefix
    'id',         -- primary-key column
    'views',      -- CRDT columns to watch
    'likes',
    'tags',
    'headline'
);
