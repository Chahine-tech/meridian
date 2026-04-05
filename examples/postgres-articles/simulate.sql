-- simulate.sql — Simulate concurrent writes from SQL
--
-- Run this while client.ts is running to see live updates in the terminal.
-- Run: psql $DATABASE_URL -f simulate.sql

\echo '--- Incrementing views on rust-intro (+3) ---'
UPDATE articles
SET views = meridian.gcounter_increment(views, 3, 42)
WHERE id = 'rust-intro';

\echo '--- Liking crdt-explained (+1) and rust-intro (-1) ---'
UPDATE articles
SET likes = meridian.pncounter_increment(likes, 1, 42)
WHERE id = 'crdt-explained';

UPDATE articles
SET likes = meridian.pncounter_decrement(likes, 1, 99)
WHERE id = 'rust-intro';

\echo '--- Adding tags to postgres-tips ---'
UPDATE articles
SET tags = meridian.orset_add(
    meridian.orset_add(tags, '"performance"', 1, 10),
    '"indexing"', 1, 11
)
WHERE id = 'postgres-tips';

\echo '--- Updating headline on crdt-explained (last-write-wins) ---'
UPDATE articles
SET headline = meridian.lww_set(
    headline,
    '"CRDTs Explained — A Practical Guide"',
    EXTRACT(EPOCH FROM now())::bigint * 1000,
    42
)
WHERE id = 'crdt-explained';

\echo '--- Done. Check your client.ts terminal for live updates. ---'
