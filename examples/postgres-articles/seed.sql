-- seed.sql — Insert initial articles with CRDT state
--
-- Run: psql $DATABASE_URL -f seed.sql

INSERT INTO articles (id, views, likes, tags, headline) VALUES
(
    'rust-intro',
    meridian.gcounter_increment(NULL, 0, 1),
    meridian.pncounter_increment(NULL, 0, 1),
    meridian.orset_add(NULL, '"rust"', 1, 1),
    meridian.lww_set(NULL, '"Introduction to Rust"', EXTRACT(EPOCH FROM now())::bigint * 1000, 1)
),
(
    'crdt-explained',
    meridian.gcounter_increment(NULL, 0, 1),
    meridian.pncounter_increment(NULL, 0, 1),
    meridian.orset_add(
        meridian.orset_add(NULL, '"crdt"', 1, 1),
        '"distributed"', 1, 2
    ),
    meridian.lww_set(NULL, '"CRDTs Explained"', EXTRACT(EPOCH FROM now())::bigint * 1000, 1)
),
(
    'postgres-tips',
    meridian.gcounter_increment(NULL, 0, 1),
    meridian.pncounter_increment(NULL, 0, 1),
    meridian.orset_add(NULL, '"postgres"', 1, 1),
    meridian.lww_set(NULL, '"10 Postgres Tips"', EXTRACT(EPOCH FROM now())::bigint * 1000, 1)
)
ON CONFLICT (id) DO NOTHING;
