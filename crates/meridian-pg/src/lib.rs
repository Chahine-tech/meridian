// meridian-pg — Postgres extension for CRDT types and real-time sync.
//
// Usage:
//   CREATE EXTENSION meridian_pg;
//   CREATE TABLE articles (id TEXT PRIMARY KEY, views BYTEA, likes BYTEA);
//   UPDATE articles SET views = meridian.gcounter_increment(views, 1, 42)
//     WHERE id = 'article-1';
//   SELECT meridian.gcounter_value(views) FROM articles WHERE id = 'article-1';

use pgrx::prelude::*;

mod trigger;
mod types;

pgrx::pg_module_magic!();

#[pg_schema]
mod meridian {
    use pgrx::prelude::*;

    #[pg_extern]
    pub fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}

#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
