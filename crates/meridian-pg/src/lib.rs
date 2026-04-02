// meridian-pg — Postgres extension for CRDT types and bidirectional sync.
//
// Usage:
//   CREATE EXTENSION meridian;
//   CREATE TABLE articles (id TEXT PRIMARY KEY, views meridian.gcounter);
//   UPDATE articles SET views = meridian.gcounter_increment(views, 1, 42)
//     WHERE id = 'article-1';
//   SELECT meridian.gcounter_value(views) FROM articles WHERE id = 'article-1';

use pgrx::prelude::*;

mod trigger;
mod types;

pgrx::pg_module_magic!();

#[pg_extern]
fn meridian_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
