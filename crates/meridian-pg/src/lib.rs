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
mod wal_consumer;

pgrx::pg_module_magic!();

/// Called once when the extension is loaded into a backend.
/// Registers GUCs and (if shared_preload_libraries includes meridian)
/// launches the WAL consumer background worker.
#[pg_guard]
pub extern "C" fn _PG_init() {
    wal_consumer::register_gucs();

    // Background worker registration is only valid from shared_preload_libraries.
    // Calling it at CREATE EXTENSION time is a no-op / logs a warning from pgrx.
    if unsafe { pgrx::pg_sys::process_shared_preload_libraries_in_progress } {
        wal_consumer::register_bgworker();
    }
}

#[pg_extern]
fn meridian_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
