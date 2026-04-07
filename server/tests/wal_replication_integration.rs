// Integration tests for the WAL logical replication consumer.
//
// Requires a local Postgres instance with wal_level=logical and
// replication privileges for the connecting user.
//
// Run with:
//   cargo test --features pg-sync --test wal_replication_integration
//
// The tests connect to MERIDIAN_TEST_PG_URL (defaults to the local user's
// postgres DB). They are automatically skipped if wal_level != logical.

#![cfg(feature = "pg-sync")]

use std::{
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

// Each test spawns a WAL consumer that opens replication connections.
// Running them in parallel exhausts Postgres's max_wal_senders and causes
// flaky timeouts. Serialize with a process-wide async mutex so the guard
// can be held across .await points without triggering clippy::await_holding_lock.
static SERIAL: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
async fn serial_lock() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL.get_or_init(|| tokio::sync::Mutex::new(())).lock().await
}

use meridian_server::cluster::pg_transport::PgStateApplier;
use tokio_postgres::NoTls;


fn base_url() -> String {
    std::env::var("MERIDIAN_TEST_PG_URL").unwrap_or_else(|_| {
        let user = std::env::var("USER").unwrap_or_else(|_| "postgres".into());
        format!("postgresql://{user}@localhost/postgres")
    })
}

/// Verify wal_level=logical on the server. Returns false to skip if not set.
async fn check_wal_level(base_url: &str) -> bool {
    let Ok((client, conn)) = tokio_postgres::connect(base_url, NoTls).await else {
        eprintln!("WAL tests: cannot connect to postgres — skipping");
        return false;
    };
    tokio::spawn(conn);
    let Ok(row) = client.query_one("SHOW wal_level", &[]).await else {
        return false;
    };
    let level: &str = row.get(0);
    if level != "logical" {
        eprintln!("WAL tests: wal_level={level}, need logical — skipping");
        return false;
    }
    true
}

struct TestDb {
    base_url: String,
    pub db_name: String,
    pub test_url: String,
    pub client: tokio_postgres::Client,
    pub slot: String,
    pub publication: String,
    consumer_task: Option<tokio::task::JoinHandle<()>>,
}

impl TestDb {
    /// Create an isolated test database with a unique name.
    async fn new(prefix: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let base_url = base_url();
        let (admin, conn) = tokio_postgres::connect(&base_url, NoTls).await?;
        tokio::spawn(conn);

        // UUID-quality uniqueness: pid + nanos
        let unique = format!(
            "{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos()
        );
        let db_name = format!("meridian_wal_{prefix}_{unique}");
        admin
            .simple_query(&format!("CREATE DATABASE \"{db_name}\""))
            .await?;

        let test_url = if base_url.contains("/postgres") {
            base_url.replacen("/postgres", &format!("/{db_name}"), 1)
        } else {
            format!("{base_url}/{db_name}")
        };

        let (client, conn) = tokio_postgres::connect(&test_url, NoTls).await?;
        tokio::spawn(conn);

        let slot = format!("slot_{unique}");
        let publication = format!("pub_{unique}");

        Ok(Self { base_url, db_name, test_url, client, slot, publication, consumer_task: None })
    }

    /// Spawn the WAL consumer task for this database and store the handle.
    fn spawn_consumer(&mut self, applier: Arc<impl PgStateApplier>) {
        let url = self.test_url.clone();
        let slot = self.slot.clone();
        let pub_ = self.publication.clone();
        let handle = tokio::spawn(async move {
            meridian_server::cluster::wal_replication::run(url, slot, pub_, applier).await;
        });
        self.consumer_task = Some(handle);
    }

    /// Drop the test database (best-effort cleanup).
    async fn cleanup(mut self) {
        // Abort the consumer task so it releases the replication slot.
        if let Some(task) = self.consumer_task.take() {
            task.abort();
            let _ = task.await;
        }
        // Brief pause to let Postgres mark the slot as inactive.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let Ok((admin, conn)) = tokio_postgres::connect(&self.base_url, NoTls).await else {
            return;
        };
        tokio::spawn(conn);

        // Drop the replication slot (required before DROP DATABASE).
        let _ = admin
            .simple_query(&format!(
                "SELECT pg_drop_replication_slot(slot_name) \
                 FROM pg_replication_slots WHERE slot_name = '{}'",
                self.slot
            ))
            .await;

        let _ = admin
            .simple_query(&format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE datname = '{}' AND pid <> pg_backend_pid()",
                self.db_name
            ))
            .await;

        let _ = admin
            .simple_query(&format!("DROP DATABASE IF EXISTS \"{}\"", self.db_name))
            .await;
    }

    /// Wait until `predicate` returns true, or panic after `timeout`.
    async fn wait_for(
        &self,
        timeout: Duration,
        mut predicate: impl FnMut() -> bool,
        msg: &str,
    ) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if predicate() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timeout waiting for: {msg}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}


type CallLog = Arc<Mutex<Vec<(String, String, Vec<u8>)>>>;

#[derive(Clone)]
struct RecordingApplier {
    calls: CallLog,
}

impl RecordingApplier {
    fn new() -> Self {
        Self { calls: Arc::new(Mutex::new(Vec::new())) }
    }

    fn recorded(&self) -> Vec<(String, String, Vec<u8>)> {
        self.calls.lock().unwrap().clone()
    }

    fn calls_for(&self, crdt_id: &str) -> Vec<Vec<u8>> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, id, _)| id == crdt_id)
            .map(|(_, _, b)| b.clone())
            .collect()
    }
}

impl PgStateApplier for RecordingApplier {
    async fn merge_pg_state(
        &self,
        namespace: String,
        crdt_id: String,
        state_bytes: Vec<u8>,
    ) -> Option<Vec<u8>> {
        self.calls.lock().unwrap().push((namespace, crdt_id, state_bytes));
        None
    }
}


#[tokio::test]
async fn test_insert_small_bytea() {
    let url = base_url();
    if !check_wal_level(&url).await {
        return;
    }
    let _serial = serial_lock().await;

    let mut db = TestDb::new("insert").await.expect("setup");
    db.client
        .simple_query("CREATE TABLE docs (id TEXT PRIMARY KEY, data BYTEA)")
        .await
        .expect("CREATE TABLE");

    let applier = Arc::new(RecordingApplier::new());
    db.spawn_consumer(Arc::clone(&applier));
    tokio::time::sleep(Duration::from_millis(600)).await;

    let payload: Vec<u8> = vec![0x01, 0x02, 0x03, 0x04];
    db.client
        .execute("INSERT INTO docs (id, data) VALUES ($1, $2)", &[&"doc-1", &payload])
        .await
        .expect("INSERT");

    let a = applier.clone();
    db.wait_for(
        Duration::from_secs(5),
        move || !a.calls_for("docs:doc-1:data").is_empty(),
        "INSERT to be delivered",
    )
    .await;

    let calls = applier.calls_for("docs:doc-1:data");
    assert_eq!(calls[0], payload, "BYTEA payload mismatch after INSERT");

    db.cleanup().await;
}

#[tokio::test]
async fn test_update_delivers_new_value() {
    let url = base_url();
    if !check_wal_level(&url).await {
        return;
    }
    let _serial = serial_lock().await;

    let mut db = TestDb::new("update").await.expect("setup");
    db.client
        .simple_query("CREATE TABLE docs (id TEXT PRIMARY KEY, state BYTEA)")
        .await
        .expect("CREATE TABLE");

    let applier = Arc::new(RecordingApplier::new());
    db.spawn_consumer(Arc::clone(&applier));
    tokio::time::sleep(Duration::from_millis(600)).await;

    let v1: Vec<u8> = vec![0xAA, 0xBB];
    let v2: Vec<u8> = vec![0xCC, 0xDD, 0xEE];
    db.client
        .execute("INSERT INTO docs (id, state) VALUES ($1, $2)", &[&"doc-1", &v1])
        .await
        .expect("INSERT");
    db.client
        .execute("UPDATE docs SET state = $1 WHERE id = $2", &[&v2, &"doc-1"])
        .await
        .expect("UPDATE");

    let a = applier.clone();
    let v2_clone = v2.clone();
    db.wait_for(
        Duration::from_secs(5),
        move || a.calls_for("docs:doc-1:state").contains(&v2_clone),
        "UPDATE value to be delivered",
    )
    .await;

    let calls = applier.calls_for("docs:doc-1:state");
    assert!(calls.contains(&v2), "v2 not in calls: {calls:?}");

    db.cleanup().await;
}

#[tokio::test]
async fn test_large_payload_over_8kb() {
    let url = base_url();
    if !check_wal_level(&url).await {
        return;
    }
    let _serial = serial_lock().await;

    let mut db = TestDb::new("large").await.expect("setup");
    db.client
        .simple_query("CREATE TABLE docs (id TEXT PRIMARY KEY, body BYTEA)")
        .await
        .expect("CREATE TABLE");

    let applier = Arc::new(RecordingApplier::new());
    db.spawn_consumer(Arc::clone(&applier));
    tokio::time::sleep(Duration::from_millis(600)).await;

    // 10 KB payload — well above pg_notify's 8 KB limit
    let large: Vec<u8> = (0u32..10_240).map(|i| (i % 251) as u8).collect();
    db.client
        .execute("INSERT INTO docs (id, body) VALUES ($1, $2)", &[&"big-1", &large])
        .await
        .expect("INSERT large");

    let a = applier.clone();
    let expected_len = large.len();
    db.wait_for(
        Duration::from_secs(8),
        move || {
            a.calls_for("docs:big-1:body")
                .iter()
                .any(|b| b.len() == expected_len)
        },
        "large payload to be delivered",
    )
    .await;

    let calls = applier.calls_for("docs:big-1:body");
    assert_eq!(calls[0], large, "large BYTEA payload mismatch");

    db.cleanup().await;
}

#[tokio::test]
async fn test_multiple_bytea_columns() {
    let url = base_url();
    if !check_wal_level(&url).await {
        return;
    }
    let _serial = serial_lock().await;

    let mut db = TestDb::new("multicol").await.expect("setup");
    db.client
        .simple_query(
            "CREATE TABLE items (
                id    TEXT PRIMARY KEY,
                views BYTEA,
                likes BYTEA
            )",
        )
        .await
        .expect("CREATE TABLE");

    let applier = Arc::new(RecordingApplier::new());
    db.spawn_consumer(Arc::clone(&applier));
    tokio::time::sleep(Duration::from_millis(600)).await;

    let views_bytes: Vec<u8> = vec![0x01, 0x00];
    let likes_bytes: Vec<u8> = vec![0x02, 0x00];
    db.client
        .execute(
            "INSERT INTO items (id, views, likes) VALUES ($1, $2, $3)",
            &[&"item-1", &views_bytes, &likes_bytes],
        )
        .await
        .expect("INSERT");

    let a = applier.clone();
    db.wait_for(
        Duration::from_secs(5),
        move || {
            !a.calls_for("items:item-1:views").is_empty()
                && !a.calls_for("items:item-1:likes").is_empty()
        },
        "both columns to be delivered",
    )
    .await;

    assert_eq!(applier.calls_for("items:item-1:views")[0], views_bytes);
    assert_eq!(applier.calls_for("items:item-1:likes")[0], likes_bytes);

    db.cleanup().await;
}

#[tokio::test]
async fn test_real_gcounter_crdt_value() {
    use meridian_server::crdt::{Crdt, gcounter::GCounterOp, registry::CrdtValue};

    let url = base_url();
    if !check_wal_level(&url).await {
        return;
    }
    let _serial = serial_lock().await;

    let mut db = TestDb::new("crdt").await.expect("setup");
    db.client
        .simple_query("CREATE TABLE counters (id TEXT PRIMARY KEY, value BYTEA)")
        .await
        .expect("CREATE TABLE");

    let applier = Arc::new(RecordingApplier::new());
    db.spawn_consumer(Arc::clone(&applier));
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Build a real GCounter and msgpack-encode it
    let mut counter = CrdtValue::new(meridian_server::crdt::registry::CrdtType::GCounter);
    let op1 = GCounterOp { client_id: 1, amount: 5 };
    let op2 = GCounterOp { client_id: 2, amount: 3 };
    if let CrdtValue::GCounter(ref mut g) = counter {
        g.apply(op1).unwrap();
        g.apply(op2).unwrap();
    }
    let encoded = rmp_serde::to_vec(&counter).expect("msgpack encode");
    let expected_total: i64 = 8; // 5 + 3

    db.client
        .execute(
            "INSERT INTO counters (id, value) VALUES ($1, $2)",
            &[&"ctr-1", &encoded],
        )
        .await
        .expect("INSERT");

    let a = applier.clone();
    db.wait_for(
        Duration::from_secs(5),
        move || !a.calls_for("counters:ctr-1:value").is_empty(),
        "GCounter to be delivered",
    )
    .await;

    // Decode what the WAL consumer delivered and verify it's a valid GCounter
    let received = &applier.calls_for("counters:ctr-1:value")[0];
    let decoded: CrdtValue = rmp_serde::from_slice(received).expect("msgpack decode");
    match decoded {
        CrdtValue::GCounter(g) => {
            let total: i64 = g.counters.values().map(|&v| v as i64).sum();
            assert_eq!(total, expected_total, "GCounter value mismatch");
        }
        other => panic!("expected GCounter, got {other:?}"),
    }

    db.cleanup().await;
}

#[tokio::test]
async fn test_consumer_reconnects_after_disconnect() {
    let url = base_url();
    if !check_wal_level(&url).await {
        return;
    }
    let _serial = serial_lock().await;

    let mut db = TestDb::new("reconnect").await.expect("setup");
    db.client
        .simple_query("CREATE TABLE docs (id TEXT PRIMARY KEY, data BYTEA)")
        .await
        .expect("CREATE TABLE");

    let applier = Arc::new(RecordingApplier::new());
    db.spawn_consumer(Arc::clone(&applier));
    tokio::time::sleep(Duration::from_millis(600)).await;

    // First write — confirms the consumer is alive
    let v1: Vec<u8> = vec![0x01];
    db.client
        .execute("INSERT INTO docs (id, data) VALUES ($1, $2)", &[&"doc-1", &v1])
        .await
        .expect("INSERT 1");

    let a = applier.clone();
    db.wait_for(
        Duration::from_secs(5),
        move || !a.calls_for("docs:doc-1:data").is_empty(),
        "first write delivered",
    )
    .await;

    // Kill all replication connections to simulate a network drop
    let (admin, conn) = tokio_postgres::connect(&url, NoTls).await.expect("admin connect");
    tokio::spawn(conn);
    admin
        .simple_query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = 'pgwire_replication' OR state = 'idle in transaction'",
        )
        .await
        .ok();

    // Wait a bit then write again — the consumer should have reconnected
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let v2: Vec<u8> = vec![0x02, 0x03];
    db.client
        .execute(
            "INSERT INTO docs (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2",
            &[&"doc-2", &v2],
        )
        .await
        .expect("INSERT 2");

    let a = applier.clone();
    db.wait_for(
        Duration::from_secs(10),
        move || !a.calls_for("docs:doc-2:data").is_empty(),
        "second write delivered after reconnect",
    )
    .await;

    assert_eq!(applier.calls_for("docs:doc-2:data")[0], v2);

    db.cleanup().await;
}

#[tokio::test]
async fn test_null_bytea_not_delivered() {
    let url = base_url();
    if !check_wal_level(&url).await {
        return;
    }
    let _serial = serial_lock().await;

    let mut db = TestDb::new("null").await.expect("setup");
    db.client
        .simple_query("CREATE TABLE docs (id TEXT PRIMARY KEY, data BYTEA)")
        .await
        .expect("CREATE TABLE");

    let applier = Arc::new(RecordingApplier::new());
    db.spawn_consumer(Arc::clone(&applier));
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Insert with NULL BYTEA — should not trigger applier
    db.client
        .execute("INSERT INTO docs (id, data) VALUES ($1, NULL)", &[&"null-1"])
        .await
        .expect("INSERT NULL");

    // Insert a non-null row so we know the consumer is alive
    let marker: Vec<u8> = vec![0xFF];
    db.client
        .execute("INSERT INTO docs (id, data) VALUES ($1, $2)", &[&"marker", &marker])
        .await
        .expect("INSERT marker");

    let a = applier.clone();
    db.wait_for(
        Duration::from_secs(5),
        move || !a.calls_for("docs:marker:data").is_empty(),
        "marker row delivered (proves consumer is alive)",
    )
    .await;

    // The NULL row should have produced zero calls
    assert!(
        applier.calls_for("docs:null-1:data").is_empty(),
        "NULL BYTEA should not trigger applier"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn test_non_bytea_columns_ignored() {
    let url = base_url();
    if !check_wal_level(&url).await {
        return;
    }
    let _serial = serial_lock().await;

    let mut db = TestDb::new("nonbytea").await.expect("setup");
    db.client
        .simple_query(
            "CREATE TABLE mixed (
                id    TEXT PRIMARY KEY,
                score INT,
                label TEXT,
                data  BYTEA
            )",
        )
        .await
        .expect("CREATE TABLE");

    let applier = Arc::new(RecordingApplier::new());
    db.spawn_consumer(Arc::clone(&applier));
    tokio::time::sleep(Duration::from_millis(600)).await;

    let payload: Vec<u8> = vec![0xBE, 0xEF];
    db.client
        .execute(
            "INSERT INTO mixed (id, score, label, data) VALUES ($1, $2, $3, $4)",
            &[&"row-1", &42i32, &"hello", &payload],
        )
        .await
        .expect("INSERT");

    let a = applier.clone();
    db.wait_for(
        Duration::from_secs(5),
        move || !a.calls_for("mixed:row-1:data").is_empty(),
        "BYTEA column delivered",
    )
    .await;

    let calls = applier.recorded();
    // Only 'data' column should appear — not 'score', not 'label'
    for (_, crdt_id, _) in &calls {
        assert!(
            crdt_id.ends_with(":data"),
            "non-BYTEA column triggered applier: {crdt_id}"
        );
    }

    db.cleanup().await;
}
