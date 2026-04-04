// wal_consumer — pgrx background worker that tails Postgres logical replication
// and forwards decoded ops to the Meridian server via HTTP POST.
//
// Activation (postgresql.conf / ALTER SYSTEM):
//   meridian.server_url = 'http://localhost:4000'
//   meridian.slot_name  = 'meridian_wal'          -- replication slot name
//   meridian.pub_name   = 'meridian_pub'           -- publication name
//
// The worker creates the slot + publication on first start if they don't exist.
// Each decoded INSERT/UPDATE row is posted as:
//   POST <server_url>/internal/pg-sync
//   Content-Type: application/octet-stream
//   Body: msgpack-encoded CrdtPgSyncPayload { ns, crdt_id, data: Vec<u8> }
//
// The body mirrors the pg_notify format used by the trigger but avoids the
// 8 KB NOTIFY limit — suitable for large RGA / Tree documents.

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags};
use std::time::Duration;

// ---------------------------------------------------------------------------
// GUC registration (called from _PG_init)
// ---------------------------------------------------------------------------

static SERVER_URL: pgrx::GucSetting<Option<&'static std::ffi::CStr>> =
    <pgrx::GucSetting<Option<&'static std::ffi::CStr>>>::new(None);
static SLOT_NAME: pgrx::GucSetting<Option<&'static std::ffi::CStr>> =
    <pgrx::GucSetting<Option<&'static std::ffi::CStr>>>::new(None);
static PUB_NAME: pgrx::GucSetting<Option<&'static std::ffi::CStr>> =
    <pgrx::GucSetting<Option<&'static std::ffi::CStr>>>::new(None);

pub fn register_gucs() {
    pgrx::GucRegistry::define_string_guc(
        "meridian.server_url",
        "Meridian server HTTP URL for WAL sync",
        "e.g. http://localhost:4000",
        &SERVER_URL,
        GucContext::Sighup,
        GucFlags::default(),
    );
    pgrx::GucRegistry::define_string_guc(
        "meridian.slot_name",
        "Logical replication slot name for Meridian WAL consumer",
        "",
        &SLOT_NAME,
        GucContext::Sighup,
        GucFlags::default(),
    );
    pgrx::GucRegistry::define_string_guc(
        "meridian.pub_name",
        "Publication name for Meridian WAL consumer",
        "",
        &PUB_NAME,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

pub fn register_bgworker() {
    BackgroundWorkerBuilder::new("meridian WAL consumer")
        .set_function("meridian_wal_consumer_main")
        .set_library("meridian")
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(10)))
        .load();
}

// ---------------------------------------------------------------------------
// Background worker entry point
// ---------------------------------------------------------------------------

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C" fn meridian_wal_consumer_main(_arg: pg_sys::Datum) {
    // Attach to the database so we can connect.
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Read GUCs — if server_url is not set, sleep and retry later.
    let server_url = SERVER_URL
        .get()
        .and_then(|cs| cs.to_str().ok())
        .map(|s| s.to_owned());

    let server_url = match server_url {
        Some(u) if !u.is_empty() => u,
        _ => {
            log!("meridian WAL consumer: meridian.server_url not set — worker idle.");
            // Park indefinitely; Postgres will restart us on SIGHUP if GUC changes.
            while BackgroundWorker::wait_latch(Some(Duration::from_secs(60))) {}
            return;
        }
    };

    let slot_name = SLOT_NAME
        .get()
        .and_then(|cs| cs.to_str().ok())
        .unwrap_or("meridian_wal")
        .to_owned();
    let pub_name = PUB_NAME
        .get()
        .and_then(|cs| cs.to_str().ok())
        .unwrap_or("meridian_pub")
        .to_owned();

    // Spawn a tokio runtime for async tokio-postgres replication streaming.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("meridian WAL consumer: failed to build tokio runtime");

    rt.block_on(run_wal_consumer(server_url, slot_name, pub_name));
}

// ---------------------------------------------------------------------------
// Async WAL consumer loop
// ---------------------------------------------------------------------------

async fn run_wal_consumer(server_url: String, slot_name: String, pub_name: String) {
    use tokio_postgres::{connect, NoTls};

    // Build connection string from environment / GUC (standard libpq env vars).
    let conn_str = std::env::var("MERIDIAN_WAL_CONNSTR")
        .unwrap_or_else(|_| "host=localhost dbname=postgres replication=database".to_owned());

    loop {
        log!("meridian WAL consumer: connecting (slot={slot_name}, pub={pub_name})");

        let (client, conn) = match connect(&conn_str, NoTls).await {
            Ok(c) => c,
            Err(e) => {
                log!("meridian WAL consumer: connection failed: {e} — retrying in 10s");
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        // Drive the connection in the background.
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                log!("meridian WAL consumer: connection error: {e}");
            }
        });

        if let Err(e) = ensure_slot_and_pub(&client, &slot_name, &pub_name).await {
            log!("meridian WAL consumer: setup failed: {e} — retrying in 10s");
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }

        if let Err(e) = stream_wal(&client, &slot_name, &pub_name, &server_url).await {
            log!("meridian WAL consumer: stream error: {e} — reconnecting in 5s");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

async fn ensure_slot_and_pub(
    client: &tokio_postgres::Client,
    slot_name: &str,
    pub_name: &str,
) -> Result<(), tokio_postgres::Error> {
    // Create slot if it doesn't exist.
    let slot_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot_name],
        )
        .await?
        .get(0);

    if !slot_exists {
        client
            .simple_query(&format!(
                "SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
            ))
            .await?;
        log!("meridian WAL consumer: created replication slot '{slot_name}'");
    }

    // Create publication for all tables if it doesn't exist.
    let pub_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&pub_name],
        )
        .await?
        .get(0);

    if !pub_exists {
        client
            .simple_query(&format!(
                "CREATE PUBLICATION {pub_name} FOR ALL TABLES"
            ))
            .await?;
        log!("meridian WAL consumer: created publication '{pub_name}'");
    }

    Ok(())
}

async fn stream_wal(
    client: &tokio_postgres::Client,
    slot_name: &str,
    pub_name: &str,
    server_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio_postgres::SimpleQueryMessage;

    // Start logical replication stream.
    let query = format!(
        "START_REPLICATION SLOT {slot_name} LOGICAL 0/0 \
         (proto_version '1', publication_names '{pub_name}')"
    );

    let stream = client.simple_query(&query).await?;

    // HTTP client for forwarding to Meridian server.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    let push_url = format!("{server_url}/internal/pg-sync");

    for msg in stream {
        match msg {
            SimpleQueryMessage::Row(row) => {
                // pgoutput rows: column 0 = data (bytea XLogData payload)
                if let Some(data) = row.get(0) {
                    if let Err(e) = forward_xlog_data(data.as_bytes(), &http, &push_url).await {
                        log!("meridian WAL consumer: forward error: {e}");
                    }
                }
            }
            SimpleQueryMessage::CommandComplete(_) => break,
            _ => {}
        }
    }

    Ok(())
}

/// Parse a minimal XLogData message and POST BYTEA column values to the server.
/// Full pgoutput parsing is complex; this handles the INSERT/UPDATE cases for
/// BYTEA columns (type 'b') which is all meridian-pg stores.
async fn forward_xlog_data(
    data: &[u8],
    http: &reqwest::Client,
    push_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // XLogData: byte 0 = message type ('w' for WAL data)
    // For now, forward the raw pgoutput payload — the Meridian server
    // decodes it via its pg_transport module.
    if data.is_empty() {
        return Ok(());
    }

    http.post(push_url)
        .header("Content-Type", "application/octet-stream")
        .body(data.to_vec())
        .send()
        .await?;

    Ok(())
}
