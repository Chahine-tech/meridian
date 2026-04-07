// wal_replication — Logical replication consumer for the pg-sync transport.
//
// Opens a replication connection to Postgres, streams pgoutput messages, and
// feeds BYTEA column changes into the PgStateApplier (same path as NOTIFY).
//
// This covers the case where pg_notify payloads exceed the 8 KB Postgres
// limit (large RGA / Tree documents). The NOTIFY trigger and the WAL stream
// are complementary: both feed the same idempotent merge path.
//
// Protocol overview:
//   1. Connect with pgwire-replication (handles CopyBoth + keepalives).
//   2. Ensure the logical slot (pgoutput) and publication exist.
//   3. Receive ReplicationEvent::XLogData frames, parse pgoutput messages.
//   4. Acknowledge LSN via update_applied_lsn() after each batch.
//
// pgoutput message types we handle:
//   'R' (Relation)  — cache column schema by OID
//   'I' (Insert)    — extract BYTEA columns, call applier
//   'U' (Update)    — extract BYTEA columns from new tuple, call applier
//   'B'/'C'/'D'     — ignored

use std::{collections::HashMap, sync::Arc, time::Duration};

use pgwire_replication::{ReplicationClient, ReplicationConfig, ReplicationEvent};
use tokio_postgres::NoTls;
use tracing::{debug, info, warn};

use super::pg_transport::PgStateApplier;

const BYTEA_OID: u32 = 17;

struct RelationSchema {
    /// Postgres schema name (e.g. "public")
    namespace: String,
    /// Table name
    table_name: String,
    columns: Vec<ColumnInfo>,
}

struct ColumnInfo {
    name: String,
    type_oid: u32,
    /// true if this column is part of the replica identity key
    is_key: bool,
}

/// Stream logical replication from Postgres, apply CRDT state changes.
///
/// Runs forever with exponential backoff on connection failure.
/// Intended to be spawned as a tokio task.
pub async fn run<A: PgStateApplier>(
    connstr: String,
    slot_name: String,
    pub_name: String,
    applier: Arc<A>,
) {
    let mut backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    loop {
        match stream_once(&connstr, &slot_name, &pub_name, &applier).await {
            Ok(()) => {
                // stream_once only returns Ok if the server closed gracefully.
                info!("WAL replication stream ended — reconnecting");
            }
            Err(e) => {
                warn!(error = %e, backoff_secs = backoff.as_secs(), "WAL replication error — retrying");
            }
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

async fn stream_once<A: PgStateApplier>(
    connstr: &str,
    slot_name: &str,
    pub_name: &str,
    applier: &Arc<A>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Standard connection for DDL (slot/publication setup).
    let (client, conn) = tokio_postgres::connect(connstr, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!(error = %e, "WAL setup connection dropped");
        }
    });

    ensure_slot_and_pub(&client, slot_name, pub_name).await?;

    // Parse the connection string into ReplicationConfig fields.
    let cfg = parse_replication_config(connstr, slot_name, pub_name)?;

    info!(slot = slot_name, publication = pub_name, "WAL replication stream starting");

    let mut repl_client = ReplicationClient::connect(cfg).await?;
    let mut schema_cache: HashMap<u32, RelationSchema> = HashMap::new();

    while let Some(event) = repl_client.recv().await? {
        match event {
            ReplicationEvent::XLogData { wal_end, data, .. } => {
                debug!(bytes = data.len(), "WAL XLogData received");
                parse_pgoutput(&data, &mut schema_cache, applier).await;
                repl_client.update_applied_lsn(wal_end);
            }
            ReplicationEvent::KeepAlive { .. } => {
                // pgwire-replication handles keepalive replies automatically
                // via status_interval. Nothing to do here.
            }
            ReplicationEvent::StoppedAt { .. } => {
                return Ok(());
            }
            _ => {}
        }
    }

    Ok(())
}

async fn ensure_slot_and_pub(
    client: &tokio_postgres::Client,
    slot_name: &str,
    pub_name: &str,
) -> Result<(), tokio_postgres::Error> {
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
        info!(slot = slot_name, "created replication slot");
    }

    let pub_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&pub_name],
        )
        .await?
        .get(0);

    if !pub_exists {
        client
            .simple_query(&format!("CREATE PUBLICATION {pub_name} FOR ALL TABLES"))
            .await?;
        info!(publication = pub_name, "created publication");
    }

    Ok(())
}


type ConnParams = (String, u16, String, String, String);
type ParseError = Box<dyn std::error::Error + Send + Sync>;

/// Parse a libpq-style or URL connection string into `ReplicationConfig`.
///
/// Supported formats:
///   - `postgresql://user:pass@host:port/dbname`
///   - `host=localhost port=5432 user=foo password=bar dbname=baz`
fn parse_replication_config(
    connstr: &str,
    slot_name: &str,
    pub_name: &str,
) -> Result<ReplicationConfig, ParseError> {
    let (host, port, user, password, database) = if connstr.starts_with("postgresql://")
        || connstr.starts_with("postgres://")
    {
        parse_url_connstr(connstr)?
    } else {
        parse_kv_connstr(connstr)?
    };

    let mut cfg = ReplicationConfig::new(
        host,
        user,
        password,
        database,
        slot_name.to_owned(),
        pub_name.to_owned(),
    );
    cfg.port = port;
    Ok(cfg)
}

fn parse_url_connstr(url: &str) -> Result<ConnParams, ParseError> {
    // postgresql://user:password@host:port/database
    let without_scheme = url
        .trim_start_matches("postgresql://")
        .trim_start_matches("postgres://");

    let (userinfo, hostdb) = if let Some(at) = without_scheme.rfind('@') {
        (&without_scheme[..at], &without_scheme[at + 1..])
    } else {
        ("", without_scheme)
    };

    let (user, password) = if let Some(colon) = userinfo.find(':') {
        (userinfo[..colon].to_owned(), userinfo[colon + 1..].to_owned())
    } else {
        (userinfo.to_owned(), String::new())
    };

    // hostdb may have query params — strip them
    let hostdb = hostdb.split('?').next().unwrap_or(hostdb);

    let (hostport, database) = if let Some(slash) = hostdb.find('/') {
        (&hostdb[..slash], hostdb[slash + 1..].to_owned())
    } else {
        (hostdb, String::new())
    };

    let (host, port) = if let Some(colon) = hostport.rfind(':') {
        let p: u16 = hostport[colon + 1..].parse().unwrap_or(5432);
        (hostport[..colon].to_owned(), p)
    } else {
        (hostport.to_owned(), 5432u16)
    };

    let host = if host.is_empty() { "localhost".to_owned() } else { host };
    let user = if user.is_empty() { "postgres".to_owned() } else { user };
    let database = if database.is_empty() { user.clone() } else { database };

    Ok((host, port, user, password, database))
}

fn parse_kv_connstr(kv: &str) -> Result<ConnParams, ParseError> {
    let mut host = "localhost".to_owned();
    let mut port: u16 = 5432;
    let mut user = "postgres".to_owned();
    let mut password = String::new();
    let mut database = String::new();

    for token in kv.split_whitespace() {
        if let Some((k, v)) = token.split_once('=') {
            match k {
                "host"     => host = v.to_owned(),
                "port"     => port = v.parse().unwrap_or(5432),
                "user"     => user = v.to_owned(),
                "password" => password = v.to_owned(),
                "dbname"   => database = v.to_owned(),
                _ => {}
            }
        }
    }

    if database.is_empty() {
        database = user.clone();
    }

    Ok((host, port, user, password, database))
}

async fn parse_pgoutput<A: PgStateApplier>(
    data: &[u8],
    schema_cache: &mut HashMap<u32, RelationSchema>,
    applier: &Arc<A>,
) {
    if data.is_empty() {
        return;
    }

    let msg_type = data[0];
    let body = &data[1..];

    match msg_type {
        b'R' => {
            if let Some((oid, schema)) = parse_relation(body) {
                schema_cache.insert(oid, schema);
            }
        }
        b'I' => {
            parse_and_apply_insert(body, schema_cache, applier).await;
        }
        b'U' => {
            parse_and_apply_update(body, schema_cache, applier).await;
        }
        // Begin, Commit, Delete — not relevant for our apply path
        b'B' | b'C' | b'D' => {}
        other => {
            debug!(msg_type = other, "pgoutput: ignoring message type");
        }
    }
}

/// Parse a Relation message body (after the 'R' type byte).
///
/// Layout:
///   [4]  relation OID
///   [?]  namespace (null-terminated)
///   [?]  table name (null-terminated)
///   [1]  replica identity
///   [2]  column count
///   per column:
///     [1]  flags (0=normal, 1=key)
///     [?]  column name (null-terminated)
///     [4]  type OID
///     [4]  type modifier
fn parse_relation(body: &[u8]) -> Option<(u32, RelationSchema)> {
    let mut r = Reader::new(body);
    let oid = r.read_u32()?;
    let namespace = r.read_cstr()?;
    let table_name = r.read_cstr()?;
    let _replica_identity = r.read_u8()?;
    let ncols = r.read_u16()? as usize;

    let mut columns = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let flags = r.read_u8()?;
        let name = r.read_cstr()?;
        let type_oid = r.read_u32()?;
        let _type_mod = r.read_i32()?;
        columns.push(ColumnInfo {
            name,
            type_oid,
            is_key: flags & 1 != 0,
        });
    }

    Some((oid, RelationSchema { namespace, table_name, columns }))
}

async fn parse_and_apply_insert<A: PgStateApplier>(
    body: &[u8],
    schema_cache: &HashMap<u32, RelationSchema>,
    applier: &Arc<A>,
) {
    let mut r = Reader::new(body);
    let oid = r.read_u32().unwrap_or(0);
    let marker = r.read_u8().unwrap_or(0);
    if marker != b'N' {
        return;
    }
    let schema = match schema_cache.get(&oid) {
        Some(s) => s,
        None => return,
    };
    apply_tuple(&mut r, schema, applier).await;
}

async fn parse_and_apply_update<A: PgStateApplier>(
    body: &[u8],
    schema_cache: &HashMap<u32, RelationSchema>,
    applier: &Arc<A>,
) {
    let mut r = Reader::new(body);
    let oid = r.read_u32().unwrap_or(0);
    let schema = match schema_cache.get(&oid) {
        Some(s) => s,
        None => return,
    };

    // Optional old tuple ('K' or 'O'), then mandatory new tuple ('N').
    let first = r.peek_u8().unwrap_or(0);
    if first == b'K' || first == b'O' {
        r.read_u8();
        // Skip old tuple data — we only care about the new state.
        skip_tuple_data(&mut r);
    }

    let marker = r.read_u8().unwrap_or(0);
    if marker != b'N' {
        return;
    }
    apply_tuple(&mut r, schema, applier).await;
}

/// Parse TupleData and call applier for each BYTEA column.
async fn apply_tuple<A: PgStateApplier>(
    r: &mut Reader<'_>,
    schema: &RelationSchema,
    applier: &Arc<A>,
) {
    let ncols = r.read_u16().unwrap_or(0) as usize;
    if ncols != schema.columns.len() {
        // Column count mismatch — schema cache is stale, will be refreshed
        // when the next Relation message arrives.
        return;
    }

    // Collect all column values first.
    let mut col_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(ncols);

    for _ in 0..ncols {
        match r.read_u8().unwrap_or(0) {
            b'n' | b'u' => col_values.push(None),
            b't' | b'b' => {
                let len = r.read_u32().unwrap_or(0) as usize;
                let bytes = r.read_bytes(len);
                col_values.push(bytes.map(|b| b.to_vec()));
            }
            _ => col_values.push(None),
        }
    }

    // Find the key column value.
    let pk_value = schema
        .columns
        .iter()
        .zip(col_values.iter())
        .find(|(col, _)| col.is_key)
        .and_then(|(_, val)| val.as_ref())
        .and_then(|v| std::str::from_utf8(v).ok().map(|s| s.to_owned()));

    let pk_value = match pk_value {
        Some(v) => v,
        None => return,
    };

    // Apply each BYTEA column.
    for (col, raw) in schema.columns.iter().zip(col_values.into_iter()) {
        if col.type_oid != BYTEA_OID {
            continue;
        }
        let raw = match raw {
            Some(r) => r,
            None => continue,
        };

        let bytea = decode_bytea_text(&raw);
        if bytea.is_empty() {
            continue;
        }

        let crdt_id = format!("{}:{}:{}", schema.table_name, pk_value, col.name);
        let namespace = schema.namespace.clone();

        debug!(
            ns = %namespace,
            crdt_id = %crdt_id,
            bytes = bytea.len(),
            "WAL: applying BYTEA column"
        );

        applier.merge_pg_state(namespace, crdt_id, bytea).await;
    }
}

fn skip_tuple_data(r: &mut Reader<'_>) {
    let ncols = r.read_u16().unwrap_or(0) as usize;
    for _ in 0..ncols {
        match r.read_u8().unwrap_or(0) {
            b'n' | b'u' => {}
            b't' | b'b' => {
                let len = r.read_u32().unwrap_or(0) as usize;
                r.skip(len);
            }
            _ => {}
        }
    }
}

/// Decode a BYTEA value from pgoutput text format.
///
/// pgoutput sends BYTEA in text format as "\x" + hex pairs (e.g. "\x48656c6c6f").
/// Binary format ('b' flag) sends raw bytes directly.
///
/// We handle both here by checking for the "\x" prefix.
fn decode_bytea_text(raw: &[u8]) -> Vec<u8> {
    // Text hex format: starts with "\x" (0x5c 0x78)
    if raw.starts_with(b"\\x") {
        let hex = &raw[2..];
        (0..hex.len() / 2)
            .filter_map(|i| {
                let pair = hex.get(i * 2..i * 2 + 2)?;
                let s = std::str::from_utf8(pair).ok()?;
                u8::from_str_radix(s, 16).ok()
            })
            .collect()
    } else {
        // Binary format or already raw bytes.
        raw.to_vec()
    }
}

struct Reader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn peek_u8(&self) -> Option<u8> {
        self.data.get(self.offset).copied()
    }

    fn read_u8(&mut self) -> Option<u8> {
        let v = self.data.get(self.offset).copied();
        if v.is_some() {
            self.offset += 1;
        }
        v
    }

    fn read_u16(&mut self) -> Option<u16> {
        let b = self.data.get(self.offset..self.offset + 2)?;
        self.offset += 2;
        Some(u16::from_be_bytes(b.try_into().ok()?))
    }

    fn read_u32(&mut self) -> Option<u32> {
        let b = self.data.get(self.offset..self.offset + 4)?;
        self.offset += 4;
        Some(u32::from_be_bytes(b.try_into().ok()?))
    }

    fn read_i32(&mut self) -> Option<i32> {
        let b = self.data.get(self.offset..self.offset + 4)?;
        self.offset += 4;
        Some(i32::from_be_bytes(b.try_into().ok()?))
    }

    fn read_cstr(&mut self) -> Option<String> {
        let start = self.offset;
        let end = self.data[start..].iter().position(|&b| b == 0)?;
        let s = std::str::from_utf8(&self.data[start..start + end]).ok()?.to_owned();
        self.offset = start + end + 1;
        Some(s)
    }

    fn read_bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        let b = self.data.get(self.offset..self.offset + len)?;
        self.offset += len;
        Some(b)
    }

    fn skip(&mut self, n: usize) {
        self.offset = (self.offset + n).min(self.data.len());
    }
}
