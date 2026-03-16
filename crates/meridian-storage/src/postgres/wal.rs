use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::PgPool;
use tracing::{instrument, warn};

use crate::{
    error::{Result, StorageError},
    utils::now_ms,
    wal_backend::{WalBackend, WalEntry},
};

// ---------------------------------------------------------------------------
// PgWal
// ---------------------------------------------------------------------------

/// Write-Ahead Log backed by PostgreSQL.
///
/// ## Schema
/// ```sql
/// CREATE TABLE wal_entries (
///     seq          BIGSERIAL PRIMARY KEY,
///     namespace    TEXT NOT NULL,
///     crdt_id      TEXT NOT NULL,
///     op_bytes     BYTEA NOT NULL,
///     timestamp_ms BIGINT NOT NULL
/// );
/// CREATE TABLE wal_checkpoint (
///     id           INT PRIMARY KEY DEFAULT 1,
///     seq          BIGINT NOT NULL DEFAULT 0,
///     CHECK (id = 1)
/// );
/// ```
///
/// `last_seq` and `checkpoint_seq` are cached in-memory atomics for hot-path
/// reads (compactor, handler) without hitting the DB.
pub struct PgWal {
    pool: PgPool,
    last_seq: AtomicU64,
    checkpoint_seq: AtomicU64,
}

impl PgWal {
    pub async fn new(pool: PgPool) -> Result<Self> {
        // Read current max seq and checkpoint from DB to resume after restart.
        let last: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(seq), 0) FROM wal_entries",
        )
        .fetch_one(&pool)
        .await?;

        let checkpoint: i64 =
            sqlx::query_scalar("SELECT seq FROM wal_checkpoint WHERE id = 1")
                .fetch_optional(&pool)
                .await?
                .unwrap_or(0);

        let last_seq = u64::try_from(last)
            .map_err(|_| StorageError::InvalidKey(format!("corrupt WAL seq in DB: {last}")))?;
        let checkpoint_seq = u64::try_from(checkpoint)
            .map_err(|_| StorageError::InvalidKey(format!("corrupt checkpoint seq in DB: {checkpoint}")))?;

        Ok(Self {
            pool,
            last_seq: AtomicU64::new(last_seq),
            checkpoint_seq: AtomicU64::new(checkpoint_seq),
        })
    }

    /// Run the DDL migration. Safe to call on every startup.
    pub async fn migrate(pool: &PgPool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS wal_entries (
                seq          BIGSERIAL PRIMARY KEY,
                namespace    TEXT NOT NULL,
                crdt_id      TEXT NOT NULL,
                op_bytes     BYTEA NOT NULL,
                timestamp_ms BIGINT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS wal_checkpoint (
                id  INT PRIMARY KEY DEFAULT 1,
                seq BIGINT NOT NULL DEFAULT 0,
                CHECK (id = 1)
            );
            INSERT INTO wal_checkpoint (id, seq) VALUES (1, 0) ON CONFLICT DO NOTHING;
            "#,
        )
        .execute(pool)
        .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WalBackend impl
// ---------------------------------------------------------------------------

impl WalBackend for PgWal {
    #[instrument(skip(self, op_bytes))]
    async fn append(&self, namespace: &str, crdt_id: &str, op_bytes: Vec<u8>) -> Result<u64> {
        let timestamp_ms = now_ms() as i64;

        let seq: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO wal_entries (namespace, crdt_id, op_bytes, timestamp_ms)
            VALUES ($1, $2, $3, $4)
            RETURNING seq
            "#,
        )
        .bind(namespace)
        .bind(crdt_id)
        .bind(op_bytes)
        .bind(timestamp_ms)
        .fetch_one(&self.pool)
        .await?;

        let seq = seq as u64;
        self.last_seq.fetch_max(seq, Ordering::Relaxed);
        Ok(seq)
    }

    async fn replay_from(&self, from_seq: u64) -> Result<Vec<WalEntry>> {
        let rows: Vec<(i64, String, String, Vec<u8>, i64)> = sqlx::query_as(
            "SELECT seq, namespace, crdt_id, op_bytes, timestamp_ms FROM wal_entries WHERE seq >= $1 ORDER BY seq",
        )
        .bind(from_seq as i64)
        .fetch_all(&self.pool)
        .await?;

        let entries = rows
            .into_iter()
            .map(|(seq, namespace, crdt_id, op_bytes, timestamp_ms)| WalEntry {
                seq: seq as u64,
                namespace,
                crdt_id,
                op_bytes,
                timestamp_ms: timestamp_ms as u64,
            })
            .collect();

        Ok(entries)
    }

    async fn truncate_before(&self, before_seq: u64) -> Result<()> {
        sqlx::query("DELETE FROM wal_entries WHERE seq < $1")
            .bind(before_seq as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    fn last_seq(&self) -> u64 {
        self.last_seq.load(Ordering::Relaxed)
    }

    fn checkpoint_seq(&self) -> u64 {
        self.checkpoint_seq.load(Ordering::Relaxed)
    }

    async fn set_checkpoint_seq(&self, seq: u64) -> Result<()> {
        sqlx::query(
            "UPDATE wal_checkpoint SET seq = $1 WHERE id = 1",
        )
        .bind(seq as i64)
        .execute(&self.pool)
        .await?;
        self.checkpoint_seq.store(seq, Ordering::Relaxed);
        Ok(())
    }
}

