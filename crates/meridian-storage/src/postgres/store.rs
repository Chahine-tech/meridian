use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::instrument;

use crate::{
    error::{Result, StorageError},
    store::Store,
};

/// Persistent CRDT storage backed by PostgreSQL.
///
/// ## Schema
/// ```sql
/// CREATE TABLE crdt_snapshots (
///     namespace     TEXT NOT NULL,
///     crdt_id       TEXT NOT NULL,
///     data          BYTEA NOT NULL,
///     updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
///     expires_at_ms BIGINT,           -- NULL means no expiry (permanent)
///     PRIMARY KEY (namespace, crdt_id)
/// );
/// ```
///
/// Values are msgpack-encoded — the same wire format as `SledStore`.
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Run the DDL migration to create the `crdt_snapshots` table if it
    /// doesn't exist. Safe to call on every startup.
    pub async fn migrate(pool: &PgPool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS crdt_snapshots (
                namespace     TEXT NOT NULL,
                crdt_id       TEXT NOT NULL,
                data          BYTEA NOT NULL,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at_ms BIGINT,
                PRIMARY KEY (namespace, crdt_id)
            );
            CREATE INDEX IF NOT EXISTS crdt_snapshots_namespace_idx
                ON crdt_snapshots (namespace);
            -- Partial index for efficient TTL-based GC queries.
            CREATE INDEX IF NOT EXISTS crdt_snapshots_expires_idx
                ON crdt_snapshots (expires_at_ms)
                WHERE expires_at_ms IS NOT NULL;
            -- Idempotent: add column if upgrading from a schema without it.
            ALTER TABLE crdt_snapshots
                ADD COLUMN IF NOT EXISTS expires_at_ms BIGINT;
            "#,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

}

impl<V> Store<V> for PgStore
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    #[instrument(skip(self), fields(ns, id))]
    async fn get(&self, ns: &str, id: &str) -> Result<Option<V>> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT data FROM crdt_snapshots WHERE namespace = $1 AND crdt_id = $2",
        )
        .bind(ns)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            None => Ok(None),
            Some((bytes,)) => {
                let value = rmp_serde::decode::from_slice::<V>(&bytes)
                    .map_err(StorageError::Deserialization)?;
                Ok(Some(value))
            }
        }
    }

    #[instrument(skip(self, value), fields(ns, id))]
    async fn put(&self, ns: &str, id: &str, value: &V) -> Result<()> {
        let bytes =
            rmp_serde::encode::to_vec_named(value).map_err(StorageError::Serialization)?;

        sqlx::query(
            r#"
            INSERT INTO crdt_snapshots (namespace, crdt_id, data, updated_at)
            VALUES ($1, $2, $3, now())
            ON CONFLICT (namespace, crdt_id)
            DO UPDATE SET data = EXCLUDED.data, updated_at = now()
            "#,
        )
        .bind(ns)
        .bind(id)
        .bind(bytes)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Atomic read-merge-write using `SELECT FOR UPDATE` inside a transaction.
    ///
    /// Prevents lost updates when multiple nodes share the same PostgreSQL
    /// database and write to the same CRDT concurrently. The row is locked
    /// for the duration of the merge, ensuring linearizable updates.
    #[instrument(skip(self, new_value, merge_fn), fields(ns, id))]
    async fn merge_put<F>(&self, ns: &str, id: &str, new_value: V, merge_fn: F) -> Result<()>
    where
        F: FnOnce(Option<V>, V) -> V + Send,
    {
        self.merge_put_with(ns, id, new_value, |existing, new| {
            (merge_fn(existing, new), ())
        })
        .await
    }

    /// Atomic read-merge-write with side-output, using `SELECT FOR UPDATE`.
    #[instrument(skip(self, new_value, merge_fn), fields(ns, id))]
    async fn merge_put_with<F, R>(
        &self,
        ns: &str,
        id: &str,
        new_value: V,
        merge_fn: F,
    ) -> Result<R>
    where
        F: FnOnce(Option<V>, V) -> (V, R) + Send,
        R: Send,
    {
        let mut tx = self.pool.begin().await?;

        // Lock the row (or nothing if it doesn't exist yet).
        let existing: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT data FROM crdt_snapshots WHERE namespace = $1 AND crdt_id = $2 FOR UPDATE",
        )
        .bind(ns)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;

        let existing_value = match existing {
            Some((bytes,)) => {
                let v = rmp_serde::decode::from_slice::<V>(&bytes)
                    .map_err(StorageError::Deserialization)?;
                Some(v)
            }
            None => None,
        };

        let (merged, result) = merge_fn(existing_value, new_value);
        let bytes =
            rmp_serde::encode::to_vec_named(&merged).map_err(StorageError::Serialization)?;

        sqlx::query(
            r#"
            INSERT INTO crdt_snapshots (namespace, crdt_id, data, updated_at)
            VALUES ($1, $2, $3, now())
            ON CONFLICT (namespace, crdt_id)
            DO UPDATE SET data = EXCLUDED.data, updated_at = now()
            "#,
        )
        .bind(ns)
        .bind(id)
        .bind(bytes)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(result)
    }

    /// Atomic read-merge-write with side-output and optional TTL column.
    ///
    /// When `expires_at_ms` is `Some(ts)`, the `expires_at_ms` column is set
    /// to `ts` so the TTL GC task can efficiently find and delete the row.
    #[instrument(skip(self, new_value, merge_fn, expires_at_ms), fields(ns, id))]
    async fn merge_put_with_expiry<F, R>(
        &self,
        ns: &str,
        id: &str,
        new_value: V,
        expires_at_ms: Option<u64>,
        merge_fn: F,
    ) -> Result<R>
    where
        F: FnOnce(Option<V>, V) -> (V, R) + Send,
        R: Send,
    {
        let mut tx = self.pool.begin().await?;

        let existing: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT data FROM crdt_snapshots WHERE namespace = $1 AND crdt_id = $2 FOR UPDATE",
        )
        .bind(ns)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;

        let existing_value = match existing {
            Some((bytes,)) => {
                let v = rmp_serde::decode::from_slice::<V>(&bytes)
                    .map_err(StorageError::Deserialization)?;
                Some(v)
            }
            None => None,
        };

        let (merged, result) = merge_fn(existing_value, new_value);
        let bytes =
            rmp_serde::encode::to_vec_named(&merged).map_err(StorageError::Serialization)?;

        // Cast to i64 for PostgreSQL BIGINT — clamp to i64::MAX to avoid overflow
        // for TTLs in the far future (> year 292 million).
        let expires: Option<i64> = expires_at_ms.map(|ms| ms.min(i64::MAX as u64) as i64);

        sqlx::query(
            r#"
            INSERT INTO crdt_snapshots (namespace, crdt_id, data, updated_at, expires_at_ms)
            VALUES ($1, $2, $3, now(), $4)
            ON CONFLICT (namespace, crdt_id)
            DO UPDATE SET data = EXCLUDED.data, updated_at = now(), expires_at_ms = EXCLUDED.expires_at_ms
            "#,
        )
        .bind(ns)
        .bind(id)
        .bind(bytes)
        .bind(expires)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(result)
    }

    /// Delete all CRDT entries whose `expires_at_ms < before_ms`.
    ///
    /// Uses the partial index `crdt_snapshots_expires_idx` for O(expired) cost.
    #[instrument(skip(self), fields(before_ms))]
    async fn delete_expired(&self, before_ms: u64) -> Result<Vec<(String, String)>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            r#"
            DELETE FROM crdt_snapshots
            WHERE expires_at_ms IS NOT NULL AND expires_at_ms < $1
            RETURNING namespace, crdt_id
            "#,
        )
        .bind(before_ms as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    #[instrument(skip(self), fields(ns, id))]
    async fn delete(&self, ns: &str, id: &str) -> Result<()> {
        sqlx::query(
            "DELETE FROM crdt_snapshots WHERE namespace = $1 AND crdt_id = $2",
        )
        .bind(ns)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[instrument(skip(self), fields(prefix))]
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, V)>> {
        let rows: Vec<(String, String, Vec<u8>)> = sqlx::query_as(
            "SELECT namespace, crdt_id, data FROM crdt_snapshots WHERE namespace = $1 ORDER BY crdt_id",
        )
        .bind(prefix)
        .fetch_all(&self.pool)
        .await?;

        let mut results = Vec::with_capacity(rows.len());
        for (ns, id, bytes) in rows {
            let value = rmp_serde::decode::from_slice::<V>(&bytes)
                .map_err(StorageError::Deserialization)?;
            results.push((format!("{ns}/{id}"), value));
        }
        Ok(results)
    }
}
