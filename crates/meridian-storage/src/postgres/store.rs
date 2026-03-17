use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::instrument;

use crate::{
    error::{Result, StorageError},
    store::Store,
};

// ---------------------------------------------------------------------------
// PgStore
// ---------------------------------------------------------------------------

/// Persistent CRDT storage backed by PostgreSQL.
///
/// ## Schema
/// ```sql
/// CREATE TABLE crdt_snapshots (
///     namespace TEXT NOT NULL,
///     crdt_id   TEXT NOT NULL,
///     data      BYTEA NOT NULL,
///     updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
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
                namespace  TEXT NOT NULL,
                crdt_id    TEXT NOT NULL,
                data       BYTEA NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (namespace, crdt_id)
            );
            CREATE INDEX IF NOT EXISTS crdt_snapshots_namespace_idx
                ON crdt_snapshots (namespace);
            "#,
        )
        .execute(pool)
        .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Store impl
// ---------------------------------------------------------------------------

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
