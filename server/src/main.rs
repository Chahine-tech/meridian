use std::sync::Arc;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use meridian_server::server::{run, Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::from_env();
    let metrics = init_metrics()?;

    init_storage_and_run(config, metrics).await
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn init_metrics() -> anyhow::Result<PrometheusHandle> {
    Ok(PrometheusBuilder::new().install_recorder()?)
}

/// Selects the storage backend based on env vars and calls [`run`].
///
/// This is a separate function (rather than returning `(store, wal)`) because
/// each backend branch produces a different concrete type — Rust generics
/// require the type to be known at the call site, so we resolve it here and
/// call `run` directly.
async fn init_storage_and_run(
    config: Config,
    metrics: PrometheusHandle,
) -> anyhow::Result<()> {
    #[cfg(feature = "storage-postgres")]
    if let Some(ref url) = config.database_url {
        use meridian_server::storage::{PgStore, PgWal};
        use sqlx::postgres::PgPoolOptions;
        use tracing::info;

        info!(url, "postgres backend selected");
        let pool = PgPoolOptions::new().max_connections(16).connect(url).await?;
        PgStore::migrate(&pool).await?;
        PgWal::migrate(&pool).await?;
        let store = Arc::new(PgStore::new(pool.clone()));
        let inner_wal = PgWal::new(pool).await?;

        #[cfg(feature = "wal-archive-s3")]
        if let Some(s3_cfg) = meridian_storage::S3ArchiveConfig::from_env() {
            info!(bucket = s3_cfg.bucket, "S3 WAL archiving enabled");
            let wal = Arc::new(meridian_storage::S3ArchivedWal::new(inner_wal, s3_cfg).await?);
            return run(config, metrics, store, wal).await;
        }

        let wal = Arc::new(inner_wal);
        return run(config, metrics, store, wal).await;
    }

    #[cfg(feature = "storage-redis")]
    if let Some(ref url) = config.redis_url {
        use meridian_server::storage::{RedisStore, RedisWal};
        use tracing::info;

        info!(url, "redis backend selected");
        let store = Arc::new(RedisStore::new(url).await?);
        let inner_wal = RedisWal::new(url).await?;

        #[cfg(feature = "wal-archive-s3")]
        if let Some(s3_cfg) = meridian_storage::S3ArchiveConfig::from_env() {
            info!(bucket = s3_cfg.bucket, "S3 WAL archiving enabled");
            let wal = Arc::new(meridian_storage::S3ArchivedWal::new(inner_wal, s3_cfg).await?);
            return run(config, metrics, store, wal).await;
        }

        let wal = Arc::new(inner_wal);
        return run(config, metrics, store, wal).await;
    }

    #[cfg(feature = "storage-sled")]
    {
        use meridian_server::storage::{SledStore, SledWal};
        use tracing::info;

        info!(path = config.data_dir, "sled backend selected");
        let store = Arc::new(SledStore::open(&config.data_dir)?);
        let inner_wal = SledWal::new(store.db())?;

        #[cfg(feature = "wal-archive-s3")]
        if let Some(s3_cfg) = meridian_storage::S3ArchiveConfig::from_env() {
            info!(bucket = s3_cfg.bucket, "S3 WAL archiving enabled");
            let wal = Arc::new(meridian_storage::S3ArchivedWal::new(inner_wal, s3_cfg).await?);
            return run(config, metrics, store, wal).await;
        }

        let wal = Arc::new(inner_wal);
        run(config, metrics, store, wal).await
    }

    #[cfg(not(any(feature = "storage-sled", feature = "storage-postgres", feature = "storage-redis")))]
    compile_error!("no storage backend enabled — build with --features storage-sled, storage-postgres, or storage-redis")
}
