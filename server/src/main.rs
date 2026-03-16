use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use meridian_server::server::{run, Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::from_env();

    let prometheus_handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");

    // ----- Storage: postgres if DATABASE_URL is set, otherwise sled -----
    #[cfg(feature = "storage-postgres")]
    if let Some(ref url) = config.database_url {
        use meridian_server::storage::{PgStore, PgWal};
        use sqlx::postgres::PgPoolOptions;

        info!(url, "postgres backend selected");
        let pool = PgPoolOptions::new().max_connections(16).connect(url).await?;
        PgStore::migrate(&pool).await?;
        PgWal::migrate(&pool).await?;
        let store = Arc::new(PgStore::new(pool.clone()));
        let wal = Arc::new(PgWal::new(pool).await?);
        return run(config, prometheus_handle, store, wal).await;
    }

    #[cfg(feature = "storage-sled")]
    {
        use meridian_server::storage::{SledStore, SledWal};
        let store = Arc::new(SledStore::open(&config.data_dir)?);
        info!(path = config.data_dir, "sled store opened");
        let wal = Arc::new(SledWal::new(store.db())?);
        return run(config, prometheus_handle, store, wal).await;
    }

    #[cfg(not(any(feature = "storage-sled", feature = "storage-postgres")))]
    compile_error!("no storage backend enabled — build with --features storage-sled or storage-postgres")
}
