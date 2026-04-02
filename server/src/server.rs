use std::sync::Arc;

use axum::Extension;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    api::build_router,
    api::ws::SubscriptionManager,
    auth::{AuthState, TokenSigner},
    crdt::registry::CrdtValue,
    rate_limit::RateLimiter,
    storage::{CrdtStore, Store, WalBackend},
    tasks::{run_presence_gc, run_snapshot_flusher, run_wal_compactor},
    webhooks::{WebhookConfig, WebhookDispatcher},
    AppState,
};

// Config

pub struct Config {
    pub bind: String,
    pub data_dir: String,
    pub signing_key_hex: Option<String>,
    /// PostgreSQL connection URL. If set, takes priority over sled.
    /// Requires `--features storage-postgres`.
    pub database_url: Option<String>,
    /// Redis connection URL. If set and `DATABASE_URL` is not, overrides sled.
    /// Requires `--features storage-redis`.
    pub redis_url: Option<String>,
    /// Pre-built Postgres pool to reuse for the pg-sync transport.
    /// Set by `main.rs` when `storage-postgres` + `pg-sync` are both active,
    /// so we don't open a second connection pool.
    #[cfg(feature = "pg-sync")]
    pub pg_pool: Option<sqlx::postgres::PgPool>,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            bind: std::env::var("MERIDIAN_BIND").unwrap_or_else(|_| "0.0.0.0:3000".into()),
            data_dir: std::env::var("MERIDIAN_DATA_DIR").unwrap_or_else(|_| "./data".into()),
            signing_key_hex: std::env::var("MERIDIAN_SIGNING_KEY").ok(),
            database_url: std::env::var("DATABASE_URL").ok(),
            redis_url: std::env::var("REDIS_URL").ok(),
            #[cfg(feature = "pg-sync")]
            pg_pool: None,
        }
    }

    #[cfg(any(feature = "cluster", feature = "cluster-http", feature = "pg-sync"))]
    fn bind_port(&self) -> u16 {
        self.bind
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .unwrap_or(3000)
    }
}

// run — wire all services and start the HTTP server

pub async fn run<S, W>(
    #[cfg_attr(not(feature = "pg-sync"), allow(unused_mut))]
    mut config: Config,
    prometheus_handle: PrometheusHandle,
    store: Arc<S>,
    wal: Arc<W>,
) -> anyhow::Result<()>
where
    S: CrdtStore + Store<CrdtValue>,
    W: WalBackend,
{
    let signer = match config.signing_key_hex {
        Some(ref hex) => {
            info!("using signing key from MERIDIAN_SIGNING_KEY");
            Arc::new(TokenSigner::from_hex(hex)?)
        }
        None => {
            tracing::warn!("MERIDIAN_SIGNING_KEY not set — generating ephemeral key (dev mode)");
            Arc::new(TokenSigner::generate())
        }
    };

    let auth_state = Arc::new(AuthState {
        signer: Arc::clone(&signer),
        rate_limiter: Arc::new(RateLimiter::new()),
    });

    let cancel = CancellationToken::new();

    let webhooks = WebhookConfig::from_env().map(|webhook_config| {
        info!(url = webhook_config.url, "webhooks enabled");
        WebhookDispatcher::new(webhook_config, cancel.clone())
    });

    if webhooks.is_none() {
        info!("MERIDIAN_WEBHOOK_URL not set — webhooks disabled");
    }

    let subscriptions = Arc::new(SubscriptionManager::new());

    // Cluster transport modes (mutually exclusive, pick the first that applies):
    //   `--features cluster`      → Redis Pub/Sub (recommended, low latency)
    //   `--features cluster-http` → HTTP push (PostgreSQL-only deployments)
    //   `--features pg-sync`      → PostgreSQL NOTIFY/LISTEN (single-binary, no Redis)
    #[cfg(any(feature = "cluster", feature = "cluster-http", feature = "pg-sync"))]
    let cluster = {
        use meridian_cluster::{ClusterConfig, ClusterHandle, ClusterTransport};
        use crate::cluster::anti_entropy::StoreApplier;

        if let Some(cfg) = ClusterConfig::from_env(config.bind_port()) {
            let node_id = cfg.node_id;

            #[cfg(feature = "cluster")]
            let (transport, cfg) = {
                use meridian_cluster::RedisTransport;
                match &cfg.redis_url {
                    Some(url) => {
                        info!(node_id = %node_id, "cluster enabled — Redis Pub/Sub transport");
                        let t: Arc<dyn ClusterTransport> = Arc::new(RedisTransport::new(url, node_id).await?);
                        (t, cfg)
                    }
                    None => anyhow::bail!("--features cluster requires REDIS_URL"),
                }
            };

            #[cfg(all(feature = "cluster-http", not(feature = "cluster")))]
            let (transport, cfg, http_transport_ref) = {
                use meridian_cluster::HttpPushTransport;
                info!(node_id = %node_id, peers = cfg.peers.len(), "cluster enabled — HTTP push transport");

                let t = Arc::new(
                    HttpPushTransport::with_wal(cfg.peers.clone(), node_id, Arc::clone(&wal))
                );

                let internal_bind = std::env::var("MERIDIAN_INTERNAL_BIND")
                    .unwrap_or_else(|_| "0.0.0.0:3001".into());
                let internal_router = t.router();
                let cancel_clone = cancel.clone();
                // Bind before spawning so startup failures surface immediately.
                let internal_listener = TcpListener::bind(&internal_bind).await
                    .map_err(|e| anyhow::anyhow!("failed to bind internal cluster port {internal_bind}: {e}"))?;
                info!(addr = internal_bind, "cluster internal API listening");
                tokio::spawn(async move {
                    if let Err(e) = axum::serve(internal_listener, internal_router)
                        .with_graceful_shutdown(async move { cancel_clone.cancelled().await })
                        .await
                    {
                        tracing::error!(error = %e, "internal cluster server failed");
                    }
                });

                let transport_dyn: Arc<dyn ClusterTransport> = t.clone();
                (transport_dyn, cfg, t)
            };

            // pg-sync transport: PostgreSQL NOTIFY/LISTEN.
            // Activated when `--features pg-sync` is set and DATABASE_URL is present.
            // Requires no extra infrastructure beyond the existing Postgres pool.
            #[cfg(all(feature = "pg-sync", not(feature = "cluster"), not(feature = "cluster-http")))]
            let (transport, cfg) = {
                use crate::cluster::pg_transport::{PostgresNotifyTransport, StorePgApplier};
                use sqlx::postgres::PgPoolOptions;

                // Prefer the pool already open for storage (passed from main.rs).
                // Fall back to PG_SYNC_DATABASE_URL for non-postgres storage backends.
                let pg_pool = match config.pg_pool.take() {
                    Some(pool) => pool,
                    None => {
                        let pg_url = config.database_url.clone()
                            .or_else(|| std::env::var("PG_SYNC_DATABASE_URL").ok())
                            .expect("pg-sync requires DATABASE_URL or PG_SYNC_DATABASE_URL");
                        PgPoolOptions::new().max_connections(4).connect(&pg_url).await?
                    }
                };

                info!(node_id = %node_id, "cluster enabled — PostgreSQL NOTIFY/LISTEN transport");
                let pg_transport = Arc::new(
                    PostgresNotifyTransport::new(pg_pool, node_id).await?
                );

                // Wire the state applier so trigger payloads (kind="state") are
                // merged into the store before being forwarded to WS clients.
                let applier = Arc::new(StorePgApplier::new(
                    Arc::clone(&store),
                    Arc::clone(&subscriptions) as Arc<dyn meridian_cluster::LocalBroadcast>,
                ));
                pg_transport.spawn_listener(applier);

                let t: Arc<dyn ClusterTransport> = pg_transport;
                (t, cfg)
            };

            let handle = Arc::new(ClusterHandle::new(cfg, transport));

            handle.spawn_receiver(Arc::clone(&subscriptions), cancel.clone());

            let applier = Arc::new(StoreApplier::new(Arc::clone(&store)));
            handle.spawn_anti_entropy(
                Arc::clone(&wal),
                Arc::clone(&applier),
                Arc::clone(&subscriptions),
                cancel.clone(),
            );

            // Pull anti-entropy: catch up from peers on restart (HTTP only).
            #[cfg(all(feature = "cluster-http", not(feature = "cluster")))]
            handle.spawn_pull_anti_entropy(
                Arc::clone(&wal),
                http_transport_ref,
                Arc::clone(&applier),
                Arc::clone(&subscriptions),
                cancel.clone(),
            );

            Some(handle)
        } else {
            info!("cluster not configured — running in single-node mode");
            None
        }
    };

    #[cfg(not(any(feature = "cluster", feature = "cluster-http", feature = "pg-sync")))]
    let _ = &subscriptions; // suppress unused warning in single-node builds

    let state = AppState {
        store: Arc::clone(&store),
        wal: Arc::clone(&wal),
        subscriptions: Arc::clone(&subscriptions),
        signer: Arc::clone(&signer),
        webhooks,
        #[cfg(any(feature = "cluster", feature = "cluster-http", feature = "pg-sync"))]
        cluster,
    };

    let gc_handle = tokio::spawn(run_presence_gc(
        Arc::clone(&store),
        Arc::clone(&subscriptions),
        cancel.clone(),
    ));

    let flush_handle = tokio::spawn(run_snapshot_flusher(Arc::clone(&store), cancel.clone()));

    let compact_handle = tokio::spawn(run_wal_compactor(
        Arc::clone(&store),
        Arc::clone(&wal),
        cancel.clone(),
    ));

    let router = build_router(state, auth_state).layer(Extension(prometheus_handle));

    let listener = TcpListener::bind(&config.bind).await?;
    info!(addr = config.bind, "Meridian listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            // ctrl_c() only fails if the platform doesn't support SIGINT — treat
            // that as a non-fatal condition and rely on SIGTERM / process kill.
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::warn!(error = %e, "failed to install Ctrl-C handler — server will not shut down gracefully on SIGINT");
            }
            info!("shutdown signal received");
            cancel.cancel();
        })
        .await?;

    let (gc, flush, compact) = tokio::join!(gc_handle, flush_handle, compact_handle);
    if let Err(e) = gc      { tracing::error!(error = %e, "presence GC task panicked"); }
    if let Err(e) = flush   { tracing::error!(error = %e, "snapshot flusher task panicked"); }
    if let Err(e) = compact { tracing::error!(error = %e, "WAL compactor task panicked"); }

    info!("Meridian stopped cleanly");
    Ok(())
}
