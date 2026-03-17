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

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

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
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            bind: std::env::var("MERIDIAN_BIND").unwrap_or_else(|_| "0.0.0.0:3000".into()),
            data_dir: std::env::var("MERIDIAN_DATA_DIR").unwrap_or_else(|_| "./data".into()),
            signing_key_hex: std::env::var("MERIDIAN_SIGNING_KEY").ok(),
            database_url: std::env::var("DATABASE_URL").ok(),
            redis_url: std::env::var("REDIS_URL").ok(),
        }
    }

    #[cfg(any(feature = "cluster", feature = "cluster-http"))]
    fn bind_port(&self) -> u16 {
        self.bind
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .unwrap_or(3000)
    }
}

// ---------------------------------------------------------------------------
// run — wire all services and start the HTTP server
// ---------------------------------------------------------------------------

pub async fn run<S, W>(
    config: Config,
    prometheus_handle: PrometheusHandle,
    store: Arc<S>,
    wal: Arc<W>,
) -> anyhow::Result<()>
where
    S: CrdtStore + Store<CrdtValue>,
    W: WalBackend,
{
    // ----- Auth -----
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

    // ----- Graceful shutdown token -----
    let cancel = CancellationToken::new();

    // ----- Webhooks (optional) -----
    let webhooks = WebhookConfig::from_env().map(|webhook_config| {
        info!(url = webhook_config.url, "webhooks enabled");
        WebhookDispatcher::new(webhook_config, cancel.clone())
    });

    if webhooks.is_none() {
        info!("MERIDIAN_WEBHOOK_URL not set — webhooks disabled");
    }

    // ----- Shared state -----
    let subscriptions = Arc::new(SubscriptionManager::new());

    // ----- Cluster (optional) -----
    // Two mutually-exclusive modes:
    //   `--features cluster`      → Redis Pub/Sub (recommended, low latency)
    //   `--features cluster-http` → HTTP push (PostgreSQL-only deployments)
    #[cfg(any(feature = "cluster", feature = "cluster-http"))]
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
                tokio::spawn(async move {
                    let listener = TcpListener::bind(&internal_bind).await
                        .expect("failed to bind internal cluster port");
                    info!(addr = internal_bind, "cluster internal API listening");
                    axum::serve(listener, internal_router)
                        .with_graceful_shutdown(async move { cancel_clone.cancelled().await })
                        .await
                        .expect("internal cluster server failed");
                });

                let transport_dyn: Arc<dyn ClusterTransport> = t.clone();
                (transport_dyn, cfg, t)
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

    #[cfg(not(any(feature = "cluster", feature = "cluster-http")))]
    let _ = &subscriptions; // suppress unused warning in single-node builds

    let state = AppState {
        store: Arc::clone(&store),
        wal: Arc::clone(&wal),
        subscriptions: Arc::clone(&subscriptions),
        signer: Arc::clone(&signer),
        webhooks,
        #[cfg(any(feature = "cluster", feature = "cluster-http"))]
        cluster,
    };

    // ----- Background tasks -----
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

    // ----- HTTP server -----
    let router = build_router(state, auth_state).layer(Extension(prometheus_handle));

    let listener = TcpListener::bind(&config.bind).await?;
    info!(addr = config.bind, "Meridian listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl-C handler");
            info!("shutdown signal received");
            cancel.cancel();
        })
        .await?;

    let _ = tokio::join!(gc_handle, flush_handle, compact_handle);

    info!("Meridian stopped cleanly");
    Ok(())
}
