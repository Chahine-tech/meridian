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
        Some(hex) => {
            info!("using signing key from MERIDIAN_SIGNING_KEY");
            Arc::new(TokenSigner::from_hex(&hex)?)
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
    let state = AppState {
        store: Arc::clone(&store),
        wal: Arc::clone(&wal),
        subscriptions: Arc::clone(&subscriptions),
        signer: Arc::clone(&signer),
        webhooks,
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
