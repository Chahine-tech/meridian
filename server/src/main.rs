use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use meridian_server::{
    api::build_router,
    api::ws::SubscriptionManager,
    auth::TokenSigner,
    storage::SledStore,
    tasks::{run_presence_gc, run_snapshot_flusher},
    AppState,
};

// ---------------------------------------------------------------------------
// Config (from env vars)
// ---------------------------------------------------------------------------

struct Config {
    /// TCP bind address. Default: 0.0.0.0:3000
    bind: String,
    /// Path to sled data directory. Default: ./data
    data_dir: String,
    /// Hex-encoded 32-byte ed25519 seed. If unset, a random key is generated
    /// (dev mode — tokens won't survive restart).
    signing_key_hex: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        Self {
            bind: std::env::var("MERIDIAN_BIND").unwrap_or_else(|_| "0.0.0.0:3000".into()),
            data_dir: std::env::var("MERIDIAN_DATA_DIR").unwrap_or_else(|_| "./data".into()),
            signing_key_hex: std::env::var("MERIDIAN_SIGNING_KEY").ok(),
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Structured logging — RUST_LOG controls verbosity (default: info)
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::from_env();

    // ----- Storage -----
    let store = Arc::new(SledStore::open(&config.data_dir)?);
    info!(path = config.data_dir, "sled store opened");

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

    // ----- Shared state -----
    let subscriptions = Arc::new(SubscriptionManager::new());
    let state = AppState {
        store: Arc::clone(&store),
        subscriptions: Arc::clone(&subscriptions),
        signer: Arc::clone(&signer),
    };

    // ----- Graceful shutdown token -----
    let cancel = CancellationToken::new();

    // ----- Background tasks -----
    let gc_handle = tokio::spawn(run_presence_gc(
        Arc::clone(&store),
        Arc::clone(&subscriptions),
        cancel.clone(),
    ));

    let flush_handle = tokio::spawn(run_snapshot_flusher(
        Arc::clone(&store),
        cancel.clone(),
    ));

    // ----- HTTP server -----
    let router = build_router(state);
    let listener = TcpListener::bind(&config.bind).await?;
    info!(addr = config.bind, "Meridian listening");

    // Serve until Ctrl-C
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl-C handler");
            info!("shutdown signal received");
            cancel.cancel();
        })
        .await?;

    // Wait for background tasks to finish their final flush/gc tick.
    let _ = tokio::join!(gc_handle, flush_handle);

    info!("Meridian stopped cleanly");
    Ok(())
}
