pub mod api;
pub mod auth;
#[cfg(feature = "cluster")]
pub mod cluster;
pub mod crdt;
pub mod metrics;
pub mod namespace;
pub mod rate_limit;
pub mod server;
pub mod storage;
pub mod tasks;
pub mod webhooks;

use std::sync::Arc;

use crate::{
    api::{handlers::AppStateExt, ws::{SubscriptionManager, WsState}},
    auth::TokenSigner,
    storage::{CrdtStore, WalBackend},
    webhooks::WebhookDispatcher,
};

/// All shared services, wrapped in Arc so axum can clone freely.
pub struct AppState<S: CrdtStore, W: WalBackend> {
    pub store: Arc<S>,
    pub wal: Arc<W>,
    pub subscriptions: Arc<SubscriptionManager>,
    pub signer: Arc<TokenSigner>,
    /// `None` when `MERIDIAN_WEBHOOK_URL` is not set.
    pub webhooks: Option<WebhookDispatcher>,
    /// `None` when `--features cluster` is not enabled or no cluster config found.
    #[cfg(feature = "cluster")]
    pub cluster: Option<Arc<meridian_cluster::ClusterHandle>>,
}

// Manual Clone impl — Arc<S> and Arc<W> are always Clone regardless of S/W bounds.
impl<S: CrdtStore, W: WalBackend> Clone for AppState<S, W> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            wal: Arc::clone(&self.wal),
            subscriptions: Arc::clone(&self.subscriptions),
            signer: Arc::clone(&self.signer),
            webhooks: self.webhooks.clone(),
            #[cfg(feature = "cluster")]
            cluster: self.cluster.clone(),
        }
    }
}

impl<S: CrdtStore, W: WalBackend> AppStateExt for AppState<S, W> {
    type S = S;
    type W = W;

    fn store(&self) -> &Self::S {
        &self.store
    }

    fn subscriptions(&self) -> &Arc<SubscriptionManager> {
        &self.subscriptions
    }

    fn signer(&self) -> &Arc<TokenSigner> {
        &self.signer
    }

    fn wal(&self) -> &Arc<W> {
        &self.wal
    }

    fn webhooks(&self) -> Option<&WebhookDispatcher> {
        self.webhooks.as_ref()
    }

    #[cfg(feature = "cluster")]
    fn cluster(&self) -> Option<&Arc<meridian_cluster::ClusterHandle>> {
        self.cluster.as_ref()
    }
}

impl<S: CrdtStore, W: WalBackend> WsState for AppState<S, W> {
    type S = S;

    fn store(&self) -> &Self::S {
        &self.store
    }

    fn subscriptions(&self) -> &Arc<SubscriptionManager> {
        &self.subscriptions
    }

    fn webhooks(&self) -> Option<&WebhookDispatcher> {
        self.webhooks.as_ref()
    }

    #[cfg(feature = "cluster")]
    fn cluster(&self) -> Option<&Arc<meridian_cluster::ClusterHandle>> {
        self.cluster.as_ref()
    }
}
