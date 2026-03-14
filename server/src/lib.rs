pub mod api;
pub mod auth;
pub mod crdt;
pub mod metrics;
pub mod namespace;
pub mod rate_limit;
pub mod storage;
pub mod tasks;

use std::sync::Arc;

use crate::{
    api::{handlers::AppStateExt, ws::{SubscriptionManager, WsState}},
    auth::TokenSigner,
    storage::{SledStore, Wal},
};

// ---------------------------------------------------------------------------
// AppState — concrete shared state injected into every handler
// ---------------------------------------------------------------------------

/// All shared services, wrapped in Arc so axum can clone freely.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<SledStore>,
    pub subscriptions: Arc<SubscriptionManager>,
    pub signer: Arc<TokenSigner>,
}

impl AppStateExt for AppState {
    type S = SledStore;

    fn store(&self) -> &Self::S {
        &self.store
    }

    fn subscriptions(&self) -> &Arc<SubscriptionManager> {
        &self.subscriptions
    }

    fn signer(&self) -> &Arc<TokenSigner> {
        &self.signer
    }

    fn wal(&self) -> &Arc<Wal> {
        &self.store.wal
    }
}

impl WsState for AppState {
    type S = SledStore;

    fn store(&self) -> &Self::S {
        &self.store
    }

    fn subscriptions(&self) -> &Arc<SubscriptionManager> {
        &self.subscriptions
    }
}
