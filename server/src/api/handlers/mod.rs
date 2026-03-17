pub mod crdt;
pub mod history;
pub mod metrics;
pub mod tokens;

use std::sync::Arc;

use crate::{auth::TokenSigner, storage::{CrdtStore, WalBackend}, api::ws::SubscriptionManager, webhooks::WebhookDispatcher};

/// Trait implemented by `AppState` — gives handlers access to shared services
/// without depending on the concrete `AppState` struct directly.
pub trait AppStateExt: Clone + Send + Sync + 'static {
    type S: CrdtStore;
    type W: WalBackend;
    fn store(&self) -> &Self::S;
    fn subscriptions(&self) -> &Arc<SubscriptionManager>;
    fn signer(&self) -> &Arc<TokenSigner>;
    fn wal(&self) -> &Arc<Self::W>;
    fn webhooks(&self) -> Option<&WebhookDispatcher>;

    /// Returns the cluster handle if clustering is enabled.
    #[cfg(any(feature = "cluster", feature = "cluster-http"))]
    fn cluster(&self) -> Option<&Arc<meridian_cluster::ClusterHandle>> {
        None
    }
}
