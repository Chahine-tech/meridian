pub mod crdt;
pub mod history;
pub mod metrics;
pub mod tokens;

use std::sync::Arc;

use crate::{auth::TokenSigner, storage::{Store, Wal}, api::ws::SubscriptionManager, webhooks::WebhookDispatcher};

/// Trait implemented by `AppState` — gives handlers access to shared services
/// without depending on the concrete `AppState` struct directly.
pub trait AppStateExt: Clone + Send + Sync + 'static {
    type S: Store;
    fn store(&self) -> &Self::S;
    fn subscriptions(&self) -> &Arc<SubscriptionManager>;
    fn signer(&self) -> &Arc<TokenSigner>;
    fn wal(&self) -> &Arc<Wal>;
    fn webhooks(&self) -> Option<&WebhookDispatcher>;
}
