pub mod crdt;
pub mod tokens;

use std::sync::Arc;

use crate::{auth::TokenSigner, storage::Store, api::ws::SubscriptionManager};

/// Trait implemented by `AppState` — gives handlers access to shared services
/// without depending on the concrete `AppState` struct directly.
pub trait AppStateExt: Clone + Send + Sync + 'static {
    type S: Store;
    fn store(&self) -> &Self::S;
    fn subscriptions(&self) -> &Arc<SubscriptionManager>;
    fn signer(&self) -> &Arc<TokenSigner>;
}
