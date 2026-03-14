pub mod config;
pub mod dispatcher;

pub use config::WebhookConfig;
pub use dispatcher::WebhookDispatcher;

use serde::Serialize;

/// A webhook event fired after every successful CRDT op.
#[derive(Debug, Clone, Serialize)]
pub struct WebhookEvent {
    /// Namespace the op occurred in.
    pub ns: String,
    /// CRDT key that was mutated.
    pub crdt_id: String,
    /// Transport that delivered the op: `"http"` or `"ws"`.
    pub source: String,
    /// Unix timestamp (ms) when the op was applied server-side.
    pub timestamp_ms: u64,
}
