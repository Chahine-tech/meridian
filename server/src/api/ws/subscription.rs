use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::broadcast;
use tracing::debug;

use super::protocol::ServerMsg;

// ---------------------------------------------------------------------------
// SubscriptionManager
// ---------------------------------------------------------------------------

/// Fan-out hub: maps namespace → broadcast channel.
///
/// `broadcast::Sender<Arc<ServerMsg>>` — Arc avoids cloning the payload
/// for each subscriber. A single `send()` delivers to all active receivers.
///
/// ## Channel lifecycle
/// A channel is created on first `subscribe()`. It is pruned when a new
/// `subscribe()` finds `receiver_count() == 0` (all previous subscribers gone).
///
/// ## Capacity
/// 256 messages per channel. A lagging receiver gets `RecvError::Lagged` and
/// must be disconnected by the WebSocket handler.
pub struct SubscriptionManager {
    channels: DashMap<String, broadcast::Sender<Arc<ServerMsg>>>,
}

const CHANNEL_CAPACITY: usize = 256;

impl SubscriptionManager {
    pub fn new() -> Self {
        Self { channels: DashMap::new() }
    }

    /// Subscribe to a namespace. Returns a receiver for incoming server messages.
    pub fn subscribe(&self, ns: &str) -> broadcast::Receiver<Arc<ServerMsg>> {
        // If existing channel has no receivers, replace it with a fresh one.
        // This avoids accumulating dead senders without a background cleanup task.
        let needs_new = self
            .channels
            .get(ns)
            .map(|s| s.receiver_count() == 0)
            .unwrap_or(true);

        if needs_new {
            let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
            self.channels.insert(ns.to_owned(), tx);
            debug!(ns, "created new broadcast channel");
            return rx;
        }

        // SAFETY: we checked `needs_new = false` so the entry exists.
        // Clone the sender then drop the guard before any await — no guard crosses an await here.
        self.channels.get(ns).expect("entry exists").subscribe()
    }

    /// Publish a message to all subscribers of `ns`. No-op if no subscribers.
    pub fn publish(&self, ns: &str, msg: Arc<ServerMsg>) {
        if let Some(sender) = self.channels.get(ns) {
            // send() is sync and returns Err if 0 receivers — silently drop.
            let _ = sender.send(msg);
        }
    }

    /// Number of active receivers for `ns`.
    pub fn subscriber_count(&self, ns: &str) -> usize {
        self.channels
            .get(ns)
            .map(|s| s.receiver_count())
            .unwrap_or(0)
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscribe_and_publish() {
        let mgr = SubscriptionManager::new();
        let mut rx = mgr.subscribe("my-room");

        let msg = Arc::new(ServerMsg::Ack { seq: 1 });
        mgr.publish("my-room", msg.clone());

        let received = rx.recv().await.unwrap();
        assert!(matches!(*received, ServerMsg::Ack { seq: 1 }));
    }

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let mgr = SubscriptionManager::new();
        let mut rx1 = mgr.subscribe("room");
        let mut rx2 = mgr.subscribe("room");

        mgr.publish("room", Arc::new(ServerMsg::Ack { seq: 42 }));

        assert!(matches!(*rx1.recv().await.unwrap(), ServerMsg::Ack { seq: 42 }));
        assert!(matches!(*rx2.recv().await.unwrap(), ServerMsg::Ack { seq: 42 }));
    }

    #[tokio::test]
    async fn publish_to_unknown_ns_is_noop() {
        let mgr = SubscriptionManager::new();
        // Should not panic
        mgr.publish("ghost-room", Arc::new(ServerMsg::Ack { seq: 0 }));
    }

    #[tokio::test]
    async fn subscriber_count_correct() {
        let mgr = SubscriptionManager::new();
        assert_eq!(mgr.subscriber_count("ns"), 0);

        let _rx1 = mgr.subscribe("ns");
        assert_eq!(mgr.subscriber_count("ns"), 1);

        let _rx2 = mgr.subscribe("ns");
        assert_eq!(mgr.subscriber_count("ns"), 2);
    }

    #[tokio::test]
    async fn different_namespaces_isolated() {
        let mgr = SubscriptionManager::new();
        let mut rx_a = mgr.subscribe("ns-a");
        let mut rx_b = mgr.subscribe("ns-b");

        mgr.publish("ns-a", Arc::new(ServerMsg::Ack { seq: 1 }));

        // ns-a receives
        assert!(rx_a.recv().await.is_ok());
        // ns-b should NOT have received anything
        assert!(rx_b.try_recv().is_err());
    }
}
