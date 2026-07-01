use dashmap::DashMap;
use tokio::sync::watch;
use tracing::debug;

/// Tracks active WebSocket sessions and allows forceful revocation.
///
/// Each session registers a `watch::Receiver<bool>`. Calling `revoke(ns, client_id)`
/// sends `true` on the sender, which the WS handler loop selects and uses to close
/// the socket immediately.
pub struct SessionRegistry {
    /// Key: `"{ns}\x00{client_id}"` — cheaply constructed, avoids nested maps.
    senders: DashMap<String, watch::Sender<bool>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self { senders: DashMap::new() }
    }

    fn key(ns: &str, client_id: u64) -> String {
        format!("{ns}\x00{client_id}")
    }

    /// Register a new session. Returns a receiver that fires when the session is revoked.
    /// Overwrites any previous registration for the same (ns, client_id) pair.
    pub fn register(&self, ns: &str, client_id: u64) -> watch::Receiver<bool> {
        let k = Self::key(ns, client_id);
        let (tx, rx) = watch::channel(false);
        self.senders.insert(k, tx);
        rx
    }

    /// Revoke the session identified by (ns, client_id).
    /// Returns `true` if the session was found and signalled, `false` if not connected.
    pub fn revoke(&self, ns: &str, client_id: u64) -> bool {
        let k = Self::key(ns, client_id);
        if let Some(entry) = self.senders.get(&k) {
            let _ = entry.send(true);
            debug!(ns, client_id, "session revoked");
            true
        } else {
            false
        }
    }

    /// Deregister a session on clean disconnect.
    pub fn deregister(&self, ns: &str, client_id: u64) {
        self.senders.remove(&Self::key(ns, client_id));
    }

    /// Number of active sessions across all namespaces.
    pub fn active_count(&self) -> usize {
        self.senders.len()
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn revoke_signals_receiver() {
        let reg = SessionRegistry::new();
        let mut rx = reg.register("ns", 1);
        assert!(!*rx.borrow());
        assert!(reg.revoke("ns", 1));
        rx.changed().await.unwrap();
        assert!(*rx.borrow());
    }

    #[tokio::test]
    async fn revoke_unknown_returns_false() {
        let reg = SessionRegistry::new();
        assert!(!reg.revoke("ns", 99));
    }

    #[tokio::test]
    async fn deregister_removes_entry() {
        let reg = SessionRegistry::new();
        let _rx = reg.register("ns", 1);
        assert_eq!(reg.active_count(), 1);
        reg.deregister("ns", 1);
        assert_eq!(reg.active_count(), 0);
        // After deregister, revoke returns false
        assert!(!reg.revoke("ns", 1));
    }

    #[tokio::test]
    async fn isolated_namespaces() {
        let reg = SessionRegistry::new();
        let mut rx_a = reg.register("ns-a", 1);
        let _rx_b = reg.register("ns-b", 1);
        reg.revoke("ns-a", 1);
        rx_a.changed().await.unwrap();
        // ns-b client 1 is not revoked
        assert!(!reg.revoke("ns-b", 99));
    }
}
