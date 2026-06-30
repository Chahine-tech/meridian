use std::{
    collections::HashMap,
    sync::Mutex,
};

use meridian_core::crdt::{HybridLogicalClock, VectorClock};

/// Per-namespace registry of connected clients and their observed VectorClocks.
///
/// The registry is updated when a client sends `ClientMsg::Sync` — the `since_vc`
/// in the Sync request tells us exactly what the client has already seen.
///
/// **Floor vector clock**: the component-wise minimum over all registered client
/// VCs. A tombstone whose INSERT is dominated by the floor VC has been observed by
/// every currently-connected client, making it safe to GC (once the offline-client
/// grace period has elapsed).
///
/// Clients are only entered into the registry on their first Sync — connections
/// that have never Synced are not counted, avoiding a starvation problem where a
/// newly-connected-but-idle client would block all GC.
///
/// On disconnect, the client is removed from the registry. This is conservative:
/// removing a client means we lose the "all clients have seen X" guarantee for
/// them — but after the grace period we still GC based on wall clock, so the
/// registry is a tightening constraint, never a permanent block.
#[derive(Default)]
pub struct ClientRegistry {
    /// namespace → (client_id → last-known VectorClock)
    inner: Mutex<HashMap<String, HashMap<u64, VectorClock>>>,
}

impl ClientRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a client has observed everything up to `vc` in namespace `ns`.
    ///
    /// The VC is merged (not replaced) so the entry only ever advances.
    pub fn update_vc(&self, ns: &str, client_id: u64, vc: &VectorClock) {
        let mut guard = self.inner.lock().unwrap();
        let ns_map = guard.entry(ns.to_owned()).or_default();
        ns_map.entry(client_id).or_default().merge(vc);
    }

    /// Remove a client from the registry (typically called on WebSocket disconnect).
    pub fn unregister(&self, ns: &str, client_id: u64) {
        let mut guard = self.inner.lock().unwrap();
        if let Some(ns_map) = guard.get_mut(ns) {
            ns_map.remove(&client_id);
            if ns_map.is_empty() {
                guard.remove(ns);
            }
        }
    }

    /// Compute the component-wise minimum VectorClock over all registered clients
    /// in `ns`. Returns `None` when no clients are registered (compactor should
    /// fall back to time-based GC in this case).
    pub fn floor_vc(&self, ns: &str) -> Option<VectorClock> {
        let guard = self.inner.lock().unwrap();
        let ns_map = guard.get(ns)?;
        if ns_map.is_empty() {
            return None;
        }

        // Collect all node_ids mentioned by any client.
        let all_node_ids: std::collections::BTreeSet<u64> = ns_map
            .values()
            .flat_map(|vc| vc.entries.keys().copied())
            .collect();

        let mut floor = VectorClock::new();
        for node_id in all_node_ids {
            // floor[node_id] = min over all clients of client_vc[node_id].
            // A client that has never mentioned this node_id contributes 0.
            let min_v = ns_map
                .values()
                .map(|vc| vc.get(node_id))
                .min()
                .unwrap_or(0);
            if min_v > 0 {
                floor.entries.insert(node_id, min_v);
            }
        }

        // If the floor is all-zeros, return None (no useful constraint).
        if floor.entries.is_empty() {
            return None;
        }

        Some(floor)
    }

    /// Number of registered clients across all namespaces. Used for metrics/debug.
    pub fn total_clients(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .values()
            .map(|m| m.len())
            .sum()
    }
}

/// Helper: is `hlc` dominated by `floor_vc`?
///
/// Returns `true` when `floor_vc[hlc.node_id] >= hlc.logical`, meaning every
/// registered client has seen the event timestamped by `hlc`.
///
/// Mirrors the condition used in `Rga::delta_since` / `TreeCrdt::delta_since`.
pub fn dominated_by_floor(hlc: HybridLogicalClock, floor_vc: &VectorClock) -> bool {
    floor_vc.get(hlc.node_id) >= u32::from(hlc.logical)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hlc(node_id: u64, logical: u16) -> HybridLogicalClock {
        HybridLogicalClock { wall_ms: 1, logical, node_id }
    }

    fn vc_with(node_id: u64, count: u32) -> VectorClock {
        let mut vc = VectorClock::new();
        for _ in 0..count {
            vc.increment(node_id);
        }
        vc
    }

    #[test]
    fn floor_vc_none_with_no_clients() {
        let reg = ClientRegistry::new();
        assert!(reg.floor_vc("ns").is_none());
    }

    #[test]
    fn floor_vc_none_when_all_clients_have_empty_vc() {
        let reg = ClientRegistry::new();
        // Client registered with an empty VC — contributes 0 to every component.
        reg.update_vc("ns", 1, &VectorClock::new());
        // floor is empty (all components are 0), so None is returned.
        assert!(reg.floor_vc("ns").is_none());
    }

    #[test]
    fn floor_vc_single_client() {
        let reg = ClientRegistry::new();
        let vc = vc_with(42, 5);
        reg.update_vc("ns", 1, &vc);
        let floor = reg.floor_vc("ns").expect("should have floor");
        assert_eq!(floor.get(42), 5);
    }

    #[test]
    fn floor_vc_is_component_wise_min() {
        let reg = ClientRegistry::new();
        // Client 1 has seen node 42 up to 5 and node 99 up to 3.
        let mut vc1 = vc_with(42, 5);
        vc1.merge(&vc_with(99, 3));
        reg.update_vc("ns", 1, &vc1);

        // Client 2 has seen node 42 up to 2 and node 99 up to 10.
        let mut vc2 = vc_with(42, 2);
        vc2.merge(&vc_with(99, 10));
        reg.update_vc("ns", 2, &vc2);

        let floor = reg.floor_vc("ns").expect("should have floor");
        assert_eq!(floor.get(42), 2, "floor[42] = min(5, 2) = 2");
        assert_eq!(floor.get(99), 3, "floor[99] = min(3, 10) = 3");
    }

    #[test]
    fn floor_vc_missing_node_in_one_client_gives_zero() {
        // Client 1 knows about node 42, client 2 doesn't.
        // Since client 2 contributes 0 for node 42, min = 0 → not in floor.
        let reg = ClientRegistry::new();
        reg.update_vc("ns", 1, &vc_with(42, 5));
        reg.update_vc("ns", 2, &VectorClock::new()); // knows nothing
        // min(5, 0) = 0 → excluded from floor → floor is empty → None
        assert!(reg.floor_vc("ns").is_none());
    }

    #[test]
    fn unregister_removes_client() {
        let reg = ClientRegistry::new();
        reg.update_vc("ns", 1, &vc_with(42, 3));
        reg.update_vc("ns", 2, &vc_with(42, 1));

        // With both registered: floor[42] = min(3,1) = 1.
        assert_eq!(reg.floor_vc("ns").unwrap().get(42), 1);

        // Unregister the slower client — remaining client has vc[42]=3.
        reg.unregister("ns", 2);
        assert_eq!(reg.floor_vc("ns").unwrap().get(42), 3);
    }

    #[test]
    fn unregister_last_client_clears_namespace() {
        let reg = ClientRegistry::new();
        reg.update_vc("ns", 7, &vc_with(1, 1));
        reg.unregister("ns", 7);
        assert!(reg.floor_vc("ns").is_none());
        assert_eq!(reg.total_clients(), 0);
    }

    #[test]
    fn update_vc_merges_not_replaces() {
        let reg = ClientRegistry::new();
        reg.update_vc("ns", 1, &vc_with(10, 5));
        // Second update with only a different node — should merge, not wipe node 10.
        reg.update_vc("ns", 1, &vc_with(20, 3));
        let floor = reg.floor_vc("ns").expect("should have floor");
        assert_eq!(floor.get(10), 5, "prior observations must be preserved");
        assert_eq!(floor.get(20), 3);
    }

    #[test]
    fn namespaces_are_isolated() {
        let reg = ClientRegistry::new();
        reg.update_vc("ns1", 1, &vc_with(1, 5));
        reg.update_vc("ns2", 1, &vc_with(1, 1));
        // ns1 and ns2 have independent floors.
        assert_eq!(reg.floor_vc("ns1").unwrap().get(1), 5);
        assert_eq!(reg.floor_vc("ns2").unwrap().get(1), 1);
        assert_eq!(reg.total_clients(), 2);
    }

    #[test]
    fn dominated_by_floor_true_when_covered() {
        let floor = vc_with(1, 5);
        assert!(dominated_by_floor(make_hlc(1, 3), &floor), "logical 3 <= 5 → dominated");
        assert!(dominated_by_floor(make_hlc(1, 5), &floor), "logical 5 == 5 → dominated");
    }

    #[test]
    fn dominated_by_floor_false_when_not_covered() {
        let floor = vc_with(1, 3);
        assert!(!dominated_by_floor(make_hlc(1, 4), &floor), "logical 4 > 3 → not dominated");
        // node_id=2 not in floor → floor.get(2)=0 < logical(1)
        assert!(!dominated_by_floor(make_hlc(2, 1), &floor), "unknown node → not dominated");
    }
}
