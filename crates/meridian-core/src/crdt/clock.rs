use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A compact vector clock mapping client_id → version.
///
/// Uses `BTreeMap` (not `HashMap`) for deterministic serialization order,
/// which is required for correct comparison and hashing.
///
/// `u32` versions: 4 bytes vs 8 bytes — matters over many sync frames.
/// Overflow at ~4 billion writes per client — not a practical concern.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorClock {
    /// Maps client_id → highest version seen from that client.
    pub entries: BTreeMap<u64, u32>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the clock for `client_id` and return the new version.
    pub fn increment(&mut self, client_id: u64) -> u32 {
        let v = self.entries.entry(client_id).or_insert(0);
        *v = v.wrapping_add(1);
        *v
    }

    /// Return the current version for `client_id` (0 if unseen).
    pub fn get(&self, client_id: u64) -> u32 {
        self.entries.get(&client_id).copied().unwrap_or(0)
    }

    /// True if self has seen everything `other` has seen (component-wise ≥).
    pub fn dominates(&self, other: &VectorClock) -> bool {
        other.entries.iter().all(|(id, &v)| self.get(*id) >= v)
    }

    /// True if neither clock dominates the other (concurrent).
    pub fn concurrent_with(&self, other: &VectorClock) -> bool {
        !self.dominates(other) && !other.dominates(self)
    }

    /// Component-wise maximum (lattice join).
    pub fn merge(&mut self, other: &VectorClock) {
        for (&id, &v) in &other.entries {
            let entry = self.entries.entry(id).or_insert(0);
            if v > *entry {
                *entry = v;
            }
        }
    }
}

/// Hybrid Logical Clock: (wall_ms, logical, node_id).
///
/// Combines physical time with a logical tie-breaker to give:
/// - Causality guarantees (like Lamport clocks)
/// - Proximity to wall time (useful for TTL semantics)
/// - Total order via `Ord` (derived in field order: wall_ms, logical, node_id)
///
/// The `Ord` derivation is intentional — field order gives correct ordering:
/// wall_ms is primary, logical breaks ties within the same millisecond,
/// node_id makes the ordering total even for perfectly concurrent events.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct HybridLogicalClock {
    /// Milliseconds since Unix epoch.
    pub wall_ms: u64,
    /// Logical counter — tie-breaker within the same millisecond.
    pub logical: u16,
    /// Node/client identifier — makes the order total.
    pub node_id: u64,
}

impl HybridLogicalClock {
    pub fn new(node_id: u64) -> Self {
        Self {
            wall_ms: now_ms(),
            logical: 0,
            node_id,
        }
    }

    /// Create a zero clock (useful as a "before everything" sentinel).
    pub fn zero(node_id: u64) -> Self {
        Self {
            wall_ms: 0,
            logical: 0,
            node_id,
        }
    }

    /// Advance the clock on a local event.
    /// Returns the new clock value to stamp the event with.
    pub fn tick(&mut self, wall_now_ms: u64) -> HybridLogicalClock {
        if wall_now_ms > self.wall_ms {
            self.wall_ms = wall_now_ms;
            self.logical = 0;
        } else {
            // Same or past wall time — advance logical counter.
            self.logical = self.logical.saturating_add(1);
        }
        *self
    }

    /// Advance the clock on receiving a message with `received` HLC.
    /// NTP-safe: handles backward clock jumps gracefully.
    pub fn receive(&mut self, wall_now_ms: u64, received: &HybridLogicalClock) -> HybridLogicalClock {
        let max_wall = wall_now_ms.max(received.wall_ms).max(self.wall_ms);

        self.logical = if max_wall == self.wall_ms && max_wall == received.wall_ms {
            self.logical.max(received.logical).saturating_add(1)
        } else if max_wall == self.wall_ms {
            self.logical.saturating_add(1)
        } else if max_wall == received.wall_ms {
            received.logical.saturating_add(1)
        } else {
            0
        };

        self.wall_ms = max_wall;
        *self
    }

    /// True if `self` is older than `ttl_ms` relative to `current` wall time.
    /// Uses `>=` so that ttl=0 entries expire at exactly their creation time.
    pub fn is_expired(&self, current_wall_ms: u64, ttl_ms: u64) -> bool {
        current_wall_ms >= self.wall_ms.saturating_add(ttl_ms)
    }
}

/// Current Unix timestamp in milliseconds.
pub fn now_ms() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        js_sys::Date::now() as u64
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_clock_increment() {
        let mut vc = VectorClock::new();
        assert_eq!(vc.increment(1), 1);
        assert_eq!(vc.increment(1), 2);
        assert_eq!(vc.increment(2), 1);
        assert_eq!(vc.get(1), 2);
        assert_eq!(vc.get(99), 0);
    }

    #[test]
    fn vector_clock_dominates() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.increment(1);
        a.increment(1);
        b.increment(1);
        assert!(a.dominates(&b));
        assert!(!b.dominates(&a));
    }

    #[test]
    fn vector_clock_concurrent() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.increment(1);
        b.increment(2);
        assert!(a.concurrent_with(&b));
    }

    #[test]
    fn vector_clock_merge() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.increment(1);
        a.increment(1); // a[1]=2
        b.increment(1); // b[1]=1
        b.increment(2); // b[2]=1
        a.merge(&b);
        assert_eq!(a.get(1), 2); // max(2,1)
        assert_eq!(a.get(2), 1); // from b
    }

    #[test]
    fn hlc_ordering() {
        let a = HybridLogicalClock { wall_ms: 100, logical: 0, node_id: 1 };
        let b = HybridLogicalClock { wall_ms: 100, logical: 1, node_id: 1 };
        let c = HybridLogicalClock { wall_ms: 101, logical: 0, node_id: 0 };
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn hlc_tick_advances() {
        let mut hlc = HybridLogicalClock { wall_ms: 100, logical: 5, node_id: 1 };
        // Same wall time → advance logical
        let ts = hlc.tick(100);
        assert_eq!(ts.wall_ms, 100);
        assert_eq!(ts.logical, 6);
        // Future wall time → reset logical
        let ts2 = hlc.tick(200);
        assert_eq!(ts2.wall_ms, 200);
        assert_eq!(ts2.logical, 0);
    }

    #[test]
    fn hlc_expiry() {
        let hlc = HybridLogicalClock { wall_ms: 1000, logical: 0, node_id: 1 };
        assert!(!hlc.is_expired(1500, 1000)); // 1500 < 2000 → alive
        assert!(hlc.is_expired(2000, 1000));  // 2000 >= 2000 → expired (ttl=0 edge case)
        assert!(hlc.is_expired(2001, 1000));  // 2001 >= 2000 → expired
    }
}
