use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;

const RATE_LIMIT_MAX_REQUESTS: usize = 100;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);

/// Sliding-window rate limiter keyed by token (client_id + namespace).
///
/// Each entry holds a ring-buffer of recent request timestamps.
/// A request is allowed if fewer than `RATE_LIMIT_MAX_REQUESTS` timestamps
/// fall within the last `RATE_LIMIT_WINDOW`.
#[derive(Clone)]
pub struct RateLimiter {
    windows: Arc<DashMap<String, Vec<Instant>>>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self {
            windows: Arc::new(DashMap::new()),
        }
    }
}

impl RateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if the request is allowed, `false` if rate-limited.
    pub fn check(&self, key: &str) -> bool {
        let now = Instant::now();
        let cutoff = now - RATE_LIMIT_WINDOW;

        let mut entry = self.windows.entry(key.to_owned()).or_default();
        // Drop timestamps outside the window.
        entry.retain(|&timestamp| timestamp > cutoff);

        if entry.len() >= RATE_LIMIT_MAX_REQUESTS {
            return false;
        }

        entry.push(now);
        true
    }
}
