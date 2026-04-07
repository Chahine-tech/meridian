use std::collections::VecDeque;
use std::sync::Mutex;

use meridian_core::protocol::ClientMsg;

/// Maximum number of ops buffered while disconnected.
/// When full, the oldest op is dropped (sliding window).
pub const MAX_PENDING: usize = 100;

pub struct OpQueue {
    inner: Mutex<VecDeque<ClientMsg>>,
}

impl OpQueue {
    pub fn new() -> Self {
        Self { inner: Mutex::new(VecDeque::with_capacity(MAX_PENDING)) }
    }

    /// Push an op. If the queue is at capacity, the oldest entry is dropped.
    pub fn push(&self, msg: ClientMsg) {
        let mut q = self.inner.lock().expect("op_queue lock poisoned");
        if q.len() >= MAX_PENDING {
            q.pop_front();
        }
        q.push_back(msg);
    }

    /// Drain all queued ops in FIFO order, leaving the queue empty.
    pub fn drain(&self) -> Vec<ClientMsg> {
        let mut q = self.inner.lock().expect("op_queue lock poisoned");
        q.drain(..).collect()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("op_queue lock poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for OpQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(id: &str) -> ClientMsg {
        ClientMsg::Subscribe { crdt_id: id.into() }
    }

    #[test]
    fn push_and_drain() {
        let q = OpQueue::new();
        q.push(sub("a"));
        q.push(sub("b"));
        assert_eq!(q.len(), 2);
        let drained = q.drain();
        assert_eq!(drained.len(), 2);
        assert!(q.is_empty());
    }

    #[test]
    fn sliding_window_eviction() {
        let q = OpQueue::new();
        // Fill to capacity + 1
        for i in 0..=MAX_PENDING {
            q.push(sub(&format!("{i}")));
        }
        assert_eq!(q.len(), MAX_PENDING);
        // The oldest ("0") was evicted; "1" is now first
        let drained = q.drain();
        assert!(matches!(&drained[0], ClientMsg::Subscribe { crdt_id } if crdt_id == "1"));
    }

    #[test]
    fn drain_is_fifo() {
        let q = OpQueue::new();
        q.push(sub("first"));
        q.push(sub("second"));
        q.push(sub("third"));
        let drained = q.drain();
        let ids: Vec<&str> = drained.iter().map(|m| match m {
            ClientMsg::Subscribe { crdt_id } => crdt_id.as_str(),
            _ => "",
        }).collect();
        assert_eq!(ids, ["first", "second", "third"]);
    }
}
