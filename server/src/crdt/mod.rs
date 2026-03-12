pub mod clock;
pub mod gcounter;
pub mod lwwregister;
pub mod orset;
pub mod pncounter;
pub mod presence;
pub mod registry;

pub use clock::{HybridLogicalClock, VectorClock};
pub use gcounter::GCounter;
pub use lwwregister::LwwRegister;
pub use orset::ORSet;
pub use pncounter::PNCounter;
pub use presence::Presence;
pub use registry::CrdtValue;

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CrdtError {
    #[error("serialization failed: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),

    #[error("deserialization failed: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    #[error("invalid operation: {0}")]
    InvalidOp(String),

    #[error("clock drift too large: client={client_ms}ms server={server_ms}ms diff={diff_ms}ms")]
    ClockDrift {
        client_ms: u64,
        server_ms: u64,
        diff_ms: u64,
    },
}

/// Core CRDT trait.
///
/// Implementations must satisfy the lattice join axioms:
/// - **Commutativity**:  merge(a, b) == merge(b, a)
/// - **Associativity**:  merge(merge(a, b), c) == merge(a, merge(b, c))
/// - **Idempotency**:    merge(a, a) == a
pub trait Crdt: Clone + Send + Sync + 'static {
    /// A client-initiated mutation.
    type Op: Clone + Send + Sync + Serialize + DeserializeOwned;

    /// An incremental state fragment for efficient sync.
    /// Must satisfy: merge_delta(state, delta_since(state, vc)) leaves state unchanged
    /// if state already incorporates everything in vc.
    type Delta: Clone + Send + Sync + Serialize + DeserializeOwned;

    /// The observable value clients read.
    type Value: Serialize;

    /// Apply a client operation to local state.
    /// Returns the resulting delta (to be broadcast) if state actually changed.
    fn apply(&mut self, op: Self::Op) -> Result<Option<Self::Delta>, CrdtError>;

    /// Merge another full CRDT state into self (lattice join).
    /// Must be idempotent, commutative, and associative.
    fn merge(&mut self, other: &Self);

    /// Merge an incremental delta into self.
    /// Must be idempotent: merge_delta(s, delta_since(s, vc)) is a no-op.
    fn merge_delta(&mut self, delta: Self::Delta);

    /// Compute the delta that brings a client from `since` up to the current state.
    /// Returns None if the client is already up to date.
    fn delta_since(&self, since: &VectorClock) -> Option<Self::Delta>;

    /// The current observable value.
    fn value(&self) -> Self::Value;

    /// True if this CRDT holds no data (initial / empty state).
    fn is_empty(&self) -> bool;
}
