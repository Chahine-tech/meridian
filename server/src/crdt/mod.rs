// Re-export all pure CRDT types from meridian-core.
// Server-internal code using `crate::crdt::*` continues to work unchanged.
pub use meridian_core::crdt::*;
pub use meridian_core::crdt::{
    clock, crdtmap, gcounter, lwwregister, orset, pncounter, presence, registry,
};

// Server-only: atomic apply (glues core logic + storage layer)
pub mod ops;

// Server-only: per-namespace client VectorClock registry for precise GC.
pub mod client_registry;
pub use client_registry::ClientRegistry;
