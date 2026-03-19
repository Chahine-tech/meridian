// Re-export all pure CRDT types from meridian-core.
// Server-internal code using `crate::crdt::*` continues to work unchanged.
pub use meridian_core::crdt::*;
pub use meridian_core::crdt::{
    clock, crdtmap, gcounter, lwwregister, orset, pncounter, presence, registry,
};

// Server-only: atomic apply (glues core logic + storage layer)
pub mod ops;
