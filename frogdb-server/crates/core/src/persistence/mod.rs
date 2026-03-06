//! Persistence layer for FrogDB.
//!
//! The core storage, serialization, WAL, and snapshot implementations live in
//! the `frogdb-persistence` crate. This module re-exports them and adds
//! recovery logic that depends on `HashMapStore` and other core types.

mod recovery;

// Re-export everything from frogdb-persistence for backward compatibility.
pub use frogdb_persistence::*;
// Re-export submodules so that `crate::persistence::rocks::*` etc. still work.
pub use frogdb_persistence::{rocks, serialization, snapshot, wal};

pub use recovery::{RecoveryStats, recover_all_shards, recover_shard};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_harness;

#[cfg(test)]
mod crash_recovery_tests;
