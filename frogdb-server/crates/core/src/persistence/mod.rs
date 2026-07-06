//! Persistence layer for FrogDB.
//!
//! The core storage, serialization, WAL, snapshot, and recovery-protocol
//! implementations live in the `frogdb-persistence` crate. This module
//! re-exports them and adds the store-side recovery adapter that binds the
//! format's recovery protocol to `HashMapStore` and other core types.

mod store_recovery;

// Re-export everything from frogdb-persistence for backward compatibility
// (this includes `RecoveryStats`, `RecoveryError`, `RestoreSink`, and the
// format-level `recovery` protocol module).
pub use frogdb_persistence::*;
// Re-export submodules so that `crate::persistence::rocks::*` etc. still work.
pub use frogdb_persistence::{rocks, serialization, snapshot, wal};

pub use store_recovery::{recover_all_shards, recover_shard};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_harness;

#[cfg(test)]
mod crash_recovery_tests;
