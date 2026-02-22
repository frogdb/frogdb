//! Persistence layer for FrogDB.
//!
//! This module provides durable storage using RocksDB, including:
//! - Binary serialization of values
//! - Write-Ahead Logging (WAL) with configurable durability modes
//! - Recovery from persistent storage on startup
//! - Snapshot abstractions for point-in-time backups

mod recovery;
mod rocks;
mod serialization;
mod snapshot;
mod wal;

pub use recovery::{RecoveryStats, recover_all_shards, recover_shard};
pub use rocks::{CompressionType, RocksConfig, RocksStore};
pub use serialization::{HEADER_SIZE, SerializationError, deserialize, serialize};
pub use snapshot::{
    NoopSnapshotCoordinator, OnWriteHook, RocksSnapshotCoordinator, SnapshotConfig,
    SnapshotCoordinator, SnapshotError, SnapshotHandle, SnapshotMetadata, SnapshotMetadataFile,
};
pub use wal::{DurabilityMode, RocksWalWriter, WalConfig, WalLagStats, spawn_periodic_sync};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_harness;

#[cfg(test)]
mod crash_recovery_tests;
