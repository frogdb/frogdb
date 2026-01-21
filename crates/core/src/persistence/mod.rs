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

pub use recovery::{recover_all_shards, recover_shard, RecoveryStats};
pub use rocks::{CompressionType, RocksConfig, RocksStore};
pub use serialization::{deserialize, serialize, SerializationError, HEADER_SIZE};
pub use snapshot::{
    NoopSnapshotCoordinator, OnWriteHook, RocksSnapshotCoordinator, SnapshotConfig,
    SnapshotCoordinator, SnapshotError, SnapshotHandle, SnapshotMetadata, SnapshotMetadataFile,
};
pub use wal::{spawn_periodic_sync, DurabilityMode, RocksWalWriter, WalConfig};

#[cfg(test)]
mod tests;
