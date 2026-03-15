//! Persistence layer for FrogDB.
//!
//! This crate provides durable storage using RocksDB, including:
//! - Binary serialization of values
//! - Write-Ahead Logging (WAL) with configurable durability modes
//! - Snapshot abstractions for point-in-time backups
//!
//! Recovery logic that populates the in-memory store lives in `frogdb-core`,
//! since it depends on `HashMapStore` and other core types.

pub mod rocks;
pub mod serialization;
pub mod snapshot;
pub mod wal;

pub use rocks::{CompressionType, RocksConfig, RocksStore};
pub use serialization::{HEADER_SIZE, SerializationError, deserialize, serialize};
pub use snapshot::{
    NoopSnapshotCoordinator, OnWriteHook, PreSnapshotHook, RocksSnapshotCoordinator,
    SnapshotConfig, SnapshotCoordinator, SnapshotError, SnapshotHandle, SnapshotMetadata,
    SnapshotMetadataFile,
};
pub use wal::{DurabilityMode, RocksWalWriter, WalConfig, WalLagStats, spawn_periodic_sync};
