//! Persistence layer for FrogDB.
//!
//! This crate provides durable storage using RocksDB, including:
//! - Binary serialization of values
//! - Write-Ahead Logging (WAL) with configurable durability modes
//! - Snapshot abstractions for point-in-time backups
//! - Recovery of persisted state back into the store
//!
//! Recovery owns the *format's* read protocol (column-family iteration,
//! deserialization, expiry filtering, warm-tier precedence) but not how an
//! entry lands in memory: it drives a [`RestoreSink`] that `frogdb-core`
//! implements over its `HashMapStore`.

pub mod recovery;
pub mod rocks;
pub mod serialization;
pub mod snapshot;
pub mod wal;

pub use recovery::{
    RecoveryError, RecoveryStats, RestoreSink, recover_shard_into, recover_warm_shard_into,
};
pub use rocks::{CfTier, CompressionType, RocksConfig, RocksStore, StagedCheckpoint};
pub use serialization::{
    HEADER_SIZE, SerializationError, deserialize, merge_hll_serialized, partial_merge_hll_deltas,
    serialize, serialize_hll_delta,
};
pub use snapshot::{
    NoopSnapshotCoordinator, PreSnapshotHook, RocksSnapshotCoordinator, SnapshotConfig,
    SnapshotCoordinator, SnapshotError, SnapshotHandle, SnapshotMetadata, SnapshotMetadataFile,
    SnapshotMode, SnapshotRequest, SnapshotScheduler,
};
pub use wal::{
    DurabilityMode, FakeFailure, FakeWalLog, FakeWalSink, RecordedWalEffect, RocksWalWriter,
    WalConfig, WalEffectKind, WalFailurePolicy, WalLagStats, WalSink, spawn_periodic_sync,
};
