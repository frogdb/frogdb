//! Server-side replication handling.
//!
//! The core replication protocol (frame encoding, state management, tracker,
//! primary/replica handlers, fullsync) lives in the `frogdb-replication` crate,
//! and the shard-touching runtime behind its injected seams (replica apply,
//! full-resync export/install, the replica-loss write fence) lives in
//! `frogdb-replication-runtime`.
//!
//! What is left here is the connection-coupled half: the `REPLICAOF` /
//! `REPLCONF` / `PSYNC` command handlers. This module re-exports the other two
//! crates so the server's call sites keep a single `crate::replication` facade.

pub use crate::commands::replication::*;
// Transaction reconstruction + the consume loop live in the replication crate;
// the server contributes only the `ReplicaApplier` impl (`ReplicaCommandExecutor`),
// which lives in the replication-runtime crate.
pub use frogdb_replication::{ApplyError, ReplicaApplier, consume_frames};
pub use frogdb_replication_runtime::{
    LiveSnapshotInstaller, ReplicaCommandExecutor, live_snapshot_source,
};

// Re-export from frogdb-replication for backward compatibility.
pub use frogdb_replication::{
    LagThresholdConfig, LagThresholds, PrimaryReplicationHandler, ReplicaConnection,
    ReplicaReplicationHandler, ReplicationIdentity, SharedReplicationState, SplitBrainBufferConfig,
    WaitVerdict,
};
