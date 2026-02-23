//! Server-side replication handling.
//!
//! The core replication protocol (frame encoding, state management, tracker,
//! primary/replica handlers, fullsync) lives in the `frogdb-replication` crate.
//!
//! This module provides the executor, which applies replicated commands to shards.

pub mod executor;

pub use crate::commands::replication::*;
pub use executor::{ReplicaCommandExecutor, ReplicationError, consume_frames};

// Re-export from frogdb-replication for backward compatibility.
pub use frogdb_replication::{
    FullSyncState, PrimaryReplicationHandler, ReplicaConnection, ReplicaReplicationHandler,
};
