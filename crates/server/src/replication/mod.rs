//! Server-side replication handling.
//!
//! This module implements the replication protocol for both primary and replica roles:
//!
//! - `primary`: Handles incoming replica connections and WAL streaming
//! - `replica`: Handles connecting to primary and receiving replication data
//! - `fullsync`: Full resynchronization protocol (RDB transfer)
//! - `commands`: Replication-related command implementations (REPLICAOF, PSYNC, etc.)
//! - `executor`: Replica command executor for applying replicated commands

pub mod executor;
pub mod fullsync;
pub mod primary;
pub mod replica;

pub use crate::commands::replication::*;
pub use executor::{ReplicaCommandExecutor, ReplicationError, consume_frames};
pub use fullsync::FullSyncState;
pub use primary::PrimaryReplicationHandler;
pub use replica::{ReplicaConnection, ReplicaReplicationHandler};
