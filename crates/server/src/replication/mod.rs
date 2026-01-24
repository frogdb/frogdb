//! Server-side replication handling.
//!
//! This module implements the replication protocol for both primary and replica roles:
//!
//! - `primary`: Handles incoming replica connections and WAL streaming
//! - `replica`: Handles connecting to primary and receiving replication data
//! - `fullsync`: Full resynchronization protocol (RDB transfer)
//! - `commands`: Replication-related command implementations (REPLICAOF, PSYNC, etc.)

pub mod commands;
pub mod fullsync;
pub mod primary;
pub mod replica;

pub use commands::*;
pub use fullsync::FullSyncState;
pub use primary::PrimaryReplicationHandler;
pub use replica::{ReplicaConnection, ReplicaReplicationHandler};
