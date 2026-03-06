//! Replication module for FrogDB.
//!
//! This module provides the core abstractions and implementations for
//! primary-replica replication:
//!
//! - `ReplicationState`: Tracks replication IDs and offsets
//! - `ReplicationTracker`: Concrete implementation for tracking replica ACKs
//! - Frame encoding/decoding for WAL streaming
//!
//! # Architecture
//!
//! FrogDB uses an async replication model similar to Redis:
//!
//! ```text
//! Primary                         Replica
//!    │                               │
//!    │◄──────── PSYNC ───────────────│
//!    │                               │
//!    │──── FULLRESYNC/CONTINUE ─────►│
//!    │                               │
//!    │──── WAL Frames ──────────────►│
//!    │          ...                  │
//!    │◄──────── ACK ─────────────────│
//! ```
//!
//! # Protocol
//!
//! The replication protocol follows Redis PSYNC2:
//! - PSYNC <repl_id> <offset> - Partial sync request
//! - FULLRESYNC <repl_id> <offset> - Full resync response
//! - CONTINUE - Continue with partial sync
//! - REPLCONF ACK <offset> - Replica acknowledges offset

pub mod frame;
pub mod fullsync;
pub mod primary;
pub mod replica;
pub mod split_brain_log;
pub mod state;
pub mod tracker;

pub use frame::{
    FRAME_MAGIC, FRAME_VERSION, ReplicationFrame, ReplicationFrameCodec, serialize_command_to_resp,
};
pub use fullsync::{FullSyncMetadata, FullSyncState};
pub use primary::{LagThresholdConfig, PrimaryReplicationHandler, SplitBrainBufferConfig};
pub use replica::{ReplicaConnection, ReplicaReplicationHandler};
pub use state::ReplicationState;
pub use tracker::{ReplicaInfo, ReplicationTrackerImpl};

use bytes::Bytes;
use std::sync::Arc;

/// Trait for broadcasting replication frames to replicas.
///
/// This trait decouples the shard workers from the full replication handler,
/// allowing shards to broadcast write commands without knowing about the
/// underlying replication implementation.
pub trait ReplicationBroadcaster: Send + Sync {
    /// Broadcast a command to all replicas.
    ///
    /// # Arguments
    /// * `cmd_name` - The command name (e.g., "SET")
    /// * `args` - The command arguments
    ///
    /// # Returns
    /// The new replication offset after this command.
    fn broadcast_command(&self, cmd_name: &str, args: &[Bytes]) -> u64;

    /// Check if replication is active (has connected replicas).
    fn is_active(&self) -> bool;

    /// Get the current replication offset.
    fn current_offset(&self) -> u64;

    /// Extract commands with offset > `last_replicated_offset` from the ring buffer.
    /// Returns `(offset, RESP-encoded command)` pairs in order. Non-destructive.
    fn extract_divergent_writes(&self, last_replicated_offset: u64) -> Vec<(u64, Bytes)>;
}

/// No-op broadcaster for when not running as primary or no replicas connected.
pub struct NoopBroadcaster;

impl ReplicationBroadcaster for NoopBroadcaster {
    fn broadcast_command(&self, _cmd_name: &str, _args: &[Bytes]) -> u64 {
        0
    }

    fn is_active(&self) -> bool {
        false
    }

    fn current_offset(&self) -> u64 {
        0
    }

    fn extract_divergent_writes(&self, _last_replicated_offset: u64) -> Vec<(u64, Bytes)> {
        Vec::new()
    }
}

/// A type alias for the broadcaster wrapped in an Arc.
pub type SharedBroadcaster = Arc<dyn ReplicationBroadcaster>;
