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

pub mod apply;
pub mod frame;
pub mod fullsync;
pub mod offset_coordinator;
pub mod primary;
pub mod replica;
pub mod replica_session;
pub mod split_brain_log;
pub mod state;
pub mod tracker;
pub mod wait_coordinator;

pub use apply::{ApplyError, ReplicaApplier, consume_frames, parse_frame_payload};
pub use frame::{
    CONTROL_SHARD, FRAME_MAGIC, FRAME_VERSION, ReplicationFrame, ReplicationFrameCodec,
    serialize_command_to_resp,
};
pub use fullsync::FullSyncMetadata;
pub use offset_coordinator::OffsetCoordinator;
pub use primary::{LagThresholdConfig, PrimaryReplicationHandler, SplitBrainBufferConfig};
pub use replica::{ReplicaConnection, ReplicaReplicationHandler};
pub use replica_session::{Phase, ReplicaCapabilities, ReplicaInfo, ReplicaSession, SyncKind};
pub use state::{
    ReplicationState, StagedReplicationMetadata, consume_staged_replication_metadata,
    read_staged_replication_metadata,
};
pub use tracker::ReplicationTrackerImpl;
pub use wait_coordinator::{AckSolicitor, WaitCoordinator, WaitVerdict};

use bytes::Bytes;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

/// Supertrait combining `AsyncRead + AsyncWrite` for use in trait objects.
pub trait AsyncReadWrite: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> AsyncReadWrite for T {}

/// A type-erased async I/O stream.
///
/// Used so the replication crate remains TLS-agnostic — the server crate
/// provides either a plain TCP or TLS-wrapped stream via this alias.
pub type BoxedStream = Box<dyn AsyncReadWrite + Unpin + Send>;

/// Trait for broadcasting replication frames to replicas.
///
/// This trait decouples the shard workers from the full replication handler,
/// allowing shards to broadcast write commands without knowing about the
/// underlying replication implementation.
pub trait ReplicationBroadcaster: Send + Sync {
    /// Broadcast a command tagged with the shard it executed on.
    ///
    /// The origin shard travels in the replication frame so the replica applies
    /// the write on that shard directly, instead of re-deriving routing from
    /// `args[0]` (wrong for keyless commands and MULTI/EXEC framing). No-op /
    /// test broadcasters that don't stream frames may ignore the tag; the real
    /// primary handler stamps the frame.
    ///
    /// This is the frame-emit interface every broadcaster must implement — the
    /// untagged (`CONTROL_SHARD`) path and backlog introspection live on the
    /// concrete [`primary::PrimaryReplicationHandler`], not here.
    ///
    /// # Returns
    /// The new replication offset after this command.
    fn broadcast_command_on_shard(&self, shard_id: u16, cmd_name: &str, args: &[Bytes]) -> u64;

    /// Check if replication is active (has connected replicas).
    fn is_active(&self) -> bool;

    /// Get the current replication offset.
    fn current_offset(&self) -> u64;

    /// Broadcast a transaction atomically, tagging every frame of the group
    /// (`MULTI`, each command, `EXEC`) with the shard it executed on.
    ///
    /// A MULTI/EXEC transaction runs entirely on one shard, so the whole group
    /// carries a single origin shard. The replica reconstructs the group and
    /// applies it atomically on that shard. This is the single definition of the
    /// MULTI/EXEC framing.
    fn broadcast_transaction_on_shard(&self, shard_id: u16, commands: &[(&str, &[Bytes])]) -> u64 {
        self.broadcast_command_on_shard(shard_id, "MULTI", &[]);
        for &(cmd_name, args) in commands {
            self.broadcast_command_on_shard(shard_id, cmd_name, args);
        }
        self.broadcast_command_on_shard(shard_id, "EXEC", &[])
    }
}

/// No-op broadcaster for when not running as primary or no replicas connected.
pub struct NoopBroadcaster;

impl ReplicationBroadcaster for NoopBroadcaster {
    fn broadcast_command_on_shard(&self, _shard_id: u16, _cmd_name: &str, _args: &[Bytes]) -> u64 {
        0
    }

    fn is_active(&self) -> bool {
        false
    }

    fn current_offset(&self) -> u64 {
        0
    }
}

/// A type alias for the broadcaster wrapped in an Arc.
pub type SharedBroadcaster = Arc<dyn ReplicationBroadcaster>;
