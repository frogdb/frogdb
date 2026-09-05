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
pub mod feed_account;
pub mod feed_gate;
pub mod feed_sequencer;
pub mod frame;
pub mod fullsync;
pub mod identity;
pub mod invariants;
#[cfg(test)]
mod model;
pub mod net_bytes;
pub mod offset_coordinator;
pub mod primary;
#[cfg(test)]
mod properties;
pub mod replica;
pub mod replica_session;
pub mod session_machine;
pub mod split_brain_log;
pub mod state;
pub mod sync_counters;
pub mod tracker;
pub mod version_compat;
pub mod view;
pub mod wait_coordinator;
#[cfg(test)]
mod wire_golden;

pub use apply::{
    ApplyError, ConsumeStats, ControlApplier, ReplicaApplier, ReplicaTxnBound, StreamedFrame,
    consume_frames, parse_frame_payload,
};
pub use feed_account::{FeedOutputAccount, FeedVerdict, SharedFeedAccount};
pub use feed_gate::ReplicaFeedGate;
pub use feed_sequencer::{FeedAction, FeedInput, FeedSequencer};
pub use frame::{
    CONTROL_SHARD, FRAME_MAGIC, FRAME_VERSION, ReplicationFrame, ReplicationFrameCodec,
    serialize_command_to_resp,
};
pub use fullsync::{FullSyncMetadata, ShardCoverage};
pub use identity::{ReplicationIdentity, SharedReplicationState};
pub use net_bytes::{NetByteCounters, NetByteCountersSnapshot};
pub use offset_coordinator::OffsetCoordinator;
pub use primary::{
    BacklogConfig, BacklogGeometry, BacklogTtl, DivergenceRecord, FunctionSnapshotHook,
    LagThresholdConfig, LagThresholds, PreCheckpointHook, PrimaryReplicationHandler, StintPlan,
    plan_primary_stint,
};
pub use replica::{ReplicaConnection, ReplicaReplicationHandler};
pub use replica_session::{
    AnnouncedOption, AnnouncementError, Phase, ReplicaAnnouncement, ReplicaCapabilities,
    ReplicaDeparture, ReplicaInfo, ReplicaSession, SyncKind, ack_age_secs,
};
pub use state::{
    ReplicationState, StagedReplicationMetadata, consume_staged_replication_metadata,
    discard_staged_full_sync, read_staged_replication_metadata,
};
pub use sync_counters::{SyncCounters, SyncCountersSnapshot, SyncOutcome};
pub use tracker::{ReplicationTrackerImpl, ack_is_fresh};
pub use wait_coordinator::{AckSolicitor, RoleFence, WaitCoordinator, WaitVerdict};

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

    /// Whether writes must be stamped and broadcast right now.
    ///
    /// True while replicas are connected, and — when none are — while the
    /// primary's backlog still holds a resume point a reconnecting replica could
    /// be continued from: skipping those writes would punch a hole a later
    /// `+CONTINUE` cannot fill (see
    /// [`primary::PartialSyncReplay::has_resume_history`]).
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Records every frame emit as `(shard, command)` and hands back a
    /// monotonically advancing offset, as the primary's coordinator does.
    #[derive(Default)]
    struct RecordingBroadcaster {
        emitted: Mutex<Vec<(u16, String)>>,
        offset: AtomicU64,
    }

    impl ReplicationBroadcaster for RecordingBroadcaster {
        fn broadcast_command_on_shard(
            &self,
            shard_id: u16,
            cmd_name: &str,
            _args: &[Bytes],
        ) -> u64 {
            self.emitted
                .lock()
                .unwrap()
                .push((shard_id, cmd_name.to_string()));
            self.offset.fetch_add(10, Ordering::Relaxed) + 10
        }

        fn is_active(&self) -> bool {
            true
        }

        fn current_offset(&self) -> u64 {
            self.offset.load(Ordering::Relaxed)
        }
    }

    /// The single definition of the MULTI/EXEC framing: every frame of the
    /// group carries the one shard the transaction ran on, and the offset
    /// handed back is the one *after* the EXEC — the position a `WAIT` on this
    /// transaction has to wait for.
    #[test]
    fn a_broadcast_transaction_frames_the_whole_group_on_one_shard() {
        let broadcaster = RecordingBroadcaster::default();
        let empty: &[Bytes] = &[];
        let offset =
            broadcaster.broadcast_transaction_on_shard(4, &[("SET", empty), ("DEL", empty)]);

        assert_eq!(
            *broadcaster.emitted.lock().unwrap(),
            vec![
                (4, "MULTI".to_string()),
                (4, "SET".to_string()),
                (4, "DEL".to_string()),
                (4, "EXEC".to_string()),
            ],
            "the group must be framed MULTI … EXEC, all on the origin shard"
        );
        assert_eq!(
            offset, 40,
            "the offset reported is the stream position after the EXEC frame"
        );
        assert_eq!(broadcaster.current_offset(), 40);
    }

    /// The no-op broadcaster is the "nothing is following this node" answer:
    /// inactive, and at no offset. A truthy `is_active` would make every write
    /// path stamp and broadcast frames nobody reads.
    #[test]
    fn the_noop_broadcaster_is_inactive_and_at_no_offset() {
        let noop = NoopBroadcaster;
        let empty: &[Bytes] = &[];

        assert!(!noop.is_active());
        assert_eq!(noop.current_offset(), 0);
        assert_eq!(noop.broadcast_command_on_shard(3, "SET", empty), 0);
        assert_eq!(noop.broadcast_transaction_on_shard(3, &[("SET", empty)]), 0);
    }
}
