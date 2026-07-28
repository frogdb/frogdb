//! The node's replication identity, owned once per process.
//!
//! A node's replication identity is `(replication_id, failover window, live
//! offset)`. It belongs to the *node*, not to whichever role handler happens to
//! be running: a replica that gets promoted keeps the history it just finished
//! applying, and a primary that gets demoted keeps the offset it reached. When
//! each handler minted its own [`ReplicationState`] the two roles disagreed the
//! moment a role changed — a promoted node advertised the id it had already
//! replaced, and `REPLICAOF` on a live primary reset its id to a freshly
//! generated one that no replica had ever followed.
//!
//! [`ReplicationIdentity`] is the single cell both handlers share. It is cheap
//! to clone (two `Arc`s) and deliberately synchronous: the role transition that
//! mints a new id runs under `RoleManager`'s blocking mutex and cannot `.await`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::replica::offset::AppliedOffset;
use crate::state::ReplicationState;
use crate::tracker::ReplicationTrackerImpl;

/// The process-wide handle to [`ReplicationState`].
///
/// A `parking_lot` lock, not `tokio`: every critical section here is a field
/// read or a couple of field writes, and the promotion path that must take it
/// is synchronous.
pub type SharedReplicationState = Arc<RwLock<ReplicationState>>;

/// One node, one replication identity — shared by whichever role handlers exist.
///
/// Both [`crate::primary::PrimaryReplicationHandler`] and
/// [`crate::replica::ReplicaReplicationHandler`] are handed a clone of the same
/// cell, so `INFO replication`, a PSYNC window check, and a replica's next
/// reconnect all read one value.
#[derive(Clone)]
pub struct ReplicationIdentity {
    state: SharedReplicationState,
    /// Live stream position — how far the stream has been *received*. The
    /// primary advances it through
    /// [`crate::offset_coordinator::OffsetCoordinator::advance`]; the replica
    /// advances it through [`crate::replica::ReplicaOffset::frame_advance`], at
    /// frame-decode time. This is the ACK / lag / cluster-bus number.
    live: Arc<AtomicU64>,
    /// Offset of the data this node actually **holds** — the received offset
    /// minus whatever is still queued between the decode loop and the applier.
    ///
    /// On a primary the two are equal by construction (a write is applied on its
    /// shard before it is broadcast, and
    /// [`crate::offset_coordinator::OffsetCoordinator::advance`] moves both). On
    /// a replica they diverge by the depth of the frame channel: `streaming.rs`
    /// advances `live` as soon as a frame is decoded, while
    /// [`crate::consume_frames`] advances this counter only after the frame has
    /// been applied.
    ///
    /// Everything that describes *history this node can vouch for* must read
    /// this one, never `live`: the promotion boundary (minted replid window +
    /// backlog floor) and the persisted `offset_at_save`. Freezing a boundary
    /// too low only costs a full resync; freezing it above data the node never
    /// applied hands a sibling `+CONTINUE` over a hole.
    ///
    /// Wrapped rather than raw because the counter carries the applier's
    /// admission gate with it: a promotion freezes the two together, so the
    /// boundary it reads is exactly what the applier has claimed — and the
    /// applier claims a group before, never after, handing it to a shard.
    ///
    /// "Claimed" is not quite "applied to the keyspace" in two admitted-failure
    /// cases: a group whose `apply_group` returns `Err` keeps its claim (the
    /// node has already diverged for that write, and stalling the offset would
    /// desynchronise every later frame), and a crash between a claim and its
    /// shard write leaves the persisted offset a group ahead of the data. Both
    /// are tracked in
    /// `.scratch/replication-cluster-rework/issues/08-divergence-retires-nothing.md`.
    applied: AppliedOffset,
}

impl ReplicationIdentity {
    /// Build the node's identity from its recovered state, adopting `tracker`'s
    /// offset atomic as the single live head.
    ///
    /// The tracker owns that atomic for its INFO/ROLE and lag accessors; taking
    /// it here (rather than allocating a second one) is what makes the primary
    /// handler, the replica handler, the cluster bus's HealthProbe handle, and
    /// INFO all report the same offset.
    ///
    /// The live head is seeded from the recovered `offset_at_save`, so a node
    /// that restarts mid-stream resumes from where it saved rather than from 0.
    /// The seed only ever raises the head — the offset is monotonic, and a
    /// tracker that was already positioned must not be rewound.
    pub fn adopting(state: ReplicationState, tracker: &Arc<ReplicationTrackerImpl>) -> Self {
        let live = tracker.offset_handle();
        live.fetch_max(state.offset_at_save, Ordering::AcqRel);
        // Recovered state describes data on disk, so at boot the node holds
        // everything it has received: applied starts level with live.
        let applied = AppliedOffset::over(Arc::new(AtomicU64::new(live.load(Ordering::Acquire))));
        Self {
            state: Arc::new(RwLock::new(state)),
            live,
            applied,
        }
    }

    /// Build a standalone identity with its own offset atomic — tests and any
    /// wiring with no tracker.
    pub fn detached(state: ReplicationState) -> Self {
        let live = Arc::new(AtomicU64::new(state.offset_at_save));
        let applied = AppliedOffset::detached(state.offset_at_save);
        Self {
            state: Arc::new(RwLock::new(state)),
            live,
            applied,
        }
    }

    /// The shared state cell, for the seams that already speak in it
    /// (`ClusterDeps::replication_state`, `OffsetCoordinator`, INFO).
    pub fn state(&self) -> SharedReplicationState {
        self.state.clone()
    }

    /// The shared live-offset atomic.
    pub fn live(&self) -> Arc<AtomicU64> {
        self.live.clone()
    }

    /// The shared applied offset — the data this node holds, plus the gate a
    /// promotion freezes it with (see the `applied` field's contract).
    pub fn applied(&self) -> AppliedOffset {
        self.applied.clone()
    }

    /// The live stream position (received).
    pub fn current_offset(&self) -> u64 {
        self.live.load(Ordering::Acquire)
    }

    /// The offset of the data this node holds — never above
    /// [`Self::current_offset`].
    pub fn applied_offset(&self) -> u64 {
        self.applied.current()
    }

    /// This node's current replication id.
    pub fn replication_id(&self) -> String {
        self.state.read().replication_id.clone()
    }

    /// A snapshot of the state, for callers that want to persist or render it.
    pub fn snapshot(&self) -> ReplicationState {
        self.state.read().clone()
    }
}
