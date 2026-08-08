//! Cluster state and Raft state machine implementation.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::storage::RaftStateMachine;
use openraft::{EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Serialize};

use tokio::sync::mpsc;

use crate::storage::{ClusterSnapshotStore, StoredClusterSnapshot};
use crate::types::{
    CLUSTER_SLOTS, ClusterCommand, ClusterError, ClusterEvent, ClusterResponse, ClusterSnapshot,
    ConfigEpoch, NodeId, NodeInfo, NodeRole, SlotMigration, SlotRange, TypeConfig,
};

/// The cluster state, protected by a read-write lock for concurrent access.
#[derive(Debug, Clone, Default)]
pub struct ClusterState {
    cell: Arc<RwLock<StateCell>>,
    /// This node's current ID. Shared across all connections so that HARD reset
    /// (which generates a new node ID) is visible immediately. Not Raft-replicated.
    self_node_id: Arc<AtomicU64>,
}

/// The authoritative replicated state together with the immutable
/// [`ClusterSnapshot`] published to readers.
///
/// Both live under one lock on purpose: the published value is rebuilt inside
/// the same critical section as the mutation that invalidated it, so a reader
/// can never observe a snapshot older than a mutation that has already
/// returned. Rebuilding outside the lock would widen the commit-to-apply window
/// FM-CLUSTER-037 bounds into one the EXEC re-validation work cannot bound.
#[derive(Debug)]
struct StateCell {
    inner: ClusterStateInner,
    /// Rebuilt from `inner` on every snapshot-visible mutation. Readers clone
    /// the `Arc`, so [`ClusterState::snapshot`] copies a pointer rather than the
    /// 16384-entry slot table.
    published: Arc<ClusterSnapshot>,
}

impl StateCell {
    fn new(inner: ClusterStateInner) -> Self {
        let published = Arc::new(inner.to_snapshot());
        Self { inner, published }
    }

    /// Re-derive the published snapshot from the authoritative state.
    fn republish(&mut self) {
        self.published = Arc::new(self.inner.to_snapshot());
    }
}

impl Default for StateCell {
    fn default() -> Self {
        Self::new(ClusterStateInner::default())
    }
}

/// A write guard over [`ClusterStateInner`] that republishes the reader snapshot
/// when it is dropped.
///
/// Every mutation of a snapshot-visible field goes through this guard, which is
/// what makes "every mutation republishes" structural rather than a convention:
/// an early `return`, a `?`, or an unwinding panic all run `Drop` before the
/// lock is released, so no caller can leave a stale snapshot behind. The rebuild
/// is unconditional — a command that validated and rejected republishes an
/// identical value rather than relying on nothing having been touched yet.
///
/// The only mutations that deliberately skip it are the two openraft bookkeeping
/// setters ([`ClusterState::set_last_applied_log`],
/// [`ClusterState::set_last_membership`]), whose fields no snapshot carries. The
/// lock lives behind a private field, so those two and this guard are the whole
/// mutation surface.
pub(crate) struct PublishOnDrop<'a> {
    cell: RwLockWriteGuard<'a, StateCell>,
}

impl std::ops::Deref for PublishOnDrop<'_> {
    type Target = ClusterStateInner;

    fn deref(&self) -> &ClusterStateInner {
        &self.cell.inner
    }
}

impl std::ops::DerefMut for PublishOnDrop<'_> {
    fn deref_mut(&mut self) -> &mut ClusterStateInner {
        &mut self.cell.inner
    }
}

impl Drop for PublishOnDrop<'_> {
    fn drop(&mut self) {
        self.cell.republish();
    }
}

/// Inner state of the cluster.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterStateInner {
    /// All nodes in the cluster.
    pub nodes: BTreeMap<NodeId, NodeInfo>,
    /// Slot to node assignment.
    pub slot_assignment: BTreeMap<u16, NodeId>,
    /// Current configuration epoch.
    pub config_epoch: ConfigEpoch,
    /// Active slot migrations.
    pub migrations: BTreeMap<u16, SlotMigration>,
    /// Monotonic counter minting the `seq` of each prepared slot handoff.
    ///
    /// Replicated (not node-local) so every node agrees on which finalization
    /// attempt a drain confirmation refers to: an ack from an aborted attempt
    /// carries a stale `seq` and is refused rather than vouching for the
    /// attempt that replaced it. Never reset by anything but `ResetCluster`,
    /// and never rewound by a restore: both snapshot vehicles carry it, because
    /// the migrations that survive into a snapshot cannot re-derive the seqs of
    /// the ones that finished (FM-CLUSTER-100).
    #[serde(default)]
    pub handoff_seq: u64,
    /// Last applied log index.
    pub last_applied_log: Option<LogId<NodeId>>,
    /// Last membership configuration.
    pub last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    /// The finalized active version. `None` means pre-versioning (original install,
    /// no finalization has ever occurred). Gates check this to decide behavior.
    #[serde(default)]
    pub active_version: Option<String>,
}

impl ClusterState {
    /// Create a new empty cluster state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create cluster state from a snapshot, preserving the existing `self_node_id`
    /// (which is local state, not Raft-replicated).
    pub fn from_snapshot(snapshot: ClusterSnapshot, self_node_id: Arc<AtomicU64>) -> Self {
        let inner = ClusterStateInner {
            nodes: snapshot.nodes,
            slot_assignment: snapshot.slot_assignment,
            config_epoch: snapshot.config_epoch,
            migrations: snapshot.migrations,
            // The generation counter is carried by the DTO rather than
            // re-derived: `max(seq)` over the surviving migrations is not the
            // same number, because a completed or aborted handoff removes its
            // record while its `seq` stays spent forever (FM-CLUSTER-100).
            handoff_seq: snapshot.handoff_seq,
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            active_version: snapshot.active_version,
        };
        Self {
            cell: Arc::new(RwLock::new(StateCell::new(inner))),
            self_node_id,
        }
    }

    /// Read the authoritative replicated state.
    ///
    /// Prefer [`Self::snapshot`] for anything that wants a consistent view of
    /// the whole topology — this borrows the lock for as long as the guard
    /// lives, and blocks writers.
    pub(crate) fn read_inner(&self) -> MappedRwLockReadGuard<'_, ClusterStateInner> {
        RwLockReadGuard::map(self.cell.read(), |cell| &cell.inner)
    }

    /// Take the write lock, republishing the reader snapshot when the returned
    /// guard drops. See [`PublishOnDrop`].
    pub(crate) fn write_inner(&self) -> PublishOnDrop<'_> {
        PublishOnDrop {
            cell: self.cell.write(),
        }
    }

    /// Record how far openraft has applied.
    ///
    /// `last_applied_log` is not part of [`ClusterSnapshot`], so this
    /// deliberately does not republish: every entry openraft applies — including
    /// blank ones — advances it, and rebuilding the slot table per entry would
    /// move the cost this seam exists to remove onto the apply path.
    fn set_last_applied_log(&self, log_id: LogId<NodeId>) {
        self.cell.write().inner.last_applied_log = Some(log_id);
    }

    /// Record the membership configuration openraft committed.
    ///
    /// Like [`Self::set_last_applied_log`], `last_membership` is openraft
    /// bookkeeping that no snapshot carries, so no republish is needed.
    fn set_last_membership(&self, membership: StoredMembership<NodeId, openraft::BasicNode>) {
        self.cell.write().inner.last_membership = membership;
    }

    /// Get this node's current ID. Returns `None` if not yet set (value is 0).
    pub fn self_node_id(&self) -> Option<u64> {
        let id = self.self_node_id.load(Ordering::Relaxed);
        if id == 0 { None } else { Some(id) }
    }

    /// Set this node's current ID.
    pub fn set_self_node_id(&self, id: u64) {
        self.self_node_id.store(id, Ordering::Relaxed);
    }

    /// Get the shared `self_node_id` atomic for passing to `from_snapshot`.
    pub fn self_node_id_atomic(&self) -> Arc<AtomicU64> {
        self.self_node_id.clone()
    }

    /// Serialize the replicated state together with the openraft metadata
    /// describing how far it has advanced.
    ///
    /// Both halves are produced under a single read lock so the metadata always
    /// describes exactly the bytes it accompanies.
    pub fn encode_snapshot(
        &self,
    ) -> Result<(SnapshotMeta<NodeId, openraft::BasicNode>, Vec<u8>), serde_json::Error> {
        let inner = self.read_inner();
        let data = serde_json::to_vec(&*inner)?;
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied_log,
            last_membership: inner.last_membership.clone(),
            snapshot_id: match inner.last_applied_log {
                Some(log_id) => format!("snapshot-{}", log_id.index),
                None => "snapshot-0".to_string(),
            },
        };
        Ok((meta, data))
    }

    /// Replace the replicated state wholesale with the contents of a snapshot.
    ///
    /// `last_applied_log` and `last_membership` are taken from the snapshot
    /// metadata rather than the serialized body: openraft treats the metadata as
    /// authoritative for how far the state machine has advanced, and the two can
    /// legitimately differ when a leader ships a snapshot it trimmed itself.
    pub fn restore_from_snapshot(
        &self,
        mut restored: ClusterStateInner,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
    ) {
        restored.last_applied_log = meta.last_log_id;
        restored.last_membership = meta.last_membership.clone();
        *self.write_inner() = restored;
    }

    /// Get a snapshot of the current state.
    ///
    /// Pointer-cheap: the value is rebuilt on mutation, not on read (see
    /// [`StateCell`]), so the routing seam can take one per keyed command
    /// without copying the slot table. Two calls with no intervening mutation
    /// return the same allocation.
    pub fn snapshot(&self) -> Arc<ClusterSnapshot> {
        Arc::clone(&self.cell.read().published)
    }

    /// Get node info by ID.
    pub fn get_node(&self, node_id: NodeId) -> Option<NodeInfo> {
        self.read_inner().nodes.get(&node_id).cloned()
    }

    /// Get all nodes.
    pub fn get_all_nodes(&self) -> Vec<NodeInfo> {
        self.read_inner().nodes.values().cloned().collect()
    }

    /// Get the node owning a slot.
    pub fn get_slot_owner(&self, slot: u16) -> Option<NodeId> {
        self.read_inner().slot_assignment.get(&slot).copied()
    }

    /// Get all slots assigned to a node as ranges.
    pub fn get_node_slots(&self, node_id: NodeId) -> Vec<SlotRange> {
        self.snapshot().get_node_slots(node_id)
    }

    /// Get the current configuration epoch.
    pub fn config_epoch(&self) -> ConfigEpoch {
        self.read_inner().config_epoch
    }

    /// Check if a slot is migrating.
    pub fn is_slot_migrating(&self, slot: u16) -> bool {
        self.read_inner().migrations.contains_key(&slot)
    }

    /// Get migration info for a slot.
    pub fn get_slot_migration(&self, slot: u16) -> Option<SlotMigration> {
        self.read_inner().migrations.get(&slot).cloned()
    }

    /// Apply a command to the local state during bootstrap, bypassing Raft
    /// consensus but NOT the validation performed by [`Self::apply_command`].
    ///
    /// This is the single validated mutation path: bootstrap seeding constructs
    /// the same [`ClusterCommand`]s that Raft replicates to followers, so the
    /// bootstrap node enforces the exact same invariants (node-exists,
    /// slot-already-assigned, version-mismatch warnings, `CLUSTER_SLOTS` bounds)
    /// that Raft-applied commands do. Followers receive these mutations via Raft
    /// log replication; only the local bootstrap node uses this seam directly.
    pub fn apply_local(&self, cmd: ClusterCommand) -> Result<ClusterResponse, ClusterError> {
        // Bootstrap runs before any event consumer is wired, so the derived
        // events are dropped here (this is correct, not a lost event).
        self.apply_command(cmd).map(|(response, _events)| response)
    }

    /// Drive a slot handoff to the prepared-and-drained state, returning a
    /// `proposed_at_ms` that sits inside the resulting barrier window.
    ///
    /// Test-only shorthand for the two entries a real finalizer proposes before
    /// `CompleteSlotMigration`. It exists so the pre-existing migration tests
    /// keep asserting what they were written to assert (ownership transfer,
    /// parameter validation, event fanout) instead of each one re-deriving the
    /// two-phase preamble.
    #[cfg(test)]
    pub(crate) fn arm_handoff_for_test(
        &self,
        slot: u16,
        source_node: NodeId,
        target_node: NodeId,
    ) -> u64 {
        let prepared_at_ms = 1_000_000;
        let (_, events) = self
            .apply_command(ClusterCommand::PrepareSlotHandoff {
                slot,
                source_node,
                target_node,
                barrier_ms: crate::types::HANDOFF_BARRIER_MS,
                lease_ms: crate::types::HANDOFF_LEASE_MS,
                proposed_at_ms: prepared_at_ms,
            })
            .expect("prepare must succeed");
        let seq = match events.as_slice() {
            [ClusterEvent::SlotHandoffPrepared { seq, .. }] => *seq,
            other => panic!("expected exactly one prepared event, got {other:?}"),
        };
        self.apply_command(ClusterCommand::ConfirmSlotHandoffDrained { slot, seq })
            .expect("confirm must succeed");
        prepared_at_ms
    }

    /// Check if all slots are assigned.
    pub fn all_slots_assigned(&self) -> bool {
        let inner = self.read_inner();
        inner.slot_assignment.len() == CLUSTER_SLOTS as usize
    }

    /// Get the finalized active version, if any.
    pub fn active_version(&self) -> Option<String> {
        self.read_inner().active_version.clone()
    }

    /// Override a node's reported binary version. Test-only.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_node_version(&self, node_id: NodeId, version: String) {
        if let Some(info) = self.write_inner().nodes.get_mut(&node_id) {
            info.version = version;
        }
    }

    /// Override all nodes' reported binary versions. Test-only.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_all_node_versions(&self, version: &str) {
        let mut inner = self.write_inner();
        for info in inner.nodes.values_mut() {
            info.version = version.to_string();
        }
    }
}

/// What [`ClusterStateInner::reconcile_incoming_epoch`] did with the
/// `config_epoch` an incoming [`NodeInfo`] claimed.
///
/// Returned (rather than only logged) so the resolution policy is unit-testable
/// without reading tracing output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EpochReconciliation {
    /// The claimed epoch was recorded as-is: either the unassigned `0`, or a
    /// nonzero epoch no other primary holds.
    Accepted,
    /// The record claimed the unassigned `0` for a node already holding a real
    /// epoch (a re-registration, which rebuilds `NodeInfo` from scratch); the
    /// recorded epoch was kept instead of being reset.
    Preserved(ConfigEpoch),
    /// The claimed epoch collided with another primary's; a fresh epoch was
    /// minted from the cluster-wide counter and assigned to the incoming node.
    Reassigned {
        /// The epoch the incoming node claimed.
        claimed: ConfigEpoch,
        /// The epoch it was given instead.
        assigned: ConfigEpoch,
    },
}

impl ClusterStateInner {
    /// Build the reader-facing view of this state.
    ///
    /// The one place that decides which fields are snapshot-visible: anything
    /// copied here has to be republished on mutation, anything left out (the
    /// openraft bookkeeping) does not.
    fn to_snapshot(&self) -> ClusterSnapshot {
        ClusterSnapshot {
            nodes: self.nodes.clone(),
            slot_assignment: self.slot_assignment.clone(),
            config_epoch: self.config_epoch,
            migrations: self.migrations.clone(),
            handoff_seq: self.handoff_seq,
            active_version: self.active_version.clone(),
        }
    }

    /// The highest `config_epoch` any known node currently claims.
    fn max_node_epoch(&self) -> ConfigEpoch {
        self.nodes
            .values()
            .map(|n| n.config_epoch)
            .max()
            .unwrap_or(0)
    }

    /// Mint the next cluster-wide config epoch, advancing the counter past every
    /// epoch currently claimed by a node.
    ///
    /// The `max` guard preserves the cluster invariant that the cluster-wide
    /// counter is never below any per-node `config_epoch` (see
    /// `website/src/content/docs/architecture/clustering.md`), which a node
    /// joining with a claimed epoch higher than the counter could otherwise
    /// break.
    fn mint_config_epoch(&mut self) -> ConfigEpoch {
        self.config_epoch = self.config_epoch.max(self.max_node_epoch()) + 1;
        self.config_epoch
    }

    /// Resolve the `config_epoch` an incoming node claims against the epochs
    /// already recorded, rewriting `node.config_epoch` in place.
    ///
    /// Policy (Redis's gossip rule, decided in one linearized transition):
    ///
    /// - `config_epoch == 0` is "unassigned" — the bootstrap and self-registration
    ///   paths build `NodeInfo::new_primary`, which starts at `0`. It never
    ///   collides, but it must not *lower* an epoch the node already holds, so a
    ///   re-registration keeps the recorded epoch.
    /// - A nonzero claim that another **primary** already holds is a collision:
    ///   the incoming node is given a freshly minted epoch instead. Only
    ///   primaries are compared, matching Redis's
    ///   `clusterHandleConfigEpochCollision`, which skips replicas because only a
    ///   primary's epoch arbitrates slot ownership.
    /// - An uncontested nonzero claim is accepted, and the cluster-wide counter
    ///   is raised to at least that value so the counter keeps dominating every
    ///   per-node epoch.
    pub(crate) fn reconcile_incoming_epoch(&mut self, node: &mut NodeInfo) -> EpochReconciliation {
        let claimed = node.config_epoch;

        if claimed == 0 {
            if let Some(existing) = self.nodes.get(&node.id)
                && existing.config_epoch != 0
            {
                node.config_epoch = existing.config_epoch;
                return EpochReconciliation::Preserved(existing.config_epoch);
            }
            return EpochReconciliation::Accepted;
        }

        let collides = node.is_primary()
            && self
                .nodes
                .values()
                .any(|n| n.id != node.id && n.is_primary() && n.config_epoch == claimed);

        if collides {
            let assigned = self.mint_config_epoch();
            node.config_epoch = assigned;
            return EpochReconciliation::Reassigned { claimed, assigned };
        }

        self.config_epoch = self.config_epoch.max(claimed);
        EpochReconciliation::Accepted
    }
}

/// Event emitted when this node is demoted from primary to replica.
#[derive(Debug, Clone)]
pub struct DemotionEvent {
    /// The node ID that was demoted.
    pub demoted_node_id: NodeId,
    /// The node ID of the new primary (if known).
    pub new_primary_id: Option<NodeId>,
    /// The configuration epoch at the time of demotion.
    pub epoch: u64,
}

/// Event emitted when this node is promoted from replica to primary.
#[derive(Debug, Clone)]
pub struct PromotionEvent {
    /// The node ID that was promoted.
    pub promoted_node_id: NodeId,
    /// The configuration epoch at the time of promotion.
    pub epoch: u64,
}

/// A change to *this* node's cluster-state role, in Raft apply order.
///
/// Demotions and promotions share one channel on purpose. A failover round can
/// flip a node primary → replica → primary within a few log entries, and the
/// data-path transition each event drives is not commutative: applying them out
/// of order leaves the node fenced as a replica while cluster state calls it a
/// primary (or the reverse). One channel and one consumer make the data-path
/// role a faithful replay of the metadata plane's ordering.
#[derive(Debug, Clone)]
pub enum RoleChangeEvent {
    /// This node lost its primary role.
    Demoted(DemotionEvent),
    /// This node gained the primary role.
    Promoted(PromotionEvent),
}

/// Event emitted when a slot migration completes (fires on ALL nodes).
#[derive(Debug, Clone)]
pub struct SlotMigrationCompleteEvent {
    pub slot: u16,
    pub source_node: NodeId,
    pub target_node: NodeId,
}

/// A transition of a two-phase slot handoff, in Raft apply order (fires on ALL
/// nodes — the node-local "am I the source?" filter lives in the runtime
/// consumer, which reads `self_node_id` from [`ClusterState`]).
///
/// Both variants share one channel for the same reason role changes do: a
/// prepare and its release are not commutative. Delivering a `Released` before
/// the `Prepared` that armed the barrier would leave the slot fenced until the
/// barrier timed out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotHandoffEvent {
    /// A handoff was prepared; the source must arm its barrier and drain.
    Prepared {
        /// The slot being handed off.
        slot: u16,
        /// The node that must arm the barrier.
        source_node: NodeId,
        /// The node that will own the slot after completion.
        target_node: NodeId,
        /// Attempt identifier to echo back when confirming the drain.
        seq: u64,
        /// How long the barrier should be armed for.
        barrier_ms: u64,
    },
    /// A prepared handoff was released (completed, aborted, cancelled, pruned,
    /// or reset); the source may drop its barrier.
    Released {
        /// The slot whose handoff was released.
        slot: u16,
        /// The node that armed the barrier.
        source_node: NodeId,
        /// The attempt identifier being released.
        seq: u64,
    },
}

/// Raft state machine for cluster coordination.
pub struct ClusterStateMachine {
    state: ClusterState,
    /// This node's ID, used to detect role changes that concern this node.
    self_node_id: Option<NodeId>,
    /// Channel carrying this node's own role changes in Raft apply order.
    role_change_tx: Option<mpsc::UnboundedSender<RoleChangeEvent>>,
    /// Channel to notify when a slot migration completes.
    migration_complete_tx: Option<mpsc::UnboundedSender<SlotMigrationCompleteEvent>>,
    /// Channel carrying two-phase slot-handoff transitions in apply order.
    slot_handoff_tx: Option<mpsc::UnboundedSender<SlotHandoffEvent>>,
    /// Durable home for snapshots. When absent the state machine is purely
    /// in-memory and a restart re-derives everything from the log or the leader.
    snapshot_store: Option<ClusterSnapshotStore>,
}

impl ClusterStateMachine {
    /// Create a new state machine.
    pub fn new() -> Self {
        Self {
            state: ClusterState::new(),
            self_node_id: None,
            role_change_tx: None,
            migration_complete_tx: None,
            slot_handoff_tx: None,
            snapshot_store: None,
        }
    }

    /// Create a state machine with existing state.
    pub fn with_state(state: ClusterState) -> Self {
        Self {
            state,
            self_node_id: None,
            role_change_tx: None,
            migration_complete_tx: None,
            slot_handoff_tx: None,
            snapshot_store: None,
        }
    }

    /// Give the state machine a durable home for its snapshots, restoring any
    /// snapshot already persisted there.
    ///
    /// Call this before handing the state machine to openraft: the restored
    /// `last_applied_log` is what [`RaftStateMachine::applied_state`] reports, so
    /// openraft replays only the entries after the snapshot instead of expecting
    /// a log prefix that purge already deleted.
    #[allow(clippy::result_large_err)]
    pub fn attach_snapshot_store(
        &mut self,
        store: ClusterSnapshotStore,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(stored) = store.load()? {
            let restored: ClusterStateInner =
                serde_json::from_slice(&stored.data).map_err(|e| {
                    StorageError::from_io_error(
                        openraft::ErrorSubject::Snapshot(Some(stored.meta.signature())),
                        openraft::ErrorVerb::Read,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    )
                })?;

            let config_epoch = restored.config_epoch;
            let node_count = restored.nodes.len();
            self.state.restore_from_snapshot(restored, &stored.meta);

            tracing::info!(
                last_log_id = ?stored.meta.last_log_id,
                config_epoch,
                node_count,
                "Restored cluster state machine from persisted snapshot"
            );
        }

        self.snapshot_store = Some(store);
        Ok(())
    }

    /// Write a snapshot to the durable store, if one is attached.
    #[allow(clippy::result_large_err)]
    fn persist_snapshot(
        &self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        data: &[u8],
    ) -> Result<(), StorageError<NodeId>> {
        let Some(store) = &self.snapshot_store else {
            return Ok(());
        };
        store.save(&StoredClusterSnapshot {
            meta: meta.clone(),
            data: data.to_vec(),
        })
    }

    /// Configure detection of *this* node's own cluster-state role changes.
    ///
    /// Every applied command that demotes or promotes `self_node_id` sends the
    /// matching [`RoleChangeEvent`] through the returned receiver, in apply
    /// order. This is the metadata plane's only outbound edge to the data path:
    /// Raft owns the cluster-state role, and the consumer of this receiver is
    /// what turns that into an actual promotion or demotion of the node's
    /// replication role.
    pub fn enable_role_change_detection(
        &mut self,
        self_node_id: NodeId,
    ) -> mpsc::UnboundedReceiver<RoleChangeEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.self_node_id = Some(self_node_id);
        self.role_change_tx = Some(tx);
        rx
    }

    /// Configure slot migration completion notifications.
    ///
    /// When a `CompleteSlotMigration` command is successfully applied,
    /// a `SlotMigrationCompleteEvent` is sent through the returned receiver.
    pub fn enable_migration_complete_notification(
        &mut self,
    ) -> mpsc::UnboundedReceiver<SlotMigrationCompleteEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.migration_complete_tx = Some(tx);
        rx
    }

    /// Configure two-phase slot-handoff notifications.
    ///
    /// Every applied `PrepareSlotHandoff` and every arm that releases a prepared
    /// handoff sends a [`SlotHandoffEvent`] through the returned receiver, in
    /// apply order. The consumer
    /// ([`frogdb_cluster_runtime::run_slot_handoff_barrier`]) is what turns a
    /// prepare into an armed slot barrier plus a shard drain.
    pub fn enable_slot_handoff_notification(
        &mut self,
    ) -> mpsc::UnboundedReceiver<SlotHandoffEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.slot_handoff_tx = Some(tx);
        rx
    }

    /// Get a reference to the cluster state.
    pub fn state(&self) -> &ClusterState {
        &self.state
    }

    /// This node's cluster-state role, or `None` when role-change detection is
    /// off or this node is not (yet) in the cluster.
    fn self_role(&self) -> Option<NodeRole> {
        let self_id = self.self_node_id?;
        self.state.get_node(self_id).map(|n| n.role)
    }

    /// Reconcile the data path with this node's *restored* cluster-state role.
    ///
    /// Boot has two independent sources for this node's role: the cluster state
    /// restored by [`Self::attach_snapshot_store`], and the data path's live
    /// replica flag, which starts from local config. Log-tail replay emits a
    /// role change for every entry it applies, but a role folded into the
    /// snapshot produces no entry to replay — so a node that was a replica when
    /// the snapshot was taken would come back up reading `role:master` on the
    /// data path while the cluster still lists it as a replica.
    ///
    /// Call this once at boot, after role-change detection is enabled, and then
    /// periodically at runtime (see [`SelfRoleReconciler`]). It emits the one
    /// event that closes the gap and returns the role it reconciled to, or
    /// `None` when the two views already agree (the common case, where emitting
    /// would churn the replication identity for nothing).
    pub fn reconcile_self_role(&self, data_path_is_replica: bool) -> Option<NodeRole> {
        match self.self_role_reconciler()?.reconcile(data_path_is_replica) {
            RoleReconcile::ReDriven(role) => Some(role),
            RoleReconcile::Agreed | RoleReconcile::Detached => None,
        }
    }

    /// A standalone handle onto the same reconciliation, for callers that
    /// cannot hold the state machine itself.
    ///
    /// `None` until role-change detection is enabled — without the channel and
    /// this node's id there is nothing to reconcile against.
    pub fn self_role_reconciler(&self) -> Option<SelfRoleReconciler> {
        Some(SelfRoleReconciler {
            state: self.state.clone(),
            self_node_id: self.self_node_id?,
            // Weak on purpose — see [`SelfRoleReconciler`]: a long-lived
            // reconciler must not be what keeps the role-change channel (and
            // therefore its consumer, and everything the consumer holds) alive
            // past shutdown.
            role_change_tx: self.role_change_tx.as_ref()?.downgrade(),
        })
    }

    /// Send the [`RoleChangeEvent`] matching a freshly-observed role for this
    /// node. Used by paths that change the role without producing a
    /// [`ClusterEvent`], i.e. snapshot installs.
    fn emit_self_role_change(&self, role: Option<NodeRole>) {
        let (Some(role), Some(reconciler)) = (role, self.self_role_reconciler()) else {
            return;
        };
        reconciler.emit(role);
    }
}

/// The metadata plane's outbound "what role is *this* node" edge, as a
/// cloneable handle.
///
/// [`ClusterStateMachine`] is moved into `openraft::Raft::new` at startup, so
/// anything that needs to re-check this node's role at runtime cannot hold the
/// state machine. This carries exactly what the check needs — the live cluster
/// state, this node's id, and the role-change channel — and every one of them
/// is shared, so a clone observes the same state the state machine applies
/// into and emits onto the same channel the consumer drains.
///
/// Runtime reconciliation is what makes role convergence *eventual* rather than
/// best-effort: a data-path promotion or demotion can fail (a promotion has to
/// persist a replication identity first, a demotion needs the new primary's
/// address to be known), and a failure that nothing re-drives leaves the node
/// serving one role while the cluster believes the other until it restarts.
///
/// The channel end is **weak**. The role-change consumer owns the data path's
/// role controller — and through it the storage engine — and it stops when the
/// channel closes, which is how a shutting-down node lets go of both. A
/// reconciler holding a strong sender would keep that channel open for the
/// life of its task, so the consumer would never see the close, the store
/// would stay open, and restarting the node in the same process would fail on
/// the RocksDB lock. Losing the upgrade is therefore not an error: it means
/// the data path this reconciler exists to correct is gone
/// ([`RoleReconcile::Detached`]).
#[derive(Clone)]
pub struct SelfRoleReconciler {
    state: ClusterState,
    self_node_id: NodeId,
    role_change_tx: mpsc::WeakUnboundedSender<RoleChangeEvent>,
}

/// The outcome of one [`SelfRoleReconciler::reconcile`] pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleReconcile {
    /// The two role views agree (or this node is not a cluster member yet).
    /// The steady state: nothing was emitted.
    Agreed,
    /// The views disagreed; a role change was emitted to re-drive the data
    /// path to this cluster-state role.
    ReDriven(NodeRole),
    /// The role-change consumer is gone — the node is shutting down. A caller
    /// looping on this must stop.
    Detached,
}

impl SelfRoleReconciler {
    /// This node's cluster-state role, or `None` when it is not (yet) a member.
    pub fn self_role(&self) -> Option<NodeRole> {
        self.state.get_node(self.self_node_id).map(|n| n.role)
    }

    /// Emit a role change when the data path disagrees with cluster state.
    pub fn reconcile(&self, data_path_is_replica: bool) -> RoleReconcile {
        let Some(role) = self.self_role() else {
            return RoleReconcile::Agreed;
        };
        if (role == NodeRole::Replica) == data_path_is_replica {
            return RoleReconcile::Agreed;
        }
        if self.emit(role) {
            RoleReconcile::ReDriven(role)
        } else {
            RoleReconcile::Detached
        }
    }

    /// Send the [`RoleChangeEvent`] matching `role` for this node. Returns
    /// false once the consumer is gone.
    fn emit(&self, role: NodeRole) -> bool {
        let Some(tx) = self.role_change_tx.upgrade() else {
            return false;
        };
        let self_id = self.self_node_id;
        let epoch = self.state.config_epoch();
        tx.send(match role {
            NodeRole::Primary => RoleChangeEvent::Promoted(PromotionEvent {
                promoted_node_id: self_id,
                epoch,
            }),
            NodeRole::Replica => RoleChangeEvent::Demoted(DemotionEvent {
                demoted_node_id: self_id,
                // A snapshot carries the topology but not the causal story of
                // how it changed; the consumer resolves the primary from live
                // cluster state rather than trusting a reconstructed id.
                new_primary_id: self.state.get_node(self_id).and_then(|n| n.primary_id),
                epoch,
            }),
        })
        .is_ok()
    }
}

impl Default for ClusterStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftStateMachine<TypeConfig> for ClusterStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let inner = self.state.read_inner();
        Ok((inner.last_applied_log, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClusterResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
    {
        let mut results = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;

            match entry.payload {
                EntryPayload::Blank => {
                    results.push(ClusterResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    // apply_command owns the variant→event mapping and returns
                    // events only on its Ok path (emit-on-failure is impossible).
                    // apply's job is to route those node-agnostic events: apply
                    // the node-local self-filter it owns (self_node_id lives on
                    // the state machine, not on ClusterState) and forward to the
                    // right channel.
                    let (response, events) = self.state.apply_command(cmd).unwrap_or_else(|e| {
                        tracing::warn!(error = %e, "Failed to apply cluster command");
                        // Forward the typed error across the apply boundary
                        // instead of flattening it to a display string
                        // (proposal 32): the variant survives for consumers.
                        (ClusterResponse::Error(e), Vec::new())
                    });

                    for event in events {
                        match event {
                            // Role changes are only relevant when they name
                            // *this* node, and both kinds share one channel so
                            // the data path replays them in apply order.
                            ClusterEvent::NodeDemoted {
                                demoted_node_id,
                                new_primary_id,
                                epoch,
                            } if Some(demoted_node_id) == self.self_node_id => {
                                if let Some(ref tx) = self.role_change_tx {
                                    let _ = tx.send(RoleChangeEvent::Demoted(DemotionEvent {
                                        demoted_node_id,
                                        new_primary_id,
                                        epoch,
                                    }));
                                }
                            }
                            ClusterEvent::NodePromoted {
                                promoted_node_id,
                                epoch,
                            } if Some(promoted_node_id) == self.self_node_id => {
                                if let Some(ref tx) = self.role_change_tx {
                                    let _ = tx.send(RoleChangeEvent::Promoted(PromotionEvent {
                                        promoted_node_id,
                                        epoch,
                                    }));
                                }
                            }
                            // Migration-complete fires on ALL nodes (no self-filter).
                            ClusterEvent::SlotMigrationCompleted {
                                slot,
                                source_node,
                                target_node,
                            } => {
                                if let Some(ref tx) = self.migration_complete_tx {
                                    let _ = tx.send(SlotMigrationCompleteEvent {
                                        slot,
                                        source_node,
                                        target_node,
                                    });
                                }
                            }
                            // Handoff transitions fire on ALL nodes; the
                            // "am I the source?" filter is the consumer's, so a
                            // node that is not the source simply finds nothing
                            // to do rather than the state machine having to know
                            // its own id here.
                            ClusterEvent::SlotHandoffPrepared {
                                slot,
                                source_node,
                                target_node,
                                seq,
                                barrier_ms,
                            } => {
                                if let Some(ref tx) = self.slot_handoff_tx {
                                    let _ = tx.send(SlotHandoffEvent::Prepared {
                                        slot,
                                        source_node,
                                        target_node,
                                        seq,
                                        barrier_ms,
                                    });
                                }
                            }
                            ClusterEvent::SlotHandoffReleased {
                                slot,
                                source_node,
                                seq,
                            } => {
                                if let Some(ref tx) = self.slot_handoff_tx {
                                    let _ = tx.send(SlotHandoffEvent::Released {
                                        slot,
                                        source_node,
                                        seq,
                                    });
                                }
                            }
                            // A role change of another node: nothing to route here.
                            ClusterEvent::NodeDemoted { .. }
                            | ClusterEvent::NodePromoted { .. } => {}
                        }
                    }

                    results.push(response);
                }
                EntryPayload::Membership(membership) => {
                    self.state
                        .set_last_membership(StoredMembership::new(Some(log_id), membership));
                    results.push(ClusterResponse::Ok);
                }
            }

            // Update last applied log
            self.state.set_last_applied_log(log_id);
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ClusterStateMachine {
            state: self.state.clone(),
            self_node_id: None,
            role_change_tx: None,
            migration_complete_tx: None,
            slot_handoff_tx: None,
            // The builder must keep the store: it is the component that actually
            // produces snapshots, and an unpersisted snapshot is exactly what
            // makes log purge lossy.
            snapshot_store: self.snapshot_store.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        // `Cursor::new(Vec::new())` and `Cursor::new(vec![])` are the same
        // value, so the mutation that swaps one for the other is a documented
        // equivalent. The buffer being *empty* is the part that matters and is
        // forced by `begin_receiving_snapshot_hands_back_an_empty_buffer`.
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let snapshot_state: ClusterStateInner = serde_json::from_slice(&data).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(Some(meta.signature())),
                openraft::ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        // A snapshot install replaces the whole state without replaying the
        // entries that produced it, so the role changes folded into it emit no
        // events. Diff this node's own role across the install and synthesize
        // the one event that reconciles the data path — otherwise a node that
        // fell far enough behind to need a snapshot silently keeps the role it
        // had before it fell behind.
        let role_before = self.self_role();

        // Persist before applying: a snapshot visible in memory but absent from
        // disk is the durability gap this store exists to close.
        self.persist_snapshot(meta, &data)?;
        self.state.restore_from_snapshot(snapshot_state, meta);

        let role_after = self.self_role();
        if role_before != role_after {
            self.emit_self_role_change(role_after);
        }

        tracing::info!(
            last_log_id = ?meta.last_log_id,
            ?role_before,
            ?role_after,
            "Installed cluster state snapshot"
        );
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        // With a store attached, only a *durable* snapshot counts. Synthesizing
        // one from live state here would let openraft purge the log entries that
        // snapshot covers while nothing on disk reproduces them.
        if let Some(store) = &self.snapshot_store {
            return Ok(store.load()?.map(|stored| Snapshot {
                meta: stored.meta,
                snapshot: Box::new(Cursor::new(stored.data)),
            }));
        }

        let (meta, data) = self.state.encode_snapshot().map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        if meta.last_log_id.is_none() {
            return Ok(None);
        }

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for ClusterStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (meta, data) = self.state.encode_snapshot().map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        // openraft purges log entries covered by a snapshot it believes exists,
        // so the snapshot has to reach disk before it is handed back.
        self.persist_snapshot(&meta, &data)?;

        tracing::info!(
            last_log_id = ?meta.last_log_id,
            persisted = self.snapshot_store.is_some(),
            "Built cluster state snapshot"
        );

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ClusterError, NodeRole};
    use std::collections::HashMap;
    use std::net::SocketAddr;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    /// Take the next role change, requiring it to be a demotion.
    fn expect_demotion(rx: &mut mpsc::UnboundedReceiver<RoleChangeEvent>) -> DemotionEvent {
        match rx.try_recv().expect("expected a role-change event") {
            RoleChangeEvent::Demoted(e) => e,
            other => panic!("expected a demotion, got {other:?}"),
        }
    }

    /// Take the next role change, requiring it to be a promotion.
    fn expect_promotion(rx: &mut mpsc::UnboundedReceiver<RoleChangeEvent>) -> PromotionEvent {
        match rx.try_recv().expect("expected a role-change event") {
            RoleChangeEvent::Promoted(e) => e,
            other => panic!("expected a promotion, got {other:?}"),
        }
    }

    // FM-CLUSTER-078
    #[test]
    fn test_snapshot_observes_topology_applied_since_the_last_read() {
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(1, test_addr(6379), test_addr(16379)),
            })
            .unwrap();

        let before = state.snapshot();
        assert_eq!(before.get_slot_owner(42), None);

        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 100)],
            })
            .unwrap();

        let after = state.snapshot();
        assert_eq!(
            after.get_slot_owner(42),
            Some(1),
            "a mutation applied through apply_command must be visible in the next snapshot"
        );
        assert!(
            !Arc::ptr_eq(&before, &after),
            "a mutation must publish a new snapshot rather than edit the one readers hold"
        );
        // The snapshot a reader already held is an immutable value: the
        // mutation cannot reach back into a decision made against it.
        assert_eq!(before.get_slot_owner(42), None);
    }

    // FM-CLUSTER-078
    #[test]
    fn test_repeated_snapshots_without_mutation_share_one_allocation() {
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(1, test_addr(6379), test_addr(16379)),
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, CLUSTER_SLOTS - 1)],
            })
            .unwrap();

        let first = state.snapshot();
        let second = state.snapshot();
        assert!(
            Arc::ptr_eq(&first, &second),
            "snapshot() must hand out the published value, not a fresh copy of the slot table"
        );

        // Read-only accessors are readers too: they must not invalidate the
        // published value.
        let _ = state.get_slot_owner(7);
        let _ = state.config_epoch();
        let _ = state.get_all_nodes();
        assert!(Arc::ptr_eq(&first, &state.snapshot()));
    }

    /// Timing probe for the cost FM-CLUSTER-078's publication scheme removes,
    /// not an assertion — ignored so it never runs in CI. Reproduce with:
    ///
    /// ```text
    /// cargo test --release -p frogdb-cluster snapshot_cost -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "timing probe, not an assertion"]
    fn snapshot_cost_on_a_fully_assigned_cluster() {
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(1, test_addr(6379), test_addr(16379)),
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, CLUSTER_SLOTS - 1)],
            })
            .unwrap();

        const ITERS: u32 = 10_000;

        // What `snapshot()` used to do: rebuild the whole view per read.
        let start = std::time::Instant::now();
        for _ in 0..ITERS {
            std::hint::black_box(state.read_inner().to_snapshot());
        }
        let rebuilt = start.elapsed() / ITERS;

        // What it does now.
        let start = std::time::Instant::now();
        for _ in 0..ITERS {
            std::hint::black_box(state.snapshot());
        }
        let published = start.elapsed() / ITERS;

        println!("rebuild per call:   {rebuilt:?}");
        println!("published per call: {published:?}");
    }

    // FM-CLUSTER-078
    #[test]
    fn test_rejected_command_leaves_snapshot_agreeing_with_state() {
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(1, test_addr(6379), test_addr(16379)),
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 10)],
            })
            .unwrap();

        // Rejected halfway through its validation loop.
        let err = state.apply_command(ClusterCommand::RemoveSlots {
            node_id: 1,
            slots: vec![SlotRange::new(5, 20)],
        });
        assert!(err.is_err());

        let snapshot = state.snapshot();
        assert_eq!(snapshot.get_slot_owner(5), Some(1), "no slot was removed");
        assert_eq!(snapshot.slot_assignment, state.read_inner().slot_assignment);
        assert_eq!(snapshot.nodes, state.read_inner().nodes);
        assert_eq!(snapshot.config_epoch, state.config_epoch());
    }

    // FM-CLUSTER-078
    #[test]
    fn test_snapshot_install_republishes_the_reader_view() {
        let state = ClusterState::new();
        let stale = state.snapshot();

        let mut restored = ClusterStateInner::default();
        restored.nodes.insert(
            9,
            NodeInfo::new_primary(9, test_addr(6389), test_addr(16389)),
        );
        restored.slot_assignment.insert(3, 9);
        restored.config_epoch = 12;
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "snapshot-0".to_string(),
        };
        state.restore_from_snapshot(restored, &meta);

        let fresh = state.snapshot();
        assert!(!Arc::ptr_eq(&stale, &fresh));
        assert_eq!(fresh.get_slot_owner(3), Some(9));
        assert_eq!(fresh.config_epoch, 12);
        assert!(fresh.get_node(9).is_some());
    }

    // FM-CLUSTER-001
    #[test]
    fn test_add_node() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));

        let result = state.apply_command(ClusterCommand::AddNode { node: node.clone() });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));

        let retrieved = state.get_node(1).unwrap();
        assert_eq!(retrieved.addr, test_addr(6379));
    }

    // FM-CLUSTER-001
    #[test]
    fn test_apply_local_shares_validated_path() {
        // Bootstrap seeding goes through apply_local, which must enforce the
        // same invariants as apply_command (single validated mutation path).
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_local(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_local(ClusterCommand::AddNode { node: node2 })
            .unwrap();

        // First-time seeding on a fresh empty state must succeed.
        state
            .apply_local(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 100)],
            })
            .unwrap();
        assert_eq!(state.get_slot_owner(50), Some(1));

        // Seeding a slot already owned by another node is rejected, exactly as
        // apply_command would reject it — the bypass no longer silently wins.
        let result = state.apply_local(ClusterCommand::AssignSlots {
            node_id: 2,
            slots: vec![SlotRange::single(50)],
        });
        assert!(matches!(
            result,
            Err(ClusterError::SlotAlreadyAssigned(50, 1))
        ));

        // Assigning to an unknown node is rejected rather than silently skipped.
        let result = state.apply_local(ClusterCommand::AssignSlots {
            node_id: 999,
            slots: vec![SlotRange::single(200)],
        });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(999))));
    }

    // FM-CLUSTER-001
    #[test]
    fn test_add_duplicate_node() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));

        state
            .apply_command(ClusterCommand::AddNode { node: node.clone() })
            .unwrap();
        // AddNode is an upsert — adding the same node again should succeed
        let result = state.apply_command(ClusterCommand::AddNode {
            node: NodeInfo::new_primary(1, test_addr(6380), test_addr(16380)),
        });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));

        // Verify the node was updated with the new addresses
        let info = state.get_node(1).unwrap();
        assert_eq!(info.addr, test_addr(6380));
        assert_eq!(info.cluster_addr, test_addr(16380));
    }

    // FM-CLUSTER-001
    #[test]
    fn test_add_node_reregistration_keeps_recorded_role() {
        // A node re-registers itself on every boot with the only role it can
        // know unaided — primary. If `AddNode` took that at face value, a
        // restarted replica would demote-by-restart into a slotless primary and
        // its primary would lose a replica. Re-registration refreshes the
        // address; `SetRole`/`Failover` own the role.
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(1, test_addr(6379), test_addr(16379)),
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(2, test_addr(6380), test_addr(16380)),
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::SetRole {
                node_id: 2,
                role: NodeRole::Replica,
                primary_id: Some(1),
            })
            .unwrap();

        // Node 2 restarts and re-registers, claiming primary on a new address.
        state
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(2, test_addr(6390), test_addr(16390)),
            })
            .unwrap();

        let info = state.get_node(2).unwrap();
        assert_eq!(info.role, NodeRole::Replica, "recorded role must survive");
        assert_eq!(info.primary_id, Some(1), "its primary must survive too");
        assert_eq!(info.addr, test_addr(6390), "the address still refreshes");
        assert_eq!(info.cluster_addr, test_addr(16390));
    }

    // FM-CLUSTER-003
    #[test]
    fn test_assign_slots() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();

        let result = state.apply_command(ClusterCommand::AssignSlots {
            node_id: 1,
            slots: vec![SlotRange::new(0, 100)],
        });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));

        assert_eq!(state.get_slot_owner(50), Some(1));
        assert_eq!(state.get_slot_owner(101), None);
    }

    // FM-CLUSTER-002
    #[test]
    fn test_remove_node_clears_slots() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 100)],
            })
            .unwrap();

        state
            .apply_command(ClusterCommand::RemoveNode { node_id: 1 })
            .unwrap();

        assert_eq!(state.get_slot_owner(50), None);
        assert!(state.get_node(1).is_none());
    }

    // FM-CLUSTER-005
    #[test]
    fn test_set_role() {
        let state = ClusterState::new();
        let primary = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let replica = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: primary })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: replica })
            .unwrap();

        state
            .apply_command(ClusterCommand::SetRole {
                node_id: 2,
                role: NodeRole::Replica,
                primary_id: Some(1),
            })
            .unwrap();

        let node = state.get_node(2).unwrap();
        assert!(node.is_replica());
        assert_eq!(node.primary_id, Some(1));
    }

    // FM-CLUSTER-015
    #[test]
    fn test_increment_epoch() {
        let state = ClusterState::new();
        assert_eq!(state.config_epoch(), 0);

        state.apply_command(ClusterCommand::IncrementEpoch).unwrap();
        assert_eq!(state.config_epoch(), 1);

        state.apply_command(ClusterCommand::IncrementEpoch).unwrap();
        assert_eq!(state.config_epoch(), 2);
    }

    // FM-CLUSTER-031
    #[test]
    fn test_slot_migration() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::single(42)],
            })
            .unwrap();

        // Begin migration
        state
            .apply_command(ClusterCommand::BeginSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();

        assert!(state.is_slot_migrating(42));
        let migration = state.get_slot_migration(42).unwrap();
        assert_eq!(migration.source_node, 1);
        assert_eq!(migration.target_node, 2);

        // Complete migration
        let proposed_at_ms = state.arm_handoff_for_test(42, 1, 2);
        state
            .apply_command(ClusterCommand::CompleteSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
                proposed_at_ms,
            })
            .unwrap();

        assert!(!state.is_slot_migrating(42));
        assert_eq!(state.get_slot_owner(42), Some(2));
    }

    // FM-CLUSTER-043
    #[tokio::test]
    async fn test_demotion_detection_fires_for_self() {
        let cluster = ClusterState::new();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_role_change_detection(2);

        // Add two nodes
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        sm.state()
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        sm.state()
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();

        // SetRole demoting node 2 to replica
        let entry = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 1,
            },
            payload: EntryPayload::Normal(ClusterCommand::SetRole {
                node_id: 2,
                role: NodeRole::Replica,
                primary_id: Some(1),
            }),
        };

        sm.apply(vec![entry]).await.unwrap();

        let event = expect_demotion(&mut rx);
        assert_eq!(event.demoted_node_id, 2);
        assert_eq!(event.new_primary_id, Some(1));
    }

    // FM-CLUSTER-043
    #[tokio::test]
    async fn test_demotion_detection_ignores_other_nodes() {
        let cluster = ClusterState::new();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_role_change_detection(1); // Watching node 1

        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        sm.state()
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        sm.state()
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();

        // SetRole demoting node 2 (not self)
        let entry = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 1,
            },
            payload: EntryPayload::Normal(ClusterCommand::SetRole {
                node_id: 2,
                role: NodeRole::Replica,
                primary_id: Some(1),
            }),
        };

        sm.apply(vec![entry]).await.unwrap();

        // No event for node 1 watching
        assert!(rx.try_recv().is_err());
    }

    // FM-CLUSTER-043
    #[tokio::test]
    async fn test_demotion_detection_not_fired_for_rejected_set_role() {
        let cluster = ClusterState::new();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_role_change_detection(2); // self = node 2

        // Only node 1 exists; node 2 (self) was never added to the topology.
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        sm.state()
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();

        // SetRole self-demotion whose target node is absent -> apply_command
        // rejects it with NodeNotFound. A rejected mutation must NOT emit a
        // demotion event into the role machinery.
        let entry = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 1,
            },
            payload: EntryPayload::Normal(ClusterCommand::SetRole {
                node_id: 2,
                role: NodeRole::Replica,
                primary_id: Some(1),
            }),
        };

        let responses = sm.apply(vec![entry]).await.unwrap();

        // Response is an error and no demotion event was emitted. The typed
        // ClusterError survives the full Raft apply boundary (proposal 32) —
        // the variant can be named, not just `Error(_)`.
        assert!(matches!(
            responses[0],
            ClusterResponse::Error(ClusterError::NodeNotFound(2))
        ));
        assert!(rx.try_recv().is_err());
    }

    // FM-CLUSTER-034
    #[tokio::test]
    async fn test_migration_complete_event_fires() {
        let cluster = ClusterState::new();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_migration_complete_notification();

        // Add two nodes and assign slot 42 to node 1
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        sm.state()
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        sm.state()
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();
        sm.state()
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::single(42)],
            })
            .unwrap();

        // Begin migration
        let begin = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 1,
            },
            payload: EntryPayload::Normal(ClusterCommand::BeginSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
            }),
        };
        sm.apply(vec![begin]).await.unwrap();
        // No event for begin
        assert!(rx.try_recv().is_err());

        // Complete migration
        let proposed_at_ms = sm.state().arm_handoff_for_test(42, 1, 2);
        let complete = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 2,
            },
            payload: EntryPayload::Normal(ClusterCommand::CompleteSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
                proposed_at_ms,
            }),
        };
        sm.apply(vec![complete]).await.unwrap();

        let event = rx.try_recv().unwrap();
        assert_eq!(event.slot, 42);
        assert_eq!(event.source_node, 1);
        assert_eq!(event.target_node, 2);
    }

    // ========================================================================
    // FinalizeUpgrade tests
    // ========================================================================

    // FM-CLUSTER-008
    #[test]
    fn test_finalize_upgrade_succeeds_when_all_nodes_at_target() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();

        state.set_all_node_versions("0.2.0");

        let result = state.apply_command(ClusterCommand::FinalizeUpgrade {
            version: "0.2.0".to_string(),
        });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));
        assert_eq!(state.active_version(), Some("0.2.0".to_string()));
    }

    // FM-CLUSTER-008
    #[test]
    fn test_finalize_upgrade_rejects_when_node_behind() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();

        state.set_node_version(1, "0.2.0".to_string());
        state.set_node_version(2, "0.1.0".to_string());

        let result = state.apply_command(ClusterCommand::FinalizeUpgrade {
            version: "0.2.0".to_string(),
        });
        assert!(matches!(result, Err(ClusterError::InvalidOperation(_))));
        assert_eq!(state.active_version(), None);
    }

    // FM-CLUSTER-008
    #[test]
    fn test_finalize_upgrade_rejects_empty_version_node() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();

        state.set_node_version(1, "0.2.0".to_string());
        state.set_node_version(2, String::new());

        let result = state.apply_command(ClusterCommand::FinalizeUpgrade {
            version: "0.2.0".to_string(),
        });
        assert!(matches!(result, Err(ClusterError::InvalidOperation(_))));
    }

    // FM-CLUSTER-008
    #[test]
    fn test_finalize_upgrade_allows_nodes_ahead_of_target() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();

        state.set_node_version(1, "0.2.1".to_string());

        let result = state.apply_command(ClusterCommand::FinalizeUpgrade {
            version: "0.2.0".to_string(),
        });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));
        assert_eq!(state.active_version(), Some("0.2.0".to_string()));
    }

    // FM-CLUSTER-008
    #[test]
    fn test_finalize_upgrade_invalid_target_version() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();

        let result = state.apply_command(ClusterCommand::FinalizeUpgrade {
            version: "not-a-version".to_string(),
        });
        assert!(matches!(result, Err(ClusterError::InvalidOperation(_))));
    }

    // FM-CLUSTER-008
    #[test]
    fn finalize_upgrade_rejects_an_unparseable_target_with_no_nodes() {
        // Zero members — bootstrap, or the aftermath of `CLUSTER RESET HARD`.
        // The per-node validation loop never runs here, so the target parse
        // has to happen outside it or a garbage string lands in
        // `active_version` and permanently disables every version gate.
        let state = ClusterState::new();
        assert!(state.snapshot().nodes.is_empty());

        for garbage in ["not-a-version", "", "1", "v1.2.3"] {
            let result = state.apply_command(ClusterCommand::FinalizeUpgrade {
                version: garbage.to_string(),
            });
            match result {
                Err(ClusterError::InvalidOperation(msg)) => {
                    assert!(msg.contains("invalid target version"), "{msg}");
                }
                other => panic!("expected InvalidOperation for {garbage:?}, got {other:?}"),
            }
            assert_eq!(
                state.active_version(),
                None,
                "a refused finalization must leave the active version untouched"
            );
        }
    }

    // FM-CLUSTER-008
    #[test]
    fn finalize_upgrade_with_no_nodes_accepts_a_valid_target() {
        // The empty-cluster success path is legitimate (bootstrap) and must
        // survive the hoisted parse.
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::FinalizeUpgrade {
                version: "1.4.0".to_string(),
            })
            .expect("a valid target on an empty cluster is accepted");
        assert_eq!(state.active_version(), Some("1.4.0".to_string()));
    }

    // FM-CLUSTER-008
    #[test]
    fn test_finalize_upgrade_idempotent() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();

        state.set_node_version(1, "0.2.0".to_string());

        state
            .apply_command(ClusterCommand::FinalizeUpgrade {
                version: "0.2.0".to_string(),
            })
            .unwrap();
        assert_eq!(state.active_version(), Some("0.2.0".to_string()));

        // Second finalize to same version should also succeed
        let result = state.apply_command(ClusterCommand::FinalizeUpgrade {
            version: "0.2.0".to_string(),
        });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));
    }

    // FM-CLUSTER-001
    #[test]
    fn test_add_node_mixed_version_succeeds() {
        let state = ClusterState::new();
        let mut node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        node1.version = "0.1.0".to_string();
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();

        let mut node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        node2.version = "0.2.0".to_string();
        // Should succeed even with version mismatch (warning only)
        let result = state.apply_command(ClusterCommand::AddNode { node: node2 });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));
    }

    // ========================================================================
    // config_epoch collision resolution at AddNode (issue 64)
    // ========================================================================

    /// Build a primary claiming a specific `config_epoch`, as a node rejoining
    /// with restored on-disk state would.
    fn primary_claiming(id: NodeId, port: u16, epoch: ConfigEpoch) -> NodeInfo {
        let mut node = NodeInfo::new_primary(id, test_addr(port), test_addr(port + 10000));
        node.config_epoch = epoch;
        node
    }

    /// The headline case: a second primary claiming an epoch an existing primary
    /// already holds is admitted with a *freshly minted* epoch, and the incumbent
    /// keeps the contested one. The cluster-wide counter advances with it, so it
    /// still dominates every per-node epoch.
    // FM-CLUSTER-011
    #[test]
    fn test_add_node_epoch_collision_reassigns_incoming_node() {
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::AddNode {
                node: primary_claiming(1, 6379, 5),
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode {
                node: primary_claiming(2, 6380, 5),
            })
            .unwrap();

        assert_eq!(
            state.get_node(1).unwrap().config_epoch,
            5,
            "incumbent keeps"
        );
        let reassigned = state.get_node(2).unwrap().config_epoch;
        assert_ne!(reassigned, 5, "collision must be resolved");
        assert!(reassigned > 5, "fresh epoch must exceed every existing one");
        assert_eq!(state.config_epoch(), reassigned);
    }

    /// The minted epoch comes from the cluster-wide counter, so it clears a
    /// counter that has already run ahead of every per-node epoch (e.g. after
    /// `IncrementEpoch`), not just the contested value.
    // FM-CLUSTER-010
    #[test]
    fn test_add_node_collision_mints_above_cluster_counter() {
        let state = ClusterState::new();
        for _ in 0..9 {
            state.apply_command(ClusterCommand::IncrementEpoch).unwrap();
        }
        assert_eq!(state.config_epoch(), 9);

        state
            .apply_command(ClusterCommand::AddNode {
                node: primary_claiming(1, 6379, 3),
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode {
                node: primary_claiming(2, 6380, 3),
            })
            .unwrap();

        assert_eq!(state.get_node(1).unwrap().config_epoch, 3);
        assert_eq!(state.get_node(2).unwrap().config_epoch, 10);
        assert_eq!(state.config_epoch(), 10);
    }

    /// An uncontested nonzero claim is recorded verbatim, but pulls the
    /// cluster-wide counter up to it: the counter must never trail a per-node
    /// epoch, or the next mint would hand out a duplicate.
    // FM-CLUSTER-010
    #[test]
    fn test_add_node_uncontested_epoch_raises_cluster_counter() {
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::AddNode {
                node: primary_claiming(1, 6379, 9),
            })
            .unwrap();

        assert_eq!(state.get_node(1).unwrap().config_epoch, 9);
        assert_eq!(state.config_epoch(), 9);
    }

    /// `config_epoch == 0` means "unassigned" — the bootstrap/self-registration
    /// convention — so several fresh nodes at 0 are not a collision.
    // FM-CLUSTER-012
    #[test]
    fn test_add_node_zero_epoch_is_not_a_collision() {
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(1, test_addr(6379), test_addr(16379)),
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(2, test_addr(6380), test_addr(16380)),
            })
            .unwrap();

        assert_eq!(state.get_node(1).unwrap().config_epoch, 0);
        assert_eq!(state.get_node(2).unwrap().config_epoch, 0);
        assert_eq!(state.config_epoch(), 0);
    }

    /// Self-registration rebuilds `NodeInfo` from scratch (epoch 0), so an
    /// upsert must not reset an epoch the node already earned — otherwise a
    /// restart would silently free an epoch for someone else to claim.
    // FM-CLUSTER-012
    #[test]
    fn test_add_node_zero_epoch_preserves_recorded_epoch() {
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::AddNode {
                node: primary_claiming(1, 6379, 7),
            })
            .unwrap();

        // Re-register with fresh addresses and no epoch, as a restart does.
        state
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(1, test_addr(6390), test_addr(16390)),
            })
            .unwrap();

        let node = state.get_node(1).unwrap();
        assert_eq!(node.addr, test_addr(6390), "address update still applies");
        assert_eq!(node.config_epoch, 7, "recorded epoch is not reset to 0");
    }

    /// Only primaries arbitrate slot ownership, so only primaries collide —
    /// matching Redis's `clusterHandleConfigEpochCollision`, which returns early
    /// unless both nodes are masters.
    // FM-CLUSTER-011
    #[test]
    fn test_add_node_replica_sharing_primary_epoch_is_not_a_collision() {
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::AddNode {
                node: primary_claiming(1, 6379, 3),
            })
            .unwrap();

        let mut replica = NodeInfo::new_replica(2, test_addr(6380), test_addr(16380), 1);
        replica.config_epoch = 3;
        state
            .apply_command(ClusterCommand::AddNode { node: replica })
            .unwrap();

        assert_eq!(state.get_node(1).unwrap().config_epoch, 3);
        assert_eq!(state.get_node(2).unwrap().config_epoch, 3);
    }

    /// A node re-registering with the epoch it already holds is an upsert, not a
    /// collision with itself.
    // FM-CLUSTER-011
    #[test]
    fn test_add_node_self_epoch_is_not_a_collision() {
        let state = ClusterState::new();
        state
            .apply_command(ClusterCommand::AddNode {
                node: primary_claiming(1, 6379, 4),
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode {
                node: primary_claiming(1, 6390, 4),
            })
            .unwrap();

        let node = state.get_node(1).unwrap();
        assert_eq!(node.config_epoch, 4);
        assert_eq!(node.addr, test_addr(6390));
    }

    /// The invariant every epoch-touching command must preserve:
    /// `config_epoch >= max(per-node config_epoch)`.
    ///
    /// `CLUSTER INFO` reports the counter verbatim (the `max(counter, raft_term)`
    /// fold is gone), so `cluster_current_epoch >= cluster_my_epoch` now holds
    /// only because the state machine keeps it true. A command that recorded a
    /// per-node epoch above the counter would also make the next mint hand out a
    /// duplicate, so this is a correctness invariant, not just a display one.
    ///
    /// Driven as a sweep over a mixed sequence rather than one case per command:
    /// the invariant is global, and the failure mode is a *new* command forgetting
    /// it — which a per-command test cannot catch.
    // FM-CLUSTER-010
    #[test]
    fn test_config_epoch_counter_dominates_every_node_epoch_across_command_sequence() {
        let state = ClusterState::new();

        let assert_invariant = |label: &str| {
            let inner = state.read_inner();
            let highest = inner
                .nodes
                .values()
                .map(|node| node.config_epoch)
                .max()
                .unwrap_or(0);
            assert!(
                inner.config_epoch >= highest,
                "after {label}: counter {} trails node epoch {highest}",
                inner.config_epoch
            );

            // The second half of the same guarantee: an epoch identifies at most
            // one primary. Two primaries at the same nonzero epoch is the
            // collision Redis's `clusterHandleConfigEpochCollision` exists to
            // break, and it is what makes "highest epoch wins" ambiguous when
            // two nodes claim the same slot. Epoch 0 is the unassigned marker
            // and is deliberately shareable.
            let mut by_epoch: HashMap<ConfigEpoch, NodeId> = HashMap::new();
            for node in inner.nodes.values().filter(|n| n.is_primary()) {
                if node.config_epoch == 0 {
                    continue;
                }
                if let Some(other) = by_epoch.insert(node.config_epoch, node.id) {
                    panic!(
                        "after {label}: primaries {other} and {} share config_epoch {}",
                        node.id, node.config_epoch
                    );
                }
            }
        };

        let sequence = vec![
            // Fresh nodes at the "unassigned" epoch.
            (
                "add node 1",
                ClusterCommand::AddNode {
                    node: NodeInfo::new_primary(1, test_addr(6379), test_addr(16379)),
                },
            ),
            (
                "add node 2",
                ClusterCommand::AddNode {
                    node: NodeInfo::new_primary(2, test_addr(6380), test_addr(16380)),
                },
            ),
            // A rejoining node carrying a large restored epoch: the ratchet case.
            (
                "add node 3 claiming 42",
                ClusterCommand::AddNode {
                    node: primary_claiming(3, 6381, 42),
                },
            ),
            // A colliding claim: resolution mints above the counter.
            (
                "add node 4 colliding at 42",
                ClusterCommand::AddNode {
                    node: primary_claiming(4, 6382, 42),
                },
            ),
            ("increment", ClusterCommand::IncrementEpoch),
            (
                "assign slots",
                ClusterCommand::AssignSlots {
                    node_id: 3,
                    slots: vec![SlotRange::new(0, 100)],
                },
            ),
            (
                "mark 3 failed",
                ClusterCommand::MarkNodeFailed { node_id: 3 },
            ),
            (
                "fail over 3 to 4",
                ClusterCommand::Failover {
                    old_primary_id: 3,
                    new_primary_id: 4,
                    force: true,
                },
            ),
            (
                "demote 2",
                ClusterCommand::SetRole {
                    node_id: 2,
                    role: NodeRole::Replica,
                    primary_id: Some(4),
                },
            ),
            ("remove node 1", ClusterCommand::RemoveNode { node_id: 1 }),
            // Re-add the removed node claiming the epoch it left with: the
            // counter must still dominate afterwards.
            (
                "re-add node 1 claiming 41",
                ClusterCommand::AddNode {
                    node: primary_claiming(1, 6379, 41),
                },
            ),
            (
                "hard reset on node 4",
                ClusterCommand::ResetCluster {
                    node_id: 4,
                    new_node_id: Some(99),
                },
            ),
        ];

        assert_invariant("empty state");
        for (label, command) in sequence {
            // Every step's outcome is pinned: a sweep that silently accepted
            // errors would keep passing if a command started rejecting the
            // input it is supposed to handle, and the invariant would then be
            // asserted against a state that never changed.
            state
                .apply_command(command)
                .unwrap_or_else(|e| panic!("step \"{label}\" must succeed, got {e:?}"));
            assert_invariant(label);
        }

        // The last step resets node 4 to id 99, which forgets every peer: the
        // final membership is that node alone. Pinned so a change in any
        // command's semantics shows up here rather than silently reshaping the
        // state the invariant was being checked against.
        let inner = state.read_inner();
        let mut ids: Vec<NodeId> = inner.nodes.keys().copied().collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![99], "final membership after the hard reset");
        assert_eq!(
            inner.slot_assignment.len(),
            0,
            "the hard reset must drop every slot assignment"
        );
    }

    // ========================================================================
    // Zero-coverage command tests
    // ========================================================================

    // FM-CLUSTER-004
    #[test]
    fn test_remove_slots_success() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 100)],
            })
            .unwrap();

        let result = state.apply_command(ClusterCommand::RemoveSlots {
            node_id: 1,
            slots: vec![SlotRange::new(0, 100)],
        });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));
        assert_eq!(state.get_slot_owner(50), None);
    }

    // FM-CLUSTER-004
    #[test]
    fn test_remove_slots_not_assigned() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();

        let result = state.apply_command(ClusterCommand::RemoveSlots {
            node_id: 1,
            slots: vec![SlotRange::single(50)],
        });
        assert!(matches!(result, Err(ClusterError::SlotNotAssigned(_))));
    }

    // FM-CLUSTER-013
    #[test]
    fn test_mark_node_failed() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();

        let result = state.apply_command(ClusterCommand::MarkNodeFailed { node_id: 1 });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));

        let info = state.get_node(1).unwrap();
        assert!(info.flags.fail);
    }

    // FM-CLUSTER-013
    #[test]
    fn test_mark_node_failed_nonexistent() {
        let state = ClusterState::new();

        let result = state.apply_command(ClusterCommand::MarkNodeFailed { node_id: 999 });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

    // FM-CLUSTER-014
    #[test]
    fn test_mark_node_recovered() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();
        state
            .apply_command(ClusterCommand::MarkNodeFailed { node_id: 1 })
            .unwrap();

        let result = state.apply_command(ClusterCommand::MarkNodeRecovered { node_id: 1 });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));

        let info = state.get_node(1).unwrap();
        assert!(!info.flags.fail);
        assert!(!info.flags.pfail);
    }

    // FM-CLUSTER-014
    #[test]
    fn test_mark_node_recovered_nonexistent() {
        let state = ClusterState::new();

        let result = state.apply_command(ClusterCommand::MarkNodeRecovered { node_id: 999 });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

    // FM-CLUSTER-035
    #[test]
    fn test_cancel_slot_migration() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::single(42)],
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::BeginSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();

        let result = state.apply_command(ClusterCommand::CancelSlotMigration { slot: 42 });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));
        assert!(!state.is_slot_migrating(42));
    }

    // FM-CLUSTER-035
    #[test]
    fn test_cancel_slot_migration_nonexistent() {
        let state = ClusterState::new();

        // CancelSlotMigration is infallible — cancelling a non-migrating slot succeeds
        let result = state.apply_command(ClusterCommand::CancelSlotMigration { slot: 42 });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));
    }

    // FM-CLUSTER-006
    #[test]
    fn test_reset_cluster_soft() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 8191)],
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 2,
                slots: vec![SlotRange::new(8192, 16383)],
            })
            .unwrap();
        state.apply_command(ClusterCommand::IncrementEpoch).unwrap();

        let result = state.apply_command(ClusterCommand::ResetCluster {
            node_id: 1,
            new_node_id: None,
        });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));

        // Only node 1 remains
        assert!(state.get_node(1).is_some());
        assert!(state.get_node(2).is_none());

        // Slots and migrations cleared
        assert_eq!(state.get_slot_owner(50), None);
        assert_eq!(state.get_slot_owner(10000), None);

        // Epoch preserved in soft reset
        assert_eq!(state.config_epoch(), 1);

        // Node 1 is a primary
        let info = state.get_node(1).unwrap();
        assert_eq!(info.role, NodeRole::Primary);
    }

    // FM-CLUSTER-006
    #[test]
    fn test_reset_cluster_hard() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 8191)],
            })
            .unwrap();
        // Increment epoch to 3
        state.apply_command(ClusterCommand::IncrementEpoch).unwrap();
        state.apply_command(ClusterCommand::IncrementEpoch).unwrap();
        state.apply_command(ClusterCommand::IncrementEpoch).unwrap();

        let result = state.apply_command(ClusterCommand::ResetCluster {
            node_id: 1,
            new_node_id: Some(99),
        });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));

        // Old node 1 is gone, new node 99 exists with same address
        assert!(state.get_node(1).is_none());
        let info = state.get_node(99).unwrap();
        assert_eq!(info.addr, test_addr(6379));
        assert_eq!(info.role, NodeRole::Primary);

        // Epoch reset to 0 in hard reset
        assert_eq!(state.config_epoch(), 0);
    }

    // FM-CLUSTER-006
    #[test]
    fn test_reset_cluster_nonexistent_node() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::single(42)],
            })
            .unwrap();

        let result = state.apply_command(ClusterCommand::ResetCluster {
            node_id: 999,
            new_node_id: None,
        });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));

        // All nodes, slots, and migrations cleared
        assert!(state.get_node(1).is_none());
        assert!(state.get_node(999).is_none());
        assert_eq!(state.get_slot_owner(42), None);
    }

    // ========================================================================
    // Error-path tests for commands with happy-path-only coverage
    // ========================================================================

    // FM-CLUSTER-002
    #[test]
    fn test_remove_node_nonexistent() {
        let state = ClusterState::new();

        let result = state.apply_command(ClusterCommand::RemoveNode { node_id: 999 });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

    // FM-CLUSTER-003
    #[test]
    fn test_assign_slots_node_not_found() {
        let state = ClusterState::new();

        let result = state.apply_command(ClusterCommand::AssignSlots {
            node_id: 999,
            slots: vec![SlotRange::single(50)],
        });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

    // FM-CLUSTER-003
    #[test]
    fn test_assign_slots_already_assigned() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::single(50)],
            })
            .unwrap();

        let result = state.apply_command(ClusterCommand::AssignSlots {
            node_id: 2,
            slots: vec![SlotRange::single(50)],
        });
        assert!(matches!(
            result,
            Err(ClusterError::SlotAlreadyAssigned(50, 1))
        ));
    }

    // FM-CLUSTER-003
    #[test]
    fn test_assign_slots_idempotent() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::single(50)],
            })
            .unwrap();

        // Assigning the same slot to the same node again should succeed
        let result = state.apply_command(ClusterCommand::AssignSlots {
            node_id: 1,
            slots: vec![SlotRange::single(50)],
        });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));
    }

    // FM-CLUSTER-005
    #[test]
    fn test_set_role_node_not_found() {
        let state = ClusterState::new();

        let result = state.apply_command(ClusterCommand::SetRole {
            node_id: 999,
            role: NodeRole::Primary,
            primary_id: None,
        });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

    // FM-CLUSTER-005
    #[test]
    fn test_set_role_replica_without_primary() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();

        let result = state.apply_command(ClusterCommand::SetRole {
            node_id: 1,
            role: NodeRole::Replica,
            primary_id: None,
        });
        assert!(matches!(result, Err(ClusterError::InvalidOperation(_))));
    }

    // FM-CLUSTER-005
    #[test]
    fn test_set_role_replica_primary_not_found() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();

        let result = state.apply_command(ClusterCommand::SetRole {
            node_id: 1,
            role: NodeRole::Replica,
            primary_id: Some(999),
        });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

    // FM-CLUSTER-032
    #[test]
    fn test_begin_migration_source_not_found() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();

        let result = state.apply_command(ClusterCommand::BeginSlotMigration {
            slot: 42,
            source_node: 999,
            target_node: 2,
        });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

    // FM-CLUSTER-032
    #[test]
    fn test_begin_migration_target_not_found() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();

        let result = state.apply_command(ClusterCommand::BeginSlotMigration {
            slot: 42,
            source_node: 1,
            target_node: 999,
        });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

    // FM-CLUSTER-031
    #[test]
    fn test_begin_migration_already_in_progress() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        let node3 = NodeInfo::new_primary(3, test_addr(6381), test_addr(16381));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node3 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::single(42)],
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::BeginSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();

        // Different migration on the same slot returns MigrationInProgress
        let result = state.apply_command(ClusterCommand::BeginSlotMigration {
            slot: 42,
            source_node: 1,
            target_node: 3,
        });
        assert!(matches!(result, Err(ClusterError::MigrationInProgress(42))));
    }

    // FM-CLUSTER-031
    #[test]
    fn test_begin_migration_idempotent() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::single(42)],
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::BeginSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();

        // Same exact migration again should succeed (idempotent)
        let result = state.apply_command(ClusterCommand::BeginSlotMigration {
            slot: 42,
            source_node: 1,
            target_node: 2,
        });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));
    }

    // FM-CLUSTER-032
    #[test]
    fn test_begin_migration_wrong_owner() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::single(42)],
            })
            .unwrap();

        // source_node=2 but slot 42 is owned by node 1
        let result = state.apply_command(ClusterCommand::BeginSlotMigration {
            slot: 42,
            source_node: 2,
            target_node: 1,
        });
        assert!(matches!(result, Err(ClusterError::InvalidOperation(_))));
    }

    // FM-CLUSTER-033
    #[test]
    fn test_complete_migration_no_active() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();

        let result = state.apply_command(ClusterCommand::CompleteSlotMigration {
            slot: 42,
            source_node: 1,
            target_node: 2,
            proposed_at_ms: 1_000_000,
        });
        assert!(matches!(result, Err(ClusterError::InvalidOperation(_))));
    }

    // ========================================================================
    // Failover composite command tests
    // ========================================================================

    /// Build: node 1 = primary owning slots 0-100, nodes 2 and 3 = replicas of 1.
    fn failover_fixture() -> ClusterState {
        let state = ClusterState::new();
        let primary = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let replica2 = NodeInfo::new_replica(2, test_addr(6380), test_addr(16380), 1);
        let replica3 = NodeInfo::new_replica(3, test_addr(6381), test_addr(16381), 1);
        for node in [primary, replica2, replica3] {
            state
                .apply_command(ClusterCommand::AddNode { node })
                .unwrap();
        }
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 100)],
            })
            .unwrap();
        state
    }

    // FM-CLUSTER-040
    #[test]
    fn test_failover_force_removes_old_and_transfers_everything() {
        let state = failover_fixture();
        let epoch_before = state.config_epoch();

        let result = state.apply_command(ClusterCommand::Failover {
            old_primary_id: 1,
            new_primary_id: 2,
            force: true,
        });
        assert!(matches!(result, Ok((ClusterResponse::Epoch(_), _))));

        // Old primary removed
        assert!(state.get_node(1).is_none());

        // Successor promoted
        let new_primary = state.get_node(2).unwrap();
        assert!(new_primary.is_primary());
        assert_eq!(new_primary.primary_id, None);

        // All slots transferred (none ownerless)
        for slot in 0..=100u16 {
            assert_eq!(state.get_slot_owner(slot), Some(2), "slot {slot}");
        }

        // Sibling replica re-parented to the successor
        assert_eq!(state.get_node(3).unwrap().primary_id, Some(2));

        // Epoch bumped exactly once, claimed by the successor
        assert_eq!(state.config_epoch(), epoch_before + 1);
        assert_eq!(new_primary.config_epoch, epoch_before + 1);
    }

    // FM-CLUSTER-041
    #[test]
    fn test_failover_graceful_demotes_old_primary() {
        let state = failover_fixture();
        let epoch_before = state.config_epoch();

        state
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: false,
            })
            .unwrap();

        // Old primary demoted to a replica of the successor (not removed)
        let old = state.get_node(1).unwrap();
        assert!(old.is_replica());
        assert_eq!(old.primary_id, Some(2));

        // Successor promoted, owns the slots
        assert!(state.get_node(2).unwrap().is_primary());
        assert_eq!(state.get_slot_owner(50), Some(2));

        // Sibling replica re-parented
        assert_eq!(state.get_node(3).unwrap().primary_id, Some(2));

        assert_eq!(state.config_epoch(), epoch_before + 1);
    }

    // FM-CLUSTER-039
    #[test]
    fn test_failover_validation_failure_mutates_nothing() {
        let state = failover_fixture();
        let epoch_before = state.config_epoch();

        // Target does not exist — the whole transition must be rejected.
        let result = state.apply_command(ClusterCommand::Failover {
            old_primary_id: 1,
            new_primary_id: 999,
            force: true,
        });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(999))));

        // Nothing changed: node, role, slots, epoch all intact.
        let old = state.get_node(1).unwrap();
        assert!(old.is_primary());
        assert_eq!(state.get_slot_owner(50), Some(1));
        assert_eq!(state.config_epoch(), epoch_before);
        assert_eq!(state.get_node(2).unwrap().primary_id, Some(1));
    }

    // FM-CLUSTER-039
    #[test]
    fn test_failover_graceful_requires_old_node() {
        let state = failover_fixture();
        state
            .apply_command(ClusterCommand::RemoveNode { node_id: 1 })
            .unwrap();

        let result = state.apply_command(ClusterCommand::Failover {
            old_primary_id: 1,
            new_primary_id: 2,
            force: false,
        });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(1))));
    }

    // FM-CLUSTER-039
    #[test]
    fn test_failover_same_node_rejected() {
        let state = failover_fixture();
        let result = state.apply_command(ClusterCommand::Failover {
            old_primary_id: 2,
            new_primary_id: 2,
            force: true,
        });
        assert!(matches!(result, Err(ClusterError::InvalidOperation(_))));
    }

    // FM-CLUSTER-042
    #[test]
    fn test_failover_force_replay_is_safe() {
        let state = failover_fixture();

        for _ in 0..2 {
            // A client retry after a lost response re-issues the same command;
            // the second application must succeed and leave a coherent state.
            let result = state.apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: true,
            });
            assert!(matches!(result, Ok((ClusterResponse::Epoch(_), _))));
        }

        assert!(state.get_node(1).is_none());
        assert!(state.get_node(2).unwrap().is_primary());
        assert_eq!(state.get_slot_owner(50), Some(2));
    }

    // FM-CLUSTER-036
    #[test]
    fn test_failover_force_cancels_migrations_of_removed_node() {
        let state = failover_fixture();
        // Add another primary to migrate toward
        let node4 = NodeInfo::new_primary(4, test_addr(6382), test_addr(16382));
        state
            .apply_command(ClusterCommand::AddNode { node: node4 })
            .unwrap();
        state
            .apply_command(ClusterCommand::BeginSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 4,
            })
            .unwrap();
        assert!(state.is_slot_migrating(42));

        state
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: true,
            })
            .unwrap();

        // The migration referenced a removed node; it can never complete and
        // must not block future migrations of slot 42.
        assert!(!state.is_slot_migrating(42));
    }

    // FM-CLUSTER-036
    #[test]
    fn test_failover_graceful_keeps_unrelated_migrations() {
        let state = failover_fixture();
        let node4 = NodeInfo::new_primary(4, test_addr(6382), test_addr(16382));
        let node5 = NodeInfo::new_primary(5, test_addr(6383), test_addr(16383));
        state
            .apply_command(ClusterCommand::AddNode { node: node4 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node5 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 4,
                slots: vec![SlotRange::single(200)],
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::BeginSlotMigration {
                slot: 200,
                source_node: 4,
                target_node: 5,
            })
            .unwrap();

        state
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: false,
            })
            .unwrap();

        assert!(state.is_slot_migrating(200));
    }

    // FM-CLUSTER-040
    #[test]
    fn test_failover_absorb_between_primaries() {
        // A primary absorbing a failed primary's slots (CLUSTER FAILOVER FORCE
        // run on a primary): both nodes are primaries, force removes the target.
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 99)],
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 2,
                slots: vec![SlotRange::new(100, 199)],
            })
            .unwrap();

        state
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: true,
            })
            .unwrap();

        assert!(state.get_node(1).is_none());
        assert_eq!(state.get_slot_owner(50), Some(2));
        assert_eq!(state.get_slot_owner(150), Some(2));
        assert!(state.get_node(2).unwrap().is_primary());
    }

    // FM-CLUSTER-013
    #[test]
    fn test_mark_node_failed_bumps_epoch_atomically() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();
        let epoch_before = state.config_epoch();

        state
            .apply_command(ClusterCommand::MarkNodeFailed { node_id: 1 })
            .unwrap();

        assert!(state.get_node(1).unwrap().flags.fail);
        assert_eq!(state.config_epoch(), epoch_before + 1);
    }

    // FM-CLUSTER-013
    #[test]
    fn test_mark_node_failed_missing_node_does_not_bump_epoch() {
        let state = ClusterState::new();
        let epoch_before = state.config_epoch();
        let result = state.apply_command(ClusterCommand::MarkNodeFailed { node_id: 999 });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
        assert_eq!(state.config_epoch(), epoch_before);
    }

    // FM-CLUSTER-042
    #[test]
    fn test_failover_command_serde_roundtrip() {
        let cmd = ClusterCommand::Failover {
            old_primary_id: 7,
            new_primary_id: 9,
            force: false,
        };
        let json = serde_json::to_vec(&cmd).unwrap();
        let back: ClusterCommand = serde_json::from_slice(&json).unwrap();
        match back {
            ClusterCommand::Failover {
                old_primary_id,
                new_primary_id,
                force,
            } => {
                assert_eq!(old_primary_id, 7);
                assert_eq!(new_primary_id, 9);
                assert!(!force);
            }
            other => panic!("expected Failover, got {other:?}"),
        }
    }

    // FM-CLUSTER-042
    #[test]
    fn test_state_snapshot_roundtrip_after_failover() {
        // The Raft snapshot path serializes ClusterStateInner as JSON
        // (get_current_snapshot / install_snapshot); a post-failover state must
        // round-trip through it.
        let state = failover_fixture();
        state
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: false,
            })
            .unwrap();

        let data = serde_json::to_vec(&*state.read_inner()).unwrap();
        let restored: ClusterStateInner = serde_json::from_slice(&data).unwrap();

        let original = state.read_inner();
        assert_eq!(restored.nodes, original.nodes);
        assert_eq!(restored.slot_assignment, original.slot_assignment);
        assert_eq!(restored.config_epoch, original.config_epoch);
        assert_eq!(restored.migrations, original.migrations);
    }

    // FM-CLUSTER-044
    #[tokio::test]
    async fn test_demotion_detection_fires_for_graceful_failover_of_self() {
        let cluster = failover_fixture();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_role_change_detection(1); // self = old primary

        let entry = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 1,
            },
            payload: EntryPayload::Normal(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: false,
            }),
        };
        sm.apply(vec![entry]).await.unwrap();

        let event = expect_demotion(&mut rx);
        assert_eq!(event.demoted_node_id, 1);
        assert_eq!(event.new_primary_id, Some(2));
    }

    // FM-CLUSTER-044
    #[tokio::test]
    async fn test_promotion_detection_fires_for_failover_successor() {
        let cluster = failover_fixture();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_role_change_detection(2); // self = the successor

        let entry = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 1,
            },
            payload: EntryPayload::Normal(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: false,
            }),
        };
        sm.apply(vec![entry]).await.unwrap();

        let event = expect_promotion(&mut rx);
        assert_eq!(event.promoted_node_id, 2);
        // The failover bumps the epoch and the successor claims it, so the
        // promotion carries the epoch the node now owns, not the previous one.
        assert_eq!(event.epoch, sm.state().get_node(2).unwrap().config_epoch);
    }

    // FM-CLUSTER-044
    #[tokio::test]
    async fn test_force_failover_still_promotes_the_successor() {
        // Force failover emits no demotion (the old primary is removed, not
        // demoted) but the successor's promotion is a real transition.
        let cluster = failover_fixture();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_role_change_detection(2);

        let entry = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 1,
            },
            payload: EntryPayload::Normal(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: true,
            }),
        };
        sm.apply(vec![entry]).await.unwrap();

        assert_eq!(expect_promotion(&mut rx).promoted_node_id, 2);
    }

    // FM-CLUSTER-044
    #[tokio::test]
    async fn test_promotion_detection_fires_for_self_set_role() {
        let cluster = failover_fixture();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_role_change_detection(2); // self = a replica

        let entry = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 1,
            },
            payload: EntryPayload::Normal(ClusterCommand::SetRole {
                node_id: 2,
                role: NodeRole::Primary,
                primary_id: None,
            }),
        };
        sm.apply(vec![entry]).await.unwrap();

        assert_eq!(expect_promotion(&mut rx).promoted_node_id, 2);
    }

    /// Re-applying `SetRole { Primary }` on a node that is already a primary
    /// must stay silent. A data-path promotion mints a new replication ID and
    /// forces every attached replica into a full resync, so a replayed or
    /// duplicated log entry has to be inert.
    // FM-CLUSTER-043
    #[tokio::test]
    async fn test_promotion_detection_silent_when_already_primary() {
        let cluster = failover_fixture();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_role_change_detection(1); // self = the primary

        let entry = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 1,
            },
            payload: EntryPayload::Normal(ClusterCommand::SetRole {
                node_id: 1,
                role: NodeRole::Primary,
                primary_id: None,
            }),
        };
        sm.apply(vec![entry]).await.unwrap();

        assert!(rx.try_recv().is_err());
    }

    /// Demotions and promotions travel one channel, so a flap arrives in apply
    /// order. Two channels would let the consumer settle on the wrong terminal
    /// role.
    // FM-CLUSTER-044
    #[tokio::test]
    async fn test_role_changes_preserve_apply_order() {
        let cluster = failover_fixture();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_role_change_detection(1); // self = the primary

        let entries: Vec<_> = [
            ClusterCommand::SetRole {
                node_id: 1,
                role: NodeRole::Replica,
                primary_id: Some(2),
            },
            ClusterCommand::SetRole {
                node_id: 1,
                role: NodeRole::Primary,
                primary_id: None,
            },
        ]
        .into_iter()
        .enumerate()
        .map(|(i, cmd)| openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: i as u64 + 1,
            },
            payload: EntryPayload::Normal(cmd),
        })
        .collect();
        sm.apply(entries).await.unwrap();

        assert_eq!(expect_demotion(&mut rx).demoted_node_id, 1);
        assert_eq!(expect_promotion(&mut rx).promoted_node_id, 1);
        assert!(rx.try_recv().is_err());
    }

    /// Serialize `state` as a snapshot body plus matching metadata.
    fn snapshot_payload(
        state: &ClusterState,
    ) -> (SnapshotMeta<NodeId, openraft::BasicNode>, Vec<u8>) {
        let data = serde_json::to_vec(&*state.read_inner()).unwrap();
        let meta = SnapshotMeta {
            last_log_id: Some(openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 7,
            }),
            last_membership: Default::default(),
            snapshot_id: "test-snapshot".to_string(),
        };
        (meta, data)
    }

    /// A snapshot install skips the entries that produced it, so the role
    /// changes folded into it emit no `ClusterEvent`. The install has to
    /// reconcile this node's role itself, or a node that fell far enough behind
    /// to need a snapshot keeps serving in the role it had before it fell
    /// behind.
    // FM-CLUSTER-045
    #[tokio::test]
    async fn test_install_snapshot_emits_promotion_when_self_role_flipped() {
        let mut sm = ClusterStateMachine::with_state(failover_fixture());
        let mut rx = sm.enable_role_change_detection(2); // self = a replica

        // Build the post-failover topology in a separate state, then ship it.
        let promoted = failover_fixture();
        promoted
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: false,
            })
            .unwrap();
        let (meta, data) = snapshot_payload(&promoted);

        sm.install_snapshot(&meta, Box::new(Cursor::new(data)))
            .await
            .unwrap();

        assert_eq!(expect_promotion(&mut rx).promoted_node_id, 2);
    }

    // FM-CLUSTER-045
    #[tokio::test]
    async fn test_install_snapshot_emits_demotion_when_self_role_flipped() {
        let mut sm = ClusterStateMachine::with_state(failover_fixture());
        let mut rx = sm.enable_role_change_detection(1); // self = the primary

        let demoted = failover_fixture();
        demoted
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: false,
            })
            .unwrap();
        let (meta, data) = snapshot_payload(&demoted);

        sm.install_snapshot(&meta, Box::new(Cursor::new(data)))
            .await
            .unwrap();

        let event = expect_demotion(&mut rx);
        assert_eq!(event.demoted_node_id, 1);
        assert_eq!(event.new_primary_id, Some(2));
    }

    /// An install that does not change this node's role must stay silent, so
    /// routine snapshot catch-up never churns the data path.
    // FM-CLUSTER-045
    #[tokio::test]
    async fn test_install_snapshot_silent_when_self_role_unchanged() {
        let mut sm = ClusterStateMachine::with_state(failover_fixture());
        let mut rx = sm.enable_role_change_detection(3); // self = an untouched replica

        let after = failover_fixture();
        after
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: false,
            })
            .unwrap();
        let (meta, data) = snapshot_payload(&after);

        sm.install_snapshot(&meta, Box::new(Cursor::new(data)))
            .await
            .unwrap();

        assert!(rx.try_recv().is_err());
    }

    /// Boot restore is the third way a role reaches this node without an entry
    /// to replay: the state came off disk, not out of the log. A node that was
    /// a replica when the snapshot was cut boots with a data path that still
    /// believes it is a primary, so the reconciliation has to demote it.
    // FM-CLUSTER-046
    #[test]
    fn test_reconcile_self_role_demotes_a_restored_replica() {
        let mut sm = ClusterStateMachine::with_state(failover_fixture());
        let mut rx = sm.enable_role_change_detection(2); // self = a replica of 1

        // The data path booted as a primary (no `replicaof` in config).
        assert_eq!(sm.reconcile_self_role(false), Some(NodeRole::Replica));

        let event = expect_demotion(&mut rx);
        assert_eq!(event.demoted_node_id, 2);
        assert_eq!(event.new_primary_id, Some(1));
    }

    // FM-CLUSTER-046
    #[test]
    fn test_reconcile_self_role_promotes_a_restored_primary() {
        let mut sm = ClusterStateMachine::with_state(failover_fixture());
        let mut rx = sm.enable_role_change_detection(1); // self = the primary

        // The data path booted as a replica (a stale `replicaof` in config).
        assert_eq!(sm.reconcile_self_role(true), Some(NodeRole::Primary));

        assert_eq!(expect_promotion(&mut rx).promoted_node_id, 1);
    }

    /// The common boot: both views already agree. Emitting anyway would mint a
    /// fresh replication identity on every restart of a healthy primary and
    /// force its replicas into a full resync.
    // FM-CLUSTER-046
    #[test]
    fn test_reconcile_self_role_silent_when_views_agree() {
        let mut sm = ClusterStateMachine::with_state(failover_fixture());
        let mut rx = sm.enable_role_change_detection(1);
        assert_eq!(sm.reconcile_self_role(false), None);
        assert!(rx.try_recv().is_err());

        let mut sm = ClusterStateMachine::with_state(failover_fixture());
        let mut rx = sm.enable_role_change_detection(2);
        assert_eq!(sm.reconcile_self_role(true), None);
        assert!(rx.try_recv().is_err());
    }

    /// The reconciler is the *repeat* half of role convergence: while the data
    /// path is still stuck on the wrong role — a promotion whose identity
    /// persist keeps failing, a demotion that never found its new primary — it
    /// must keep emitting, and it must stop the moment the data path catches
    /// up. A one-shot boot-time reconcile would leave a failed role change
    /// stranded until the process restarted.
    // FM-CLUSTER-046
    #[test]
    fn test_reconciler_re_emits_while_the_views_disagree() {
        let mut sm = ClusterStateMachine::with_state(failover_fixture());
        let mut rx = sm.enable_role_change_detection(1);
        let reconciler = sm
            .self_role_reconciler()
            .expect("detection is enabled, so the handle exists");

        // Data path still reports replica across three ticks: three events.
        for _ in 0..3 {
            assert_eq!(
                reconciler.reconcile(true),
                RoleReconcile::ReDriven(NodeRole::Primary)
            );
            assert_eq!(expect_promotion(&mut rx).promoted_node_id, 1);
        }

        // The promotion finally lands and the re-drive stops.
        assert_eq!(reconciler.reconcile(false), RoleReconcile::Agreed);
        assert!(rx.try_recv().is_err());
    }

    /// A reconciler outlives the node it reconciles: it is held by a task, and
    /// the shutdown that drops the role-change consumer is what ends that task.
    /// It must therefore never *keep* the consumer alive — the consumer owns
    /// the data path's role controller and, through it, the storage engine, so
    /// a strong sender here left a shut-down node holding its RocksDB lock and
    /// broke restart-in-process. Dropping the receiver stands in for that
    /// shutdown: the next tick reports `Detached` instead of emitting.
    // FM-CLUSTER-046
    #[test]
    fn test_reconciler_detaches_once_the_consumer_is_gone() {
        let mut sm = ClusterStateMachine::with_state(failover_fixture());
        let mut rx = sm.enable_role_change_detection(1);
        let reconciler = sm
            .self_role_reconciler()
            .expect("detection is enabled, so the handle exists");
        assert_eq!(
            reconciler.reconcile(true),
            RoleReconcile::ReDriven(NodeRole::Primary),
            "a live data path still gets re-drives"
        );
        assert_eq!(expect_promotion(&mut rx).promoted_node_id, 1);

        // Shutdown: the state machine goes down with the node, leaving only the
        // task-held reconciler.
        drop(sm);
        assert_eq!(
            reconciler.reconcile(true),
            RoleReconcile::Detached,
            "with the node gone the reconciler must report detachment, not emit"
        );
        assert!(
            matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Disconnected)),
            "the consumer's channel must close so its loop ends and it drops the role controller"
        );
    }

    /// The handle exists only once role-change detection is enabled — without
    /// this node's id and the channel there is nothing to reconcile against.
    // FM-CLUSTER-046
    #[test]
    fn test_self_role_reconciler_absent_until_detection_is_enabled() {
        let mut sm = ClusterStateMachine::with_state(failover_fixture());
        assert!(sm.self_role_reconciler().is_none());
        let _rx = sm.enable_role_change_detection(1);
        assert!(sm.self_role_reconciler().is_some());
    }

    /// A node not (yet) in the restored state has no cluster-state role to
    /// reconcile against — first boot, before self-registration commits.
    // FM-CLUSTER-046
    #[test]
    fn test_reconcile_self_role_noop_when_self_absent() {
        let mut sm = ClusterStateMachine::with_state(failover_fixture());
        let mut rx = sm.enable_role_change_detection(99);
        assert_eq!(sm.reconcile_self_role(false), None);
        assert!(rx.try_recv().is_err());
    }

    // FM-CLUSTER-044
    #[tokio::test]
    async fn test_demotion_detection_not_fired_for_failed_failover() {
        let cluster = failover_fixture();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_role_change_detection(1);

        // Invalid target — apply fails, so no demotion event.
        let entry = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 1,
            },
            payload: EntryPayload::Normal(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 999,
                force: false,
            }),
        };
        sm.apply(vec![entry]).await.unwrap();

        assert!(rx.try_recv().is_err());
    }

    // FM-CLUSTER-044
    #[tokio::test]
    async fn test_demotion_detection_not_fired_for_force_failover() {
        // Force failover removes the old primary; that is not a demotion.
        let cluster = failover_fixture();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_role_change_detection(1);

        let entry = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 1,
            },
            payload: EntryPayload::Normal(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: true,
            }),
        };
        sm.apply(vec![entry]).await.unwrap();

        assert!(rx.try_recv().is_err());
    }

    // FM-CLUSTER-033
    #[test]
    fn test_complete_migration_params_mismatch() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        let node3 = NodeInfo::new_primary(3, test_addr(6381), test_addr(16381));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node2 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AddNode { node: node3 })
            .unwrap();
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::single(42)],
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::BeginSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();

        // Complete with wrong target (node 3 instead of node 2)
        let result = state.apply_command(ClusterCommand::CompleteSlotMigration {
            slot: 42,
            source_node: 1,
            target_node: 3,
            proposed_at_ms: 1_000_000,
        });
        assert!(matches!(result, Err(ClusterError::InvalidOperation(_))));
    }

    // ========================================================================
    // apply_command event-derivation tests (synchronous, no Raft Entry types)
    //
    // These pin the variant→event mapping directly on apply_command — the
    // module that owns it — without constructing openraft Entry/LogId or an
    // async state machine. The node-agnostic self-filter is exercised by the
    // Entry-based apply tests above.
    // ========================================================================

    /// Build two primaries (1, 2) so SetRole/Failover have valid topology.
    fn two_primaries() -> ClusterState {
        let state = ClusterState::new();
        for id in [1u64, 2] {
            let node = NodeInfo::new_primary(
                id,
                test_addr(6378 + id as u16),
                test_addr(16378 + id as u16),
            );
            state
                .apply_command(ClusterCommand::AddNode { node })
                .unwrap();
        }
        state
    }

    // FM-CLUSTER-043
    #[test]
    fn set_role_replica_emits_node_demoted() {
        let state = two_primaries();
        let (resp, events) = state
            .apply_command(ClusterCommand::SetRole {
                node_id: 2,
                role: NodeRole::Replica,
                primary_id: Some(1),
            })
            .unwrap();
        assert!(matches!(resp, ClusterResponse::Ok));
        assert_eq!(
            events,
            vec![ClusterEvent::NodeDemoted {
                demoted_node_id: 2,
                new_primary_id: Some(1),
                epoch: state.config_epoch(),
            }]
        );
    }

    // FM-CLUSTER-043
    #[test]
    fn set_role_primary_emits_no_event() {
        // Promoting to (or reasserting) Primary is not a demotion.
        let state = two_primaries();
        let (_, events) = state
            .apply_command(ClusterCommand::SetRole {
                node_id: 2,
                role: NodeRole::Primary,
                primary_id: None,
            })
            .unwrap();
        assert!(events.is_empty());
    }

    // FM-CLUSTER-043
    #[test]
    fn set_role_self_demotion_emits_no_event_on_error() {
        // The previously-missing coverage: a rejected SetRole self-demotion
        // (target node absent) returns Err and therefore carries no events at
        // all — emit-on-failure is structurally impossible.
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state
            .apply_command(ClusterCommand::AddNode { node: node1 })
            .unwrap();

        // Node 2 was never added, so this is rejected with NodeNotFound.
        let result = state.apply_command(ClusterCommand::SetRole {
            node_id: 2,
            role: NodeRole::Replica,
            primary_id: Some(1),
        });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(2))));
    }

    // FM-CLUSTER-044
    #[test]
    fn graceful_failover_emits_node_demoted_for_old_primary() {
        let state = failover_fixture();
        let (resp, events) = state
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: false,
            })
            .unwrap();
        assert!(matches!(resp, ClusterResponse::Epoch(_)));
        // Demotion of the old primary first, then promotion of the successor:
        // one failover is two role changes, and the order is the one a node
        // that watches both would have to replay.
        assert_eq!(
            events,
            vec![
                ClusterEvent::NodeDemoted {
                    demoted_node_id: 1,
                    new_primary_id: Some(2),
                    epoch: state.config_epoch(),
                },
                ClusterEvent::NodePromoted {
                    promoted_node_id: 2,
                    epoch: state.config_epoch(),
                }
            ]
        );
    }

    // FM-CLUSTER-015
    #[test]
    fn increment_epoch_returns_typed_epoch() {
        // IncrementEpoch returns the post-increment config epoch as a typed
        // ClusterResponse::Epoch, not a stringly-encoded Value (proposal 32).
        let state = ClusterState::new();
        assert_eq!(state.config_epoch(), 0);
        let (resp, events) = state.apply_command(ClusterCommand::IncrementEpoch).unwrap();
        assert!(matches!(resp, ClusterResponse::Epoch(1)));
        assert!(events.is_empty());
        assert_eq!(state.config_epoch(), 1);
    }

    // FM-CLUSTER-044
    #[test]
    fn force_failover_emits_promotion_only() {
        // Force failover removes the old primary; that is not a demotion. The
        // successor's replica -> primary transition is still real.
        let state = failover_fixture();
        let (_, events) = state
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: true,
            })
            .unwrap();
        assert_eq!(
            events,
            vec![ClusterEvent::NodePromoted {
                promoted_node_id: 2,
                epoch: state.config_epoch(),
            }]
        );
    }

    // FM-CLUSTER-034
    #[test]
    fn complete_migration_emits_event_on_success() {
        let state = ClusterState::new();
        for id in [1u64, 2] {
            let node = NodeInfo::new_primary(
                id,
                test_addr(6378 + id as u16),
                test_addr(16378 + id as u16),
            );
            state
                .apply_command(ClusterCommand::AddNode { node })
                .unwrap();
        }
        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::single(42)],
            })
            .unwrap();
        state
            .apply_command(ClusterCommand::BeginSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();

        let proposed_at_ms = state.arm_handoff_for_test(42, 1, 2);
        let (resp, events) = state
            .apply_command(ClusterCommand::CompleteSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
                proposed_at_ms,
            })
            .unwrap();
        assert!(matches!(resp, ClusterResponse::Ok));
        assert_eq!(
            events,
            vec![
                ClusterEvent::SlotMigrationCompleted {
                    slot: 42,
                    source_node: 1,
                    target_node: 2,
                },
                ClusterEvent::SlotHandoffReleased {
                    slot: 42,
                    source_node: 1,
                    seq: 1,
                },
            ]
        );
    }

    // FM-CLUSTER-034
    #[test]
    fn complete_migration_emits_no_event_on_error() {
        // No migration in progress -> Err -> no events.
        let state = two_primaries();
        let result = state.apply_command(ClusterCommand::CompleteSlotMigration {
            slot: 42,
            source_node: 1,
            target_node: 2,
            proposed_at_ms: 1_000_000,
        });
        assert!(matches!(result, Err(ClusterError::InvalidOperation(_))));
    }

    // FM-CLUSTER-015
    #[test]
    fn non_event_command_returns_no_events() {
        // A plain successful mutation with no associated event.
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let (resp, events) = state
            .apply_command(ClusterCommand::AddNode { node })
            .unwrap();
        assert!(matches!(resp, ClusterResponse::Ok));
        assert!(events.is_empty());
    }

    // ---- Readers over the replicated state ---------------------------------

    /// The three whole-table readers report the table, not a constant: a caller
    /// that lists nodes, asks for a node's slot ranges, or checks slot coverage
    /// must see what was applied.
    // FM-CLUSTER-078
    #[test]
    fn state_readers_report_the_applied_table() {
        let state = ClusterState::new();
        assert!(state.get_all_nodes().is_empty());
        assert!(state.get_node_slots(1).is_empty());
        assert!(
            !state.all_slots_assigned(),
            "an empty slot table is not full coverage"
        );

        for id in [1u64, 2] {
            state
                .apply_command(ClusterCommand::AddNode {
                    node: NodeInfo::new_primary(
                        id,
                        test_addr(6378 + id as u16),
                        test_addr(16378 + id as u16),
                    ),
                })
                .unwrap();
        }
        let ids: Vec<NodeId> = state.get_all_nodes().iter().map(|n| n.id).collect();
        assert_eq!(ids, vec![1, 2]);

        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 9), SlotRange::new(20, 24)],
            })
            .unwrap();
        assert_eq!(
            state.get_node_slots(1),
            vec![SlotRange::new(0, 9), SlotRange::new(20, 24)],
            "get_node_slots compacts the owned slots into ranges"
        );
        assert!(state.get_node_slots(2).is_empty());
        assert!(
            !state.all_slots_assigned(),
            "15 of 16384 slots is not full coverage"
        );

        state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 2,
                slots: vec![
                    SlotRange::new(10, 19),
                    SlotRange::new(25, CLUSTER_SLOTS - 1),
                ],
            })
            .unwrap();
        assert!(
            state.all_slots_assigned(),
            "every slot is now owned by someone"
        );
    }

    /// `self_node_id` is local, not replicated, and `0` is its "unset"
    /// sentinel — never a node id in its own right.
    // FM-CLUSTER-006
    #[test]
    fn self_node_id_treats_zero_as_unset() {
        let state = ClusterState::new();
        assert_eq!(
            state.self_node_id(),
            None,
            "a node that has not been given an id reports none"
        );

        state.set_self_node_id(7);
        assert_eq!(state.self_node_id(), Some(7));

        state.set_self_node_id(1);
        assert_eq!(state.self_node_id(), Some(1));

        state.set_self_node_id(0);
        assert_eq!(
            state.self_node_id(),
            None,
            "0 is the unset sentinel, not node 0"
        );
    }

    /// `from_snapshot` rebuilds the replicated half from the DTO and *keeps* the
    /// caller's `self_node_id` cell — the identity is local state a restore must
    /// not clear, and the cell is shared so a later `set_self_node_id` is
    /// visible through both handles.
    // FM-CLUSTER-006
    #[test]
    fn from_snapshot_restores_the_table_and_keeps_the_local_identity() {
        let original = ClusterState::new();
        original.set_self_node_id(4);
        original
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(4, test_addr(6379), test_addr(16379)),
            })
            .unwrap();
        original
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 4,
                slots: vec![SlotRange::new(0, 10)],
            })
            .unwrap();
        original
            .apply_command(ClusterCommand::IncrementEpoch)
            .unwrap();

        let snapshot = (*original.snapshot()).clone();
        let restored = ClusterState::from_snapshot(snapshot, original.self_node_id_atomic());

        assert_eq!(restored.self_node_id(), Some(4), "identity survives");
        assert_eq!(restored.get_all_nodes().len(), 1);
        assert_eq!(restored.get_slot_owner(5), Some(4));
        assert_eq!(restored.config_epoch(), original.config_epoch());
        assert!(restored.config_epoch() > 0);
        assert_eq!(
            restored.snapshot().get_slot_owner(5),
            Some(4),
            "the reader snapshot is published from the restored table"
        );

        // The identity cell is shared, not copied.
        restored.set_self_node_id(9);
        assert_eq!(original.self_node_id(), Some(9));
    }

    // ---- handoff generation across a restore (FM-CLUSTER-100) --------------

    /// A cluster whose handoff generation has already advanced past a
    /// *completed* handoff: slot 5 moved from node 1 to node 2, and the
    /// migration record that carried `seq = 1` is gone with it. Nothing left in
    /// the state names the generation, which is what makes re-deriving it from
    /// the live migrations impossible.
    fn state_past_a_completed_handoff() -> ClusterState {
        let state = ClusterState::new();
        for id in [1u64, 2] {
            let port = 6379 + id as u16;
            state
                .apply_local(ClusterCommand::AddNode {
                    node: NodeInfo::new_primary(id, test_addr(port), test_addr(port + 10_000)),
                })
                .expect("seeding a primary must succeed");
        }
        state
            .apply_local(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 10)],
            })
            .expect("seeding slots must succeed");
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
            })
            .expect("begin must succeed");
        let prepared_at_ms = state.arm_handoff_for_test(5, 1, 2);
        state
            .apply_local(ClusterCommand::CompleteSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
                proposed_at_ms: prepared_at_ms,
            })
            .expect("complete must succeed");
        assert!(
            state.get_slot_migration(5).is_none(),
            "the record that carried seq 1 is gone"
        );
        state
    }

    /// Open a fresh migration of slot 5 back the other way and prepare it,
    /// returning the `seq` the state minted. This is the only client-visible
    /// reading of the generation counter.
    fn mint_next_handoff_seq(state: &ClusterState) -> u64 {
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 2,
                target_node: 1,
            })
            .expect("begin must succeed");
        let (_, events) = state
            .apply_command(ClusterCommand::PrepareSlotHandoff {
                slot: 5,
                source_node: 2,
                target_node: 1,
                barrier_ms: crate::types::HANDOFF_BARRIER_MS,
                lease_ms: crate::types::HANDOFF_LEASE_MS,
                proposed_at_ms: 2_000_000,
            })
            .expect("prepare must succeed");
        match events.as_slice() {
            [ClusterEvent::SlotHandoffPrepared { seq, .. }] => *seq,
            other => panic!("expected exactly one prepared event, got {other:?}"),
        }
    }

    /// The openraft vehicle: the serialized `ClusterStateInner` a leader ships
    /// and `install_snapshot` deserializes. The generation is replicated state,
    /// so it travels in the body like every other replicated field.
    // FM-CLUSTER-100
    #[test]
    fn an_installed_snapshot_carries_the_handoff_generation() {
        let state = state_past_a_completed_handoff();
        let (meta, data) = state.encode_snapshot().expect("encoding must succeed");
        let shipped: ClusterStateInner =
            serde_json::from_slice(&data).expect("decoding must succeed");

        let restored = ClusterState::new();
        restored.restore_from_snapshot(shipped, &meta);

        assert_eq!(
            mint_next_handoff_seq(&restored),
            2,
            "a restored node that re-mints seq 1 hands out a generation \
             some node has already fenced against"
        );
    }

    /// The DTO vehicle: `ClusterSnapshot` -> `from_snapshot`. It must carry the
    /// generation too — a completed handoff leaves no migration to re-derive it
    /// from, so anything the DTO drops is reused, not recovered.
    // FM-CLUSTER-100
    #[test]
    fn from_snapshot_carries_the_handoff_generation() {
        let state = state_past_a_completed_handoff();
        let dto = (*state.snapshot()).clone();
        let restored = ClusterState::from_snapshot(dto, state.self_node_id_atomic());

        assert_eq!(
            mint_next_handoff_seq(&restored),
            2,
            "the reader DTO is also a restore vehicle: dropping the counter \
             re-mints a seq that is already spent"
        );
    }

    /// `ResetCluster` is the one thing allowed to rewind the generation, and it
    /// clears every migration in the same entry — nothing can hold a fence
    /// stamped against a slot this node still owns.
    // FM-CLUSTER-100
    #[test]
    fn a_reset_is_the_one_rewind_and_it_survives_a_restore() {
        let state = state_past_a_completed_handoff();
        state
            .apply_local(ClusterCommand::ResetCluster {
                node_id: 1,
                new_node_id: None,
            })
            .expect("reset must succeed");
        assert_eq!(
            state.read_inner().handoff_seq,
            0,
            "reset rewinds the generation deliberately"
        );

        let (meta, data) = state.encode_snapshot().expect("encoding must succeed");
        let shipped: ClusterStateInner =
            serde_json::from_slice(&data).expect("decoding must succeed");
        let restored = ClusterState::new();
        restored.restore_from_snapshot(shipped, &meta);
        assert_eq!(
            restored.read_inner().handoff_seq,
            0,
            "and the rewind is what the snapshot carries, not a floor of its own"
        );

        let dto = (*state.snapshot()).clone();
        let via_dto = ClusterState::from_snapshot(dto, state.self_node_id_atomic());
        assert_eq!(via_dto.read_inner().handoff_seq, 0);
    }

    /// The epoch minter is bounded below by the highest epoch any node claims,
    /// which is what keeps the cluster counter dominating every per-node value
    /// even when a node joins claiming an epoch above the counter.
    // FM-CLUSTER-010
    #[test]
    fn max_node_epoch_tracks_the_highest_claim() {
        let state = ClusterState::new();
        assert_eq!(
            state.read_inner().max_node_epoch(),
            0,
            "no nodes, no claims"
        );

        let mut high = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        high.config_epoch = 9;
        state
            .apply_command(ClusterCommand::AddNode { node: high })
            .unwrap();
        let mut low = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        low.config_epoch = 3;
        state
            .apply_command(ClusterCommand::AddNode { node: low })
            .unwrap();

        assert_eq!(state.read_inner().max_node_epoch(), 9);
        assert_eq!(
            state.write_inner().mint_config_epoch(),
            10,
            "the next minted epoch clears every claim"
        );
    }

    // ---- openraft state-machine surface ------------------------------------

    /// A membership entry is openraft bookkeeping, but it still has to land:
    /// `applied_state` is what openraft reads back at startup to decide who the
    /// voters are.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn membership_entries_are_recorded_for_applied_state() {
        let mut sm = ClusterStateMachine::new();
        let (before_applied, before_membership) = sm.applied_state().await.unwrap();
        assert!(before_applied.is_none());
        assert!(before_membership.voter_ids().next().is_none());

        let log_id = LogId::new(openraft::CommittedLeaderId::new(1, 1), 7);
        let membership = openraft::Membership::new(
            vec![std::collections::BTreeSet::from([1u64, 2])],
            std::collections::BTreeMap::from([
                (1u64, openraft::BasicNode { addr: "a".into() }),
                (2u64, openraft::BasicNode { addr: "b".into() }),
            ]),
        );
        sm.apply(vec![openraft::Entry {
            log_id,
            payload: EntryPayload::Membership(membership),
        }])
        .await
        .unwrap();

        let (applied, stored) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id));
        assert_eq!(
            stored.voter_ids().collect::<Vec<_>>(),
            vec![1, 2],
            "the committed membership must be readable after a restart"
        );
        assert_eq!(stored.log_id(), &Some(log_id));
    }

    /// The snapshot builder is a handle onto the *same* state, not a fresh empty
    /// one — a builder over a blank state would hand openraft a snapshot that
    /// silently erases the topology it purges log entries for.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn snapshot_builder_shares_the_live_state() {
        use openraft::storage::RaftSnapshotBuilder;

        let mut sm = ClusterStateMachine::new();
        sm.state()
            .apply_command(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(5, test_addr(6379), test_addr(16379)),
            })
            .unwrap();
        sm.state()
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 5,
                slots: vec![SlotRange::new(0, 3)],
            })
            .unwrap();

        let mut builder = sm.get_snapshot_builder().await;
        assert_eq!(
            builder.state().get_all_nodes().len(),
            1,
            "the builder reads the state machine's own state"
        );

        let snapshot = builder.build_snapshot().await.unwrap();
        let encoded: ClusterStateInner =
            serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
        assert!(encoded.nodes.contains_key(&5));
        assert_eq!(encoded.slot_assignment.get(&3), Some(&5));
    }

    /// The receive buffer openraft streams a snapshot into starts empty and at
    /// position zero. A buffer that already held bytes would leave whatever it
    /// held in front of a snapshot shorter than itself.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn begin_receiving_snapshot_hands_back_an_empty_buffer() {
        let mut sm = ClusterStateMachine::new();
        let cursor = sm.begin_receiving_snapshot().await.unwrap();
        assert!(
            cursor.get_ref().is_empty(),
            "a receive buffer that starts non-empty corrupts the received snapshot"
        );
        assert_eq!(cursor.position(), 0);
    }
}
