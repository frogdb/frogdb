//! Core types for cluster coordination.

use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use thiserror::Error;

/// Number of slots in a Redis Cluster (16384).
pub const CLUSTER_SLOTS: u16 = 16384;

/// Type alias for node identifiers.
/// Uses u64 for compatibility with openraft.
pub type NodeId = u64;

/// Configuration epoch for cluster state versioning.
/// Higher epochs take precedence in conflict resolution.
pub type ConfigEpoch = u64;

/// openraft type configuration for FrogDB cluster.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct TypeConfig;

impl openraft::RaftTypeConfig for TypeConfig {
    type D = ClusterCommand;
    type R = ClusterResponse;
    type Node = BasicNode;
    type NodeId = NodeId;
    type Entry = openraft::Entry<TypeConfig>;
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type Responder = openraft::impls::OneshotResponder<TypeConfig>;
    type AsyncRuntime = openraft::TokioRuntime;
}

/// Role of a node in the cluster.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Primary node - handles reads and writes for its slots.
    #[default]
    Primary,
    /// Replica node - replicates data from a primary.
    Replica,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::Primary => write!(f, "master"),
            NodeRole::Replica => write!(f, "slave"),
        }
    }
}

/// Information about a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeInfo {
    /// Unique node identifier.
    pub id: NodeId,
    /// Node address for client connections.
    pub addr: SocketAddr,
    /// Node address for cluster bus (Raft) communication.
    pub cluster_addr: SocketAddr,
    /// Node role (primary or replica).
    pub role: NodeRole,
    /// If this is a replica, the ID of its primary.
    pub primary_id: Option<NodeId>,
    /// Configuration epoch when this node was last updated.
    pub config_epoch: ConfigEpoch,
    /// Flags describing node state.
    pub flags: NodeFlags,
    /// Priority for replica promotion during auto-failover.
    /// Lower values are preferred. 0 means never promote.
    #[serde(default = "default_replica_priority")]
    pub replica_priority: u32,
    /// Binary version of the frogdb-server running on this node (semver).
    /// Empty string for nodes from before version tracking was added.
    #[serde(default)]
    pub version: String,
}

fn default_replica_priority() -> u32 {
    100
}

impl NodeInfo {
    /// Create a new primary node.
    pub fn new_primary(id: NodeId, addr: SocketAddr, cluster_addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            cluster_addr,
            role: NodeRole::Primary,
            primary_id: None,
            config_epoch: 0,
            flags: NodeFlags::default(),
            replica_priority: 100,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Create a new replica node.
    pub fn new_replica(
        id: NodeId,
        addr: SocketAddr,
        cluster_addr: SocketAddr,
        primary_id: NodeId,
    ) -> Self {
        Self {
            id,
            addr,
            cluster_addr,
            role: NodeRole::Replica,
            primary_id: Some(primary_id),
            config_epoch: 0,
            flags: NodeFlags::default(),
            replica_priority: 100,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Check if this node is a primary.
    pub fn is_primary(&self) -> bool {
        self.role == NodeRole::Primary
    }

    /// Check if this node is a replica.
    pub fn is_replica(&self) -> bool {
        self.role == NodeRole::Replica
    }
}

/// Flags describing node state.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeFlags {
    /// Node is in handshake state (not fully joined).
    pub handshake: bool,
    /// Node has failed (not responding).
    pub fail: bool,
    /// Node is suspected to have failed.
    pub pfail: bool,
    /// Node has no address yet.
    pub noaddr: bool,
}

impl NodeFlags {
    /// Create flags for a node that's fully connected.
    ///
    /// "Connected" *is* the zero value — no handshake pending, no failure
    /// suspicion, an address on file — so a mutation replacing this body with
    /// `Default::default()` is the identical program (documented equivalent
    /// mutant). The constructor exists to name the state at the call site, not
    /// to compute a different one; the distinguishable sibling is
    /// [`NodeFlags::handshaking`], which does set a field.
    pub fn connected() -> Self {
        Self::default()
    }

    /// Create flags for a node in handshake state.
    pub fn handshaking() -> Self {
        Self {
            handshake: true,
            ..Default::default()
        }
    }
}

/// A range of hash slots.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SlotRange {
    /// Start slot (inclusive).
    pub start: u16,
    /// End slot (inclusive).
    pub end: u16,
}

impl SlotRange {
    /// Create a new slot range.
    pub fn new(start: u16, end: u16) -> Self {
        debug_assert!(start <= end);
        debug_assert!(end < CLUSTER_SLOTS);
        Self { start, end }
    }

    /// Create a single-slot range.
    pub fn single(slot: u16) -> Self {
        Self::new(slot, slot)
    }

    /// Check if this range contains a slot.
    pub fn contains(&self, slot: u16) -> bool {
        slot >= self.start && slot <= self.end
    }

    /// Get the number of slots in this range.
    pub fn len(&self) -> usize {
        (self.end - self.start + 1) as usize
    }

    /// Check if this range is empty.
    pub fn is_empty(&self) -> bool {
        false // A range always has at least one slot
    }

    /// Iterate over all slots in this range.
    pub fn iter(&self) -> impl Iterator<Item = u16> {
        self.start..=self.end
    }
}

impl std::fmt::Display for SlotRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.start == self.end {
            write!(f, "{}", self.start)
        } else {
            write!(f, "{}-{}", self.start, self.end)
        }
    }
}

impl std::str::FromStr for SlotRange {
    type Err = ClusterError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((start, end)) = s.split_once('-') {
            let start: u16 = start
                .parse()
                .map_err(|_| ClusterError::InvalidSlot(s.to_string()))?;
            let end: u16 = end
                .parse()
                .map_err(|_| ClusterError::InvalidSlot(s.to_string()))?;
            if start > end || end >= CLUSTER_SLOTS {
                return Err(ClusterError::InvalidSlot(s.to_string()));
            }
            Ok(SlotRange::new(start, end))
        } else {
            let slot: u16 = s
                .parse()
                .map_err(|_| ClusterError::InvalidSlot(s.to_string()))?;
            if slot >= CLUSTER_SLOTS {
                return Err(ClusterError::InvalidSlot(s.to_string()));
            }
            Ok(SlotRange::single(slot))
        }
    }
}

/// Partition all [`CLUSTER_SLOTS`] hash slots as evenly as possible across
/// `node_ids`, in the given order, assigning any remainder to the last node.
///
/// Returns one `(NodeId, SlotRange)` per node, together covering `0..CLUSTER_SLOTS`
/// exactly once with disjoint, contiguous, inclusive ranges. The order of
/// `node_ids` is significant and preserved: the node at index 0 always owns the
/// range starting at slot 0, so callers must pass a deterministically ordered
/// slice (bootstrap uses `BTreeMap` node-id order).
///
/// Returns an empty `Vec` for empty input. `node_ids.len()` must be
/// `<= CLUSTER_SLOTS` (a cluster cannot have more primaries than slots); callers
/// already guarantee a small member count far below the slot count, so this is a
/// `debug_assert`, not a runtime error.
///
/// This is the single owner of the bootstrap even-split arithmetic: both the
/// local-seed path and the Raft-replication path in the server's cluster
/// bootstrap consume it, so their slot ownership is identical by construction
/// rather than by hand-synced comment.
pub fn even_slot_ranges(node_ids: &[NodeId]) -> Vec<(NodeId, SlotRange)> {
    let num_nodes = node_ids.len();
    if num_nodes == 0 {
        return Vec::new();
    }
    debug_assert!(
        num_nodes <= CLUSTER_SLOTS as usize,
        "cannot partition {} slots across {} nodes (more nodes than slots)",
        CLUSTER_SLOTS,
        num_nodes
    );

    let slots_per_node = CLUSTER_SLOTS as usize / num_nodes;
    node_ids
        .iter()
        .enumerate()
        .map(|(i, &nid)| {
            let start = (i * slots_per_node) as u16;
            let end = if i == num_nodes - 1 {
                CLUSTER_SLOTS - 1 // Last node gets the remainder.
            } else {
                ((i + 1) * slots_per_node - 1) as u16
            };
            (nid, SlotRange::new(start, end))
        })
        .collect()
}

/// Commands that modify cluster state via Raft consensus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterCommand {
    /// Add a node to the cluster.
    AddNode { node: NodeInfo },

    /// Remove a node from the cluster.
    RemoveNode { node_id: NodeId },

    /// Assign slots to a node.
    AssignSlots {
        node_id: NodeId,
        slots: Vec<SlotRange>,
    },

    /// Remove slots from a node.
    RemoveSlots {
        node_id: NodeId,
        slots: Vec<SlotRange>,
    },

    /// Set a node's role.
    SetRole {
        node_id: NodeId,
        role: NodeRole,
        primary_id: Option<NodeId>,
    },

    /// Increment the cluster configuration epoch.
    IncrementEpoch,

    /// Set one node's `config_epoch` to exactly `epoch` (`CLUSTER
    /// SET-CONFIG-EPOCH`). Redis-compatible bootstrap-only command: accepted
    /// only while the node knows no other member and its own epoch is still 0.
    SetConfigEpoch { node_id: NodeId, epoch: ConfigEpoch },

    /// Atomically fail over a primary to a successor in one replicated
    /// state-machine transition: transfer every slot owned by
    /// `old_primary_id` to `new_primary_id`, promote the successor to
    /// primary, apply the old primary's fate (`force`: remove it from the
    /// cluster; graceful: demote it to a replica of the successor),
    /// re-parent the old primary's remaining replicas, and bump the config
    /// epoch — all-or-nothing.
    ///
    /// This is the composite of `RemoveNode`/`SetRole` + `AssignSlots` +
    /// `IncrementEpoch` that callers previously issued as separate Raft
    /// entries; issuing them separately leaves the cluster with ownerless
    /// slots if the leader crashes between entries.
    Failover {
        /// The primary losing its slots (failed, or being demoted).
        old_primary_id: NodeId,
        /// The node taking over (a replica being promoted, or an absorbing
        /// primary).
        new_primary_id: NodeId,
        /// `true`: the old primary is presumed dead and is removed from the
        /// cluster. `false` (graceful): the old primary is demoted to a
        /// replica of the new primary.
        force: bool,
    },

    /// Mark a node as failed.
    MarkNodeFailed { node_id: NodeId },

    /// Mark a node as recovered.
    MarkNodeRecovered { node_id: NodeId },

    /// Begin slot migration (marks slot as migrating).
    BeginSlotMigration {
        slot: u16,
        source_node: NodeId,
        target_node: NodeId,
    },

    /// Phase one of a two-phase slot handoff: record a *prepared* handoff on
    /// the migration so the source can quiesce the slot before ownership
    /// moves (rework issue 02).
    ///
    /// Applying this emits [`ClusterEvent::SlotHandoffPrepared`] on every node;
    /// the source arms a slot-scoped write barrier for `barrier_ms` and drains
    /// its shard, then proposes [`ClusterCommand::ConfirmSlotHandoffDrained`].
    /// Only a prepared *and* drained handoff admits a
    /// [`ClusterCommand::CompleteSlotMigration`].
    ///
    /// The two durations are different bounds on different things and both are
    /// load-bearing — see [`SlotHandoff`].
    PrepareSlotHandoff {
        slot: u16,
        source_node: NodeId,
        target_node: NodeId,
        /// How long the source holds its slot-scoped barrier, and therefore
        /// the window in which a `CompleteSlotMigration` is admissible.
        barrier_ms: u64,
        /// How long the *prepared record* itself stays valid. Longer than
        /// `barrier_ms`: it is the backstop that lets a later attempt supersede
        /// a prepare whose finalizer died.
        lease_ms: u64,
        /// The proposer's wall-clock reading (through the clock seam) at
        /// propose time. Carried in the entry rather than read during `apply`
        /// so every node computes the same deadlines from the same number —
        /// a state machine that read a local clock would diverge.
        proposed_at_ms: u64,
    },

    /// The source has drained its shard for a prepared handoff. `seq` names the
    /// prepare attempt being confirmed, so a drain ack that arrives after its
    /// own attempt was aborted cannot vouch for a later one.
    ConfirmSlotHandoffDrained { slot: u16, seq: u64 },

    /// Abandon a prepared handoff without abandoning the migration: the
    /// prepared record is cleared (releasing the source's barrier) and the
    /// `SlotMigration` stays exactly as it was, so the operator can retry
    /// `CLUSTER SETSLOT … NODE` without re-opening the migration.
    AbortSlotHandoff { slot: u16, seq: u64 },

    /// Complete slot migration.
    ///
    /// Admissible only against a handoff that is prepared, drained, and still
    /// inside its barrier window — a `Complete` that arrives after the source
    /// resumed serving is refused rather than half-applied.
    CompleteSlotMigration {
        slot: u16,
        source_node: NodeId,
        target_node: NodeId,
        /// The proposer's wall-clock reading at propose time, compared against
        /// the prepared handoff's barrier deadline. See `proposed_at_ms` on
        /// [`ClusterCommand::PrepareSlotHandoff`] for why it is carried rather
        /// than read at apply.
        proposed_at_ms: u64,
    },

    /// Cancel slot migration.
    CancelSlotMigration { slot: u16 },

    /// Finalize a rolling upgrade, advancing the active version and unlocking
    /// version-gated features. Only proposed after all nodes are verified at the
    /// target version. This is irreversible.
    FinalizeUpgrade { version: String },

    /// Reset cluster state (CLUSTER RESET SOFT/HARD).
    ResetCluster {
        /// The node performing the reset.
        node_id: NodeId,
        /// If Some, this is a HARD reset: replace node_id with new_node_id and reset epoch.
        new_node_id: Option<NodeId>,
    },
}

/// Response from applying a cluster command.
///
/// This is the openraft `R` value — the only value that crosses back from a
/// Raft apply. It carries structure end-to-end: the typed [`ClusterError`]
/// survives the apply boundary instead of being flattened to a display string,
/// and successful epoch-bumping commands return a typed [`ConfigEpoch`] rather
/// than a stringly-encoded number (proposal 32).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterResponse {
    /// Command succeeded.
    Ok,
    /// Command succeeded and produced a configuration epoch (e.g.
    /// `IncrementEpoch`, `Failover`).
    Epoch(ConfigEpoch),
    /// Command failed, carrying the typed error the state machine produced.
    Error(ClusterError),
}

impl ClusterResponse {
    /// Return the typed error iff this response is an [`ClusterResponse::Error`].
    ///
    /// Lets callers that only want the `Display` string stay terse while a
    /// caller that wants to branch on the variant can match it directly.
    pub fn as_error(&self) -> Option<&ClusterError> {
        match self {
            ClusterResponse::Error(e) => Some(e),
            _ => None,
        }
    }
}

/// A node-agnostic description of a side effect a successful cluster mutation
/// produced. [`crate::state::ClusterState::apply_command`] returns these on its
/// `Ok` path (so emit-on-failure is structurally impossible), and the state
/// machine's `apply` translates them into the channel payload types after
/// applying the node-local self-filter it owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterEvent {
    /// A node was demoted from primary to replica (via `SetRole { Replica }`
    /// or a graceful `Failover`). Node-agnostic: the state machine decides
    /// whether *this* node is the demoted one.
    NodeDemoted {
        /// The node that was demoted.
        demoted_node_id: NodeId,
        /// The node that is the new primary (if known).
        new_primary_id: Option<NodeId>,
        /// The configuration epoch at the time of demotion.
        epoch: u64,
    },
    /// A node was promoted from replica to primary (via `SetRole { Primary }`
    /// or the successor half of a `Failover`). Node-agnostic, and emitted only
    /// on a real role *transition* — re-applying a command that leaves an
    /// already-primary node primary produces nothing, so a log replay cannot
    /// drive a spurious data-path promotion.
    NodePromoted {
        /// The node that was promoted.
        promoted_node_id: NodeId,
        /// The configuration epoch at the time of promotion.
        epoch: u64,
    },
    /// A slot migration completed (relevant on all nodes, no self-filter).
    SlotMigrationCompleted {
        /// The slot whose migration completed.
        slot: u16,
        /// The node that previously owned the slot.
        source_node: NodeId,
        /// The node that now owns the slot.
        target_node: NodeId,
    },
    /// A slot handoff entered the *prepared* state: the source must now arm a
    /// slot-scoped write barrier, drain its shard, and confirm. Node-agnostic —
    /// the runtime decides whether *this* node is `source_node`.
    SlotHandoffPrepared {
        /// The slot being handed off.
        slot: u16,
        /// The node that must arm the barrier and drain.
        source_node: NodeId,
        /// The node that will own the slot once the handoff completes.
        target_node: NodeId,
        /// Attempt identifier, echoed back in
        /// [`ClusterCommand::ConfirmSlotHandoffDrained`].
        seq: u64,
        /// How long the source holds the barrier before letting it lapse.
        barrier_ms: u64,
    },
    /// A prepared slot handoff was released. Emitted by **every** arm that
    /// removes a prepared record — complete, abort, cancel, failover pruning,
    /// reset — so the invariant "a prepared handoff never disappears without a
    /// release event" holds and the source can always drop its barrier.
    SlotHandoffReleased {
        /// The slot whose handoff was released.
        slot: u16,
        /// The node that armed the barrier.
        source_node: NodeId,
        /// The attempt identifier being released.
        seq: u64,
    },
}

/// Cluster configuration options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// This node's ID.
    pub node_id: NodeId,
    /// Address for client connections.
    pub addr: SocketAddr,
    /// Address for cluster bus (Raft) communication.
    pub cluster_addr: SocketAddr,
    /// Initial cluster nodes to connect to (for joining existing cluster).
    pub initial_nodes: Vec<SocketAddr>,
    /// Directory for storing cluster state.
    pub data_dir: std::path::PathBuf,
    /// Election timeout in milliseconds.
    pub election_timeout_ms: u64,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            addr: "127.0.0.1:6379".parse().unwrap(),
            cluster_addr: "127.0.0.1:16379".parse().unwrap(),
            initial_nodes: Vec::new(),
            data_dir: std::path::PathBuf::from("./frogdb-cluster"),
            election_timeout_ms: 1000,
            heartbeat_interval_ms: 250,
        }
    }
}

/// Errors that can occur in cluster operations.
///
/// `Serialize`/`Deserialize` are required because this type is now carried in
/// [`ClusterResponse`], the openraft `R` value, which must be `serde` under the
/// enabled feature. Every variant holds only plain data (`NodeId`/`u16`/`String`),
/// so the enum stays serde-clean — keep it that way (a `#[from] io::Error`-style
/// variant would break the derive).
#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum ClusterError {
    /// Node not found.
    #[error("node {0} not found")]
    NodeNotFound(NodeId),

    /// Node already exists.
    #[error("node {0} already exists")]
    NodeAlreadyExists(NodeId),

    /// Slot not assigned to any node.
    #[error("slot {0} not assigned")]
    SlotNotAssigned(u16),

    /// Slot already assigned to another node.
    #[error("slot {0} already assigned to node {1}")]
    SlotAlreadyAssigned(u16, NodeId),

    /// Invalid slot number or range.
    #[error("invalid slot: {0}")]
    InvalidSlot(String),

    /// Not the Raft leader.
    #[error("not the cluster leader")]
    NotLeader,

    /// Raft error.
    #[error("raft error: {0}")]
    RaftError(String),

    /// Storage error.
    #[error("storage error: {0}")]
    StorageError(String),

    /// Network error.
    #[error("network error: {0}")]
    NetworkError(String),

    /// Cluster not initialized.
    #[error("cluster not initialized")]
    NotInitialized,

    /// Invalid operation.
    #[error("invalid operation: {0}")]
    InvalidOperation(String),

    /// Migration in progress.
    #[error("migration in progress for slot {0}")]
    MigrationInProgress(u16),

    /// A two-phase handoff precondition failed: not prepared, not drained, a
    /// stale attempt `seq`, or the barrier window already lapsed.
    ///
    /// Always **retryable** and always non-destructive — the `SlotMigration`
    /// record is left exactly as it was, so re-issuing `CLUSTER SETSLOT … NODE`
    /// starts a fresh handoff attempt. The connection layer renders this as
    /// `TRYAGAIN` rather than `ERR` for that reason.
    #[error("slot {0} handoff not ready: {1}")]
    HandoffNotReady(u16, String),
}

impl ClusterError {
    /// True if the caller can simply re-issue the same command later.
    pub fn is_retryable(&self) -> bool {
        matches!(self, ClusterError::HandoffNotReady(..))
    }
}

/// Default source-local barrier window for a prepared handoff, in milliseconds.
///
/// This is simultaneously the *prepare deadline*: the source arms its
/// slot-scoped pause with this timeout, so if the finalizer dies the barrier
/// lapses on its own and the source resumes serving without needing anyone to
/// tell it to. It is also the window inside which a `CompleteSlotMigration` is
/// admissible — past it, ownership must not move, because the source is
/// already serving writes again.
///
/// Sized from the measured finalization window (loaded p99 1.93 ms for the
/// residual, `finalization-window-measurement-2026-08-05.md`) with two orders
/// of magnitude of headroom for the Raft round trip that follows the drain.
pub const HANDOFF_BARRIER_MS: u64 = 100;

/// Default budget the finalizer waits for the drain confirmation before it
/// aborts, in milliseconds. Deliberately smaller than [`HANDOFF_BARRIER_MS`] so
/// the abort proposal and a successful `Complete` both still land inside the
/// barrier window.
pub const HANDOFF_DRAIN_WAIT_MS: u64 = 50;

/// Default bound on the source's own shard round trip, in milliseconds. Shaped
/// after the VLL continuation lock's `CONTINUATION_DRAIN_TIMEOUT`. On expiry the
/// source simply never confirms, so finalization aborts with the migration
/// record intact — it does **not** drop the barrier and proceed the way
/// Dragonfly does.
pub const HANDOFF_DRAIN_TIMEOUT_MS: u64 = 2_000;

/// Default lease on the *prepared record itself*, in milliseconds. Longer than
/// the barrier: it is the consensus-level backstop that lets a later attempt
/// supersede a prepare whose finalizer died mid-flight, and it is what an
/// operator sees dangling in `CLUSTER` state until it expires or
/// `SETSLOT STABLE` clears it.
pub const HANDOFF_LEASE_MS: u64 = 10_000;

/// The prepared phase of a two-phase slot handoff.
///
/// Every field is plain replicated data. Nothing here is computed from a clock
/// read during `apply` — see [`SlotHandoff::prepared_at_ms`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlotHandoff {
    /// Attempt identifier, minted from the replicated `handoff_seq` counter.
    /// A drain confirmation or abort must carry the matching `seq`, so a late
    /// ack from an aborted attempt cannot vouch for the attempt that replaced
    /// it.
    pub seq: u64,
    /// Wall-clock milliseconds, as read by the *proposer* through the clock
    /// seam. The state machine never reads a clock itself: two nodes applying
    /// the same entry at different instants would compute different deadlines
    /// and diverge. Readers compare this data against their own `system_now()`.
    pub prepared_at_ms: u64,
    /// Barrier window length; see [`HANDOFF_BARRIER_MS`].
    pub barrier_ms: u64,
    /// Prepared-record lease length; see [`HANDOFF_LEASE_MS`].
    pub lease_ms: u64,
    /// Set by `ConfirmSlotHandoffDrained` once the source's shard round trip
    /// returned. Only a drained handoff admits `CompleteSlotMigration`.
    pub drained: bool,
}

impl SlotHandoff {
    /// Wall-clock deadline (ms) after which the source has resumed serving.
    pub fn barrier_expires_at_ms(&self) -> u64 {
        self.prepared_at_ms.saturating_add(self.barrier_ms)
    }

    /// Wall-clock deadline (ms) after which the prepared record itself is dead
    /// and a fresh prepare may supersede it.
    pub fn lease_expires_at_ms(&self) -> u64 {
        self.prepared_at_ms.saturating_add(self.lease_ms)
    }

    /// True if `now_ms` is past the barrier window.
    pub fn barrier_expired_at(&self, now_ms: u64) -> bool {
        now_ms >= self.barrier_expires_at_ms()
    }

    /// True if `now_ms` is past the prepared-record lease.
    pub fn lease_expired_at(&self, now_ms: u64) -> bool {
        now_ms >= self.lease_expires_at_ms()
    }

    /// True if this handoff can still admit a `CompleteSlotMigration` proposed
    /// at `now_ms`: drained, inside its barrier window, inside its lease.
    pub fn admits_complete_at(&self, now_ms: u64) -> bool {
        self.drained && !self.barrier_expired_at(now_ms) && !self.lease_expired_at(now_ms)
    }
}

/// An in-progress slot migration.
///
/// Migration is an ownership swap bracketed by `BeginSlotMigration` (inserts
/// this record, making the slot "migrating") and `CompleteSlotMigration` /
/// `CancelSlotMigration` (remove it). Routing derives the importing/migrating
/// role by comparing the local node id against `source_node`/`target_node`, and
/// `CLUSTER SETSLOT IMPORTING` and `MIGRATING` both lower to the same begin
/// command.
///
/// The record carries exactly **one** intermediate state, and only over the
/// finalization instant: [`handoff`](Self::handoff). Ownership cannot flip
/// atomically with respect to in-flight writes on the source, so finalization
/// is two-phase (rework issue 02) — `PrepareSlotHandoff` sets `handoff`, the
/// source arms a slot-scoped write barrier and drains, `ConfirmSlotHandoffDrained`
/// marks it drained, and only then may `CompleteSlotMigration` move ownership.
/// `handoff` is `None` for the whole bulk-transfer phase, which is where a
/// migration spends essentially all of its life.
///
/// This is deliberately *not* a general `MigrationState` enum. One used to live
/// here, but only its initial variant was ever constructed and nothing
/// transitioned it; it was deleted. If incremental key transfer ever needs
/// resumable progress, that is a second axis and wants its own field rather
/// than a state enum that conflates transfer progress with handoff fencing.
/// Old Raft snapshots predating either field still deserialize — serde ignores
/// unknown fields, and `handoff` defaults to `None`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlotMigration {
    /// The slot being migrated.
    pub slot: u16,
    /// Source node ID.
    pub source_node: NodeId,
    /// Target node ID.
    pub target_node: NodeId,
    /// The prepared finalization handoff, if finalization is in flight.
    #[serde(default)]
    pub handoff: Option<SlotHandoff>,
}

impl SlotMigration {
    /// A migration in its ordinary (bulk-transfer, no handoff prepared) state.
    pub fn new(slot: u16, source_node: NodeId, target_node: NodeId) -> Self {
        Self {
            slot,
            source_node,
            target_node,
            handoff: None,
        }
    }

    /// The prepared handoff if it is still within its lease at `now_ms`.
    /// An expired prepared record is treated as absent by every reader, so a
    /// dead finalizer cannot wedge the slot.
    pub fn live_handoff_at(&self, now_ms: u64) -> Option<&SlotHandoff> {
        self.handoff
            .as_ref()
            .filter(|h| !h.lease_expired_at(now_ms))
    }
}

/// Snapshot of the complete cluster state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    /// All nodes in the cluster.
    pub nodes: BTreeMap<NodeId, NodeInfo>,
    /// Slot to node assignment.
    /// Key is slot number, value is the node ID owning that slot.
    pub slot_assignment: BTreeMap<u16, NodeId>,
    /// Current configuration epoch.
    pub config_epoch: ConfigEpoch,
    /// Active slot migrations.
    pub migrations: BTreeMap<u16, SlotMigration>,
    // `leader_id` removed (proposal 33): the leader is Raft runtime state, not
    // replicated metadata, so this DTO's builder cannot supply it. The debug
    // seam reads it live from `Raft::metrics().current_leader` instead.
    /// The finalized active version, if any.
    #[serde(default)]
    pub active_version: Option<String>,
}

impl ClusterSnapshot {
    /// Create a new empty snapshot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get node info by ID.
    pub fn get_node(&self, node_id: NodeId) -> Option<&NodeInfo> {
        self.nodes.get(&node_id)
    }

    /// Get the node owning a slot.
    pub fn get_slot_owner(&self, slot: u16) -> Option<NodeId> {
        self.slot_assignment.get(&slot).copied()
    }

    /// Get all slots assigned to a node.
    pub fn get_node_slots(&self, node_id: NodeId) -> Vec<SlotRange> {
        let mut slots: BTreeSet<u16> = BTreeSet::new();
        for (&slot, &owner) in &self.slot_assignment {
            if owner == node_id {
                slots.insert(slot);
            }
        }

        // Compact into ranges
        let mut ranges = Vec::new();
        let mut iter = slots.into_iter();
        if let Some(first) = iter.next() {
            let mut start = first;
            let mut end = first;
            for slot in iter {
                if slot == end + 1 {
                    end = slot;
                } else {
                    ranges.push(SlotRange::new(start, end));
                    start = slot;
                    end = slot;
                }
            }
            ranges.push(SlotRange::new(start, end));
        }
        ranges
    }

    /// Get all primary nodes.
    pub fn get_primaries(&self) -> Vec<&NodeInfo> {
        self.nodes.values().filter(|n| n.is_primary()).collect()
    }

    /// Get replicas for a primary node.
    pub fn get_replicas(&self, primary_id: NodeId) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|n| n.primary_id == Some(primary_id))
            .collect()
    }

    /// Check if all slots are assigned.
    pub fn all_slots_assigned(&self) -> bool {
        self.slot_assignment.len() == CLUSTER_SLOTS as usize
    }

    /// Get unassigned slots.
    pub fn get_unassigned_slots(&self) -> Vec<u16> {
        (0..CLUSTER_SLOTS)
            .filter(|s| !self.slot_assignment.contains_key(s))
            .collect()
    }

    /// Count assigned slots.
    pub fn assigned_slot_count(&self) -> usize {
        self.slot_assignment.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    /// A node table entry written before replica priorities existed must read
    /// back at the neutral default, not at "never promote" (0) and not at the
    /// most-preferred value (1): either would silently rewrite the failover
    /// order of every pre-upgrade node.
    // FM-CLUSTER-057
    #[test]
    fn test_missing_replica_priority_defaults_to_the_neutral_value() {
        assert_eq!(default_replica_priority(), 100);

        let legacy = r#"{
            "id": 1,
            "addr": "127.0.0.1:6379",
            "cluster_addr": "127.0.0.1:16379",
            "role": "Primary",
            "primary_id": null,
            "config_epoch": 0,
            "flags": {"handshake": false, "fail": false, "pfail": false, "noaddr": false}
        }"#;
        let node: NodeInfo = serde_json::from_str(legacy).unwrap();
        assert_eq!(node.replica_priority, 100);
        assert_eq!(node.version, "");
    }

    /// The two flag constructors are not interchangeable: a handshaking node is
    /// exactly a connected one plus the `handshake` bit, and nothing else.
    // FM-CLUSTER-001
    #[test]
    fn test_node_flags_handshaking_sets_only_the_handshake_bit() {
        let handshaking = NodeFlags::handshaking();
        assert!(handshaking.handshake);
        assert!(!handshaking.fail);
        assert!(!handshaking.pfail);
        assert!(!handshaking.noaddr);

        let connected = NodeFlags::connected();
        assert!(!connected.handshake);
        assert_ne!(
            handshaking, connected,
            "handshaking must be distinguishable from connected"
        );
        assert_eq!(
            connected,
            NodeFlags::default(),
            "connected is the zero value"
        );
    }

    /// Inclusive-range arithmetic: a range of one slot has length 1, and the
    /// length of `start..=end` is `end - start + 1` — every neighbouring
    /// formula (`end - start`, `end - start - 1`, `end + start + 1`) gets a
    /// different answer for at least one of these cases.
    // FM-CLUSTER-075
    #[test]
    fn test_slot_range_len_counts_both_endpoints() {
        assert_eq!(SlotRange::new(10, 20).len(), 11);
        assert_eq!(SlotRange::single(5).len(), 1);
        assert_eq!(SlotRange::new(0, CLUSTER_SLOTS - 1).len(), 16384);

        // A slot range is never empty: it always owns at least its own slot,
        // which is why `is_empty` is a constant rather than a length test.
        assert!(!SlotRange::new(0, 0).is_empty());
        assert!(!SlotRange::new(10, 20).is_empty());
        assert_eq!(
            SlotRange::single(7).iter().collect::<Vec<_>>(),
            vec![7],
            "len() and iter() must agree on the single-slot case"
        );
    }

    /// `start == end` is a legal one-slot range in the `a-b` form, not a
    /// rejected inversion: `SETSLOT`/`ADDSLOTS` callers write `"5-5"`.
    // FM-CLUSTER-075
    #[test]
    fn test_slot_range_parse_accepts_a_degenerate_range() {
        assert_eq!("5-5".parse::<SlotRange>().unwrap(), SlotRange::single(5));
        assert_eq!("5-6".parse::<SlotRange>().unwrap(), SlotRange::new(5, 6));
        assert!("6-5".parse::<SlotRange>().is_err());
    }

    /// Role partitioning of the node table: `get_primaries` is exactly the
    /// primaries, and `get_replicas(p)` is exactly the nodes following `p` —
    /// neither the complement nor everything.
    // FM-CLUSTER-071
    #[test]
    fn test_cluster_snapshot_partitions_nodes_by_role() {
        let mut snapshot = ClusterSnapshot::new();
        for node in [
            NodeInfo::new_primary(1, test_addr(6379), test_addr(16379)),
            NodeInfo::new_primary(2, test_addr(6380), test_addr(16380)),
            NodeInfo::new_replica(3, test_addr(6381), test_addr(16381), 1),
            NodeInfo::new_replica(4, test_addr(6382), test_addr(16382), 1),
            NodeInfo::new_replica(5, test_addr(6383), test_addr(16383), 2),
        ] {
            snapshot.nodes.insert(node.id, node);
        }

        let primaries: Vec<NodeId> = snapshot.get_primaries().iter().map(|n| n.id).collect();
        assert_eq!(primaries, vec![1, 2]);

        let replicas_of_1: Vec<NodeId> = snapshot.get_replicas(1).iter().map(|n| n.id).collect();
        assert_eq!(replicas_of_1, vec![3, 4]);
        let replicas_of_2: Vec<NodeId> = snapshot.get_replicas(2).iter().map(|n| n.id).collect();
        assert_eq!(replicas_of_2, vec![5]);
        assert!(
            snapshot.get_replicas(3).is_empty(),
            "a replica has no replicas of its own"
        );
    }

    /// The three slot-coverage readers agree with each other and with the slot
    /// table: a partially covered cluster is not "all assigned", its unassigned
    /// list is exactly the complement of what is assigned, and the count is the
    /// table's size.
    // FM-CLUSTER-073
    #[test]
    fn test_cluster_snapshot_slot_coverage_readers_agree() {
        let mut snapshot = ClusterSnapshot::new();
        assert!(
            !snapshot.all_slots_assigned(),
            "an empty table covers nothing"
        );
        assert_eq!(snapshot.assigned_slot_count(), 0);
        assert_eq!(
            snapshot.get_unassigned_slots().len(),
            CLUSTER_SLOTS as usize
        );

        for slot in 0..11u16 {
            snapshot.slot_assignment.insert(slot, 1);
        }
        assert!(!snapshot.all_slots_assigned());
        assert_eq!(snapshot.assigned_slot_count(), 11);

        let unassigned = snapshot.get_unassigned_slots();
        assert_eq!(unassigned.len(), CLUSTER_SLOTS as usize - 11);
        assert_eq!(
            unassigned.first().copied(),
            Some(11),
            "the unassigned list is the complement, not the assigned set"
        );
        assert_eq!(unassigned.last().copied(), Some(CLUSTER_SLOTS - 1));
        assert!(
            (0..11u16).all(|s| !unassigned.contains(&s)),
            "an assigned slot must never be reported unassigned"
        );

        for slot in 11..CLUSTER_SLOTS {
            snapshot.slot_assignment.insert(slot, 2);
        }
        assert!(snapshot.all_slots_assigned());
        assert_eq!(snapshot.assigned_slot_count(), CLUSTER_SLOTS as usize);
        assert!(snapshot.get_unassigned_slots().is_empty());
    }

    // FM-CLUSTER-075
    #[test]
    fn test_slot_range_contains() {
        let range = SlotRange::new(100, 200);
        assert!(range.contains(100));
        assert!(range.contains(150));
        assert!(range.contains(200));
        assert!(!range.contains(99));
        assert!(!range.contains(201));
    }

    // FM-CLUSTER-075
    #[test]
    fn test_slot_range_display() {
        assert_eq!(SlotRange::new(0, 100).to_string(), "0-100");
        assert_eq!(SlotRange::single(42).to_string(), "42");
    }

    // FM-CLUSTER-075
    #[test]
    fn test_slot_range_parse() {
        assert_eq!(
            "0-100".parse::<SlotRange>().unwrap(),
            SlotRange::new(0, 100)
        );
        assert_eq!("42".parse::<SlotRange>().unwrap(), SlotRange::single(42));
        assert!("invalid".parse::<SlotRange>().is_err());
        assert!("100-50".parse::<SlotRange>().is_err()); // start > end
        assert!("20000".parse::<SlotRange>().is_err()); // out of range
    }

    // FM-CLUSTER-072
    #[test]
    fn test_cluster_snapshot_get_node_slots() {
        let mut snapshot = ClusterSnapshot::new();
        let node_id = 1;

        // Assign some slots
        for slot in 0..10 {
            snapshot.slot_assignment.insert(slot, node_id);
        }
        for slot in 20..25 {
            snapshot.slot_assignment.insert(slot, node_id);
        }

        let ranges = snapshot.get_node_slots(node_id);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], SlotRange::new(0, 9));
        assert_eq!(ranges[1], SlotRange::new(20, 24));
    }

    // FM-CLUSTER-015
    #[test]
    fn test_cluster_response_as_error() {
        // Error carries the typed variant, retrievable via as_error().
        let err = ClusterResponse::Error(ClusterError::NodeNotFound(7));
        assert!(matches!(
            err.as_error(),
            Some(ClusterError::NodeNotFound(7))
        ));

        // Non-error variants yield None.
        assert!(ClusterResponse::Ok.as_error().is_none());
        assert!(ClusterResponse::Epoch(3).as_error().is_none());
    }

    // FM-CLUSTER-015
    #[test]
    fn test_cluster_response_serde_round_trip() {
        // The new derives + the openraft `R: Serialize + Deserialize` bound:
        // a typed error and the typed epoch both survive a serde round-trip.
        let err = ClusterResponse::Error(ClusterError::SlotAlreadyAssigned(1, 2));
        let json = serde_json::to_string(&err).unwrap();
        let back: ClusterResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            back.as_error(),
            Some(ClusterError::SlotAlreadyAssigned(1, 2))
        ));

        let epoch = ClusterResponse::Epoch(7);
        let json = serde_json::to_string(&epoch).unwrap();
        let back: ClusterResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, ClusterResponse::Epoch(7)));
    }

    /// The headline test: for a representative spread of node counts (divisors,
    /// non-divisors, and edges), the partition covers every slot in
    /// `0..CLUSTER_SLOTS` exactly once with disjoint, contiguous ranges, and the
    /// remainder always lands on the last node.
    // FM-CLUSTER-007
    #[test]
    fn test_even_slot_ranges_partition_correctness() {
        for num_nodes in [1usize, 2, 3, 5, 7, 16] {
            let node_ids: Vec<NodeId> = (1..=num_nodes as NodeId).collect();
            let ranges = even_slot_ranges(&node_ids);

            // One entry per node, in input order.
            assert_eq!(ranges.len(), num_nodes, "num_nodes={num_nodes}");
            for (i, (nid, _)) in ranges.iter().enumerate() {
                assert_eq!(*nid, node_ids[i], "order preserved, num_nodes={num_nodes}");
            }

            // Every slot covered exactly once.
            let mut covered = vec![false; CLUSTER_SLOTS as usize];
            for (_, range) in &ranges {
                for slot in range.iter() {
                    assert!(
                        !covered[slot as usize],
                        "slot {slot} covered twice, num_nodes={num_nodes}"
                    );
                    covered[slot as usize] = true;
                }
            }
            assert!(
                covered.iter().all(|&c| c),
                "not all slots covered, num_nodes={num_nodes}"
            );

            // Disjoint + contiguous: first starts at 0, each range abuts the next.
            assert_eq!(ranges[0].1.start, 0, "starts at 0, num_nodes={num_nodes}");
            for pair in ranges.windows(2) {
                assert_eq!(
                    pair[0].1.end + 1,
                    pair[1].1.start,
                    "ranges abut, num_nodes={num_nodes}"
                );
            }

            // Remainder lands on the last node.
            assert_eq!(
                ranges.last().unwrap().1.end,
                CLUSTER_SLOTS - 1,
                "last range ends at CLUSTER_SLOTS-1, num_nodes={num_nodes}"
            );
        }
    }

    /// Order-in equals partition-out: the node at index 0 always owns the range
    /// starting at slot 0, regardless of the node ids' numeric order. This pins
    /// the "position i -> node i" contract both bootstrap call sites rely on.
    // FM-CLUSTER-007
    #[test]
    fn test_even_slot_ranges_order_preservation() {
        let ascending = even_slot_ranges(&[10, 20, 30]);
        let reordered = even_slot_ranges(&[30, 10, 20]);

        // Same ranges assigned by slice position...
        let ascending_ranges: Vec<SlotRange> = ascending.iter().map(|(_, r)| *r).collect();
        let reordered_ranges: Vec<SlotRange> = reordered.iter().map(|(_, r)| *r).collect();
        assert_eq!(ascending_ranges, reordered_ranges);

        // ...but bound to whichever node sits at that position.
        assert_eq!(ascending[0].0, 10);
        assert_eq!(reordered[0].0, 30);
        assert_eq!(ascending[0].1.start, 0);
        assert_eq!(reordered[0].1.start, 0);
    }

    /// Exact boundaries for the common cases — the literal values a regression in
    /// either bootstrap block would change.
    // FM-CLUSTER-007
    #[test]
    fn test_even_slot_ranges_exact_boundaries() {
        let three = even_slot_ranges(&[1, 2, 3]);
        assert_eq!(
            three,
            vec![
                (1, SlotRange::new(0, 5460)),
                (2, SlotRange::new(5461, 10921)),
                (3, SlotRange::new(10922, 16383)),
            ]
        );

        let one = even_slot_ranges(&[42]);
        assert_eq!(one, vec![(42, SlotRange::new(0, 16383))]);
    }

    /// Degenerate input: an empty slice yields an empty partition.
    // FM-CLUSTER-007
    #[test]
    fn test_even_slot_ranges_empty() {
        assert!(even_slot_ranges(&[]).is_empty());
    }

    /// Cross-path equivalence (regression guard for the original bug): mapping
    /// `even_slot_ranges` into `AssignSlots` commands yields the identical command
    /// sequence for the "local" and "raft" bootstrap paths. Trivially true now
    /// that both share the function — the test documents the invariant the
    /// hand-synced comment used to assert.
    // FM-CLUSTER-007
    #[test]
    fn test_even_slot_ranges_cross_path_equivalence() {
        let node_ids: Vec<NodeId> = vec![7, 11, 13];
        let to_commands = |ids: &[NodeId]| -> Vec<(NodeId, Vec<SlotRange>)> {
            even_slot_ranges(ids)
                .into_iter()
                .map(|(nid, range)| {
                    let cmd = ClusterCommand::AssignSlots {
                        node_id: nid,
                        slots: vec![range],
                    };
                    match cmd {
                        ClusterCommand::AssignSlots { node_id, slots } => (node_id, slots),
                        _ => unreachable!(),
                    }
                })
                .collect()
        };
        let local_path = to_commands(&node_ids);
        let raft_path = to_commands(&node_ids);
        assert_eq!(local_path, raft_path);
    }
}
