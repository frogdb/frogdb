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

    /// Complete slot migration.
    CompleteSlotMigration {
        slot: u16,
        source_node: NodeId,
        target_node: NodeId,
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
    /// A slot migration completed (relevant on all nodes, no self-filter).
    SlotMigrationCompleted {
        /// The slot whose migration completed.
        slot: u16,
        /// The node that previously owned the slot.
        source_node: NodeId,
        /// The node that now owns the slot.
        target_node: NodeId,
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
}

/// An in-progress slot migration.
///
/// Migration is a **two-point ownership swap**: `BeginSlotMigration` inserts
/// this record (making the slot "migrating"), `CompleteSlotMigration` /
/// `CancelSlotMigration` remove it. There is no intermediate state machine —
/// routing derives the importing/migrating role by comparing the local node id
/// against `source_node`/`target_node`, and `CLUSTER SETSLOT IMPORTING` and
/// `MIGRATING` both lower to the same begin command. (A `MigrationState` enum
/// used to live here, but only its initial variant was ever constructed and
/// nothing transitioned it; it was deleted. If incremental key transfer ever
/// needs resumable progress, reintroduce real states driven by the migration
/// executor. Old Raft snapshots carrying the `state` field still deserialize —
/// serde ignores unknown fields.)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlotMigration {
    /// The slot being migrated.
    pub slot: u16,
    /// Source node ID.
    pub source_node: NodeId,
    /// Target node ID.
    pub target_node: NodeId,
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

    #[test]
    fn test_slot_range_contains() {
        let range = SlotRange::new(100, 200);
        assert!(range.contains(100));
        assert!(range.contains(150));
        assert!(range.contains(200));
        assert!(!range.contains(99));
        assert!(!range.contains(201));
    }

    #[test]
    fn test_slot_range_display() {
        assert_eq!(SlotRange::new(0, 100).to_string(), "0-100");
        assert_eq!(SlotRange::single(42).to_string(), "42");
    }

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
    #[test]
    fn test_even_slot_ranges_empty() {
        assert!(even_slot_ranges(&[]).is_empty());
    }

    /// Cross-path equivalence (regression guard for the original bug): mapping
    /// `even_slot_ranges` into `AssignSlots` commands yields the identical command
    /// sequence for the "local" and "raft" bootstrap paths. Trivially true now
    /// that both share the function — the test documents the invariant the
    /// hand-synced comment used to assert.
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
