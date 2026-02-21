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

    /// Format node info in Redis CLUSTER NODES format.
    pub fn to_cluster_nodes_line(&self, myself: bool, slots: &[SlotRange]) -> String {
        let flags = if myself {
            format!("myself,{}", self.role)
        } else {
            self.role.to_string()
        };

        let primary_id = self
            .primary_id
            .map(|id| format!("{:016x}", id))
            .unwrap_or_else(|| "-".to_string());

        let slots_str: Vec<String> = slots.iter().map(|r| r.to_string()).collect();

        format!(
            "{:016x} {}@{} {} {} 0 {} connected {}",
            self.id,
            self.addr,
            self.cluster_addr.port(),
            flags,
            primary_id,
            self.config_epoch,
            slots_str.join(" ")
        )
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
}

/// Response from applying a cluster command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterResponse {
    /// Command succeeded.
    Ok,
    /// Command returned a value.
    Value(String),
    /// Command failed.
    Error(String),
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
#[derive(Debug, Error, Clone)]
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

/// Slot migration state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlotMigration {
    /// The slot being migrated.
    pub slot: u16,
    /// Source node ID.
    pub source_node: NodeId,
    /// Target node ID.
    pub target_node: NodeId,
    /// Migration state.
    pub state: MigrationState,
}

/// State of a slot migration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MigrationState {
    /// Migration has been initiated.
    Initiated,
    /// Keys are being transferred.
    Migrating,
    /// Migration is complete, waiting for confirmation.
    Completing,
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
    /// The Raft leader node ID (if known).
    pub leader_id: Option<NodeId>,
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
    fn test_node_info_to_cluster_nodes_line() {
        let node = NodeInfo::new_primary(
            1,
            "127.0.0.1:6379".parse().unwrap(),
            "127.0.0.1:16379".parse().unwrap(),
        );
        let slots = vec![SlotRange::new(0, 5460)];
        let line = node.to_cluster_nodes_line(true, &slots);
        assert!(line.contains("myself,master"));
        assert!(line.contains("0-5460"));
    }
}
