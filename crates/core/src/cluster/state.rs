//! Cluster state and Raft state machine implementation.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::types::{
    ClusterCommand, ClusterError, ClusterResponse, ClusterSnapshot, ConfigEpoch, MigrationState,
    NodeId, NodeInfo, NodeRole, SlotMigration, SlotRange, TypeConfig, CLUSTER_SLOTS,
};

/// The cluster state, protected by a read-write lock for concurrent access.
#[derive(Debug, Clone, Default)]
pub struct ClusterState {
    inner: Arc<RwLock<ClusterStateInner>>,
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
    /// Last applied log index.
    pub last_applied_log: Option<LogId<NodeId>>,
    /// Last membership configuration.
    pub last_membership: StoredMembership<NodeId, openraft::BasicNode>,
}

impl ClusterState {
    /// Create a new empty cluster state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create cluster state from a snapshot.
    pub fn from_snapshot(snapshot: ClusterSnapshot) -> Self {
        let inner = ClusterStateInner {
            nodes: snapshot.nodes,
            slot_assignment: snapshot.slot_assignment,
            config_epoch: snapshot.config_epoch,
            migrations: snapshot.migrations,
            last_applied_log: None,
            last_membership: StoredMembership::default(),
        };
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    /// Get a snapshot of the current state.
    pub fn snapshot(&self) -> ClusterSnapshot {
        let inner = self.inner.read();
        ClusterSnapshot {
            nodes: inner.nodes.clone(),
            slot_assignment: inner.slot_assignment.clone(),
            config_epoch: inner.config_epoch,
            migrations: inner.migrations.clone(),
            leader_id: None, // Will be set by caller
        }
    }

    /// Get node info by ID.
    pub fn get_node(&self, node_id: NodeId) -> Option<NodeInfo> {
        self.inner.read().nodes.get(&node_id).cloned()
    }

    /// Get all nodes.
    pub fn get_all_nodes(&self) -> Vec<NodeInfo> {
        self.inner.read().nodes.values().cloned().collect()
    }

    /// Get the node owning a slot.
    pub fn get_slot_owner(&self, slot: u16) -> Option<NodeId> {
        self.inner.read().slot_assignment.get(&slot).copied()
    }

    /// Get all slots assigned to a node as ranges.
    pub fn get_node_slots(&self, node_id: NodeId) -> Vec<SlotRange> {
        self.snapshot().get_node_slots(node_id)
    }

    /// Get the current configuration epoch.
    pub fn config_epoch(&self) -> ConfigEpoch {
        self.inner.read().config_epoch
    }

    /// Check if a slot is migrating.
    pub fn is_slot_migrating(&self, slot: u16) -> bool {
        self.inner.read().migrations.contains_key(&slot)
    }

    /// Get migration info for a slot.
    pub fn get_slot_migration(&self, slot: u16) -> Option<SlotMigration> {
        self.inner.read().migrations.get(&slot).cloned()
    }

    /// Apply a command to the state.
    fn apply_command(&self, cmd: ClusterCommand) -> Result<ClusterResponse, ClusterError> {
        let mut inner = self.inner.write();

        match cmd {
            ClusterCommand::AddNode { node } => {
                if inner.nodes.contains_key(&node.id) {
                    return Err(ClusterError::NodeAlreadyExists(node.id));
                }
                tracing::info!(node_id = node.id, addr = %node.addr, "Adding node to cluster");
                inner.nodes.insert(node.id, node);
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::RemoveNode { node_id } => {
                if !inner.nodes.contains_key(&node_id) {
                    return Err(ClusterError::NodeNotFound(node_id));
                }
                // Remove slot assignments for this node
                inner.slot_assignment.retain(|_, &mut owner| owner != node_id);
                inner.nodes.remove(&node_id);
                tracing::info!(node_id, "Removed node from cluster");
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::AssignSlots { node_id, slots } => {
                if !inner.nodes.contains_key(&node_id) {
                    return Err(ClusterError::NodeNotFound(node_id));
                }

                for range in &slots {
                    for slot in range.iter() {
                        if let Some(&existing_owner) = inner.slot_assignment.get(&slot) {
                            if existing_owner != node_id {
                                return Err(ClusterError::SlotAlreadyAssigned(slot, existing_owner));
                            }
                        }
                    }
                }

                for range in slots {
                    for slot in range.iter() {
                        inner.slot_assignment.insert(slot, node_id);
                    }
                }
                tracing::debug!(node_id, "Assigned slots to node");
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::RemoveSlots { node_id, slots } => {
                for range in slots {
                    for slot in range.iter() {
                        if let Some(&owner) = inner.slot_assignment.get(&slot) {
                            if owner == node_id {
                                inner.slot_assignment.remove(&slot);
                            }
                        }
                    }
                }
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::SetRole {
                node_id,
                role,
                primary_id,
            } => {
                // Validate node exists first
                if !inner.nodes.contains_key(&node_id) {
                    return Err(ClusterError::NodeNotFound(node_id));
                }

                // Validate primary_id if setting replica role
                if role == NodeRole::Replica {
                    if let Some(pid) = primary_id {
                        if !inner.nodes.contains_key(&pid) {
                            return Err(ClusterError::NodeNotFound(pid));
                        }
                    } else {
                        return Err(ClusterError::InvalidOperation(
                            "replica requires a primary_id".to_string(),
                        ));
                    }
                }

                // Now we can safely modify
                let node = inner.nodes.get_mut(&node_id).unwrap();
                node.role = role;
                node.primary_id = primary_id;
                tracing::info!(node_id, ?role, "Set node role");
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::IncrementEpoch => {
                inner.config_epoch += 1;
                tracing::debug!(epoch = inner.config_epoch, "Incremented config epoch");
                Ok(ClusterResponse::Value(inner.config_epoch.to_string()))
            }

            ClusterCommand::MarkNodeFailed { node_id } => {
                let node = inner
                    .nodes
                    .get_mut(&node_id)
                    .ok_or(ClusterError::NodeNotFound(node_id))?;
                node.flags.fail = true;
                tracing::warn!(node_id, "Marked node as failed");
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::MarkNodeRecovered { node_id } => {
                let node = inner
                    .nodes
                    .get_mut(&node_id)
                    .ok_or(ClusterError::NodeNotFound(node_id))?;
                node.flags.fail = false;
                node.flags.pfail = false;
                tracing::info!(node_id, "Marked node as recovered");
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::BeginSlotMigration {
                slot,
                source_node,
                target_node,
            } => {
                if inner.migrations.contains_key(&slot) {
                    return Err(ClusterError::MigrationInProgress(slot));
                }

                if !inner.nodes.contains_key(&source_node) {
                    return Err(ClusterError::NodeNotFound(source_node));
                }
                if !inner.nodes.contains_key(&target_node) {
                    return Err(ClusterError::NodeNotFound(target_node));
                }

                let owner = inner
                    .slot_assignment
                    .get(&slot)
                    .ok_or(ClusterError::SlotNotAssigned(slot))?;
                if *owner != source_node {
                    return Err(ClusterError::InvalidOperation(format!(
                        "slot {} is owned by {}, not {}",
                        slot, owner, source_node
                    )));
                }

                inner.migrations.insert(
                    slot,
                    SlotMigration {
                        slot,
                        source_node,
                        target_node,
                        state: MigrationState::Initiated,
                    },
                );
                tracing::info!(slot, source_node, target_node, "Started slot migration");
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::CompleteSlotMigration {
                slot,
                source_node,
                target_node,
            } => {
                let migration = inner
                    .migrations
                    .get(&slot)
                    .ok_or(ClusterError::InvalidOperation(format!(
                        "no migration in progress for slot {}",
                        slot
                    )))?;

                if migration.source_node != source_node || migration.target_node != target_node {
                    return Err(ClusterError::InvalidOperation(
                        "migration parameters don't match".to_string(),
                    ));
                }

                // Transfer slot ownership
                inner.slot_assignment.insert(slot, target_node);
                inner.migrations.remove(&slot);
                tracing::info!(slot, source_node, target_node, "Completed slot migration");
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::CancelSlotMigration { slot } => {
                inner.migrations.remove(&slot);
                tracing::info!(slot, "Cancelled slot migration");
                Ok(ClusterResponse::Ok)
            }
        }
    }
}

/// Raft state machine for cluster coordination.
pub struct ClusterStateMachine {
    state: ClusterState,
}

impl ClusterStateMachine {
    /// Create a new state machine.
    pub fn new() -> Self {
        Self {
            state: ClusterState::new(),
        }
    }

    /// Create a state machine with existing state.
    pub fn with_state(state: ClusterState) -> Self {
        Self { state }
    }

    /// Get a reference to the cluster state.
    pub fn state(&self) -> &ClusterState {
        &self.state
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
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, openraft::BasicNode>), StorageError<NodeId>>
    {
        let inner = self.state.inner.read();
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
                    let result = self.state.apply_command(cmd).unwrap_or_else(|e| {
                        tracing::warn!(error = %e, "Failed to apply cluster command");
                        ClusterResponse::Error(e.to_string())
                    });
                    results.push(result);
                }
                EntryPayload::Membership(membership) => {
                    let mut inner = self.state.inner.write();
                    inner.last_membership = StoredMembership::new(Some(log_id), membership);
                    results.push(ClusterResponse::Ok);
                }
            }

            // Update last applied log
            self.state.inner.write().last_applied_log = Some(log_id);
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ClusterStateMachine {
            state: self.state.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
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

        let mut inner = self.state.inner.write();
        *inner = snapshot_state;
        inner.last_applied_log = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();

        tracing::info!(
            last_log_id = ?meta.last_log_id,
            "Installed cluster state snapshot"
        );
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.state.inner.read();

        let Some(last_applied_log) = inner.last_applied_log else {
            return Ok(None);
        };

        let data = serde_json::to_vec(&*inner).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: Some(last_applied_log),
                last_membership: inner.last_membership.clone(),
                snapshot_id: format!("snapshot-{}", last_applied_log.index),
            },
            snapshot: Box::new(Cursor::new(data)),
        };

        Ok(Some(snapshot))
    }
}

impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for ClusterStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let inner = self.state.inner.read();

        let last_applied_log = inner.last_applied_log;

        let data = serde_json::to_vec(&*inner).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        let snapshot_id = match last_applied_log {
            Some(log_id) => format!("snapshot-{}", log_id.index),
            None => "snapshot-0".to_string(),
        };

        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied_log,
                last_membership: inner.last_membership.clone(),
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(data)),
        };

        tracing::info!(
            last_log_id = ?last_applied_log,
            "Built cluster state snapshot"
        );

        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    #[test]
    fn test_add_node() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));

        let result = state.apply_command(ClusterCommand::AddNode { node: node.clone() });
        assert!(matches!(result, Ok(ClusterResponse::Ok)));

        let retrieved = state.get_node(1).unwrap();
        assert_eq!(retrieved.addr, test_addr(6379));
    }

    #[test]
    fn test_add_duplicate_node() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));

        state.apply_command(ClusterCommand::AddNode { node: node.clone() }).unwrap();
        let result = state.apply_command(ClusterCommand::AddNode { node });

        assert!(matches!(result, Err(ClusterError::NodeAlreadyExists(1))));
    }

    #[test]
    fn test_assign_slots() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state.apply_command(ClusterCommand::AddNode { node }).unwrap();

        let result = state.apply_command(ClusterCommand::AssignSlots {
            node_id: 1,
            slots: vec![SlotRange::new(0, 100)],
        });
        assert!(matches!(result, Ok(ClusterResponse::Ok)));

        assert_eq!(state.get_slot_owner(50), Some(1));
        assert_eq!(state.get_slot_owner(101), None);
    }

    #[test]
    fn test_remove_node_clears_slots() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        state.apply_command(ClusterCommand::AddNode { node }).unwrap();
        state.apply_command(ClusterCommand::AssignSlots {
            node_id: 1,
            slots: vec![SlotRange::new(0, 100)],
        }).unwrap();

        state.apply_command(ClusterCommand::RemoveNode { node_id: 1 }).unwrap();

        assert_eq!(state.get_slot_owner(50), None);
        assert!(state.get_node(1).is_none());
    }

    #[test]
    fn test_set_role() {
        let state = ClusterState::new();
        let primary = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let replica = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state.apply_command(ClusterCommand::AddNode { node: primary }).unwrap();
        state.apply_command(ClusterCommand::AddNode { node: replica }).unwrap();

        state.apply_command(ClusterCommand::SetRole {
            node_id: 2,
            role: NodeRole::Replica,
            primary_id: Some(1),
        }).unwrap();

        let node = state.get_node(2).unwrap();
        assert!(node.is_replica());
        assert_eq!(node.primary_id, Some(1));
    }

    #[test]
    fn test_increment_epoch() {
        let state = ClusterState::new();
        assert_eq!(state.config_epoch(), 0);

        state.apply_command(ClusterCommand::IncrementEpoch).unwrap();
        assert_eq!(state.config_epoch(), 1);

        state.apply_command(ClusterCommand::IncrementEpoch).unwrap();
        assert_eq!(state.config_epoch(), 2);
    }

    #[test]
    fn test_slot_migration() {
        let state = ClusterState::new();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        state.apply_command(ClusterCommand::AddNode { node: node1 }).unwrap();
        state.apply_command(ClusterCommand::AddNode { node: node2 }).unwrap();
        state.apply_command(ClusterCommand::AssignSlots {
            node_id: 1,
            slots: vec![SlotRange::single(42)],
        }).unwrap();

        // Begin migration
        state.apply_command(ClusterCommand::BeginSlotMigration {
            slot: 42,
            source_node: 1,
            target_node: 2,
        }).unwrap();

        assert!(state.is_slot_migrating(42));
        let migration = state.get_slot_migration(42).unwrap();
        assert_eq!(migration.source_node, 1);
        assert_eq!(migration.target_node, 2);

        // Complete migration
        state.apply_command(ClusterCommand::CompleteSlotMigration {
            slot: 42,
            source_node: 1,
            target_node: 2,
        }).unwrap();

        assert!(!state.is_slot_migrating(42));
        assert_eq!(state.get_slot_owner(42), Some(2));
    }
}
