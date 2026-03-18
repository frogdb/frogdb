//! Cluster state and Raft state machine implementation.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::storage::RaftStateMachine;
use openraft::{EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use tokio::sync::mpsc;

use crate::types::{
    CLUSTER_SLOTS, ClusterCommand, ClusterResponse, ClusterSnapshot, ConfigEpoch, NodeId, NodeInfo,
    NodeRole, SlotMigration, SlotRange, TypeConfig,
};

/// The cluster state, protected by a read-write lock for concurrent access.
#[derive(Debug, Clone, Default)]
pub struct ClusterState {
    pub(crate) inner: Arc<RwLock<ClusterStateInner>>,
    /// This node's current ID. Shared across all connections so that HARD reset
    /// (which generates a new node ID) is visible immediately. Not Raft-replicated.
    self_node_id: Arc<AtomicU64>,
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

    /// Create cluster state from a snapshot, preserving the existing `self_node_id`
    /// (which is local state, not Raft-replicated).
    pub fn from_snapshot(snapshot: ClusterSnapshot, self_node_id: Arc<AtomicU64>) -> Self {
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
            self_node_id,
        }
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

    /// Add a node to the cluster state directly (for local initialization).
    /// This bypasses Raft consensus and should only be used during node startup.
    pub fn add_node(&self, node: NodeInfo) {
        let mut inner = self.inner.write();
        let node_id = node.id;
        let node_addr = node.addr;
        if inner.nodes.contains_key(&node.id) {
            tracing::info!(node_id = node_id, addr = %node_addr, "Updating node in local cluster state");
        } else {
            tracing::info!(node_id = node_id, addr = %node_addr, "Adding node to local cluster state");
        }
        inner.nodes.insert(node.id, node);
    }

    /// Assign slots to a node directly (for local initialization during bootstrap).
    /// This bypasses Raft consensus and should only be used during initial cluster formation.
    pub fn assign_slots(&self, node_id: NodeId, slots: impl IntoIterator<Item = u16>) {
        let mut inner = self.inner.write();
        if !inner.nodes.contains_key(&node_id) {
            tracing::warn!(node_id, "Cannot assign slots to unknown node");
            return;
        }
        let mut count = 0;
        for slot in slots {
            if slot < CLUSTER_SLOTS {
                inner.slot_assignment.insert(slot, node_id);
                count += 1;
            }
        }
        tracing::info!(node_id, slot_count = count, "Assigned slots to node");
    }

    /// Check if all slots are assigned.
    pub fn all_slots_assigned(&self) -> bool {
        let inner = self.inner.read();
        inner.slot_assignment.len() == CLUSTER_SLOTS as usize
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

/// Event emitted when a slot migration completes (fires on ALL nodes).
#[derive(Debug, Clone)]
pub struct SlotMigrationCompleteEvent {
    pub slot: u16,
    pub source_node: NodeId,
    pub target_node: NodeId,
}

/// Raft state machine for cluster coordination.
pub struct ClusterStateMachine {
    state: ClusterState,
    /// This node's ID, used to detect self-demotion events.
    self_node_id: Option<NodeId>,
    /// Channel to notify when this node is demoted from primary to replica.
    demotion_tx: Option<mpsc::UnboundedSender<DemotionEvent>>,
    /// Channel to notify when a slot migration completes.
    migration_complete_tx: Option<mpsc::UnboundedSender<SlotMigrationCompleteEvent>>,
}

impl ClusterStateMachine {
    /// Create a new state machine.
    pub fn new() -> Self {
        Self {
            state: ClusterState::new(),
            self_node_id: None,
            demotion_tx: None,
            migration_complete_tx: None,
        }
    }

    /// Create a state machine with existing state.
    pub fn with_state(state: ClusterState) -> Self {
        Self {
            state,
            self_node_id: None,
            demotion_tx: None,
            migration_complete_tx: None,
        }
    }

    /// Configure self-demotion detection.
    ///
    /// When a `SetRole { role: Replica }` command is applied for `self_node_id`,
    /// a `DemotionEvent` is sent through the returned receiver.
    pub fn enable_demotion_detection(
        &mut self,
        self_node_id: NodeId,
    ) -> mpsc::UnboundedReceiver<DemotionEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.self_node_id = Some(self_node_id);
        self.demotion_tx = Some(tx);
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
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
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
                    // Check for self-demotion before applying
                    if let Some(self_id) = self.self_node_id
                        && let ClusterCommand::SetRole {
                            node_id,
                            role: NodeRole::Replica,
                            primary_id,
                        } = &cmd
                        && *node_id == self_id
                        && let Some(ref tx) = self.demotion_tx
                    {
                        let epoch = self.state.config_epoch();
                        let _ = tx.send(DemotionEvent {
                            demoted_node_id: self_id,
                            new_primary_id: *primary_id,
                            epoch,
                        });
                    }

                    // Extract migration event data before apply consumes cmd
                    let migration_event = if let ClusterCommand::CompleteSlotMigration {
                        slot,
                        source_node,
                        target_node,
                    } = &cmd
                    {
                        Some(SlotMigrationCompleteEvent {
                            slot: *slot,
                            source_node: *source_node,
                            target_node: *target_node,
                        })
                    } else {
                        None
                    };

                    let result = self.state.apply_command(cmd).unwrap_or_else(|e| {
                        tracing::warn!(error = %e, "Failed to apply cluster command");
                        ClusterResponse::Error(e.to_string())
                    });

                    // Emit migration complete event on success
                    if let Some(event) = migration_event
                        && !matches!(result, ClusterResponse::Error(_))
                        && let Some(ref tx) = self.migration_complete_tx
                    {
                        let _ = tx.send(event);
                    }

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
            self_node_id: None,
            demotion_tx: None,
            migration_complete_tx: None,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
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

        state
            .apply_command(ClusterCommand::AddNode { node: node.clone() })
            .unwrap();
        // AddNode is an upsert — adding the same node again should succeed
        let result = state.apply_command(ClusterCommand::AddNode {
            node: NodeInfo::new_primary(1, test_addr(6380), test_addr(16380)),
        });
        assert!(matches!(result, Ok(ClusterResponse::Ok)));

        // Verify the node was updated with the new addresses
        let info = state.get_node(1).unwrap();
        assert_eq!(info.addr, test_addr(6380));
        assert_eq!(info.cluster_addr, test_addr(16380));
    }

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
        assert!(matches!(result, Ok(ClusterResponse::Ok)));

        assert_eq!(state.get_slot_owner(50), Some(1));
        assert_eq!(state.get_slot_owner(101), None);
    }

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
        state
            .apply_command(ClusterCommand::CompleteSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();

        assert!(!state.is_slot_migrating(42));
        assert_eq!(state.get_slot_owner(42), Some(2));
    }

    #[tokio::test]
    async fn test_demotion_detection_fires_for_self() {
        let cluster = ClusterState::new();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_demotion_detection(2);

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

        let event = rx.try_recv().unwrap();
        assert_eq!(event.demoted_node_id, 2);
        assert_eq!(event.new_primary_id, Some(1));
    }

    #[tokio::test]
    async fn test_demotion_detection_ignores_other_nodes() {
        let cluster = ClusterState::new();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_demotion_detection(1); // Watching node 1

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
        let complete = openraft::Entry::<TypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 2,
            },
            payload: EntryPayload::Normal(ClusterCommand::CompleteSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
            }),
        };
        sm.apply(vec![complete]).await.unwrap();

        let event = rx.try_recv().unwrap();
        assert_eq!(event.slot, 42);
        assert_eq!(event.source_node, 1);
        assert_eq!(event.target_node, 2);
    }
}
