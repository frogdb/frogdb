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
    CLUSTER_SLOTS, ClusterCommand, ClusterError, ClusterEvent, ClusterResponse, ClusterSnapshot,
    ConfigEpoch, NodeId, NodeInfo, SlotMigration, SlotRange, TypeConfig,
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
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            active_version: snapshot.active_version,
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
            active_version: inner.active_version.clone(),
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

    /// Check if all slots are assigned.
    pub fn all_slots_assigned(&self) -> bool {
        let inner = self.inner.read();
        inner.slot_assignment.len() == CLUSTER_SLOTS as usize
    }

    /// Get the finalized active version, if any.
    pub fn active_version(&self) -> Option<String> {
        self.inner.read().active_version.clone()
    }

    /// Override a node's reported binary version. Test-only.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_node_version(&self, node_id: NodeId, version: String) {
        if let Some(info) = self.inner.write().nodes.get_mut(&node_id) {
            info.version = version;
        }
    }

    /// Override all nodes' reported binary versions. Test-only.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_all_node_versions(&self, version: &str) {
        let mut inner = self.inner.write();
        for info in inner.nodes.values_mut() {
            info.version = version.to_string();
        }
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
                            // Demotion is only relevant when *this* node is demoted.
                            ClusterEvent::NodeDemoted {
                                demoted_node_id,
                                new_primary_id,
                                epoch,
                            } if Some(demoted_node_id) == self.self_node_id => {
                                if let Some(ref tx) = self.demotion_tx {
                                    let _ = tx.send(DemotionEvent {
                                        demoted_node_id,
                                        new_primary_id,
                                        epoch,
                                    });
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
                            // A demotion of another node: nothing to route here.
                            ClusterEvent::NodeDemoted { .. } => {}
                        }
                    }

                    results.push(response);
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
    use crate::types::{ClusterError, NodeRole};
    use std::net::SocketAddr;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    #[test]
    fn test_add_node() {
        let state = ClusterState::new();
        let node = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));

        let result = state.apply_command(ClusterCommand::AddNode { node: node.clone() });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));

        let retrieved = state.get_node(1).unwrap();
        assert_eq!(retrieved.addr, test_addr(6379));
    }

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
    async fn test_demotion_detection_not_fired_for_rejected_set_role() {
        let cluster = ClusterState::new();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_demotion_detection(2); // self = node 2

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

    // ========================================================================
    // FinalizeUpgrade tests
    // ========================================================================

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
    // Zero-coverage command tests
    // ========================================================================

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

    #[test]
    fn test_mark_node_failed_nonexistent() {
        let state = ClusterState::new();

        let result = state.apply_command(ClusterCommand::MarkNodeFailed { node_id: 999 });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

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

    #[test]
    fn test_mark_node_recovered_nonexistent() {
        let state = ClusterState::new();

        let result = state.apply_command(ClusterCommand::MarkNodeRecovered { node_id: 999 });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

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

    #[test]
    fn test_cancel_slot_migration_nonexistent() {
        let state = ClusterState::new();

        // CancelSlotMigration is infallible — cancelling a non-migrating slot succeeds
        let result = state.apply_command(ClusterCommand::CancelSlotMigration { slot: 42 });
        assert!(matches!(result, Ok((ClusterResponse::Ok, _))));
    }

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

    #[test]
    fn test_remove_node_nonexistent() {
        let state = ClusterState::new();

        let result = state.apply_command(ClusterCommand::RemoveNode { node_id: 999 });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

    #[test]
    fn test_assign_slots_node_not_found() {
        let state = ClusterState::new();

        let result = state.apply_command(ClusterCommand::AssignSlots {
            node_id: 999,
            slots: vec![SlotRange::single(50)],
        });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
    }

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

    #[test]
    fn test_mark_node_failed_missing_node_does_not_bump_epoch() {
        let state = ClusterState::new();
        let epoch_before = state.config_epoch();
        let result = state.apply_command(ClusterCommand::MarkNodeFailed { node_id: 999 });
        assert!(matches!(result, Err(ClusterError::NodeNotFound(_))));
        assert_eq!(state.config_epoch(), epoch_before);
    }

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

        let data = serde_json::to_vec(&*state.inner.read()).unwrap();
        let restored: ClusterStateInner = serde_json::from_slice(&data).unwrap();

        let original = state.inner.read();
        assert_eq!(restored.nodes, original.nodes);
        assert_eq!(restored.slot_assignment, original.slot_assignment);
        assert_eq!(restored.config_epoch, original.config_epoch);
        assert_eq!(restored.migrations, original.migrations);
    }

    #[tokio::test]
    async fn test_demotion_detection_fires_for_graceful_failover_of_self() {
        let cluster = failover_fixture();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_demotion_detection(1); // self = old primary

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

        let event = rx.try_recv().unwrap();
        assert_eq!(event.demoted_node_id, 1);
        assert_eq!(event.new_primary_id, Some(2));
    }

    #[tokio::test]
    async fn test_demotion_detection_not_fired_for_failed_failover() {
        let cluster = failover_fixture();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_demotion_detection(1);

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

    #[tokio::test]
    async fn test_demotion_detection_not_fired_for_force_failover() {
        // Force failover removes the old primary; that is not a demotion.
        let cluster = failover_fixture();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_demotion_detection(1);

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
        assert_eq!(
            events,
            vec![ClusterEvent::NodeDemoted {
                demoted_node_id: 1,
                new_primary_id: Some(2),
                epoch: state.config_epoch(),
            }]
        );
    }

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

    #[test]
    fn force_failover_emits_no_event() {
        // Force failover removes the old primary; that is not a demotion.
        let state = failover_fixture();
        let (_, events) = state
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: true,
            })
            .unwrap();
        assert!(events.is_empty());
    }

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

        let (resp, events) = state
            .apply_command(ClusterCommand::CompleteSlotMigration {
                slot: 42,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();
        assert!(matches!(resp, ClusterResponse::Ok));
        assert_eq!(
            events,
            vec![ClusterEvent::SlotMigrationCompleted {
                slot: 42,
                source_node: 1,
                target_node: 2,
            }]
        );
    }

    #[test]
    fn complete_migration_emits_no_event_on_error() {
        // No migration in progress -> Err -> no events.
        let state = two_primaries();
        let result = state.apply_command(ClusterCommand::CompleteSlotMigration {
            slot: 42,
            source_node: 1,
            target_node: 2,
        });
        assert!(matches!(result, Err(ClusterError::InvalidOperation(_))));
    }

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
}
