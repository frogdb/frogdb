//! Raft state machine for cluster coordination.

use std::io::Cursor;
use openraft::storage::RaftStateMachine;
use openraft::{EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership};
use tokio::sync::mpsc;
use crate::state::{ClusterState, ClusterStateInner, DemotionEvent, SlotMigrationCompleteEvent};
use crate::types::{ClusterCommand, ClusterResponse, NodeId, NodeRole, TypeConfig};

pub struct ClusterStateMachine {
    state: ClusterState,
    self_node_id: Option<NodeId>,
    demotion_tx: Option<mpsc::UnboundedSender<DemotionEvent>>,
    migration_complete_tx: Option<mpsc::UnboundedSender<SlotMigrationCompleteEvent>>,
}

impl ClusterStateMachine {
    pub fn new() -> Self { Self { state: ClusterState::new(), self_node_id: None, demotion_tx: None, migration_complete_tx: None } }
    pub fn with_state(state: ClusterState) -> Self { Self { state, self_node_id: None, demotion_tx: None, migration_complete_tx: None } }
    pub fn enable_demotion_detection(&mut self, self_node_id: NodeId) -> mpsc::UnboundedReceiver<DemotionEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.self_node_id = Some(self_node_id);
        self.demotion_tx = Some(tx);
        rx
    }
    pub fn enable_migration_complete_notification(&mut self) -> mpsc::UnboundedReceiver<SlotMigrationCompleteEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.migration_complete_tx = Some(tx);
        rx
    }
    pub fn state(&self) -> &ClusterState { &self.state }
}

impl Default for ClusterStateMachine { fn default() -> Self { Self::new() } }

impl RaftStateMachine<TypeConfig> for ClusterStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, openraft::BasicNode>), StorageError<NodeId>> {
        let inner = self.state.inner.read();
        Ok((inner.last_applied_log, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClusterResponse>, StorageError<NodeId>> where I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send {
        let mut results = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            match entry.payload {
                EntryPayload::Blank => { results.push(ClusterResponse::Ok); }
                EntryPayload::Normal(cmd) => {
                    if let Some(self_id) = self.self_node_id && let ClusterCommand::SetRole { node_id, role: NodeRole::Replica, primary_id } = &cmd && *node_id == self_id && let Some(ref tx) = self.demotion_tx {
                        let epoch = self.state.config_epoch();
                        let _ = tx.send(DemotionEvent { demoted_node_id: self_id, new_primary_id: *primary_id, epoch });
                    }
                    let migration_event = if let ClusterCommand::CompleteSlotMigration { slot, source_node, target_node } = &cmd { Some(SlotMigrationCompleteEvent { slot: *slot, source_node: *source_node, target_node: *target_node }) } else { None };
                    let result = self.state.apply_command(cmd).unwrap_or_else(|e| { tracing::warn!(error = %e, "Failed to apply cluster command"); ClusterResponse::Error(e.to_string()) });
                    if let Some(event) = migration_event && !matches!(result, ClusterResponse::Error(_)) && let Some(ref tx) = self.migration_complete_tx { let _ = tx.send(event); }
                    results.push(result);
                }
                EntryPayload::Membership(membership) => { let mut inner = self.state.inner.write(); inner.last_membership = StoredMembership::new(Some(log_id), membership); results.push(ClusterResponse::Ok); }
            }
            self.state.inner.write().last_applied_log = Some(log_id);
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder { ClusterStateMachine { state: self.state.clone(), self_node_id: None, demotion_tx: None, migration_complete_tx: None } }
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> { Ok(Box::new(Cursor::new(Vec::new()))) }

    async fn install_snapshot(&mut self, meta: &SnapshotMeta<NodeId, openraft::BasicNode>, snapshot: Box<Cursor<Vec<u8>>>) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let snapshot_state: ClusterStateInner = serde_json::from_slice(&data).map_err(|e| StorageError::from_io_error(openraft::ErrorSubject::Snapshot(Some(meta.signature())), openraft::ErrorVerb::Read, std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
        let mut inner = self.state.inner.write();
        *inner = snapshot_state;
        inner.last_applied_log = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        tracing::info!(last_log_id = ?meta.last_log_id, "Installed cluster state snapshot");
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.state.inner.read();
        let Some(last_applied_log) = inner.last_applied_log else { return Ok(None); };
        let data = serde_json::to_vec(&*inner).map_err(|e| StorageError::from_io_error(openraft::ErrorSubject::Snapshot(None), openraft::ErrorVerb::Write, std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
        let snapshot = Snapshot { meta: SnapshotMeta { last_log_id: Some(last_applied_log), last_membership: inner.last_membership.clone(), snapshot_id: format!("snapshot-{}", last_applied_log.index) }, snapshot: Box::new(Cursor::new(data)) };
        Ok(Some(snapshot))
    }
}

impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for ClusterStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let inner = self.state.inner.read();
        let last_applied_log = inner.last_applied_log;
        let data = serde_json::to_vec(&*inner).map_err(|e| StorageError::from_io_error(openraft::ErrorSubject::Snapshot(None), openraft::ErrorVerb::Write, std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
        let snapshot_id = match last_applied_log { Some(log_id) => format!("snapshot-{}", log_id.index), None => "snapshot-0".to_string() };
        let snapshot = Snapshot { meta: SnapshotMeta { last_log_id: last_applied_log, last_membership: inner.last_membership.clone(), snapshot_id }, snapshot: Box::new(Cursor::new(data)) };
        tracing::info!(last_log_id = ?last_applied_log, "Built cluster state snapshot");
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ClusterCommand, NodeInfo, NodeRole, SlotRange};
    use openraft::EntryPayload;
    use std::net::SocketAddr;

    fn test_addr(port: u16) -> SocketAddr { format!("127.0.0.1:{}", port).parse().unwrap() }

    #[tokio::test]
    async fn test_demotion_detection_fires_for_self() {
        let cluster = ClusterState::new();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_demotion_detection(2);
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        sm.state().apply_command(ClusterCommand::AddNode { node: node1 }).unwrap();
        sm.state().apply_command(ClusterCommand::AddNode { node: node2 }).unwrap();
        let entry = openraft::Entry::<TypeConfig> { log_id: openraft::LogId { leader_id: openraft::CommittedLeaderId::new(1, 1), index: 1 }, payload: EntryPayload::Normal(ClusterCommand::SetRole { node_id: 2, role: NodeRole::Replica, primary_id: Some(1) }) };
        sm.apply(vec![entry]).await.unwrap();
        let event = rx.try_recv().unwrap();
        assert_eq!(event.demoted_node_id, 2);
        assert_eq!(event.new_primary_id, Some(1));
    }

    #[tokio::test]
    async fn test_demotion_detection_ignores_other_nodes() {
        let cluster = ClusterState::new();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_demotion_detection(1);
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        sm.state().apply_command(ClusterCommand::AddNode { node: node1 }).unwrap();
        sm.state().apply_command(ClusterCommand::AddNode { node: node2 }).unwrap();
        let entry = openraft::Entry::<TypeConfig> { log_id: openraft::LogId { leader_id: openraft::CommittedLeaderId::new(1, 1), index: 1 }, payload: EntryPayload::Normal(ClusterCommand::SetRole { node_id: 2, role: NodeRole::Replica, primary_id: Some(1) }) };
        sm.apply(vec![entry]).await.unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_migration_complete_event_fires() {
        let cluster = ClusterState::new();
        let mut sm = ClusterStateMachine::with_state(cluster);
        let mut rx = sm.enable_migration_complete_notification();
        let node1 = NodeInfo::new_primary(1, test_addr(6379), test_addr(16379));
        let node2 = NodeInfo::new_primary(2, test_addr(6380), test_addr(16380));
        sm.state().apply_command(ClusterCommand::AddNode { node: node1 }).unwrap();
        sm.state().apply_command(ClusterCommand::AddNode { node: node2 }).unwrap();
        sm.state().apply_command(ClusterCommand::AssignSlots { node_id: 1, slots: vec![SlotRange::single(42)] }).unwrap();
        let begin = openraft::Entry::<TypeConfig> { log_id: openraft::LogId { leader_id: openraft::CommittedLeaderId::new(1, 1), index: 1 }, payload: EntryPayload::Normal(ClusterCommand::BeginSlotMigration { slot: 42, source_node: 1, target_node: 2 }) };
        sm.apply(vec![begin]).await.unwrap();
        assert!(rx.try_recv().is_err());
        let complete = openraft::Entry::<TypeConfig> { log_id: openraft::LogId { leader_id: openraft::CommittedLeaderId::new(1, 1), index: 2 }, payload: EntryPayload::Normal(ClusterCommand::CompleteSlotMigration { slot: 42, source_node: 1, target_node: 2 }) };
        sm.apply(vec![complete]).await.unwrap();
        let event = rx.try_recv().unwrap();
        assert_eq!(event.slot, 42);
        assert_eq!(event.source_node, 1);
        assert_eq!(event.target_node, 2);
    }
}
