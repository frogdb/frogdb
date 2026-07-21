//! Debug UI node-state provider.
//!
//! A single server-side adapter that implements the debug crate's
//! [`NodeStateProvider`] trait — the coherent seam that replaced the former
//! `ReplicationInfoProvider` / `ClientInfoProvider` / `ClusterInfoProvider`
//! trio. It reads replication identity from the replication tracker, connected
//! clients from the client registry, and cluster topology from the shared
//! [`ClusterState`], so every debug panel is fed from one place.

use std::sync::Arc;

use frogdb_cluster::types::{ClusterSnapshot, NodeFlags, NodeRole, SlotMigration};
use frogdb_core::{CLUSTER_SLOTS, ClientRegistry, ClusterState, NodeInfo, ReplicationTrackerImpl};
use frogdb_debug::{
    ClientSnapshot, ClusterNodeSnapshot, ClusterOverviewSnapshot, MigrationSnapshot,
    NodeStateProvider, ReplicationView, client_snapshots_from_registry,
};

/// Composite adapter feeding node-observable state to the debug web UI.
pub struct ServerDebugProvider {
    client_registry: Arc<ClientRegistry>,
    cluster_state: Option<Arc<ClusterState>>,
    self_node_id: Option<u64>,
    replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
    role: String,
    master_host: Option<String>,
    master_port: Option<u16>,
}

impl ServerDebugProvider {
    /// Build a provider. `cluster_state` is `Some` only in cluster mode;
    /// `replication_tracker` is `Some` only when replication is configured.
    pub fn new(
        client_registry: Arc<ClientRegistry>,
        cluster_state: Option<Arc<ClusterState>>,
        self_node_id: Option<u64>,
        replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
        role: String,
        master_host: Option<String>,
        master_port: Option<u16>,
    ) -> Self {
        Self {
            client_registry,
            cluster_state,
            self_node_id,
            replication_tracker,
            role,
            master_host,
            master_port,
        }
    }
}

impl NodeStateProvider for ServerDebugProvider {
    fn replication(&self) -> ReplicationView {
        let (connected_replicas, replication_offset) = self
            .replication_tracker
            .as_ref()
            .map(|t| (t.get_all_replicas().len(), t.current_offset()))
            .unwrap_or((0, 0));
        ReplicationView {
            role: self.role.clone(),
            connected_replicas,
            master_host: self.master_host.clone(),
            master_port: self.master_port,
            replication_offset,
        }
    }

    fn client_snapshots(&self) -> Vec<ClientSnapshot> {
        client_snapshots_from_registry(&self.client_registry)
    }

    fn cluster_overview(&self) -> Option<ClusterOverviewSnapshot> {
        let cluster_state = self.cluster_state.as_ref()?;
        let snapshot = cluster_state.snapshot();
        let nodes: Vec<ClusterNodeSnapshot> = snapshot
            .nodes
            .values()
            .map(|node| convert_node(node, &snapshot))
            .collect();
        let slots_assigned = snapshot.slot_assignment.len();
        let migrations = snapshot
            .migrations
            .values()
            .map(convert_migration)
            .collect();

        Some(ClusterOverviewSnapshot {
            enabled: true,
            self_node_id: self.self_node_id,
            nodes,
            slots_assigned,
            total_slots: CLUSTER_SLOTS,
            config_epoch: snapshot.config_epoch,
            leader_id: snapshot.leader_id,
            active_version: snapshot.active_version.clone(),
            migrations,
        })
    }

    fn node_detail(&self, node_id: u64) -> Option<ClusterNodeSnapshot> {
        let cluster_state = self.cluster_state.as_ref()?;
        let snapshot = cluster_state.snapshot();
        let node = snapshot.nodes.get(&node_id)?;
        Some(convert_node(node, &snapshot))
    }
}

/// Convert a `NodeInfo` into a `ClusterNodeSnapshot`.
///
/// Slot-range compaction is delegated to the canonical
/// [`ClusterSnapshot::get_node_slots`] in `frogdb-cluster` (the same routine
/// `CLUSTER NODES`/`CLUSTER SHARDS` use) rather than re-implemented here; the
/// only adaptation is mapping `SlotRange` to the debug UI's `(start, end)`
/// pair shape.
fn convert_node(node: &NodeInfo, snapshot: &ClusterSnapshot) -> ClusterNodeSnapshot {
    let ranges = snapshot.get_node_slots(node.id);
    let slot_count: usize = ranges.iter().map(|r| r.len()).sum();
    let slot_ranges: Vec<(u16, u16)> = ranges.iter().map(|r| (r.start, r.end)).collect();

    ClusterNodeSnapshot {
        id: node.id,
        addr: node.addr.to_string(),
        cluster_addr: node.cluster_addr.to_string(),
        role: match node.role {
            NodeRole::Primary => "primary".to_string(),
            NodeRole::Replica => "replica".to_string(),
        },
        primary_id: node.primary_id,
        config_epoch: node.config_epoch,
        flags: flags_to_strings(&node.flags),
        slot_ranges,
        slot_count,
        version: node.version.clone(),
    }
}

/// Convert `NodeFlags` to a vec of human-readable flag strings.
fn flags_to_strings(flags: &NodeFlags) -> Vec<String> {
    let mut v = Vec::new();
    if flags.fail {
        v.push("fail".to_string());
    }
    if flags.pfail {
        v.push("pfail".to_string());
    }
    if flags.handshake {
        v.push("handshake".to_string());
    }
    if flags.noaddr {
        v.push("noaddr".to_string());
    }
    v
}

/// Convert a `SlotMigration` to a `MigrationSnapshot`.
fn convert_migration(migration: &SlotMigration) -> MigrationSnapshot {
    MigrationSnapshot {
        slot: migration.slot,
        source_node: migration.source_node,
        target_node: migration.target_node,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flags_to_strings_empty() {
        let flags = NodeFlags {
            handshake: false,
            fail: false,
            pfail: false,
            noaddr: false,
        };
        assert!(flags_to_strings(&flags).is_empty());
    }

    #[test]
    fn test_flags_to_strings_all() {
        let flags = NodeFlags {
            handshake: true,
            fail: true,
            pfail: true,
            noaddr: true,
        };
        let result = flags_to_strings(&flags);
        assert_eq!(result, vec!["fail", "pfail", "handshake", "noaddr"]);
    }
}
