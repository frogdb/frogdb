//! Debug UI data providers.
//!
//! Adapters that implement the debug crate's provider traits using
//! the server's internal state types.

use std::sync::Arc;

use frogdb_cluster::types::{ClusterSnapshot, NodeFlags, NodeRole, SlotMigration};
use frogdb_core::{CLUSTER_SLOTS, ClusterState, NodeInfo};
use frogdb_debug::{
    ClusterInfoProvider, ClusterNodeSnapshot, ClusterOverviewSnapshot, MigrationSnapshot,
};

/// Adapter that provides cluster information to the debug web UI.
pub struct ClusterStateProvider {
    cluster_state: Arc<ClusterState>,
    self_node_id: Option<u64>,
}

impl ClusterStateProvider {
    pub fn new(cluster_state: Arc<ClusterState>, self_node_id: Option<u64>) -> Self {
        Self {
            cluster_state,
            self_node_id,
        }
    }
}

impl ClusterInfoProvider for ClusterStateProvider {
    fn cluster_overview(&self) -> ClusterOverviewSnapshot {
        let snapshot = self.cluster_state.snapshot();
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

        ClusterOverviewSnapshot {
            enabled: true,
            self_node_id: self.self_node_id,
            nodes,
            slots_assigned,
            total_slots: CLUSTER_SLOTS,
            config_epoch: snapshot.config_epoch,
            leader_id: snapshot.leader_id,
            active_version: snapshot.active_version.clone(),
            migrations,
        }
    }

    fn node_detail(&self, node_id: u64) -> Option<ClusterNodeSnapshot> {
        let snapshot = self.cluster_state.snapshot();
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
