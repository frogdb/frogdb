//! Debug UI data providers.
//!
//! Adapters that implement the debug crate's provider traits using
//! the server's internal state types.

use std::collections::BTreeMap;
use std::sync::Arc;

use frogdb_cluster::types::{MigrationState, NodeFlags, NodeRole, SlotMigration};
use frogdb_core::{CLUSTER_SLOTS, ClusterState, NodeId, NodeInfo};
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
            .map(|node| convert_node(node, &snapshot.slot_assignment))
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
        Some(convert_node(node, &snapshot.slot_assignment))
    }
}

/// Convert a `NodeInfo` into a `ClusterNodeSnapshot`.
fn convert_node(node: &NodeInfo, slot_assignment: &BTreeMap<u16, NodeId>) -> ClusterNodeSnapshot {
    // Collect slots assigned to this node and compact into ranges.
    let mut slots: Vec<u16> = slot_assignment
        .iter()
        .filter(|(_, owner)| **owner == node.id)
        .map(|(&slot, _)| slot)
        .collect();
    slots.sort_unstable();
    let slot_ranges = compact_slot_ranges(&slots);
    let slot_count = slots.len();

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

/// Compact a sorted list of slot numbers into (start, end) inclusive ranges.
fn compact_slot_ranges(slots: &[u16]) -> Vec<(u16, u16)> {
    if slots.is_empty() {
        return Vec::new();
    }
    let mut ranges = Vec::new();
    let mut start = slots[0];
    let mut end = slots[0];
    for &slot in &slots[1..] {
        if slot == end + 1 {
            end = slot;
        } else {
            ranges.push((start, end));
            start = slot;
            end = slot;
        }
    }
    ranges.push((start, end));
    ranges
}

/// Convert a `SlotMigration` to a `MigrationSnapshot`.
fn convert_migration(migration: &SlotMigration) -> MigrationSnapshot {
    MigrationSnapshot {
        slot: migration.slot,
        source_node: migration.source_node,
        target_node: migration.target_node,
        state: match migration.state {
            MigrationState::Initiated => "initiated".to_string(),
            MigrationState::Migrating => "migrating".to_string(),
            MigrationState::Completing => "completing".to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_slot_ranges_empty() {
        assert_eq!(compact_slot_ranges(&[]), Vec::<(u16, u16)>::new());
    }

    #[test]
    fn test_compact_slot_ranges_single() {
        assert_eq!(compact_slot_ranges(&[5]), vec![(5, 5)]);
    }

    #[test]
    fn test_compact_slot_ranges_contiguous() {
        assert_eq!(compact_slot_ranges(&[0, 1, 2, 3, 4]), vec![(0, 4)]);
    }

    #[test]
    fn test_compact_slot_ranges_gaps() {
        assert_eq!(
            compact_slot_ranges(&[0, 1, 2, 5, 6, 10]),
            vec![(0, 2), (5, 6), (10, 10)]
        );
    }

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
