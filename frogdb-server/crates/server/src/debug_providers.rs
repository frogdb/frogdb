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
use frogdb_telemetry::LiveMode;

use crate::role_manager::RoleManagerHandle;

/// Composite adapter feeding node-observable state to the debug web UI.
pub struct ServerDebugProvider {
    client_registry: Arc<ClientRegistry>,
    cluster_state: Option<Arc<ClusterState>>,
    self_node_id: Option<u64>,
    replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
    /// Live operating mode, shared with `/status`; follows runtime role changes.
    mode: LiveMode,
    /// Owns the live primary target, so master host/port track runtime
    /// `REPLICAOF` promote/demote instead of freezing at the boot config.
    role_manager: RoleManagerHandle,
}

impl ServerDebugProvider {
    /// Build a provider. `cluster_state` is `Some` only in cluster mode;
    /// `replication_tracker` is `Some` only when replication is configured.
    pub fn new(
        client_registry: Arc<ClientRegistry>,
        cluster_state: Option<Arc<ClusterState>>,
        self_node_id: Option<u64>,
        replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
        mode: LiveMode,
        role_manager: RoleManagerHandle,
    ) -> Self {
        Self {
            client_registry,
            cluster_state,
            self_node_id,
            replication_tracker,
            mode,
            role_manager,
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
        let role = self.mode.current();
        // Report the primary only while the live replica flag is set (not the
        // cluster-label string), matching how INFO/ROLE gate master_host/master_port.
        let master = self
            .mode
            .is_replica()
            .then(|| self.role_manager.primary_target())
            .flatten();
        ReplicationView {
            role: role.to_string(),
            connected_replicas,
            master_host: master.map(|addr| addr.ip().to_string()),
            master_port: master.map(|addr| addr.port()),
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

    /// Issue 12: the debug node-state provider must draw role and master
    /// host/port from the shared live source (the RoleManager's flag + primary
    /// target), so a runtime `REPLICAOF` promote/demote is reflected instead of
    /// freezing at the boot config.
    #[test]
    fn replication_view_tracks_live_role_and_primary_target() {
        use crate::role_manager::{ReplicaStream, ReplicaStreamer, RoleManager};
        use frogdb_core::RoleController;
        use std::net::SocketAddr;
        use std::sync::atomic::AtomicBool;

        struct NoopStream;
        impl ReplicaStream for NoopStream {}
        struct NoopStreamer;
        impl ReplicaStreamer for NoopStreamer {
            fn start(&self, _primary: SocketAddr) -> Box<dyn ReplicaStream> {
                Box::new(NoopStream)
            }
        }

        let flag = Arc::new(AtomicBool::new(false));
        let handle =
            RoleManagerHandle::new(RoleManager::new(flag.clone(), Arc::new(NoopStreamer), None));
        let provider = ServerDebugProvider::new(
            Arc::new(ClientRegistry::new()),
            None,
            None,
            None,
            LiveMode::new(false, flag.clone(), "primary"),
            handle.clone(),
        );

        // Boot: primary, no master reported.
        let view = provider.replication();
        assert_eq!(view.role, "primary");
        assert!(view.master_host.is_none());
        assert!(view.master_port.is_none());

        // Runtime demotion flips the shared flag and records the target.
        let primary: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        handle.request_demote(primary);
        let view = provider.replication();
        assert_eq!(view.role, "replica");
        assert_eq!(view.master_host.as_deref(), Some("127.0.0.1"));
        assert_eq!(view.master_port, Some(7000));

        // Promotion clears both back to the primary shape.
        handle.request_promote();
        let view = provider.replication();
        assert_eq!(view.role, "primary");
        assert!(view.master_host.is_none());
        assert!(view.master_port.is_none());
    }

    /// Issue 12 follow-up: in cluster mode `LiveMode::current()` reports
    /// `"cluster"`, but a boot-configured replica must still surface its
    /// primary target — the master gate reads the live replica flag directly,
    /// not the cluster-labeled role string.
    #[test]
    fn replication_view_reports_master_in_cluster_mode_when_replica_flag_set() {
        use crate::role_manager::{ReplicaStream, ReplicaStreamer, RoleManager};
        use frogdb_core::RoleController;
        use std::net::SocketAddr;
        use std::sync::atomic::AtomicBool;

        struct NoopStream;
        impl ReplicaStream for NoopStream {}
        struct NoopStreamer;
        impl ReplicaStreamer for NoopStreamer {
            fn start(&self, _primary: SocketAddr) -> Box<dyn ReplicaStream> {
                Box::new(NoopStream)
            }
        }

        let flag = Arc::new(AtomicBool::new(false));
        let handle =
            RoleManagerHandle::new(RoleManager::new(flag.clone(), Arc::new(NoopStreamer), None));
        let provider = ServerDebugProvider::new(
            Arc::new(ClientRegistry::new()),
            None,
            None,
            None,
            LiveMode::new(true, flag.clone(), "primary"),
            handle.clone(),
        );

        let primary: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        handle.request_demote(primary);

        let view = provider.replication();
        assert_eq!(view.role, "cluster");
        assert_eq!(view.master_host.as_deref(), Some("127.0.0.1"));
        assert_eq!(view.master_port, Some(7000));
    }
}
