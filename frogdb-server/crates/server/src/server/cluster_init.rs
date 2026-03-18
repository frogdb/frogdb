//! Cluster/Raft initialization and background tasks.

use anyhow::Result;
use frogdb_core::sync::Arc;
use frogdb_core::{
    ClusterNetworkFactory, ClusterRaft, ClusterState, ClusterStateMachine, ClusterStorage,
    MetricsRecorder, ReplicationTrackerImpl, ShardMessage, SharedBroadcaster,
};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::Config;
use crate::failure_detector::{
    FailureDetector, FailureDetectorConfig, spawn_failure_detector_task,
};
use crate::net::{TcpListener, spawn};

use super::util::hash_addr_to_node_id;

/// Result of the cluster initialization phase.
pub(super) struct ClusterInitResult {
    pub cluster_state: Option<Arc<ClusterState>>,
    pub node_id: Option<u64>,
    pub raft: Option<Arc<ClusterRaft>>,
    pub network_factory: Option<Arc<ClusterNetworkFactory>>,
    pub failure_detector: Option<Arc<FailureDetector>>,
    pub failure_detector_handle: Option<crate::net::JoinHandle<()>>,
    pub is_replica_flag: Arc<std::sync::atomic::AtomicBool>,
}

/// Initialize cluster state, Raft, failure detector, and background tasks.
#[allow(clippy::too_many_arguments)]
pub(super) async fn init_cluster(
    config: &Config,
    listener: &TcpListener,
    cluster_bus_listener: &Option<TcpListener>,
    shard_senders: &Arc<Vec<mpsc::Sender<ShardMessage>>>,
    num_shards: usize,
    replication_broadcaster: &SharedBroadcaster,
    replication_tracker: &Option<Arc<ReplicationTrackerImpl>>,
    metrics_recorder: &Arc<dyn MetricsRecorder>,
) -> Result<ClusterInitResult> {
    let (cluster_state, node_id, raft, network_factory) = if config.cluster.enabled {
        // Derive node_id from cluster_bus address for deterministic IDs
        let cluster_addr = config.cluster.cluster_bus_socket_addr();
        let node_id = if config.cluster.node_id != 0 {
            config.cluster.node_id
        } else {
            hash_addr_to_node_id(&cluster_addr)
        };

        info!(
            node_id = node_id,
            cluster_bus_addr = %config.cluster.cluster_bus_addr,
            "Cluster mode enabled"
        );

        // Initialize Raft storage
        let raft_path = config.persistence.data_dir.join("raft");
        let raft_storage = ClusterStorage::open(&raft_path)
            .map_err(|e| anyhow::anyhow!("Failed to open Raft storage: {}", e))?;

        // Initialize Raft state machine with cluster state
        let cluster = ClusterState::new();
        cluster.set_self_node_id(node_id);
        let mut state_machine = ClusterStateMachine::with_state(cluster.clone());

        // Enable demotion detection for split-brain logging
        let demotion_rx = if config.replication.split_brain_log_enabled {
            Some(state_machine.enable_demotion_detection(node_id))
        } else {
            None
        };

        // Enable slot migration completion notifications for blocked client handling
        let migration_rx = state_machine.enable_migration_complete_notification();

        // Initialize Raft network factory
        let network_factory = ClusterNetworkFactory::new();

        // Process initial_nodes and register addresses
        let mut initial_members: std::collections::BTreeMap<u64, openraft::BasicNode> =
            std::collections::BTreeMap::new();

        for addr_str in &config.cluster.initial_nodes {
            if let Ok(peer_cluster_addr) = addr_str.parse::<std::net::SocketAddr>() {
                let peer_node_id = hash_addr_to_node_id(&peer_cluster_addr);
                network_factory.register_node(peer_node_id, peer_cluster_addr);
                initial_members.insert(
                    peer_node_id,
                    openraft::BasicNode {
                        addr: peer_cluster_addr.to_string(),
                    },
                );
                info!(peer_node_id = peer_node_id, addr = %peer_cluster_addr, "Registered initial cluster peer");
            }
        }

        // Register this node's address
        let cluster_bus_addr = config.cluster.cluster_bus_socket_addr();
        network_factory.register_node(node_id, cluster_bus_addr);

        // Ensure this node is in initial_members
        initial_members
            .entry(node_id)
            .or_insert_with(|| openraft::BasicNode {
                addr: cluster_bus_addr.to_string(),
            });

        // Determine if this node should bootstrap (lowest node_id)
        let should_bootstrap = initial_members.keys().next().copied() == Some(node_id);

        // Create Raft config
        let raft_config = openraft::Config {
            election_timeout_min: config.cluster.election_timeout_ms,
            election_timeout_max: config.cluster.election_timeout_ms * 2,
            heartbeat_interval: config.cluster.heartbeat_interval_ms,
            ..Default::default()
        };

        // Clone network factory before passing to Raft (so we can use it later for CLUSTER MEET/FORGET)
        let network_factory_clone = network_factory.clone();

        // Initialize Raft instance
        let raft = openraft::Raft::new(
            node_id,
            Arc::new(raft_config),
            network_factory,
            raft_storage,
            state_machine,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize Raft: {}", e))?;

        info!(node_id = node_id, "Raft initialized");

        // Bootstrap Raft cluster if this is the bootstrap node
        if should_bootstrap && !initial_members.is_empty() {
            // Check if already initialized (restart case)
            let metrics = raft.metrics().borrow().clone();
            let already_initialized = metrics.membership_config.membership().nodes().count() > 0;

            if !already_initialized {
                info!(
                    node_id = node_id,
                    member_count = initial_members.len(),
                    "Bootstrapping Raft cluster"
                );
                if let Err(e) = raft.initialize(initial_members.clone()).await {
                    warn!(error = %e, "Raft initialization error (may be already initialized)");
                }
            } else {
                info!(
                    node_id = node_id,
                    "Raft already initialized, skipping bootstrap"
                );
            }
        }

        // Add all initial nodes to cluster_state
        for (peer_id, basic_node) in &initial_members {
            if let Ok(peer_cluster_addr) = basic_node.addr.parse::<std::net::SocketAddr>() {
                let client_port = peer_cluster_addr.port().saturating_sub(10000);
                let client_addr = std::net::SocketAddr::new(peer_cluster_addr.ip(), client_port);
                let peer_node = frogdb_core::cluster::NodeInfo::new_primary(
                    *peer_id,
                    client_addr,
                    peer_cluster_addr,
                );
                cluster.add_node(peer_node);
            }
        }

        // Add this node to the cluster state (ensure it's there even if not in initial_members)
        // Use effective_client_addr (respects FROGDB_CLUSTER__CLIENT_ADDR) so nodes
        // advertise reachable IPs instead of 0.0.0.0 when bound to all interfaces.
        // For OS-assigned ports (port 0), use the actual bound port.
        let client_addr = {
            let configured = config.cluster.effective_client_addr(&config.server);
            let actual_port = listener.local_addr()?.port();
            if config.server.port == 0 {
                std::net::SocketAddr::new(configured.ip(), actual_port)
            } else {
                configured
            }
        };
        let cluster_bus_addr = {
            let configured = config.cluster.cluster_bus_socket_addr();
            if let Some(cbl) = cluster_bus_listener {
                let actual = cbl.local_addr()?;
                // Use configured IP (reachable address), actual port if OS-assigned
                std::net::SocketAddr::new(
                    configured.ip(),
                    if actual.port() != configured.port() {
                        actual.port()
                    } else {
                        configured.port()
                    },
                )
            } else {
                configured
            }
        };
        let mut this_node =
            frogdb_core::cluster::NodeInfo::new_primary(node_id, client_addr, cluster_bus_addr);
        this_node.replica_priority = config.cluster.replica_priority;
        cluster.add_node(this_node);
        info!(node_id = node_id, "Node added to cluster state");

        // Auto-assign slots evenly on bootstrap
        // Only the bootstrap node assigns slots to avoid conflicts
        if should_bootstrap && !initial_members.is_empty() && !cluster.all_slots_assigned() {
            let node_ids: Vec<u64> = initial_members.keys().copied().collect();
            let num_nodes = node_ids.len();
            let slots_per_node = 16384 / num_nodes;

            for (i, &nid) in node_ids.iter().enumerate() {
                let start = i * slots_per_node;
                let end = if i == num_nodes - 1 {
                    16384 // Last node gets remainder
                } else {
                    (i + 1) * slots_per_node
                };
                cluster.assign_slots(nid, (start as u16)..(end as u16));
            }
            info!(
                node_count = num_nodes,
                slots_per_node = slots_per_node,
                "Auto-assigned slots to cluster nodes"
            );
        }

        let raft = Arc::new(raft);

        // Spawn self-registration via Raft so all nodes converge on correct peer
        // addresses. The initial add_node() calls above use guessed client ports
        // (cluster_port - 10000), which are wrong when ports are OS-assigned.
        // Each node knows its own real addresses, so it proposes AddNode for itself
        // via Raft consensus to correct the cluster state.
        // Leaders propose directly; followers forward through the cluster bus.
        {
            let raft_clone = raft.clone();
            let network_factory = network_factory_clone.clone();
            let mut self_node =
                frogdb_core::cluster::NodeInfo::new_primary(node_id, client_addr, cluster_bus_addr);
            self_node.replica_priority = config.cluster.replica_priority;
            tokio::spawn(async move {
                for attempt in 0..30 {
                    let cmd = frogdb_core::cluster::ClusterCommand::AddNode {
                        node: self_node.clone(),
                    };
                    match raft_clone.client_write(cmd).await {
                        Ok(_) => {
                            info!(
                                node_id = node_id,
                                "Registered self in cluster state via Raft"
                            );
                            return;
                        }
                        Err(e) => {
                            // Check if this is a ForwardToLeader error
                            use openraft::error::{ClientWriteError, RaftError};
                            if let RaftError::APIError(ClientWriteError::ForwardToLeader(fwd)) = &e
                                && let Some(leader_id) = fwd.leader_id
                                && let Some(leader_addr) = network_factory.get_node_addr(leader_id)
                            {
                                let net = frogdb_core::cluster::ClusterNetwork::new(
                                    leader_id,
                                    leader_addr,
                                );
                                let fwd_cmd = frogdb_core::cluster::ClusterCommand::AddNode {
                                    node: self_node.clone(),
                                };
                                if net.forward_write(fwd_cmd).await.is_ok() {
                                    info!(
                                        node_id = node_id,
                                        leader_id = leader_id,
                                        "Registered self via leader forward"
                                    );
                                    return;
                                }
                            }
                            if attempt < 29 {
                                tokio::time::sleep(Duration::from_millis(500)).await;
                            } else {
                                warn!(
                                    node_id = node_id,
                                    error = %e,
                                    "Failed to self-register after 30 attempts"
                                );
                            }
                        }
                    }
                }
            });
        }

        // Replicate bootstrap slot assignments via Raft so follower nodes receive them.
        // The local assign_slots() above only updates the bootstrap node's ClusterState;
        // followers have separate ClusterState instances and need Raft log replication.
        if should_bootstrap && !initial_members.is_empty() {
            let raft_clone = raft.clone();
            let network_factory = network_factory_clone.clone();
            let node_ids: Vec<u64> = initial_members.keys().copied().collect();
            tokio::spawn(async move {
                let num_nodes = node_ids.len();
                let slots_per_node = 16384 / num_nodes;

                for (i, &nid) in node_ids.iter().enumerate() {
                    let start = (i * slots_per_node) as u16;
                    let end = if i == num_nodes - 1 {
                        16383u16
                    } else {
                        ((i + 1) * slots_per_node - 1) as u16
                    };
                    let cmd = frogdb_core::cluster::ClusterCommand::AssignSlots {
                        node_id: nid,
                        slots: vec![frogdb_core::cluster::SlotRange::new(start, end)],
                    };

                    for attempt in 0..30 {
                        match raft_clone.client_write(cmd.clone()).await {
                            Ok(_) => {
                                info!(
                                    node_id = nid,
                                    start = start,
                                    end = end,
                                    "Replicated slot assignment via Raft"
                                );
                                break;
                            }
                            Err(e) => {
                                use openraft::error::{ClientWriteError, RaftError};
                                if let RaftError::APIError(ClientWriteError::ForwardToLeader(fwd)) =
                                    &e
                                    && let Some(leader_id) = fwd.leader_id
                                    && let Some(leader_addr) =
                                        network_factory.get_node_addr(leader_id)
                                {
                                    let net = frogdb_core::cluster::ClusterNetwork::new(
                                        leader_id,
                                        leader_addr,
                                    );
                                    if net.forward_write(cmd.clone()).await.is_ok() {
                                        info!(
                                            node_id = nid,
                                            start = start,
                                            end = end,
                                            "Replicated slot assignment via leader forward"
                                        );
                                        break;
                                    }
                                }
                                if attempt < 29 {
                                    tokio::time::sleep(Duration::from_millis(500)).await;
                                } else {
                                    warn!(
                                        node_id = nid,
                                        error = %e,
                                        "Failed to replicate slot assignment after 30 attempts"
                                    );
                                }
                            }
                        }
                    }
                }
            });
        }

        // Spawn split-brain demotion handler if enabled
        if let Some(mut demotion_rx) = demotion_rx {
            let data_dir = config.persistence.data_dir.clone();
            let broadcaster: SharedBroadcaster = replication_broadcaster.clone();
            let tracker = replication_tracker.clone();
            let metrics = metrics_recorder.clone();
            spawn(async move {
                while let Some(event) = demotion_rx.recv().await {
                    tracing::warn!(
                        demoted_node = event.demoted_node_id,
                        new_primary = ?event.new_primary_id,
                        epoch = event.epoch,
                        "Split-brain demotion detected"
                    );

                    // Determine divergence boundary
                    let min_acked = tracker
                        .as_ref()
                        .and_then(|t| t.min_acked_offset())
                        .unwrap_or(0);
                    let current = broadcaster.current_offset();

                    if current > min_acked {
                        let divergent = broadcaster.extract_divergent_writes(min_acked);
                        if !divergent.is_empty() {
                            let header = frogdb_replication::split_brain_log::SplitBrainLogHeader {
                                timestamp: String::new(),
                                old_primary: format!("{:x}", event.demoted_node_id),
                                new_primary: event
                                    .new_primary_id
                                    .map(|id| format!("{:x}", id))
                                    .unwrap_or_else(|| "unknown".to_string()),
                                epoch_old: event.epoch,
                                epoch_new: event.epoch.saturating_add(1),
                                seq_diverge_start: min_acked,
                                seq_diverge_end: current,
                                ops_discarded: divergent.len(),
                            };

                            match frogdb_replication::split_brain_log::write_log(
                                &data_dir, header, &divergent,
                            ) {
                                Ok(path) => {
                                    tracing::warn!(
                                        ops = divergent.len(),
                                        path = %path.display(),
                                        "Split-brain: {} divergent writes logged to {}",
                                        divergent.len(),
                                        path.display()
                                    );
                                }
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        "Failed to write split-brain log"
                                    );
                                }
                            }

                            frogdb_telemetry::definitions::SplitBrainEventsTotal::inc(&*metrics);
                            frogdb_telemetry::definitions::SplitBrainOpsDiscardedTotal::inc_by(
                                &*metrics,
                                divergent.len() as u64,
                            );
                            frogdb_telemetry::definitions::SplitBrainRecoveryPending::set(
                                &*metrics, 1.0,
                            );
                        }
                    }
                }
            });
        }

        // Spawn slot migration handler for blocked client MOVED responses
        {
            let cluster_for_migration = cluster.clone();
            let shard_senders_for_migration = shard_senders.clone();
            let num_shards_for_migration = num_shards;
            spawn(async move {
                let mut migration_rx = migration_rx;
                while let Some(event) = migration_rx.recv().await {
                    let target_addr = match cluster_for_migration.get_node(event.target_node) {
                        Some(node_info) => node_info.addr,
                        None => {
                            tracing::warn!(
                                slot = event.slot,
                                target_node = event.target_node,
                                "Migration complete but target node not found in cluster state"
                            );
                            continue;
                        }
                    };

                    let target_shard = event.slot as usize % num_shards_for_migration;
                    if let Some(sender) = shard_senders_for_migration.get(target_shard) {
                        let _ = sender
                            .send(ShardMessage::SlotMigrated {
                                slot: event.slot,
                                target_addr,
                            })
                            .await;
                    }
                }
            });
        }

        (
            Some(Arc::new(cluster)),
            Some(node_id),
            Some(raft),
            Some(Arc::new(network_factory_clone)),
        )
    } else {
        (None, None, None, None)
    };

    // Create failure detector early so we can pass it to shards
    let (failure_detector, failure_detector_handle) =
        if let (Some(raft_arc), Some(state_arc), Some(nid)) = (&raft, &cluster_state, node_id) {
            let detector_config = FailureDetectorConfig {
                check_interval_ms: config.cluster.heartbeat_interval_ms,
                connect_timeout_ms: config.cluster.heartbeat_interval_ms / 2,
                fail_threshold: config.cluster.fail_threshold,
                auto_failover: config.cluster.auto_failover,
            };

            let detector = Arc::new(FailureDetector::new(
                nid,
                detector_config,
                state_arc.clone(),
                raft_arc.clone(),
            ));

            info!(
                node_id = nid,
                auto_failover = config.cluster.auto_failover,
                fail_threshold = config.cluster.fail_threshold,
                "Failure detector initialized"
            );

            let handle = spawn_failure_detector_task(detector.clone());
            (Some(detector), Some(handle))
        } else {
            (None, None)
        };

    // Create shared is_replica flag. This single AtomicBool is shared by all
    // shard workers, the acceptor, and all connection handlers. REPLICAOF NO ONE
    // toggles this flag to promote from replica to primary server-wide.
    let is_replica_flag = Arc::new(std::sync::atomic::AtomicBool::new(
        config.replication.is_replica(),
    ));

    Ok(ClusterInitResult {
        cluster_state,
        node_id,
        raft,
        network_factory,
        failure_detector,
        failure_detector_handle,
        is_replica_flag,
    })
}
