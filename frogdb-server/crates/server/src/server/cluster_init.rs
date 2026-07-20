//! Cluster/Raft initialization and background tasks.

use anyhow::Result;
use frogdb_core::sync::Arc;
use frogdb_core::{
    ClusterNetworkFactory, ClusterRaft, ClusterState, ClusterStateMachine, ClusterStorage,
    MetricsRecorder, ReplicationTrackerImpl, ShardSender, SharedBroadcaster,
};
use std::time::Duration;
use tracing::{info, warn};

use crate::config::Config;
use crate::failure_detector::{
    FailureDetector, FailureDetectorConfig, spawn_failure_detector_task,
};
use crate::net::{TcpListener, spawn};
use crate::slot_migration::SlotMigrationCoordinator;

use super::util::hash_addr_to_node_id;

/// Result of the cluster initialization phase.
pub(super) struct ClusterInitResult {
    pub cluster_state: Option<Arc<ClusterState>>,
    pub node_id: Option<u64>,
    pub raft: Option<Arc<ClusterRaft>>,
    pub network_factory: Option<Arc<ClusterNetworkFactory>>,
    pub slot_migration: Option<Arc<SlotMigrationCoordinator>>,
    pub failure_detector: Option<Arc<FailureDetector>>,
    pub failure_detector_handle: Option<crate::net::JoinHandle<()>>,
    pub is_replica_flag: Arc<std::sync::atomic::AtomicBool>,
    /// Server-wide role-transition controller (the `RoleManager` handle),
    /// injected into shard workers so `REPLICAOF` can drive Role
    /// Promotion/Demotion, and used by the demotion-event consumer.
    pub role_controller: Arc<dyn frogdb_core::RoleController>,
    /// The offset atomic the cluster-bus HealthProbe answers with. In cluster
    /// mode this is always `Some` (minted here if replication did not vend one)
    /// and is shared with the runtime replica streamer so a runtime-demoted
    /// node keeps publishing its offset to the failure detector.
    pub shared_replication_offset: Option<Arc<frogdb_core::sync::AtomicU64>>,
}

/// Initialize cluster state, Raft, failure detector, and background tasks.
///
/// The Raft storage is opened upstream by recovery phase 6 and handed in here;
/// this phase only constructs the Raft instance, replays the log, and bootstraps
/// (all async, networking-entangled work the recovery seam deliberately omits).
#[allow(clippy::too_many_arguments)]
pub(super) async fn init_cluster(
    config: &Config,
    recovered_raft_storage: Option<ClusterStorage>,
    listener: &TcpListener,
    cluster_bus_listener: &Option<TcpListener>,
    shard_senders: &Arc<Vec<ShardSender>>,
    num_shards: usize,
    replication_broadcaster: &SharedBroadcaster,
    primary_replication_handler: Option<&Arc<crate::replication::PrimaryReplicationHandler>>,
    replication_tracker: &Option<Arc<ReplicationTrackerImpl>>,
    metrics_recorder: &Arc<dyn MetricsRecorder>,
    shared_replication_offset: Option<Arc<frogdb_core::sync::AtomicU64>>,
    #[cfg(not(feature = "turmoil"))] tls_manager: &Option<Arc<crate::tls::TlsManager>>,
) -> Result<ClusterInitResult> {
    // Create the shared is_replica flag and the RoleManager that owns it. This
    // single AtomicBool is shared by all shard workers, the acceptor, and all
    // connection handlers; the RoleManager is the sole writer, driving Role
    // Promotion/Demotion (REPLICAOF, failover) and the streaming lifecycle each
    // implies. Built here (before the demotion-event consumer) so both the
    // consumer and the shard workers share one controller.
    let is_replica_flag = Arc::new(std::sync::atomic::AtomicBool::new(
        config.replication.is_replica(),
    ));
    // The single offset atomic the cluster-bus HealthProbe answers with. In
    // cluster mode it must always exist so a runtime-demoted node (which may
    // have booted as a primary or standalone, i.e. with no boot-time replica
    // offset) can publish its replication offset to the failure detector the
    // same way a boot-configured replica does. Reuse the boot-time atomic when
    // present; otherwise mint one here.
    let shared_replication_offset = if config.cluster.enabled {
        Some(
            shared_replication_offset
                .unwrap_or_else(|| Arc::new(frogdb_core::sync::AtomicU64::new(0))),
        )
    } else {
        shared_replication_offset
    };
    let streamer: Arc<dyn crate::role_manager::ReplicaStreamer> =
        Arc::new(crate::role_manager::RealReplicaStreamer::new(
            config,
            shard_senders.clone(),
            num_shards,
            is_replica_flag.clone(),
            shared_replication_offset.clone(),
            #[cfg(not(feature = "turmoil"))]
            tls_manager,
        ));
    let role_manager = crate::role_manager::RoleManager::new(is_replica_flag.clone(), streamer);
    let role_handle = crate::role_manager::RoleManagerHandle::new(role_manager);
    let role_controller: Arc<dyn frogdb_core::RoleController> = Arc::new(role_handle.clone());

    let (cluster_state, node_id, raft, network_factory, slot_migration) = if config.cluster.enabled
    {
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

        // Raft storage was opened by recovery phase 6; cluster mode being
        // enabled guarantees phase 6 produced it.
        let raft_storage = recovered_raft_storage
            .expect("cluster enabled implies Raft storage opened by recovery phase 6");

        // Initialize Raft state machine with cluster state
        let cluster = ClusterState::new();
        cluster.set_self_node_id(node_id);
        let mut state_machine = ClusterStateMachine::with_state(cluster.clone());

        // Enable demotion detection unconditionally. The demotion-event consumer
        // performs a real data-path Role Demotion (via `request_demote`) during
        // failover, so it must run whenever cluster mode is on — the kill-switch
        // is `cluster.enabled` itself. `split_brain_log_enabled` gates ONLY the
        // split-brain log line inside the consumer, never the demotion behavior.
        let demotion_rx = state_machine.enable_demotion_detection(node_id);

        // Enable slot migration completion notifications for blocked client handling
        let migration_rx = state_machine.enable_migration_complete_notification();

        // Initialize Raft network factory
        #[allow(unused_mut)] // mut needed for set_connect_factory in non-turmoil builds
        let mut network_factory = ClusterNetworkFactory::new();

        // Wire up TLS connection factory for encrypted cluster bus.
        // Captures Arc<TlsManager> (not a snapshot connector) so that
        // certificate hot-reload propagates to new outgoing connections.
        #[cfg(not(feature = "turmoil"))]
        if config.tls.enabled
            && config.tls.tls_cluster
            && let Some(mgr) = tls_manager
        {
            let mgr = mgr.clone();
            let handshake_timeout =
                std::time::Duration::from_millis(config.tls.handshake_timeout_ms);
            use frogdb_core::cluster::network::{
                BoxedStream as ClusterBoxedStream, ConnectFactory as ClusterConnectFactory,
            };
            let factory: ClusterConnectFactory =
                std::sync::Arc::new(move |addr: std::net::SocketAddr| {
                    let mgr = mgr.clone();
                    Box::pin(async move {
                        let connector = mgr.connector().ok_or_else(|| {
                            std::io::Error::other("TLS client connector not configured")
                        })?;
                        let tcp = tokio::time::timeout(
                            handshake_timeout,
                            tokio::net::TcpStream::connect(addr),
                        )
                        .await
                        .map_err(|_| {
                            std::io::Error::new(std::io::ErrorKind::TimedOut, "TLS connect timeout")
                        })??;
                        let server_name = rustls::pki_types::ServerName::from(addr.ip());
                        let tls_stream =
                            connector.connect(server_name, tcp).await.map_err(|e| {
                                std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                            })?;
                        Ok(Box::new(tls_stream) as ClusterBoxedStream)
                    })
                        as std::pin::Pin<
                            Box<
                                dyn std::future::Future<
                                        Output = std::io::Result<ClusterBoxedStream>,
                                    > + Send,
                            >,
                        >
                });
            network_factory.set_connect_factory(factory);
            info!("Cluster bus TLS enabled for outgoing connections");
        }

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
                if let Err(e) = cluster
                    .apply_local(frogdb_core::cluster::ClusterCommand::AddNode { node: peer_node })
                {
                    warn!(peer_node_id = *peer_id, error = %e, "Failed to seed initial peer into cluster state");
                }
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
        if let Err(e) =
            cluster.apply_local(frogdb_core::cluster::ClusterCommand::AddNode { node: this_node })
        {
            warn!(node_id = node_id, error = %e, "Failed to seed self into cluster state");
        }
        info!(node_id = node_id, "Node added to cluster state");

        // Auto-assign slots evenly on bootstrap
        // Only the bootstrap node assigns slots to avoid conflicts
        if should_bootstrap && !initial_members.is_empty() && !cluster.all_slots_assigned() {
            let node_ids: Vec<u64> = initial_members.keys().copied().collect();
            let num_nodes = node_ids.len();
            let slots_per_node = 16384 / num_nodes;

            for (i, &nid) in node_ids.iter().enumerate() {
                // Match the inclusive ranges used by the Raft replication path
                // below so the bootstrap node and followers converge on the same
                // slot ownership.
                let start = (i * slots_per_node) as u16;
                let end = if i == num_nodes - 1 {
                    16383u16 // Last node gets remainder
                } else {
                    ((i + 1) * slots_per_node - 1) as u16
                };
                let cmd = frogdb_core::cluster::ClusterCommand::AssignSlots {
                    node_id: nid,
                    slots: vec![frogdb_core::cluster::SlotRange::new(start, end)],
                };
                if let Err(e) = cluster.apply_local(cmd) {
                    warn!(node_id = nid, error = %e, "Failed to seed slot assignment into cluster state");
                }
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
                                let net = network_factory.connect(leader_id, leader_addr);
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
                                    let net = network_factory.connect(leader_id, leader_addr);
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

        // Spawn the demotion-event consumer. It ALWAYS runs in cluster mode: it
        // performs the real data-path Role Demotion during failover. Split-brain
        // *logging* is an optional side-effect gated by `split_brain_log_enabled`
        // (modeled as `Option<SplitBrainLogger>`) — disabling the log must never
        // disable the demotion behavior.
        {
            let mut demotion_rx = demotion_rx;
            let split_brain_logger = if config.replication.split_brain_log_enabled {
                Some(SplitBrainLogger {
                    data_dir: config.persistence.data_dir.clone(),
                    broadcaster: replication_broadcaster.clone(),
                    // Backlog introspection lives on the concrete primary handler,
                    // not the frame-emit broadcaster trait. Only a primary reaches
                    // the demotion path with divergent writes to extract; a
                    // replica's handler is `None` and yields nothing.
                    primary_handler: primary_replication_handler.cloned(),
                    tracker: replication_tracker.clone(),
                    metrics: metrics_recorder.clone(),
                })
            } else {
                None
            };
            let consumer = DemotionConsumer {
                split_brain_logger,
                cluster_state: cluster.clone(),
                role_controller: role_controller.clone(),
            };
            spawn(async move {
                while let Some(event) = demotion_rx.recv().await {
                    consumer.handle(&event);
                }
            });
        }

        let cluster_state_arc = Arc::new(cluster);
        let network_factory_arc = Arc::new(network_factory_clone);

        // Build the slot migration coordinator and spawn its event dispatcher
        // for blocked-client MOVED notifications post-completion.
        let slot_migration = Arc::new(SlotMigrationCoordinator::new(
            cluster_state_arc.clone(),
            raft.clone(),
            network_factory_arc.clone(),
        ));
        slot_migration.spawn_event_dispatcher(migration_rx, shard_senders.clone(), num_shards);

        (
            Some(cluster_state_arc),
            Some(node_id),
            Some(raft),
            Some(network_factory_arc),
            Some(slot_migration),
        )
    } else {
        (None, None, None, None, None)
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

            let nf = network_factory
                .as_ref()
                .expect("network_factory must exist when cluster is enabled");
            let detector = Arc::new(FailureDetector::new(
                nid,
                detector_config,
                state_arc.clone(),
                raft_arc.clone(),
                nf.clone(),
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

    Ok(ClusterInitResult {
        cluster_state,
        node_id,
        raft,
        network_factory,
        slot_migration,
        failure_detector,
        failure_detector_handle,
        is_replica_flag,
        role_controller,
        shared_replication_offset,
    })
}

/// Split-brain divergent-write logger: the optional side-effect of a demotion.
///
/// Constructed only when `split_brain_log_enabled` is set. Holds exactly the
/// collaborators the log path needs; when logging is disabled the
/// [`DemotionConsumer`] holds `None` here and the demotion still runs.
struct SplitBrainLogger {
    data_dir: std::path::PathBuf,
    broadcaster: SharedBroadcaster,
    primary_handler: Option<Arc<crate::replication::PrimaryReplicationHandler>>,
    tracker: Option<Arc<ReplicationTrackerImpl>>,
    metrics: Arc<dyn MetricsRecorder>,
}

impl SplitBrainLogger {
    /// Emit the split-brain log line and, if this node diverged from the new
    /// primary, persist the divergent writes and bump the split-brain telemetry.
    fn log(&self, event: &frogdb_core::DemotionEvent) {
        tracing::warn!(
            demoted_node = event.demoted_node_id,
            new_primary = ?event.new_primary_id,
            epoch = event.epoch,
            "Split-brain demotion detected"
        );

        // Determine divergence boundary.
        let min_acked = self
            .tracker
            .as_ref()
            .and_then(|t| t.min_acked_offset())
            .unwrap_or(0);
        let current = self.broadcaster.current_offset();

        if current > min_acked {
            let divergent = self
                .primary_handler
                .as_ref()
                .map(|h| h.extract_divergent_writes(min_acked))
                .unwrap_or_default();
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
                    &self.data_dir,
                    header,
                    &divergent,
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
                        tracing::error!(error = %e, "Failed to write split-brain log");
                    }
                }

                frogdb_telemetry::definitions::SplitBrainEventsTotal::inc(&*self.metrics);
                frogdb_telemetry::definitions::SplitBrainOpsDiscardedTotal::inc_by(
                    &*self.metrics,
                    divergent.len() as u64,
                );
                frogdb_telemetry::definitions::SplitBrainRecoveryPending::set(&*self.metrics, 1.0);
            }
        }
    }
}

/// Consumes `DemotionEvent`s from the Raft Metadata Plane and reflects each onto
/// the local data path via a real Role Demotion.
///
/// Split-brain *logging* is an optional side-effect (`split_brain_logger`, gated
/// by config); the demotion it performs is not. This is the seam that decouples
/// cluster failover behavior from logging configuration (issue 07): whether or
/// not the logger is present, [`Self::handle`] always issues the demotion.
struct DemotionConsumer {
    split_brain_logger: Option<SplitBrainLogger>,
    cluster_state: ClusterState,
    role_controller: Arc<dyn frogdb_core::RoleController>,
}

impl DemotionConsumer {
    /// Process one demotion event: optionally log the split-brain divergence,
    /// then reconfigure the local data path to replicate from the committed new
    /// primary. The Raft plane owns the decision (ADR-0001); this only reflects
    /// it onto the data path.
    fn handle(&self, event: &frogdb_core::DemotionEvent) {
        // Split-brain logging is opt-out; the demotion below is not.
        if let Some(logger) = &self.split_brain_logger {
            logger.log(event);
        }

        if let Some(new_id) = event.new_primary_id {
            match self
                .cluster_state
                .snapshot()
                .nodes
                .get(&new_id)
                .map(|n| n.addr)
            {
                Some(addr) => {
                    tracing::warn!(
                        new_primary = %addr,
                        "Role Demotion: reconfiguring data path to replicate from new primary"
                    );
                    self.role_controller.request_demote(addr);
                }
                None => {
                    tracing::warn!(
                        new_primary = new_id,
                        "Demotion event: new primary not yet in cluster state; cannot reconfigure data path"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::DemotionEvent;
    use frogdb_core::RoleController;
    use frogdb_core::cluster::{ClusterCommand, NodeInfo};
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A `RoleController` that records every transition request, so tests can
    /// assert the demotion consumer drove the data path.
    #[derive(Default)]
    struct RecordingController {
        demotes: Mutex<Vec<SocketAddr>>,
        promotes: AtomicUsize,
    }
    impl RoleController for RecordingController {
        fn request_promote(&self) {
            self.promotes.fetch_add(1, Ordering::SeqCst);
        }
        fn request_demote(&self, primary: SocketAddr) {
            self.demotes.lock().unwrap().push(primary);
        }
    }

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    /// A cluster state that knows the new primary `new_id` at client `client_addr`.
    fn cluster_with_primary(new_id: u64, client_addr: SocketAddr) -> ClusterState {
        let cluster = ClusterState::new();
        let node = NodeInfo::new_primary(new_id, client_addr, addr("127.0.0.1:17000"));
        cluster
            .apply_local(ClusterCommand::AddNode { node })
            .expect("seed new primary");
        cluster
    }

    fn demotion_event(new_primary: Option<u64>) -> DemotionEvent {
        DemotionEvent {
            demoted_node_id: 1,
            new_primary_id: new_primary,
            epoch: 3,
        }
    }

    /// Criterion 1: with split-brain logging DISABLED, a demotion event still
    /// demotes the node (the bug this issue fixes — a disabled log line used to
    /// silently disable automatic failover demotion).
    #[test]
    fn demotion_fires_when_split_brain_log_disabled() {
        let new_id = 2u64;
        let primary = addr("127.0.0.1:7002");
        let controller = Arc::new(RecordingController::default());
        let consumer = DemotionConsumer {
            split_brain_logger: None, // logging disabled
            cluster_state: cluster_with_primary(new_id, primary),
            role_controller: controller.clone(),
        };

        consumer.handle(&demotion_event(Some(new_id)));

        assert_eq!(
            *controller.demotes.lock().unwrap(),
            vec![primary],
            "demotion must fire even with split-brain logging disabled"
        );
    }

    /// A real `SplitBrainLogger` wired with no-op collaborators. Its `log()`
    /// runs (emitting the split-brain log line) but, with a zero broadcaster
    /// offset and no primary handler, takes the no-divergence fast path — so the
    /// test exercises the logger-present arm without staging divergent writes.
    fn noop_logger() -> SplitBrainLogger {
        SplitBrainLogger {
            data_dir: std::env::temp_dir(),
            broadcaster: Arc::new(frogdb_core::NoopBroadcaster),
            primary_handler: None,
            tracker: None,
            metrics: Arc::new(frogdb_core::NoopMetricsRecorder),
        }
    }

    /// Criterion 2: with logging ENABLED (a real logger present) the demotion
    /// behavior is identical to the disabled case. Same event, same topology,
    /// only the logger presence differs — the resulting demotion must match.
    #[test]
    fn demotion_identical_whether_or_not_log_enabled() {
        let new_id = 2u64;
        let primary = addr("127.0.0.1:7002");

        let controller_off = Arc::new(RecordingController::default());
        DemotionConsumer {
            split_brain_logger: None, // logging disabled
            cluster_state: cluster_with_primary(new_id, primary),
            role_controller: controller_off.clone(),
        }
        .handle(&demotion_event(Some(new_id)));

        let controller_on = Arc::new(RecordingController::default());
        DemotionConsumer {
            split_brain_logger: Some(noop_logger()), // logging enabled
            cluster_state: cluster_with_primary(new_id, primary),
            role_controller: controller_on.clone(),
        }
        .handle(&demotion_event(Some(new_id)));

        assert_eq!(*controller_on.demotes.lock().unwrap(), vec![primary]);
        assert_eq!(
            *controller_off.demotes.lock().unwrap(),
            *controller_on.demotes.lock().unwrap(),
            "demotion behavior must be identical regardless of the log gate"
        );
    }

    /// A demotion event whose new primary is not yet in the committed topology
    /// cannot resolve an address, so no demotion is issued (and nothing panics).
    #[test]
    fn no_demotion_when_new_primary_absent_from_topology() {
        let controller = Arc::new(RecordingController::default());
        let consumer = DemotionConsumer {
            split_brain_logger: None,
            cluster_state: ClusterState::new(), // empty topology
            role_controller: controller.clone(),
        };

        consumer.handle(&demotion_event(Some(99)));

        assert!(
            controller.demotes.lock().unwrap().is_empty(),
            "unknown new primary must not trigger a demotion"
        );
    }
}
