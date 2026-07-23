//! Cluster/Raft initialization and background tasks.

use anyhow::Result;
use frogdb_core::cluster::{ClusterWriter, Proposed};
use frogdb_core::sync::Arc;
use frogdb_core::{
    ClusterNetworkFactory, ClusterRaft, ClusterState, ClusterStateMachine, ClusterStorage,
    MetricsRecorder, ShardSender,
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
    /// The concrete `RoleManagerHandle` (not just the trait object above), so
    /// `Server::start_subsystems` can register the boot-spawned replica
    /// handler with it — a capability outside `frogdb_core::RoleController`.
    pub role_manager_handle: crate::role_manager::RoleManagerHandle,
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
    // The primary handler now owns the whole split-brain divergence window
    // (offsets + backlog), so the logger no longer needs the broadcaster or
    // the tracker.
    primary_replication_handler: Option<&Arc<crate::replication::PrimaryReplicationHandler>>,
    metrics_recorder: &Arc<dyn MetricsRecorder>,
    // The resolved `replicaof` primary address when this node boots as a
    // replica (`None` otherwise); resolved once by `init_replication` and
    // threaded here to seed the `RoleManager`'s `primary_target` — the
    // single source `ROLE`/INFO read, live, for the whole process lifetime.
    boot_primary_addr: Option<std::net::SocketAddr>,
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
    let role_manager =
        crate::role_manager::RoleManager::new(is_replica_flag.clone(), streamer, boot_primary_addr);
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

        // Under turmoil the default `plain_tcp_connect_factory` dials with a real
        // `tokio::net::TcpStream`, which is not routed through turmoil's simulated
        // network. Inject a factory that dials via `turmoil::net::TcpStream` so
        // outgoing Raft RPCs (vote / append-entries / snapshot) reach peers over
        // the simulated fabric — the cluster twin of the replica connect factory
        // in `replication_init.rs`.
        //
        // The dial is bounded by a connect timeout: the one-shot Raft RPC path
        // (`send_rpc_oneshot`) does not time out the connect itself, and a
        // turmoil network partition black-holes the SYN so a naked connect would
        // hang forever. openraft would then never observe a failure to retry, so
        // after the partition heals the surviving leader would never re-dial the
        // recovered node and the cluster would not reconverge — the timeout is
        // what lets a stuck dial fail and be retried on a fresh (now reachable)
        // connection.
        //
        // The timeout must not be *too* short, though. turmoil 0.7.1 registers a
        // connection's ephemeral port at dial time (`StreamSocket`, ref_ct=2) and
        // only reclaims it when the `TcpStream`'s read/write halves drop — which
        // are constructed *only after* SYN-ACK. A connect that is cancelled or
        // times out before SYN-ACK (guaranteed while a peer is unreachable) leaks
        // its port permanently. A very short timeout therefore turns openraft's
        // retry loop into a rapid port-leak that exhausts the host's range within
        // a sustained partition. 2s keeps the re-dial cadence low enough that the
        // leak stays far under the (widened) port pool for the brief partition
        // windows these sims inject, while still failing fast enough to retry
        // promptly once the partition heals.
        #[cfg(feature = "turmoil")]
        {
            use frogdb_core::cluster::network::{
                BoxedStream as ClusterBoxedStream, ConnectFactory as ClusterConnectFactory,
            };
            const TURMOIL_CONNECT_TIMEOUT: std::time::Duration =
                std::time::Duration::from_millis(2000);
            let factory: ClusterConnectFactory =
                std::sync::Arc::new(move |addr: std::net::SocketAddr| {
                    Box::pin(async move {
                        let stream = tokio::time::timeout(
                            TURMOIL_CONNECT_TIMEOUT,
                            turmoil::net::TcpStream::connect(addr),
                        )
                        .await
                        .map_err(|_| {
                            std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "turmoil cluster-bus connect timed out",
                            )
                        })??;
                        Ok(Box::new(stream) as ClusterBoxedStream)
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
        }

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
            let assignments = frogdb_core::cluster::even_slot_ranges(&node_ids);
            for (nid, range) in &assignments {
                let cmd = frogdb_core::cluster::ClusterCommand::AssignSlots {
                    node_id: *nid,
                    slots: vec![*range],
                };
                if let Err(e) = cluster.apply_local(cmd) {
                    warn!(node_id = *nid, error = %e, "Failed to seed slot assignment into cluster state");
                }
            }
            info!(
                node_count = assignments.len(),
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
            // The bootstrap spawns deliberately *drop* the redirect: on a
            // non-leader they retry until a leader accepts the write (directly or
            // via forward), so a `ProposeError` simply means "retry". The writer
            // owns the propose → forward decision; this loop keeps only the
            // retry-and-ignore policy.
            let writer = ClusterWriter::new(
                raft.clone(),
                Arc::new(network_factory_clone.clone()),
                Arc::new(cluster.clone()),
            );
            let mut self_node =
                frogdb_core::cluster::NodeInfo::new_primary(node_id, client_addr, cluster_bus_addr);
            self_node.replica_priority = config.cluster.replica_priority;
            tokio::spawn(async move {
                for attempt in 0..30 {
                    let cmd = frogdb_core::cluster::ClusterCommand::AddNode {
                        node: self_node.clone(),
                    };
                    match writer.propose(cmd).await {
                        Ok(Proposed::Committed(_)) => {
                            info!(
                                node_id = node_id,
                                "Registered self in cluster state via Raft"
                            );
                            return;
                        }
                        Ok(Proposed::Forwarded) => {
                            info!(node_id = node_id, "Registered self via leader forward");
                            return;
                        }
                        Err(e) => {
                            if attempt < 29 {
                                tokio::time::sleep(Duration::from_millis(500)).await;
                            } else {
                                warn!(
                                    node_id = node_id,
                                    error = ?e,
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
            // Same drop-the-redirect retry policy as the self-registration spawn;
            // the writer owns the propose → forward decision.
            let writer = ClusterWriter::new(
                raft.clone(),
                Arc::new(network_factory_clone.clone()),
                Arc::new(cluster.clone()),
            );
            let node_ids: Vec<u64> = initial_members.keys().copied().collect();
            tokio::spawn(async move {
                for (nid, range) in frogdb_core::cluster::even_slot_ranges(&node_ids) {
                    let (start, end) = (range.start, range.end);
                    let cmd = frogdb_core::cluster::ClusterCommand::AssignSlots {
                        node_id: nid,
                        slots: vec![range],
                    };

                    for attempt in 0..30 {
                        match writer.propose(cmd.clone()).await {
                            Ok(Proposed::Committed(_)) => {
                                info!(
                                    node_id = nid,
                                    start = start,
                                    end = end,
                                    "Replicated slot assignment via Raft"
                                );
                                break;
                            }
                            Ok(Proposed::Forwarded) => {
                                info!(
                                    node_id = nid,
                                    start = start,
                                    end = end,
                                    "Replicated slot assignment via leader forward"
                                );
                                break;
                            }
                            Err(e) => {
                                if attempt < 29 {
                                    tokio::time::sleep(Duration::from_millis(500)).await;
                                } else {
                                    warn!(
                                        node_id = nid,
                                        error = ?e,
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
                    // The primary handler owns the whole divergence window (offset
                    // coordinator + Replication Backlog); the logger only formats
                    // and writes the record it returns. Only a primary reaches the
                    // demotion path with divergent writes; a replica's handler is
                    // `None` and never diverges.
                    primary_handler: primary_replication_handler.cloned(),
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
        role_manager_handle: role_handle,
    })
}

/// Split-brain divergent-write logger: the optional side-effect of a demotion.
///
/// Constructed only when `split_brain_log_enabled` is set. Holds exactly the
/// collaborators the log path needs; when logging is disabled the
/// [`DemotionConsumer`] holds `None` here and the demotion still runs.
struct SplitBrainLogger {
    data_dir: std::path::PathBuf,
    primary_handler: Option<Arc<crate::replication::PrimaryReplicationHandler>>,
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

        // The primary handler owns the divergence window: it computes
        // `(start, end]` and the divergent writes from its own offset coordinator
        // and Replication Backlog. `None` means this node did not diverge (caught
        // up, or nothing in the backlog past the acked point) — the logger's only
        // job is to format the record's header and write it. A replica's handler
        // is `None` and never diverges.
        let Some(record) = self
            .primary_handler
            .as_ref()
            .and_then(|h| h.divergence_record())
        else {
            return;
        };

        let header = split_brain_header(event, &record);

        match frogdb_replication::split_brain_log::write_log(&self.data_dir, header, &record.writes)
        {
            Ok(path) => {
                tracing::warn!(
                    ops = record.writes.len(),
                    path = %path.display(),
                    "Split-brain: {} divergent writes logged to {}",
                    record.writes.len(),
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
            record.writes.len() as u64,
        );
        frogdb_telemetry::definitions::SplitBrainRecoveryPending::set(&*self.metrics, 1.0);
    }
}

/// Format the split-brain log header from the demotion event and the divergence
/// record the primary handler computed.
///
/// The metadata-plane fields (`old_primary`/`new_primary`/`epoch_*`) come from
/// the [`frogdb_core::DemotionEvent`]; the divergence-window fields are a direct
/// mapping from the [`frogdb_replication::DivergenceRecord`]
/// (`start → seq_diverge_start`, `end → seq_diverge_end`,
/// `writes.len() → ops_discarded`). Extracted so this format contract is pinned
/// by a unit test and cannot silently drift now that the window computation lives
/// in the handler.
fn split_brain_header(
    event: &frogdb_core::DemotionEvent,
    record: &frogdb_replication::DivergenceRecord,
) -> frogdb_replication::split_brain_log::SplitBrainLogHeader {
    frogdb_replication::split_brain_log::SplitBrainLogHeader {
        timestamp: String::new(),
        old_primary: format!("{:x}", event.demoted_node_id),
        new_primary: event
            .new_primary_id
            .map(|id| format!("{:x}", id))
            .unwrap_or_else(|| "unknown".to_string()),
        epoch_old: event.epoch,
        epoch_new: event.epoch.saturating_add(1),
        seq_diverge_start: record.start,
        seq_diverge_end: record.end,
        ops_discarded: record.writes.len(),
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
        fn primary_target(&self) -> Option<SocketAddr> {
            self.demotes.lock().unwrap().last().copied()
        }
        fn master_link_up(&self) -> bool {
            false
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

    /// A real `SplitBrainLogger` with no primary handler. Its `log()` runs
    /// (emitting the split-brain log line) but, with `primary_handler: None`,
    /// `divergence_record()` is never reached and it takes the no-divergence
    /// fast path — so the test exercises the logger-present arm without staging
    /// divergent writes.
    fn noop_logger() -> SplitBrainLogger {
        SplitBrainLogger {
            data_dir: std::env::temp_dir(),
            primary_handler: None,
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

    /// Regression guard for the split-brain log format contract: the header must
    /// map the divergence record's fields exactly (`start → seq_diverge_start`,
    /// `end → seq_diverge_end`, `writes.len() → ops_discarded`) and derive the
    /// metadata fields from the `DemotionEvent`. Pinned here so the mapping cannot
    /// silently drift now that the window computation moved into the handler.
    #[test]
    fn split_brain_header_maps_record_and_event_fields() {
        let event = DemotionEvent {
            demoted_node_id: 0xabc,
            new_primary_id: Some(0xdef),
            epoch: 7,
        };
        let record = frogdb_replication::DivergenceRecord {
            start: 100,
            end: 250,
            writes: vec![
                (150, bytes::Bytes::from_static(b"a")),
                (250, bytes::Bytes::from_static(b"b")),
            ],
        };

        let header = split_brain_header(&event, &record);

        // Divergence-window fields map straight from the record.
        assert_eq!(header.seq_diverge_start, record.start);
        assert_eq!(header.seq_diverge_end, record.end);
        assert_eq!(header.ops_discarded, record.writes.len());
        // Metadata-plane fields come from the event (hex-formatted).
        assert_eq!(header.old_primary, "abc");
        assert_eq!(header.new_primary, "def");
        assert_eq!(header.epoch_old, 7);
        assert_eq!(header.epoch_new, 8);
    }

    /// An unknown new primary formats as "unknown".
    #[test]
    fn split_brain_header_unknown_new_primary() {
        let event = DemotionEvent {
            demoted_node_id: 1,
            new_primary_id: None,
            epoch: 3,
        };
        let record = frogdb_replication::DivergenceRecord {
            start: 0,
            end: 10,
            writes: vec![(10, bytes::Bytes::from_static(b"x"))],
        };
        let header = split_brain_header(&event, &record);
        assert_eq!(header.new_primary, "unknown");
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

    // ========================================================================
    // Split-brain divergence lifecycle (end-to-end through the real logger)
    //
    // Issue 15 (audit gap E#5): the individual mechanisms (`divergence_record`,
    // `write_log`, `has_pending_logs`, the header mapping) are unit-tested, but
    // nothing drove the full production glue — `SplitBrainLogger::log` on a real
    // `PrimaryReplicationHandler` that actually diverged — end to end. The old
    // `noop_logger()` (`primary_handler: None`) never reached `divergence_record`,
    // so the log-file body, the `SplitBrainOpsDiscardedTotal` count, and the
    // buffer-overflow truncation path were all unexercised.
    //
    // These tests stage a real divergence on a real handler and invoke the exact
    // code the Raft `DemotionConsumer` runs in production (`logger.log` /
    // `consumer.handle`), asserting the audit file contents, the telemetry, and
    // that the demotion (which triggers discard-via-resync-from-the-new-primary)
    // is still issued.
    // ========================================================================

    use frogdb_replication::{
        LagThresholdConfig, Phase, PrimaryReplicationHandler, ReplicationBroadcaster,
        ReplicationTrackerImpl, SplitBrainBufferConfig, state::ReplicationState,
    };
    use frogdb_telemetry::PrometheusRecorder;

    /// Build a real primary handler with an enabled split-brain backlog of
    /// `buffer_entries` capacity. No I/O beyond a temp state path.
    fn split_brain_handler(
        data_dir: &std::path::Path,
        buffer_entries: usize,
    ) -> Arc<PrimaryReplicationHandler> {
        Arc::new(PrimaryReplicationHandler::new(
            ReplicationState::new(),
            data_dir.join("replication_state.json"),
            Arc::new(ReplicationTrackerImpl::new()),
            None,
            data_dir.to_path_buf(),
            LagThresholdConfig {
                threshold_bytes: 0,
                threshold_secs: 0,
                cooldown: Duration::from_secs(0),
            },
            SplitBrainBufferConfig {
                enabled: true,
                max_entries: buffer_entries,
                max_bytes: 64 * 1024 * 1024,
            },
            0,
        ))
    }

    /// Broadcast a `SET key val` through the primary path (advances the live
    /// offset AND records into the backlog, exactly as production writes do).
    fn broadcast_set(handler: &PrimaryReplicationHandler, key: &str, val: &str) -> u64 {
        handler.broadcast_command_on_shard(
            0,
            "SET",
            &[
                bytes::Bytes::from(key.to_string()),
                bytes::Bytes::from(val.to_string()),
            ],
        )
    }

    /// Register a streaming replica and pin its acked offset at `acked`, so it
    /// contributes to `min_acked` (the divergence lower bound).
    fn streaming_replica_acked_at(handler: &PrimaryReplicationHandler, addr: &str, acked: u64) {
        let session = handler.tracker().register_replica(addr.parse().unwrap());
        session.force_phase_for_test(Phase::Streaming);
        assert!(
            session.seed_acked_position(acked),
            "seed must advance the replica's acked offset"
        );
    }

    /// Locate the single `split_brain_discarded_*.log` written into `dir` and
    /// return its contents. Panics if there is not exactly one.
    fn read_split_brain_log(dir: &std::path::Path) -> String {
        let mut matches: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .map(|n| n.to_string_lossy().starts_with("split_brain_discarded_"))
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "expected exactly one split-brain log, found {:?}",
            matches
        );
        std::fs::read_to_string(matches.pop().unwrap()).unwrap()
    }

    /// Full split-brain lifecycle through the production `DemotionConsumer`:
    ///
    /// A primary accepts writes the cluster acknowledged (up to `acked`), then —
    /// during the partition modelled here by the replica no longer advancing its
    /// ACK — accepts further "divergent" writes past that acked point. On the
    /// committed demotion the consumer must:
    ///   1. capture exactly the divergent writes (offset > acked) into the audit
    ///      log, excluding the acknowledged ones (which the new primary retains);
    ///   2. stamp the divergence window (`seq_diverge_start`/`seq_diverge_end`)
    ///      and `ops_discarded` to match the record;
    ///   3. bump `SplitBrainOpsDiscardedTotal` / `SplitBrainEventsTotal` and raise
    ///      the `SplitBrainRecoveryPending` gauge; and
    ///   4. still issue the Role Demotion — the reconciliation step that discards
    ///      the divergent writes by resyncing this node from the new primary.
    ///
    /// (4) only *initiates* the discard; the data-path replacement is the resync
    /// path — see `test_broadcast_lag_disconnect_and_resync` in
    /// `tests/integration_replication.rs`, which pins that a partial resync
    /// converges the live keyspace while a full resync stages a checkpoint that
    /// installs on the next boot. The Raft transport that would normally deliver
    /// the `DemotionEvent` is stubbed by invoking `consumer.handle` directly with
    /// the event the metadata plane commits.
    #[test]
    fn split_brain_lifecycle_captures_audit_and_initiates_discard() {
        let dir = tempfile::tempdir().unwrap();
        let handler = split_brain_handler(dir.path(), 1000);

        // Writes the cluster acknowledged.
        broadcast_set(&handler, "acked_key_a", "va");
        let acked = broadcast_set(&handler, "acked_key_b", "vb");
        // A streaming replica pinned at the acked head fixes the divergence floor.
        streaming_replica_acked_at(&handler, "127.0.0.1:6390", acked);

        // Divergent writes committed past the acked point (the "both sides keep
        // writing" phase of the split).
        let mut divergent_keys = Vec::new();
        let mut last = acked;
        for i in 0..5 {
            let key = format!("div_key_{i}");
            last = broadcast_set(&handler, &key, &format!("div_val_{i}"));
            divergent_keys.push(key);
        }
        let current = last;

        // Sanity: the handler agrees it diverged over exactly (acked, current].
        let record = handler.divergence_record().expect("primary diverged");
        assert_eq!(record.start, acked);
        assert_eq!(record.end, current);
        assert_eq!(record.writes.len(), divergent_keys.len());

        // Drive the real production consumer with a committed demotion event.
        let new_id = 0xB0Bu64;
        let new_primary_addr = addr("127.0.0.1:7002");
        let recorder = Arc::new(PrometheusRecorder::new());
        let controller = Arc::new(RecordingController::default());
        let consumer = DemotionConsumer {
            split_brain_logger: Some(SplitBrainLogger {
                data_dir: dir.path().to_path_buf(),
                primary_handler: Some(handler.clone()),
                metrics: recorder.clone(),
            }),
            cluster_state: cluster_with_primary(new_id, new_primary_addr),
            role_controller: controller.clone(),
        };

        let event = DemotionEvent {
            demoted_node_id: 0xA,
            new_primary_id: Some(new_id),
            epoch: 41,
        };
        consumer.handle(&event);

        // ---- (1)+(2) audit file: exactly the divergent writes + the window.
        assert!(
            frogdb_replication::split_brain_log::has_pending_logs(dir.path()),
            "a pending split-brain log must exist after a divergent demotion"
        );
        let body = read_split_brain_log(dir.path());
        assert!(body.contains(&format!("seq_diverge_start={acked}")));
        assert!(body.contains(&format!("seq_diverge_end={current}")));
        assert!(body.contains(&format!("ops_discarded={}", divergent_keys.len())));
        assert!(body.contains("old_primary=a")); // 0xA hex
        assert!(body.contains("new_primary=b0b")); // 0xB0B hex
        assert!(body.contains("epoch_old=41"));
        assert!(body.contains("epoch_new=42"));
        // The divergent writes are present...
        for key in &divergent_keys {
            assert!(
                body.contains(key),
                "audit log missing divergent write {key}"
            );
        }
        // ...and the acknowledged writes are NOT surrendered (the new primary
        // already holds them; only writes past the acked floor are discarded).
        assert!(
            !body.contains("acked_key_a") && !body.contains("acked_key_b"),
            "acknowledged writes must not appear in the discard audit"
        );

        // ---- (3) telemetry matches the discard count exactly.
        assert_eq!(
            recorder.counter_value("frogdb_split_brain_ops_discarded_total"),
            Some(divergent_keys.len() as u64),
            "SplitBrainOpsDiscardedTotal must equal the number of discarded ops"
        );
        assert_eq!(
            recorder.counter_value("frogdb_split_brain_events_total"),
            Some(1),
            "one split-brain event was recorded"
        );
        assert_eq!(
            recorder.gauge_value("frogdb_split_brain_recovery_pending"),
            Some(1.0),
            "recovery-pending gauge must be raised while a log awaits processing"
        );

        // ---- (4) the demotion (discard-via-resync) is still issued.
        assert_eq!(
            *controller.demotes.lock().unwrap(),
            vec![new_primary_addr],
            "demotion to the new primary must fire so the divergent node resyncs \
             (discarding its divergent writes)"
        );
    }

    /// Buffer-overflow / truncation behavior (issue 15, audit gap E#5: "the
    /// truncation path untested"). When the divergence exceeds
    /// `split_brain_buffer_size`, the ring buffer evicts oldest-first (FIFO), so
    /// the audit captures only the retained *tail* of the divergent writes.
    ///
    /// This test pins the *actual* designed behavior and its boundary honestly:
    ///   - Well-defined, no corruption: every retained RESP entry is intact and
    ///     the retained set is the newest `max_entries` writes, in order.
    ///   - NOT fully observable: there is no truncation marker in the file, and
    ///     `ops_discarded` counts only the *retained* entries — it undercounts
    ///     the true divergence span `(seq_diverge_start, seq_diverge_end]`. The
    ///     evicted oldest writes are silently absent from the audit.
    ///
    /// The observable signal that truncation occurred is indirect: the first
    /// retained offset sits strictly above `seq_diverge_start`, leaving an
    /// unaudited gap. Documented here rather than asserted as "correct" because
    /// it is a real limitation of the current audit (a follow-up could emit a
    /// truncation marker / dedicated metric).
    #[test]
    fn split_brain_buffer_overflow_truncates_audit_silently() {
        let dir = tempfile::tempdir().unwrap();
        const CAP: usize = 8;
        let handler = split_brain_handler(dir.path(), CAP);

        // No streaming replica ⇒ divergence floor is 0, so *every* write is
        // divergent — but the buffer only retains the last CAP of them.
        const TOTAL: usize = 20;
        let mut last = 0u64;
        for i in 0..TOTAL {
            last = broadcast_set(
                &handler,
                &format!("of_key_{i:02}"),
                &format!("of_val_{i:02}"),
            );
        }
        let current = last;

        let record = handler.divergence_record().expect("primary diverged");
        assert_eq!(record.start, 0, "no acked replica ⇒ zero floor");
        assert_eq!(record.end, current);
        // The window spans all TOTAL writes, but only CAP survived the buffer.
        assert_eq!(
            record.writes.len(),
            CAP,
            "overflow retains exactly the newest max_entries writes"
        );
        // Truncation is silent: the oldest retained offset is strictly above the
        // divergence start, so [start, oldest_retained) is an unaudited gap.
        let oldest_retained = record.writes.first().map(|(o, _)| *o).unwrap();
        assert!(
            oldest_retained > record.start,
            "overflow leaves an unaudited gap below the oldest retained write"
        );

        let recorder = Arc::new(PrometheusRecorder::new());
        let logger = SplitBrainLogger {
            data_dir: dir.path().to_path_buf(),
            primary_handler: Some(handler.clone()),
            metrics: recorder.clone(),
        };
        logger.log(&DemotionEvent {
            demoted_node_id: 1,
            new_primary_id: Some(2),
            epoch: 5,
        });

        let body = read_split_brain_log(dir.path());

        // The newest CAP writes are retained, intact and in order...
        for i in (TOTAL - CAP)..TOTAL {
            assert!(
                body.contains(&format!("of_key_{i:02}")),
                "retained tail must contain of_key_{i:02}"
            );
        }
        // ...the evicted oldest writes are silently dropped (no truncation marker).
        for i in 0..(TOTAL - CAP) {
            assert!(
                !body.contains(&format!("of_key_{i:02}")),
                "evicted write of_key_{i:02} must be absent from the audit"
            );
        }
        assert!(
            !body.to_lowercase().contains("truncat"),
            "current audit emits NO truncation marker (documented boundary)"
        );

        // The window header still reports the *true* span (start=0..current),
        // while ops_discarded reflects only the retained tail — so the header
        // and the body disagree on the true divergence size. This is the
        // observability gap: ops_discarded undercounts the real divergence.
        assert!(body.contains("seq_diverge_start=0"));
        assert!(body.contains(&format!("seq_diverge_end={current}")));
        assert!(body.contains(&format!("ops_discarded={CAP}")));
        assert_eq!(
            recorder.counter_value("frogdb_split_brain_ops_discarded_total"),
            Some(CAP as u64),
            "telemetry counts only the retained (audited) ops, not the true \
             divergence span — the truncation is not separately observable"
        );
    }
}
