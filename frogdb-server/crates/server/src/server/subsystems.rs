//! Subsystem startup and shutdown logic extracted from `run_until()`.

use frogdb_cluster::version_gate;
use frogdb_core::ShardMessage;
use frogdb_core::clock;
use frogdb_core::sync::{Arc, AtomicU64};
use frogdb_core::{ClusterState, MetricsRecorder};
use frogdb_debug::{ConfigEntry, DebugState, ServerInfo};
use frogdb_telemetry::{LiveMode, StatusCollector, SystemMetricsCollector};
use std::time::Duration;
use tracing::{error, info};

use crate::acceptor::{Acceptor, AcceptorContext, PortSpec};
use crate::admin::handlers::AdminState;
use crate::connection::deps::{AdminDeps, ClusterDeps, CoreDeps, ObservabilityDeps};
use crate::net::{JoinHandle, spawn};
use crate::observability_server::ObservabilityServer;
use crate::replication::{ReplicaCommandExecutor, consume_frames};

use crate::config::MemoryConfigExt;

use super::Server;

use anyhow::Result;

/// Handles for all spawned subsystem tasks.
///
/// Collected during startup so shutdown can cleanly stop everything.
/// How often the backlog-TTL ticker runs. Matches Redis's 1 Hz
/// `replicationCron`: the TTL is measured in seconds, so a second of slack on a
/// timer whose default is an hour costs nothing.
const BACKLOG_TTL_TICK: Duration = Duration::from_secs(1);

pub(super) struct SubsystemHandles {
    pub http_server: Option<JoinHandle<()>>,
    pub system_collector: Option<JoinHandle<()>>,
    pub cluster_bus: Option<JoinHandle<()>>,
    pub replica: Option<(JoinHandle<()>, JoinHandle<()>)>,
    pub acceptor: JoinHandle<()>,
    pub admin_acceptor: Option<JoinHandle<()>>,
    #[cfg(not(feature = "turmoil"))]
    pub tls_acceptor: Option<JoinHandle<()>>,
    /// Certificate file watcher; `None` when TLS or `tls.watch-certs` is off.
    #[cfg(not(feature = "turmoil"))]
    pub cert_watcher: Option<JoinHandle<()>>,
    pub failure_detector: Option<JoinHandle<()>>,
    /// Handle to the shard-worker supervisor. Completes once every shard worker
    /// has terminated, so shutdown awaits it in place of the per-worker handles.
    pub shard_supervisor: Option<JoinHandle<()>>,
    pub periodic_sync_handle: Option<JoinHandle<()>>,
    pub periodic_snapshot_handle: Option<JoinHandle<()>>,
    /// Ticks the replication backlog's idle TTL (Redis `replicationCron`'s
    /// backlog-freeing job). Present on every role, because the primary handler
    /// it drives is built on every role.
    pub backlog_ttl_handle: Option<JoinHandle<()>>,
}

impl Server {
    /// The live handshake timeout to hand to accept loops and connect
    /// factories.
    ///
    /// When TLS is enabled this is the [`crate::tls_runtime::TlsRuntimeHandle`]'s
    /// shared handle, so a runtime change reaches every TLS path. When TLS is
    /// off there is no handshake path to read it, but the acceptor context
    /// still needs a value, so a detached one carrying the configured default
    /// is used.
    #[cfg(not(feature = "turmoil"))]
    fn tls_handshake_timeout(&self) -> crate::tls_runtime::HandshakeTimeout {
        match self.tls_runtime {
            Some(ref tls_rt) => tls_rt.handshake_timeout(),
            None => crate::tls_runtime::HandshakeTimeout::new(self.config.tls.handshake_timeout_ms),
        }
    }

    /// Start all subsystems and return handles for later shutdown.
    pub(super) fn start_subsystems(&mut self) -> Result<SubsystemHandles> {
        // Capture server start time
        let start_time = clock::now();

        // Live operating mode shared by the status collector and (when HTTP is
        // enabled) the debug node-state provider. Cluster mode is config-static;
        // the primary/replica axis follows the shared role flag so a runtime
        // `REPLICAOF` promote/demote is reflected on every surface. A boot
        // replica promoted via `REPLICAOF NO ONE` is a real primary, so both a
        // `primary` and a `replica` config map to the `"primary"` non-replica
        // label to match INFO's `role:master`.
        let non_replica_label = if self.config.replication.is_standalone() {
            "standalone"
        } else {
            "primary"
        };
        let mode = LiveMode::new(
            self.config.cluster.enabled,
            self.is_replica_flag.clone(),
            non_replica_label,
        );

        // Single hot-shard collector, shared by every surface that reports
        // per-shard load: FROGDB.HOTSHARDS (through the core `ObservabilityConfig`
        // seam), the `/status` JSON's `hot_shards` section, and the debug web UI
        // panel. One collector means the three can never disagree, and its
        // thresholds are the ConfigManager's own shared atomics, adopted here,
        // so `CONFIG SET hotshards-*` retunes all three at once and CONFIG GET
        // reports what the collector actually classifies with.
        let hot_shard_collector = Arc::new(frogdb_debug::HotShardCollector::with_shared_config(
            self.shard_senders.clone(),
            self.config_manager.hotshard_config(),
        ));
        let observability_collectors = Arc::new(
            crate::server_observability::ServerObservability::default()
                .with_hot_shards(hot_shard_collector.clone()),
        );

        // Create quorum checker for self-fencing (write rejection on quorum loss)
        // Prefer failure_detector (Raft mode), fallback to replication_quorum_checker.
        //
        // In Raft mode the checker is always installed and wrapped in a
        // `SelfFenceGate`, which consults the live `self-fence-on-quorum-loss`
        // flag at each write pre-check. Gating installation instead would freeze
        // the decision at startup.
        //
        // Built before the status collector so `/status` can report the fence
        // reason from *this* object: the write gate's verdict and the reported
        // reason are then the same evaluation, not two that can drift.
        let self_fence_gate = self.failure_detector.as_ref().map(|fd| {
            Arc::new(crate::cluster::flags::SelfFenceGate::new(
                fd.clone() as Arc<dyn frogdb_core::command::QuorumChecker>,
                self.config_manager.cluster_flags(),
            ))
        });
        let quorum_checker: Option<Arc<dyn frogdb_core::command::QuorumChecker>> =
            match &self_fence_gate {
                Some(gate) => Some(gate.clone()),
                None => self.replication_quorum_checker.clone().map(|c| c as _),
            };
        // Same selection, as the observability seam: whichever object the write
        // gate consults is the one that explains a rejection.
        let write_fence: Option<Arc<dyn frogdb_core::WriteFenceReporter>> = match &self_fence_gate {
            Some(gate) => Some(gate.clone()),
            // `replication_self_fence` is the same object as
            // `replication_quorum_checker`, un-erased (a `dyn QuorumChecker`
            // cannot be re-cast to another trait).
            None => self.replication_self_fence.clone().map(|c| c as _),
        };

        // Single status collector shared by the HTTP `/status` endpoint and the
        // STATUS JSON connection command, so the two surfaces can never disagree.
        // Built unconditionally over the object-safe metrics recorder so STATUS
        // JSON works even when the HTTP server is disabled (the no-op recorder
        // reports absent counters as 0, never faked).
        let mut status_collector = StatusCollector::new(
            self.config_manager.status_thresholds(),
            self.health_checker.clone(),
            self.shard_senders.clone(),
            self.client_registry.clone(),
            self.metrics_recorder.clone(),
            start_time,
            self.config_manager.max_clients_flag(),
            self.config.memory.maxmemory,
            self.config.persistence.enabled,
            self.config.persistence.durability_mode.clone(),
            mode.clone(),
        )
        .with_hot_shards(hot_shard_collector.clone());
        if let Some(reporter) = write_fence {
            status_collector = status_collector.with_write_fence(reporter);
        }
        let status_collector = Arc::new(status_collector);

        // Start HTTP server if enabled (metrics, health, debug, admin REST)
        let http_server_handle = if let Some(ref prometheus) = self.prometheus_recorder {
            // Create debug state for the debug web UI
            let config_entries = vec![
                ConfigEntry {
                    name: "bind".into(),
                    value: self.config.server.bind.clone(),
                },
                ConfigEntry {
                    name: "port".into(),
                    value: self.config.server.port.to_string(),
                },
                ConfigEntry {
                    name: "num_shards".into(),
                    value: self.shard_senders.len().to_string(),
                },
                ConfigEntry {
                    name: "http_bind".into(),
                    value: self.config.http.bind.clone(),
                },
                ConfigEntry {
                    name: "http_port".into(),
                    value: self.config.http.port.to_string(),
                },
            ];
            // Single coherent node-state provider (replication, clients, cluster).
            // The provider draws role from the shared live mode and master
            // host/port from the RoleManager, so both track runtime `REPLICAOF`.
            let node_state_provider = Arc::new(crate::debug_providers::ServerDebugProvider::new(
                self.client_registry.clone(),
                self.cluster_state.clone(),
                self.node_id,
                self.replication_tracker.clone(),
                mode.clone(),
                self.role_manager_handle.clone(),
                self.raft
                    .clone()
                    .map(|r| r as Arc<dyn crate::debug_providers::RaftMetricsReader>),
            ));

            let debug_state = DebugState::new(
                ServerInfo {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    start_time,
                    num_shards: self.shard_senders.len(),
                    bind_addr: self.config.server.bind.clone(),
                    port: self.config.server.port,
                },
                config_entries,
            )
            .with_node_state(node_state_provider)
            .with_shard_senders(self.shard_senders.clone())
            .with_hot_shards(hot_shard_collector.clone());

            // SAFETY: http_listener is Some when prometheus_recorder is Some
            // (both are gated on config.http.enabled in Server::new()).
            let http_listener = self
                .http_listener
                .take()
                .expect("http_listener must be set when HTTP server is enabled");
            let http_bound_addr = http_listener.local_addr()?;

            let http_config = crate::config::HttpConfig {
                bind: self.config.http.bind.clone(),
                port: http_bound_addr.port(),
                enabled: true,
                token: self.config.http.token.clone(),
            };

            // Create admin state for admin REST endpoints (if admin is enabled)
            let admin_state = if self.config.admin.enabled {
                Some(Arc::new(AdminState {
                    cluster_state: self.cluster_state.clone(),
                    replication_tracker: self.replication_tracker.clone(),
                    node_id: self.node_id,
                    client_addr: self.config.bind_addr(),
                    cluster_bus_addr: if self.config.cluster.enabled {
                        Some(self.config.cluster.cluster_bus_addr.clone())
                    } else {
                        None
                    },
                    shutdown_tx: None, // TODO: wire up shutdown channel from Server
                    raft: self.raft.clone(),
                }))
            } else {
                None
            };

            let mut server = ObservabilityServer::new(
                http_config,
                prometheus.clone(),
                self.health_checker.clone(),
            )
            .with_listener(http_listener)
            .with_debug_state(debug_state)
            .with_status_collector(status_collector.clone());

            if let Some(admin_state) = admin_state {
                server = server.with_admin_state(admin_state);
            }

            // Wire up TLS for HTTPS when configured
            #[cfg(not(feature = "turmoil"))]
            if self.config.tls.enabled
                && !self.config.tls.no_tls_on_http
                && let Some(ref tls_rt) = self.tls_runtime
            {
                server = server.with_tls(tls_rt.manager().clone(), tls_rt.handshake_timeout());
            }

            let scheme = {
                #[cfg(not(feature = "turmoil"))]
                {
                    if self.config.tls.enabled && !self.config.tls.no_tls_on_http {
                        "https"
                    } else {
                        "http"
                    }
                }
                #[cfg(feature = "turmoil")]
                {
                    "http"
                }
            };

            info!(
                addr = %http_bound_addr,
                debug_ui = %format!("{}://{}/debug", scheme, http_bound_addr),
                status_json = %format!("{}://{}/status/json", scheme, http_bound_addr),
                "HTTP server starting"
            );

            Some(server.spawn())
        } else {
            None
        };

        // Start system metrics collector if metrics enabled
        let system_collector_handle = if self.prometheus_recorder.is_some() {
            Some(SystemMetricsCollector::spawn_collector(
                self.metrics_recorder.clone(),
                Duration::from_secs(5),
                self.shared_maxmemory.clone(),
                self.shard_memory_used.clone(),
            ))
        } else {
            None
        };

        // Start version metrics collector (records active_version, mixed_version, gate status)
        if self.prometheus_recorder.is_some() {
            let recorder = self.metrics_recorder.clone();
            let cluster_state = self.cluster_state.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(15));
                loop {
                    ticker.tick().await;
                    record_version_metrics(&recorder, cluster_state.as_ref());
                }
            });
        }

        // Determine if admin port is enabled (used for both acceptors)
        let admin_enabled = self.config.admin.enabled;

        // Start cluster bus TCP server if cluster mode is enabled
        let cluster_bus_handle = if let Some(ref raft) = self.raft {
            // SAFETY: cluster_bus_listener is Some when raft is Some
            // (both gated on config.cluster.enabled in Server::new()).
            let cluster_bus_listener = self
                .cluster_bus_listener
                .take()
                .expect("cluster_bus_listener must be set when cluster is enabled");
            let ctx = Arc::new(crate::cluster::bus::ClusterBusContext {
                raft: raft.clone(),
                shard_senders: self.shard_senders.clone(),
                num_shards: self.config.server.num_shards.max(1),
                node_id: self
                    .node_id
                    .expect("node_id must be set when cluster is enabled"),
                replication_offset: self
                    .shared_replication_offset
                    .clone()
                    .unwrap_or_else(|| Arc::new(AtomicU64::new(0))),
                // The same handle INFO's `master_link_status` reads, so a peer
                // scoring this node for promotion (FM-CLUSTER-106) and an
                // operator reading INFO cannot disagree about the link.
                replica_link: Arc::new(self.role_manager_handle.clone())
                    as Arc<dyn crate::cluster::ReplicaLinkState>,
                // Both bus directions accumulate into the network factory's
                // counter pair, which is what `CLUSTER INFO` reads.
                bus_stats: self
                    .network_factory
                    .as_ref()
                    .map(|nf| nf.bus_stats().clone())
                    .unwrap_or_default(),
                #[cfg(not(feature = "turmoil"))]
                tls: if self.config.tls.enabled && self.config.tls.tls_cluster {
                    self.tls_runtime.as_ref().map(|h| {
                        Arc::new(crate::cluster::ClusterBusTls::new(h.clone()))
                            as Arc<dyn crate::cluster::bus::BusTlsAcceptor>
                    })
                } else {
                    None
                },
            });
            Some(spawn(async move {
                if let Err(e) = crate::cluster::bus::run(cluster_bus_listener, ctx).await {
                    error!(error = %e, "Cluster bus server error");
                }
            }))
        } else {
            None
        };

        // The live replication state (replication id + failover window +
        // offset) INFO reports. Both role handlers hold the *same*
        // `ReplicationIdentity` cell, so either one answers for the node: a
        // replica's FULLRESYNC adoption, a promotion's freshly minted
        // `master_replid`, and a demotion's inherited id are all visible here
        // without re-reading whichever handler happens to exist. Taken from the
        // primary handler because it is the one built on every role.
        let info_replication_state = self
            .primary_replication_handler
            .as_ref()
            .map(|h| h.shared_state());

        // Start replica replication if running as replica
        let replica_handle = if let (Some(handler), Some(frame_rx)) =
            (self.replica_handler.take(), self.replica_frame_rx.take())
        {
            let shard_senders = self.shard_senders.clone();
            let num_shards = self.config.server.num_shards.max(1);

            // Adopt this boot-spawned handler into the RoleManager, before the
            // acceptor (spawned later in this function) can start serving
            // client connections — so no REPLICAOF can race ahead of this
            // call. Without this, promoting away from a boot-spawned Replica
            // would leave its reconnect loop dialing the old primary forever,
            // since RoleManager otherwise has no idea this handler exists.
            self.role_manager_handle
                .register_boot_replica_handler(handler.clone(), handler.primary_addr());

            // Get shared replication state for the frame consumer to update active_version
            let replication_state = Some(handler.shared_state());

            // This stream's applying stint, opened *before* the connection task
            // so every connection it builds is captured under it. Every group is
            // claimed against the stint before it reaches a shard, so a
            // promotion can freeze the boundary without waiting for — or
            // cancelling — an apply in flight, and a connection retired
            // mid-full-sync can no longer reset the offsets.
            let stint = handler.applied_offset().begin_replica_stint();

            // Spawn replication connection task (connects to primary and receives frames)
            let handler_clone = handler.clone();
            let repl_conn_handle = spawn(async move {
                if let Err(e) = handler_clone.start().await {
                    error!(error = %e, "Replica replication connection error");
                }
            });

            // Spawn frame consumer task (applies replicated commands to shards).
            // The control seam carries the process-wide state that has no shard
            // to route to — the function-library registry (issue 48).
            let executor = ReplicaCommandExecutor::new(shard_senders, num_shards)
                .with_control_applier(Arc::new(crate::function_store::FunctionStore::new(
                    self.function_registry.clone(),
                    self.config_manager.clone(),
                )));
            let is_replica_for_consumer = self.is_replica_flag.clone();
            let txn_bound = std::sync::Arc::new(frogdb_replication::ReplicaTxnBound::new(
                self.config.replication.replica_txn_max_commands,
                self.config.replication.replica_txn_max_bytes,
            ));
            let frame_consumer_handle = spawn(async move {
                consume_frames(
                    frame_rx,
                    executor,
                    is_replica_for_consumer,
                    replication_state,
                    stint,
                    txn_bound,
                )
                .await;
            });

            info!("Replica replication tasks started");

            Some((repl_conn_handle, frame_consumer_handle))
        } else {
            None
        };

        // Create cluster pub/sub forwarder (None in standalone mode)
        let pubsub_forwarder: Option<Arc<crate::cluster::pubsub::ClusterPubSubForwarder>> =
            if let (Some(cluster_state), Some(node_id), Some(network_factory)) =
                (&self.cluster_state, self.node_id, &self.network_factory)
            {
                Some(Arc::new(
                    crate::cluster::pubsub::ClusterPubSubForwarder::Cluster {
                        cluster_state: cluster_state.clone(),
                        network_factory: network_factory.clone(),
                        node_id,
                    },
                ))
            } else {
                None
            };

        // Tick the replication backlog's idle TTL. Redis frees the backlog from
        // `replicationCron` for the same reason: once no replica has been
        // attached for `repl-backlog-ttl` seconds, buffering every write costs
        // memory and a push for resume history nobody is waiting for. The tick
        // is cheap (a replica count and one mutex) and reads the TTL live, so a
        // `CONFIG SET repl-backlog-ttl` applies without a restart — including
        // `0`, which parks the timer.
        let backlog_ttl_handle = self.primary_replication_handler.clone().map(|handler| {
            spawn(async move {
                let mut ticker = tokio::time::interval(BACKLOG_TTL_TICK);
                loop {
                    ticker.tick().await;
                    handler.expire_idle_backlog();
                }
            })
        });

        // Create MONITOR broadcaster (shared across all connections)
        let monitor_broadcaster = Arc::new(crate::monitor::MonitorBroadcaster::new(
            self.config.monitor.channel_capacity,
        ));

        // Server-wide latency histograms for INFO latencystats. Built during
        // infrastructure init and already injected into the ConfigManager, so we
        // reuse the same instance here for the acceptors.
        let latency_histograms = self.latency_histograms.clone();

        // Create server-wide hotkey sampling session
        let hotkey_session = frogdb_core::new_shared_hotkey_session();

        // Create shared cursor store for FT.AGGREGATE WITHCURSOR / FT.CURSOR
        let cursor_store = Arc::new(crate::cursor_store::AggregateCursorStore::new());
        {
            let store = cursor_store.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
                loop {
                    interval.tick().await;
                    store.evict_expired();
                }
            });
        }

        // Shared dependencies for every acceptor (main, admin, TLS ports).
        // Built once here; only the per-listener `PortSpec` (listener,
        // is_admin, TLS manager) differs between the three ports below.
        let acceptor_ctx = AcceptorContext {
            core: CoreDeps {
                registry: self.registry.clone(),
                shard_senders: self.shard_senders.clone(),
                acl_manager: self.acl_manager.clone(),
            },
            admin: AdminDeps {
                client_registry: self.client_registry.clone(),
                config_manager: self.config_manager.clone(),
                snapshot_coordinator: self.snapshot_coordinator.clone(),
                function_registry: self.function_registry.clone(),
                cursor_store: cursor_store.clone(),
                recovery_stats: self.recovery_stats.clone(),
            },
            cluster: ClusterDeps {
                cluster_state: self.cluster_state.clone(),
                node_id: self.node_id,
                raft: self.raft.clone(),
                network_factory: self.network_factory.clone(),
                slot_migration: self.slot_migration.clone(),
                replication_tracker: self.replication_tracker.clone(),
                primary_replication_handler: self.primary_replication_handler.clone(),
                replication_state: info_replication_state.clone(),
                quorum_checker: quorum_checker.clone(),
                // `DEBUG REPLICATION CHECK`'s two out-of-crate view groups.
                // Both are wired on every role and in every mode — the
                // command answers everywhere, so a group that went missing in
                // standalone would silently stop evaluating `INV-FENCE-1` /
                // `INV-ROLE-1` exactly where nothing else checks them.
                replication_self_fence: self.replication_self_fence.clone(),
                role_controller: Some(Arc::new(self.role_manager_handle.clone())),
                pubsub_forwarder: pubsub_forwarder.clone(),
            },
            observability: ObservabilityDeps {
                metrics_recorder: self.metrics_recorder.clone(),
                shared_tracer: self.shared_tracer.clone(),
                tracing_config: self.config.tracing.clone(),
                monitor_broadcaster: monitor_broadcaster.clone(),
                latency_histograms: latency_histograms.clone(),
                hotkey_session: hotkey_session.clone(),
                keyspace_stats: self.keyspace_stats.clone(),
                status_collector: Some(status_collector.clone()),
                collectors: observability_collectors.clone(),
            },
            new_conn_senders: std::mem::take(&mut self.new_conn_senders),
            allow_cross_slot: self.config.server.allow_cross_slot_standalone,
            scatter_gather_timeout_ms: self.config.server.scatter_gather_timeout_ms,
            pubsub_output_buffer_hard_limit: self.config.server.pubsub_output_buffer_hard_limit,
            admin_enabled,
            memory_diag_config: self.config.memory.to_diag_config(),
            max_clients: self.config_manager.max_clients_flag(),
            is_replica: self.is_replica_flag.clone(),
            conn_monitor: self.conn_monitor.clone(),
            #[cfg(feature = "turmoil")]
            chaos_config: std::sync::Arc::new(self.config.chaos.clone()),
            #[cfg(not(feature = "turmoil"))]
            tls_handshake_timeout: self.tls_handshake_timeout(),
        };

        // Create main acceptor (regular client connections)
        // When admin port is enabled, this acceptor blocks admin commands
        let listener = self
            .listener
            .take()
            .expect("listener must be available for start_subsystems");
        let acceptor = Acceptor::bind(
            acceptor_ctx.clone(),
            PortSpec {
                listener,
                is_admin: false, // regular port
                #[cfg(not(feature = "turmoil"))]
                tls_manager: None, // No TLS on the main plaintext port
            },
        );

        // Spawn main acceptor task
        let acceptor_handle = spawn(async move {
            if let Err(e) = acceptor.run().await {
                error!(error = %e, "Acceptor error");
            }
        });

        // Spawn admin acceptor if admin port is enabled
        let admin_acceptor_handle = if let Some(admin_listener) = self.admin_listener.take() {
            let admin_acceptor = Acceptor::bind(
                acceptor_ctx.clone(),
                PortSpec {
                    listener: admin_listener,
                    is_admin: true, // admin port
                    // Admin port gets TLS only if no_tls_on_admin_port is false
                    #[cfg(not(feature = "turmoil"))]
                    tls_manager: if !self.config.tls.no_tls_on_admin_port {
                        self.tls_runtime.as_ref().map(|h| h.manager().clone())
                    } else {
                        None
                    },
                },
            );

            Some(spawn(async move {
                if let Err(e) = admin_acceptor.run().await {
                    error!(error = %e, "Admin acceptor error");
                }
            }))
        } else {
            None
        };

        // Spawn TLS acceptor if TLS is enabled and a TLS listener exists
        #[cfg(not(feature = "turmoil"))]
        let tls_acceptor_handle = if let Some(tls_listener) = self.tls_listener.take() {
            if let Some(ref tls_rt) = self.tls_runtime {
                let tls_acceptor = Acceptor::bind(
                    acceptor_ctx,
                    PortSpec {
                        listener: tls_listener,
                        is_admin: false, // TLS port
                        tls_manager: Some(tls_rt.manager().clone()),
                    },
                );

                Some(spawn(async move {
                    if let Err(e) = tls_acceptor.run().await {
                        error!(error = %e, "TLS acceptor error");
                    }
                }))
            } else {
                None
            }
        } else {
            None
        };

        // Spawn the certificate watcher. Deliberately not tied to the TLS
        // listener above: the cluster bus, replication links and the admin port
        // can all use TLS without a TLS client port being configured, and their
        // certificates need reloading just the same. Returns `None` when
        // `tls.watch-certs` is off.
        #[cfg(not(feature = "turmoil"))]
        let cert_watcher_handle = self
            .tls_runtime
            .as_ref()
            .and_then(|tls_rt| crate::tls_watch::spawn_cert_watcher(tls_rt.clone()));

        // Record initial max_clients gauge
        {
            let max_clients = self.config_manager.max_clients();
            frogdb_telemetry::definitions::ConnectionsMax::set(
                &*self.metrics_recorder,
                max_clients as f64,
            );
        }

        // Mark server as ready
        self.health_checker.set_ready();

        // Move failure_detector_handle out of self
        let failure_detector_handle = self.failure_detector_handle.take();

        // Move shard supervisor and periodic handles out of self
        let shard_supervisor = self.shard_supervisor_handle.take();
        let periodic_sync_handle = self.periodic_sync_handle.take();
        let periodic_snapshot_handle = self.periodic_snapshot_handle.take();

        Ok(SubsystemHandles {
            http_server: http_server_handle,
            system_collector: system_collector_handle,
            cluster_bus: cluster_bus_handle,
            replica: replica_handle,
            acceptor: acceptor_handle,
            admin_acceptor: admin_acceptor_handle,
            #[cfg(not(feature = "turmoil"))]
            tls_acceptor: tls_acceptor_handle,
            #[cfg(not(feature = "turmoil"))]
            cert_watcher: cert_watcher_handle,
            failure_detector: failure_detector_handle,
            shard_supervisor,
            periodic_sync_handle,
            periodic_snapshot_handle,
            backlog_ttl_handle,
        })
    }

    /// Shut down all subsystems cleanly.
    pub(super) async fn shutdown_subsystems(&mut self, handles: SubsystemHandles) {
        // Mark server as not ready during shutdown
        self.health_checker.shutdown();

        // Abort the acceptors first: nothing below this point is prepared to
        // serve a fresh connection, and a PSYNC accepted later would register a
        // downstream session behind the drain that exists to end them all.
        handles.acceptor.abort();
        if let Some(handle) = handles.admin_acceptor {
            handle.abort();
        }
        #[cfg(not(feature = "turmoil"))]
        if let Some(handle) = handles.tls_acceptor {
            handle.abort();
        }

        // Send shutdown to all shards
        for sender in self.shard_senders.iter() {
            let _ = sender.send(ShardMessage::Shutdown).await;
        }

        // Wait for the shard supervisor to observe every worker terminate. The
        // supervisor owns the individual worker handles; its task completes only
        // once all shards have drained. `health_checker.shutdown()` above already
        // flipped the shutdown signal, so these completions are treated as
        // expected teardown rather than fail-stop-triggering crashes.
        if let Some(handle) = handles.shard_supervisor {
            let _ = handle.await;
        }

        // Stop periodic sync task if running
        if let Some(handle) = handles.periodic_sync_handle {
            handle.abort();
        }

        // Stop periodic snapshot task if running
        if let Some(handle) = handles.periodic_snapshot_handle {
            handle.abort();
        }

        // Wait for any in-progress snapshot to complete before final flush
        if self.snapshot_coordinator.in_progress() {
            info!("Waiting for in-progress snapshot to complete...");
            while self.snapshot_coordinator.in_progress() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            info!("Snapshot completed");
        }

        // Stop HTTP server, system collector, and cluster bus
        if let Some(handle) = handles.http_server {
            handle.abort();
        }
        if let Some(handle) = handles.system_collector {
            handle.abort();
        }
        if let Some(handle) = handles.cluster_bus {
            handle.abort();
        }

        // Stop replica replication tasks if running
        if let Some((conn_handle, consumer_handle)) = handles.replica {
            conn_handle.abort();
            consumer_handle.abort();
        }

        // Tear down downstream replica sessions. Aborting the acceptors above
        // only stops new connections; established sessions keep streaming past
        // this shutdown and keep the storage engine open behind it. The drain
        // also latches the handler, so a connection accepted just before the
        // abort cannot open a new session while this runs.
        if let Some(ref handler) = self.primary_replication_handler {
            handler
                .shutdown_downstream_sessions(Duration::from_secs(2))
                .await;
        }

        // Stop the backlog-TTL ticker
        if let Some(handle) = handles.backlog_ttl_handle {
            handle.abort();
        }

        // Stop failure detector task if running
        if let Some(handle) = handles.failure_detector {
            handle.abort();
        }

        // Shutdown tracer and flush pending spans
        if let Some(ref tracer) = self.shared_tracer {
            info!("Shutting down distributed tracer...");
            tracer.shutdown();
        }

        // Persist replication offset so it survives restart (never rewinds to a
        // stale boot value). Done before the RocksDB flush so the durable offset
        // is bounded by the data that is about to be flushed.
        // Skipped while this node is a replica: the primary handler exists on
        // every role (so a promotion has live seams) but shares one state file
        // with the replica handler, and saving this node's boot-time copy would
        // overwrite the replica's adopted replication id/offset — forcing a full
        // resync on the next start. Read live, so a promoted node does persist.
        if !self
            .is_replica_flag
            .load(std::sync::atomic::Ordering::Relaxed)
            && let Some(ref handler) = self.primary_replication_handler
        {
            match handler.save_state() {
                Ok(()) => info!("Replication state persisted on shutdown"),
                Err(e) => error!(error = %e, "Failed to persist replication state on shutdown"),
            }
        }
        // Note: deliberately no save-on-shutdown for `self.replica_handler`
        // here (there used to be dead code attempting it). `start_subsystems`
        // already `take()`s the handler before spawning the connection/
        // consumer tasks, so `self.replica_handler` is always `None` by the
        // time shutdown runs — the old block could never execute. There is
        // currently no replacement persistence hook for replica state (unlike
        // the primary handler's pre-snapshot hook above); a clean restart
        // resumes from the last-recovered offset rather than the exact
        // shutdown offset. Tracked separately from this fix.

        // Final flush of RocksDB
        if let Some(ref rocks) = self.rocks_store {
            if let Err(e) = rocks.flush() {
                error!(error = %e, "Failed to flush RocksDB on shutdown");
            } else {
                info!("RocksDB flushed successfully");
            }
        }

        // Stop the certificate watcher
        #[cfg(not(feature = "turmoil"))]
        if let Some(handle) = handles.cert_watcher {
            handle.abort();
        }
    }

    /// Check for pending split-brain logs and set metric.
    pub(super) fn check_split_brain_logs(&self) {
        if frogdb_replication::split_brain_log::has_pending_logs(&self.config.persistence.data_dir)
        {
            tracing::warn!("Unprocessed split-brain log files found in data directory");
            frogdb_telemetry::definitions::SplitBrainRecoveryPending::set(
                &*self.metrics_recorder,
                1.0,
            );
        }
    }

    /// Run startup latency test if configured.
    pub(super) fn run_startup_latency_test(&mut self) {
        if self.config.latency.startup_test {
            info!(
                "Running startup latency test for {} seconds...",
                self.config.latency.startup_test_duration_secs
            );

            let result = crate::latency_test::run_intrinsic_latency_test(
                self.config.latency.startup_test_duration_secs,
                None,
            );

            // Check against warning threshold
            if result.max_us > self.config.latency.warning_threshold_us {
                tracing::warn!(
                    max_latency_us = result.max_us,
                    threshold_us = self.config.latency.warning_threshold_us,
                    "High intrinsic latency detected. This may indicate virtualization \
                     overhead or system contention."
                );
            }

            info!(
                min_us = result.min_us,
                max_us = result.max_us,
                avg_us = format!("{:.1}", result.avg_us),
                p99_us = result.p99_us,
                samples = result.samples,
                "Latency baseline established"
            );

            // Store globally for INFO command access
            crate::latency_test::set_global_baseline(
                result.clone(),
                self.config.latency.warning_threshold_us,
            );
            self.latency_baseline = Some(result);
        }
    }
}

/// Record version-related metrics from cluster state.
///
/// Called periodically to update active_version, mixed_version, and gate metrics.
fn record_version_metrics(
    recorder: &Arc<dyn MetricsRecorder>,
    cluster_state: Option<&Arc<ClusterState>>,
) {
    if let Some(cluster_state) = cluster_state {
        let snapshot = cluster_state.snapshot();

        // Active version metric
        if let Some(ref active) = snapshot.active_version {
            frogdb_telemetry::definitions::ActiveVersion::set(&**recorder, 1.0, active.as_str());
        }

        // Mixed-version detection
        let versions: Vec<&str> = snapshot
            .nodes
            .values()
            .filter(|n| !n.version.is_empty())
            .map(|n| n.version.as_str())
            .collect();
        let min = versions.iter().min();
        let max = versions.iter().max();
        let mixed = min != max && min.is_some();
        frogdb_telemetry::definitions::ClusterMixedVersion::set(
            &**recorder,
            if mixed { 1.0 } else { 0.0 },
        );

        // Version gate metrics
        for gate in version_gate::VERSION_GATES {
            let active =
                version_gate::is_gate_active(gate.name, snapshot.active_version.as_deref());
            frogdb_telemetry::definitions::VersionGateActive::set(
                &**recorder,
                if active { 1.0 } else { 0.0 },
                gate.name,
            );
        }
    }
}
