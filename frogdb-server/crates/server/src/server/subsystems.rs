//! Subsystem startup and shutdown logic extracted from `run_until()`.

use frogdb_cluster::version_gate;
use frogdb_core::ShardMessage;
use frogdb_core::sync::{Arc, AtomicU64};
use frogdb_core::{ClusterState, MetricsRecorder};
use frogdb_debug::{ConfigEntry, DebugState, ServerInfo};
use frogdb_telemetry::{StatusCollector, SystemMetricsCollector};
use std::time::Duration;
use tracing::{error, info};

use crate::acceptor::Acceptor;
use crate::admin::handlers::AdminState;
use crate::net::{JoinHandle, spawn};
use crate::observability_server::ObservabilityServer;
use crate::replication::{ReplicaCommandExecutor, consume_frames};

use crate::config::{HotShardsConfigExt, MemoryConfigExt, StatusConfigExt};

use super::Server;

use anyhow::Result;

/// Handles for all spawned subsystem tasks.
///
/// Collected during startup so shutdown can cleanly stop everything.
pub(super) struct SubsystemHandles {
    pub http_server: Option<JoinHandle<()>>,
    pub system_collector: Option<JoinHandle<()>>,
    pub cluster_bus: Option<JoinHandle<()>>,
    pub replica: Option<(JoinHandle<()>, JoinHandle<()>)>,
    pub acceptor: JoinHandle<()>,
    pub admin_acceptor: Option<JoinHandle<()>>,
    #[cfg(not(feature = "turmoil"))]
    pub tls_acceptor: Option<JoinHandle<()>>,
    pub failure_detector: Option<JoinHandle<()>>,
    pub shard_handles: Vec<JoinHandle<()>>,
    pub periodic_sync_handle: Option<JoinHandle<()>>,
    pub periodic_snapshot_handle: Option<JoinHandle<()>>,
}

impl Server {
    /// Start all subsystems and return handles for later shutdown.
    pub(super) fn start_subsystems(&mut self) -> Result<SubsystemHandles> {
        // Capture server start time
        let start_time = std::time::Instant::now();

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
            .with_client_info(self.client_registry.clone());

            // Wire up cluster info if cluster mode is enabled
            let debug_state = if let Some(ref cluster_state) = self.cluster_state {
                debug_state.with_cluster_info(Arc::new(
                    crate::debug_providers::ClusterStateProvider::new(
                        cluster_state.clone(),
                        self.node_id,
                    ),
                ))
            } else {
                debug_state
            };

            // Create status collector for /status/json endpoint
            let status_collector_config = self.config.status.to_collector_config();
            let mode = if self.config.cluster.enabled {
                "cluster".to_string()
            } else if self.config.replication.is_primary() {
                "primary".to_string()
            } else if self.config.replication.is_replica() {
                "replica".to_string()
            } else {
                "standalone".to_string()
            };
            let status_collector = Arc::new(StatusCollector::new(
                status_collector_config,
                self.health_checker.clone(),
                self.shard_senders.clone(),
                self.client_registry.clone(),
                prometheus.clone(),
                start_time,
                self.config_manager.max_clients_flag(),
                self.config.memory.maxmemory,
                self.config.persistence.enabled,
                self.config.persistence.durability_mode.clone(),
                mode,
            ));

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
            .with_status_collector(status_collector);

            if let Some(admin_state) = admin_state {
                server = server.with_admin_state(admin_state);
            }

            // Wire up TLS for HTTPS when configured
            #[cfg(not(feature = "turmoil"))]
            if self.config.tls.enabled
                && !self.config.tls.no_tls_on_http
                && let Some(ref mgr) = self.tls_manager
            {
                server = server.with_tls(
                    mgr.clone(),
                    std::time::Duration::from_millis(self.config.tls.handshake_timeout_ms),
                );
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
            let ctx = Arc::new(crate::cluster_bus::ClusterBusContext {
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
                #[cfg(not(feature = "turmoil"))]
                tls_manager: if self.config.tls.enabled && self.config.tls.tls_cluster {
                    self.tls_manager.clone()
                } else {
                    None
                },
                #[cfg(not(feature = "turmoil"))]
                tls_cluster_migration: self.config.tls.tls_cluster_migration,
                #[cfg(not(feature = "turmoil"))]
                tls_handshake_timeout: std::time::Duration::from_millis(
                    self.config.tls.handshake_timeout_ms,
                ),
            });
            Some(spawn(async move {
                if let Err(e) = crate::cluster_bus::run(cluster_bus_listener, ctx).await {
                    error!(error = %e, "Cluster bus server error");
                }
            }))
        } else {
            None
        };

        // Start replica replication if running as replica
        let replica_handle = if let (Some(handler), Some(frame_rx)) =
            (self.replica_handler.take(), self.replica_frame_rx.take())
        {
            let shard_senders = self.shard_senders.clone();
            let num_shards = self.config.server.num_shards.max(1);

            // Get shared replication state for the frame consumer to update active_version
            let replication_state = Some(handler.shared_state());

            // Spawn replication connection task (connects to primary and receives frames)
            let handler_clone = handler.clone();
            let repl_conn_handle = spawn(async move {
                if let Err(e) = handler_clone.start().await {
                    error!(error = %e, "Replica replication connection error");
                }
            });

            // Spawn frame consumer task (applies replicated commands to shards)
            let executor = ReplicaCommandExecutor::new(shard_senders, num_shards);
            let is_replica_for_consumer = self.is_replica_flag.clone();
            let frame_consumer_handle = spawn(async move {
                consume_frames(
                    frame_rx,
                    executor,
                    is_replica_for_consumer,
                    replication_state,
                )
                .await;
            });

            info!("Replica replication tasks started");

            Some((repl_conn_handle, frame_consumer_handle))
        } else {
            None
        };

        // Create quorum checker for self-fencing (write rejection on quorum loss)
        // Prefer failure_detector (Raft mode), fallback to replication_quorum_checker
        let quorum_checker: Option<Arc<dyn frogdb_core::command::QuorumChecker>> =
            if let Some(ref fd) = self.failure_detector {
                if self.config.cluster.self_fence_on_quorum_loss {
                    Some(fd.clone() as Arc<dyn frogdb_core::command::QuorumChecker>)
                } else {
                    None
                }
            } else {
                self.replication_quorum_checker.clone()
            };

        // Create cluster pub/sub forwarder (None in standalone mode)
        let pubsub_forwarder: Option<Arc<crate::cluster_pubsub::ClusterPubSubForwarder>> =
            if let (Some(cluster_state), Some(node_id), Some(network_factory)) =
                (&self.cluster_state, self.node_id, &self.network_factory)
            {
                Some(Arc::new(
                    crate::cluster_pubsub::ClusterPubSubForwarder::Cluster {
                        cluster_state: cluster_state.clone(),
                        network_factory: network_factory.clone(),
                        node_id,
                    },
                ))
            } else {
                None
            };

        // Create MONITOR broadcaster (shared across all connections)
        let monitor_broadcaster = Arc::new(crate::monitor::MonitorBroadcaster::new(
            self.config.monitor.channel_capacity,
        ));

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

        // Create main acceptor (regular client connections)
        // When admin port is enabled, this acceptor blocks admin commands
        let listener = self
            .listener
            .take()
            .expect("listener must be available for start_subsystems");
        let new_conn_senders = std::mem::take(&mut self.new_conn_senders);
        let acceptor = Acceptor::new(
            listener,
            new_conn_senders.clone(),
            self.shard_senders.clone(),
            self.registry.clone(),
            self.client_registry.clone(),
            self.config_manager.clone(),
            self.config.server.allow_cross_slot_standalone,
            self.config.server.scatter_gather_timeout_ms,
            self.metrics_recorder.clone(),
            self.acl_manager.clone(),
            self.snapshot_coordinator.clone(),
            self.function_registry.clone(),
            cursor_store.clone(),
            self.shared_tracer.clone(),
            self.config.tracing.clone(),
            self.replication_tracker.clone(),
            self.cluster_state.clone(),
            self.node_id,
            false, // is_admin = false for regular port
            admin_enabled,
            self.config.hotshards.to_collector_config(),
            self.config.memory.to_diag_config(),
            self.raft.clone(),
            self.network_factory.clone(),
            self.primary_replication_handler.clone(),
            self.config_manager.max_clients_flag(),
            self.is_replica_flag.clone(),
            quorum_checker.clone(),
            self.conn_monitor.clone(),
            pubsub_forwarder.clone(),
            monitor_broadcaster.clone(),
            #[cfg(feature = "turmoil")]
            std::sync::Arc::new(self.config.chaos.clone()),
            #[cfg(not(feature = "turmoil"))]
            None, // No TLS on the main plaintext port
            #[cfg(not(feature = "turmoil"))]
            std::time::Duration::from_millis(self.config.tls.handshake_timeout_ms),
        );

        // Spawn main acceptor task
        let acceptor_handle = spawn(async move {
            if let Err(e) = acceptor.run().await {
                error!(error = %e, "Acceptor error");
            }
        });

        // Spawn admin acceptor if admin port is enabled
        let admin_acceptor_handle = if let Some(admin_listener) = self.admin_listener.take() {
            let admin_acceptor = Acceptor::new(
                admin_listener,
                new_conn_senders.clone(),
                self.shard_senders.clone(),
                self.registry.clone(),
                self.client_registry.clone(),
                self.config_manager.clone(),
                self.config.server.allow_cross_slot_standalone,
                self.config.server.scatter_gather_timeout_ms,
                self.metrics_recorder.clone(),
                self.acl_manager.clone(),
                self.snapshot_coordinator.clone(),
                self.function_registry.clone(),
                cursor_store.clone(),
                self.shared_tracer.clone(),
                self.config.tracing.clone(),
                self.replication_tracker.clone(),
                self.cluster_state.clone(),
                self.node_id,
                true, // is_admin = true for admin port
                admin_enabled,
                self.config.hotshards.to_collector_config(),
                self.config.memory.to_diag_config(),
                self.raft.clone(),
                self.network_factory.clone(),
                self.primary_replication_handler.clone(),
                self.config_manager.max_clients_flag(),
                self.is_replica_flag.clone(),
                quorum_checker.clone(),
                self.conn_monitor.clone(),
                pubsub_forwarder.clone(),
                monitor_broadcaster.clone(),
                #[cfg(feature = "turmoil")]
                std::sync::Arc::new(self.config.chaos.clone()),
                // Admin port gets TLS only if no_tls_on_admin_port is false
                #[cfg(not(feature = "turmoil"))]
                if !self.config.tls.no_tls_on_admin_port {
                    self.tls_manager.clone()
                } else {
                    None
                },
                #[cfg(not(feature = "turmoil"))]
                std::time::Duration::from_millis(self.config.tls.handshake_timeout_ms),
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
            if let Some(ref tls_manager) = self.tls_manager {
                let tls_acceptor = Acceptor::new(
                    tls_listener,
                    new_conn_senders.clone(),
                    self.shard_senders.clone(),
                    self.registry.clone(),
                    self.client_registry.clone(),
                    self.config_manager.clone(),
                    self.config.server.allow_cross_slot_standalone,
                    self.config.server.scatter_gather_timeout_ms,
                    self.metrics_recorder.clone(),
                    self.acl_manager.clone(),
                    self.snapshot_coordinator.clone(),
                    self.function_registry.clone(),
                    cursor_store.clone(),
                    self.shared_tracer.clone(),
                    self.config.tracing.clone(),
                    self.replication_tracker.clone(),
                    self.cluster_state.clone(),
                    self.node_id,
                    false, // is_admin = false for TLS port
                    admin_enabled,
                    self.config.hotshards.to_collector_config(),
                    self.config.memory.to_diag_config(),
                    self.raft.clone(),
                    self.network_factory.clone(),
                    self.primary_replication_handler.clone(),
                    self.config_manager.max_clients_flag(),
                    self.is_replica_flag.clone(),
                    quorum_checker.clone(),
                    self.conn_monitor.clone(),
                    pubsub_forwarder.clone(),
                    monitor_broadcaster.clone(),
                    Some(tls_manager.clone()), // TLS enabled
                    std::time::Duration::from_millis(self.config.tls.handshake_timeout_ms),
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

        // Record initial max_clients gauge
        {
            let max_clients = self.config_manager.max_clients();
            self.metrics_recorder.record_gauge(
                frogdb_telemetry::metric_names::CONNECTIONS_MAX,
                max_clients as f64,
                &[],
            );
        }

        // Mark server as ready
        self.health_checker.set_ready();

        // Move failure_detector_handle out of self
        let failure_detector_handle = self.failure_detector_handle.take();

        // Move shard handles and periodic handles out of self
        let shard_handles = std::mem::take(&mut self.shard_handles);
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
            failure_detector: failure_detector_handle,
            shard_handles,
            periodic_sync_handle,
            periodic_snapshot_handle,
        })
    }

    /// Shut down all subsystems cleanly.
    pub(super) async fn shutdown_subsystems(&mut self, handles: SubsystemHandles) {
        // Mark server as not ready during shutdown
        self.health_checker.shutdown();

        // Send shutdown to all shards
        for sender in self.shard_senders.iter() {
            let _ = sender.send(ShardMessage::Shutdown).await;
        }

        // Wait for shard workers to finish
        for handle in handles.shard_handles {
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

        // Stop failure detector task if running
        if let Some(handle) = handles.failure_detector {
            handle.abort();
        }

        // Shutdown tracer and flush pending spans
        if let Some(ref tracer) = self.shared_tracer {
            info!("Shutting down distributed tracer...");
            tracer.shutdown();
        }

        // Final flush of RocksDB
        if let Some(ref rocks) = self.rocks_store {
            if let Err(e) = rocks.flush() {
                error!(error = %e, "Failed to flush RocksDB on shutdown");
            } else {
                info!("RocksDB flushed successfully");
            }
        }

        // Abort acceptors
        handles.acceptor.abort();
        if let Some(handle) = handles.admin_acceptor {
            handle.abort();
        }
        #[cfg(not(feature = "turmoil"))]
        if let Some(handle) = handles.tls_acceptor {
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
            recorder.record_gauge(
                frogdb_telemetry::metric_names::ACTIVE_VERSION,
                1.0,
                &[("version", active.as_str())],
            );
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
        recorder.record_gauge(
            frogdb_telemetry::metric_names::CLUSTER_MIXED_VERSION,
            if mixed { 1.0 } else { 0.0 },
            &[],
        );

        // Version gate metrics
        for gate in version_gate::VERSION_GATES {
            let active =
                version_gate::is_gate_active(gate.name, snapshot.active_version.as_deref());
            recorder.record_gauge(
                frogdb_telemetry::metric_names::VERSION_GATE_ACTIVE,
                if active { 1.0 } else { 0.0 },
                &[("gate", gate.name)],
            );
        }
    }
}
