//! Infrastructure initialization phase of server construction.
//!
//! Handles metrics, listeners, registries, persistence, channels,
//! and function loading.

use anyhow::Result;
use frogdb_core::persistence::{
    NoopSnapshotCoordinator, RocksSnapshotCoordinator, RocksStore, SnapshotCoordinator,
};
use frogdb_core::sync::{Arc, AtomicU64};
use frogdb_core::{CommandRegistry, ExpiryIndex, HashMapStore, MetricsRecorder, ShardMessage};
use frogdb_telemetry::{HealthChecker, PrometheusRecorder, TaskMonitorRegistry};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::{Config, SnapshotConfigExt};
use crate::net::TcpListener;
use crate::runtime_config::ConfigManager;

use super::ServerListeners;
use super::listeners::bind_listeners;
use super::startup;
use super::util::{
    NEW_CONN_CHANNEL_CAPACITY, SHARD_CHANNEL_CAPACITY, build_eviction_config, build_wal_config,
};

use frogdb_core::EvictionConfig;
use frogdb_core::persistence::WalConfig;

/// Result of the infrastructure initialization phase.
pub(super) struct InitResult {
    pub listener: TcpListener,
    pub admin_listener: Option<TcpListener>,
    pub metrics_listener: Option<tokio::net::TcpListener>,
    pub admin_http_listener: Option<tokio::net::TcpListener>,
    pub cluster_bus_listener: Option<TcpListener>,
    pub registry: Arc<CommandRegistry>,
    pub client_registry: Arc<frogdb_core::ClientRegistry>,
    pub config_manager: Arc<ConfigManager>,
    pub num_shards: usize,
    pub shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
    pub shard_receivers: Vec<mpsc::Receiver<ShardMessage>>,
    pub new_conn_senders: Vec<mpsc::Sender<frogdb_core::shard::NewConnection>>,
    pub new_conn_receivers: Vec<mpsc::Receiver<frogdb_core::shard::NewConnection>>,
    pub rocks_store: Option<Arc<RocksStore>>,
    pub recovered_stores: Vec<(HashMapStore, ExpiryIndex)>,
    pub periodic_sync_handle: Option<crate::net::JoinHandle<()>>,
    pub snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
    pub periodic_snapshot_handle: Option<crate::net::JoinHandle<()>>,
    pub metrics_recorder: Arc<dyn MetricsRecorder>,
    pub prometheus_recorder: Option<Arc<PrometheusRecorder>>,
    pub health_checker: HealthChecker,
    pub shared_maxmemory: Arc<AtomicU64>,
    pub shard_memory_used: Arc<Vec<AtomicU64>>,
    pub wal_config: WalConfig,
    pub eviction_config: EvictionConfig,
    pub slowlog_next_id: Arc<AtomicU64>,
    pub function_registry: frogdb_core::SharedFunctionRegistry,
    pub shard_monitor: tokio_metrics::TaskMonitor,
    pub conn_monitor: tokio_metrics::TaskMonitor,
    pub task_registry: TaskMonitorRegistry,
}

/// Run the infrastructure initialization phase.
pub(super) async fn init_infrastructure(
    config: &Config,
    listeners: ServerListeners,
    log_reload_handle: Option<crate::runtime_config::LogReloadHandle>,
) -> Result<InitResult> {
    // Configure sorted set index backend before any data structures are created
    let score_index_backend = match config.server.sorted_set_index {
        crate::config::server::SortedSetIndexConfig::Btreemap => {
            frogdb_core::ScoreIndexBackend::BTree
        }
        crate::config::server::SortedSetIndexConfig::Skiplist => {
            frogdb_core::ScoreIndexBackend::SkipList
        }
    };
    frogdb_core::set_default_score_index(score_index_backend);

    // Initialize metrics
    let health_checker = HealthChecker::new();
    let (metrics_recorder, prometheus_recorder): (
        Arc<dyn MetricsRecorder>,
        Option<Arc<PrometheusRecorder>>,
    ) = if config.metrics.enabled {
        let mut recorder = PrometheusRecorder::new();
        if config.latency_bands.enabled {
            recorder = recorder.with_latency_bands(config.latency_bands.bands.clone());
            tracing::info!(
                bands = ?config.latency_bands.bands,
                "Latency band tracking enabled"
            );
        }
        let recorder = Arc::new(recorder);
        // Record server info
        recorder.record_gauge(
            frogdb_telemetry::metric_names::INFO,
            1.0,
            &[
                ("version", env!("CARGO_PKG_VERSION")),
                ("mode", "standalone"),
            ],
        );
        // Record maxmemory at startup
        if config.memory.maxmemory > 0 {
            recorder.record_gauge(
                frogdb_telemetry::metric_names::MEMORY_MAXMEMORY_BYTES,
                config.memory.maxmemory as f64,
                &[],
            );
        }
        (recorder.clone(), Some(recorder))
    } else {
        (Arc::new(frogdb_core::NoopMetricsRecorder::new()), None)
    };

    // Bind all listeners (RESP, admin, metrics, cluster bus)
    let bound = bind_listeners(config, listeners).await?;
    let listener = bound.resp;
    let admin_listener = bound.admin_resp;
    let metrics_listener = bound.metrics;
    let admin_http_listener = bound.admin_http;
    let cluster_bus_listener = bound.cluster_bus;

    // Create command registry
    let mut registry = CommandRegistry::new();
    crate::register_commands(&mut registry);
    let registry = Arc::new(registry);

    // Create client registry
    let client_registry = Arc::new(frogdb_core::ClientRegistry::new());

    // Create configuration manager
    let mut config_manager = ConfigManager::new(config);
    if let Some(handle) = log_reload_handle {
        config_manager.set_log_reload_handle(handle);
    }
    let config_manager = Arc::new(config_manager);

    // Determine number of shards
    let num_shards = if config.server.num_shards == 0 {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    } else {
        config.server.num_shards
    };

    info!(num_shards, "Initializing shards");

    // Create task monitor registry for tokio-metrics instrumentation
    let mut task_registry = TaskMonitorRegistry::new();
    let shard_monitor = task_registry.register("shard_worker");
    let conn_monitor = task_registry.register("connection");
    let wal_sync_monitor = task_registry.register("wal_sync");

    // Initialize persistence if enabled
    let (rocks_store, recovered_stores, periodic_sync_handle) = if config.persistence.enabled {
        let (rocks, stores, sync_handle) = startup::init_persistence(
            &config.persistence,
            num_shards,
            Some(wal_sync_monitor),
            config.tiered_storage.enabled,
        )?;
        (Some(rocks), stores, sync_handle)
    } else {
        info!("Persistence disabled");
        (
            None,
            (0..num_shards).map(|_| Default::default()).collect(),
            None,
        )
    };

    // Create snapshot coordinator
    let mut rocks_snapshot_coordinator: Option<Arc<RocksSnapshotCoordinator>> = None;
    let (snapshot_coordinator, periodic_snapshot_handle): (
        Arc<dyn SnapshotCoordinator>,
        Option<crate::net::JoinHandle<()>>,
    ) = if let Some(ref rocks) = rocks_store {
        // Real snapshot coordinator with RocksDB
        match RocksSnapshotCoordinator::new(
            rocks.clone(),
            config.snapshot.to_core_config(),
            metrics_recorder.clone(),
            config.persistence.data_dir.clone(),
        ) {
            Ok(coordinator) => {
                let coordinator = Arc::new(coordinator);
                rocks_snapshot_coordinator = Some(coordinator.clone());

                // Spawn periodic snapshot task if enabled
                let snapshot_handle = if config.snapshot.snapshot_interval_secs > 0 {
                    Some(startup::spawn_periodic_snapshot_task(
                        coordinator.clone(),
                        config.snapshot.snapshot_interval_secs,
                    ))
                } else {
                    None
                };

                (coordinator, snapshot_handle)
            }
            Err(e) => {
                warn!(error = %e, "Failed to create snapshot coordinator, using noop");
                (Arc::new(NoopSnapshotCoordinator::new()), None)
            }
        }
    } else {
        // No persistence - use noop coordinator
        (Arc::new(NoopSnapshotCoordinator::new()), None)
    };

    // Create channels for each shard
    let mut shard_senders = Vec::with_capacity(num_shards);
    let mut shard_receivers = Vec::with_capacity(num_shards);
    let mut new_conn_senders = Vec::with_capacity(num_shards);
    let mut new_conn_receivers = Vec::with_capacity(num_shards);

    for _ in 0..num_shards {
        let (msg_tx, msg_rx) = mpsc::channel(SHARD_CHANNEL_CAPACITY);
        let (conn_tx, conn_rx) = mpsc::channel(NEW_CONN_CHANNEL_CAPACITY);

        shard_senders.push(msg_tx);
        shard_receivers.push(msg_rx);
        new_conn_senders.push(conn_tx);
        new_conn_receivers.push(conn_rx);
    }

    let shard_senders = Arc::new(shard_senders);

    // Set pre-snapshot hook to flush all shard search indexes before snapshotting
    if let Some(ref coord) = rocks_snapshot_coordinator {
        let senders = shard_senders.clone();
        coord.set_pre_snapshot_hook(std::sync::Arc::new(move || {
            let senders = senders.clone();
            Box::pin(async move {
                let mut receivers = Vec::with_capacity(senders.len());
                for sender in senders.iter() {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let _ = sender
                        .send(ShardMessage::FlushSearchIndexes { response_tx: tx })
                        .await;
                    receivers.push(rx);
                }
                for rx in receivers {
                    let _ = rx.await;
                }
            })
        }));
    }

    // Create shared memory atomics for SystemMetricsCollector
    let shared_maxmemory = Arc::new(AtomicU64::new(config.memory.maxmemory));
    let shard_memory_used: Arc<Vec<AtomicU64>> =
        Arc::new((0..num_shards).map(|_| AtomicU64::new(0)).collect());

    let wal_config = build_wal_config(&config.persistence);
    let eviction_config = build_eviction_config(&config.memory);

    // Create shared slowlog ID counter for global ordering across shards
    let slowlog_next_id = Arc::new(AtomicU64::new(0));

    // Create shared function registry
    let function_registry = frogdb_core::new_shared_registry();

    // Load persisted functions from disk (if persistence is enabled)
    if config.persistence.enabled {
        let functions_path = config.persistence.data_dir.join("functions.fdb");
        match frogdb_core::load_from_file(&functions_path) {
            Ok(libraries) if !libraries.is_empty() => {
                info!(count = libraries.len(), "Loading persisted functions");
                let mut registry = function_registry.write().unwrap();
                for (name, code) in libraries {
                    match frogdb_core::load_library(&code) {
                        Ok(library) => {
                            if let Err(e) = registry.load_library(library, false) {
                                warn!(library = %name, error = %e, "Failed to load persisted function library");
                            }
                        }
                        Err(e) => {
                            warn!(library = %name, error = %e, "Failed to parse persisted function library");
                        }
                    }
                }
                info!("Persisted functions loaded");
            }
            Ok(_) => {
                // No functions to load, that's fine
            }
            Err(e) => {
                warn!(error = %e, "Failed to load persisted functions");
            }
        }
    }

    Ok(InitResult {
        listener,
        admin_listener,
        metrics_listener,
        admin_http_listener,
        cluster_bus_listener,
        registry,
        client_registry,
        config_manager,
        num_shards,
        shard_senders,
        shard_receivers,
        new_conn_senders,
        new_conn_receivers,
        rocks_store,
        recovered_stores,
        periodic_sync_handle,
        snapshot_coordinator,
        periodic_snapshot_handle,
        metrics_recorder,
        prometheus_recorder,
        health_checker,
        shared_maxmemory,
        shard_memory_used,
        wal_config,
        eviction_config,
        slowlog_next_id,
        function_registry,
        shard_monitor,
        conn_monitor,
        task_registry,
    })
}
