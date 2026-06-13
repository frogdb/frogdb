//! Infrastructure initialization phase of server construction.
//!
//! Handles metrics, listeners, registries, persistence, channels,
//! and function loading.

use anyhow::Result;
use frogdb_core::persistence::{
    NoopSnapshotCoordinator, RocksSnapshotCoordinator, RocksStore, SnapshotCoordinator,
};
use frogdb_core::sync::{Arc, AtomicU64};
use frogdb_core::{
    CommandRegistry, ExpiryIndex, HashMapStore, MetricsRecorder, ShardMessage, ShardReceiver,
    ShardSender,
};
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
    pub http_listener: Option<tokio::net::TcpListener>,
    pub cluster_bus_listener: Option<TcpListener>,
    pub registry: Arc<CommandRegistry>,
    pub client_registry: Arc<frogdb_core::ClientRegistry>,
    pub config_manager: Arc<ConfigManager>,
    pub num_shards: usize,
    pub shard_senders: Arc<Vec<ShardSender>>,
    pub shard_receivers: Vec<ShardReceiver>,
    pub new_conn_senders: Vec<mpsc::Sender<frogdb_core::shard::NewConnection>>,
    pub new_conn_receivers: Vec<mpsc::Receiver<frogdb_core::shard::NewConnection>>,
    pub rocks_store: Option<Arc<RocksStore>>,
    pub recovered_stores: Vec<(HashMapStore, ExpiryIndex)>,
    /// Replication state recovered by phase 5 (reconciled with staged metadata),
    /// consumed by the replication init phase to construct the handler.
    pub recovered_replication: frogdb_core::ReplicationState,
    pub periodic_sync_handle: Option<crate::net::JoinHandle<()>>,
    pub snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
    pub periodic_snapshot_handle: Option<crate::net::JoinHandle<()>>,
    /// Slot for the primary replication handler, consumed by the pre-snapshot
    /// hook to persist the replication offset alongside each snapshot. Filled
    /// during the replication init phase (in `mod.rs`).
    pub repl_state_save_slot:
        Arc<std::sync::OnceLock<Arc<crate::replication::PrimaryReplicationHandler>>>,
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
    #[cfg(not(feature = "turmoil"))]
    pub tls_manager: Option<Arc<crate::tls::TlsManager>>,
    pub tls_listener: Option<TcpListener>,
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
    ) = if config.http.enabled {
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
        // Record binary version metric (info gauge, always 1)
        recorder.record_gauge(
            frogdb_telemetry::metric_names::BINARY_VERSION,
            1.0,
            &[("version", env!("CARGO_PKG_VERSION"))],
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
    let http_listener = bound.http;
    let cluster_bus_listener = bound.cluster_bus;
    let tls_listener = bound.tls;

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

    // Run crash recovery through the orchestrator seam: install staged
    // checkpoint, open RocksDB, restore per-shard stores.
    let recovered = crate::recovery::recover(&crate::recovery::RecoveryInputs::from_config(
        config, num_shards,
    ))?;
    let rocks_store = recovered.rocks;
    let recovered_stores = recovered.shards;
    let persisted_functions = recovered.functions;
    let recovered_replication = recovered.replication;

    // Runtime concern: spawn the periodic WAL sync task after recovery.
    let periodic_sync_handle = startup::spawn_wal_sync_if_periodic(
        &config.persistence,
        &rocks_store,
        Some(wal_sync_monitor),
    );

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

        shard_senders.push(ShardSender::new(msg_tx));
        shard_receivers.push(ShardReceiver::new(msg_rx));
        new_conn_senders.push(conn_tx);
        new_conn_receivers.push(conn_rx);
    }

    let shard_senders = Arc::new(shard_senders);

    // Slot for the primary replication handler, filled after the replication
    // phase (which runs later). The pre-snapshot hook reads it to persist the
    // replication offset alongside each snapshot — coupling offset durability to
    // snapshot durability, the standard model (Redis stores repl-id + offset in
    // the RDB). Empty until then; an early snapshot simply skips the save.
    let repl_state_save_slot: Arc<
        std::sync::OnceLock<Arc<crate::replication::PrimaryReplicationHandler>>,
    > = Arc::new(std::sync::OnceLock::new());

    // Set pre-snapshot hook to flush all shard search indexes and persist the
    // replication offset before snapshotting.
    if let Some(ref coord) = rocks_snapshot_coordinator {
        let senders = shard_senders.clone();
        let saver = repl_state_save_slot.clone();
        coord.set_pre_snapshot_hook(std::sync::Arc::new(move || {
            let senders = senders.clone();
            let saver = saver.clone();
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
                // Persist the replication offset that matches this snapshot's data.
                if let Some(handler) = saver.get()
                    && let Err(e) = handler.save_state().await
                {
                    warn!(error = %e, "Failed to persist replication state before snapshot");
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

    // Wire the persisted function libraries recovered by phase 4 into the
    // registry. Recovery already read them from disk; this is component wiring
    // (parse each library, load it) and stays in the init layer.
    if !persisted_functions.is_empty() {
        info!(
            count = persisted_functions.len(),
            "Loading persisted functions"
        );
        let mut registry = function_registry.write().unwrap();
        for (name, code) in persisted_functions {
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

    // Initialize TLS manager if TLS is enabled
    #[cfg(not(feature = "turmoil"))]
    let tls_manager = if config.tls.enabled {
        let mgr = crate::tls::TlsManager::new(&config.tls)?;
        info!("TLS manager initialized");
        Some(Arc::new(mgr))
    } else {
        None
    };

    Ok(InitResult {
        listener,
        admin_listener,
        http_listener,
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
        recovered_replication,
        periodic_sync_handle,
        snapshot_coordinator,
        periodic_snapshot_handle,
        repl_state_save_slot,
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
        #[cfg(not(feature = "turmoil"))]
        tls_manager,
        tls_listener,
    })
}
