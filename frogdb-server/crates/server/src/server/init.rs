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
    CommandRegistry, ExpiryIndex, HashMapStore, MetricsRecorder, SearchMsg, ShardReceiver,
    ShardSender,
};
use frogdb_telemetry::otlp::{CompositeRecorder, OtlpRecorder};
use frogdb_telemetry::{HealthChecker, PrometheusRecorder, TaskMonitorRegistry};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::{Config, ConfigExt, SnapshotConfigExt};
use crate::net::TcpListener;
use crate::runtime_config::{ConfigCollaborators, ConfigManager, ShardConfigNotifier};

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
    /// ACL manager, built here so it can be injected into `config_manager` at
    /// construction (requirepass CONFIG SET/GET). Shared with the acceptors.
    pub acl_manager: Arc<frogdb_core::AclManager>,
    /// Server-wide latency histograms, built here so they can be injected into
    /// `config_manager` at construction (latency-tracking). Shared with the
    /// acceptors for INFO latencystats.
    pub latency_histograms: Arc<frogdb_core::CommandLatencyHistograms>,
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
    /// Raft storage opened by phase 6 (cluster mode only), consumed by the
    /// cluster init phase to construct the Raft instance.
    pub recovered_raft_storage: Option<frogdb_core::ClusterStorage>,
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
    /// Process-wide keyspace hit/miss accumulator, shared between the shard
    /// workers and the connection handlers (INFO / RESETSTAT).
    pub keyspace_stats: Arc<frogdb_core::KeyspaceStats>,
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

    // Prometheus recorder: built only when the HTTP observability server is
    // enabled, and kept as the concrete type so the `/metrics` scrape endpoint
    // can read counters back.
    let prometheus_recorder: Option<Arc<PrometheusRecorder>> = if config.http.enabled {
        let mut recorder = PrometheusRecorder::new();
        if config.latency_bands.enabled {
            recorder = recorder.with_latency_bands(config.latency_bands.bands.clone());
            tracing::info!(
                bands = ?config.latency_bands.bands,
                "Latency band tracking enabled"
            );
        }
        Some(Arc::new(recorder))
    } else {
        None
    };

    // OTLP recorder (optional): constructed from the `metrics` section when
    // `otlp-enabled`. It has Drop-based shutdown that flushes on process exit,
    // so it must live for the process lifetime — it does, held inside the
    // `metrics_recorder` fan-out below which the `Server` retains.
    let otlp_recorder = build_otlp_recorder(config);

    // Whether at least one real metrics backend is present. This is exactly the
    // condition under which `assemble_metrics_recorder` returns a non-noop
    // recorder, so it gates the process-identity gauges below.
    let has_metrics_backend = prometheus_recorder.is_some() || otlp_recorder.is_some();

    // Fan-out recorder handed to the runtime. When both Prometheus and OTLP are
    // present they are combined via `CompositeRecorder`; otherwise whichever
    // exists is used directly, falling back to a no-op. When `otlp-enabled` is
    // false this is byte-identical to the previous behavior.
    let metrics_recorder: Arc<dyn MetricsRecorder> =
        assemble_metrics_recorder(prometheus_recorder.clone(), otlp_recorder);

    // Record process-identity gauges whenever any real backend is active, so both
    // the Prometheus scrape endpoint and the OTLP exporter receive them — the
    // latter matters in OTLP-only mode (`http.enabled = false` +
    // `metrics.otlp-enabled = true`), where Prometheus is absent. When every
    // backend is disabled this block is skipped, keeping the all-disabled path
    // byte-identical.
    if has_metrics_backend {
        record_process_identity_gauges(&*metrics_recorder, config);
    }

    // Process-wide keyspace hit/miss accumulator. Independent of the metrics
    // recorder so INFO reports correct values even when Prometheus is disabled;
    // shared by-Arc with every shard worker and the connection handlers.
    let keyspace_stats = Arc::new(frogdb_core::KeyspaceStats::new());

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

    // Determine number of shards (needed before building the shard channels and
    // the config manager's shard notifier).
    let num_shards = if config.server.num_shards == 0 {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    } else {
        config.server.num_shards
    };

    info!(num_shards, "Initializing shards");

    // Create channels for each shard. Built before the config manager so the
    // shard config notifier (a config-manager collaborator) can be constructed
    // with real senders.
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

    // Create configuration manager with its collaborators injected at
    // construction. Building them here (rather than wiring via post-construction
    // setters) makes the side-effecting CONFIG SET paths -- requirepass,
    // maxmemory-clients eviction, latency-tracking, and shard propagation --
    // non-optional, so they can never silently no-op.
    //
    // The shard notifier borrows the manager's own runtime `Arc`, so the runtime
    // handle is built up front and shared between the notifier and the manager.
    let acl_manager = frogdb_core::AclManager::new(config.to_acl_config());
    let latency_histograms = Arc::new(frogdb_core::CommandLatencyHistograms::new(true));
    let runtime = Arc::new(std::sync::RwLock::new(
        crate::runtime_config::RuntimeConfig::from_config(config),
    ));
    let shard_notifier = Arc::new(ShardConfigNotifier::new(
        shard_senders.clone(),
        runtime.clone(),
        num_shards,
    ));
    let collaborators = ConfigCollaborators {
        acl_manager: acl_manager.clone(),
        latency_histograms: latency_histograms.clone(),
        client_eviction_registry: client_registry.clone(),
        shard_notifier,
    };
    let mut config_manager = ConfigManager::with_collaborators(config, runtime, collaborators);
    if let Some(handle) = log_reload_handle {
        config_manager.set_log_reload_handle(handle);
    }
    let config_manager = Arc::new(config_manager);

    // Create task monitor registry for tokio-metrics instrumentation
    let mut task_registry = TaskMonitorRegistry::new();
    let shard_monitor = task_registry.register("shard_worker");
    let conn_monitor = task_registry.register("connection");
    let wal_sync_monitor = task_registry.register("wal_sync");

    // Run crash recovery through the orchestrator seam: install staged
    // checkpoint, open RocksDB, restore per-shard stores.
    let recovered = crate::recovery::recover(&crate::recovery::RecoveryInputs::from_config(
        config,
        num_shards,
        metrics_recorder.clone(),
    ))?;
    let rocks_store = recovered.rocks;
    let recovered_stores = recovered.shards;
    let persisted_functions = recovered.functions;
    let recovered_replication = recovered.replication;
    let recovered_raft_storage = recovered.raft_storage;

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
                        .send(SearchMsg::FlushSearchIndexes { response_tx: tx })
                        .await;
                    receivers.push(rx);
                }
                for rx in receivers {
                    let _ = rx.await;
                }
                // Drain every shard's WAL flush-engine into RocksDB so the
                // checkpoint captures all acknowledged writes. Under non-`sync`
                // durability a write is acked once staged in the flush engine and
                // committed to RocksDB only on a later size/timeout trigger; without
                // this drain, `BGSAVE` would snapshot a RocksDB missing the most
                // recent writes — a silently-incomplete recovery artifact. Done
                // after the search flush so any search_meta writes are drained too.
                let mut wal_receivers = Vec::with_capacity(senders.len());
                for sender in senders.iter() {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let _ = sender.send(SearchMsg::FlushWal { response_tx: tx }).await;
                    wal_receivers.push(rx);
                }
                for rx in wal_receivers {
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
        acl_manager,
        latency_histograms,
        num_shards,
        shard_senders,
        shard_receivers,
        new_conn_senders,
        new_conn_receivers,
        rocks_store,
        recovered_stores,
        recovered_replication,
        recovered_raft_storage,
        periodic_sync_handle,
        snapshot_coordinator,
        periodic_snapshot_handle,
        repl_state_save_slot,
        metrics_recorder,
        prometheus_recorder,
        keyspace_stats,
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

/// Build the OTLP metrics recorder from the `metrics` config section.
///
/// Returns `None` when OTLP export is disabled (`otlp-enabled = false`) or when
/// the exporter fails to initialize. The config-crate `MetricsConfig` is mapped
/// onto the telemetry crate's own `MetricsConfig` (the two are intentionally
/// decoupled — telemetry does not depend on the config crate).
fn build_otlp_recorder(config: &Config) -> Option<Arc<dyn MetricsRecorder>> {
    if !config.metrics.otlp_enabled {
        return None;
    }

    let telemetry_config = frogdb_telemetry::MetricsConfig {
        enabled: config.metrics.enabled,
        port: config.metrics.port,
        otlp_enabled: config.metrics.otlp_enabled,
        otlp_endpoint: config.metrics.otlp_endpoint.clone(),
        otlp_interval_secs: config.metrics.otlp_interval_secs,
        ..Default::default()
    };

    OtlpRecorder::new(&telemetry_config)
        .map(|recorder| Arc::new(recorder) as Arc<dyn MetricsRecorder>)
}

/// Record the process-identity gauges (server info, binary version, and the
/// configured maxmemory) through the fan-out recorder. Called during init
/// whenever at least one real metrics backend is present so every backend --
/// Prometheus and/or OTLP -- receives them.
fn record_process_identity_gauges(recorder: &dyn MetricsRecorder, config: &Config) {
    // Record server info
    frogdb_telemetry::definitions::Info::set(
        recorder,
        1.0,
        env!("CARGO_PKG_VERSION"),
        "standalone",
    );
    // Record binary version metric (info gauge, always 1)
    frogdb_telemetry::definitions::BinaryVersion::set(recorder, 1.0, env!("CARGO_PKG_VERSION"));
    // Record maxmemory at startup
    if config.memory.maxmemory > 0 {
        frogdb_telemetry::definitions::MemoryMaxmemoryBytes::set(
            recorder,
            config.memory.maxmemory as f64,
        );
    }
}

/// Combine the Prometheus recorder (if any) and the OTLP recorder (if any) into
/// the single fan-out recorder handed to the runtime.
///
/// When both backends are present they are wrapped in a [`CompositeRecorder` so
/// every sample reaches both. When only one is present it is used directly
/// (preserving `Arc` identity with the Prometheus recorder in the
/// Prometheus-only case). When neither is present a no-op recorder is returned.
fn assemble_metrics_recorder(
    prometheus: Option<Arc<PrometheusRecorder>>,
    otlp: Option<Arc<dyn MetricsRecorder>>,
) -> Arc<dyn MetricsRecorder> {
    match (prometheus, otlp) {
        (Some(prometheus), Some(otlp)) => Arc::new(CompositeRecorder::new(vec![prometheus, otlp])),
        (Some(prometheus), None) => prometheus,
        (None, Some(otlp)) => otlp,
        (None, None) => Arc::new(frogdb_core::NoopMetricsRecorder::new()),
    }
}

#[cfg(test)]
mod metrics_recorder_tests {
    use super::*;
    use frogdb_core::MetricsRecorder;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Test recorder that counts the `increment_counter` and `record_gauge`
    /// calls it receives, letting a test observe whether it was included in a
    /// composite fan-out and whether the identity gauges reached it.
    #[derive(Default)]
    struct CountingRecorder {
        increments: AtomicU64,
        gauges: AtomicU64,
    }

    impl MetricsRecorder for CountingRecorder {
        fn increment_counter(&self, _name: &str, value: u64, _labels: &[(&str, &str)]) {
            self.increments.fetch_add(value, Ordering::Relaxed);
        }
        fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {
            self.gauges.fetch_add(1, Ordering::Relaxed);
        }
        fn record_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    }

    #[test]
    fn build_otlp_recorder_none_when_disabled() {
        let mut config = Config::default();
        config.metrics.otlp_enabled = false;
        assert!(build_otlp_recorder(&config).is_none());
    }

    #[tokio::test]
    async fn build_otlp_recorder_constructs_when_enabled() {
        // Constructing the OTLP exporter builds a lazy tonic channel (no live
        // collector required) and spawns a periodic reader, so it needs a Tokio
        // runtime — hence `#[tokio::test]`.
        let mut config = Config::default();
        config.metrics.otlp_enabled = true;
        let recorder = build_otlp_recorder(&config);
        assert!(
            recorder.is_some(),
            "otlp-enabled = true must yield an OTLP recorder"
        );
    }

    #[test]
    fn assemble_fans_out_to_otlp_backend() {
        // Prometheus present + OTLP present => composite that fans out to BOTH.
        let prometheus = Arc::new(PrometheusRecorder::new());
        let otlp_backend = Arc::new(CountingRecorder::default());
        let otlp_dyn: Arc<dyn MetricsRecorder> = otlp_backend.clone();

        let recorder = assemble_metrics_recorder(Some(prometheus.clone()), Some(otlp_dyn));
        // UFCS: chokepoint lint targets method-call emission syntax
        MetricsRecorder::increment_counter(recorder.as_ref(), "wired_total", 3, &[("k", "v")]);

        // The OTLP-slot backend received the sample => it is in the composite.
        assert_eq!(otlp_backend.increments.load(Ordering::Relaxed), 3);
        // Prometheus also received it.
        assert!(prometheus.encode().contains("wired_total"));
    }

    #[test]
    fn assemble_prometheus_only_preserves_identity() {
        // No OTLP => the Prometheus Arc is used directly (no composite wrapper).
        let prometheus = Arc::new(PrometheusRecorder::new());
        let recorder = assemble_metrics_recorder(Some(prometheus.clone()), None);
        assert!(Arc::ptr_eq(
            &(prometheus as Arc<dyn MetricsRecorder>),
            &recorder
        ));
    }

    #[test]
    fn otlp_only_assembly_receives_identity_gauges() {
        // Regression: in OTLP-only mode (no Prometheus / `http.enabled = false`)
        // the process-identity gauges must still reach the OTLP backend. Assemble
        // an OTLP-only recorder (None prometheus, Some otlp) and record the
        // identity gauges through the fan-out; the OTLP-slot backend must observe
        // them.
        let otlp_backend = Arc::new(CountingRecorder::default());
        let otlp_dyn: Arc<dyn MetricsRecorder> = otlp_backend.clone();
        let recorder = assemble_metrics_recorder(None, Some(otlp_dyn));

        let mut config = Config::default();
        // Ensure the maxmemory gauge fires too: Info + BinaryVersion + maxmemory.
        config.memory.maxmemory = 1024;
        record_process_identity_gauges(&*recorder, &config);

        assert_eq!(
            otlp_backend.gauges.load(Ordering::Relaxed),
            3,
            "OTLP-only assembly must receive all three identity gauges"
        );
    }

    #[test]
    fn assemble_none_yields_noop() {
        // Neither backend => a no-op recorder that cannot read counters back.
        let recorder = assemble_metrics_recorder(None, None);
        // UFCS: chokepoint lint targets method-call emission syntax
        MetricsRecorder::increment_counter(recorder.as_ref(), "x", 1, &[]);
        assert!(recorder.counter_value("x").is_none());
    }
}
