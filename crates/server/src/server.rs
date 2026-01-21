//! Main server implementation.

use anyhow::Result;
use frogdb_core::persistence::{
    recover_all_shards, spawn_periodic_sync, CompressionType, DurabilityMode, NoopSnapshotCoordinator,
    RocksConfig, RocksSnapshotCoordinator, RocksStore, SnapshotCoordinator, WalConfig,
};
use frogdb_core::sync::{Arc, AtomicU64, Ordering};
use frogdb_core::{AclManager, ClientRegistry, CommandRegistry, EvictionConfig, EvictionPolicy, MetricsRecorder, ShardMessage, ShardWorker};
use frogdb_metrics::{HealthChecker, MetricsServer, PrometheusRecorder, SystemMetricsCollector};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::acceptor::Acceptor;
use crate::config::{Config, MemoryConfig, PersistenceConfig};
use crate::runtime_config::ConfigManager;

/// Channel capacity for shard message queues.
const SHARD_CHANNEL_CAPACITY: usize = 1024;

/// Channel capacity for new connection queues.
const NEW_CONN_CHANNEL_CAPACITY: usize = 256;

/// Global connection ID counter.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Global transaction ID counter for VLL (Very Lightweight Locking).
static NEXT_TXID: AtomicU64 = AtomicU64::new(1);

/// Generate a unique connection ID.
pub fn next_conn_id() -> u64 {
    NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
}

/// Generate a unique transaction ID for scatter-gather operations.
pub fn next_txid() -> u64 {
    NEXT_TXID.fetch_add(1, Ordering::SeqCst)
}

/// Result type for persistence initialization.
type PersistenceInitResult = (
    Arc<RocksStore>,
    Vec<(frogdb_core::HashMapStore, frogdb_core::ExpiryIndex)>,
    Option<tokio::task::JoinHandle<()>>,
);

/// FrogDB server.
pub struct Server {
    /// Server configuration.
    config: Config,

    /// TCP listener.
    listener: TcpListener,

    /// Command registry.
    registry: Arc<CommandRegistry>,

    /// Client registry for CLIENT commands.
    client_registry: Arc<ClientRegistry>,

    /// Configuration manager for CONFIG commands.
    config_manager: Arc<ConfigManager>,

    /// Shard message senders.
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// New connection senders (one per shard).
    new_conn_senders: Vec<mpsc::Sender<frogdb_core::shard::NewConnection>>,

    /// Shard worker handles.
    shard_handles: Vec<tokio::task::JoinHandle<()>>,

    /// Optional RocksDB store for persistence.
    rocks_store: Option<Arc<RocksStore>>,

    /// Optional periodic sync task handle.
    periodic_sync_handle: Option<tokio::task::JoinHandle<()>>,

    /// Optional periodic snapshot task handle.
    periodic_snapshot_handle: Option<tokio::task::JoinHandle<()>>,

    /// Snapshot coordinator (shared across all shards).
    snapshot_coordinator: Arc<dyn SnapshotCoordinator>,

    /// Metrics recorder.
    metrics_recorder: Arc<dyn MetricsRecorder>,

    /// Prometheus recorder (for HTTP endpoint).
    prometheus_recorder: Option<Arc<PrometheusRecorder>>,

    /// Health checker.
    health_checker: HealthChecker,

    /// ACL manager for authentication and authorization.
    acl_manager: Arc<AclManager>,
}

impl Server {
    /// Create a new server instance.
    pub async fn new(config: Config) -> Result<Self> {
        // Initialize metrics
        let health_checker = HealthChecker::new();
        let (metrics_recorder, prometheus_recorder): (
            Arc<dyn MetricsRecorder>,
            Option<Arc<PrometheusRecorder>>,
        ) = if config.metrics.enabled {
            let recorder = Arc::new(PrometheusRecorder::new());
            // Record server info
            recorder.record_gauge(
                frogdb_metrics::metric_names::INFO,
                1.0,
                &[
                    ("version", env!("CARGO_PKG_VERSION")),
                    ("mode", "standalone"),
                ],
            );
            // Record maxmemory at startup
            if config.memory.maxmemory > 0 {
                recorder.record_gauge(
                    frogdb_metrics::metric_names::MEMORY_MAXMEMORY_BYTES,
                    config.memory.maxmemory as f64,
                    &[],
                );
            }
            (recorder.clone(), Some(recorder))
        } else {
            (Arc::new(frogdb_core::NoopMetricsRecorder::new()), None)
        };

        // Bind TCP listener
        let listener = TcpListener::bind(config.bind_addr()).await?;

        info!(
            addr = %config.bind_addr(),
            "TCP listener bound"
        );

        // Create command registry
        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);
        let registry = Arc::new(registry);

        // Create client registry
        let client_registry = Arc::new(ClientRegistry::new());

        // Create configuration manager
        let config_manager = Arc::new(ConfigManager::new(&config));

        // Determine number of shards
        let num_shards = if config.server.num_shards == 0 {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        } else {
            config.server.num_shards
        };

        info!(num_shards, "Initializing shards");

        // Initialize persistence if enabled
        let (rocks_store, recovered_stores, periodic_sync_handle) =
            if config.persistence.enabled {
                let (rocks, stores, sync_handle) =
                    Self::init_persistence(&config.persistence, num_shards)?;
                (Some(rocks), Some(stores), sync_handle)
            } else {
                info!("Persistence disabled");
                (None, None, None)
            };

        // Create snapshot coordinator
        let (snapshot_coordinator, periodic_snapshot_handle): (
            Arc<dyn SnapshotCoordinator>,
            Option<tokio::task::JoinHandle<()>>,
        ) = if let Some(ref rocks) = rocks_store {
            // Real snapshot coordinator with RocksDB
            match RocksSnapshotCoordinator::new(
                rocks.clone(),
                config.snapshot.to_core_config(),
                metrics_recorder.clone(),
            ) {
                Ok(coordinator) => {
                    let coordinator = Arc::new(coordinator);

                    // Spawn periodic snapshot task if enabled
                    let snapshot_handle = if config.snapshot.snapshot_interval_secs > 0 {
                        Some(Self::spawn_periodic_snapshot_task(
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

        // Spawn shard workers
        let mut shard_handles = Vec::with_capacity(num_shards);
        let wal_config = build_wal_config(&config.persistence);

        // Build eviction config from memory settings
        let eviction_config = build_eviction_config(&config.memory);

        // Create shared slowlog ID counter for global ordering across shards
        let slowlog_next_id = Arc::new(AtomicU64::new(0));

        // Convert recovered stores to an iterator if available
        let mut recovered_iter = recovered_stores.map(|v| v.into_iter());

        for (shard_id, (msg_rx, conn_rx)) in shard_receivers
            .into_iter()
            .zip(new_conn_receivers.into_iter())
            .enumerate()
        {
            let worker = if let Some(ref rocks) = rocks_store {
                // Get recovered store for this shard
                let (store, _expiry_index) = recovered_iter
                    .as_mut()
                    .and_then(|iter| iter.next())
                    .unwrap_or_default();

                ShardWorker::with_persistence(
                    shard_id,
                    num_shards,
                    store,
                    msg_rx,
                    conn_rx,
                    shard_senders.clone(),
                    registry.clone(),
                    rocks.clone(),
                    wal_config.clone(),
                    snapshot_coordinator.clone(),
                    eviction_config.clone(),
                    metrics_recorder.clone(),
                    slowlog_next_id.clone(),
                )
            } else {
                ShardWorker::with_eviction(
                    shard_id,
                    num_shards,
                    msg_rx,
                    conn_rx,
                    shard_senders.clone(),
                    registry.clone(),
                    eviction_config.clone(),
                    metrics_recorder.clone(),
                    slowlog_next_id.clone(),
                )
            };

            let handle = tokio::spawn(async move {
                worker.run().await;
            });

            shard_handles.push(handle);
        }

        // Create ACL manager
        let acl_manager = AclManager::new(config.to_acl_config());

        Ok(Self {
            config,
            listener,
            registry,
            client_registry,
            config_manager,
            shard_senders,
            new_conn_senders,
            shard_handles,
            rocks_store,
            periodic_sync_handle,
            periodic_snapshot_handle,
            snapshot_coordinator,
            metrics_recorder,
            prometheus_recorder,
            health_checker,
            acl_manager,
        })
    }

    /// Spawn periodic snapshot task.
    fn spawn_periodic_snapshot_task(
        coordinator: Arc<dyn SnapshotCoordinator>,
        interval_secs: u64,
    ) -> tokio::task::JoinHandle<()> {
        info!(
            interval_secs,
            "Starting periodic snapshot task"
        );

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

            loop {
                interval.tick().await;

                if coordinator.in_progress() {
                    tracing::debug!("Skipping periodic snapshot - already in progress");
                    continue;
                }

                match coordinator.start_snapshot() {
                    Ok(handle) => {
                        tracing::info!(epoch = handle.epoch(), "Periodic snapshot started");
                        // Handle completes when background task finishes
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Periodic snapshot failed to start");
                    }
                }
            }
        })
    }

    /// Initialize persistence layer.
    fn init_persistence(
        config: &PersistenceConfig,
        num_shards: usize,
    ) -> Result<PersistenceInitResult> {
        use std::fs;

        info!(
            data_dir = %config.data_dir.display(),
            durability_mode = %config.durability_mode,
            "Initializing persistence"
        );

        // Ensure data directory exists
        fs::create_dir_all(&config.data_dir)?;

        // Build RocksDB config
        let rocks_config = RocksConfig {
            write_buffer_size: config.write_buffer_size_mb * 1024 * 1024,
            compression: parse_compression(&config.compression),
            max_background_jobs: num_cpus::get() as i32,
            create_if_missing: true,
        };

        // Open RocksDB
        let rocks = Arc::new(RocksStore::open(
            &config.data_dir,
            num_shards,
            &rocks_config,
        )?);

        // Recover data if database has existing data
        let recovered = if rocks.has_data() {
            info!("Recovering data from RocksDB...");
            let (stores, stats) = recover_all_shards(&rocks)?;
            info!(
                keys_loaded = stats.keys_loaded,
                keys_expired = stats.keys_expired_skipped,
                bytes = stats.bytes_loaded,
                duration_ms = stats.duration_ms,
                "Recovery complete"
            );
            stores
        } else {
            info!("No existing data found, starting fresh");
            (0..num_shards)
                .map(|_| Default::default())
                .collect()
        };

        // Start periodic sync if using periodic durability mode
        let sync_handle = if config.durability_mode.to_lowercase() == "periodic" {
            info!(
                interval_ms = config.sync_interval_ms,
                "Starting periodic WAL sync"
            );
            Some(spawn_periodic_sync(rocks.clone(), config.sync_interval_ms))
        } else {
            None
        };

        Ok((rocks, recovered, sync_handle))
    }

    /// Run the server.
    pub async fn run(self) -> Result<()> {
        // Start metrics server if enabled
        let metrics_server_handle = if let Some(ref prometheus) = self.prometheus_recorder {
            let metrics_config = frogdb_metrics::MetricsConfig {
                enabled: self.config.metrics.enabled,
                bind: self.config.metrics.bind.clone(),
                port: self.config.metrics.port,
                otlp_enabled: self.config.metrics.otlp_enabled,
                otlp_endpoint: self.config.metrics.otlp_endpoint.clone(),
                otlp_interval_secs: self.config.metrics.otlp_interval_secs,
            };

            let server = MetricsServer::new(
                metrics_config,
                prometheus.clone(),
                self.health_checker.clone(),
            );

            info!(
                addr = %self.config.metrics.bind_addr(),
                "Metrics server starting"
            );

            Some(server.spawn())
        } else {
            None
        };

        // Start system metrics collector if metrics enabled
        let system_collector_handle = if self.prometheus_recorder.is_some() {
            Some(SystemMetricsCollector::spawn_collector(
                self.metrics_recorder.clone(),
                Duration::from_secs(15),
            ))
        } else {
            None
        };

        let acceptor = Acceptor::new(
            self.listener,
            self.new_conn_senders,
            self.shard_senders.clone(),
            self.registry.clone(),
            self.client_registry.clone(),
            self.config_manager.clone(),
            self.config.server.allow_cross_slot_standalone,
            self.config.server.scatter_gather_timeout_ms,
            self.metrics_recorder.clone(),
            self.acl_manager.clone(),
            self.snapshot_coordinator.clone(),
        );

        // Spawn acceptor task
        let acceptor_handle = tokio::spawn(async move {
            if let Err(e) = acceptor.run().await {
                error!(error = %e, "Acceptor error");
            }
        });

        // Mark server as ready
        self.health_checker.set_ready();

        info!(
            addr = %self.config.bind_addr(),
            "FrogDB server ready"
        );

        // Wait for shutdown signal
        shutdown_signal().await;

        info!("Shutdown signal received, stopping server...");

        // Mark server as not ready during shutdown
        self.health_checker.shutdown();

        // Send shutdown to all shards
        for sender in self.shard_senders.iter() {
            let _ = sender.send(ShardMessage::Shutdown).await;
        }

        // Wait for shard workers to finish
        for handle in self.shard_handles {
            let _ = handle.await;
        }

        // Stop periodic sync task if running
        if let Some(handle) = self.periodic_sync_handle {
            handle.abort();
        }

        // Stop periodic snapshot task if running
        if let Some(handle) = self.periodic_snapshot_handle {
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

        // Stop metrics server and system collector
        if let Some(handle) = metrics_server_handle {
            handle.abort();
        }
        if let Some(handle) = system_collector_handle {
            handle.abort();
        }

        // Final flush of RocksDB
        if let Some(ref rocks) = self.rocks_store {
            if let Err(e) = rocks.flush() {
                error!(error = %e, "Failed to flush RocksDB on shutdown");
            } else {
                info!("RocksDB flushed successfully");
            }
        }

        // Abort acceptor
        acceptor_handle.abort();

        info!("Server shutdown complete");

        Ok(())
    }

    /// Get the snapshot coordinator.
    pub fn snapshot_coordinator(&self) -> &Arc<dyn SnapshotCoordinator> {
        &self.snapshot_coordinator
    }
}

/// Parse compression type from config string.
fn parse_compression(s: &str) -> CompressionType {
    match s.to_lowercase().as_str() {
        "none" => CompressionType::None,
        "snappy" => CompressionType::Snappy,
        "lz4" => CompressionType::Lz4,
        "zstd" => CompressionType::Zstd,
        _ => {
            warn!(compression = %s, "Unknown compression type, using LZ4");
            CompressionType::Lz4
        }
    }
}

/// Build WAL config from persistence config.
fn build_wal_config(config: &PersistenceConfig) -> WalConfig {
    let mode = match config.durability_mode.to_lowercase().as_str() {
        "async" => DurabilityMode::Async,
        "sync" => DurabilityMode::Sync,
        _ => DurabilityMode::Periodic {
            interval_ms: config.sync_interval_ms,
        },
    };

    WalConfig {
        mode,
        batch_size_threshold: config.batch_size_threshold_kb * 1024,
        batch_timeout_ms: config.batch_timeout_ms,
    }
}

/// Build eviction config from memory config.
fn build_eviction_config(config: &MemoryConfig) -> EvictionConfig {
    let policy = config
        .maxmemory_policy
        .parse::<EvictionPolicy>()
        .unwrap_or_else(|_| {
            warn!(
                policy = %config.maxmemory_policy,
                "Unknown eviction policy, using noeviction"
            );
            EvictionPolicy::NoEviction
        });

    EvictionConfig {
        maxmemory: config.maxmemory,
        policy,
        maxmemory_samples: config.maxmemory_samples,
        lfu_log_factor: config.lfu_log_factor,
        lfu_decay_time: config.lfu_decay_time,
    }
}

/// Helper module for CPU count.
mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    }
}

/// Wait for a shutdown signal (SIGTERM or SIGINT).
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Register all built-in commands.
pub fn register_commands(registry: &mut CommandRegistry) {
    // Connection commands
    registry.register(commands::PingCommand);
    registry.register(commands::EchoCommand);
    registry.register(commands::QuitCommand);
    registry.register(commands::CommandCommand);
    registry.register(crate::commands::HelloCommand);

    // String commands (basic)
    registry.register(commands::GetCommand);
    registry.register(commands::SetCommand);
    registry.register(commands::DelCommand);
    registry.register(commands::ExistsCommand);

    // String commands (extended)
    registry.register(crate::commands::string::SetnxCommand);
    registry.register(crate::commands::string::SetexCommand);
    registry.register(crate::commands::string::PsetexCommand);
    registry.register(crate::commands::string::AppendCommand);
    registry.register(crate::commands::string::StrlenCommand);
    registry.register(crate::commands::string::GetrangeCommand);
    registry.register(crate::commands::string::SetrangeCommand);
    registry.register(crate::commands::string::GetdelCommand);
    registry.register(crate::commands::string::GetexCommand);

    // Numeric commands
    registry.register(crate::commands::string::IncrCommand);
    registry.register(crate::commands::string::DecrCommand);
    registry.register(crate::commands::string::IncrbyCommand);
    registry.register(crate::commands::string::DecrbyCommand);
    registry.register(crate::commands::string::IncrbyfloatCommand);

    // Multi-key string commands
    registry.register(crate::commands::string::MgetCommand);
    registry.register(crate::commands::string::MsetCommand);
    registry.register(crate::commands::string::MsetnxCommand);

    // TTL/Expiry commands
    registry.register(crate::commands::expiry::ExpireCommand);
    registry.register(crate::commands::expiry::PexpireCommand);
    registry.register(crate::commands::expiry::ExpireatCommand);
    registry.register(crate::commands::expiry::PexpireatCommand);
    registry.register(crate::commands::expiry::TtlCommand);
    registry.register(crate::commands::expiry::PttlCommand);
    registry.register(crate::commands::expiry::PersistCommand);
    registry.register(crate::commands::expiry::ExpiretimeCommand);
    registry.register(crate::commands::expiry::PexpiretimeCommand);

    // Generic commands
    registry.register(crate::commands::generic::TypeCommand);
    registry.register(crate::commands::generic::RenameCommand);
    registry.register(crate::commands::generic::RenamenxCommand);
    registry.register(crate::commands::generic::TouchCommand);
    registry.register(crate::commands::generic::UnlinkCommand);
    registry.register(crate::commands::generic::ObjectCommand);
    registry.register(crate::commands::generic::DebugCommand);

    // Sorted set commands - basic
    registry.register(crate::commands::sorted_set::ZaddCommand);
    registry.register(crate::commands::sorted_set::ZremCommand);
    registry.register(crate::commands::sorted_set::ZscoreCommand);
    registry.register(crate::commands::sorted_set::ZmscoreCommand);
    registry.register(crate::commands::sorted_set::ZcardCommand);
    registry.register(crate::commands::sorted_set::ZincrbyCommand);

    // Sorted set commands - ranking
    registry.register(crate::commands::sorted_set::ZrankCommand);
    registry.register(crate::commands::sorted_set::ZrevrankCommand);

    // Sorted set commands - range queries
    registry.register(crate::commands::sorted_set::ZrangeCommand);
    registry.register(crate::commands::sorted_set::ZrangebyscoreCommand);
    registry.register(crate::commands::sorted_set::ZrevrangebyscoreCommand);
    registry.register(crate::commands::sorted_set::ZrangebylexCommand);
    registry.register(crate::commands::sorted_set::ZrevrangebylexCommand);
    registry.register(crate::commands::sorted_set::ZcountCommand);
    registry.register(crate::commands::sorted_set::ZlexcountCommand);

    // Sorted set commands - pop & random
    registry.register(crate::commands::sorted_set::ZpopminCommand);
    registry.register(crate::commands::sorted_set::ZpopmaxCommand);
    registry.register(crate::commands::sorted_set::ZmpopCommand);
    registry.register(crate::commands::sorted_set::ZrandmemberCommand);

    // Sorted set commands - set operations
    registry.register(crate::commands::sorted_set::ZunionCommand);
    registry.register(crate::commands::sorted_set::ZunionstoreCommand);
    registry.register(crate::commands::sorted_set::ZinterCommand);
    registry.register(crate::commands::sorted_set::ZinterstoreCommand);
    registry.register(crate::commands::sorted_set::ZintercardCommand);
    registry.register(crate::commands::sorted_set::ZdiffCommand);
    registry.register(crate::commands::sorted_set::ZdiffstoreCommand);

    // Sorted set commands - other
    registry.register(crate::commands::sorted_set::ZscanCommand);
    registry.register(crate::commands::sorted_set::ZrangestoreCommand);
    registry.register(crate::commands::sorted_set::ZremrangebyrankCommand);
    registry.register(crate::commands::sorted_set::ZremrangebyscoreCommand);
    registry.register(crate::commands::sorted_set::ZremrangebylexCommand);

    // Persistence commands
    registry.register(crate::commands::persistence::BgsaveCommand);
    registry.register(crate::commands::persistence::LastsaveCommand);
    registry.register(crate::commands::persistence::DumpCommand);
    registry.register(crate::commands::persistence::RestoreCommand);

    // Hash commands
    registry.register(crate::commands::hash::HsetCommand);
    registry.register(crate::commands::hash::HsetnxCommand);
    registry.register(crate::commands::hash::HgetCommand);
    registry.register(crate::commands::hash::HdelCommand);
    registry.register(crate::commands::hash::HmsetCommand);
    registry.register(crate::commands::hash::HmgetCommand);
    registry.register(crate::commands::hash::HgetallCommand);
    registry.register(crate::commands::hash::HkeysCommand);
    registry.register(crate::commands::hash::HvalsCommand);
    registry.register(crate::commands::hash::HexistsCommand);
    registry.register(crate::commands::hash::HlenCommand);
    registry.register(crate::commands::hash::HincrbyCommand);
    registry.register(crate::commands::hash::HincrbyfloatCommand);
    registry.register(crate::commands::hash::HstrlenCommand);
    registry.register(crate::commands::hash::HscanCommand);
    registry.register(crate::commands::hash::HrandfieldCommand);

    // Set commands
    registry.register(crate::commands::set::SaddCommand);
    registry.register(crate::commands::set::SremCommand);
    registry.register(crate::commands::set::SmembersCommand);
    registry.register(crate::commands::set::SismemberCommand);
    registry.register(crate::commands::set::SmismemberCommand);
    registry.register(crate::commands::set::ScardCommand);
    registry.register(crate::commands::set::SunionCommand);
    registry.register(crate::commands::set::SinterCommand);
    registry.register(crate::commands::set::SdiffCommand);
    registry.register(crate::commands::set::SunionstoreCommand);
    registry.register(crate::commands::set::SinterstoreCommand);
    registry.register(crate::commands::set::SdiffstoreCommand);
    registry.register(crate::commands::set::SintercardCommand);
    registry.register(crate::commands::set::SrandmemberCommand);
    registry.register(crate::commands::set::SpopCommand);
    registry.register(crate::commands::set::SmoveCommand);
    registry.register(crate::commands::set::SscanCommand);

    // List commands
    registry.register(crate::commands::list::LpushCommand);
    registry.register(crate::commands::list::RpushCommand);
    registry.register(crate::commands::list::LpushxCommand);
    registry.register(crate::commands::list::RpushxCommand);
    registry.register(crate::commands::list::LpopCommand);
    registry.register(crate::commands::list::RpopCommand);
    registry.register(crate::commands::list::LlenCommand);
    registry.register(crate::commands::list::LrangeCommand);
    registry.register(crate::commands::list::LindexCommand);
    registry.register(crate::commands::list::LsetCommand);
    registry.register(crate::commands::list::LinsertCommand);
    registry.register(crate::commands::list::LremCommand);
    registry.register(crate::commands::list::LtrimCommand);
    registry.register(crate::commands::list::LposCommand);
    registry.register(crate::commands::list::LmoveCommand);
    registry.register(crate::commands::list::LmpopCommand);

    // Blocking commands (list and sorted set)
    registry.register(crate::commands::blocking::BlpopCommand);
    registry.register(crate::commands::blocking::BrpopCommand);
    registry.register(crate::commands::blocking::BlmoveCommand);
    registry.register(crate::commands::blocking::BlmpopCommand);
    registry.register(crate::commands::blocking::BzpopminCommand);
    registry.register(crate::commands::blocking::BzpopmaxCommand);
    registry.register(crate::commands::blocking::BzmpopCommand);
    registry.register(crate::commands::blocking::BrpoplpushCommand);

    // Transaction commands
    registry.register(crate::commands::transaction::MultiCommand);
    registry.register(crate::commands::transaction::ExecCommand);
    registry.register(crate::commands::transaction::DiscardCommand);
    registry.register(crate::commands::transaction::WatchCommand);
    registry.register(crate::commands::transaction::UnwatchCommand);

    // Scripting commands
    registry.register(crate::commands::scripting::EvalCommand);
    registry.register(crate::commands::scripting::EvalshaCommand);
    registry.register(crate::commands::scripting::ScriptCommand);

    // Scan commands
    registry.register(crate::commands::scan::ScanCommand);
    registry.register(crate::commands::scan::KeysCommand);

    // Server commands
    registry.register(crate::commands::server::DbsizeCommand);
    registry.register(crate::commands::server::FlushdbCommand);
    registry.register(crate::commands::server::FlushallCommand);
    registry.register(crate::commands::server::TimeCommand);
    registry.register(crate::commands::server::ShutdownCommand);

    // Info command
    registry.register(crate::commands::info::InfoCommand);

    // Client commands (handled specially in connection.rs, but registered for introspection)
    registry.register(crate::commands::client::ClientCommand);

    // Config commands (handled specially in connection.rs, but registered for introspection)
    registry.register(crate::commands::config::ConfigCommand);

    // Slowlog commands (handled specially in connection.rs, but registered for introspection)
    registry.register(crate::commands::slowlog::SlowlogCommand);

    // Stream commands
    registry.register(crate::commands::stream::XaddCommand);
    registry.register(crate::commands::stream::XlenCommand);
    registry.register(crate::commands::stream::XrangeCommand);
    registry.register(crate::commands::stream::XrevrangeCommand);
    registry.register(crate::commands::stream::XdelCommand);
    registry.register(crate::commands::stream::XtrimCommand);
    registry.register(crate::commands::stream::XreadCommand);
    registry.register(crate::commands::stream::XgroupCommand);
    registry.register(crate::commands::stream::XreadgroupCommand);
    registry.register(crate::commands::stream::XackCommand);
    registry.register(crate::commands::stream::XpendingCommand);
    registry.register(crate::commands::stream::XclaimCommand);
    registry.register(crate::commands::stream::XautoclaimCommand);
    registry.register(crate::commands::stream::XinfoCommand);
    registry.register(crate::commands::stream::XsetidCommand);

    // Auth/ACL commands (handled specially in connection.rs, but registered for introspection)
    registry.register(crate::commands::auth::Auth);
    registry.register(crate::commands::acl::Acl);

    // Bitmap commands
    registry.register(crate::commands::bitmap::SetbitCommand);
    registry.register(crate::commands::bitmap::GetbitCommand);
    registry.register(crate::commands::bitmap::BitcountCommand);
    registry.register(crate::commands::bitmap::BitopCommand);
    registry.register(crate::commands::bitmap::BitposCommand);
    registry.register(crate::commands::bitmap::BitfieldCommand);
    registry.register(crate::commands::bitmap::BitfieldRoCommand);

    // Geo commands
    registry.register(crate::commands::geo::GeoaddCommand);
    registry.register(crate::commands::geo::GeodistCommand);
    registry.register(crate::commands::geo::GeohashCommand);
    registry.register(crate::commands::geo::GeoposCommand);
    registry.register(crate::commands::geo::GeosearchCommand);
    registry.register(crate::commands::geo::GeosearchstoreCommand);
    registry.register(crate::commands::geo::GeoradiusCommand);
    registry.register(crate::commands::geo::GeoradiusbymemberCommand);

    // Bloom filter commands
    registry.register(crate::commands::bloom::BfReserve);
    registry.register(crate::commands::bloom::BfAdd);
    registry.register(crate::commands::bloom::BfMadd);
    registry.register(crate::commands::bloom::BfExists);
    registry.register(crate::commands::bloom::BfMexists);
    registry.register(crate::commands::bloom::BfInsert);
    registry.register(crate::commands::bloom::BfInfo);
    registry.register(crate::commands::bloom::BfCard);
    registry.register(crate::commands::bloom::BfScandump);
    registry.register(crate::commands::bloom::BfLoadchunk);

    // HyperLogLog commands
    registry.register(crate::commands::hyperloglog::PfaddCommand);
    registry.register(crate::commands::hyperloglog::PfcountCommand);
    registry.register(crate::commands::hyperloglog::PfmergeCommand);
    registry.register(crate::commands::hyperloglog::PfdebugCommand);
    registry.register(crate::commands::hyperloglog::PfselftestCommand);
}

// Commands module
pub mod commands {
    use bytes::Bytes;
    use frogdb_core::{
        Arity, Command, CommandContext, CommandError, CommandFlags, Expiry, SetCondition,
        SetOptions, SetResult, Value,
    };
    use frogdb_protocol::Response;

    /// PING command.
    pub struct PingCommand;

    impl Command for PingCommand {
        fn name(&self) -> &'static str {
            "PING"
        }

        fn arity(&self) -> Arity {
            Arity::Range { min: 0, max: 1 }
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::FAST | CommandFlags::STALE | CommandFlags::LOADING
        }

        fn execute(
            &self,
            _ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            if args.is_empty() {
                Ok(Response::pong())
            } else {
                Ok(Response::bulk(args[0].clone()))
            }
        }

        fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
            vec![] // Keyless command
        }
    }

    /// ECHO command.
    pub struct EchoCommand;

    impl Command for EchoCommand {
        fn name(&self) -> &'static str {
            "ECHO"
        }

        fn arity(&self) -> Arity {
            Arity::Fixed(1)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::FAST
        }

        fn execute(
            &self,
            _ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            Ok(Response::bulk(args[0].clone()))
        }

        fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
            vec![] // Keyless command
        }
    }

    /// QUIT command.
    pub struct QuitCommand;

    impl Command for QuitCommand {
        fn name(&self) -> &'static str {
            "QUIT"
        }

        fn arity(&self) -> Arity {
            Arity::Fixed(0)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
        }

        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, CommandError> {
            Ok(Response::ok())
        }

        fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
            vec![] // Keyless command
        }
    }

    /// COMMAND command - server command introspection.
    pub struct CommandCommand;

    impl Command for CommandCommand {
        fn name(&self) -> &'static str {
            "COMMAND"
        }

        fn arity(&self) -> Arity {
            Arity::AtLeast(0)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::LOADING | CommandFlags::STALE
        }

        fn execute(
            &self,
            _ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            if args.is_empty() {
                // COMMAND - return info about all commands
                return Ok(Response::Array(vec![])); // Simplified for now
            }

            let subcommand = args[0].to_ascii_uppercase();
            match subcommand.as_slice() {
                b"COUNT" => {
                    // COMMAND COUNT - return number of commands
                    // Approximate count of supported commands
                    Ok(Response::Integer(100)) // Placeholder count
                }
                b"DOCS" => {
                    // COMMAND DOCS [command-name ...] - return docs for commands
                    if args.len() == 1 {
                        // Return docs for all commands (empty for now)
                        Ok(Response::Array(vec![]))
                    } else {
                        // Return docs for specified commands
                        let mut results = Vec::new();
                        for cmd_name in &args[1..] {
                            let cmd_str = String::from_utf8_lossy(cmd_name).to_uppercase();
                            // Build basic doc entry
                            let doc = Response::Array(vec![
                                Response::bulk(cmd_name.clone()),
                                Response::Array(vec![
                                    Response::bulk(Bytes::from_static(b"summary")),
                                    Response::bulk(Bytes::from(format!("{} command", cmd_str))),
                                    Response::bulk(Bytes::from_static(b"since")),
                                    Response::bulk(Bytes::from_static(b"1.0.0")),
                                    Response::bulk(Bytes::from_static(b"group")),
                                    Response::bulk(Bytes::from_static(b"generic")),
                                ]),
                            ]);
                            results.push(doc);
                        }
                        Ok(Response::Array(results))
                    }
                }
                b"INFO" => {
                    // COMMAND INFO [command-name ...] - return info for commands
                    if args.len() == 1 {
                        Ok(Response::Array(vec![]))
                    } else {
                        let mut results = Vec::new();
                        for cmd_name in &args[1..] {
                            // Build basic command info
                            // Format: [name, arity, [flags], first_key, last_key, step]
                            let info = Response::Array(vec![
                                Response::bulk(cmd_name.clone()),
                                Response::Integer(-1), // Variable arity
                                Response::Array(vec![]), // Flags
                                Response::Integer(0), // First key
                                Response::Integer(0), // Last key
                                Response::Integer(0), // Step
                            ]);
                            results.push(info);
                        }
                        Ok(Response::Array(results))
                    }
                }
                b"GETKEYS" => {
                    // COMMAND GETKEYS command [args...] - return keys for a command
                    if args.len() < 2 {
                        return Err(CommandError::WrongArity { command: "COMMAND|GETKEYS" });
                    }
                    // For simplicity, return empty array
                    Ok(Response::Array(vec![]))
                }
                b"HELP" => {
                    // COMMAND HELP
                    let help = vec![
                        Response::bulk(Bytes::from_static(b"COMMAND [subcommand [arg [arg ...]]]")),
                        Response::bulk(Bytes::from_static(b"Return info about Redis commands.")),
                        Response::bulk(Bytes::from_static(b"Subcommands:")),
                        Response::bulk(Bytes::from_static(b"  (no subcommand) -- Return info about all commands")),
                        Response::bulk(Bytes::from_static(b"  COUNT -- Return count of commands")),
                        Response::bulk(Bytes::from_static(b"  DOCS [cmd ...] -- Return documentation for commands")),
                        Response::bulk(Bytes::from_static(b"  INFO [cmd ...] -- Return info for commands")),
                        Response::bulk(Bytes::from_static(b"  GETKEYS cmd [args...] -- Extract keys from command")),
                        Response::bulk(Bytes::from_static(b"  HELP -- Print this help")),
                    ];
                    Ok(Response::Array(help))
                }
                _ => {
                    Err(CommandError::InvalidArgument {
                        message: format!(
                            "unknown subcommand '{}'. Try COMMAND HELP.",
                            String::from_utf8_lossy(&subcommand)
                        ),
                    })
                }
            }
        }

        fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
            vec![] // Keyless command
        }
    }

    /// GET command.
    pub struct GetCommand;

    impl Command for GetCommand {
        fn name(&self) -> &'static str {
            "GET"
        }

        fn arity(&self) -> Arity {
            Arity::Fixed(1)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::FAST
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            let key = &args[0];

            match ctx.store.get_with_expiry_check(key) {
                Some(value) => {
                    if let Some(sv) = value.as_string() {
                        Ok(Response::bulk(sv.as_bytes()))
                    } else {
                        Err(CommandError::WrongType)
                    }
                }
                None => Ok(Response::null()),
            }
        }

        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            if args.is_empty() {
                vec![]
            } else {
                vec![&args[0]]
            }
        }
    }

    /// SET command with full option support.
    pub struct SetCommand;

    impl Command for SetCommand {
        fn name(&self) -> &'static str {
            "SET"
        }

        fn arity(&self) -> Arity {
            Arity::AtLeast(2) // SET key value [options...]
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::WRITE | CommandFlags::FAST
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            let key = args[0].clone();
            let value = args[1].clone();

            // Parse options
            let mut opts = SetOptions::default();
            let mut i = 2;

            while i < args.len() {
                let opt = args[i].to_ascii_uppercase();
                match opt.as_slice() {
                    b"NX" => {
                        if opts.condition != SetCondition::Always {
                            return Err(CommandError::SyntaxError);
                        }
                        opts.condition = SetCondition::NX;
                    }
                    b"XX" => {
                        if opts.condition != SetCondition::Always {
                            return Err(CommandError::SyntaxError);
                        }
                        opts.condition = SetCondition::XX;
                    }
                    b"GET" => {
                        opts.return_old = true;
                    }
                    b"KEEPTTL" => {
                        opts.keep_ttl = true;
                    }
                    b"EX" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::SyntaxError);
                        }
                        let secs = parse_u64(&args[i])?;
                        if secs == 0 {
                            return Err(CommandError::InvalidArgument {
                                message: "invalid expire time in 'set' command".to_string(),
                            });
                        }
                        opts.expiry = Some(Expiry::Ex(secs));
                    }
                    b"PX" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::SyntaxError);
                        }
                        let ms = parse_u64(&args[i])?;
                        if ms == 0 {
                            return Err(CommandError::InvalidArgument {
                                message: "invalid expire time in 'set' command".to_string(),
                            });
                        }
                        opts.expiry = Some(Expiry::Px(ms));
                    }
                    b"EXAT" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::SyntaxError);
                        }
                        let ts = parse_u64(&args[i])?;
                        opts.expiry = Some(Expiry::ExAt(ts));
                    }
                    b"PXAT" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::SyntaxError);
                        }
                        let ts = parse_u64(&args[i])?;
                        opts.expiry = Some(Expiry::PxAt(ts));
                    }
                    _ => return Err(CommandError::SyntaxError),
                }
                i += 1;
            }

            // Check for conflicting options
            if opts.keep_ttl && opts.expiry.is_some() {
                return Err(CommandError::SyntaxError);
            }

            match ctx.store.set_with_options(key, Value::string(value), opts) {
                SetResult::Ok => Ok(Response::ok()),
                SetResult::OkWithOldValue(old) => {
                    match old {
                        Some(v) => {
                            if let Some(sv) = v.as_string() {
                                Ok(Response::bulk(sv.as_bytes()))
                            } else {
                                // Old value was wrong type but we replaced it anyway
                                Ok(Response::null())
                            }
                        }
                        None => Ok(Response::null()),
                    }
                }
                SetResult::NotSet => Ok(Response::null()),
            }
        }

        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            if args.is_empty() {
                vec![]
            } else {
                vec![&args[0]]
            }
        }
    }

    /// Parse a string as u64.
    fn parse_u64(arg: &[u8]) -> Result<u64, CommandError> {
        std::str::from_utf8(arg)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(CommandError::NotInteger)
    }

    /// DEL command.
    pub struct DelCommand;

    impl Command for DelCommand {
        fn name(&self) -> &'static str {
            "DEL"
        }

        fn arity(&self) -> Arity {
            Arity::AtLeast(1)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::WRITE
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            // Multi-key DEL: delete all keys and return count
            // Cross-shard routing is handled by connection handler
            let mut deleted = 0i64;
            for key in args {
                if ctx.store.delete(key) {
                    deleted += 1;
                }
            }
            Ok(Response::Integer(deleted))
        }

        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            args.iter().map(|a| a.as_ref()).collect()
        }
    }

    /// EXISTS command.
    pub struct ExistsCommand;

    impl Command for ExistsCommand {
        fn name(&self) -> &'static str {
            "EXISTS"
        }

        fn arity(&self) -> Arity {
            Arity::AtLeast(1)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::FAST
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            // Multi-key EXISTS: count how many keys exist
            // Note: Redis counts duplicates (EXISTS key key returns 2 if key exists)
            let mut count = 0i64;
            for key in args {
                if ctx.store.contains(key) {
                    count += 1;
                }
            }
            Ok(Response::Integer(count))
        }

        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            args.iter().map(|a| a.as_ref()).collect()
        }
    }
}
