//! Debug state for the debug web UI.
//!
//! This module provides the state required to serve debug information
//! from the metrics server.

use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

use crate::bundle::{BundleConfig, BundleGenerator, BundleInfo, BundleStore, DiagnosticCollector};
use frogdb_core::{ClientRegistry, ShardSender};
use frogdb_telemetry::SharedTracer;

/// A configuration entry for display in the debug UI.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ConfigEntry {
    /// Configuration key name.
    pub name: String,
    /// Configuration value.
    pub value: String,
}

/// Information about the server for debug display.
#[derive(Clone, Debug)]
pub struct ServerInfo {
    /// Server version
    pub version: String,
    /// Server start time
    pub start_time: Instant,
    /// Number of shards
    pub num_shards: usize,
    /// Bind address
    pub bind_addr: String,
    /// Port
    pub port: u16,
}

impl Default for ServerInfo {
    fn default() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            start_time: Instant::now(),
            num_shards: 0,
            bind_addr: "0.0.0.0".to_string(),
            port: 6379,
        }
    }
}

/// Trait for providing replication information to the debug UI.
pub trait ReplicationInfoProvider: Send + Sync {
    /// Get the replication role (primary/replica).
    fn role(&self) -> &str;
    /// Get connected replicas count.
    fn connected_replicas(&self) -> usize;
    /// Get master host if this is a replica.
    fn master_host(&self) -> Option<String>;
    /// Get master port if this is a replica.
    fn master_port(&self) -> Option<u16>;
    /// Get replication offset.
    fn replication_offset(&self) -> u64;
}

/// Debug query types that can be sent to shards.
#[derive(Debug, Clone)]
pub enum DebugQuery {
    /// Get slowlog entries from all shards.
    GetSlowlog { count: usize },
    /// Get latency histogram data.
    GetLatency,
    /// Get shard statistics (keys, memory, queue depth).
    GetShardStats,
}

/// Response from a debug query.
#[derive(Debug, Clone)]
pub enum DebugQueryResponse {
    /// Slowlog entries from a shard.
    Slowlog(Vec<SlowlogEntry>),
    /// Latency data from a shard.
    Latency(LatencyData),
    /// Shard statistics.
    ShardStats(ShardStats),
}

/// A slowlog entry.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SlowlogEntry {
    /// Entry ID.
    pub id: u64,
    /// Timestamp (Unix seconds).
    pub timestamp: u64,
    /// Duration in microseconds.
    pub duration_us: u64,
    /// Command and arguments.
    pub command: String,
    /// Client address.
    pub client_addr: Option<String>,
    /// Client name.
    pub client_name: Option<String>,
}

/// Latency data for a specific event type.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LatencyData {
    /// Event type name.
    pub event: String,
    /// Latest latency in microseconds.
    pub latest_us: u64,
    /// Minimum latency in microseconds.
    pub min_us: u64,
    /// Maximum latency in microseconds.
    pub max_us: u64,
    /// Average latency in microseconds.
    pub avg_us: u64,
    /// Sample count.
    pub samples: u64,
}

/// Statistics for a single shard.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ShardStats {
    /// Shard ID.
    pub shard_id: usize,
    /// Number of keys.
    pub keys: u64,
    /// Memory usage in bytes.
    pub memory_bytes: u64,
    /// Current queue depth.
    pub queue_depth: u64,
}

/// A snapshot of a connected client for display in the debug UI.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ClientSnapshot {
    /// Unique connection ID.
    pub id: u64,
    /// Remote address (IP:port).
    pub addr: String,
    /// Client name (from CLIENT SETNAME).
    pub name: String,
    /// Library name (from CLIENT SETINFO).
    pub lib_name: String,
    /// Library version (from CLIENT SETINFO).
    pub lib_ver: String,
    /// Connection age in seconds.
    pub age_secs: u64,
    /// Seconds since last command.
    pub idle_secs: u64,
    /// Flags string (e.g. "N", "xb", "P").
    pub flags: String,
    /// Total commands processed.
    pub commands_total: u64,
    /// Total bytes received.
    pub bytes_recv: u64,
    /// Total bytes sent.
    pub bytes_sent: u64,
}

/// Trait for providing connected client information to the debug UI.
pub trait ClientInfoProvider: Send + Sync {
    /// Get a snapshot of all connected clients.
    fn client_snapshots(&self) -> Vec<ClientSnapshot>;
}

/// Snapshot of a cluster node for the debug UI.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClusterNodeSnapshot {
    /// Unique node identifier.
    pub id: u64,
    /// Client-facing address (IP:port).
    pub addr: String,
    /// Cluster bus address (IP:port).
    pub cluster_addr: String,
    /// Role: "primary" or "replica".
    pub role: String,
    /// For replicas, the ID of their primary node.
    pub primary_id: Option<u64>,
    /// Configuration epoch when this node was last updated.
    pub config_epoch: u64,
    /// Health/state flags (e.g. "fail", "pfail", "handshake").
    pub flags: Vec<String>,
    /// Slot ranges assigned to this node as (start, end) inclusive pairs.
    pub slot_ranges: Vec<(u16, u16)>,
    /// Total number of slots assigned.
    pub slot_count: usize,
    /// Binary version running on this node.
    pub version: String,
}

/// Snapshot of the overall cluster state for the debug UI.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClusterOverviewSnapshot {
    /// Whether cluster mode is enabled.
    pub enabled: bool,
    /// This node's ID (to highlight "self" in the UI).
    pub self_node_id: Option<u64>,
    /// All known nodes.
    pub nodes: Vec<ClusterNodeSnapshot>,
    /// Number of slots assigned to any node.
    pub slots_assigned: usize,
    /// Total slot count (always 16384).
    pub total_slots: u16,
    /// Current configuration epoch.
    pub config_epoch: u64,
    /// Raft leader node ID, if known.
    pub leader_id: Option<u64>,
    /// Finalized active version for rolling upgrades.
    pub active_version: Option<String>,
    /// Active slot migrations.
    pub migrations: Vec<MigrationSnapshot>,
}

/// Snapshot of a single slot migration in progress.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MigrationSnapshot {
    /// Slot being migrated.
    pub slot: u16,
    /// Source node ID.
    pub source_node: u64,
    /// Target node ID.
    pub target_node: u64,
    /// Migration state (e.g. "initiated", "migrating", "completing").
    pub state: String,
}

/// Trait for providing cluster information to the debug UI.
pub trait ClusterInfoProvider: Send + Sync {
    /// Get a snapshot of the full cluster state.
    fn cluster_overview(&self) -> ClusterOverviewSnapshot;
    /// Get info about a specific node by ID.
    fn node_detail(&self, node_id: u64) -> Option<ClusterNodeSnapshot>;
}

/// State required for the debug web UI.
#[derive(Clone, Default)]
pub struct DebugState {
    /// Server information.
    pub server_info: ServerInfo,
    /// Replication info provider (optional).
    pub replication_info: Option<Arc<dyn ReplicationInfoProvider>>,
    /// Shard message senders for querying shard data.
    /// This is optional and will be None if not wired up.
    shard_senders: Option<Arc<Vec<mpsc::Sender<DebugShardMessage>>>>,
    /// Bundle store for diagnostic bundles.
    bundle_store: Option<Arc<BundleStore>>,
    /// Bundle configuration.
    bundle_config: BundleConfig,
    /// Shard senders for bundle data collection (uses ShardMessage).
    bundle_shard_senders: Option<Arc<Vec<ShardSender>>>,
    /// Shared tracer for trace collection.
    shared_tracer: Option<SharedTracer>,
    /// Configuration entries for display.
    pub config_entries: Vec<ConfigEntry>,
    /// Client info provider (optional).
    client_info: Option<Arc<dyn ClientInfoProvider>>,
    /// Cluster info provider (optional, None when not in cluster mode).
    cluster_info: Option<Arc<dyn ClusterInfoProvider>>,
}

/// Message type for debug queries to shards.
pub struct DebugShardMessage {
    pub query: DebugQuery,
    pub response_tx: tokio::sync::oneshot::Sender<DebugQueryResponse>,
}

impl DebugState {
    /// Create a new debug state with server info and config entries.
    pub fn new(server_info: ServerInfo, config_entries: Vec<ConfigEntry>) -> Self {
        Self {
            server_info,
            replication_info: None,
            shard_senders: None,
            bundle_store: None,
            bundle_config: BundleConfig::default(),
            bundle_shard_senders: None,
            shared_tracer: None,
            config_entries,
            client_info: None,
            cluster_info: None,
        }
    }

    /// Set the replication info provider.
    pub fn with_replication_info(mut self, provider: Arc<dyn ReplicationInfoProvider>) -> Self {
        self.replication_info = Some(provider);
        self
    }

    /// Set the client info provider.
    pub fn with_client_info(mut self, provider: Arc<dyn ClientInfoProvider>) -> Self {
        self.client_info = Some(provider);
        self
    }

    /// Set the cluster info provider.
    pub fn with_cluster_info(mut self, provider: Arc<dyn ClusterInfoProvider>) -> Self {
        self.cluster_info = Some(provider);
        self
    }

    /// Get snapshots of all connected clients.
    pub fn get_clients(&self) -> Vec<ClientSnapshot> {
        self.client_info
            .as_ref()
            .map(|p| p.client_snapshots())
            .unwrap_or_default()
    }

    /// Whether cluster mode is enabled.
    pub fn cluster_enabled(&self) -> bool {
        self.cluster_info.is_some()
    }

    /// Get a snapshot of the cluster overview.
    pub fn cluster_overview(&self) -> Option<ClusterOverviewSnapshot> {
        self.cluster_info.as_ref().map(|p| p.cluster_overview())
    }

    /// Get detail for a specific cluster node.
    pub fn cluster_node_detail(&self, node_id: u64) -> Option<ClusterNodeSnapshot> {
        self.cluster_info.as_ref()?.node_detail(node_id)
    }

    /// Set the shard message senders.
    pub fn with_shard_senders(
        mut self,
        senders: Arc<Vec<mpsc::Sender<DebugShardMessage>>>,
    ) -> Self {
        self.shard_senders = Some(senders);
        self
    }

    /// Configure bundle support.
    pub fn with_bundle_support(
        mut self,
        config: BundleConfig,
        shard_senders: Arc<Vec<ShardSender>>,
        shared_tracer: Option<SharedTracer>,
    ) -> Self {
        self.bundle_store = Some(Arc::new(BundleStore::new(config.clone())));
        self.bundle_config = config;
        self.bundle_shard_senders = Some(shard_senders);
        self.shared_tracer = shared_tracer;
        self
    }

    /// Get uptime in seconds.
    pub fn uptime_seconds(&self) -> u64 {
        self.server_info.start_time.elapsed().as_secs()
    }

    /// Get the server role.
    pub fn role(&self) -> &str {
        self.replication_info
            .as_ref()
            .map(|r| r.role())
            .unwrap_or("standalone")
    }

    /// Query slowlog from all shards (aggregated).
    pub async fn get_slowlog(&self, _count: usize) -> Vec<SlowlogEntry> {
        // For now, return empty. This will be implemented when we wire up shard queries.
        Vec::new()
    }

    /// Query latency data from all shards.
    pub async fn get_latency(&self) -> Vec<LatencyData> {
        // For now, return empty. This will be implemented when we wire up shard queries.
        Vec::new()
    }

    /// Query shard statistics.
    pub async fn get_shard_stats(&self) -> Vec<ShardStats> {
        // For now, return empty. This will be implemented when we wire up shard queries.
        Vec::new()
    }

    // =========================================================================
    // Bundle operations
    // =========================================================================

    /// Check if bundle support is enabled.
    pub fn bundle_enabled(&self) -> bool {
        self.bundle_store.is_some() && self.bundle_shard_senders.is_some()
    }

    /// List all stored bundles.
    pub fn list_bundles(&self) -> Vec<BundleInfo> {
        if let Some(ref store) = self.bundle_store {
            store.list()
        } else {
            Vec::new()
        }
    }

    /// Get a stored bundle by ID.
    pub fn get_bundle(&self, id: &str) -> Option<Vec<u8>> {
        self.bundle_store.as_ref()?.get(id)
    }

    /// Generate a new bundle with optional duration for sampling.
    ///
    /// Returns the bundle ID and the ZIP data.
    pub async fn generate_bundle(&self, duration_secs: u64) -> Result<(String, Vec<u8>), String> {
        let shard_senders = self
            .bundle_shard_senders
            .as_ref()
            .ok_or_else(|| "Bundle support not enabled".to_string())?;

        let collector = DiagnosticCollector::new(
            shard_senders.clone(),
            self.shared_tracer.clone(),
            self.bundle_config.clone(),
        );

        // Collect diagnostic data
        let mut data = if duration_secs == 0 {
            collector.collect_instant().await
        } else {
            collector.collect_with_duration(duration_secs).await
        };

        // Add cluster state information
        data.cluster_state = crate::bundle::ClusterStateJson {
            mode: self.role().to_string(),
            role: self.role().to_string(),
            num_shards: self.server_info.num_shards,
            cluster_enabled: false, // Will be updated when cluster info is available
            nodes: None,
        };

        // Generate bundle
        let generator = BundleGenerator::new(self.bundle_config.clone());
        let id = BundleGenerator::generate_id();
        let zip_data = generator
            .create_zip(&id, &data, duration_secs)
            .map_err(|e| format!("Failed to create bundle: {}", e))?;

        // Store the bundle
        if let Some(ref store) = self.bundle_store {
            store
                .store(&id, &zip_data)
                .map_err(|e| format!("Failed to store bundle: {}", e))?;
        }

        Ok((id, zip_data))
    }

    /// Generate a bundle and return it without storing (for streaming).
    pub async fn generate_bundle_streaming(
        &self,
        duration_secs: u64,
    ) -> Result<(String, Vec<u8>), String> {
        let shard_senders = self
            .bundle_shard_senders
            .as_ref()
            .ok_or_else(|| "Bundle support not enabled".to_string())?;

        let collector = DiagnosticCollector::new(
            shard_senders.clone(),
            self.shared_tracer.clone(),
            self.bundle_config.clone(),
        );

        // Collect diagnostic data
        let mut data = if duration_secs == 0 {
            collector.collect_instant().await
        } else {
            collector.collect_with_duration(duration_secs).await
        };

        // Add cluster state information
        data.cluster_state = crate::bundle::ClusterStateJson {
            mode: self.role().to_string(),
            role: self.role().to_string(),
            num_shards: self.server_info.num_shards,
            cluster_enabled: false,
            nodes: None,
        };

        // Generate bundle
        let generator = BundleGenerator::new(self.bundle_config.clone());
        let id = BundleGenerator::generate_id();
        let zip_data = generator
            .create_zip(&id, &data, duration_secs)
            .map_err(|e| format!("Failed to create bundle: {}", e))?;

        // Store for later retrieval
        if let Some(ref store) = self.bundle_store {
            let _ = store.store(&id, &zip_data);
        }

        Ok((id, zip_data))
    }
}

// ============================================================================
// ClientInfoProvider implementation for ClientRegistry
// ============================================================================

impl ClientInfoProvider for ClientRegistry {
    fn client_snapshots(&self) -> Vec<ClientSnapshot> {
        let all = self.get_all_stats();
        all.into_iter()
            .map(|(_id, info, stats)| {
                let name = info
                    .name
                    .as_ref()
                    .map(|n| String::from_utf8_lossy(n).to_string())
                    .unwrap_or_default();
                let lib_name = info
                    .lib_name
                    .as_ref()
                    .map(|n| String::from_utf8_lossy(n).to_string())
                    .unwrap_or_default();
                let lib_ver = info
                    .lib_ver
                    .as_ref()
                    .map(|n| String::from_utf8_lossy(n).to_string())
                    .unwrap_or_default();

                ClientSnapshot {
                    id: info.id,
                    addr: info.addr.to_string(),
                    name,
                    lib_name,
                    lib_ver,
                    age_secs: info.created_at.elapsed().as_secs(),
                    idle_secs: info.last_command_at.elapsed().as_secs(),
                    flags: info.flags.to_flag_string(),
                    commands_total: stats.commands_total,
                    bytes_recv: stats.bytes_recv,
                    bytes_sent: stats.bytes_sent,
                }
            })
            .collect()
    }
}
