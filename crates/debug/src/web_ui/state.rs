//! Debug state for the debug web UI.
//!
//! This module provides the state required to serve debug information
//! from the metrics server.

use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

use crate::bundle::{BundleConfig, BundleGenerator, BundleInfo, BundleStore, DiagnosticCollector};
use frogdb_core::ShardMessage;
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
    bundle_shard_senders: Option<Arc<Vec<mpsc::Sender<ShardMessage>>>>,
    /// Shared tracer for trace collection.
    shared_tracer: Option<SharedTracer>,
    /// Configuration entries for display.
    pub config_entries: Vec<ConfigEntry>,
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
        }
    }

    /// Set the replication info provider.
    pub fn with_replication_info(mut self, provider: Arc<dyn ReplicationInfoProvider>) -> Self {
        self.replication_info = Some(provider);
        self
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
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
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
