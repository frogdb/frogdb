//! Debug state for the debug web UI.
//!
//! This module provides the state required to serve debug information
//! from the metrics server.
//!
//! ## Node-state surface
//!
//! Node-observable state (replication identity, connected clients, cluster
//! topology) is exposed through a single [`NodeStateProvider`] trait rather than
//! the three parallel provider traits this module used to declare. A single
//! server-side adapter implements it, giving the debug UI one coherent seam onto
//! subsystem state — the same shape a future one-shot `NodeStateSnapshot`
//! `collect()` can populate.
//!
//! Per-shard statistics, latency histograms, and the slowlog are gathered on
//! demand by scattering `ShardMessage` requests over the real shard senders —
//! the same messages `INFO` and the `LATENCY`/`SLOWLOG` commands use — so no
//! surface returns a stubbed empty panel.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::bundle::{BundleConfig, BundleGenerator, BundleInfo, BundleStore, DiagnosticCollector};
use frogdb_core::{
    ClientRegistry, LatencyEvent, LatencySample, ObservabilityMsg, ShardSender, SlowLogEntry,
};
use frogdb_telemetry::{NodeStateSnapshot, ShardState, SharedTracer};

/// Deadline for the debug UI's single node-state scatter. The debug panels are
/// best-effort: a shard that misses this deadline yields an empty snapshot
/// rather than blocking the page.
const DEBUG_SCATTER_TIMEOUT: Duration = Duration::from_secs(5);

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

/// A read view of this node's replication identity for the debug UI.
#[derive(Clone, Debug)]
pub struct ReplicationView {
    /// Replication role (`standalone` / `primary` / `replica` / `cluster`).
    pub role: String,
    /// Number of connected replicas (primary only).
    pub connected_replicas: usize,
    /// Master host, when this node is a replica.
    pub master_host: Option<String>,
    /// Master port, when this node is a replica.
    pub master_port: Option<u16>,
    /// Current replication offset.
    pub replication_offset: u64,
}

impl Default for ReplicationView {
    fn default() -> Self {
        Self {
            role: "standalone".to_string(),
            connected_replicas: 0,
            master_host: None,
            master_port: None,
            replication_offset: 0,
        }
    }
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

/// Statistics for a single internal shard.
///
/// A pure render view over one [`ShardState`] row from the shared
/// [`NodeStateSnapshot`]; it carries no aggregation of its own.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ShardStats {
    /// Shard ID.
    pub shard_id: usize,
    /// Number of keys resident on the shard.
    pub keys: u64,
    /// Data memory usage in bytes.
    pub memory_bytes: u64,
    /// Peak (high-water-mark) memory usage in bytes.
    pub peak_memory_bytes: u64,
    /// Keys resident in the hot tier.
    pub hot_keys: u64,
    /// Keys resident in the warm (spilled) tier.
    pub warm_keys: u64,
}

impl From<&ShardState> for ShardStats {
    fn from(s: &ShardState) -> Self {
        ShardStats {
            shard_id: s.shard_id,
            keys: s.keys as u64,
            memory_bytes: s.data_memory as u64,
            peak_memory_bytes: s.peak_memory,
            hot_keys: s.hot_keys as u64,
            warm_keys: s.warm_keys as u64,
        }
    }
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
///
/// Presence in [`ClusterOverviewSnapshot::migrations`] *is* the state: FrogDB
/// migration is a two-point begin/complete ownership swap with no intermediate
/// states to display.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MigrationSnapshot {
    /// Slot being migrated.
    pub slot: u16,
    /// Source node ID.
    pub source_node: u64,
    /// Target node ID.
    pub target_node: u64,
}

/// Single coherent provider of node-observable state for the debug UI.
///
/// Collapses the former `ReplicationInfoProvider` / `ClientInfoProvider` /
/// `ClusterInfoProvider` trio into one trait so there is a single upstream seam
/// onto subsystem state instead of three hand-assembled ones. A server-side
/// adapter implements it; the cluster methods default to "not in cluster mode"
/// so a standalone node needs to supply only replication + client reads.
pub trait NodeStateProvider: Send + Sync {
    /// This node's replication identity.
    fn replication(&self) -> ReplicationView;

    /// Snapshots of all currently connected clients.
    fn client_snapshots(&self) -> Vec<ClientSnapshot>;

    /// Full cluster overview, or `None` when cluster mode is disabled.
    fn cluster_overview(&self) -> Option<ClusterOverviewSnapshot> {
        None
    }

    /// Detail for a single cluster node, or `None` when unknown / not clustered.
    fn node_detail(&self, _node_id: u64) -> Option<ClusterNodeSnapshot> {
        None
    }
}

/// Build [`ClientSnapshot`]s directly from a [`ClientRegistry`].
///
/// Exposed so a server-side [`NodeStateProvider`] can reuse the registry →
/// snapshot mapping without re-declaring it.
pub fn client_snapshots_from_registry(registry: &ClientRegistry) -> Vec<ClientSnapshot> {
    registry
        .get_all_stats()
        .into_iter()
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

/// State required for the debug web UI.
#[derive(Clone, Default)]
pub struct DebugState {
    /// Server information.
    pub server_info: ServerInfo,
    /// Real shard senders for on-demand scatter queries (shard stats, latency,
    /// slowlog) and diagnostic-bundle collection. `None` until wired.
    shard_senders: Option<Arc<Vec<ShardSender>>>,
    /// Bundle store for diagnostic bundles.
    bundle_store: Option<Arc<BundleStore>>,
    /// Bundle configuration.
    bundle_config: BundleConfig,
    /// Shared tracer for trace collection.
    shared_tracer: Option<SharedTracer>,
    /// Configuration entries for display.
    pub config_entries: Vec<ConfigEntry>,
    /// Unified node-state provider (replication, clients, cluster topology).
    node_state: Option<Arc<dyn NodeStateProvider>>,
}

impl DebugState {
    /// Create a new debug state with server info and config entries.
    pub fn new(server_info: ServerInfo, config_entries: Vec<ConfigEntry>) -> Self {
        Self {
            server_info,
            shard_senders: None,
            bundle_store: None,
            bundle_config: BundleConfig::default(),
            shared_tracer: None,
            config_entries,
            node_state: None,
        }
    }

    /// Set the unified node-state provider.
    pub fn with_node_state(mut self, provider: Arc<dyn NodeStateProvider>) -> Self {
        self.node_state = Some(provider);
        self
    }

    /// Set the real shard senders used for scatter queries and bundles.
    pub fn with_shard_senders(mut self, senders: Arc<Vec<ShardSender>>) -> Self {
        self.shard_senders = Some(senders);
        self
    }

    /// This node's replication identity (defaults to standalone when unwired).
    pub fn replication(&self) -> ReplicationView {
        self.node_state
            .as_ref()
            .map(|p| p.replication())
            .unwrap_or_default()
    }

    /// Get snapshots of all connected clients.
    pub fn get_clients(&self) -> Vec<ClientSnapshot> {
        self.node_state
            .as_ref()
            .map(|p| p.client_snapshots())
            .unwrap_or_default()
    }

    /// Get a snapshot of the cluster overview.
    pub fn cluster_overview(&self) -> Option<ClusterOverviewSnapshot> {
        self.node_state.as_ref().and_then(|p| p.cluster_overview())
    }

    /// Get detail for a specific cluster node.
    pub fn cluster_node_detail(&self, node_id: u64) -> Option<ClusterNodeSnapshot> {
        self.node_state
            .as_ref()
            .and_then(|p| p.node_detail(node_id))
    }

    /// Configure bundle support (store + tracer). Shard senders are shared with
    /// the scatter-query path via [`Self::with_shard_senders`].
    pub fn with_bundle_support(
        mut self,
        config: BundleConfig,
        shared_tracer: Option<SharedTracer>,
    ) -> Self {
        self.bundle_store = Some(Arc::new(BundleStore::new(config.clone())));
        self.bundle_config = config;
        self.shared_tracer = shared_tracer;
        self
    }

    /// Get uptime in seconds.
    pub fn uptime_seconds(&self) -> u64 {
        self.server_info.start_time.elapsed().as_secs()
    }

    /// Get the server role.
    pub fn role(&self) -> String {
        self.replication().role
    }

    // =========================================================================
    // On-demand shard scatter queries
    // =========================================================================

    /// Query per-shard statistics (keys, memory, tiered counts) from all shards.
    ///
    /// Reads the shared [`NodeStateSnapshot`] — the same single-scatter gather
    /// INFO and telemetry `/status` render from — and projects its per-shard
    /// rows into the debug panel's [`ShardStats`] view.
    pub async fn get_shard_stats(&self) -> Vec<ShardStats> {
        let Some(senders) = self.shard_senders.as_ref() else {
            return Vec::new();
        };
        let snapshot = NodeStateSnapshot::collect(senders, DEBUG_SCATTER_TIMEOUT)
            .await
            .unwrap_or_default();
        let mut stats: Vec<ShardStats> = snapshot.per_shard.iter().map(ShardStats::from).collect();
        stats.sort_by_key(|s| s.shard_id);
        stats
    }

    /// Query latency histogram data from all shards.
    ///
    /// Mirrors the `LATENCY` command's gather: the newest sample per event
    /// (across shards) supplies `latest`, and the merged per-event history
    /// supplies min / max / avg / sample count.
    pub async fn get_latency(&self) -> Vec<LatencyData> {
        let Some(senders) = self.shard_senders.as_ref() else {
            return Vec::new();
        };

        // Newest latest-sample per event across shards.
        let mut latest: HashMap<LatencyEvent, LatencySample> = HashMap::new();
        for sender in senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ObservabilityMsg::LatencyLatest { response_tx })
                .await
                .is_ok()
                && let Ok(samples) = response_rx.await
            {
                for (event, sample) in samples {
                    latest
                        .entry(event)
                        .and_modify(|existing| {
                            if sample.timestamp > existing.timestamp {
                                *existing = sample;
                            }
                        })
                        .or_insert(sample);
                }
            }
        }

        let mut data = Vec::with_capacity(latest.len());
        for (event, latest_sample) in latest {
            let mut min_ms = u64::MAX;
            let mut max_ms = 0u64;
            let mut total_ms: u128 = 0;
            let mut count: u64 = 0;
            for sender in senders.iter() {
                let (response_tx, response_rx) = oneshot::channel();
                if sender
                    .send(ObservabilityMsg::LatencyHistory { event, response_tx })
                    .await
                    .is_ok()
                    && let Ok(history) = response_rx.await
                {
                    for sample in history {
                        min_ms = min_ms.min(sample.latency_ms);
                        max_ms = max_ms.max(sample.latency_ms);
                        total_ms += sample.latency_ms as u128;
                        count += 1;
                    }
                }
            }
            // Fall back to the latest sample if history was empty (e.g. trimmed).
            if count == 0 {
                min_ms = latest_sample.latency_ms;
                max_ms = latest_sample.latency_ms;
                total_ms = latest_sample.latency_ms as u128;
                count = 1;
            }
            let avg_ms = (total_ms / count as u128) as u64;
            data.push(LatencyData {
                event: event.as_str().to_string(),
                latest_us: latest_sample.latency_ms * 1000,
                min_us: min_ms * 1000,
                max_us: max_ms * 1000,
                avg_us: avg_ms * 1000,
                samples: count,
            });
        }
        data.sort_by(|a, b| a.event.cmp(&b.event));
        data
    }

    /// Query slowlog entries from all shards (aggregated, newest first).
    pub async fn get_slowlog(&self, count: usize) -> Vec<SlowlogEntry> {
        let Some(senders) = self.shard_senders.as_ref() else {
            return Vec::new();
        };
        let mut entries = Vec::new();
        for sender in senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ObservabilityMsg::SlowlogGet { count, response_tx })
                .await
                .is_ok()
                && let Ok(shard_entries) = response_rx.await
            {
                entries.extend(shard_entries.into_iter().map(slowlog_entry_from_core));
            }
        }
        // Newest first, then by descending id for stable ordering.
        entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp).then(b.id.cmp(&a.id)));
        entries.truncate(count);
        entries
    }

    // =========================================================================
    // Bundle operations
    // =========================================================================

    /// Check if bundle support is enabled.
    pub fn bundle_enabled(&self) -> bool {
        self.bundle_store.is_some() && self.shard_senders.is_some()
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
            .shard_senders
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
            mode: self.role(),
            role: self.role(),
            num_shards: self.server_info.num_shards,
            cluster_enabled: self.cluster_overview().is_some(),
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
            .shard_senders
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
            mode: self.role(),
            role: self.role(),
            num_shards: self.server_info.num_shards,
            cluster_enabled: self.cluster_overview().is_some(),
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

/// Convert a core [`SlowLogEntry`] into the debug UI's [`SlowlogEntry`].
fn slowlog_entry_from_core(entry: SlowLogEntry) -> SlowlogEntry {
    let command = entry
        .command
        .iter()
        .map(|part| String::from_utf8_lossy(part).to_string())
        .collect::<Vec<_>>()
        .join(" ");
    SlowlogEntry {
        id: entry.id,
        timestamp: entry.timestamp.max(0) as u64,
        duration_us: entry.duration_us,
        command,
        client_addr: (!entry.client_addr.is_empty()).then_some(entry.client_addr),
        client_name: (!entry.client_name.is_empty()).then_some(entry.client_name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::{
        Envelope, InfoShardSnapshot, LatencySample, ShardMemoryStats, ShardMessage, TieredCounts,
    };
    use tokio::sync::mpsc;

    /// Spawn a mock shard worker over the real `ShardMessage` protocol.
    ///
    /// It answers the three read requests the debug UI scatters — `InfoSnapshot`,
    /// `LatencyLatest`/`LatencyHistory`, and `SlowlogGet` — with fabricated data
    /// keyed off `shard_id`, giving a live per-shard fixture without booting a
    /// full server.
    fn spawn_mock_shard(shard_id: usize, keys: usize) -> ShardSender {
        let (tx, mut rx) = mpsc::channel::<Envelope>(32);
        tokio::spawn(async move {
            while let Some(env) = rx.recv().await {
                match env.message {
                    ShardMessage::Observability(ObservabilityMsg::InfoSnapshot { response_tx }) => {
                        let mut snap = InfoShardSnapshot {
                            shard_id,
                            ..Default::default()
                        };
                        snap.memory = ShardMemoryStats {
                            shard_id,
                            keys,
                            data_memory: keys * 100,
                            peak_memory: (keys * 200) as u64,
                            ..Default::default()
                        };
                        snap.tiered = TieredCounts {
                            hot_keys: keys,
                            warm_keys: shard_id,
                            ..Default::default()
                        };
                        let _ = response_tx.send(snap);
                    }
                    ShardMessage::Observability(ObservabilityMsg::LatencyLatest {
                        response_tx,
                    }) => {
                        let sample = LatencySample::with_timestamp(1_000 + shard_id as i64, 5);
                        let _ = response_tx.send(vec![(LatencyEvent::Command, sample)]);
                    }
                    ShardMessage::Observability(ObservabilityMsg::LatencyHistory {
                        response_tx,
                        ..
                    }) => {
                        let history = vec![
                            LatencySample::with_timestamp(1_000 + shard_id as i64, 5),
                            LatencySample::with_timestamp(999 + shard_id as i64, 3),
                        ];
                        let _ = response_tx.send(history);
                    }
                    ShardMessage::Observability(ObservabilityMsg::SlowlogGet {
                        response_tx,
                        ..
                    }) => {
                        let _ = response_tx.send(vec![SlowLogEntry {
                            id: shard_id as u64,
                            timestamp: 1_700_000_000 + shard_id as i64,
                            duration_us: 12_345,
                            command: vec![
                                bytes::Bytes::from_static(b"GET"),
                                bytes::Bytes::from_static(b"key"),
                            ],
                            client_addr: "127.0.0.1:6379".to_string(),
                            client_name: String::new(),
                        }]);
                    }
                    _ => {}
                }
            }
        });
        ShardSender::new(tx)
    }

    fn state_with_shards(keys_per_shard: &[usize]) -> DebugState {
        let senders: Vec<ShardSender> = keys_per_shard
            .iter()
            .enumerate()
            .map(|(id, &keys)| spawn_mock_shard(id, keys))
            .collect();
        DebugState::new(ServerInfo::default(), Vec::new()).with_shard_senders(Arc::new(senders))
    }

    /// Cross-surface agreement: the telemetry `/status` `ShardStatus` view and
    /// the debug UI `ShardStats` view are both pure renderers over one shared
    /// [`ShardState`] row, so they can never report a different id/keys/memory
    /// for the same shard.
    #[test]
    fn shard_views_agree_across_surfaces() {
        use frogdb_telemetry::status::ShardStatus;

        let row = ShardState {
            shard_id: 2,
            keys: 15,
            data_memory: 4096,
            peak_memory: 8192,
            hot_keys: 10,
            warm_keys: 5,
        };
        let status: ShardStatus = (&row).into();
        let debug: ShardStats = (&row).into();

        assert_eq!(status.id, debug.shard_id);
        assert_eq!(status.keys as u64, debug.keys);
        assert_eq!(status.memory_bytes, debug.memory_bytes);
        assert_eq!(status.peak_memory_bytes, debug.peak_memory_bytes);
    }

    #[tokio::test]
    async fn shard_stats_returns_real_rows() {
        let state = state_with_shards(&[3, 7]);
        let stats = state.get_shard_stats().await;

        assert_eq!(stats.len(), 2, "one row per shard");
        assert_eq!(stats[0].shard_id, 0);
        assert_eq!(stats[0].keys, 3);
        assert_eq!(stats[0].memory_bytes, 300);
        assert_eq!(stats[0].hot_keys, 3);
        assert_eq!(stats[1].shard_id, 1);
        assert_eq!(stats[1].keys, 7);
        assert_eq!(stats[1].warm_keys, 1);
        let total_keys: u64 = stats.iter().map(|s| s.keys).sum();
        assert_eq!(total_keys, 10);
    }

    #[tokio::test]
    async fn latency_returns_real_data() {
        let state = state_with_shards(&[1, 1]);
        let data = state.get_latency().await;

        assert_eq!(data.len(), 1, "one aggregated row for the command event");
        let cmd = &data[0];
        assert_eq!(cmd.event, "command");
        assert_eq!(cmd.max_us, 5_000, "5ms max -> 5000us");
        assert_eq!(cmd.min_us, 3_000, "3ms min -> 3000us");
        assert_eq!(cmd.samples, 4, "two samples per shard, two shards");
        assert!(cmd.avg_us > 0);
    }

    #[tokio::test]
    async fn slowlog_returns_real_entries() {
        let state = state_with_shards(&[1, 1]);
        let entries = state.get_slowlog(10).await;

        assert_eq!(entries.len(), 2, "one entry per shard");
        assert_eq!(entries[0].command, "GET key");
        assert_eq!(entries[0].client_addr.as_deref(), Some("127.0.0.1:6379"));
        assert_eq!(entries[0].client_name, None);
    }

    #[tokio::test]
    async fn unwired_state_returns_empty_not_panic() {
        let state = DebugState::new(ServerInfo::default(), Vec::new());
        assert!(state.get_shard_stats().await.is_empty());
        assert!(state.get_latency().await.is_empty());
        assert!(state.get_slowlog(10).await.is_empty());
        assert_eq!(state.role(), "standalone");
    }

    /// End-to-end panel render: the shard-stats partial handler must produce a
    /// non-empty table populated with real key counts from the live fixture.
    #[tokio::test]
    async fn shard_stats_partial_renders_non_empty_panel() {
        use http_body_util::BodyExt;

        let state = state_with_shards(&[42, 8]);
        let response = crate::web_ui::handlers::handle_partial_shard_stats(&state).await;
        assert_eq!(response.status(), hyper::StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let html = String::from_utf8_lossy(&body);

        assert!(html.contains("Shard Statistics"), "panel header present");
        assert!(
            !html.contains("No shard statistics available"),
            "panel must not render the empty-state placeholder"
        );
        // Real per-shard key counts from the fixture appear in the table.
        assert!(html.contains("42"), "shard 0 key count rendered");
        assert!(html.contains('8'), "shard 1 key count rendered");
    }
}
