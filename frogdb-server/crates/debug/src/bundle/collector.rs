//! Diagnostic data collection from shards and traces.

use std::sync::Arc;

use frogdb_core::{ShardMemoryStats, ShardMessage};
use frogdb_telemetry::SharedTracer;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

use super::BundleConfig;

/// Collected diagnostic data for a bundle.
#[derive(Debug, Default, Serialize)]
pub struct DiagnosticData {
    /// Per-shard memory statistics.
    pub shard_memory: Vec<ShardMemoryStatsJson>,
    /// Recent trace entries.
    pub traces: Vec<TraceEntryJson>,
    /// Cluster state information.
    pub cluster_state: ClusterStateJson,
    /// Collection metadata.
    pub metadata: CollectionMetadata,
}

/// Serializable shard memory statistics.
#[derive(Debug, Serialize)]
pub struct ShardMemoryStatsJson {
    pub shard_id: usize,
    pub data_memory: usize,
    pub keys: usize,
    pub peak_memory: u64,
    pub memory_limit: u64,
    pub overhead_estimate: usize,
}

impl From<ShardMemoryStats> for ShardMemoryStatsJson {
    fn from(s: ShardMemoryStats) -> Self {
        Self {
            shard_id: s.shard_id,
            data_memory: s.data_memory,
            keys: s.keys,
            peak_memory: s.peak_memory,
            memory_limit: s.memory_limit,
            overhead_estimate: s.overhead_estimate,
        }
    }
}

/// Serializable trace entry.
#[derive(Debug, Serialize)]
pub struct TraceEntryJson {
    pub trace_id: String,
    pub timestamp_ms: u64,
    pub command: String,
    pub sampled: bool,
}

/// Serializable cluster state.
#[derive(Debug, Default, Serialize)]
pub struct ClusterStateJson {
    /// Server mode (standalone/cluster).
    pub mode: String,
    /// Role (primary/replica/standalone).
    pub role: String,
    /// Number of shards.
    pub num_shards: usize,
    /// Whether cluster mode is enabled.
    pub cluster_enabled: bool,
    /// Cluster node information (if available).
    pub nodes: Option<Vec<String>>,
}

/// Metadata about the collection.
#[derive(Debug, Default, Serialize)]
pub struct CollectionMetadata {
    /// Timestamp when collection started (Unix seconds).
    pub collected_at: u64,
    /// Duration of the collection period in seconds (0 for instant).
    pub duration_secs: u64,
    /// FrogDB version.
    pub version: String,
}

/// Collects diagnostic data from shards and tracing infrastructure.
pub struct DiagnosticCollector {
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
    shared_tracer: Option<SharedTracer>,
    config: BundleConfig,
}

impl DiagnosticCollector {
    /// Create a new diagnostic collector.
    pub fn new(
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        shared_tracer: Option<SharedTracer>,
        config: BundleConfig,
    ) -> Self {
        Self {
            shard_senders,
            shared_tracer,
            config,
        }
    }

    /// Collect an instant snapshot of diagnostic data.
    pub async fn collect_instant(&self) -> DiagnosticData {
        let shard_memory = self.gather_memory_stats().await;
        let traces = self.gather_traces();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        DiagnosticData {
            shard_memory,
            traces,
            cluster_state: ClusterStateJson::default(),
            metadata: CollectionMetadata {
                collected_at: now,
                duration_secs: 0,
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
        }
    }

    /// Collect diagnostic data over a time period.
    ///
    /// Takes a snapshot before and after sleeping for `duration_secs`,
    /// allowing comparison of metrics over time.
    pub async fn collect_with_duration(&self, duration_secs: u64) -> DiagnosticData {
        let _before = self.gather_memory_stats().await;

        tokio::time::sleep(std::time::Duration::from_secs(duration_secs)).await;

        let shard_memory = self.gather_memory_stats().await;
        let traces = self.gather_traces();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        DiagnosticData {
            shard_memory,
            traces,
            cluster_state: ClusterStateJson::default(),
            metadata: CollectionMetadata {
                collected_at: now,
                duration_secs,
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
        }
    }

    /// Gather memory stats from all shards.
    async fn gather_memory_stats(&self) -> Vec<ShardMemoryStatsJson> {
        let mut stats = Vec::with_capacity(self.shard_senders.len());

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::MemoryStats { response_tx })
                .await
                .is_ok()
                && let Ok(shard_stats) = response_rx.await
            {
                stats.push(shard_stats.into());
            }
        }

        stats
    }

    /// Gather recent traces.
    fn gather_traces(&self) -> Vec<TraceEntryJson> {
        let Some(tracer) = &self.shared_tracer else {
            return Vec::new();
        };

        tracer
            .get_recent_traces(self.config.max_trace_entries)
            .into_iter()
            .map(|t| TraceEntryJson {
                trace_id: t.trace_id,
                timestamp_ms: t.timestamp_ms,
                command: t.command,
                sampled: t.sampled,
            })
            .collect()
    }
}
