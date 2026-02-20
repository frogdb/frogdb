//! FrogDB Metrics and Observability
//!
//! This crate provides comprehensive metrics collection and export for FrogDB:
//! - Prometheus HTTP endpoint (`/metrics`)
//! - OTLP push export (optional)
//! - Health check endpoints (`/health/live`, `/health/ready`)
//! - System metrics collection (CPU, memory, uptime)
//! - Typed metrics with compile-time safety

pub mod bundle;
pub mod config;
pub mod debug;
pub mod definitions;
pub mod health;
pub mod hotshards;
pub mod labels;
pub mod latency_bands;
pub mod memorydiag;
pub mod otlp;
pub mod prometheus_recorder;
pub mod server;
pub mod status;
pub mod system;
pub mod tracing;
pub mod typed;

#[cfg(feature = "testing")]
pub mod testing;

use std::sync::Arc;
use std::time::Instant;

pub use bundle::{
    BundleConfig, BundleGenerator, BundleInfo, BundleManifest, BundleStore, ClusterStateJson,
    DiagnosticCollector, DiagnosticData, ShardMemoryJson, SlowlogEntryJson, TraceEntryJson,
};
pub use config::MetricsConfig;
pub use config::TracingConfig;
pub use debug::{ConfigEntry, DebugState, ServerInfo};
pub use health::HealthChecker;
pub use hotshards::{
    format_hotshards_info, format_hotshards_report, HotShardCollector, HotShardConfig,
    HotShardReport, ShardStats, ShardStatus,
};
pub use latency_bands::LatencyBandTracker;
pub use memorydiag::{
    calculate_variance, format_report as format_memory_report, MemoryDiagCollector,
    MemoryDiagConfig, MemoryDiagReport, MemorySummary, ShardMemoryVariance,
};
pub use prometheus_recorder::PrometheusRecorder;
pub use server::MetricsServer;
pub use status::{ServerStatus, StatusCollector, StatusCollectorConfig};
pub use system::SystemMetricsCollector;
pub use tracing::{
    create_tracer, OtelTracer, RecentTraceEntry, RequestSpan, ScatterGatherSpan, SharedTracer,
    TracingStatus,
};

// Re-export typed metrics system
pub use definitions::{ALL_METRICS, METRICS_COUNT};
pub use labels::{
    BlockingResolution, ErrorType, PersistenceErrorType, RejectionReason, ScatterGatherStatus,
    ServerMode, TransactionOutcome,
};
pub use typed::{MetricDefinition, MetricType};

#[cfg(feature = "testing")]
pub use tracing::TestTracer;

use frogdb_core::MetricsRecorder;

/// Metric names used throughout FrogDB.
pub mod metric_names {
    // System metrics
    pub const UPTIME_SECONDS: &str = "frogdb_uptime_seconds";
    pub const INFO: &str = "frogdb_info";
    pub const MEMORY_RSS_BYTES: &str = "frogdb_memory_rss_bytes";
    pub const CPU_USER_SECONDS: &str = "frogdb_cpu_user_seconds_total";
    pub const CPU_SYSTEM_SECONDS: &str = "frogdb_cpu_system_seconds_total";

    // Connection metrics
    pub const CONNECTIONS_TOTAL: &str = "frogdb_connections_total";
    pub const CONNECTIONS_CURRENT: &str = "frogdb_connections_current";
    pub const CONNECTIONS_REJECTED: &str = "frogdb_connections_rejected_total";

    // Command metrics
    pub const COMMANDS_TOTAL: &str = "frogdb_commands_total";
    pub const COMMANDS_DURATION: &str = "frogdb_commands_duration_seconds";
    pub const COMMANDS_ERRORS: &str = "frogdb_commands_errors_total";

    // Data/keyspace metrics
    pub const KEYS_TOTAL: &str = "frogdb_keys_total";
    pub const KEYS_WITH_EXPIRY: &str = "frogdb_keys_with_expiry";
    pub const KEYS_EXPIRED: &str = "frogdb_keys_expired_total";
    pub const KEYSPACE_HITS: &str = "frogdb_keyspace_hits_total";
    pub const KEYSPACE_MISSES: &str = "frogdb_keyspace_misses_total";

    // Shard metrics
    pub const SHARD_KEYS: &str = "frogdb_shard_keys";
    pub const SHARD_MEMORY_BYTES: &str = "frogdb_shard_memory_bytes";
    pub const SHARD_QUEUE_DEPTH: &str = "frogdb_shard_queue_depth";

    // Persistence metrics
    pub const WAL_WRITES: &str = "frogdb_wal_writes_total";
    pub const WAL_BYTES: &str = "frogdb_wal_bytes_total";
    pub const WAL_FLUSH_DURATION: &str = "frogdb_wal_flush_duration_seconds";
    pub const SNAPSHOT_IN_PROGRESS: &str = "frogdb_snapshot_in_progress";
    pub const SNAPSHOT_LAST_TIMESTAMP: &str = "frogdb_snapshot_last_timestamp";

    // Persistence lag metrics
    pub const WAL_PENDING_OPS: &str = "frogdb_wal_pending_ops";
    pub const WAL_PENDING_BYTES: &str = "frogdb_wal_pending_bytes";
    pub const WAL_DURABILITY_LAG_MS: &str = "frogdb_wal_durability_lag_ms";
    pub const WAL_SYNC_LAG_MS: &str = "frogdb_wal_sync_lag_ms";
    pub const WAL_LAST_FLUSH_TIMESTAMP: &str = "frogdb_wal_last_flush_timestamp";
    pub const WAL_LAST_SYNC_TIMESTAMP: &str = "frogdb_wal_last_sync_timestamp";

    // Pub/Sub metrics
    pub const PUBSUB_CHANNELS: &str = "frogdb_pubsub_channels";
    pub const PUBSUB_PATTERNS: &str = "frogdb_pubsub_patterns";
    pub const PUBSUB_SUBSCRIBERS: &str = "frogdb_pubsub_subscribers";
    pub const PUBSUB_MESSAGES: &str = "frogdb_pubsub_messages_total";

    // Network metrics
    pub const NET_INPUT_BYTES: &str = "frogdb_net_input_bytes_total";
    pub const NET_OUTPUT_BYTES: &str = "frogdb_net_output_bytes_total";

    // Memory/Eviction metrics
    pub const MEMORY_USED_BYTES: &str = "frogdb_memory_used_bytes";
    pub const MEMORY_MAXMEMORY_BYTES: &str = "frogdb_memory_maxmemory_bytes";
    pub const MEMORY_PEAK_BYTES: &str = "frogdb_memory_peak_bytes";
    pub const MEMORY_FRAGMENTATION_RATIO: &str = "frogdb_memory_fragmentation_ratio";
    pub const EVICTION_KEYS_TOTAL: &str = "frogdb_eviction_keys_total";
    pub const EVICTION_BYTES_TOTAL: &str = "frogdb_eviction_bytes_total";
    pub const EVICTION_SAMPLES_TOTAL: &str = "frogdb_eviction_samples_total";
    pub const EVICTION_OOM_TOTAL: &str = "frogdb_eviction_oom_total";

    // Blocking commands metrics
    pub const BLOCKED_CLIENTS: &str = "frogdb_blocked_clients";
    pub const BLOCKED_KEYS: &str = "frogdb_blocked_keys";
    pub const BLOCKED_TIMEOUT_TOTAL: &str = "frogdb_blocked_timeout_total";
    pub const BLOCKED_SATISFIED_TOTAL: &str = "frogdb_blocked_satisfied_total";

    // Lua scripting metrics
    pub const LUA_SCRIPTS_TOTAL: &str = "frogdb_lua_scripts_total";
    pub const LUA_SCRIPTS_DURATION: &str = "frogdb_lua_scripts_duration_seconds";
    pub const LUA_SCRIPTS_ERRORS: &str = "frogdb_lua_scripts_errors_total";
    pub const LUA_SCRIPTS_CACHE_HITS: &str = "frogdb_lua_scripts_cache_hits_total";
    pub const LUA_SCRIPTS_CACHE_MISSES: &str = "frogdb_lua_scripts_cache_misses_total";

    // Transaction metrics
    pub const TRANSACTIONS_TOTAL: &str = "frogdb_transactions_total";
    pub const TRANSACTIONS_DURATION: &str = "frogdb_transactions_duration_seconds";
    pub const TRANSACTIONS_QUEUED_COMMANDS: &str = "frogdb_transactions_queued_commands";

    // Scatter-gather metrics
    pub const SCATTER_GATHER_TOTAL: &str = "frogdb_scatter_gather_total";
    pub const SCATTER_GATHER_DURATION: &str = "frogdb_scatter_gather_duration_seconds";
    pub const SCATTER_GATHER_SHARDS: &str = "frogdb_scatter_gather_shards";

    // Latency band metrics
    pub const LATENCY_BAND_REQUESTS: &str = "frogdb_latency_band_requests_total";

    // Additional metrics from OBSERVABILITY.md spec
    pub const SHARD_QUEUE_LATENCY: &str = "frogdb_shard_queue_latency_seconds";
    pub const CONNECTIONS_MAX: &str = "frogdb_connections_max";
    pub const SNAPSHOT_DURATION: &str = "frogdb_snapshot_duration_seconds";
    pub const SNAPSHOT_SIZE_BYTES: &str = "frogdb_snapshot_size_bytes";
    pub const PERSISTENCE_ERRORS: &str = "frogdb_persistence_errors_total";
}

/// Create a metrics recorder based on configuration.
///
/// Returns a `PrometheusRecorder` that can be used throughout the application.
/// If OTLP is enabled, a `CompositeRecorder` is returned that writes to both
/// Prometheus and OTLP.
pub fn create_metrics_recorder(config: &MetricsConfig) -> Arc<dyn MetricsRecorder> {
    if !config.enabled {
        return Arc::new(frogdb_core::NoopMetricsRecorder::new());
    }

    let recorder = PrometheusRecorder::new();

    // Record server info gauge
    recorder.record_gauge(
        metric_names::INFO,
        1.0,
        &[
            ("version", env!("CARGO_PKG_VERSION")),
            ("mode", "standalone"),
        ],
    );

    Arc::new(recorder)
}

/// Helper struct for timing command execution.
pub struct CommandTimer {
    start: Instant,
    command: String,
    recorder: Arc<dyn MetricsRecorder>,
    band_tracker: Option<Arc<LatencyBandTracker>>,
}

impl CommandTimer {
    /// Start timing a command.
    pub fn new(command: String, recorder: Arc<dyn MetricsRecorder>) -> Self {
        Self {
            start: Instant::now(),
            command,
            recorder,
            band_tracker: None,
        }
    }

    /// Start timing a command with latency band tracking.
    pub fn with_band_tracker(
        command: String,
        recorder: Arc<dyn MetricsRecorder>,
        band_tracker: Option<Arc<LatencyBandTracker>>,
    ) -> Self {
        Self {
            start: Instant::now(),
            command,
            recorder,
            band_tracker,
        }
    }

    /// Record the command completion (success).
    pub fn finish(self) {
        let elapsed = self.start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        self.recorder.increment_counter(
            metric_names::COMMANDS_TOTAL,
            1,
            &[("command", &self.command)],
        );
        self.recorder.record_histogram(
            metric_names::COMMANDS_DURATION,
            elapsed_secs,
            &[("command", &self.command)],
        );

        // Record to latency band tracker
        if let Some(tracker) = &self.band_tracker {
            tracker.record(elapsed.as_millis() as u64);
        }
    }

    /// Record a command error.
    pub fn finish_with_error(self, error_type: &str) {
        let elapsed = self.start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        self.recorder.increment_counter(
            metric_names::COMMANDS_TOTAL,
            1,
            &[("command", &self.command)],
        );
        self.recorder.record_histogram(
            metric_names::COMMANDS_DURATION,
            elapsed_secs,
            &[("command", &self.command)],
        );
        self.recorder.increment_counter(
            metric_names::COMMANDS_ERRORS,
            1,
            &[("command", &self.command), ("error", error_type)],
        );

        // Record to latency band tracker (errors still count for latency)
        if let Some(tracker) = &self.band_tracker {
            tracker.record(elapsed.as_millis() as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prometheus_recorder::PrometheusRecorder;

    #[test]
    fn test_create_noop_recorder_when_disabled() {
        let config = MetricsConfig {
            enabled: false,
            ..Default::default()
        };
        let recorder = create_metrics_recorder(&config);
        // Should not panic when recording
        recorder.increment_counter("test", 1, &[]);
    }

    #[test]
    fn test_create_prometheus_recorder_when_enabled() {
        let config = MetricsConfig {
            enabled: true,
            ..Default::default()
        };
        let recorder = create_metrics_recorder(&config);
        recorder.increment_counter("test", 1, &[]);
    }

    #[test]
    fn test_command_timer_finish() {
        let recorder = Arc::new(PrometheusRecorder::new());
        let timer = CommandTimer::new("GET".to_string(), recorder.clone());
        timer.finish();
        let output = recorder.encode();
        assert!(output.contains("frogdb_commands_total"));
        assert!(output.contains("frogdb_commands_duration_seconds"));
        assert!(output.contains(r#"command="GET""#));
    }

    #[test]
    fn test_command_timer_finish_with_error() {
        let recorder = Arc::new(PrometheusRecorder::new());
        let timer = CommandTimer::new("SET".to_string(), recorder.clone());
        timer.finish_with_error("oom");
        let output = recorder.encode();
        assert!(output.contains("frogdb_commands_errors_total"));
        assert!(output.contains(r#"command="SET""#));
        assert!(output.contains(r#"error="oom""#));
    }
}
