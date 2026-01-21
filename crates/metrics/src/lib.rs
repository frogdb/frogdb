//! FrogDB Metrics and Observability
//!
//! This crate provides comprehensive metrics collection and export for FrogDB:
//! - Prometheus HTTP endpoint (`/metrics`)
//! - OTLP push export (optional)
//! - Health check endpoints (`/health/live`, `/health/ready`)
//! - System metrics collection (CPU, memory, uptime)

pub mod config;
pub mod health;
pub mod otlp;
pub mod prometheus_recorder;
pub mod server;
pub mod system;

use std::sync::Arc;
use std::time::Instant;

pub use config::MetricsConfig;
pub use health::HealthChecker;
pub use prometheus_recorder::PrometheusRecorder;
pub use server::MetricsServer;
pub use system::SystemMetricsCollector;

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
    pub const EVICTION_KEYS_TOTAL: &str = "frogdb_eviction_keys_total";
    pub const EVICTION_BYTES_TOTAL: &str = "frogdb_eviction_bytes_total";
    pub const EVICTION_SAMPLES_TOTAL: &str = "frogdb_eviction_samples_total";
    pub const EVICTION_OOM_TOTAL: &str = "frogdb_eviction_oom_total";
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
        &[("version", env!("CARGO_PKG_VERSION")), ("mode", "standalone")],
    );

    Arc::new(recorder)
}

/// Helper struct for timing command execution.
pub struct CommandTimer {
    start: Instant,
    command: String,
    recorder: Arc<dyn MetricsRecorder>,
}

impl CommandTimer {
    /// Start timing a command.
    pub fn new(command: String, recorder: Arc<dyn MetricsRecorder>) -> Self {
        Self {
            start: Instant::now(),
            command,
            recorder,
        }
    }

    /// Record the command completion (success).
    pub fn finish(self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        self.recorder.increment_counter(
            metric_names::COMMANDS_TOTAL,
            1,
            &[("command", &self.command)],
        );
        self.recorder.record_histogram(
            metric_names::COMMANDS_DURATION,
            elapsed,
            &[("command", &self.command)],
        );
    }

    /// Record a command error.
    pub fn finish_with_error(self, error_type: &str) {
        let elapsed = self.start.elapsed().as_secs_f64();
        self.recorder.increment_counter(
            metric_names::COMMANDS_TOTAL,
            1,
            &[("command", &self.command)],
        );
        self.recorder.record_histogram(
            metric_names::COMMANDS_DURATION,
            elapsed,
            &[("command", &self.command)],
        );
        self.recorder.increment_counter(
            metric_names::COMMANDS_ERRORS,
            1,
            &[("command", &self.command), ("error", error_type)],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
