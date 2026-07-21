//! FrogDB Metrics and Observability
//!
//! This crate provides comprehensive metrics collection and export for FrogDB:
//! - Prometheus HTTP endpoint (`/metrics`)
//! - OTLP push export (optional)
//! - Health check endpoints (`/health/live`, `/health/ready`)
//! - System metrics collection (CPU, memory, uptime)
//!
//! # Emitting metrics
//!
//! Metrics are emitted through the typed handles generated in
//! [`frogdb_types::metrics::definitions`] (re-exported here as
//! [`definitions`]). A handle fixes the metric's name, type, help text, and
//! label schema in one declaration; the `MetricsRecorder` trait object it is
//! handed decides which backend (Prometheus, OTLP, no-op) receives the
//! sample. Raw `recorder.increment_counter(...)` calls with string names are
//! reserved for backend implementations — `just lint-metrics-chokepoint`
//! enforces this.
//!
//! ```ignore
//! use frogdb_telemetry::definitions::CommandsTotal;
//!
//! CommandsTotal::inc(&*recorder, "GET");
//! ```

pub mod config;
pub mod health;
pub mod http_handlers;
mod interned_registry;
pub mod latency_bands;
pub mod node_state;
pub mod otlp;
pub mod prometheus_recorder;
pub mod status;
pub mod system;
pub mod task_monitors;
pub mod tracing;

#[cfg(feature = "testing")]
pub mod testing;

use std::sync::Arc;
use std::time::Instant;

pub use config::MetricsConfig;
pub use config::TracingConfig;
pub use health::HealthChecker;
pub use http_handlers::{
    handle_health_live, handle_health_ready, handle_metrics, handle_status_json,
};
pub use latency_bands::LatencyBandTracker;
pub use node_state::{NodeStateSnapshot, ShardScatterError, ShardState};
pub use prometheus_recorder::{
    CommandSnapshot, DashboardMetrics, PrometheusRecorder, ShardSnapshot,
};
pub use status::{ServerStatus, StatusCollector, StatusCollectorConfig};
pub use system::SystemMetricsCollector;
pub use task_monitors::TaskMonitorRegistry;
pub use tracing::{
    OtelTracer, RecentTraceEntry, RequestSpan, ScatterGatherSpan, SharedTracer, TracingStatus,
    create_tracer,
};

// Re-export the typed metric registry from its home in frogdb-types so
// existing `frogdb_telemetry::definitions::…` / `ALL_METRICS` imports keep
// working (dashboard-gen, server call sites).
pub use frogdb_types::metrics::{
    ALL_METRICS, METRICS_COUNT, MetricDefinition, MetricType, definition_for, definitions, labels,
};

#[cfg(feature = "testing")]
pub use tracing::TestTracer;

use definitions::{CommandsDuration, CommandsErrors, CommandsTotal};
use frogdb_core::MetricsRecorder;

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

    /// Create a timer with a pre-captured start time, avoiding a redundant `Instant::now()` call.
    pub fn with_start_time(
        start: Instant,
        command: String,
        recorder: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self {
            start,
            command,
            recorder,
        }
    }

    /// Record the command completion (success).
    pub fn finish(self) {
        let elapsed = self.start.elapsed();

        CommandsTotal::inc(&*self.recorder, &self.command);
        CommandsDuration::observe(&*self.recorder, elapsed.as_secs_f64(), &self.command);
        self.recorder
            .record_command_latency_ms(elapsed.as_millis() as u64);
    }

    /// Record a command error.
    pub fn finish_with_error(self, error_type: &str) {
        let elapsed = self.start.elapsed();

        CommandsTotal::inc(&*self.recorder, &self.command);
        CommandsDuration::observe(&*self.recorder, elapsed.as_secs_f64(), &self.command);
        CommandsErrors::inc(&*self.recorder, &self.command, error_type);
        // Errors still count for latency band tracking
        self.recorder
            .record_command_latency_ms(elapsed.as_millis() as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prometheus_recorder::PrometheusRecorder;

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
