//! System metrics collection.

use frogdb_core::MetricsRecorder;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::time::interval;
use tracing::{debug, warn};

use crate::metric_names;

/// Collects and reports system-level metrics.
pub struct SystemMetricsCollector {
    recorder: Arc<dyn MetricsRecorder>,
    start_time: Instant,
    system: System,
    pid: Pid,
}

impl SystemMetricsCollector {
    /// Create a new system metrics collector.
    pub fn new(recorder: Arc<dyn MetricsRecorder>) -> Self {
        let pid = Pid::from_u32(std::process::id());
        let mut system = System::new();
        // Initial refresh to populate process data
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::everything(),
        );

        Self {
            recorder,
            start_time: Instant::now(),
            system,
            pid,
        }
    }

    /// Collect system metrics once.
    pub fn collect(&mut self) {
        // Refresh process-specific metrics
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::everything(),
        );

        // Uptime
        let uptime = self.start_time.elapsed().as_secs_f64();
        self.recorder
            .record_gauge(metric_names::UPTIME_SECONDS, uptime, &[]);

        // Process metrics
        if let Some(process) = self.system.process(self.pid) {
            // Memory (RSS)
            let memory_bytes = process.memory() as f64;
            self.recorder
                .record_gauge(metric_names::MEMORY_RSS_BYTES, memory_bytes, &[]);

            // CPU time - sysinfo gives us percentage, but we want cumulative seconds
            // We'll approximate by tracking CPU usage over time
            let cpu_usage = process.cpu_usage() as f64;
            debug!(
                uptime_secs = uptime,
                memory_mb = memory_bytes / 1_048_576.0,
                cpu_percent = cpu_usage,
                "System metrics collected"
            );
        } else {
            warn!(pid = ?self.pid, "Failed to get process info");
        }
    }

    /// Spawn a background task that collects system metrics periodically.
    pub fn spawn_collector(
        recorder: Arc<dyn MetricsRecorder>,
        collection_interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut collector = SystemMetricsCollector::new(recorder);
            let mut ticker = interval(collection_interval);

            loop {
                ticker.tick().await;
                collector.collect();
            }
        })
    }
}

/// Record server start info metric.
pub fn record_server_info(recorder: &Arc<dyn MetricsRecorder>, version: &str, mode: &str) {
    recorder.record_gauge(metric_names::INFO, 1.0, &[("version", version), ("mode", mode)]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::NoopMetricsRecorder;

    #[test]
    fn test_system_metrics_collector_creation() {
        let recorder = Arc::new(NoopMetricsRecorder::new());
        let collector = SystemMetricsCollector::new(recorder);
        assert!(collector.start_time.elapsed().as_secs() < 1);
    }

    #[test]
    fn test_system_metrics_collection() {
        let recorder = Arc::new(NoopMetricsRecorder::new());
        let mut collector = SystemMetricsCollector::new(recorder);
        // Should not panic
        collector.collect();
    }

    #[tokio::test]
    async fn test_spawn_collector() {
        let recorder = Arc::new(NoopMetricsRecorder::new());
        let handle =
            SystemMetricsCollector::spawn_collector(recorder, Duration::from_millis(100));

        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Abort the task
        handle.abort();
        let _ = handle.await;
    }
}
