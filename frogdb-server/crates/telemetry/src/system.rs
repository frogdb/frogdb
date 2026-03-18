//! System metrics collection.

use frogdb_core::MetricsRecorder;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::time::interval;
use tracing::{debug, warn};

use crate::metric_names;

/// Get cumulative CPU times (user, system) via getrusage.
#[cfg(unix)]
fn get_cpu_times() -> (f64, f64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        let usage = usage.assume_init();
        let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1_000_000.0;
        let sys = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1_000_000.0;
        (user, sys)
    }
}

/// Collects and reports system-level metrics.
pub struct SystemMetricsCollector {
    recorder: Arc<dyn MetricsRecorder>,
    start_time: Instant,
    system: System,
    pid: Pid,
    /// Maximum memory limit (0 = unlimited).
    maxmemory: Arc<AtomicU64>,
    /// Per-shard memory usage atomics, summed for total used memory.
    shard_memory: Arc<Vec<AtomicU64>>,
}

impl SystemMetricsCollector {
    /// Create a new system metrics collector.
    pub fn new(
        recorder: Arc<dyn MetricsRecorder>,
        maxmemory: Arc<AtomicU64>,
        shard_memory: Arc<Vec<AtomicU64>>,
    ) -> Self {
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
            maxmemory,
            shard_memory,
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

        // CPU times via getrusage (cumulative seconds)
        #[cfg(unix)]
        {
            let (user, sys) = get_cpu_times();
            self.recorder
                .record_gauge(metric_names::CPU_USER_SECONDS, user, &[]);
            self.recorder
                .record_gauge(metric_names::CPU_SYSTEM_SECONDS, sys, &[]);
        }

        // Maxmemory gauge
        let maxmem = self.maxmemory.load(Ordering::Relaxed);
        if maxmem > 0 {
            self.recorder
                .record_gauge(metric_names::MEMORY_MAXMEMORY_BYTES, maxmem as f64, &[]);
        }

        // Process metrics
        if let Some(process) = self.system.process(self.pid) {
            // Memory (RSS)
            let rss = process.memory();
            self.recorder
                .record_gauge(metric_names::MEMORY_RSS_BYTES, rss as f64, &[]);

            // Memory fragmentation ratio = RSS / used
            let used: u64 = self
                .shard_memory
                .iter()
                .map(|a| a.load(Ordering::Relaxed))
                .sum();
            if used > 0 {
                self.recorder.record_gauge(
                    metric_names::MEMORY_FRAGMENTATION_RATIO,
                    rss as f64 / used as f64,
                    &[],
                );
            }

            let cpu_usage = process.cpu_usage() as f64;
            debug!(
                uptime_secs = uptime,
                memory_mb = rss as f64 / 1_048_576.0,
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
        maxmemory: Arc<AtomicU64>,
        shard_memory: Arc<Vec<AtomicU64>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut collector = SystemMetricsCollector::new(recorder, maxmemory, shard_memory);
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
    recorder.record_gauge(
        metric_names::INFO,
        1.0,
        &[("version", version), ("mode", mode)],
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::NoopMetricsRecorder;

    fn test_maxmemory() -> Arc<AtomicU64> {
        Arc::new(AtomicU64::new(0))
    }

    fn test_shard_memory() -> Arc<Vec<AtomicU64>> {
        Arc::new(vec![])
    }

    #[test]
    fn test_system_metrics_collector_creation() {
        let recorder = Arc::new(NoopMetricsRecorder::new());
        let collector =
            SystemMetricsCollector::new(recorder, test_maxmemory(), test_shard_memory());
        assert!(collector.start_time.elapsed().as_secs() < 1);
    }

    #[test]
    fn test_system_metrics_collection() {
        let recorder = Arc::new(NoopMetricsRecorder::new());
        let mut collector =
            SystemMetricsCollector::new(recorder, test_maxmemory(), test_shard_memory());
        // Should not panic
        collector.collect();
    }

    #[tokio::test]
    async fn test_spawn_collector() {
        let recorder = Arc::new(NoopMetricsRecorder::new());
        let handle = SystemMetricsCollector::spawn_collector(
            recorder,
            Duration::from_millis(100),
            test_maxmemory(),
            test_shard_memory(),
        );

        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Abort the task
        handle.abort();
        let _ = handle.await;
    }
}
