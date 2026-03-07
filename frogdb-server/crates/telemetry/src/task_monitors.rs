//! tokio-metrics TaskMonitor registry for per-task busy/idle/scheduled timing.

use std::sync::Arc;
use std::time::Duration;

use frogdb_core::MetricsRecorder;
use tokio::task::JoinHandle;
use tokio_metrics::TaskMonitor;

/// Registry of named `TaskMonitor` instances.
///
/// Monitors are registered during server construction, then `spawn_collector`
/// starts a background task that periodically drains interval snapshots and
/// records them as Prometheus gauges/counters.
pub struct TaskMonitorRegistry {
    monitors: Vec<(String, TaskMonitor)>,
}

impl Default for TaskMonitorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskMonitorRegistry {
    pub fn new() -> Self {
        Self {
            monitors: Vec::new(),
        }
    }

    /// Register a named monitor and return it for instrumenting spawned tasks.
    pub fn register(&mut self, name: &str) -> TaskMonitor {
        let monitor = TaskMonitor::new();
        self.monitors.push((name.to_owned(), monitor.clone()));
        monitor
    }

    /// Spawn a background task that periodically drains each monitor's
    /// interval stats and records them to the given `MetricsRecorder`.
    pub fn spawn_collector(
        self,
        recorder: Arc<dyn MetricsRecorder>,
        interval: Duration,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            // Keep per-monitor interval iterators alive across ticks so we
            // only see the delta since the last collection.
            let mut iters: Vec<_> = self.monitors.iter().map(|(_, m)| m.intervals()).collect();

            loop {
                ticker.tick().await;

                for (idx, (name, _monitor)) in self.monitors.iter().enumerate() {
                    // Drain all accumulated intervals, keeping only the last
                    // (most recent complete interval).
                    let interval = iters[idx].by_ref().last();
                    let Some(stats) = interval else {
                        continue;
                    };

                    let labels: [(&str, &str); 1] = [("task", name)];

                    recorder.record_gauge(
                        "frogdb_task_instrumented_count",
                        stats.instrumented_count as f64,
                        &labels,
                    );
                    recorder.record_gauge(
                        "frogdb_task_dropped_count",
                        stats.dropped_count as f64,
                        &labels,
                    );
                    recorder.record_gauge(
                        "frogdb_task_total_poll_duration_seconds",
                        stats.total_poll_duration.as_secs_f64(),
                        &labels,
                    );
                    recorder.record_gauge(
                        "frogdb_task_total_scheduled_duration_seconds",
                        stats.total_scheduled_duration.as_secs_f64(),
                        &labels,
                    );
                    recorder.record_gauge(
                        "frogdb_task_total_idle_duration_seconds",
                        stats.total_idle_duration.as_secs_f64(),
                        &labels,
                    );

                    let mean_poll = if stats.total_poll_count > 0 {
                        stats.total_poll_duration.as_secs_f64() / stats.total_poll_count as f64
                    } else {
                        0.0
                    };
                    recorder.record_gauge(
                        "frogdb_task_mean_poll_duration_seconds",
                        mean_poll,
                        &labels,
                    );
                }
            }
        })
    }
}
