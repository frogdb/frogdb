//! tokio-metrics TaskMonitor registry for per-task busy/idle/scheduled timing.

use std::sync::Arc;
use std::time::Duration;

use frogdb_core::MetricsRecorder;
use frogdb_types::metrics::definitions::{
    TaskDroppedCount, TaskInstrumentedCount, TaskMeanPollDuration, TaskTotalIdleDuration,
    TaskTotalPollDuration, TaskTotalScheduledDuration,
};
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
                    // Get the metrics delta since the last collection.
                    let interval = iters[idx].next();
                    let Some(stats) = interval else {
                        continue;
                    };

                    let recorder = &*recorder;
                    TaskInstrumentedCount::set(recorder, stats.instrumented_count as f64, name);
                    TaskDroppedCount::set(recorder, stats.dropped_count as f64, name);
                    TaskTotalPollDuration::set(
                        recorder,
                        stats.total_poll_duration.as_secs_f64(),
                        name,
                    );
                    TaskTotalScheduledDuration::set(
                        recorder,
                        stats.total_scheduled_duration.as_secs_f64(),
                        name,
                    );
                    TaskTotalIdleDuration::set(
                        recorder,
                        stats.total_idle_duration.as_secs_f64(),
                        name,
                    );

                    let mean_poll = if stats.total_poll_count > 0 {
                        stats.total_poll_duration.as_secs_f64() / stats.total_poll_count as f64
                    } else {
                        0.0
                    };
                    TaskMeanPollDuration::set(recorder, mean_poll, name);
                }
            }
        })
    }
}
