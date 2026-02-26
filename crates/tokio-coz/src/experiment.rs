use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rand::Rng;

use crate::config::ProfilerConfig;
use crate::progress::ProgressPointRegistry;
use crate::results::{ExperimentResult, ResultCollector};
use crate::state::SharedState;

/// Runs the experiment loop as a background tokio task.
///
/// For each discovered span, iterates through speedup percentages (e.g. 0%, 10%, ..., 100%),
/// activating one experiment at a time, measuring progress point deltas, then recording results.
pub struct ExperimentEngine {
    state: Arc<SharedState>,
    config: ProfilerConfig,
    results: Arc<ResultCollector>,
    running: Arc<AtomicBool>,
}

impl ExperimentEngine {
    pub fn new(
        state: Arc<SharedState>,
        config: ProfilerConfig,
        results: Arc<ResultCollector>,
        running: Arc<AtomicBool>,
    ) -> Self {
        Self {
            state,
            config,
            results,
            running,
        }
    }

    /// Run the experiment loop. Call this from a spawned tokio task.
    pub async fn run(&self) {
        // Wait a bit for spans to be discovered before starting experiments.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        while self.running.load(Ordering::Relaxed) {
            let span_keys = self.state.all_span_keys();
            if span_keys.is_empty() {
                // No spans discovered yet, wait and retry.
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }

            // Randomly select a span to experiment on.
            let span = {
                let mut rng = rand::thread_rng();
                span_keys[rng.gen_range(0..span_keys.len())]
            };

            let span_name = self
                .state
                .span_name(span)
                .unwrap_or_else(|| format!("span_{}", span.0));

            for &speedup in &self.config.speedup_steps {
                if !self.running.load(Ordering::Relaxed) {
                    break;
                }

                for _round in 0..self.config.rounds_per_experiment {
                    if !self.running.load(Ordering::Relaxed) {
                        break;
                    }

                    // Snapshot progress counters before the experiment.
                    let throughput_before = ProgressPointRegistry::global()
                        .map(|r| r.snapshot_throughput())
                        .unwrap_or_default();

                    // Activate the experiment.
                    self.state.activate_experiment(span, speedup);

                    // Let the experiment run.
                    tokio::time::sleep(self.config.experiment_duration).await;

                    // Deactivate.
                    self.state.deactivate_experiment();

                    // Snapshot progress counters after the experiment.
                    let throughput_after = ProgressPointRegistry::global()
                        .map(|r| r.snapshot_throughput())
                        .unwrap_or_default();

                    // Compute deltas for each progress point.
                    let throughput_deltas = compute_deltas(&throughput_before, &throughput_after);

                    self.results.record(ExperimentResult {
                        span_name: span_name.clone(),
                        span_key: span,
                        speedup_pct: speedup,
                        duration: self.config.experiment_duration,
                        throughput_deltas,
                    });

                    // Brief pause between rounds for state to settle.
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    }
}

/// Compute deltas between two snapshots of (name, count) pairs.
fn compute_deltas(before: &[(String, u64)], after: &[(String, u64)]) -> Vec<(String, u64)> {
    after
        .iter()
        .map(|(name, after_count)| {
            let before_count = before
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, c)| *c)
                .unwrap_or(0);
            (name.clone(), after_count.saturating_sub(before_count))
        })
        .collect()
}
