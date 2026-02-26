use std::collections::HashMap;
use std::time::Duration;

use parking_lot::Mutex;
use serde::Serialize;

use crate::state::SpanKey;

/// Raw result from a single experiment run.
pub struct ExperimentResult {
    pub span_name: String,
    pub span_key: SpanKey,
    pub speedup_pct: u8,
    pub duration: Duration,
    /// (progress_point_name, delta_count) for each progress point during this experiment.
    pub throughput_deltas: Vec<(String, u64)>,
}

/// Aggregated speedup data point for one (span, speedup%) combination.
#[derive(Debug, Clone, Serialize)]
pub struct SpeedupDataPoint {
    pub speedup_pct: u8,
    /// Average throughput rate (events/sec) across all rounds at this speedup.
    pub avg_throughput_rate: f64,
    /// Number of experiment rounds that contributed to this average.
    pub sample_count: u32,
}

/// Full profile for a single span across all speedup levels.
#[derive(Debug, Clone, Serialize)]
pub struct SpanProfile {
    pub span_name: String,
    /// One data point per tested speedup percentage, per progress point.
    /// Key: progress_point_name → list of data points.
    pub curves: HashMap<String, Vec<SpeedupDataPoint>>,
}

/// Collects experiment results and computes optimization curves.
pub struct ResultCollector {
    results: Mutex<Vec<ExperimentResult>>,
}

impl Default for ResultCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ResultCollector {
    pub fn new() -> Self {
        Self {
            results: Mutex::new(Vec::new()),
        }
    }

    /// Record one experiment result.
    pub fn record(&self, result: ExperimentResult) {
        self.results.lock().push(result);
    }

    /// Aggregate all results into per-span profiles.
    pub fn aggregate(&self) -> Vec<SpanProfile> {
        let results = self.results.lock();

        // Group by span_name.
        let mut by_span: HashMap<String, Vec<&ExperimentResult>> = HashMap::new();
        for r in results.iter() {
            by_span.entry(r.span_name.clone()).or_default().push(r);
        }

        let mut profiles = Vec::new();

        for (span_name, span_results) in by_span {
            // Discover all progress point names referenced in these results.
            let mut progress_names: Vec<String> = span_results
                .iter()
                .flat_map(|r| r.throughput_deltas.iter().map(|(name, _)| name.clone()))
                .collect();
            progress_names.sort();
            progress_names.dedup();

            let mut curves: HashMap<String, Vec<SpeedupDataPoint>> = HashMap::new();

            for progress_name in &progress_names {
                // Group by speedup_pct.
                let mut by_speedup: HashMap<u8, Vec<f64>> = HashMap::new();

                for r in &span_results {
                    let delta = r
                        .throughput_deltas
                        .iter()
                        .find(|(n, _)| n == progress_name)
                        .map(|(_, d)| *d)
                        .unwrap_or(0);

                    let rate = delta as f64 / r.duration.as_secs_f64();
                    by_speedup.entry(r.speedup_pct).or_default().push(rate);
                }

                let mut data_points: Vec<SpeedupDataPoint> = by_speedup
                    .into_iter()
                    .map(|(pct, rates)| {
                        let avg = rates.iter().sum::<f64>() / rates.len() as f64;
                        SpeedupDataPoint {
                            speedup_pct: pct,
                            avg_throughput_rate: avg,
                            sample_count: rates.len() as u32,
                        }
                    })
                    .collect();

                data_points.sort_by_key(|p| p.speedup_pct);
                curves.insert(progress_name.clone(), data_points);
            }

            profiles.push(SpanProfile { span_name, curves });
        }

        profiles.sort_by(|a, b| a.span_name.cmp(&b.span_name));
        profiles
    }
}

/// Compute the optimization impact for a span profile.
///
/// Returns the estimated throughput improvement (%) at 100% speedup for each progress point.
pub fn compute_impact(profile: &SpanProfile) -> HashMap<String, f64> {
    let mut impacts = HashMap::new();

    for (progress_name, data_points) in &profile.curves {
        // Find the baseline (0% speedup) rate.
        let baseline = data_points
            .iter()
            .find(|p| p.speedup_pct == 0)
            .map(|p| p.avg_throughput_rate)
            .unwrap_or(0.0);

        if baseline <= 0.0 {
            impacts.insert(progress_name.clone(), 0.0);
            continue;
        }

        // Find the maximum speedup rate (usually at 100%).
        let max_rate = data_points
            .iter()
            .max_by(|a, b| {
                a.avg_throughput_rate
                    .partial_cmp(&b.avg_throughput_rate)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|p| p.avg_throughput_rate)
            .unwrap_or(baseline);

        let impact_pct = ((max_rate - baseline) / baseline) * 100.0;
        impacts.insert(progress_name.clone(), impact_pct);
    }

    impacts
}
