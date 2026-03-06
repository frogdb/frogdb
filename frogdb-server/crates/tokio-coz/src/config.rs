use std::time::Duration;

/// Controls the order in which spans are selected for experiments.
#[derive(Debug, Clone, Default)]
pub enum SelectionStrategy {
    /// Pick a span uniformly at random each cycle. Some spans may be skipped
    /// entirely in short runs (default, matches original behavior).
    #[default]
    Random,
    /// Cycle through all discovered spans in order, guaranteeing every span
    /// is visited before any is revisited.
    RoundRobin,
}

/// Configuration for the causal profiler.
#[derive(Debug, Clone)]
pub struct ProfilerConfig {
    /// Duration of each experiment (one span × one speedup%).
    pub experiment_duration: Duration,
    /// Speedup percentages to test (e.g. [0, 10, 20, ..., 100]).
    pub speedup_steps: Vec<u8>,
    /// Minimum delay threshold — don't sleep for less than this.
    pub min_delay: Duration,
    /// Maximum delay cap per poll to prevent pathological stalls.
    pub max_delay: Duration,
    /// Output file path for JSON results.
    pub output_path: Option<String>,
    /// Number of rounds to repeat each (span, speedup%) pair.
    pub rounds_per_experiment: u32,
    /// Strategy for selecting which span to experiment on each cycle.
    pub selection_strategy: SelectionStrategy,
}

impl Default for ProfilerConfig {
    fn default() -> Self {
        Self {
            experiment_duration: Duration::from_secs(2),
            speedup_steps: (0..=10).map(|i| i * 10).collect(),
            min_delay: Duration::from_micros(1),
            max_delay: Duration::from_millis(100),
            output_path: Some("tokio-coz-profile.json".to_string()),
            rounds_per_experiment: 3,
            selection_strategy: SelectionStrategy::Random,
        }
    }
}

impl ProfilerConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn experiment_duration(mut self, d: Duration) -> Self {
        self.experiment_duration = d;
        self
    }

    pub fn speedup_steps(mut self, steps: Vec<u8>) -> Self {
        self.speedup_steps = steps;
        self
    }

    pub fn min_delay(mut self, d: Duration) -> Self {
        self.min_delay = d;
        self
    }

    pub fn max_delay(mut self, d: Duration) -> Self {
        self.max_delay = d;
        self
    }

    pub fn output_path(mut self, path: impl Into<String>) -> Self {
        self.output_path = Some(path.into());
        self
    }

    pub fn no_output_file(mut self) -> Self {
        self.output_path = None;
        self
    }

    pub fn rounds_per_experiment(mut self, n: u32) -> Self {
        self.rounds_per_experiment = n;
        self
    }

    pub fn selection_strategy(mut self, s: SelectionStrategy) -> Self {
        self.selection_strategy = s;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = ProfilerConfig::default();
        assert_eq!(config.experiment_duration, Duration::from_secs(2));
        assert_eq!(
            config.speedup_steps,
            (0u8..=10).map(|i| i * 10).collect::<Vec<u8>>()
        );
        assert_eq!(config.min_delay, Duration::from_micros(1));
        assert_eq!(config.max_delay, Duration::from_millis(100));
        assert_eq!(
            config.output_path,
            Some("tokio-coz-profile.json".to_string())
        );
        assert_eq!(config.rounds_per_experiment, 3);
        assert!(matches!(
            config.selection_strategy,
            SelectionStrategy::Random
        ));
    }

    #[test]
    fn builder_chaining() {
        let config = ProfilerConfig::new()
            .experiment_duration(Duration::from_secs(5))
            .speedup_steps(vec![0, 50, 100])
            .min_delay(Duration::from_micros(10))
            .max_delay(Duration::from_millis(50))
            .output_path("my-output.json")
            .rounds_per_experiment(5)
            .selection_strategy(SelectionStrategy::RoundRobin);

        assert_eq!(config.experiment_duration, Duration::from_secs(5));
        assert_eq!(config.speedup_steps, vec![0, 50, 100]);
        assert_eq!(config.min_delay, Duration::from_micros(10));
        assert_eq!(config.max_delay, Duration::from_millis(50));
        assert_eq!(config.output_path, Some("my-output.json".to_string()));
        assert_eq!(config.rounds_per_experiment, 5);
        assert!(matches!(
            config.selection_strategy,
            SelectionStrategy::RoundRobin
        ));
    }
}
