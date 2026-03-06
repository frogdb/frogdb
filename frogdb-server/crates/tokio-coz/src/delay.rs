use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::config::ProfilerConfig;
use crate::state::SharedState;

/// Manages delay injection for non-target tasks.
///
/// When a target task completes a poll, it adds delay credit to the global counter.
/// Non-target tasks consume that credit by sleeping, simulating the target running faster.
pub struct DelayController {
    state: Arc<SharedState>,
    min_delay: Duration,
    max_delay: Duration,
}

impl DelayController {
    pub fn new(state: Arc<SharedState>, config: &ProfilerConfig) -> Self {
        Self {
            state,
            min_delay: config.min_delay,
            max_delay: config.max_delay,
        }
    }

    /// Called when a target task finishes a poll of `poll_duration_ns` nanoseconds.
    /// Adds proportional delay credit to the global counter.
    pub fn add_credit(&self, poll_duration_ns: u64, speedup_pct: u64) {
        if speedup_pct == 0 || poll_duration_ns == 0 {
            return;
        }
        let credit = poll_duration_ns * speedup_pct / 100;
        self.state
            .global_delay_ns
            .fetch_add(credit, Ordering::Relaxed);
    }

    /// Called when a non-target task finishes a poll.
    /// Consumes available delay credit by sleeping.
    pub fn consume_delay(&self) {
        // Try to grab all available delay credit atomically.
        let delay_ns = self.state.global_delay_ns.swap(0, Ordering::Relaxed);
        if delay_ns == 0 {
            return;
        }

        let delay = Duration::from_nanos(delay_ns);

        // Skip if below threshold.
        if delay < self.min_delay {
            // Put the credit back — it'll accumulate.
            self.state
                .global_delay_ns
                .fetch_add(delay_ns, Ordering::Relaxed);
            return;
        }

        // Cap the delay.
        let delay = delay.min(self.max_delay);

        // Block the OS thread — this is the core coz mechanism.
        std::thread::sleep(delay);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    fn make_controller(
        min_delay: Duration,
        max_delay: Duration,
    ) -> (Arc<SharedState>, DelayController) {
        let state = Arc::new(SharedState::new());
        let config = ProfilerConfig::default()
            .min_delay(min_delay)
            .max_delay(max_delay);
        let controller = DelayController::new(Arc::clone(&state), &config);
        (state, controller)
    }

    #[test]
    fn add_credit_zero_speedup_noop() {
        let (state, controller) =
            make_controller(Duration::from_micros(1), Duration::from_millis(100));
        controller.add_credit(1000, 0);
        assert_eq!(state.global_delay_ns.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn add_credit_zero_duration_noop() {
        let (state, controller) =
            make_controller(Duration::from_micros(1), Duration::from_millis(100));
        controller.add_credit(0, 50);
        assert_eq!(state.global_delay_ns.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn add_credit_computes_correctly() {
        let (state, controller) =
            make_controller(Duration::from_micros(1), Duration::from_millis(100));
        controller.add_credit(1_000_000, 50);
        assert_eq!(state.global_delay_ns.load(Ordering::Relaxed), 500_000);
    }

    #[test]
    fn add_credit_accumulates() {
        let (state, controller) =
            make_controller(Duration::from_micros(1), Duration::from_millis(100));
        controller.add_credit(1_000_000, 50); // adds 500_000
        controller.add_credit(2_000_000, 50); // adds 1_000_000
        assert_eq!(state.global_delay_ns.load(Ordering::Relaxed), 1_500_000);
    }

    #[test]
    fn consume_delay_below_threshold_returns_credit() {
        let (state, controller) = make_controller(
            Duration::from_millis(10), // min_delay = 10ms
            Duration::from_millis(100),
        );
        // Add 1µs credit (below 10ms threshold)
        state.global_delay_ns.store(1_000, Ordering::Relaxed);
        controller.consume_delay();
        // Credit should be put back
        assert_eq!(state.global_delay_ns.load(Ordering::Relaxed), 1_000);
    }

    #[test]
    fn consume_delay_above_threshold_consumes() {
        let (state, controller) = make_controller(
            Duration::from_nanos(1), // min_delay = 1ns
            Duration::from_millis(100),
        );
        // Add 1µs credit (above 1ns threshold)
        state.global_delay_ns.store(1_000, Ordering::Relaxed);
        controller.consume_delay();
        // Credit should be consumed (sleep happened)
        assert_eq!(state.global_delay_ns.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn consume_delay_caps_at_max() {
        let (state, controller) = make_controller(
            Duration::from_nanos(1),
            Duration::from_millis(1), // max_delay = 1ms
        );
        // Add 10 seconds of credit
        state
            .global_delay_ns
            .store(10_000_000_000, Ordering::Relaxed);
        let start = Instant::now();
        controller.consume_delay();
        let elapsed = start.elapsed();
        // Should sleep roughly 1ms, not 10s
        assert!(
            elapsed < Duration::from_millis(100),
            "Sleep took too long: {:?}",
            elapsed
        );
        assert_eq!(state.global_delay_ns.load(Ordering::Relaxed), 0);
    }
}
