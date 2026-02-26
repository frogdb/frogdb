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
