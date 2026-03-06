use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::delay::DelayController;
use crate::state::{CURRENT_TASK_SPANS, POLL_START_NS, SharedState, SpanKey, THREAD_GENERATION};

/// Encapsulates the four runtime hook closures.
///
/// Each hook captures shared state and operates on the same OS thread as the task poll,
/// using thread-locals as the synchronization bridge with the tracing layer.
pub struct Hooks {
    state: Arc<SharedState>,
    delay: Arc<DelayController>,
    /// Cached epoch for converting `Instant` to nanosecond timestamps.
    epoch: Instant,
}

impl Hooks {
    pub fn new(state: Arc<SharedState>, delay: Arc<DelayController>) -> Self {
        Self {
            state,
            delay,
            epoch: Instant::now(),
        }
    }

    fn now_ns(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }

    /// Hook: called when a task is spawned. Currently a no-op placeholder.
    pub fn on_task_spawn(&self) {
        // Task registration could go here if we need spawn-site tracking.
    }

    /// Hook: called just before a task is polled.
    ///
    /// Resets the thread-local span stack (handles task migration) and records poll start time.
    pub fn on_before_task_poll(&self) {
        // Clear span stack — the tracing layer will rebuild it as spans are entered.
        CURRENT_TASK_SPANS.with(|stack| stack.borrow_mut().clear());

        // Record poll start timestamp.
        POLL_START_NS.with(|cell| cell.set(self.now_ns()));

        // Check for generation change (experiment transition).
        let current_gen = self.state.generation.load(Ordering::Acquire);
        THREAD_GENERATION.with(|cell| {
            if cell.get() != current_gen {
                cell.set(current_gen);
                // Generation changed — any stale per-thread state is now invalid.
            }
        });
    }

    /// Hook: called just after a task poll completes.
    ///
    /// This is the core delay-injection logic:
    /// - If the task was inside the target span, add delay credit proportional to poll duration.
    /// - If the task was NOT inside the target span, consume available delay by sleeping.
    pub fn on_after_task_poll(&self) {
        // Fast path: no experiment running.
        if !self.state.experiment_active.load(Ordering::Acquire) {
            return;
        }

        let poll_start = POLL_START_NS.with(|cell| cell.get());
        if poll_start == 0 {
            return;
        }
        let poll_end = self.now_ns();
        let poll_duration_ns = poll_end.saturating_sub(poll_start);

        let target = SpanKey(self.state.target_span.load(Ordering::Relaxed));
        let speedup = self.state.speedup_pct.load(Ordering::Relaxed);

        // Check if this task is inside the target span (anywhere in the stack).
        let is_target = CURRENT_TASK_SPANS.with(|stack| {
            let stack = stack.borrow();
            stack.contains(&target)
        });

        if is_target {
            // Target task: add delay credit.
            self.delay.add_credit(poll_duration_ns, speedup);
        } else {
            // Non-target task: consume delay by sleeping.
            self.delay.consume_delay();
        }
    }

    /// Hook: called when a task terminates. Currently a no-op.
    pub fn on_task_terminate(&self) {
        // Cleanup could go here if we tracked per-task state.
    }
}

/// Create the four hook closures for registering with the tokio runtime builder.
///
/// Returns `(on_task_spawn, on_before_task_poll, on_after_task_poll, on_task_terminate)`.
///
/// Requires `RUSTFLAGS="--cfg tokio_unstable"` to compile.
#[cfg(tokio_unstable)]
#[allow(clippy::type_complexity)]
pub fn make_hook_closures(
    state: Arc<SharedState>,
    delay: Arc<DelayController>,
) -> (
    impl Fn(&tokio::runtime::TaskMeta<'_>) + Send + Sync + 'static,
    impl Fn(&tokio::runtime::TaskMeta<'_>) + Send + Sync + 'static,
    impl Fn(&tokio::runtime::TaskMeta<'_>) + Send + Sync + 'static,
    impl Fn(&tokio::runtime::TaskMeta<'_>) + Send + Sync + 'static,
) {
    let hooks = Arc::new(Hooks::new(state, delay));

    let h1 = hooks.clone();
    let on_spawn = move |_meta: &tokio::runtime::TaskMeta<'_>| {
        h1.on_task_spawn();
    };

    let h2 = hooks.clone();
    let on_before_poll = move |_meta: &tokio::runtime::TaskMeta<'_>| {
        h2.on_before_task_poll();
    };

    let h3 = hooks.clone();
    let on_after_poll = move |_meta: &tokio::runtime::TaskMeta<'_>| {
        h3.on_after_task_poll();
    };

    let h4 = hooks;
    let on_terminate = move |_meta: &tokio::runtime::TaskMeta<'_>| {
        h4.on_task_terminate();
    };

    (on_spawn, on_before_poll, on_after_poll, on_terminate)
}
