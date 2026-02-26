use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use dashmap::DashMap;

/// Interned span identifier for fast comparison on the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpanKey(pub u64);

impl SpanKey {
    pub const NONE: SpanKey = SpanKey(0);
}

/// Shared profiler state accessible from hooks, tracing layer, and experiment engine.
///
/// All hot-path fields use atomics for lock-free access during every task poll.
pub struct SharedState {
    // ── Experiment control ──────────────────────────────────────────
    /// Whether an experiment is currently active.
    pub experiment_active: AtomicBool,

    /// The span being virtually sped up in the current experiment.
    pub target_span: AtomicU64,

    /// Current speedup percentage (0–100).
    pub speedup_pct: AtomicU64,

    /// Generation counter — incremented on each experiment transition.
    /// Per-thread delay state resets when it sees a new generation.
    pub generation: AtomicU64,

    // ── Delay accounting ────────────────────────────────────────────
    /// Global delay credit (nanoseconds). Target tasks add credit here;
    /// non-target tasks consume it by sleeping.
    pub global_delay_ns: AtomicU64,

    // ── Span registry ───────────────────────────────────────────────
    /// Intern table: span name → SpanKey.
    pub span_names: DashMap<String, SpanKey>,

    /// Reverse lookup: SpanKey → span name (for reporting).
    pub span_keys: DashMap<SpanKey, String>,

    /// Next SpanKey ID to assign.
    pub next_span_id: AtomicU64,
}

impl Default for SharedState {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            experiment_active: AtomicBool::new(false),
            target_span: AtomicU64::new(0),
            speedup_pct: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            global_delay_ns: AtomicU64::new(0),
            span_names: DashMap::new(),
            span_keys: DashMap::new(),
            next_span_id: AtomicU64::new(1), // 0 is NONE
        }
    }

    /// Intern a span name, returning an existing or new `SpanKey`.
    pub fn intern_span(&self, name: &str) -> SpanKey {
        if let Some(key) = self.span_names.get(name) {
            return *key;
        }
        let id = self.next_span_id.fetch_add(1, Ordering::Relaxed);
        let key = SpanKey(id);
        // Race-safe: if another thread inserted first, use theirs.
        let key = *self.span_names.entry(name.to_string()).or_insert(key);
        self.span_keys
            .entry(key)
            .or_insert_with(|| name.to_string());
        key
    }

    /// Get all discovered span keys.
    pub fn all_span_keys(&self) -> Vec<SpanKey> {
        self.span_keys.iter().map(|entry| *entry.key()).collect()
    }

    /// Look up the name for a span key.
    pub fn span_name(&self, key: SpanKey) -> Option<String> {
        self.span_keys.get(&key).map(|entry| entry.value().clone())
    }

    /// Activate an experiment for a target span at a given speedup.
    pub fn activate_experiment(&self, target: SpanKey, speedup: u8) {
        self.global_delay_ns.store(0, Ordering::Relaxed);
        self.target_span.store(target.0, Ordering::Relaxed);
        self.speedup_pct.store(speedup as u64, Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::Release);
        self.experiment_active.store(true, Ordering::Release);
    }

    /// Deactivate the current experiment.
    pub fn deactivate_experiment(&self) {
        self.experiment_active.store(false, Ordering::Release);
        self.global_delay_ns.store(0, Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::Release);
    }
}

// ── Thread-local state ──────────────────────────────────────────────────────

thread_local! {
    /// The tracing span stack for the task currently being polled on this thread.
    /// Pushed by the tracing Layer on `on_enter`, popped on `on_exit`.
    /// Cleared by `on_before_task_poll` hook to handle task migration.
    pub static CURRENT_TASK_SPANS: std::cell::RefCell<Vec<SpanKey>> =
        const { std::cell::RefCell::new(Vec::new()) };

    /// Timestamp (nanos since epoch) when the current poll started.
    pub static POLL_START_NS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };

    /// Per-thread generation — used to detect experiment transitions.
    pub static THREAD_GENERATION: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
