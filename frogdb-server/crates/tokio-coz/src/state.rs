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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::thread;

    #[test]
    fn activate_deactivate_experiment() {
        let state = SharedState::new();
        let key = state.intern_span("my_span");

        state.activate_experiment(key, 50);
        assert!(state.experiment_active.load(Ordering::Relaxed));
        assert_eq!(state.target_span.load(Ordering::Relaxed), key.0);
        assert_eq!(state.speedup_pct.load(Ordering::Relaxed), 50);
        let gen_after_activate = state.generation.load(Ordering::Relaxed);
        assert!(gen_after_activate > 0);

        state.deactivate_experiment();
        assert!(!state.experiment_active.load(Ordering::Relaxed));
        assert_eq!(state.global_delay_ns.load(Ordering::Relaxed), 0);
        let gen_after_deactivate = state.generation.load(Ordering::Relaxed);
        assert!(gen_after_deactivate > gen_after_activate);
    }

    #[test]
    fn deactivate_clears_delay_credit() {
        let state = SharedState::new();
        state.global_delay_ns.store(12345, Ordering::Relaxed);
        state.deactivate_experiment();
        assert_eq!(state.global_delay_ns.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn generation_increments_on_transitions() {
        let state = SharedState::new();
        let key = state.intern_span("span");

        let g0 = state.generation.load(Ordering::Relaxed);
        state.activate_experiment(key, 10);
        let g1 = state.generation.load(Ordering::Relaxed);
        assert!(g1 > g0);

        state.deactivate_experiment();
        let g2 = state.generation.load(Ordering::Relaxed);
        assert!(g2 > g1);

        state.activate_experiment(key, 20);
        let g3 = state.generation.load(Ordering::Relaxed);
        assert!(g3 > g2);
    }

    #[test]
    fn intern_span_concurrent_same_name() {
        let state = Arc::new(SharedState::new());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let s = Arc::clone(&state);
                thread::spawn(move || s.intern_span("shared_span"))
            })
            .collect();
        let keys: Vec<SpanKey> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(keys.windows(2).all(|w| w[0] == w[1]));
    }

    #[test]
    fn all_span_keys_returns_all_interned() {
        let state = SharedState::new();
        let k1 = state.intern_span("span_a");
        let k2 = state.intern_span("span_b");
        let k3 = state.intern_span("span_c");

        let keys = state.all_span_keys();
        assert!(keys.contains(&k1));
        assert!(keys.contains(&k2));
        assert!(keys.contains(&k3));
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn span_name_reverse_lookup() {
        let state = SharedState::new();
        let key = state.intern_span("my_span");
        assert_eq!(state.span_name(key), Some("my_span".to_string()));
        assert_eq!(state.span_name(SpanKey::NONE), None);
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
