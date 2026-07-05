//! Generic name-keyed interning registry shared by the metrics backends.
//!
//! Both the OTLP and Prometheus recorders lazily create one typed metric
//! handle per unique metric name the first time it is emitted, then hand out
//! clones of that same handle on every later emission (metric handles are
//! cheap, `Arc`-backed clones). Each backend used to carry three copies of
//! this interning structure (one per metric kind); `InternedRegistry<T>`
//! implements it once and is reused for counters, gauges, and histograms in
//! both backends.

use std::collections::HashMap;
use std::sync::RwLock;

/// A name -> `T` cache that creates each entry at most once.
///
/// Lookups take a shared read lock first, so the common case (the entry is
/// already interned) never blocks on other readers. On a miss, the write
/// lock is acquired and the map is checked *again* before calling `build` —
/// this double-checked-lock pattern means that if several threads race to
/// create the same name for the first time, only one of them ever runs
/// `build`, and every caller (racing or not) gets back a clone of the same
/// stored instance.
pub struct InternedRegistry<T> {
    entries: RwLock<HashMap<String, T>>,
}

impl<T> Default for InternedRegistry<T> {
    fn default() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }
}

impl<T: Clone> InternedRegistry<T> {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the entry interned under `name`, creating it via `build` if this
    /// is the first call for that name.
    pub fn get_or_create(&self, name: &str, build: impl FnOnce() -> T) -> T {
        {
            let entries = self.entries.read().unwrap();
            if let Some(value) = entries.get(name) {
                return value.clone();
            }
        }

        let mut entries = self.entries.write().unwrap();
        if let Some(value) = entries.get(name) {
            return value.clone();
        }
        let value = build();
        entries.insert(name.to_string(), value.clone());
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn get_or_create_interns_same_name() {
        let registry: InternedRegistry<Arc<str>> = InternedRegistry::new();
        let a = registry.get_or_create("x", || Arc::from("built"));
        let b = registry.get_or_create("x", || panic!("must not rebuild an interned name"));
        assert!(
            Arc::ptr_eq(&a, &b),
            "same name must yield the same instance"
        );
    }

    #[test]
    fn get_or_create_distinct_names_are_distinct() {
        let registry: InternedRegistry<Arc<str>> = InternedRegistry::new();
        let a = registry.get_or_create("x", || Arc::from("x"));
        let b = registry.get_or_create("y", || Arc::from("y"));
        assert!(
            !Arc::ptr_eq(&a, &b),
            "distinct names must yield distinct instances"
        );
    }

    #[test]
    fn get_or_create_is_safe_under_concurrent_access() {
        let registry = Arc::new(InternedRegistry::<Arc<str>>::new());
        let build_calls = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..16)
            .map(|_| {
                let registry = Arc::clone(&registry);
                let build_calls = Arc::clone(&build_calls);
                thread::spawn(move || {
                    registry.get_or_create("shared", || {
                        build_calls.fetch_add(1, Ordering::SeqCst);
                        Arc::from("shared-value")
                    })
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Every racing caller must observe the same interned instance.
        let first = &results[0];
        assert!(
            results.iter().all(|r| Arc::ptr_eq(r, first)),
            "all callers racing on the same name must get the same instance"
        );
        // The double-checked lock guarantees `build` runs exactly once, even
        // when many threads race past the initial read-lock miss together.
        assert_eq!(
            build_calls.load(Ordering::SeqCst),
            1,
            "build must run exactly once across all racing callers"
        );
    }
}
