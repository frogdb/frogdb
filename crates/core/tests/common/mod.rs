//! Shared test utilities for frogdb-core integration tests.

use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

use shuttle::thread;

/// Spawn `n` shuttle threads, each running `f`, and collect the results.
///
/// This replaces the repeated spawn-join-collect pattern found across
/// concurrency tests.
pub fn spawn_collect<T, F>(n: usize, f: F) -> Vec<T>
where
    T: Send + 'static,
    F: Fn(usize) -> T + Send + Sync + 'static,
{
    let f = std::sync::Arc::new(f);
    let handles: Vec<_> = (0..n)
        .map(|i| {
            let f = f.clone();
            thread::spawn(move || f(i))
        })
        .collect();

    handles.into_iter().map(|h| h.join().unwrap()).collect()
}

/// Assert that all items in the slice are unique.
pub fn assert_all_unique<T: Hash + Eq + Debug>(items: &[T]) {
    let set: HashSet<_> = items.iter().collect();
    assert_eq!(
        items.len(),
        set.len(),
        "expected all items to be unique, got duplicates in {items:?}",
    );
}
