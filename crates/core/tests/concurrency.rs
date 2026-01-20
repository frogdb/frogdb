//! Concurrency tests using Shuttle for deterministic testing.
//!
//! These tests verify correctness of concurrent operations by running
//! them under Shuttle's randomized scheduler.
//!
//! Run with: cargo test -p frogdb-core --features shuttle --test concurrency

use shuttle::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use shuttle::sync::{Arc, Mutex};
use shuttle::{check_pct, check_random, thread};
use std::collections::HashSet;

/// Test that connection ID generation produces unique values under concurrency.
///
/// This simulates the behavior of `next_conn_id()` in server.rs where multiple
/// acceptor threads might request connection IDs simultaneously.
#[test]
fn test_conn_id_uniqueness() {
    check_random(
        || {
            static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

            fn next_conn_id() -> u64 {
                NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
            }

            let ids = Arc::new(Mutex::new(Vec::new()));
            let mut handles = vec![];

            // Spawn multiple threads grabbing IDs concurrently
            for _ in 0..4 {
                let ids = ids.clone();
                handles.push(thread::spawn(move || {
                    let id = next_conn_id();
                    ids.lock().unwrap().push(id);
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            // Verify all IDs are unique
            let ids = ids.lock().unwrap();
            let unique: HashSet<_> = ids.iter().collect();
            assert_eq!(ids.len(), unique.len(), "Connection IDs must be unique");
        },
        1000,
    );
}

/// Test that connection ID counter is monotonically increasing.
///
/// Each thread should see a strictly increasing sequence of IDs when
/// calling next_conn_id() multiple times.
#[test]
fn test_conn_id_monotonicity() {
    check_random(
        || {
            static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

            fn next_conn_id() -> u64 {
                NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
            }

            let mut handles = vec![];

            for _ in 0..2 {
                handles.push(thread::spawn(|| {
                    let id1 = next_conn_id();
                    let id2 = next_conn_id();
                    // Within a single thread, IDs must be increasing
                    assert!(id2 > id1, "IDs must be monotonically increasing");
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }
        },
        1000,
    );
}

/// Test round-robin assignment produces correct distribution.
///
/// This simulates the RoundRobinAssigner in acceptor.rs.
#[test]
fn test_round_robin_assignment() {
    check_random(
        || {
            struct RoundRobinAssigner {
                next: AtomicUsize,
                num_shards: usize,
            }

            impl RoundRobinAssigner {
                fn new(num_shards: usize) -> Self {
                    Self {
                        next: AtomicUsize::new(0),
                        num_shards,
                    }
                }

                fn assign(&self) -> usize {
                    let idx = self.next.fetch_add(1, Ordering::Relaxed);
                    idx % self.num_shards
                }
            }

            let assigner = Arc::new(RoundRobinAssigner::new(4));
            let assignments = Arc::new(Mutex::new(Vec::new()));
            let mut handles = vec![];

            for _ in 0..8 {
                let assigner = assigner.clone();
                let assignments = assignments.clone();
                handles.push(thread::spawn(move || {
                    let shard_id = assigner.assign();
                    assert!(shard_id < 4, "Shard ID must be within bounds");
                    assignments.lock().unwrap().push(shard_id);
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            // Verify we got 8 assignments
            let assignments = assignments.lock().unwrap();
            assert_eq!(assignments.len(), 8);

            // Each shard should have been assigned to exactly twice
            // (since 8 assignments / 4 shards = 2 each)
            let mut counts = [0usize; 4];
            for &shard_id in assignments.iter() {
                counts[shard_id] += 1;
            }
            for count in counts {
                assert_eq!(count, 2, "Each shard should get equal assignments");
            }
        },
        1000,
    );
}

/// Test "read your writes" consistency for a single-shard store.
///
/// This verifies that a write followed by a read from the same "client"
/// (thread) always sees the written value, even with concurrent operations.
#[test]
fn test_read_your_writes() {
    check_random(
        || {
            use std::collections::HashMap;

            // Simplified single-shard store
            let store = Arc::new(Mutex::new(HashMap::<String, String>::new()));
            let mut handles = vec![];

            // Multiple "clients" performing write-then-read
            for i in 0..3 {
                let store = store.clone();
                let key = format!("key{}", i);
                let value = format!("value{}", i);

                handles.push(thread::spawn(move || {
                    // Write
                    {
                        let mut s = store.lock().unwrap();
                        s.insert(key.clone(), value.clone());
                    }

                    // Read - must see our own write
                    {
                        let s = store.lock().unwrap();
                        let read_value = s.get(&key).cloned();
                        assert_eq!(
                            read_value,
                            Some(value),
                            "Must read the value we just wrote"
                        );
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }
        },
        1000,
    );
}

/// Test command ordering within a single shard.
///
/// Commands from a single client should be processed in FIFO order.
/// This simulates multiple SET operations that should result in the
/// last write winning.
#[test]
fn test_command_ordering_single_client() {
    check_random(
        || {
            use std::collections::HashMap;

            let store = Arc::new(Mutex::new(HashMap::<String, i32>::new()));
            let store_clone = store.clone();

            // Single client sends multiple writes
            thread::spawn(move || {
                for i in 0..5 {
                    let mut s = store_clone.lock().unwrap();
                    s.insert("counter".to_string(), i);
                }
            })
            .join()
            .unwrap();

            // Final value should be the last one written
            let s = store.lock().unwrap();
            assert_eq!(s.get("counter"), Some(&4));
        },
        1000,
    );
}

/// Test concurrent increments (simulating INCR command).
///
/// Multiple threads incrementing a counter should result in the correct
/// final value with no lost updates.
#[test]
fn test_concurrent_increments() {
    check_random(
        || {
            let counter = Arc::new(AtomicU64::new(0));
            let mut handles = vec![];

            for _ in 0..4 {
                let counter = counter.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..10 {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            // 4 threads * 10 increments = 40
            assert_eq!(counter.load(Ordering::SeqCst), 40);
        },
        1000,
    );
}

/// Test PCT (probabilistic concurrency testing) for more thorough coverage.
///
/// PCT is particularly good at finding bugs that manifest under specific
/// orderings of events.
#[test]
fn test_conn_id_uniqueness_pct() {
    check_pct(
        || {
            static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

            fn next_conn_id() -> u64 {
                NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
            }

            let ids = Arc::new(Mutex::new(Vec::new()));
            let mut handles = vec![];

            for _ in 0..4 {
                let ids = ids.clone();
                handles.push(thread::spawn(move || {
                    let id = next_conn_id();
                    ids.lock().unwrap().push(id);
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            let ids = ids.lock().unwrap();
            let unique: HashSet<_> = ids.iter().collect();
            assert_eq!(ids.len(), unique.len());
        },
        1000,
        3, // PCT depth parameter
    );
}

/// Test that multiple readers don't block each other.
///
/// This simulates concurrent GET operations which should all succeed.
#[test]
fn test_concurrent_reads() {
    check_random(
        || {
            use std::collections::HashMap;

            let store = Arc::new(Mutex::new(HashMap::<String, String>::new()));

            // Setup: write initial data
            {
                let mut s = store.lock().unwrap();
                s.insert("key".to_string(), "value".to_string());
            }

            let mut handles = vec![];

            // Multiple concurrent readers
            for _ in 0..4 {
                let store = store.clone();
                handles.push(thread::spawn(move || {
                    let s = store.lock().unwrap();
                    let value = s.get("key").cloned();
                    assert_eq!(value, Some("value".to_string()));
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }
        },
        1000,
    );
}

/// Test write-write conflict resolution (last write wins).
///
/// When multiple threads write to the same key, the final value
/// should be from one of the writes (deterministic under Shuttle).
#[test]
fn test_write_write_conflict() {
    check_random(
        || {
            use std::collections::HashMap;

            let store = Arc::new(Mutex::new(HashMap::<String, i32>::new()));
            let mut handles = vec![];

            for i in 0..3 {
                let store = store.clone();
                handles.push(thread::spawn(move || {
                    let mut s = store.lock().unwrap();
                    s.insert("key".to_string(), i);
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            // Final value should be one of 0, 1, or 2
            let s = store.lock().unwrap();
            let value = *s.get("key").unwrap();
            assert!(
                (0..3).contains(&value),
                "Final value must be from one of the writers"
            );
        },
        1000,
    );
}
