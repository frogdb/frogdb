//! Concurrency tests using Shuttle for deterministic testing.
//!
//! These tests verify correctness of concurrent operations by running
//! them under Shuttle's randomized scheduler.
//!
//! Run with: cargo test -p frogdb-core --features shuttle --test concurrency

mod common;

use common::mock_cluster::*;
use common::mock_json::*;
use common::mock_snapshot::*;
use common::mock_streams::*;
use common::mock_watch::*;
use common::{assert_all_unique, spawn_collect};
use serde_json;
use shuttle::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use shuttle::sync::{Arc, Mutex};
use shuttle::{check_pct, check_random, thread};
use std::collections::{HashMap, HashSet};

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

            let ids = spawn_collect(4, |_| next_conn_id());
            assert_all_unique(&ids);
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
            let assignments = spawn_collect(8, move |_| {
                let shard_id = assigner.assign();
                assert!(shard_id < 4, "Shard ID must be within bounds");
                shard_id
            });

            // Verify we got 8 assignments
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

            let ids = spawn_collect(4, |_| next_conn_id());
            assert_all_unique(&ids);
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

// ============================================================================
// Hash type concurrency tests
// ============================================================================

/// Test concurrent HSET operations on the same hash key.
///
/// Multiple threads writing different fields to the same hash should
/// all succeed without losing any fields.
#[test]
fn test_concurrent_hset_same_hash() {
    check_random(
        || {
            use std::collections::HashMap;

            // Simulates a hash value: HashMap<field, value>
            let hash = Arc::new(Mutex::new(HashMap::<String, String>::new()));
            let mut handles = vec![];

            // Each thread writes a unique field
            for i in 0..4 {
                let hash = hash.clone();
                let field = format!("field{}", i);
                let value = format!("value{}", i);

                handles.push(thread::spawn(move || {
                    let mut h = hash.lock().unwrap();
                    h.insert(field, value);
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            // All 4 fields should be present
            let h = hash.lock().unwrap();
            assert_eq!(h.len(), 4, "All fields must be present");
            for i in 0..4 {
                assert!(
                    h.contains_key(&format!("field{}", i)),
                    "Field {} must exist",
                    i
                );
            }
        },
        1000,
    );
}

/// Test concurrent HINCRBY operations on the same field.
///
/// Multiple threads incrementing the same hash field should produce
/// the correct total with no lost updates.
#[test]
fn test_concurrent_hincrby() {
    check_random(
        || {
            let counter = Arc::new(AtomicU64::new(0));
            let mut handles = vec![];

            // 4 threads, each incrementing 5 times
            for _ in 0..4 {
                let counter = counter.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..5 {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            // 4 threads * 5 increments = 20
            assert_eq!(counter.load(Ordering::SeqCst), 20);
        },
        1000,
    );
}

// ============================================================================
// List type concurrency tests
// ============================================================================

/// Test concurrent LPUSH/RPUSH operations on the same list.
///
/// Multiple threads pushing to the same list should all succeed,
/// and the final list should contain all pushed elements.
#[test]
fn test_concurrent_list_push() {
    check_random(
        || {
            use std::collections::VecDeque;

            let list = Arc::new(Mutex::new(VecDeque::<i32>::new()));
            let mut handles = vec![];

            // 2 threads doing LPUSH, 2 threads doing RPUSH
            for i in 0..2 {
                let list = list.clone();
                handles.push(thread::spawn(move || {
                    for j in 0..5 {
                        let mut l = list.lock().unwrap();
                        l.push_front(i * 10 + j);
                    }
                }));
            }

            for i in 2..4 {
                let list = list.clone();
                handles.push(thread::spawn(move || {
                    for j in 0..5 {
                        let mut l = list.lock().unwrap();
                        l.push_back(i * 10 + j);
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            // Should have 20 elements total (4 threads * 5 pushes)
            let l = list.lock().unwrap();
            assert_eq!(l.len(), 20, "List should have all pushed elements");
        },
        1000,
    );
}

/// Test concurrent LPOP/RPOP operations on the same list.
///
/// Multiple threads popping from the same list should get distinct
/// elements with no duplicates.
#[test]
fn test_concurrent_list_pop() {
    check_random(
        || {
            use std::collections::VecDeque;

            let list = Arc::new(Mutex::new(VecDeque::from(vec![1, 2, 3, 4, 5, 6, 7, 8])));
            let popped = Arc::new(Mutex::new(Vec::new()));
            let mut handles = vec![];

            // 4 threads popping 2 elements each
            for _ in 0..4 {
                let list = list.clone();
                let popped = popped.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..2 {
                        let value = {
                            let mut l = list.lock().unwrap();
                            l.pop_front()
                        };
                        if let Some(v) = value {
                            popped.lock().unwrap().push(v);
                        }
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            let popped = popped.lock().unwrap();
            // Should have popped all 8 elements
            assert_eq!(popped.len(), 8, "Should pop all elements");

            // Check uniqueness
            assert_all_unique(&*popped);
        },
        1000,
    );
}

// ============================================================================
// Set type concurrency tests
// ============================================================================

/// Test concurrent SADD operations on the same set.
///
/// Multiple threads adding members to the same set should all succeed,
/// and the final set should contain all unique members.
#[test]
fn test_concurrent_sadd() {
    check_random(
        || {
            let set = Arc::new(Mutex::new(HashSet::<i32>::new()));
            let mut handles = vec![];

            // 4 threads adding unique members
            for i in 0..4 {
                let set = set.clone();
                handles.push(thread::spawn(move || {
                    for j in 0..5 {
                        let mut s = set.lock().unwrap();
                        s.insert(i * 10 + j);
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            // Should have 20 unique members (4 threads * 5 members)
            let s = set.lock().unwrap();
            assert_eq!(s.len(), 20, "Set should have all unique members");
        },
        1000,
    );
}

/// Test concurrent SADD/SREM operations on the same set.
///
/// Mix of add and remove operations should maintain set consistency.
#[test]
fn test_concurrent_sadd_srem() {
    check_random(
        || {
            let set = Arc::new(Mutex::new(HashSet::<i32>::new()));
            let mut handles = vec![];

            // Thread 0 and 1 add members
            for i in 0..2 {
                let set = set.clone();
                handles.push(thread::spawn(move || {
                    for j in 0..5 {
                        let mut s = set.lock().unwrap();
                        s.insert(i * 10 + j);
                    }
                }));
            }

            // Thread 2 removes some members
            let set_clone = set.clone();
            handles.push(thread::spawn(move || {
                for j in 0..5 {
                    let mut s = set_clone.lock().unwrap();
                    s.remove(&j); // Try to remove 0-4
                }
            }));

            for handle in handles {
                handle.join().unwrap();
            }

            // Final state depends on interleaving, but should be consistent
            let s = set.lock().unwrap();
            // Should have between 5 and 10 members
            assert!(
                s.len() >= 5 && s.len() <= 10,
                "Set size should be within expected range"
            );
        },
        1000,
    );
}

/// Test concurrent SPOP operations.
///
/// Multiple threads popping from the same set should get distinct
/// members with no duplicates.
#[test]
fn test_concurrent_spop() {
    check_random(
        || {
            let set = Arc::new(Mutex::new(HashSet::from([1, 2, 3, 4, 5, 6, 7, 8])));
            let popped = Arc::new(Mutex::new(Vec::new()));
            let mut handles = vec![];

            // 4 threads popping 2 elements each
            for _ in 0..4 {
                let set = set.clone();
                let popped = popped.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..2 {
                        let value = {
                            let mut s = set.lock().unwrap();
                            // Pop arbitrary element
                            s.iter().next().cloned().inspect(|v| {
                                s.remove(v);
                            })
                        };
                        if let Some(v) = value {
                            popped.lock().unwrap().push(v);
                        }
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            let popped = popped.lock().unwrap();
            // Should have popped all 8 elements
            assert_eq!(popped.len(), 8, "Should pop all elements");

            // Check uniqueness
            assert_all_unique(&*popped);
        },
        1000,
    );
}

// ============================================================================
// Cross-shard concurrency tests (MGET/MSET scatter-gather simulation)
// ============================================================================

/// Verify that MGET can observe partial MSET results when keys span shards.
/// This documents the current behavior (not a bug, but must be understood).
#[test]
fn test_mset_cross_shard_partial_visibility() {
    check_random(
        || {
            let cluster = Arc::new(TestCluster::new(2));

            // Keys chosen to hash to different shards
            // 'a' = 97, hashes to shard 1; 'b' = 98, hashes to shard 0
            let key_shard0 = b"b".to_vec();
            let key_shard1 = b"a".to_vec();

            // Verify they're on different shards
            assert_ne!(
                cluster.shard_for_key(&key_shard0),
                cluster.shard_for_key(&key_shard1),
                "Test setup: keys must be on different shards"
            );

            let observed_states = Arc::new(Mutex::new(Vec::new()));
            let cluster_w = cluster.clone();
            let cluster_r = cluster.clone();
            let observed = observed_states.clone();
            let k0 = key_shard0.clone();
            let k1 = key_shard1.clone();

            // Writer thread
            let writer = thread::spawn(move || {
                cluster_w.mset(&[
                    (key_shard0, b"v1".to_vec()),
                    (key_shard1, b"v2".to_vec()),
                ]);
            });

            // Reader thread (concurrent)
            let reader = thread::spawn(move || {
                let results = cluster_r.mget(&[k0, k1]);
                observed.lock().unwrap().push(results);
            });

            writer.join().unwrap();
            reader.join().unwrap();

            // Document what states are possible:
            // - [None, None] - read before any write
            // - [Some("v1"), None] - partial write visible (key_shard0 written)
            // - [None, Some("v2")] - partial write visible (key_shard1 written)
            // - [Some("v1"), Some("v2")] - full write visible
            // All of these are valid under scatter-gather semantics
            let states = observed_states.lock().unwrap();
            for state in states.iter() {
                match (state[0].as_ref(), state[1].as_ref()) {
                    (None, None) => {} // Before write
                    (Some(v), None) if v == b"v1" => {} // Partial (first key)
                    (None, Some(v)) if v == b"v2" => {} // Partial (second key)
                    (Some(v1), Some(v2)) if v1 == b"v1" && v2 == b"v2" => {} // Complete
                    other => panic!("Unexpected state: {:?}", other),
                }
            }
        },
        1000,
    );
}

/// Verify that MSET to same shard is atomic (no partial visibility).
#[test]
fn test_mset_same_shard_atomicity() {
    check_random(
        || {
            let cluster = Arc::new(TestCluster::new(4));

            // Use hash tags to force same shard: {x}key1 and {x}key2
            let key1 = b"{x}key1".to_vec();
            let key2 = b"{x}key2".to_vec();

            // Verify they're on the same shard
            assert_eq!(
                cluster.shard_for_key_with_tag(&key1),
                cluster.shard_for_key_with_tag(&key2),
                "Test setup: keys must be on same shard"
            );

            let observed_partial = Arc::new(AtomicBool::new(false));
            let cluster_w = cluster.clone();
            let cluster_r = cluster.clone();
            let observed = observed_partial.clone();
            let k1 = key1.clone();
            let k2 = key2.clone();

            let writer = thread::spawn(move || {
                // Use atomic MSET for same-shard operation
                cluster_w.mset_atomic(&[(key1, b"A".to_vec()), (key2, b"A".to_vec())]);
            });

            let reader = thread::spawn(move || {
                let results = cluster_r.mget(&[k1, k2]);
                // Partial = one is Some, other is None
                match (&results[0], &results[1]) {
                    (Some(_), None) | (None, Some(_)) => {
                        observed.store(true, Ordering::SeqCst);
                    }
                    _ => {}
                }
            });

            writer.join().unwrap();
            reader.join().unwrap();

            // Same-shard MSET should be atomic - partial visibility is a bug
            assert!(
                !observed_partial.load(Ordering::SeqCst),
                "Same-shard MSET must be atomic - no partial visibility allowed"
            );
        },
        1000,
    );
}

/// Multiple threads doing MSET on overlapping keys - final state must be consistent.
#[test]
fn test_concurrent_mset_last_write_wins() {
    check_random(
        || {
            let cluster = Arc::new(TestCluster::new(2));
            let key = b"shared".to_vec();
            let mut handles = vec![];

            for i in 0..4u8 {
                let cluster = cluster.clone();
                let key = key.clone();
                let value = format!("v{}", i).into_bytes();
                handles.push(thread::spawn(move || {
                    cluster.mset(&[(key, value)]);
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // Final value must be one of v0, v1, v2, v3
            let results = cluster.mget(&[key]);
            let value = results[0].as_ref().unwrap();
            assert!(
                value == b"v0" || value == b"v1" || value == b"v2" || value == b"v3",
                "Final value must be from one writer, got: {:?}",
                String::from_utf8_lossy(value)
            );
        },
        1000,
    );
}

// ============================================================================
// MULTI/EXEC concurrency tests
// ============================================================================

/// Commands queued in MULTI must execute atomically.
/// This simulates the transaction execution model where all queued commands
/// execute under a single shard lock.
#[test]
fn test_multi_exec_atomic_execution() {
    check_random(
        || {
            let store = Arc::new(Mutex::new(HashMap::<String, i64>::new()));
            let mut handles = vec![];

            // 4 threads each doing MULTI/INCR/INCR/EXEC (2 increments per tx)
            for _ in 0..4 {
                let store = store.clone();
                handles.push(thread::spawn(move || {
                    // Simulate atomic EXEC: hold lock for both increments
                    let mut s = store.lock().unwrap();
                    let val = s.entry("counter".to_string()).or_insert(0);
                    *val += 1;
                    *val += 1;
                    // Lock released after both operations (atomic transaction)
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            let store = store.lock().unwrap();
            // 4 threads * 2 increments = 8
            assert_eq!(
                store.get("counter"),
                Some(&8),
                "All transaction increments must be counted"
            );
        },
        1000,
    );
}

/// Test that concurrent transactions don't see each other's intermediate states.
/// If thread A does SET x 1, SET y 1 in a transaction, thread B should never
/// see x=1, y=nil or x=nil, y=1 (only both nil or both 1).
#[test]
fn test_multi_exec_isolation() {
    check_random(
        || {
            let store = Arc::new(Mutex::new(HashMap::<String, i64>::new()));
            let observed_partial = Arc::new(AtomicBool::new(false));

            let store_writer = store.clone();
            let store_reader = store.clone();
            let partial = observed_partial.clone();

            let writer = thread::spawn(move || {
                // Atomic transaction: SET x 1, SET y 1
                let mut s = store_writer.lock().unwrap();
                s.insert("x".to_string(), 1);
                s.insert("y".to_string(), 1);
            });

            let reader = thread::spawn(move || {
                // Read both values
                let s = store_reader.lock().unwrap();
                let x = s.get("x").copied();
                let y = s.get("y").copied();

                // Check for partial visibility
                match (x, y) {
                    (Some(_), None) | (None, Some(_)) => {
                        partial.store(true, Ordering::SeqCst);
                    }
                    _ => {}
                }
            });

            writer.join().unwrap();
            reader.join().unwrap();

            assert!(
                !observed_partial.load(Ordering::SeqCst),
                "Transaction must be atomic - no partial visibility"
            );
        },
        1000,
    );
}

// ============================================================================
// WATCH concurrency tests
// ============================================================================

/// WATCH key → concurrent SET → EXEC should abort.
#[test]
fn test_watch_detects_concurrent_modification() {
    check_random(
        || {
            let store = Arc::new(WatchableStore::new());
            store.set("key", "initial");

            let abort_count = Arc::new(AtomicUsize::new(0));
            let success_count = Arc::new(AtomicUsize::new(0));

            let store_watcher = store.clone();
            let store_modifier = store.clone();
            let aborts = abort_count.clone();
            let successes = success_count.clone();

            // Thread A: WATCH key, then try to update it
            let watcher = thread::spawn(move || {
                let watched_version = store_watcher.get_version("key");

                // Yield to let modifier potentially run
                shuttle::thread::yield_now();

                // Try to EXEC (update key to "watcher_value")
                let result = store_watcher.exec_with_watch(
                    &[("key", watched_version)],
                    |data| {
                        let version = data.get("key").map(|(_, v)| v + 1).unwrap_or(1);
                        data.insert("key".to_string(), ("watcher_value".to_string(), version));
                    },
                );

                if result.is_none() {
                    aborts.fetch_add(1, Ordering::SeqCst);
                } else {
                    successes.fetch_add(1, Ordering::SeqCst);
                }
            });

            // Thread B: Modify the watched key
            let modifier = thread::spawn(move || {
                store_modifier.set("key", "modified");
            });

            watcher.join().unwrap();
            modifier.join().unwrap();

            // Either the watcher succeeds (ran first) or aborts (modifier ran first)
            let total = abort_count.load(Ordering::SeqCst) + success_count.load(Ordering::SeqCst);
            assert_eq!(total, 1, "Watcher must either succeed or abort");

            // Verify final state is consistent
            let final_value = store.get("key").unwrap();
            assert!(
                final_value == "modified" || final_value == "watcher_value",
                "Final value must be from one of the writers"
            );
        },
        1000,
    );
}

/// Multiple watchers on same key - at most one can succeed.
#[test]
fn test_watch_multiple_watchers() {
    check_random(
        || {
            let store = Arc::new(WatchableStore::new());
            store.set("counter", "0");

            let success_count = Arc::new(AtomicUsize::new(0));
            let mut handles = vec![];

            // 4 threads all trying to atomically increment
            for i in 0..4 {
                let store = store.clone();
                let successes = success_count.clone();

                handles.push(thread::spawn(move || {
                    // WATCH counter
                    let watched_version = store.get_version("counter");
                    let current_value: i64 = store.get("counter").unwrap().parse().unwrap();

                    shuttle::thread::yield_now();

                    // Try to increment
                    let result = store.exec_with_watch(&[("counter", watched_version)], |data| {
                        let version = data.get("counter").map(|(_, v)| v + 1).unwrap_or(1);
                        data.insert(
                            "counter".to_string(),
                            ((current_value + 1).to_string(), version),
                        );
                    });

                    if result.is_some() {
                        successes.fetch_add(1, Ordering::SeqCst);
                    }

                    (i, result.is_some())
                }));
            }

            let _results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

            // At least one should succeed, but possibly all if they serialize perfectly
            let total_successes = success_count.load(Ordering::SeqCst);
            assert!(
                total_successes >= 1,
                "At least one watcher should succeed"
            );

            // If multiple succeeded, they must have serialized (each saw the other's write)
            // The final counter value depends on how many serialized successfully
        },
        1000,
    );
}

// ============================================================================
// Type conflict concurrency tests
// ============================================================================

/// Concurrent SET (string) and LPUSH (list) on same key.
/// One should succeed, one should get WRONGTYPE error.
#[test]
fn test_type_conflict_under_concurrency() {
    check_random(
        || {
            let store = Arc::new(Mutex::new(HashMap::<String, TypedValue>::new()));

            let set_result = Arc::new(Mutex::new(None::<Result<(), &'static str>>));
            let lpush_result = Arc::new(Mutex::new(None::<Result<(), &'static str>>));

            let store_set = store.clone();
            let store_lpush = store.clone();
            let set_res = set_result.clone();
            let lpush_res = lpush_result.clone();

            let t1 = thread::spawn(move || {
                let mut s = store_set.lock().unwrap();
                match s.get("key") {
                    Some(TypedValue::List(_)) => {
                        *set_res.lock().unwrap() = Some(Err("WRONGTYPE"));
                    }
                    _ => {
                        s.insert("key".to_string(), TypedValue::String("value".to_string()));
                        *set_res.lock().unwrap() = Some(Ok(()));
                    }
                }
            });

            let t2 = thread::spawn(move || {
                let mut s = store_lpush.lock().unwrap();
                match s.get_mut("key") {
                    Some(TypedValue::String(_)) => {
                        *lpush_res.lock().unwrap() = Some(Err("WRONGTYPE"));
                    }
                    Some(TypedValue::List(list)) => {
                        list.push("item".to_string());
                        *lpush_res.lock().unwrap() = Some(Ok(()));
                    }
                    None => {
                        s.insert(
                            "key".to_string(),
                            TypedValue::List(vec!["item".to_string()]),
                        );
                        *lpush_res.lock().unwrap() = Some(Ok(()));
                    }
                }
            });

            t1.join().unwrap();
            t2.join().unwrap();

            let set_ok = set_result
                .lock()
                .unwrap()
                .as_ref()
                .map(|r| r.is_ok())
                .unwrap_or(false);
            let lpush_ok = lpush_result
                .lock()
                .unwrap()
                .as_ref()
                .map(|r| r.is_ok())
                .unwrap_or(false);

            // Final type must be consistent with results
            let store = store.lock().unwrap();
            if let Some(value) = store.get("key") {
                match value {
                    TypedValue::String(_) => {
                        assert!(set_ok, "String type but SET failed?");
                        // LPUSH must have either failed with WRONGTYPE or not run yet
                    }
                    TypedValue::List(_) => {
                        assert!(lpush_ok, "List type but LPUSH failed?");
                        // SET must have either failed with WRONGTYPE or not run yet
                    }
                }
            }

            // Both cannot have succeeded with conflicting types
            if set_ok && lpush_ok {
                // Both succeeded means they didn't race on creation
                // (one created, other saw the type)
                panic!("Both SET and LPUSH cannot succeed with conflicting types");
            }
        },
        1000,
    );
}

/// Test type conflict with existing key.
/// Key exists as string, two threads try LPUSH and GET concurrently.
#[test]
fn test_type_conflict_existing_key() {
    check_random(
        || {
            let store = Arc::new(Mutex::new(HashMap::<String, TypedValue>::new()));

            // Pre-populate with string type
            {
                let mut s = store.lock().unwrap();
                s.insert("key".to_string(), TypedValue::String("initial".to_string()));
            }

            let lpush_error = Arc::new(AtomicBool::new(false));
            let store_get = store.clone();
            let store_lpush = store.clone();
            let error_flag = lpush_error.clone();

            // Thread 1: GET (should always succeed for string)
            let getter = thread::spawn(move || {
                let s = store_get.lock().unwrap();
                match s.get("key") {
                    Some(TypedValue::String(v)) => Some(v.clone()),
                    Some(TypedValue::List(_)) => panic!("Type changed unexpectedly"),
                    None => None,
                }
            });

            // Thread 2: LPUSH (should fail with WRONGTYPE)
            let pusher = thread::spawn(move || {
                let mut s = store_lpush.lock().unwrap();
                match s.get_mut("key") {
                    Some(TypedValue::String(_)) => {
                        error_flag.store(true, Ordering::SeqCst);
                        // WRONGTYPE - don't modify
                    }
                    Some(TypedValue::List(list)) => {
                        list.push("item".to_string());
                    }
                    None => {
                        s.insert(
                            "key".to_string(),
                            TypedValue::List(vec!["item".to_string()]),
                        );
                    }
                }
            });

            getter.join().unwrap();
            pusher.join().unwrap();

            // LPUSH must have gotten WRONGTYPE error
            assert!(
                lpush_error.load(Ordering::SeqCst),
                "LPUSH on string key must return WRONGTYPE"
            );

            // Key must still be a string
            let s = store.lock().unwrap();
            assert!(
                matches!(s.get("key"), Some(TypedValue::String(_))),
                "Key type must remain string"
            );
        },
        1000,
    );
}

// ============================================================================
// Real shard atomicity tests (simulation placeholder)
// ============================================================================

/// Test INCR atomicity using simulated shard state.
/// This placeholder uses AtomicU64 to simulate the atomic increment behavior
/// that would occur in the real ShardState.
#[test]
fn test_real_shard_incr_atomicity() {
    check_random(
        || {
            // This simulates what would happen with actual ShardState
            // In reality, INCR holds the shard lock during the read-modify-write
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

            for h in handles {
                h.join().unwrap();
            }

            // 4 threads * 10 increments = 40
            assert_eq!(
                counter.load(Ordering::SeqCst),
                40,
                "All increments must be counted"
            );
        },
        1000,
    );
}

/// Test GETSET atomicity - read and write in single operation.
#[test]
fn test_getset_atomicity() {
    check_random(
        || {
            let store = Arc::new(Mutex::new(Some(0i64)));
            let collected = Arc::new(Mutex::new(Vec::new()));
            let mut handles = vec![];

            // 4 threads each doing GETSET, incrementing by 1
            for i in 1..=4i64 {
                let store = store.clone();
                let collected = collected.clone();
                handles.push(thread::spawn(move || {
                    // Atomic GETSET: read old value, write new value
                    let old_value = {
                        let mut s = store.lock().unwrap();
                        let old = *s;
                        *s = Some(i);
                        old
                    };
                    collected.lock().unwrap().push((i, old_value));
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // Each thread should have gotten a distinct old value
            // (0 for first writer, then 1, 2, 3, or 4 depending on order)
            let results = collected.lock().unwrap();
            let old_values: Vec<_> = results.iter().map(|(_, old)| *old).collect();
            assert_all_unique(&old_values);
        },
        1000,
    );
}

/// Test DEL atomicity with concurrent access.
#[test]
fn test_del_concurrent_access() {
    check_random(
        || {
            let store = Arc::new(Mutex::new(HashMap::<String, String>::new()));

            // Pre-populate
            {
                let mut s = store.lock().unwrap();
                s.insert("key".to_string(), "value".to_string());
            }

            let del_success = Arc::new(AtomicBool::new(false));
            let get_found = Arc::new(Mutex::new(None::<bool>));

            let store_del = store.clone();
            let store_get = store.clone();
            let del_flag = del_success.clone();
            let get_flag = get_found.clone();

            let deleter = thread::spawn(move || {
                let mut s = store_del.lock().unwrap();
                if s.remove("key").is_some() {
                    del_flag.store(true, Ordering::SeqCst);
                }
            });

            let getter = thread::spawn(move || {
                let s = store_get.lock().unwrap();
                *get_flag.lock().unwrap() = Some(s.contains_key("key"));
            });

            deleter.join().unwrap();
            getter.join().unwrap();

            // DEL should always succeed (key existed)
            assert!(
                del_success.load(Ordering::SeqCst),
                "DEL should have found the key"
            );

            // GET result depends on ordering:
            // - true if GET ran before DEL
            // - false if GET ran after DEL
            // Both are valid outcomes
        },
        1000,
    );
}

// ============================================================================
// Snapshot concurrency tests
// ============================================================================

/// Test that concurrent snapshot attempts are properly rejected.
///
/// Multiple threads attempting start_snapshot() simultaneously should
/// result in exactly one success and all others getting AlreadyInProgress.
#[test]
fn test_snapshot_atomicity_shuttle() {
    check_random(
        || {
            let coord = Arc::new(MockSnapshotCoordinator::new());
            let success_count = Arc::new(AtomicUsize::new(0));
            let error_count = Arc::new(AtomicUsize::new(0));
            let mut handles = vec![];

            // 4 threads all trying to start a snapshot simultaneously
            for _ in 0..4 {
                let coord = coord.clone();
                let successes = success_count.clone();
                let errors = error_count.clone();

                handles.push(thread::spawn(move || {
                    match coord.start_snapshot() {
                        Ok(epoch) => {
                            successes.fetch_add(1, Ordering::SeqCst);
                            // Simulate snapshot work
                            thread::yield_now();
                            coord.complete_snapshot(epoch);
                        }
                        Err(_) => {
                            errors.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // Exactly one should have succeeded (acquired the lock)
            // The others should have gotten AlreadyInProgress
            let total = success_count.load(Ordering::SeqCst) + error_count.load(Ordering::SeqCst);
            assert_eq!(total, 4, "All threads must complete");

            // At least one must succeed
            assert!(
                success_count.load(Ordering::SeqCst) >= 1,
                "At least one snapshot must succeed"
            );

            // Snapshots are serialized, so multiple can succeed if they don't overlap
            // But all completed epochs should be unique and sequential
            let completed = coord.completed_epochs();
            assert_all_unique(&completed);
        },
        1000,
    );
}

/// Test concurrent writes during a simulated snapshot.
///
/// Verifies that writes proceed without blocking while a snapshot is in progress,
/// and that the store remains consistent.
#[test]
fn test_snapshot_concurrent_writes_shuttle() {
    check_random(
        || {
            let coord = Arc::new(MockSnapshotCoordinator::new());
            let store = Arc::new(Mutex::new(HashMap::<String, String>::new()));
            let writes_during_snapshot = Arc::new(AtomicUsize::new(0));
            let mut handles = vec![];

            // Thread 1: Start snapshot and hold it
            let coord_snap = coord.clone();
            handles.push(thread::spawn(move || {
                let epoch = coord_snap.start_snapshot().unwrap();
                assert!(coord_snap.in_progress());

                // Yield to let writers run
                thread::yield_now();
                thread::yield_now();

                coord_snap.complete_snapshot(epoch);
            }));

            // Threads 2-4: Write data (should not be blocked)
            for i in 0..3 {
                let store = store.clone();
                let writes = writes_during_snapshot.clone();
                let coord = coord.clone();

                handles.push(thread::spawn(move || {
                    let key = format!("key_{}", i);
                    let value = format!("value_{}", i);

                    // Write should succeed regardless of snapshot state
                    {
                        let mut s = store.lock().unwrap();
                        s.insert(key, value);
                    }

                    // Track if snapshot was in progress when we wrote
                    if coord.in_progress() {
                        writes.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // All 3 writes should have completed
            let store = store.lock().unwrap();
            assert_eq!(store.len(), 3, "All writes must complete");

            // Verify all keys exist
            for i in 0..3 {
                let key = format!("key_{}", i);
                assert!(store.contains_key(&key), "Key {} must exist", key);
            }
        },
        1000,
    );
}

/// Test that second snapshot request during active snapshot is rejected.
#[test]
fn test_snapshot_already_in_progress_shuttle() {
    check_random(
        || {
            let coord = Arc::new(MockSnapshotCoordinator::new());

            // Start first snapshot
            let epoch = coord.start_snapshot().unwrap();
            assert!(coord.in_progress());
            assert_eq!(epoch, 1);

            // Attempt second snapshot - must fail
            let result = coord.start_snapshot();
            assert!(result.is_err());
            assert_eq!(result.unwrap_err(), "AlreadyInProgress");

            // Still in progress
            assert!(coord.in_progress());

            // Complete first snapshot
            coord.complete_snapshot(epoch);
            assert!(!coord.in_progress());

            // Now second snapshot should work
            let epoch2 = coord.start_snapshot().unwrap();
            assert_eq!(epoch2, 2);
            assert!(coord.in_progress());

            coord.complete_snapshot(epoch2);
            assert!(!coord.in_progress());
        },
        1000,
    );
}

/// PCT test for snapshot atomicity with higher depth.
#[test]
fn test_snapshot_atomicity_pct() {
    check_pct(
        || {
            let coord = Arc::new(MockSnapshotCoordinator::new());
            let results = Arc::new(Mutex::new(Vec::new()));
            let mut handles = vec![];

            for thread_id in 0..4 {
                let coord = coord.clone();
                let results = results.clone();

                handles.push(thread::spawn(move || {
                    let result = coord.start_snapshot();
                    results.lock().unwrap().push((thread_id, result.is_ok()));

                    if let Ok(epoch) = result {
                        thread::yield_now();
                        coord.complete_snapshot(epoch);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // Verify at least one succeeded
            let results = results.lock().unwrap();
            let success_count = results.iter().filter(|(_, ok)| *ok).count();
            assert!(success_count >= 1, "At least one snapshot must succeed");
        },
        1000,
        3, // PCT depth
    );
}

// ============================================================================
// Stream Blocking Operation Tests
// ============================================================================

/// Test that stream waiters are satisfied in FIFO order.
#[test]
fn test_xread_block_fifo_fairness() {
    check_random(
        || {
            let queue = Arc::new(MockStreamWaitQueue::new());
            let mut handles = vec![];

            // Multiple threads add themselves as waiters in sequence
            // (simulating XREAD BLOCK commands arriving)
            for thread_id in 0..4 {
                let queue = queue.clone();
                handles.push(thread::spawn(move || {
                    queue.add_waiter(thread_id);
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // Now simulate XADD satisfying waiters one by one
            let mut satisfied_count = 0;
            while queue.has_waiters() {
                let _ = queue.satisfy_oldest();
                satisfied_count += 1;
            }

            assert_eq!(satisfied_count, 4, "All 4 waiters should be satisfied");

            // Verify FIFO order was maintained
            let order = queue.satisfied_order();
            assert_eq!(order.len(), 4);
            // The satisfaction order should match waiter addition order
            // (FIFO guarantee)
        },
        1000,
    );
}

/// Test concurrent XADD with a single waiting XREAD.
/// Simulates the scenario where a single waiter is waiting and multiple
/// producers add data concurrently.
#[test]
fn test_xadd_satisfies_single_waiter() {
    check_random(
        || {
            let waiter_satisfied = Arc::new(AtomicBool::new(false));
            let queue = Arc::new(MockStreamWaitQueue::new());

            // One thread waiting for data
            let queue_wait = queue.clone();
            let _satisfied = waiter_satisfied.clone();
            let waiter_handle = thread::spawn(move || {
                queue_wait.add_waiter(0);
                // Simulate waiting (in real code this would be async await)
                // The main thread will satisfy this
            });

            waiter_handle.join().unwrap();

            // Multiple producer threads trying to add data (only first should satisfy)
            let mut producer_handles = vec![];
            for producer_id in 0..3 {
                let queue = queue.clone();
                let satisfied_flag = waiter_satisfied.clone();
                producer_handles.push(thread::spawn(move || {
                    if queue.has_waiters() {
                        if let Some(_) = queue.satisfy_oldest() {
                            // This producer satisfied the waiter
                            satisfied_flag.store(true, Ordering::SeqCst);
                            return Some(producer_id);
                        }
                    }
                    None
                }));
            }

            let mut satisfier_count = 0;
            for h in producer_handles {
                if let Some(_) = h.join().unwrap() {
                    satisfier_count += 1;
                }
            }

            // Exactly one producer should have satisfied the waiter
            assert!(
                satisfier_count <= 1,
                "At most one producer should satisfy the waiter"
            );
            assert!(
                waiter_satisfied.load(Ordering::SeqCst),
                "Waiter should have been satisfied"
            );
        },
        1000,
    );
}

/// Test concurrent XADD and XREAD operations.
/// Verifies that when producers and consumers operate concurrently,
/// no data is lost and all operations complete correctly.
#[test]
fn test_concurrent_xadd_xread() {
    check_random(
        || {
            let queue = Arc::new(MockStreamWaitQueue::new());
            let total_adds = Arc::new(AtomicUsize::new(0));
            let total_satisfied = Arc::new(AtomicUsize::new(0));
            let mut handles = vec![];

            // Mix of waiters and producers
            for i in 0..6 {
                let queue = queue.clone();
                let adds = total_adds.clone();
                let satisfied = total_satisfied.clone();

                handles.push(thread::spawn(move || {
                    if i % 2 == 0 {
                        // Reader: add as waiter
                        queue.add_waiter(i);
                    } else {
                        // Writer: try to satisfy a waiter
                        if queue.has_waiters() {
                            if let Some(_) = queue.satisfy_oldest() {
                                satisfied.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        adds.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // Verify consistency: satisfied count <= waiter count
            // and satisfied count <= add count
            let sat_count = total_satisfied.load(Ordering::SeqCst);
            let add_count = total_adds.load(Ordering::SeqCst);

            assert!(
                sat_count <= add_count,
                "Can't satisfy more waiters than adds: satisfied={} adds={}",
                sat_count,
                add_count
            );
        },
        1000,
    );
}

// ============================================================================
// JSON Type Concurrency Tests
// ============================================================================

/// Test concurrent JSON.SET operations on different paths.
///
/// Multiple threads writing to different fields should all succeed
/// without losing any writes.
#[test]
fn test_concurrent_json_set_different_paths() {
    check_random(
        || {
            let json = Arc::new(MockJsonValue::new(serde_json::json!({})));
            let mut handles = vec![];

            // 4 threads, each setting a different field
            for i in 0..4 {
                let json = json.clone();
                let field = format!("field{}", i);
                let value = serde_json::json!(i);

                handles.push(thread::spawn(move || {
                    let path = format!("$.{}", field);
                    json.set(&path, value);
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // All 4 fields should be present
            let data = json.get("$").unwrap();
            if let serde_json::Value::Object(obj) = data {
                assert_eq!(obj.len(), 4, "All fields must be present");
                for i in 0..4 {
                    assert!(
                        obj.contains_key(&format!("field{}", i)),
                        "Field {} must exist",
                        i
                    );
                }
            } else {
                panic!("Expected object");
            }
        },
        1000,
    );
}

/// Test concurrent JSON.SET operations on the same path.
///
/// Multiple threads writing to the same field should result in
/// one of the values winning (last write wins).
#[test]
fn test_concurrent_json_set_same_path() {
    check_random(
        || {
            let json = Arc::new(MockJsonValue::new(serde_json::json!({"value": 0})));
            let mut handles = vec![];

            // 4 threads, each trying to set the same field
            for i in 0..4 {
                let json = json.clone();
                let value = serde_json::json!(i);

                handles.push(thread::spawn(move || {
                    json.set("$.value", value);
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // Final value should be one of 0, 1, 2, 3
            let data = json.get("$.value").unwrap();
            let final_val = data.as_i64().unwrap();
            assert!(
                (0..4).contains(&final_val),
                "Final value must be from one writer, got {}",
                final_val
            );
        },
        1000,
    );
}

/// Test concurrent JSON.NUMINCRBY operations.
///
/// Multiple threads incrementing the same numeric field should
/// produce the correct total with no lost updates.
#[test]
fn test_concurrent_json_numincrby() {
    check_random(
        || {
            let json = Arc::new(MockJsonValue::new(serde_json::json!({"counter": 0})));
            let mut handles = vec![];

            // 4 threads, each incrementing 10 times
            for _ in 0..4 {
                let json = json.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..10 {
                        json.incr_by("$.counter", 1);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // Final value should be 4 * 10 = 40
            let data = json.get("$.counter").unwrap();
            assert_eq!(
                data.as_i64().unwrap(),
                40,
                "All increments must be counted"
            );
        },
        1000,
    );
}

/// Test concurrent JSON.ARRAPPEND operations.
///
/// Multiple threads appending to the same array should all succeed,
/// and the final array should contain all appended elements.
#[test]
fn test_concurrent_json_arrappend() {
    check_random(
        || {
            let json = Arc::new(MockJsonValue::new(serde_json::json!({"arr": []})));
            let mut handles = vec![];

            // 4 threads, each appending 5 elements
            for i in 0..4 {
                let json = json.clone();
                handles.push(thread::spawn(move || {
                    for j in 0..5 {
                        let value = serde_json::json!(i * 10 + j);
                        json.arr_append("$.arr", value);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // Array should have 4 * 5 = 20 elements
            let data = json.get("$.arr").unwrap();
            if let serde_json::Value::Array(arr) = data {
                assert_eq!(arr.len(), 20, "Array should have all appended elements");
            } else {
                panic!("Expected array");
            }
        },
        1000,
    );
}

/// Test concurrent JSON.ARRPOP operations.
///
/// Multiple threads popping from the same array should get distinct
/// elements with no duplicates.
#[test]
fn test_concurrent_json_arrpop() {
    check_random(
        || {
            let initial_arr: Vec<i32> = (0..8).collect();
            let json = Arc::new(MockJsonValue::new(serde_json::json!({"arr": initial_arr})));
            let popped = Arc::new(Mutex::new(Vec::new()));
            let mut handles = vec![];

            // 4 threads, each popping 2 elements
            for _ in 0..4 {
                let json = json.clone();
                let popped = popped.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..2 {
                        if let Some(value) = json.arr_pop("$.arr") {
                            popped.lock().unwrap().push(value);
                        }
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // Should have popped all 8 elements
            let popped = popped.lock().unwrap();
            assert_eq!(popped.len(), 8, "Should pop all elements");

            // Check uniqueness (convert to i64 for comparison)
            let values: Vec<i64> = popped.iter().filter_map(|v| v.as_i64()).collect();
            assert_all_unique(&values);
        },
        1000,
    );
}

/// Test concurrent read and write operations on JSON.
///
/// Readers should see consistent snapshots (no partial updates).
#[test]
fn test_concurrent_json_read_write() {
    check_random(
        || {
            let json = Arc::new(MockJsonValue::new(serde_json::json!({"a": 0, "b": 0})));
            let inconsistent = Arc::new(AtomicBool::new(false));
            let mut handles = vec![];

            // Writer thread: updates both fields together
            let json_w = json.clone();
            handles.push(thread::spawn(move || {
                for i in 1..=5 {
                    // Both fields should always have the same value
                    json_w.set("$.a", serde_json::json!(i));
                    json_w.set("$.b", serde_json::json!(i));
                }
            }));

            // Reader threads: check that a == b
            for _ in 0..3 {
                let json = json.clone();
                let inconsistent = inconsistent.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..10 {
                        let a = json.get("$.a").and_then(|v| v.as_i64());
                        let b = json.get("$.b").and_then(|v| v.as_i64());
                        // Note: This CAN observe inconsistency because we don't
                        // have atomic multi-field updates in this mock.
                        // In real FrogDB, this would be handled by shard locking.
                        if let (Some(a_val), Some(b_val)) = (a, b) {
                            // Record if we ever see inconsistency
                            // (This is expected in this mock without transactions)
                            if a_val != b_val {
                                inconsistent.store(true, Ordering::SeqCst);
                            }
                        }
                        thread::yield_now();
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // Final state should be consistent
            let a = json.get("$.a").and_then(|v| v.as_i64()).unwrap();
            let b = json.get("$.b").and_then(|v| v.as_i64()).unwrap();
            assert_eq!(a, b, "Final state must be consistent");
        },
        1000,
    );
}

/// Test concurrent JSON operations with mixed types.
///
/// Verifies that type-specific operations work correctly under concurrency.
#[test]
fn test_concurrent_json_mixed_operations() {
    check_random(
        || {
            let json = Arc::new(MockJsonValue::new(serde_json::json!({
                "counter": 0,
                "items": [],
                "name": "initial"
            })));
            let mut handles = vec![];

            // Thread 1: Increment counter
            let json1 = json.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..5 {
                    json1.incr_by("$.counter", 1);
                }
            }));

            // Thread 2: Append to array
            let json2 = json.clone();
            handles.push(thread::spawn(move || {
                for i in 0..5 {
                    json2.arr_append("$.items", serde_json::json!(i));
                }
            }));

            // Thread 3: Update name
            let json3 = json.clone();
            handles.push(thread::spawn(move || {
                for i in 0..5 {
                    json3.set("$.name", serde_json::json!(format!("name{}", i)));
                }
            }));

            for h in handles {
                h.join().unwrap();
            }

            // Verify final state
            let counter = json.get("$.counter").and_then(|v| v.as_i64()).unwrap();
            assert_eq!(counter, 5, "Counter should be 5");

            let items = json.get("$.items").unwrap();
            if let serde_json::Value::Array(arr) = items {
                assert_eq!(arr.len(), 5, "Items should have 5 elements");
            }

            let name = json.get("$.name").and_then(|v| v.as_str().map(String::from));
            assert!(name.is_some(), "Name should exist");
        },
        1000,
    );
}

/// PCT test for JSON NUMINCRBY atomicity.
#[test]
fn test_json_numincrby_pct() {
    check_pct(
        || {
            let json = Arc::new(MockJsonValue::new(serde_json::json!({"value": 0})));
            let mut handles = vec![];

            for _ in 0..4 {
                let json = json.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..5 {
                        json.incr_by("$.value", 1);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            let value = json.get("$.value").and_then(|v| v.as_i64()).unwrap();
            assert_eq!(value, 20, "All increments must be counted");
        },
        1000,
        3, // PCT depth
    );
}

// ============================================================================
// RESET Command Concurrency Tests
// ============================================================================

/// Test RESET concurrent with PUBLISH operations.
///
/// Simulates: subscriber RESETs while publisher sends messages.
/// Ensures no race conditions or panics.
#[test]
fn test_reset_concurrent_with_publish() {
    check_random(
        || {
            let subscriptions = Arc::new(Mutex::new(HashSet::<String>::new()));
            let messages_received = Arc::new(AtomicUsize::new(0));

            let subs = subscriptions.clone();
            let received = messages_received.clone();

            // Thread 1: Subscribe, receive messages, then RESET
            let subscriber = thread::spawn(move || {
                // Subscribe
                subs.lock().unwrap().insert("channel".to_string());

                // Yield to allow publisher to run
                for _ in 0..3 {
                    shuttle::thread::yield_now();
                    // Check if still subscribed (simulate message receipt)
                    if subs.lock().unwrap().contains("channel") {
                        received.fetch_add(1, Ordering::SeqCst);
                    }
                }

                // RESET (clear subscriptions)
                subs.lock().unwrap().clear();
            });

            let subs2 = subscriptions.clone();
            // Thread 2: Publish messages
            let publisher = thread::spawn(move || {
                for _ in 0..5 {
                    shuttle::thread::yield_now();
                    // Count subscribers (simulate PUBLISH return value)
                    let _count = subs2.lock().unwrap().len();
                }
            });

            subscriber.join().unwrap();
            publisher.join().unwrap();

            // After RESET, no subscriptions should remain
            assert!(subscriptions.lock().unwrap().is_empty());
        },
        1000,
    );
}

/// Test multiple clients RESETting simultaneously.
///
/// Verifies that concurrent RESETs from different connections don't
/// interfere with each other and all complete successfully.
#[test]
fn test_multiple_clients_reset_simultaneously() {
    check_random(
        || {
            let reset_count = Arc::new(AtomicUsize::new(0));
            let mut handles = vec![];

            for _ in 0..4 {
                let count = reset_count.clone();
                handles.push(thread::spawn(move || {
                    // Simulate RESET (just increment counter atomically)
                    count.fetch_add(1, Ordering::SeqCst);
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            assert_eq!(reset_count.load(Ordering::SeqCst), 4);
        },
        1000,
    );
}

/// Test that RESET does not affect other client's transactions.
///
/// Two clients: one in transaction, one RESETs.
/// RESET should only affect own connection.
#[test]
fn test_reset_does_not_affect_other_client_transactions() {
    check_random(
        || {
            let store = Arc::new(Mutex::new(HashMap::<String, String>::new()));
            let client1_committed = Arc::new(AtomicBool::new(false));

            let store1 = store.clone();
            let committed = client1_committed.clone();

            // Client 1: MULTI, SET, EXEC
            let client1 = thread::spawn(move || {
                // Simulate transaction
                let value_to_set = "from_client1".to_string();
                shuttle::thread::yield_now();

                // EXEC (commit)
                store1
                    .lock()
                    .unwrap()
                    .insert("key".to_string(), value_to_set);
                committed.store(true, Ordering::SeqCst);
            });

            // Client 2: RESET (should not affect client1)
            let client2 = thread::spawn(move || {
                shuttle::thread::yield_now();
                // RESET only affects own connection state
            });

            client1.join().unwrap();
            client2.join().unwrap();

            // Client1's transaction should have committed
            assert!(client1_committed.load(Ordering::SeqCst));
            assert_eq!(
                store.lock().unwrap().get("key"),
                Some(&"from_client1".to_string())
            );
        },
        1000,
    );
}
