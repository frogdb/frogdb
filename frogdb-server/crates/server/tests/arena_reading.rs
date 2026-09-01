//! Real-thread end-to-end tests for the broker's arena reading.
//!
//! These live in a test binary of their own because of the allocator
//! declaration below: they are the only tests in this crate whose *own*
//! allocations have to reach a jemalloc arena, and swapping the global
//! allocator under every unit test in the crate to serve two of them is a
//! surprising thing to find in `lib.rs`.
//!
//! The allocator-independent half of
//! [`frogdb_server::shard_arena_reading`]'s coverage — absent-not-zero at each
//! point of the publish sequence — stays with the module as unit tests.

#![cfg(not(target_env = "msvc"))]

/// jemalloc as this test binary's global allocator.
///
/// The assertions below are about *bytes*: a shard-shaped thread binds its own
/// arena, holds a known volume live, and the broker for that shard must report
/// at least that many bytes. That is only true if Rust allocations go through
/// jemalloc at all — under the system allocator every arena reads zero and the
/// tests would pass or fail for reasons unrelated to what they check. The
/// production binary declares the same allocator in `main.rs`, and
/// `frogdb-telemetry` does the same for its own arena tests.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;

use frogdb_memory::MemoryBroker;
use frogdb_server::shard_arena_reading::ShardArenaReadings;
use frogdb_telemetry::ShardArenaRegistry;

/// End to end on the real mechanism: a shard-shaped thread binds its own
/// arena and holds a known volume live, the registry samples it, and the
/// broker on that shard reports a bound at or above what the thread holds.
///
/// Real threads and a real arena because that is what the assertion is
/// about — the figure only becomes non-`None` when jemalloc actually
/// refreshes the slot, and no fake registry entry can produce that. The
/// refresh here stands in for the sampler task's tick; nothing on a broker
/// read path refreshes anything.
#[test]
fn a_bound_shards_broker_reports_the_sampled_upper_bound() {
    const CHUNK: usize = 64 * 1024;
    const CHUNKS: usize = 128; // 8 MiB
    let volume = (CHUNK * CHUNKS) as u64;

    let (arena_tx, arena_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let shard = std::thread::spawn(move || {
        let arena = frogdb_telemetry::jemalloc::create_arena().expect("arenas.create");
        frogdb_telemetry::jemalloc::bind_current_thread_to_arena(arena).expect("thread.arena");
        // Held live, and freed only after the read: objects freed into the
        // thread cache stay charged to the arena until it is flushed.
        let held: Vec<Vec<u8>> = (0..CHUNKS).map(|_| vec![3u8; CHUNK]).collect();
        arena_tx.send(arena).expect("report arena");
        let _ = stop_rx.recv();
        drop(held);
    });

    let arena = arena_rx.recv().expect("the shard reported its arena");
    let readings = ShardArenaReadings::new();
    let broker = MemoryBroker::new(0, readings.sampler_for(0));
    let registry = Arc::new(ShardArenaRegistry::new([(0, arena)]));
    readings.publish(registry.clone());
    assert_eq!(registry.refresh(), 1, "the shard's arena was sampled");

    let reading = broker.arena_sampled_upper_bound_bytes();
    let breakdown = broker.breakdown().arena_sampled_upper_bound_bytes;
    let _ = stop_tx.send(());
    shard.join().expect("shard thread");

    let bytes = reading.expect("a sampled shard's broker must have a figure");
    assert!(
        bytes >= volume,
        "shard 0 held {volume} B live but its broker reports {bytes} B"
    );
    assert_eq!(
        breakdown, reading,
        "the operator-facing breakdown must carry the same figure"
    );
}

/// The same broker on a *different* shard must not pick up shard 0's bytes:
/// the reading is per-core, keyed by shard id, not "some arena's figure".
#[test]
fn a_brokers_reading_is_its_own_shards_and_no_ones_else() {
    let (arena_tx, arena_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let shard = std::thread::spawn(move || {
        let arena = frogdb_telemetry::jemalloc::create_arena().expect("arenas.create");
        frogdb_telemetry::jemalloc::bind_current_thread_to_arena(arena).expect("thread.arena");
        let held: Vec<Vec<u8>> = (0..64).map(|_| vec![5u8; 64 * 1024]).collect();
        arena_tx.send(arena).expect("report arena");
        let _ = stop_rx.recv();
        drop(held);
    });

    let arena = arena_rx.recv().expect("the shard reported its arena");
    let readings = ShardArenaReadings::new();
    let bound = MemoryBroker::new(0, readings.sampler_for(0));
    let unbound = MemoryBroker::new(1, readings.sampler_for(1));
    let registry = Arc::new(ShardArenaRegistry::new([(0, arena)]));
    readings.publish(registry.clone());
    registry.refresh();

    let bound_reading = bound.arena_sampled_upper_bound_bytes();
    let unbound_reading = unbound.arena_sampled_upper_bound_bytes();
    let _ = stop_tx.send(());
    shard.join().expect("shard thread");

    assert!(bound_reading.is_some(), "shard 0 has a sampled arena");
    assert_eq!(
        unbound_reading, None,
        "shard 1 bound no arena; it must report nothing rather than \
         shard 0's bytes"
    );
}
