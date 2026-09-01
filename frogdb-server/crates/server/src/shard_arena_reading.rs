//! The adapter that lets a shard's memory broker read its own arena figure.
//!
//! Two halves of ADR-0006 §3 meet here. [`frogdb_telemetry::ShardArenaRegistry`]
//! knows which jemalloc arena each shard bound and what that arena last
//! reported; [`frogdb_memory::MemoryBroker`] wants one number for its own core,
//! through the [`frogdb_memory::ArenaSampler`] seam. Neither may name the other
//! — `frogdb-telemetry` depends on `frogdb-core`, which is where the broker is
//! built — so the adapter lives here, in the one crate that already depends on
//! both. Same reason, same shape as [`crate::net::JemallocShardArenas`].
//!
//! # Why the reading is late-bound
//!
//! A shard's arena does not exist when its worker is built: the arena is
//! created *on the shard's own thread*, as that thread's first act, and the
//! registry is assembled from `ShardExecutor::arena_of` only once every shard
//! has launched. By then the workers are gone into their threads. So the server
//! hands each worker a [`ShardArenaReading`] pointing at an empty slot, and
//! fills the slot once — with [`ShardArenaReadings::publish`] — as soon as the
//! registry exists.
//!
//! A read that lands before the slot is filled reports `None`: "no figure for
//! this core", which is exactly what the broker reported before this wiring
//! existed and what it keeps reporting for a shard that never bound an arena.
//! Absent is never zero, at any point in that sequence.
//!
//! # Read discipline
//!
//! [`ShardArenaReading::sampled_upper_bound_bytes`] is three relaxed atomic
//! loads and a slice scan. It must never refresh anything: arena statistics only
//! move on a `mallctl` epoch advance, which merges *every* arena at ~2.5 µs
//! each, and the sampler task owns that advance on its 10–100 Hz tick
//! (`arena_sampling_is_not_on_a_command_path` pins the advance to the two files
//! that may contain it). A broker asking "what does my core hold" gets the last
//! tick's answer, not a fresh one taken on its behalf.

use std::sync::{Arc, OnceLock};

use frogdb_memory::ArenaSampler;
use frogdb_telemetry::ShardArenaRegistry;

/// The shared slot every shard's reading points at, plus the one publish that
/// fills it.
///
/// Cheap to clone; every clone shares one slot.
#[derive(Debug, Clone, Default)]
pub struct ShardArenaReadings {
    registry: Arc<OnceLock<Arc<ShardArenaRegistry>>>,
}

impl ShardArenaReadings {
    /// An unfilled slot. Every reading taken from it reports `None` until
    /// [`publish`](Self::publish) runs.
    pub fn new() -> Self {
        Self::default()
    }

    /// The arena reading for `shard_id`, ready to install on that shard's
    /// broker before the shard runs.
    pub fn sampler_for(&self, shard_id: usize) -> Arc<dyn ArenaSampler> {
        Arc::new(ShardArenaReading {
            registry: Arc::clone(&self.registry),
            shard_id,
        })
    }

    /// Fill the slot with the registry the shard spawn built.
    ///
    /// Once per boot: arena bindings are fixed for the life of the process, so
    /// there is one registry and a second publish would be a bug rather than an
    /// update. The first one wins and later ones are ignored, which keeps a
    /// stray caller from swapping the figure out from under running shards.
    pub fn publish(&self, registry: Arc<ShardArenaRegistry>) {
        let _ = self.registry.set(registry);
    }
}

/// One shard's view of the registry: its own arena's last sampled figure, or
/// nothing.
#[derive(Debug)]
struct ShardArenaReading {
    registry: Arc<OnceLock<Arc<ShardArenaRegistry>>>,
    shard_id: usize,
}

impl ArenaSampler for ShardArenaReading {
    fn sampled_upper_bound_bytes(&self) -> Option<u64> {
        // Three ways to have no figure, all reported the same way, none of them
        // zero: the registry has not been published yet, this shard bound no
        // arena, or the sampler has not taken its first tick.
        let sample = self.registry.get()?.sample(self.shard_id)?;
        sample
            .is_sampled()
            .then(|| sample.allocated_upper_bound_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_memory::MemoryBroker;

    /// The window between handing a worker its reading and publishing the
    /// registry. A broker that read a zero here would think its core was empty.
    #[test]
    fn a_reading_taken_before_the_registry_exists_is_absent_not_zero() {
        let readings = ShardArenaReadings::new();
        let broker = MemoryBroker::new(0, readings.sampler_for(0));
        assert_eq!(broker.arena_sampled_upper_bound_bytes(), None);
        assert_eq!(broker.breakdown().arena_sampled_upper_bound_bytes, None);
    }

    /// A shard missing from the registry — a failed bind, or every shard under
    /// simulation — reports what it reported before this wiring existed.
    #[test]
    fn a_shard_with_no_arena_reads_the_same_as_no_wiring_at_all() {
        let readings = ShardArenaReadings::new();
        let broker = MemoryBroker::new(1, readings.sampler_for(1));
        // Shard 0 bound an arena; shard 1 did not, so it is simply absent.
        readings.publish(Arc::new(ShardArenaRegistry::new([(0, 7)])));

        assert_eq!(broker.arena_sampled_upper_bound_bytes(), None);
        assert_eq!(
            MemoryBroker::detached(1).arena_sampled_upper_bound_bytes(),
            broker.arena_sampled_upper_bound_bytes(),
            "an unbound shard must read exactly as it did before it was wired"
        );
    }

    /// A bound shard whose sampler has not ticked yet holds placeholder zeros.
    /// Reporting them would tell the broker the core is empty; the reading has
    /// to check `is_sampled()` and say nothing instead.
    #[test]
    fn a_bound_shard_reports_nothing_until_its_first_sample_lands() {
        let readings = ShardArenaReadings::new();
        let broker = MemoryBroker::new(0, readings.sampler_for(0));
        let registry = Arc::new(ShardArenaRegistry::new([(0, 7)]));
        readings.publish(registry.clone());

        assert!(!registry.sample(0).expect("shard 0 is bound").is_sampled());
        assert_eq!(broker.arena_sampled_upper_bound_bytes(), None);
    }

    /// Only the first publish counts: the registry is built once per boot, and
    /// a second one must not be able to re-point running shards' brokers.
    #[test]
    fn the_registry_is_published_once() {
        let readings = ShardArenaReadings::new();
        readings.publish(Arc::new(ShardArenaRegistry::new([(0, 7)])));
        readings.publish(Arc::new(ShardArenaRegistry::empty()));

        let registry = readings.registry.get().expect("published");
        assert_eq!(
            registry.arena_of(0),
            Some(7),
            "the first published registry must survive a second publish"
        );
    }

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
    #[cfg(not(target_env = "msvc"))]
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
    #[cfg(not(target_env = "msvc"))]
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
}
