//! The arena-figure seam: how the broker learns what its core has allocated.
//!
//! # This is a sampled upper bound, and the naming says so
//!
//! [adr/0006](../../../../../adr/0006-memory-architecture-seams.md) §3 measured
//! what an arena reading actually is. Refreshing jemalloc's per-arena
//! statistics means advancing a `mallctl` epoch — about 2.5 µs per arena — and
//! the figure that comes back **overstates** live bytes by whatever is parked
//! in the thread cache: in the spike an arena still reported 25,600 B after
//! every object charged to it had been freed. So the broker samples at
//! 10–100 Hz, not per command, and every reading it publishes is an upper
//! bound on the core's live bytes rather than a measurement of them.
//!
//! The error direction is the safe one — an upper bound refuses slightly
//! early — but no API here is allowed to pretend otherwise. There is
//! deliberately no `live_bytes()`, no `used_bytes()` and no `current_bytes()`
//! on this trait or on the broker: the only spelling is
//! [`ArenaSampler::sampled_upper_bound_bytes`], and a caller that wants a
//! different word has to change the contract to get it.
//!
//! # Who implements this
//!
//! Arena binding is not this crate's work. The real implementation lives in
//! `frogdb-server`, over `frogdb_telemetry::ShardArenaRegistry`: the registry
//! is the only crate that knows which arena a shard bound, and it sits above
//! both this crate and the one that builds the broker, so the adapter belongs
//! in the one crate that already depends on both. It is installed on the
//! broker through [`crate::MemoryBroker::set_arena_sampler`] once the shard's
//! arena is known.
//!
//! [`NoArenaReading`] stays the answer everywhere no arena is bound: the
//! simulation executor (a simulated shard is a task on the sim host's one
//! thread and has no arena of its own — adr/0006 §1) and a build without an
//! arena-capable allocator. It reports `None` — "this core has no arena
//! figure", never zero. A shard that *could* have bound an arena and could not
//! is not one of these cases: it fails boot rather than running blind
//! (`frogdb_net::RealShardExecutor::launch`).

/// The broker's read of its core's allocator state.
///
/// Implementors return a **sampled upper bound** on the bytes the core's arena
/// is holding, or `None` when the core has no arena to read (simulation, a
/// platform without jemalloc, or a sampler that has not taken its first sample
/// yet). `None` means "unknown", never "zero": a broker must not conclude a
/// core is empty because nobody told it otherwise.
pub trait ArenaSampler: Send + Sync + std::fmt::Debug {
    /// The most recent sample, in bytes. Cheap: this is a read of a figure the
    /// sampler refreshed on its own schedule, never an epoch advance taken on
    /// the caller's behalf.
    fn sampled_upper_bound_bytes(&self) -> Option<u64>;
}

/// The reading for a core with no arena of its own: the default a broker is
/// built with, and the permanent answer under simulation.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoArenaReading;

impl ArenaSampler for NoArenaReading {
    fn sampled_upper_bound_bytes(&self) -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stub_reports_unknown_not_zero() {
        assert_eq!(NoArenaReading.sampled_upper_bound_bytes(), None);
    }

    /// A hand-written sampler stands in for issue 03's, proving the seam is a
    /// one-site swap: nothing but the constructor argument changes.
    #[test]
    fn a_sampler_plugs_in_without_touching_the_broker() {
        #[derive(Debug)]
        struct Fixed(u64);
        impl ArenaSampler for Fixed {
            fn sampled_upper_bound_bytes(&self) -> Option<u64> {
                Some(self.0)
            }
        }
        let sampler: Box<dyn ArenaSampler> = Box::new(Fixed(4096));
        assert_eq!(sampler.sampled_upper_bound_bytes(), Some(4096));
    }
}
