//! Per-shard arena accounting: which arena each shard owns, and what it last
//! reported.
//!
//! This is the sampled half of ADR-0006 §3. The shard executor binds one
//! jemalloc arena per shard thread; this module records the bindings, refreshes
//! their figures on a fixed low-frequency tick, and hands the result to anything
//! that wants per-shard memory — the `frogdb_allocator_*_by_shard` gauges today,
//! the memory broker later.
//!
//! Three shapes of the design are load-bearing:
//!
//! * **Sampled, never per command.** Arena statistics only move on a `mallctl`
//!   epoch advance, and that advance merges *every* arena at ~2.5 µs each
//!   (spike-report §(a) E5). One advance per tick amortises it over every arena;
//!   an advance per command would put a microseconds-scale allocator-wide
//!   operation on the hot path for a number that changes by kilobytes. Per-request
//!   accounting rides [`crate::jemalloc::thread_allocated`] (23 ns, exact,
//!   thread-local) between ticks and reconciles here.
//! * **Upper bound, not live bytes.** Everything [`ShardArenaSample`] reports is
//!   an upper bound on the shard's live bytes, for the reason
//!   [`crate::jemalloc::ArenaStats`] documents. The overstatement direction is
//!   the safe one for a memory refusal: a budget written against it refuses
//!   slightly early rather than slightly late.
//! * **A shard with no arena is absent, not zero.** [`ShardArenaRegistry`] only
//!   holds shards that bound an arena — on a build that has arenas, that is
//!   every shard or the server did not boot. Where there are no arenas at all
//!   (simulation, or an allocator without them) a shard allocates on the
//!   automatic arena together with every non-shard thread, and reporting that
//!   arena's total as "shard N's memory" would be a much worse answer than
//!   reporting nothing.

use crate::jemalloc;
use frogdb_core::MetricsRecorder;
use frogdb_types::metrics::definitions::ArenaSamplerOnShardThreadTotal;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// One shard's last sampled arena figures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardArenaSample {
    /// The shard these figures belong to.
    pub shard_id: usize,
    /// The jemalloc arena bound to that shard's thread.
    pub arena: u32,
    /// `small.allocated` — an upper bound (see the module docs).
    pub small_allocated_upper_bound_bytes: u64,
    /// `large.allocated` — an upper bound (see the module docs).
    pub large_allocated_upper_bound_bytes: u64,
    /// `resident`: the shard's physically resident bytes.
    pub resident_bytes: u64,
    /// `pactive` in bytes: pages the arena is serving allocations from.
    pub active_bytes: u64,
    /// `pdirty` in bytes: pages freed but not yet returned to the OS. Rising
    /// here while `allocated` holds steady is reclamation lag, not growth.
    pub dirty_bytes: u64,
    /// `pmuzzy` in bytes: pages madvised away but still mapped. Zero under the
    /// server's `muzzy_decay_ms:0` default.
    pub muzzy_bytes: u64,
    /// `retained`: address space kept mapped rather than unmapped. Costs address
    /// space, not physical memory.
    pub retained_bytes: u64,
    /// How many samples have landed in this slot. `0` means the sampler has not
    /// run yet and every figure above is a placeholder zero, which a caller must
    /// distinguish from a genuinely empty shard.
    pub samples: u64,
}

impl ShardArenaSample {
    /// An upper bound on the shard's live application bytes.
    pub fn allocated_upper_bound_bytes(&self) -> u64 {
        self.small_allocated_upper_bound_bytes
            .saturating_add(self.large_allocated_upper_bound_bytes)
    }

    /// `resident / allocated` for this shard — the per-shard fragmentation
    /// number PRD R13 wants, which per-shard arenas make available for free (the
    /// spike measured 1.15–1.23). `None` before the first sample, or when the
    /// shard has no allocated bytes to divide by.
    pub fn fragmentation_ratio(&self) -> Option<f64> {
        let allocated = self.allocated_upper_bound_bytes();
        (self.samples > 0 && allocated > 0).then(|| self.resident_bytes as f64 / allocated as f64)
    }

    /// Whether the sampler has ever filled this slot.
    pub fn is_sampled(&self) -> bool {
        self.samples > 0
    }
}

/// One shard's slot. Plain relaxed atomics: the sampler is the only writer, each
/// reader wants a recent figure rather than a consistent multi-field snapshot,
/// and the numbers are a sampled upper bound to begin with — a torn read across
/// two fields is indistinguishable from a read one tick older.
#[derive(Debug)]
struct ShardArenaSlot {
    shard_id: usize,
    arena: u32,
    small: AtomicU64,
    large: AtomicU64,
    resident: AtomicU64,
    active: AtomicU64,
    dirty: AtomicU64,
    muzzy: AtomicU64,
    retained: AtomicU64,
    samples: AtomicU64,
}

/// Which arena each shard owns, plus that arena's last sampled figures.
///
/// Built once, after the shards are launched, from
/// `frogdb_net::ShardExecutor::arena_of`. Shards that report no arena — every
/// shard under simulation, and every shard on a build whose allocator has no
/// arenas — are simply absent.
#[derive(Debug, Default)]
pub struct ShardArenaRegistry {
    shards: Vec<ShardArenaSlot>,
}

impl ShardArenaRegistry {
    /// Record `(shard_id, arena)` bindings. Bindings are fixed for the life of
    /// the process: an arena is bound once at shard-thread start and never
    /// rebound (ADR-0006 §3), so there is no registration path after this.
    pub fn new(bindings: impl IntoIterator<Item = (usize, u32)>) -> Self {
        Self {
            shards: bindings
                .into_iter()
                .map(|(shard_id, arena)| ShardArenaSlot {
                    shard_id,
                    arena,
                    small: AtomicU64::new(0),
                    large: AtomicU64::new(0),
                    resident: AtomicU64::new(0),
                    active: AtomicU64::new(0),
                    dirty: AtomicU64::new(0),
                    muzzy: AtomicU64::new(0),
                    retained: AtomicU64::new(0),
                    samples: AtomicU64::new(0),
                })
                .collect(),
        }
    }

    /// A registry with no arenas — what a simulation build and an
    /// allocator-less target get. Every read reports nothing.
    pub fn empty() -> Self {
        Self::default()
    }

    /// How many shards have an arena.
    pub fn len(&self) -> usize {
        self.shards.len()
    }

    /// Whether any shard has an arena at all.
    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    /// The arena bound to `shard_id`, or `None` if that shard has none.
    pub fn arena_of(&self, shard_id: usize) -> Option<u32> {
        self.slot(shard_id).map(|s| s.arena)
    }

    /// The last sample for `shard_id`, or `None` if that shard has no arena.
    pub fn sample(&self, shard_id: usize) -> Option<ShardArenaSample> {
        self.slot(shard_id).map(Self::read_slot)
    }

    /// The last sample for every shard that has an arena, in shard order.
    pub fn samples(&self) -> impl Iterator<Item = ShardArenaSample> + '_ {
        self.shards.iter().map(Self::read_slot)
    }

    /// An upper bound on the bytes allocated across every shard arena.
    ///
    /// Deliberately not "the process's memory": it excludes the automatic arena,
    /// where every non-shard thread — the acceptor, the observability server,
    /// background tasks — allocates. `INFO memory`'s process-wide fields remain
    /// the answer to that question.
    pub fn total_allocated_upper_bound_bytes(&self) -> u64 {
        self.samples()
            .map(|s| s.allocated_upper_bound_bytes())
            .fold(0u64, u64::saturating_add)
    }

    /// Advance the allocator's stats epoch once and refresh every shard's
    /// figures from it. Returns how many shards were refreshed.
    ///
    /// The single advance before the per-arena reads is the whole point: it
    /// costs ~2.5 µs per live arena and refreshes all of them, so N shards cost
    /// one advance plus N cheap by-name reads rather than N advances.
    ///
    /// Called by [`ArenaSampler`] on its tick, and by tests. Never from a
    /// command path — `arena_sampling_is_not_on_a_command_path` pins that.
    pub fn refresh(&self) -> usize {
        self.refresh_stride(0..self.shards.len())
    }

    /// Refresh the slots in `stride` — see [`arena_stride`] for how the sampler
    /// picks one. Advances the epoch once, then reads only that slice.
    ///
    /// The epoch advance is unavoidable (it merges every arena whatever is read
    /// afterwards, which is what [`ArenaSampleRate::for_arena_count`] budgets
    /// for); what the stride bounds is the per-tick read work and the burst of
    /// cache traffic that comes with it.
    pub fn refresh_stride(&self, stride: std::ops::Range<usize>) -> usize {
        let Some(slots) = self.shards.get(stride) else {
            return 0;
        };
        if slots.is_empty() || !jemalloc::advance_epoch() {
            return 0;
        }
        let mut refreshed = 0;
        for slot in slots {
            let Some(stats) = jemalloc::read_arena_stats(slot.arena) else {
                continue;
            };
            slot.small
                .store(stats.small_allocated_upper_bound, Ordering::Relaxed);
            slot.large
                .store(stats.large_allocated_upper_bound, Ordering::Relaxed);
            slot.resident.store(stats.resident, Ordering::Relaxed);
            slot.active.store(stats.active, Ordering::Relaxed);
            slot.dirty.store(stats.dirty, Ordering::Relaxed);
            slot.muzzy.store(stats.muzzy, Ordering::Relaxed);
            slot.retained.store(stats.retained, Ordering::Relaxed);
            slot.samples.fetch_add(1, Ordering::Relaxed);
            refreshed += 1;
        }
        refreshed
    }

    fn slot(&self, shard_id: usize) -> Option<&ShardArenaSlot> {
        self.shards.iter().find(|s| s.shard_id == shard_id)
    }

    /// Whether `arena` belongs to a shard in this registry — how the sampler
    /// recognises that it has woken up on a shard thread.
    fn holds_arena(&self, arena: u32) -> bool {
        self.shards.iter().any(|s| s.arena == arena)
    }

    fn read_slot(slot: &ShardArenaSlot) -> ShardArenaSample {
        ShardArenaSample {
            shard_id: slot.shard_id,
            arena: slot.arena,
            small_allocated_upper_bound_bytes: slot.small.load(Ordering::Relaxed),
            large_allocated_upper_bound_bytes: slot.large.load(Ordering::Relaxed),
            resident_bytes: slot.resident.load(Ordering::Relaxed),
            active_bytes: slot.active.load(Ordering::Relaxed),
            dirty_bytes: slot.dirty.load(Ordering::Relaxed),
            muzzy_bytes: slot.muzzy.load(Ordering::Relaxed),
            retained_bytes: slot.retained.load(Ordering::Relaxed),
            samples: slot.samples.load(Ordering::Relaxed),
        }
    }
}

/// The most arenas the sampler reads in a single tick.
///
/// Past this the sweep is spread over consecutive ticks
/// ([`arena_stride`]) instead of doing every arena at once. The point is not
/// the total read cost — a by-name arena read is ~146 ns, noise against the
/// epoch advance — but the shape of the tick: a 64-shard server reading every
/// arena on every tick does one long burst of allocator work while shard
/// threads are running, and a burst is what shows up in a tail-latency
/// histogram. 32 keeps the burst at the size a mid-sized server already runs
/// with today.
pub const MAX_ARENAS_PER_TICK: usize = 32;

/// How many ticks a full sweep of `arenas` arenas takes.
pub fn sweep_ticks(arenas: usize) -> usize {
    arenas.div_ceil(MAX_ARENAS_PER_TICK).max(1)
}

/// The slice of arenas to read on `tick`.
///
/// Consecutive ticks walk contiguous, non-overlapping chunks and wrap after
/// [`sweep_ticks`] of them, so every arena is read exactly once per sweep and no
/// arena is ever skipped twice in a row. At or below [`MAX_ARENAS_PER_TICK`] the
/// stride is the whole range and every tick is a full sweep, which is what every
/// server up to 32 shards does.
pub fn arena_stride(arenas: usize, tick: u64) -> std::ops::Range<usize> {
    if arenas == 0 {
        return 0..0;
    }
    let sweep = sweep_ticks(arenas);
    let per_tick = arenas.div_ceil(sweep);
    let start = (tick % sweep as u64) as usize * per_tick;
    let start = start.min(arenas);
    start..(start + per_tick).min(arenas)
}

/// How often the arena sampler advances the epoch.
///
/// ADR-0006 §3 fixes the band at **10–100 Hz** and this type is where that band
/// lives: a configured value outside it is clamped rather than honoured, because
/// both ends are load-bearing. Below 10 Hz the figure a budget refuses against
/// is more than 100 ms stale; above 100 Hz the all-arena epoch merge stops being
/// free (at 100 Hz and nine arenas it is already ~0.22 % of a core, and it grows
/// linearly with shard count).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaSampleRate {
    hz: u32,
}

impl ArenaSampleRate {
    /// The slowest rate the ADR allows.
    pub const MIN_HZ: u32 = 10;
    /// The fastest rate the ADR allows.
    pub const MAX_HZ: u32 = 100;
    /// 20 Hz: a 50 ms-stale upper bound for about 0.045 % of one core at eight
    /// shards (nine arenas × 2.5 µs × 20). The low end of the band buys little
    /// (a 100 ms-stale refusal bound) and the high end costs five times as much
    /// for staleness a broker cannot act on any faster; issue 04's Linux run is
    /// where this gets retuned with real numbers.
    pub const DEFAULT_HZ: u32 = 20;

    /// Nanoseconds one epoch advance costs **per live arena** — the merge is
    /// over every arena, so this is the term that grows with shard count
    /// (spike-report §(a) E5 and its Linux re-run: 3.4 µs on macOS, 13.4 µs on
    /// aarch64 Linux).
    ///
    /// The budget uses the slowest platform measured. Budgeting against the
    /// fastest would leave the slowest silently over budget, and the sampler's
    /// rate has to be something an operator can predict from the shard count
    /// rather than something that shifts with the machine underneath.
    pub const EPOCH_COST_PER_ARENA_NANOS: u64 = 13_400;

    /// The share of one core the sampler may spend on epoch advances: 1 %.
    ///
    /// Expressed in nanoseconds of sampling per wall-clock second so the
    /// derivation stays in integers.
    pub const CPU_BUDGET_NANOS_PER_SEC: u64 = 10_000_000;

    /// Clamp `hz` into the band and build a rate.
    ///
    /// Prefer [`for_arena_count`](Self::for_arena_count) wherever the arena
    /// count is known: this band alone is not shard-count-aware, and at the
    /// ceiling the all-arena epoch merge stops being free well before the
    /// largest shard counts FrogDB supports.
    pub fn new(hz: u32) -> Self {
        Self {
            hz: hz.clamp(Self::MIN_HZ, Self::MAX_HZ),
        }
    }

    /// Clamp `hz` into the band *and* into what the CPU budget affords for
    /// `arenas` arenas.
    ///
    /// Sampling cost is `hz × arenas × EPOCH_COST_PER_ARENA`, so a fixed rate
    /// band alone bounds nothing: at the 100 Hz ceiling the cost passes 1 % of a
    /// core at eight arenas and keeps climbing linearly. This derives the
    /// affordable rate from the arena count instead, and the configured rate is
    /// honoured only where it fits underneath.
    ///
    /// [`MIN_HZ`](Self::MIN_HZ) still wins over the budget. Below it the figure
    /// a memory refusal is written against is more than 100 ms stale, which is a
    /// correctness property of the broker; spending a little over budget is the
    /// cheaper of the two failures, and the arena count where the two meet
    /// (~74 arenas) is far past where the striding above has already flattened
    /// the tick.
    pub fn for_arena_count(hz: u32, arenas: usize) -> Self {
        Self {
            hz: Self::new(hz).hz.min(Self::budget_hz(arenas)),
        }
    }

    /// The highest rate [`CPU_BUDGET_NANOS_PER_SEC`](Self::CPU_BUDGET_NANOS_PER_SEC)
    /// affords for `arenas` arenas, floored at [`MIN_HZ`](Self::MIN_HZ) and
    /// capped at [`MAX_HZ`](Self::MAX_HZ).
    pub fn budget_hz(arenas: usize) -> u32 {
        let arenas = arenas.max(1) as u64;
        let affordable =
            Self::CPU_BUDGET_NANOS_PER_SEC / (arenas * Self::EPOCH_COST_PER_ARENA_NANOS);
        (affordable as u32).clamp(Self::MIN_HZ, Self::MAX_HZ)
    }

    /// The clamped frequency, in Hz.
    pub fn hz(&self) -> u32 {
        self.hz
    }

    /// The interval between ticks.
    pub fn interval(&self) -> Duration {
        Duration::from_micros(1_000_000 / u64::from(self.hz))
    }
}

impl Default for ArenaSampleRate {
    fn default() -> Self {
        Self::new(Self::DEFAULT_HZ)
    }
}

/// The background task that keeps [`ShardArenaRegistry`] current.
pub struct ArenaSampler;

impl ArenaSampler {
    /// Spawn the sampler, or return `None` when there is nothing to sample (no
    /// shard reported an arena — a simulation build, or a target without
    /// jemalloc).
    ///
    /// `rate` should come from
    /// [`ArenaSampleRate::for_arena_count`] so the tick rate is one the arena
    /// count can afford.
    pub fn spawn(
        registry: Arc<ShardArenaRegistry>,
        rate: ArenaSampleRate,
        recorder: Arc<dyn MetricsRecorder>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if registry.is_empty() {
            return None;
        }
        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(rate.interval());
            // A sample is a snapshot of the present, not catch-up work: a
            // delayed tick must produce one fresh reading, not a burst of
            // back-to-back epoch advances.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut tick: u64 = 0;
            loop {
                ticker.tick().await;
                assert_off_shard_thread(&registry, &*recorder);
                registry.refresh_stride(arena_stride(registry.len(), tick));
                tick = tick.wrapping_add(1);
            }
        }))
    }
}

/// Fail fast (debug) or count (release) a sampler tick that woke up on a shard
/// thread.
///
/// The sampler belongs on a utility thread, off the cores the shards are pinned
/// to (issue 08 pins the shards; this is the other half of that bargain). A tick
/// that runs on a shard core steals time from request handling and never shows
/// up as anything but tail latency, so it must not be able to happen quietly.
///
/// Thread affinity itself is the wrong thing to assert: a default deployment
/// leaves the sampler's affinity mask unrestricted, so it *includes* every shard
/// core while the sampler still never runs on one. What matters is where the
/// tick is actually executing, and jemalloc already answers that — a shard
/// thread is exactly a thread bound to one of the registry's arenas
/// (ADR-0006 §3), so an unbound thread, or one bound to the automatic arena, is
/// by construction not a shard.
fn assert_off_shard_thread(registry: &ShardArenaRegistry, recorder: &dyn MetricsRecorder) {
    let Some(arena) = jemalloc::current_thread_arena() else {
        return;
    };
    if !registry.holds_arena(arena) {
        return;
    }
    debug_assert!(
        false,
        "the arena sampler is running on the shard thread bound to arena {arena}; it must run \
         on a utility thread, off the pinned shard cores"
    );
    ArenaSamplerOnShardThreadTotal::inc(recorder);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_registry_reports_nothing_rather_than_zero() {
        let registry = ShardArenaRegistry::empty();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert_eq!(registry.arena_of(0), None);
        assert_eq!(registry.sample(0), None);
        assert_eq!(registry.refresh(), 0);
        assert_eq!(registry.total_allocated_upper_bound_bytes(), 0);
    }

    #[test]
    fn a_shard_without_an_arena_is_absent_not_zero() {
        // Shard 1 has no arena (a sim shard, or a build with no arenas): it is
        // simply not here, so no caller can mistake the automatic arena's bytes
        // for its own.
        let registry = ShardArenaRegistry::new([(0, 7), (2, 9)]);
        assert_eq!(registry.arena_of(0), Some(7));
        assert_eq!(registry.arena_of(1), None);
        assert_eq!(registry.arena_of(2), Some(9));
        assert!(registry.sample(1).is_none());
    }

    #[test]
    fn an_unsampled_slot_says_so_instead_of_reporting_an_empty_shard() {
        let registry = ShardArenaRegistry::new([(0, 7)]);
        let sample = registry.sample(0).expect("shard 0 has an arena");
        assert!(!sample.is_sampled());
        assert_eq!(sample.allocated_upper_bound_bytes(), 0);
        assert_eq!(
            sample.fragmentation_ratio(),
            None,
            "an unsampled slot must not derive a ratio from its placeholder zeros"
        );
    }

    #[test]
    fn fragmentation_is_resident_over_allocated() {
        let registry = ShardArenaRegistry::new([(0, 7)]);
        let slot = &registry.shards[0];
        slot.small.store(300, Ordering::Relaxed);
        slot.large.store(700, Ordering::Relaxed);
        slot.resident.store(1_200, Ordering::Relaxed);
        slot.samples.store(1, Ordering::Relaxed);

        let sample = registry.sample(0).unwrap();
        assert_eq!(sample.allocated_upper_bound_bytes(), 1_000);
        assert_eq!(sample.fragmentation_ratio(), Some(1.2));
        assert_eq!(registry.total_allocated_upper_bound_bytes(), 1_000);
    }

    /// The rate band is ADR-0006 §3's, and out-of-band configuration is clamped
    /// into it rather than honoured — a 1 Hz sampler would hand the broker a
    /// second-stale bound, and a 1 kHz one would spend real CPU on the all-arena
    /// epoch merge.
    #[test]
    fn the_sample_rate_is_clamped_into_the_adrs_band() {
        assert_eq!(ArenaSampleRate::new(0).hz(), ArenaSampleRate::MIN_HZ);
        assert_eq!(ArenaSampleRate::new(1).hz(), ArenaSampleRate::MIN_HZ);
        assert_eq!(ArenaSampleRate::new(50).hz(), 50);
        assert_eq!(ArenaSampleRate::new(10_000).hz(), ArenaSampleRate::MAX_HZ);
        assert_eq!(ArenaSampleRate::default().hz(), ArenaSampleRate::DEFAULT_HZ);
        assert_eq!(
            ArenaSampleRate::new(20).interval(),
            Duration::from_millis(50)
        );
        assert_eq!(
            ArenaSampleRate::new(100).interval(),
            Duration::from_millis(10)
        );
    }

    /// The clamp band alone bounds nothing as shards multiply: cost is
    /// `hz × arenas × epoch-cost`, so the rate has to come down as the arena
    /// count goes up. Mocked arena counts — no allocator involved, this is
    /// arithmetic.
    #[test]
    fn the_sample_rate_is_budgeted_against_the_arena_count() {
        // Small servers are unaffected: the configured rate fits underneath.
        assert_eq!(ArenaSampleRate::for_arena_count(20, 9).hz(), 20);
        assert_eq!(ArenaSampleRate::for_arena_count(20, 32).hz(), 20);

        // At 64 arenas the budget is what decides, not the configuration.
        let at_64 = ArenaSampleRate::for_arena_count(100, 64);
        assert!(
            at_64.hz() < ArenaSampleRate::for_arena_count(100, 32).hz(),
            "doubling the arenas must lower the affordable rate, got {} Hz at 64 and {} Hz at 32",
            at_64.hz(),
            ArenaSampleRate::for_arena_count(100, 32).hz()
        );

        // The spend the budget authorises never exceeds the budget, until the
        // MIN_HZ staleness floor deliberately takes over.
        for arenas in [1usize, 8, 9, 32, 64, 128, 1024] {
            let rate = ArenaSampleRate::for_arena_count(ArenaSampleRate::MAX_HZ, arenas);
            let spend =
                u64::from(rate.hz()) * arenas as u64 * ArenaSampleRate::EPOCH_COST_PER_ARENA_NANOS;
            assert!(
                spend <= ArenaSampleRate::CPU_BUDGET_NANOS_PER_SEC
                    || rate.hz() == ArenaSampleRate::MIN_HZ,
                "{arenas} arenas at {} Hz spends {spend} ns/s, over budget without being at the \
                 staleness floor",
                rate.hz()
            );
            assert!(rate.hz() >= ArenaSampleRate::MIN_HZ);
        }
    }

    /// Past 32 arenas a tick reads a slice, and consecutive ticks cover
    /// everything exactly once — the acceptance criterion's mocked 64.
    #[test]
    fn past_thirty_two_arenas_a_tick_reads_a_stride_not_the_whole_registry() {
        // Up to the cap, every tick is still a full sweep.
        for arenas in [1usize, 8, 32] {
            assert_eq!(sweep_ticks(arenas), 1);
            assert_eq!(arena_stride(arenas, 0), 0..arenas);
            assert_eq!(arena_stride(arenas, 7), 0..arenas);
        }

        assert_eq!(sweep_ticks(64), 2);
        assert_eq!(arena_stride(64, 0), 0..32);
        assert_eq!(arena_stride(64, 1), 32..64);
        assert_eq!(arena_stride(64, 2), 0..32, "the sweep wraps");

        // For any arena count, one sweep of ticks reads every arena exactly
        // once and no tick reads more than the cap.
        for arenas in [0usize, 1, 31, 32, 33, 64, 65, 100, 1000] {
            let mut seen = vec![0u32; arenas];
            for tick in 0..sweep_ticks(arenas) as u64 {
                let stride = arena_stride(arenas, tick);
                assert!(
                    stride.len() <= MAX_ARENAS_PER_TICK,
                    "{arenas} arenas read {} in one tick",
                    stride.len()
                );
                for i in stride {
                    seen[i] += 1;
                }
            }
            assert!(
                seen.iter().all(|&n| n == 1),
                "{arenas} arenas: a sweep must read each arena exactly once, got {seen:?}"
            );
        }
    }

    /// A registry bigger than the cap refreshes in strides, and a full sweep of
    /// ticks leaves every slot sampled.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn a_strided_sweep_eventually_samples_every_shard() {
        let arena = jemalloc::create_arena().expect("arenas.create");
        let bindings: Vec<(usize, u32)> = (0..40).map(|shard| (shard, arena)).collect();
        let registry = ShardArenaRegistry::new(bindings);

        assert_eq!(sweep_ticks(registry.len()), 2);
        let first = registry.refresh_stride(arena_stride(registry.len(), 0));
        assert_eq!(first, 20, "the first tick reads half the registry");
        assert_eq!(
            registry.samples().filter(|s| s.is_sampled()).count(),
            20,
            "the shards this tick skipped must still report as unsampled rather than as zero"
        );

        registry.refresh_stride(arena_stride(registry.len(), 1));
        assert!(
            registry.samples().all(|s| s.is_sampled()),
            "a full sweep must leave no shard unsampled"
        );
    }

    /// The sampler on a utility thread is the normal case, and it must not
    /// trip the guard — the release-build counter stays at zero and the debug
    /// assertion does not fire.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn the_guard_passes_on_a_thread_that_is_not_a_shard() {
        use crate::prometheus_recorder::PrometheusRecorder;

        let shard_arena = jemalloc::create_arena().expect("arenas.create");
        let registry = ShardArenaRegistry::new([(0, shard_arena)]);
        let recorder = PrometheusRecorder::new();

        // This thread is not bound to the shard's arena, which is exactly what
        // a utility thread looks like.
        assert_off_shard_thread(&registry, &recorder);
        assert!(
            !recorder
                .encode()
                .contains("frogdb_arena_sampler_on_shard_thread_total"),
            "a utility thread must not be reported as running on a shard core"
        );
    }

    /// And the guard actually catches the condition it exists for: a thread
    /// bound to a shard's arena *is* that shard's thread.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn the_guard_catches_a_tick_running_on_a_shard_thread() {
        use crate::prometheus_recorder::PrometheusRecorder;

        let shard_arena = jemalloc::create_arena().expect("arenas.create");
        let registry = ShardArenaRegistry::new([(0, shard_arena)]);
        let recorder = PrometheusRecorder::new();

        std::thread::spawn(move || {
            jemalloc::bind_current_thread_to_arena(shard_arena).expect("thread.arena");
            assert!(
                registry.holds_arena(jemalloc::current_thread_arena().expect("bound")),
                "a thread bound to a shard's arena must be recognised as that shard's thread"
            );
            // The release path is the counter; the debug path is an assertion,
            // so only the recognition above is exercised in both.
            #[cfg(not(debug_assertions))]
            {
                assert_off_shard_thread(&registry, &recorder);
                assert!(
                    recorder
                        .encode()
                        .contains("frogdb_arena_sampler_on_shard_thread_total 1")
                );
            }
            #[cfg(debug_assertions)]
            let _ = &recorder;
        })
        .join()
        .expect("shard-shaped thread");
    }

    #[tokio::test]
    async fn the_sampler_declines_to_run_when_there_is_nothing_to_sample() {
        let registry = Arc::new(ShardArenaRegistry::empty());
        assert!(
            ArenaSampler::spawn(
                registry,
                ArenaSampleRate::default(),
                Arc::new(frogdb_core::NoopMetricsRecorder::new())
            )
            .is_none(),
            "a simulation build has no arenas; spawning a ticking task to read \
             none of them is pure overhead"
        );
    }

    /// End to end on a real thread: a shard-shaped thread binds its arena, keeps
    /// a known volume live, and the sampler's tick makes that volume visible
    /// through the registry. This is the shape `frogdb-server` wires.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg(not(target_env = "msvc"))]
    async fn the_sampler_makes_a_bound_threads_bytes_visible_per_shard() {
        const CHUNK: usize = 64 * 1024;
        const CHUNKS: usize = 128; // 8 MiB
        let volume = (CHUNK * CHUNKS) as u64;

        let (arena_tx, arena_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let shard = std::thread::spawn(move || {
            let arena = jemalloc::create_arena().expect("arenas.create");
            jemalloc::bind_current_thread_to_arena(arena).expect("thread.arena");
            let held: Vec<Vec<u8>> = (0..CHUNKS).map(|_| vec![1u8; CHUNK]).collect();
            arena_tx.send(arena).expect("report arena");
            let _ = stop_rx.recv();
            drop(held);
        });

        let arena = arena_rx.recv().expect("shard reported its arena");
        let registry = Arc::new(ShardArenaRegistry::new([(0, arena)]));
        let handle = ArenaSampler::spawn(
            registry.clone(),
            ArenaSampleRate::for_arena_count(100, 1),
            Arc::new(frogdb_core::NoopMetricsRecorder::new()),
        )
        .expect("a registry with an arena spawns a sampler");

        // Two intervals is enough for at least one tick to have landed.
        tokio::time::sleep(Duration::from_millis(60)).await;
        handle.abort();

        let sample = registry.sample(0).expect("shard 0 has an arena");
        let _ = stop_tx.send(());
        shard.join().expect("shard thread");

        assert!(sample.is_sampled(), "the sampler never filled the slot");
        assert!(
            sample.allocated_upper_bound_bytes() >= volume,
            "shard 0 held {volume} B live but its arena reports {} B",
            sample.allocated_upper_bound_bytes()
        );
        assert!(
            sample.resident_bytes >= sample.allocated_upper_bound_bytes(),
            "resident ({}) must cover allocated ({})",
            sample.resident_bytes,
            sample.allocated_upper_bound_bytes()
        );
        assert!(
            sample.fragmentation_ratio().is_some_and(|r| r >= 1.0),
            "per-shard fragmentation should be a ratio at or above 1.0, got {:?}",
            sample.fragmentation_ratio()
        );
    }

    /// PRD R3's no-cross-arena-bleed rule, in executable form: several
    /// shard-shaped threads each hold a *different* known volume, and every
    /// arena must report its own thread's bytes and none of anyone else's.
    ///
    /// The volumes are spread far apart on purpose. An arena that had absorbed
    /// even one neighbour's allocation would land outside its own window, so
    /// the assertion fails on bleed in either direction rather than merely
    /// noticing that some plausible number arrived.
    ///
    /// Real threads, not turmoil: under simulation every shard allocates from
    /// one thread's arena and this invariant is vacuously true — which is why
    /// [`specs/memory.md`] forbids forcing it from a simulated test.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn each_arena_reports_its_own_shards_bytes_and_no_others() {
        const MIB: usize = 1024 * 1024;
        /// Room for the holding vectors and whatever else the thread touches
        /// after it binds. Far below the gap between two shards' volumes, so a
        /// single leaked neighbour allocation still fails the window.
        const SLACK: u64 = 2 * MIB as u64;
        let volumes = [4 * MIB, 16 * MIB, 48 * MIB];

        let (report_tx, report_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let stop_rx = Arc::new(std::sync::Mutex::new(stop_rx));

        let shards: Vec<_> = volumes
            .iter()
            .enumerate()
            .map(|(shard_id, &volume)| {
                let report_tx = report_tx.clone();
                let stop_rx = stop_rx.clone();
                std::thread::spawn(move || {
                    let arena = jemalloc::create_arena().expect("arenas.create");
                    jemalloc::bind_current_thread_to_arena(arena).expect("thread.arena");
                    let held: Vec<Vec<u8>> = (0..volume / MIB).map(|_| vec![7u8; MIB]).collect();
                    report_tx.send((shard_id, arena)).expect("report arena");
                    // Freed-into-cache objects still count against the arena
                    // (E4), so the thread holds everything live and flushes
                    // nothing until the reads are done.
                    let _ = stop_rx.lock().expect("stop channel").recv();
                    drop(held);
                })
            })
            .collect();
        drop(report_tx);

        let bindings: Vec<(usize, u32)> = report_rx.iter().take(volumes.len()).collect();
        assert_eq!(
            bindings.len(),
            volumes.len(),
            "every shard reported an arena"
        );
        let registry = ShardArenaRegistry::new(bindings);
        assert_eq!(registry.refresh(), volumes.len(), "every arena was read");

        let samples: Vec<_> = registry.samples().collect();
        for _ in &shards {
            let _ = stop_tx.send(());
        }
        for shard in shards {
            shard.join().expect("shard thread");
        }

        let arenas: std::collections::HashSet<u32> = samples.iter().map(|s| s.arena).collect();
        assert_eq!(
            arenas.len(),
            volumes.len(),
            "shards shared an arena: {samples:?}"
        );

        for sample in &samples {
            let own = volumes[sample.shard_id] as u64;
            let allocated = sample.allocated_upper_bound_bytes();
            assert!(
                allocated >= own,
                "shard {} held {own} B but its arena reports only {allocated} B",
                sample.shard_id
            );
            assert!(
                allocated <= own + SLACK,
                "shard {} held {own} B and its arena reports {allocated} B — \
                 another shard's bytes were charged here",
                sample.shard_id
            );
        }
    }

    /// The acceptance criterion "the sampler never runs on a command path",
    /// pinned structurally rather than by inspection.
    ///
    /// Everything that makes arena statistics move goes through
    /// [`ShardArenaRegistry::refresh`], which is the only caller of
    /// [`jemalloc::advance_epoch`]; this scans the server's crates and fails if
    /// a second caller appears anywhere. The pre-existing process-wide
    /// `jemalloc::read_stats` — which does advance the epoch, and which
    /// `INFO memory` has always called — is a different function and is
    /// deliberately out of scope: this issue changed neither its behavior nor
    /// its callers.
    #[test]
    fn arena_sampling_is_not_on_a_command_path() {
        let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates directory")
            .to_path_buf();

        let mut callers = Vec::new();
        let mut stack = vec![crates.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("readable crate directory") {
                let path = entry.expect("directory entry").path();
                if path.is_dir() {
                    if path.file_name().is_some_and(|n| n == "target") {
                        continue;
                    }
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let body = std::fs::read_to_string(&path).expect("readable source file");
                    if body.contains("advance_epoch(") {
                        callers.push(
                            path.strip_prefix(&crates)
                                .unwrap_or(&path)
                                .display()
                                .to_string(),
                        );
                    }
                }
            }
        }
        callers.sort();
        assert_eq!(
            callers,
            vec![
                // The definition and its own tests.
                "telemetry/src/jemalloc.rs".to_string(),
                // The sampler, on its 10–100 Hz tick.
                "telemetry/src/shard_arenas.rs".to_string(),
            ],
            "epoch advances must stay in the sampler: an advance merges every \
             arena at ~2.5 µs each, so a command path that reached it would pay \
             an allocator-wide operation per request"
        );
    }
}
