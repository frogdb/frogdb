use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// Number of 1-second buckets to keep for operation tracking (60 seconds).
const OPERATION_BUCKET_COUNT: usize = 60;

/// A single 1-second bucket of operation counts.
#[derive(Debug, Clone, Copy, Default)]
pub struct OperationBucket {
    /// The second this bucket covers, counted monotonically from the counter's
    /// construction. `None` = never written: the ring is not yet full, or a
    /// long idle gap cleared it.
    pub second: Option<u64>,
    /// Total operations in this second.
    pub total_ops: u64,
    /// Read operations in this second.
    pub read_ops: u64,
    /// Write operations in this second.
    pub write_ops: u64,
}

/// How one dispatched operation is classified for the hot-shard windows.
///
/// The single classifier for *every* dispatch path. Before this existed, the
/// single-shard path counted reads from the `READONLY` flag while the
/// cross-shard scatter path counted "anything that is not a write" as a read,
/// so the same logical operation could land in different columns depending on
/// how it was routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OpClass {
    /// Whether the operation declares itself a read (`READONLY`).
    pub read: bool,
    /// Whether the operation declares itself a write (`WRITE`).
    pub write: bool,
}

impl OpClass {
    /// Classify from the command spec's own declarations.
    ///
    /// `READONLY` is the spec's read declaration and `WRITE` its write
    /// declaration; a command that declares neither (an admin or connection
    /// command) still counts toward `total_ops` without inflating either
    /// column. Scatter parts reach this through
    /// [`crate::shard::ScatterOp::command_flags`], so both paths share one rule.
    pub fn from_flags(flags: crate::command::CommandFlags) -> Self {
        Self {
            read: flags.contains(crate::command::CommandFlags::READONLY),
            write: flags.contains(crate::command::CommandFlags::WRITE),
        }
    }
}

/// Windowed operation counters for hot shard detection.
///
/// A ring of 1-second buckets driven off a **monotonic** baseline
/// ([`Instant`]) captured at construction, not off the wall clock. Wall-clock
/// time is not usable here: a node that boots near the epoch and then takes an
/// NTP step forward would ask this ring to advance ~10^9 buckets one second at
/// a time (stalling the shard event loop that owns it), and a backward step
/// would shrink the window and inflate every reported rate. `Instant` cannot
/// step in either direction.
///
/// Catch-up is bounded regardless: a gap of at least the bucket count means
/// every retained bucket is stale, so the ring is cleared and the current
/// second jumps directly, in O(bucket count) rather than O(gap).
#[derive(Debug)]
pub struct OperationCounters {
    /// Ring buffer of 1-second buckets.
    buckets: [OperationBucket; OPERATION_BUCKET_COUNT],
    /// Current bucket index.
    current_index: usize,
    /// The monotonic second (since [`Self::epoch`]) the current bucket covers.
    current_second: u64,
    /// Monotonic baseline: second 0 of this counter.
    epoch: Instant,
    /// Earliest second for which the ring still holds data. Advances on a
    /// clear-and-jump, so rate divisors never claim seconds of coverage the
    /// ring does not have.
    data_start_second: u64,
    /// Live `hotshards-enabled` kill switch, shared with `ConfigManager`. When
    /// false, [`Self::record_op`] does nothing and reporting stays silent
    /// rather than serving whatever the ring held when tracking was turned off.
    enabled: Arc<AtomicBool>,
    /// Whether recording was skipped since the last recorded op, so a
    /// re-enable can drop the pre-disable window instead of blending it with
    /// fresh data.
    missed_ops_while_disabled: bool,
    /// Test-only bias added to the monotonic elapsed seconds, so the suite can
    /// simulate long gaps and clock jumps without sleeping. Absent in
    /// production builds.
    #[cfg(test)]
    clock_bias_secs: u64,
}

impl Default for OperationCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationCounters {
    /// The longest window this ring can answer for, in seconds. A request for
    /// more is clamped to this, and the clamped value is what every hot-shard
    /// surface reports.
    pub const MAX_WINDOW_SECS: u64 = OPERATION_BUCKET_COUNT as u64;

    /// Create new operation counters.
    pub fn new() -> Self {
        let mut buckets = [OperationBucket::default(); OPERATION_BUCKET_COUNT];
        buckets[0].second = Some(0);
        Self {
            buckets,
            current_index: 0,
            current_second: 0,
            epoch: crate::clock::now(),
            data_start_second: 0,
            enabled: Arc::new(AtomicBool::new(true)),
            missed_ops_while_disabled: false,
            #[cfg(test)]
            clock_bias_secs: 0,
        }
    }

    /// Adopt the shared `hotshards-enabled` flag so `CONFIG SET
    /// hotshards-enabled no` stops this shard's accounting from the next
    /// dispatched command.
    pub fn set_enabled_flag(&mut self, enabled: Arc<AtomicBool>) {
        self.enabled = enabled;
    }

    /// Whether hot-shard accounting is currently switched on.
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Seconds elapsed on the monotonic baseline.
    fn elapsed_secs(&self) -> u64 {
        let base = self.epoch.elapsed().as_secs();
        #[cfg(test)]
        let base = base + self.clock_bias_secs;
        base
    }

    /// Push the monotonic baseline `secs` into the past (tests only).
    #[cfg(test)]
    fn advance_clock_for_test(&mut self, secs: u64) {
        self.clock_bias_secs += secs;
    }

    /// Drop every bucket and restart the window at `second`.
    fn clear(&mut self, second: u64) {
        self.buckets = [OperationBucket::default(); OPERATION_BUCKET_COUNT];
        self.current_index = 0;
        self.current_second = second;
        self.data_start_second = second;
        self.buckets[0].second = Some(second);
    }

    /// Advance to the current second, rolling over buckets as needed.
    fn advance_to_now(&mut self) {
        let now = self.elapsed_secs();
        // `Instant` is monotonic, so `now` can only move forward.
        let gap = now.saturating_sub(self.current_second);
        if gap == 0 {
            return;
        }

        // Idle (or stalled) for at least the whole window: every retained
        // bucket has already fallen out of it, so clear and jump instead of
        // stepping second by second.
        if gap >= Self::MAX_WINDOW_SECS {
            self.clear(now);
            return;
        }

        for _ in 0..gap {
            self.current_second += 1;
            self.current_index = (self.current_index + 1) % OPERATION_BUCKET_COUNT;
            self.buckets[self.current_index] = OperationBucket {
                second: Some(self.current_second),
                ..Default::default()
            };
        }
        // The ring only ever holds the last `MAX_WINDOW_SECS` seconds.
        self.data_start_second = self.data_start_second.max(
            self.current_second
                .saturating_sub(Self::MAX_WINDOW_SECS - 1),
        );
    }

    /// Record an operation.
    pub fn record_op(&mut self, class: OpClass) {
        // Kill switch first: nothing below may cost a disabled node anything.
        if !self.enabled() {
            self.missed_ops_while_disabled = true;
            return;
        }
        // Re-enabled after dropping ops: the retained buckets describe a window
        // with holes in it, so start a fresh window rather than report a rate
        // computed over seconds whose ops were never counted.
        if self.missed_ops_while_disabled {
            self.missed_ops_while_disabled = false;
            let now = self.elapsed_secs();
            self.clear(now);
        }

        self.advance_to_now();
        let bucket = &mut self.buckets[self.current_index];
        bucket.total_ops += 1;
        if class.read {
            bucket.read_ops += 1;
        }
        if class.write {
            bucket.write_ops += 1;
        }
    }

    /// Calculate ops/sec over the requested period, clamped to
    /// [`Self::MAX_WINDOW_SECS`].
    ///
    /// Returns (total_ops_per_sec, read_ops_per_sec, write_ops_per_sec), or all
    /// zeros while the kill switch is off (nothing is being recorded, so the
    /// ring's contents would describe a stale window).
    pub fn calculate_ops_per_sec(&mut self, period_secs: u64) -> (f64, f64, f64) {
        if !self.enabled() {
            return (0.0, 0.0, 0.0);
        }

        self.advance_to_now();

        let period_secs = period_secs.min(Self::MAX_WINDOW_SECS);
        if period_secs == 0 {
            return (0.0, 0.0, 0.0);
        }

        let now = self.current_second;

        let mut total_ops: u64 = 0;
        let mut read_ops: u64 = 0;
        let mut write_ops: u64 = 0;

        for bucket in &self.buckets {
            let Some(second) = bucket.second else {
                continue;
            };
            // The window is the `period_secs` seconds ending at `now`,
            // inclusive of both ends (second 0 of a freshly started counter is
            // in-window, so its ops are not silently dropped).
            if second <= now && now - second < period_secs {
                total_ops += bucket.total_ops;
                read_ops += bucket.read_ops;
                write_ops += bucket.write_ops;
            }
        }

        // Divide by the seconds of data the ring actually holds, not by the
        // requested window: a 60s query 3 seconds after start covers 3 seconds
        // of traffic, and dividing it by 60 would under-report real load by
        // 20x — exactly when an operator is looking for a hot shard.
        let divisor = period_secs.min(now - self.data_start_second + 1) as f64;
        (
            total_ops as f64 / divisor,
            read_ops as f64 / divisor,
            write_ops as f64 / divisor,
        )
    }
}

/// Response for hot shard stats query.
#[derive(Debug, Clone)]
pub struct HotShardStatsResponse {
    /// Shard ID.
    pub shard_id: usize,
    /// Total operations per second.
    pub ops_per_sec: f64,
    /// Read operations per second.
    pub reads_per_sec: f64,
    /// Write operations per second.
    pub writes_per_sec: f64,
    /// Current queue depth.
    pub queue_depth: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandFlags;

    fn read() -> OpClass {
        OpClass::from_flags(CommandFlags::READONLY)
    }

    fn write() -> OpClass {
        OpClass::from_flags(CommandFlags::WRITE)
    }

    #[test]
    fn classifier_reads_the_spec_declarations() {
        assert_eq!(
            read(),
            OpClass {
                read: true,
                write: false
            }
        );
        assert_eq!(
            write(),
            OpClass {
                read: false,
                write: true
            }
        );
        // Neither declared (an admin command): counted, but in no column.
        assert_eq!(
            OpClass::from_flags(CommandFlags::ADMIN),
            OpClass {
                read: false,
                write: false
            }
        );
    }

    #[test]
    fn rates_use_observed_seconds_not_the_requested_window() {
        let mut counters = OperationCounters::new();
        for _ in 0..10 {
            counters.record_op(read());
        }
        // A 60s window queried immediately must not divide 10 ops by 60.
        let (total, reads, writes) = counters.calculate_ops_per_sec(60);
        assert_eq!(total, 10.0);
        assert_eq!(reads, 10.0);
        assert_eq!(writes, 0.0);
    }

    #[test]
    fn window_is_clamped_to_the_ring_size() {
        let mut counters = OperationCounters::new();
        counters.record_op(write());
        // A request far beyond the ring answers over the ring, not over the
        // request; the same clamp the reported `period_secs` must show.
        assert_eq!(OperationCounters::MAX_WINDOW_SECS, 60);
        let (total, _, writes) = counters.calculate_ops_per_sec(86_400);
        assert_eq!(total, 1.0);
        assert_eq!(writes, 1.0);
    }

    #[test]
    fn zero_period_reports_nothing() {
        let mut counters = OperationCounters::new();
        counters.record_op(read());
        assert_eq!(counters.calculate_ops_per_sec(0), (0.0, 0.0, 0.0));
    }

    /// A forward jump larger than the ring must clear and jump, not step one
    /// bucket at a time. The wall-clock version of this loop ran ~10^9 times
    /// after an NTP correction on a node that booted near the epoch.
    #[test]
    fn huge_forward_gap_clears_and_jumps() {
        let mut counters = OperationCounters::new();
        counters.record_op(read());

        // Simulate the jump by moving the monotonic baseline into the past.
        counters.advance_clock_for_test(1_000_000);
        counters.record_op(write());

        assert_eq!(counters.current_second, counters.elapsed_secs());
        assert_eq!(counters.data_start_second, counters.current_second);
        // The pre-jump read is gone (it is far outside the window), and the
        // post-jump write is the only thing in it.
        let (total, reads, writes) = counters.calculate_ops_per_sec(60);
        assert_eq!((total, reads, writes), (1.0, 0.0, 1.0));
    }

    #[test]
    fn disabled_records_nothing_and_reports_nothing() {
        let flag = Arc::new(AtomicBool::new(true));
        let mut counters = OperationCounters::new();
        counters.set_enabled_flag(flag.clone());

        counters.record_op(read());
        assert_eq!(counters.calculate_ops_per_sec(10).0, 1.0);

        // CONFIG SET hotshards-enabled no
        flag.store(false, Ordering::Relaxed);
        counters.record_op(read());
        counters.record_op(write());
        assert_eq!(
            counters.calculate_ops_per_sec(10),
            (0.0, 0.0, 0.0),
            "a disabled collector must not serve the pre-disable window"
        );

        // ...and back on: the window starts fresh, so the ops dropped while
        // disabled are not implied by a rate computed over their seconds.
        flag.store(true, Ordering::Relaxed);
        counters.record_op(write());
        let (total, reads, writes) = counters.calculate_ops_per_sec(10);
        assert_eq!(
            (total, reads, writes),
            (1.0, 0.0, 1.0),
            "re-enabling must drop the stale window, keeping only fresh ops"
        );
    }

    #[test]
    fn buckets_roll_over_within_the_window() {
        let mut counters = OperationCounters::new();
        counters.record_op(read());
        // Two seconds later, in the same window: both seconds count, and the
        // divisor is the observed span (3 seconds: 0, 1, 2).
        counters.advance_clock_for_test(2);
        counters.record_op(read());
        let (total, reads, _) = counters.calculate_ops_per_sec(60);
        assert!((total - 2.0 / 3.0).abs() < 1e-9, "got {total}");
        assert!((reads - 2.0 / 3.0).abs() < 1e-9, "got {reads}");
    }
}
