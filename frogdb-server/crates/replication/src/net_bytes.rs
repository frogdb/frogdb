//! Lifetime tallies of replication wire bytes — Redis's connection-layer
//! `total_net_repl_input_bytes` / `total_net_repl_output_bytes`: every byte
//! read from, or written to, a link this node holds with a replica or with its
//! own primary.
//!
//! Two lanes feed each counter, and both must be counted or the total is a
//! confident undercount rather than an honest one (hardening issue 29):
//!
//! - The frame stream — the primary's broadcast fan-out, decoded on the
//!   replica by [`crate::frame::ReplicationFrameCodec`].
//! - The full-sync payload — a checkpoint or live-dataset transfer, which does
//!   not travel through the frame codec at all. Summing only the frame lane
//!   would undercount every resync by the size of the dataset: a smaller
//!   number than `0`, and a more misleading one, because it looks like real
//!   traffic.
//!
//! `INFO stats` reports the sum of both lanes; nothing here distinguishes them
//! because Redis's fields do not either.

use std::sync::atomic::{AtomicU64, Ordering};

/// The live counters, owned by the replication tracker so both `INFO`
/// renderers read one source (FM-REPLICATION-050).
#[derive(Debug, Default)]
pub struct NetByteCounters {
    output: AtomicU64,
    input: AtomicU64,
}

impl NetByteCounters {
    /// Record bytes actually written to a replication link: a forwarded
    /// frame's encoded length, or a full-sync payload's real, checksum-verified
    /// size — never a value derived from something else the payload might have
    /// been.
    ///
    /// A `0` record is a no-op rather than a wasted atomic op — every call site
    /// that hands this a size already knows whether it has one.
    pub fn record_output(&self, bytes: u64) {
        if bytes != 0 {
            self.output.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record bytes actually read off a replication link: frame bytes decoded
    /// from the stream, or a received (checksum-verified) full-sync payload's
    /// real size.
    pub fn record_input(&self, bytes: u64) {
        if bytes != 0 {
            self.input.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Read both counters as one pair, for `INFO`.
    ///
    /// Two independent `Relaxed` loads, not an atomic snapshot — the same
    /// tearing tradeoff [`crate::sync_counters::SyncCounters::snapshot`]
    /// documents, and acceptable for the same reason: both are monotone
    /// (outside [`Self::reset`]) diagnostic counters nothing derives a
    /// decision from.
    pub fn snapshot(&self) -> NetByteCountersSnapshot {
        NetByteCountersSnapshot {
            output: self.output.load(Ordering::Relaxed),
            input: self.input.load(Ordering::Relaxed),
        }
    }

    /// Zero both counters — what `CONFIG RESETSTAT` does to them
    /// (FM-REPLICATION-058). The only operation that moves them backwards;
    /// absent it they are monotone.
    pub fn reset(&self) {
        self.output.store(0, Ordering::Relaxed);
        self.input.store(0, Ordering::Relaxed);
    }
}

/// One read of [`NetByteCounters`], as `INFO` reports them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NetByteCountersSnapshot {
    /// Lifetime replication bytes sent (`total_net_repl_output_bytes`).
    pub output: u64,
    /// Lifetime replication bytes received (`total_net_repl_input_bytes`).
    pub input: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero() {
        let counters = NetByteCounters::default();
        assert_eq!(counters.snapshot(), NetByteCountersSnapshot::default());
    }

    #[test]
    fn recording_output_moves_only_output() {
        let counters = NetByteCounters::default();
        counters.record_output(100);
        assert_eq!(
            counters.snapshot(),
            NetByteCountersSnapshot {
                output: 100,
                input: 0
            }
        );
    }

    #[test]
    fn recording_input_moves_only_input() {
        let counters = NetByteCounters::default();
        counters.record_input(250);
        assert_eq!(
            counters.snapshot(),
            NetByteCountersSnapshot {
                output: 0,
                input: 250
            }
        );
    }

    #[test]
    fn records_accumulate_across_calls() {
        let counters = NetByteCounters::default();
        counters.record_output(10);
        counters.record_output(20);
        counters.record_input(5);
        assert_eq!(
            counters.snapshot(),
            NetByteCountersSnapshot {
                output: 30,
                input: 5
            }
        );
    }

    #[test]
    fn a_zero_record_is_a_no_op() {
        let counters = NetByteCounters::default();
        counters.record_output(0);
        counters.record_input(0);
        assert_eq!(counters.snapshot(), NetByteCountersSnapshot::default());
    }

    #[test]
    fn reset_zeroes_both_counters_and_counting_resumes_from_zero() {
        let counters = NetByteCounters::default();
        counters.record_output(100);
        counters.record_input(50);
        counters.reset();
        assert_eq!(counters.snapshot(), NetByteCountersSnapshot::default());

        counters.record_input(7);
        assert_eq!(
            counters.snapshot(),
            NetByteCountersSnapshot {
                output: 0,
                input: 7
            }
        );
    }
}
