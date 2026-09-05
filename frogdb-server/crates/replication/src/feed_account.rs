//! The memory-accounting seam for the primary→replica feed.
//!
//! A replica feed holds replica-bound bytes in the primary's memory in three
//! places, and only three: the live-dataset blobs staged for a full sync, the
//! backlog handoff tail materialized when streaming starts, and the frames
//! [`crate::FeedSequencer`] holds behind an armed slot-handoff barrier. Those
//! bytes cost the primary exactly as much as a client's queued replies do, and
//! Redis governs them with the same knob — `client-output-buffer-limit slave`.
//!
//! This module is how the feed reports them without knowing any of that. The
//! replication crate owns *which* bytes are feed bytes and what a shed verdict
//! does to the link; the server owns the budget they are charged to, the class
//! limits they are judged against, the clock the soft window runs on and the
//! `omem` an operator reads. The whole of the contract between the two is
//! [`FeedOutputAccount`]: one absolute figure out, one [`FeedVerdict`] back.
//!
//! Reporting an absolute total rather than a delta is deliberate and matches
//! the connection-side seam it is implemented over: a missed report costs one
//! stale figure, where a missed decrement in matched `+`/`-` bookkeeping would
//! drift the charge upward for the life of the process.
//!
//! See `specs/replication.md` FM-REPLICATION-069.

use std::fmt;
use std::sync::Arc;

/// What the account says about a feed that has just reported its size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "an unread verdict is a client-output-buffer-limit that does not exist"]
pub enum FeedVerdict {
    /// The feed is within its limits. Carry on.
    Keep,
    /// The feed is over its limits and must be dropped now. `reason` is the
    /// server's own shed-reason label (`"hard_limit"`, `"soft_limit"`,
    /// `"budget_refused"`), carried through for the log line rather than
    /// re-derived here — the replication crate does not model the classes.
    Shed { reason: &'static str },
}

/// The bytes a replica feed is holding, reported to whoever is accounting for
/// them.
///
/// Implemented by the server over the very `OutputBufferAccount` the client
/// connection held before `PSYNC`, so the charge crosses the handoff without a
/// gap and the class policy has one implementation. Implemented in this crate's
/// tests by a recorder, so the seam's own behaviour is forced without a server.
pub trait FeedOutputAccount: fmt::Debug + Send + Sync {
    /// Report the total replica-bound payload bytes this feed currently holds
    /// in memory — an absolute figure, not a delta, and zero when it holds
    /// nothing.
    fn set_buffered(&self, total_bytes: u64) -> FeedVerdict;

    /// The feed is over. Release everything it was charged.
    ///
    /// Called once, from the session's single exit path, on every way a link
    /// can end.
    fn release(&self);
}

/// A [`FeedOutputAccount`] as the session carries it: one account, shared
/// between the driver and the spawned write task.
pub type SharedFeedAccount = Arc<dyn FeedOutputAccount>;

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::Mutex;

    /// A recording account: remembers every figure reported to it, and sheds on
    /// demand.
    ///
    /// `shed_at` is the figure at or above which it starts refusing, standing
    /// in for a class limit; `shed_after` is how many reports it lets past
    /// before it does, which is how the soft window's "not the first crossing"
    /// shape is forced without a clock.
    #[derive(Debug, Default)]
    pub(crate) struct RecordingFeedAccount {
        state: Mutex<RecordingState>,
        shed_at: u64,
        shed_after: usize,
        reason: &'static str,
    }

    #[derive(Debug, Default)]
    struct RecordingState {
        reports: Vec<u64>,
        over_limit_reports: usize,
        released: usize,
    }

    impl RecordingFeedAccount {
        /// An account that never sheds — for asserting on what gets charged.
        pub(crate) fn unlimited() -> Arc<Self> {
            Arc::new(Self {
                shed_at: u64::MAX,
                ..Self::default()
            })
        }

        /// Sheds `reason` the first time a report reaches `shed_at`.
        pub(crate) fn shedding_at(shed_at: u64, reason: &'static str) -> Arc<Self> {
            Arc::new(Self {
                shed_at,
                shed_after: 0,
                reason,
                ..Self::default()
            })
        }

        /// Sheds `reason` once `shed_after` reports have already been at or
        /// above `shed_at` — the sampled soft window, without a clock.
        pub(crate) fn shedding_after(
            shed_at: u64,
            shed_after: usize,
            reason: &'static str,
        ) -> Arc<Self> {
            Arc::new(Self {
                shed_at,
                shed_after,
                reason,
                ..Self::default()
            })
        }

        /// Every figure reported, in order.
        pub(crate) fn reports(&self) -> Vec<u64> {
            self.state.lock().unwrap().reports.clone()
        }

        /// The largest figure reported, or 0 if nothing was.
        pub(crate) fn peak(&self) -> u64 {
            self.reports().into_iter().max().unwrap_or(0)
        }

        /// How many times the feed released its charge.
        pub(crate) fn releases(&self) -> usize {
            self.state.lock().unwrap().released
        }
    }

    impl FeedOutputAccount for RecordingFeedAccount {
        fn set_buffered(&self, total_bytes: u64) -> FeedVerdict {
            let mut state = self.state.lock().unwrap();
            state.reports.push(total_bytes);
            if total_bytes < self.shed_at {
                return FeedVerdict::Keep;
            }
            state.over_limit_reports += 1;
            if state.over_limit_reports > self.shed_after {
                FeedVerdict::Shed {
                    reason: self.reason,
                }
            } else {
                FeedVerdict::Keep
            }
        }

        fn release(&self) {
            self.state.lock().unwrap().released += 1;
        }
    }
}
