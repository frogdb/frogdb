//! Redis's three PSYNC-outcome counters: `sync_full`, `sync_partial_ok` and
//! `sync_partial_err`.
//!
//! These are the fields an operator reads to tell "replicas are resyncing
//! constantly" from "replication is healthy": `sync_full` climbing means
//! replicas keep falling out of the backlog and paying for full checkpoint
//! transfers, `sync_partial_err` climbing means partial resync is being
//! *attempted* and refused. The primary already resolves every `PSYNC` to
//! exactly one arm (FM-REPLICATION-013); this module is the counter behind that
//! fork.

use std::sync::atomic::{AtomicU64, Ordering};

/// Which of the three counters one resolved `PSYNC` moves.
///
/// Total over the fork: a `PSYNC` either got `+CONTINUE` or `+FULLRESYNC`, and
/// a `+FULLRESYNC` either followed a partial attempt or did not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncOutcome {
    /// `+CONTINUE` — the partial resync was granted *and served*: recorded by
    /// the streaming session once the backlog tail has been extracted, not at
    /// the fork, because the window can close in between. Moves
    /// `sync_partial_ok`.
    PartialOk,
    /// `+FULLRESYNC` for a replica that asked for one outright (`PSYNC ? …`).
    /// Moves `sync_full` only: no partial resync was attempted, so nothing was
    /// refused.
    FullResyncRequested,
    /// `+FULLRESYNC` after a partial resync was attempted and refused. Moves
    /// `sync_partial_err` **and** `sync_full`, because the refusal falls
    /// through to a full resync — Redis's `syncCommand` increments both in
    /// exactly this order.
    PartialRefused,
}

/// The replid a replica sends when it wants a full resync outright.
const FORCE_FULL_RESYNC_ID: &str = "?";

impl SyncOutcome {
    /// Classify a resolved `PSYNC`.
    ///
    /// `requested_id` is the replication id the replica sent; `granted_partial`
    /// is whether the decision was `+CONTINUE`.
    ///
    /// The fork calls this with `false` only: a grant is recorded later, by the
    /// streaming session, once the tail it promised exists. The `true` arm is
    /// kept because this is the statement of Redis's rule, and the rule is that
    /// a grant is `PartialOk` whatever id was sent — the streamer relies on
    /// exactly that when it records `PartialOk` without consulting the id.
    ///
    /// "Did the replica attempt a partial resync" is decided by the `?`
    /// sentinel, exactly as Redis decides it (`if (master_replid[0] != '?')
    /// server.stat_sync_partial_err++`), and deliberately **not** by the
    /// [`FullResyncReason`]: the backlog-disabled check runs ahead of the
    /// sentinel check in `PartialSyncReplay::can_replay`, so a first attach
    /// against a disabled backlog is classified `Disabled` even though that
    /// replica never asked for a partial. Keying off the reason would count it
    /// as a refusal.
    ///
    /// [`FullResyncReason`]: crate::primary::FullResyncReason
    pub fn classify(requested_id: &str, granted_partial: bool) -> Self {
        if granted_partial {
            Self::PartialOk
        } else if requested_id == FORCE_FULL_RESYNC_ID {
            Self::FullResyncRequested
        } else {
            Self::PartialRefused
        }
    }
}

/// The live counters, owned by the replication tracker so both `INFO`
/// renderers read one source.
#[derive(Debug, Default)]
pub struct SyncCounters {
    full: AtomicU64,
    partial_ok: AtomicU64,
    partial_err: AtomicU64,
}

impl SyncCounters {
    /// Apply one resolved `PSYNC`.
    pub fn record(&self, outcome: SyncOutcome) {
        match outcome {
            SyncOutcome::PartialOk => {
                self.partial_ok.fetch_add(1, Ordering::Relaxed);
            }
            SyncOutcome::FullResyncRequested => {
                self.full.fetch_add(1, Ordering::Relaxed);
            }
            SyncOutcome::PartialRefused => {
                self.partial_err.fetch_add(1, Ordering::Relaxed);
                self.full.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Read the triple in one call, so both renderers go through one path and
    /// cannot each invent their own field list.
    ///
    /// This is **not** an atomic snapshot: three `Relaxed` loads, and
    /// [`Self::record`] moves two counters with two separate `fetch_add`s for a
    /// refused partial. A reader racing a `PartialRefused` can therefore see
    /// `sync_partial_err` already incremented and `sync_full` not yet. Which is
    /// the same tearing Redis has (three plain `long long`s incremented outside
    /// any lock) and is acceptable for monotone diagnostic counters: every value
    /// read was true at some instant and none can go backwards. It would not be
    /// acceptable for a field an operator subtracts from another to derive a
    /// decision, and none of these are.
    pub fn snapshot(&self) -> SyncCountersSnapshot {
        SyncCountersSnapshot {
            full: self.full.load(Ordering::Relaxed),
            partial_ok: self.partial_ok.load(Ordering::Relaxed),
            partial_err: self.partial_err.load(Ordering::Relaxed),
        }
    }

    // No `reset()` yet: `CONFIG RESETSTAT` cannot reach the replication tracker
    // (`ConnCtx` carries no handle to it), and Redis does zero these three in
    // `resetServerStats`. Adding the method without the caller would be dead
    // code claiming a capability the server does not have — the divergence is
    // filed as issue 24 instead.
}

/// One read of [`SyncCounters`], as `INFO` reports them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncCountersSnapshot {
    /// Full resyncs served (`sync_full`).
    pub full: u64,
    /// Partial resyncs granted (`sync_partial_ok`).
    pub partial_ok: u64,
    /// Partial resyncs attempted and refused (`sync_partial_err`).
    pub partial_err: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // FM-REPLICATION-050
    #[test]
    fn a_granted_continue_is_partial_ok_whatever_id_was_sent() {
        assert_eq!(
            SyncOutcome::classify("repl-abc", true),
            SyncOutcome::PartialOk
        );
        // A `?` id can never be granted a partial in practice, but the
        // classification must not depend on that: the grant decides.
        assert_eq!(SyncOutcome::classify("?", true), SyncOutcome::PartialOk);
    }

    // FM-REPLICATION-050
    #[test]
    fn the_question_mark_sentinel_is_a_request_not_a_refusal() {
        assert_eq!(
            SyncOutcome::classify("?", false),
            SyncOutcome::FullResyncRequested
        );
    }

    // FM-REPLICATION-050
    #[test]
    fn a_full_resync_after_a_named_id_is_a_refused_partial() {
        assert_eq!(
            SyncOutcome::classify("repl-abc", false),
            SyncOutcome::PartialRefused
        );
    }

    // FM-REPLICATION-050
    #[test]
    fn counters_start_at_zero_and_each_outcome_moves_only_its_own_fields() {
        let counters = SyncCounters::default();
        assert_eq!(counters.snapshot(), SyncCountersSnapshot::default());

        counters.record(SyncOutcome::FullResyncRequested);
        assert_eq!(
            counters.snapshot(),
            SyncCountersSnapshot {
                full: 1,
                partial_ok: 0,
                partial_err: 0
            }
        );

        counters.record(SyncOutcome::PartialOk);
        assert_eq!(
            counters.snapshot(),
            SyncCountersSnapshot {
                full: 1,
                partial_ok: 1,
                partial_err: 0
            }
        );
    }

    // FM-REPLICATION-050
    /// Redis increments `sync_full` on a refused partial too, because the
    /// refusal falls through to a full resync. Reporting only
    /// `sync_partial_err` would make the transfer the primary actually paid for
    /// invisible.
    #[test]
    fn a_refused_partial_advances_both_partial_err_and_full() {
        let counters = SyncCounters::default();
        counters.record(SyncOutcome::PartialRefused);
        assert_eq!(
            counters.snapshot(),
            SyncCountersSnapshot {
                full: 1,
                partial_ok: 0,
                partial_err: 1
            }
        );
    }
}
