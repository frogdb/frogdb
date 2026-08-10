//! The replica-feed hold: the replication half of the slot-handoff write
//! barrier (rework issue 12).
//!
//! The barrier fences *acknowledgements*, not work — a write caught by it may
//! still apply to the local keyspace before the fence refuses to answer for it
//! (FM-CLUSTER-095). Applied writes are broadcast, so without this gate a
//! replica of the losing node would receive, during the handover window, writes
//! the primary's own clients were told to retry elsewhere. Redis 8.4 and Valkey
//! 9.0 close the same window by including `PAUSE_ACTION_REPLICA` in the atomic
//! slot-migration pause: while the pause is up the primary stops flushing its
//! replica output buffers, so replicas cannot run ahead of it. This gate is that
//! action.
//!
//! ## The gate carries its own deadline, so it cannot wedge
//!
//! A held feed that is never released is strictly worse than the anomaly it
//! closes, so the gate is never a latch that something else has to clear. It
//! stores the barrier's *deadline*, and a hold whose deadline has passed reads
//! as released without anybody touching it. Every release path therefore works:
//!
//! - normal completion and abort both publish `None` (the barrier's
//!   `unpause_slot`, via the pause state);
//! - a lapsed barrier — the finalizer died, the lease backstop ran out, nothing
//!   ever swept the pause — releases on the deadline the arm itself carried.
//!
//! The value is a pure derivation of the same `PauseState` the write barrier
//! reads (`frogdb_core::ClientRegistry`), republished on every mutation of it,
//! so the two halves of the barrier cannot disagree about whether it is up.
//!
//! ## Node-wide, not per-shard
//!
//! Frames carry a shard id, so holding only the barriered slot's shard looks
//! reachable — but replication is a single global stream with one monotonic
//! offset (`OffsetCoordinator::advance`), and a replica dedups and acks against
//! that one sequence. Shipping shard B's frames while shard A's are held would
//! put the offsets on the wire out of order, which is a worse bug than the one
//! being fixed. So the hold is node-wide for the barrier window, which is also
//! exactly what Redis/Valkey do: their pause is node-wide too.

use std::sync::Arc;
use std::time::{Duration, Instant};

use frogdb_types::clock;
use parking_lot::Mutex;
use tokio::sync::Notify;

/// Holds the primary's replica feed for the duration of a slot-handoff write
/// barrier.
///
/// Shared by the client registry that publishes the barrier state (the single
/// writer, `frogdb_core::ClientRegistry`) and by every streaming replica
/// session (the readers).
#[derive(Debug, Default)]
pub struct ReplicaFeedGate {
    /// Instant the hold lapses at; `None` when no barrier is armed. Read
    /// against [`clock::now`], the same seam the pause entries were armed on.
    held_until: Mutex<Option<Instant>>,
    /// Woken whenever the published value changes, so a session waiting out a
    /// hold notices a release rather than sleeping to the original deadline.
    changed: Notify,
}

impl ReplicaFeedGate {
    /// A gate with nothing held.
    pub fn open() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Publish the barrier state: the instant the hold lapses at, or `None`
    /// when no barrier is armed.
    ///
    /// Called by the owner of the pause state on every mutation of it. It is a
    /// republish of a derived value, not an independent latch, so a shortened
    /// deadline is honoured exactly like a lengthened one.
    pub fn publish(&self, held_until: Option<Instant>) {
        // Single exit, so the invariant hook below cannot be skipped by the
        // unchanged-republish case: the early `return` this used to take is now
        // a `changed` flag.
        let changed = {
            let mut guard = self.held_until.lock();
            let changed = *guard != held_until;
            *guard = held_until;
            changed
        };
        if changed {
            self.changed.notify_waiters();
        }
        #[cfg(any(test, debug_assertions))]
        crate::invariants::debug_assert_view_clean(
            &crate::view::ReplicationView::empty().with_feed_gate(self.view(None)),
            "ReplicaFeedGate::publish",
        );
    }

    /// This gate's contribution to the invariant projection.
    ///
    /// `barrier_budget` is passed in rather than read: the ceiling on a hold is
    /// `frogdb_cluster::HANDOFF_BARRIER_MS`, and this crate does not depend on
    /// that one. A caller that knows the budget supplies it; the gate itself is
    /// published to and never told.
    ///
    /// The held answer and the remaining hold are sampled separately on
    /// purpose — see `INV-GATE-1`.
    pub fn view(&self, barrier_budget: Option<Duration>) -> crate::view::FeedGateView {
        let now = clock::now();
        crate::view::FeedGateView {
            is_held: self.is_held(),
            hold_remaining: self
                .hold_deadline()
                .map(|deadline| deadline.saturating_duration_since(now)),
            barrier_budget,
        }
    }

    /// The deadline of the hold in force right now, or `None` when the feed is
    /// free to ship. A published deadline that has already passed answers
    /// `None`: the gate expires itself.
    pub fn hold_deadline(&self) -> Option<Instant> {
        let held_until = (*self.held_until.lock())?;
        (clock::now() < held_until).then_some(held_until)
    }

    /// Whether the feed is held right now.
    pub fn is_held(&self) -> bool {
        self.hold_deadline().is_some()
    }

    /// Resolve once the feed is free to ship — on an explicit release, on a
    /// republished shorter deadline, or on the deadline the hold carries.
    /// Returns immediately when nothing is held.
    ///
    /// Cancel-safe: it holds no state between polls beyond the notification
    /// registration, so dropping it in a `select!` loses nothing.
    pub async fn released(&self) {
        loop {
            let Some(deadline) = self.hold_deadline() else {
                return;
            };
            // Register for the wakeup *before* re-reading, so a `publish` that
            // lands between the read above and the wait below is not missed.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.is_held() {
                return;
            }
            tokio::select! {
                _ = changed => {}
                _ = tokio::time::sleep_until(deadline.into()) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // FM-CLUSTER-097
    #[tokio::test(start_paused = true)]
    async fn an_open_gate_holds_nothing() {
        let gate = ReplicaFeedGate::open();
        assert!(!gate.is_held());
        assert_eq!(gate.hold_deadline(), None);
        // …and waiting on it is not a wait at all.
        gate.released().await;
    }

    // FM-CLUSTER-097
    #[tokio::test(start_paused = true)]
    async fn a_published_deadline_holds_the_feed_until_it_lapses() {
        let gate = ReplicaFeedGate::open();
        gate.publish(Some(clock::now() + Duration::from_millis(100)));
        assert!(gate.is_held());

        tokio::time::advance(Duration::from_millis(99)).await;
        assert!(gate.is_held(), "the hold is still inside its window");

        tokio::time::advance(Duration::from_millis(2)).await;
        assert!(
            !gate.is_held(),
            "a lapsed hold releases itself, with nobody clearing it"
        );
    }

    /// The anti-wedge property stated as a wait: nothing publishes a release,
    /// and the waiter still wakes on the deadline the arm carried.
    // FM-CLUSTER-097
    #[tokio::test(start_paused = true)]
    async fn a_lapsed_hold_releases_a_waiter_without_an_explicit_release() {
        let gate = ReplicaFeedGate::open();
        gate.publish(Some(clock::now() + Duration::from_millis(100)));

        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.released().await })
        };
        tokio::time::advance(Duration::from_millis(101)).await;
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the waiter must wake on the hold's own deadline")
            .expect("the waiter task must not panic");
    }

    /// The normal exit path: the barrier is lifted well before its deadline and
    /// the feed resumes at once instead of serving out the window.
    // FM-CLUSTER-097
    #[tokio::test(start_paused = true)]
    async fn an_explicit_release_wakes_a_waiter_early() {
        let gate = ReplicaFeedGate::open();
        gate.publish(Some(clock::now() + Duration::from_secs(30)));

        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.released().await })
        };
        // Let the waiter register before the release lands.
        tokio::task::yield_now().await;
        gate.publish(None);

        tokio::time::timeout(Duration::from_millis(10), waiter)
            .await
            .expect("an explicit release must not wait for the deadline")
            .expect("the waiter task must not panic");
        assert!(!gate.is_held());
    }

    /// A republished *shorter* deadline is honoured: the gate is a derived
    /// value, not a high-water latch that only ever extends.
    // FM-CLUSTER-097
    #[tokio::test(start_paused = true)]
    async fn a_shortened_deadline_is_honoured() {
        let gate = ReplicaFeedGate::open();
        gate.publish(Some(clock::now() + Duration::from_secs(30)));
        gate.publish(Some(clock::now() + Duration::from_millis(10)));

        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.released().await })
        };
        tokio::time::advance(Duration::from_millis(11)).await;
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the waiter must wake on the republished deadline")
            .expect("the waiter task must not panic");
    }

    /// Republishing the same value must not spuriously wake waiters — the
    /// pause state republishes on every mutation, most of which are unrelated.
    // FM-CLUSTER-097
    #[tokio::test(start_paused = true)]
    async fn republishing_the_same_deadline_is_a_no_op() {
        let gate = ReplicaFeedGate::open();
        let deadline = clock::now() + Duration::from_secs(30);
        gate.publish(Some(deadline));

        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.released().await })
        };
        tokio::task::yield_now().await;
        gate.publish(Some(deadline));

        assert!(
            tokio::time::timeout(Duration::from_millis(10), waiter)
                .await
                .is_err(),
            "an identical republish must leave the hold in force"
        );
    }
}
