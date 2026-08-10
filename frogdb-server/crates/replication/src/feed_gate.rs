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
//!
//! ## The decision is separable from the cell it lives in
//!
//! Both of the gate's rules — what a republish does to the value and to the
//! waiters, and whether a published deadline is still in force — are functions
//! of plain data ([`decide_publish`], [`decide_hold`]). [`ReplicaFeedGate`] is
//! the I/O half: the mutex, the [`Notify`], the clock read, the sleep. The split
//! is the one `PartialSyncReplay::handle_partial_sync_request` already has on
//! the primary side, and it is what lets the transition be enumerated by a test
//! (or a model checker) without a runtime.

use std::sync::Arc;
use std::time::Instant;

use frogdb_types::clock;
use parking_lot::Mutex;
use tokio::sync::Notify;

/// What publishing `next` over `current` does.
///
/// Two arms and no third: a republish either changes the value in force — and
/// then every waiter has to re-evaluate, because the change may be a release, a
/// shortening or a lengthening — or it changes nothing and must not wake
/// anybody. The pause state republishes on *every* mutation of itself, most of
/// which have nothing to do with slot handoff, so "changes nothing" is the
/// common case rather than the corner one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedGatePublish {
    /// The published value already is `next`: leave the cell and the waiters
    /// alone.
    Unchanged,
    /// Store `held_until` and wake every waiter.
    Store { held_until: Option<Instant> },
}

/// The hold a set of armed slot barriers justifies: the latest deadline across
/// them, or `None` when none is armed.
///
/// The derivation half of the publish, pure over the armed deadlines. It lives
/// here rather than beside the pause state it reads because it is the rule the
/// *gate* is defined by — two overlapping handoffs compose the way the pauses
/// themselves do, and a barrier that ends while a later one is still armed must
/// leave the feed held to the later deadline. `frogdb_core`'s
/// `PauseState::feed_hold_until` is the caller that supplies the entries; the
/// model check ([`crate::model`]) is the other, and it is checking this rule
/// rather than a transcription of it.
pub fn decide_feed_hold_until(armed: impl IntoIterator<Item = Instant>) -> Option<Instant> {
    armed.into_iter().max()
}

/// Decide a publish: pure over `(current, next)`, touches nothing.
///
/// Deliberately a plain equality and not "only accept a later deadline": the
/// gate is a derived republish of the pause state, not a high-water latch, so a
/// *shortened* deadline is honoured exactly like a lengthened one and a `None`
/// releases immediately.
pub fn decide_publish(current: Option<Instant>, next: Option<Instant>) -> FeedGatePublish {
    if current == next {
        FeedGatePublish::Unchanged
    } else {
        FeedGatePublish::Store { held_until: next }
    }
}

/// The hold in force at `now`, given what was published: `Some(deadline)` while
/// the feed must not ship, `None` when it is free.
///
/// Pure over `(published, now)` — the clock is a parameter, which is what makes
/// "the gate expires itself" a fact a test can state at any instant rather than
/// a race it has to wait out. The comparison is strict, so a hold is over at
/// exactly its deadline rather than one tick later.
pub fn decide_hold(published: Option<Instant>, now: Instant) -> Option<Instant> {
    published.filter(|held_until| now < *held_until)
}

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
    ///
    /// The I/O half of [`decide_publish`]: it owns the cell and the waiters and
    /// decides nothing itself.
    pub fn publish(&self, held_until: Option<Instant>) {
        {
            let mut guard = self.held_until.lock();
            match decide_publish(*guard, held_until) {
                FeedGatePublish::Unchanged => return,
                FeedGatePublish::Store { held_until } => *guard = held_until,
            }
        }
        self.changed.notify_waiters();
    }

    /// The deadline of the hold in force right now, or `None` when the feed is
    /// free to ship. A published deadline that has already passed answers
    /// `None`: the gate expires itself.
    ///
    /// The I/O half of [`decide_hold`]: it reads the cell and the clock, and
    /// decides nothing itself.
    pub fn hold_deadline(&self) -> Option<Instant> {
        let published = *self.held_until.lock();
        decide_hold(published, clock::now())
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
mod decision_tests {
    use super::*;
    use std::time::Duration;

    /// Deadlines as offsets from one origin the test fixes up front: the
    /// decisions take the instant as a parameter, so nothing here needs the
    /// clock to move — or to be read twice, which would make two offsets
    /// incomparable.
    fn timeline() -> impl Fn(u64) -> Instant {
        let origin = clock::now();
        move |offset_ms| origin + Duration::from_millis(offset_ms)
    }

    /// The publish decision, stated over the whole 2x2 of `(current, next)`
    /// shapes: nothing→nothing, nothing→hold, hold→hold (same and different),
    /// hold→nothing. Every arm that changes the value stores it and wakes;
    /// exactly the two that do not, do not.
    // FM-CLUSTER-097
    #[test]
    fn a_publish_stores_and_wakes_exactly_when_it_changes_the_value() {
        let t = timeline();
        let held = t(100);
        let later = t(500);
        let cases = [
            (None, None, FeedGatePublish::Unchanged),
            (
                None,
                Some(held),
                FeedGatePublish::Store {
                    held_until: Some(held),
                },
            ),
            (Some(held), Some(held), FeedGatePublish::Unchanged),
            (
                Some(held),
                Some(later),
                FeedGatePublish::Store {
                    held_until: Some(later),
                },
            ),
            (
                Some(later),
                Some(held),
                FeedGatePublish::Store {
                    held_until: Some(held),
                },
            ),
            (
                Some(held),
                None,
                FeedGatePublish::Store { held_until: None },
            ),
        ];
        for (current, next, expected) in cases {
            assert_eq!(
                decide_publish(current, next),
                expected,
                "publishing {next:?} over {current:?}"
            );
        }
    }

    /// The derivation over the shapes the pause state presents: nothing armed,
    /// one barrier, and two overlapping ones in either arrival order. The
    /// answer is the *latest* deadline, so a barrier that ends while a later
    /// one is still armed cannot open the feed.
    // FM-CLUSTER-097
    #[test]
    fn the_hold_is_the_latest_deadline_across_armed_barriers() {
        let t = timeline();
        assert_eq!(decide_feed_hold_until([]), None);
        assert_eq!(decide_feed_hold_until([t(100)]), Some(t(100)));
        assert_eq!(decide_feed_hold_until([t(100), t(500)]), Some(t(500)));
        assert_eq!(
            decide_feed_hold_until([t(500), t(100)]),
            Some(t(500)),
            "arrival order must not change the composed hold"
        );
    }

    /// A shortened deadline is a real change, not a no-op the way it would be
    /// under a high-water latch — the anti-wedge property depends on it.
    // FM-CLUSTER-097
    #[test]
    fn a_shorter_deadline_is_a_change() {
        let t = timeline();
        assert_eq!(
            decide_publish(Some(t(30_000)), Some(t(10))),
            FeedGatePublish::Store {
                held_until: Some(t(10))
            }
        );
    }

    /// The hold decision over the three positions of `now` relative to the
    /// published deadline, plus the unpublished case. Strictly before the
    /// deadline holds; at it and past it do not.
    // FM-CLUSTER-097
    #[test]
    fn a_published_hold_is_in_force_strictly_before_its_deadline() {
        let t = timeline();
        let deadline = t(100);
        assert_eq!(decide_hold(Some(deadline), t(99)), Some(deadline));
        assert_eq!(
            decide_hold(Some(deadline), deadline),
            None,
            "the hold is over at its deadline, not one tick after it"
        );
        assert_eq!(decide_hold(Some(deadline), t(101)), None);
        assert_eq!(
            decide_hold(None, t(0)),
            None,
            "nothing published, nothing held, whatever the clock says"
        );
    }

    /// Time only ever ends a hold: once `decide_hold` answers `None` for a
    /// published deadline, no later instant brings it back. This is the
    /// liveness half of FM-CLUSTER-097 as a property of the decision alone.
    // FM-CLUSTER-097
    #[test]
    fn a_lapsed_hold_never_comes_back() {
        let t = timeline();
        let deadline = t(100);
        for now_ms in 100..200 {
            assert_eq!(
                decide_hold(Some(deadline), t(now_ms)),
                None,
                "a hold that lapsed at 100ms must stay lapsed at {now_ms}ms"
            );
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
