//! The flush hold: a bounded, self-expiring suppression of the WAL flush
//! engine's *own* commit triggers.
//!
//! # What it is for
//!
//! A `FULLRESYNC` checkpoint ships a per-shard coverage vector `Y_s` — the
//! offset of the last write shard `s` broadcast at the moment that shard's
//! contribution to the payload was captured — and the replica installs it as a
//! per-shard skip floor over the `(offset, current]` handoff replay. Two
//! directions have to hold for that floor to be sound:
//!
//! * every shard-`s` write at or below `Y_s` **is** in the payload — already
//!   true, because `WalPersistence` precedes `ReplicationBroadcast` in
//!   `WRITE_EFFECT_ORDER` and the drain pushes its `Flush` down the same FIFO
//!   those entries went into;
//! * no shard-`s` write **above** `Y_s` is in the payload — which is what this
//!   type buys. Between a shard's drain-ack and the checkpoint cut, further
//!   writes execute and are broadcast above `Y_s`, and the flush engine's own
//!   size/timeout triggers would happily commit them into RocksDB before
//!   `Checkpoint::create_checkpoint` runs. The replica would then skip frames
//!   whose effects it already holds — which is fine — but a `Y_s` chosen to
//!   cover them would skip frames it does *not* hold, which is silent write
//!   loss. So the payload is pinned at `Y_s` instead: while the hold is armed
//!   the engine stages but does not commit.
//!
//! # It is a hold, never a latch
//!
//! Modelled on [`ReplicaFeedGate`](../../../replication/src/feed_gate.rs): a
//! suppression that something else has to clear is strictly worse than the
//! anomaly it closes, so the arm carries its own deadline. A hold whose
//! deadline has passed reads as released without anybody touching it, and the
//! flush that pushed past it marks the hold **breached**. A breached shard may
//! have data above `Y_s` in the cut, so its watermark is reported as `0` — "no
//! frame is at or below this floor", i.e. exactly the pre-coverage behaviour
//! for that shard. Graceful degradation, not a wedged write path.
//!
//! # Mandatory flushes wait rather than skip
//!
//! An explicit [`WalCommand::Flush`](super::flush) — `flush_async`, and the
//! `flush_through` that backs `Durability::Sync` write acks — is a durability
//! promise to a caller, so it cannot simply be suppressed. It blocks on
//! [`FlushHold::wait_for_release`] instead. That is the documented cost of the
//! hold: under sync durability it briefly holds write acks too, bounded by the
//! arm's deadline.
//!
//! # Nesting
//!
//! Two replicas can full-sync at once, so arms are counted. The effective
//! deadline is the latest one armed, and only the last release actually lifts
//! the hold; a breach observed while any arm is live is reported to every one
//! of them (conservative: both payloads report `0` for that shard).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use frogdb_types::clock;
use parking_lot::{Condvar, Mutex};
use tracing::warn;

/// How long a full-sync capture may pin its shard's flush engine.
///
/// The window it has to cover is "drain-ack → `create_checkpoint` returns".
/// A RocksDB checkpoint is a hardlink walk of the live SST set plus a WAL/
/// memtable flush, so it is normally milliseconds and pathologically a second
/// or two. Ten seconds is three orders of magnitude above the flush engine's
/// default batch timeout (10ms) and far above any healthy cut, while still
/// bounding the worst case: a full-sync that wedges cannot stall a `sync`
/// durability write ack for longer than this, and the shard whose hold lapsed
/// degrades to a `0` watermark rather than shipping an unsound floor.
pub const FULL_SYNC_HOLD: std::time::Duration = std::time::Duration::from_secs(10);

/// Suppression state shared between the arming producer, the releasing
/// coordinator, and the flush thread.
#[derive(Debug)]
struct HoldState {
    /// Number of live arms. The hold lifts when this reaches zero.
    depth: usize,
    /// The effective deadline (the latest one armed), or `None` when the hold
    /// is not in force — either never armed, all arms released, or lapsed.
    until: Option<Instant>,
    /// Whether a deadline expired under a live arm, so a mandatory flush was
    /// allowed through and the cut may hold data above the captured watermark.
    breached: bool,
}

/// A bounded suppression of the flush engine's own commit triggers.
///
/// See the module docs. Cheap to check ([`Self::is_held`] is one relaxed atomic
/// load in the overwhelmingly common unheld case) because the flush thread
/// consults it on every batch decision.
#[derive(Debug)]
pub struct FlushHold {
    /// Fast path for the flush thread: `false` means "definitely not held", so
    /// the lock is never taken on the hot path. `true` means "held, unless the
    /// deadline has since passed" — the slow path decides.
    held: AtomicBool,
    state: Mutex<HoldState>,
    released: Condvar,
}

impl Default for FlushHold {
    fn default() -> Self {
        Self::new()
    }
}

impl FlushHold {
    pub fn new() -> Self {
        Self {
            held: AtomicBool::new(false),
            state: Mutex::new(HoldState {
                depth: 0,
                until: None,
                breached: false,
            }),
            released: Condvar::new(),
        }
    }

    /// A shared handle, the shape every consumer stores.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Suppress non-mandatory flush triggers until `until`.
    ///
    /// Must be called with no `.await` between the producer's drain and this,
    /// so the armed instant *is* the drain point and nothing can have been
    /// staged in between.
    pub fn arm(&self, until: Instant) {
        let mut state = self.state.lock();
        if state.depth == 0 {
            // A fresh hold starts unbreached; a breach only ever describes the
            // arms that were live when it happened.
            state.breached = false;
        }
        state.depth += 1;
        state.until = Some(match state.until {
            Some(existing) if existing > until => existing,
            _ => until,
        });
        self.held.store(true, Ordering::Release);
    }

    /// Stop suppressing. Returns whether the hold was breached — its deadline
    /// expired under a live arm and a mandatory flush went through, so the
    /// caller's payload may hold data above its captured watermark.
    ///
    /// Releasing a hold that was never armed is a no-op reporting `false`, so
    /// a guard that releases on both an explicit path and `Drop` is safe.
    pub fn release(&self) -> bool {
        let mut state = self.state.lock();
        if state.depth == 0 {
            return false;
        }
        state.depth -= 1;
        let breached = state.breached;
        if state.depth == 0 {
            state.until = None;
            state.breached = false;
            self.held.store(false, Ordering::Release);
            self.released.notify_all();
        }
        breached
    }

    /// Whether the engine's own size/timeout triggers are currently suppressed.
    ///
    /// Lapses the hold in passing: a deadline that has gone by reads as
    /// released, and is recorded as a breach so the capture that armed it can
    /// downgrade its watermark.
    pub fn is_held(&self) -> bool {
        if !self.held.load(Ordering::Acquire) {
            return false;
        }
        let mut state = self.state.lock();
        self.still_held(&mut state)
    }

    /// Block until released, or until the deadline passes.
    ///
    /// On deadline expiry the hold is marked breached and this returns: a hold
    /// that never releases is worse than the anomaly it closes, and the caller
    /// (an explicit `Flush`) owes somebody a durability answer.
    pub fn wait_for_release(&self) {
        if !self.held.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.state.lock();
        while self.still_held(&mut state) {
            // `until` is `Some` whenever `still_held` said yes.
            let remaining = state
                .until
                .map(|until| until.saturating_duration_since(clock::now()))
                .unwrap_or_default();
            let _ = self.released.wait_for(&mut state, remaining);
        }
    }

    /// Whether the hold is in force *right now*, expiring it if its deadline
    /// has passed. The single place the lapse rule lives.
    fn still_held(&self, state: &mut HoldState) -> bool {
        let Some(until) = state.until else {
            return false;
        };
        if clock::now() < until {
            return true;
        }
        state.until = None;
        state.breached = true;
        self.held.store(false, Ordering::Release);
        self.released.notify_all();
        warn!(
            "WAL flush hold expired before it was released; the full-sync \
             payload may hold writes above the captured watermark, so that \
             shard's coverage watermark degrades to 0"
        );
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn arm_holds_and_release_lifts() {
        let hold = FlushHold::new();
        assert!(!hold.is_held(), "a fresh hold suppresses nothing");
        hold.arm(clock::now() + Duration::from_secs(60));
        assert!(hold.is_held());
        assert!(
            !hold.release(),
            "a hold released inside its deadline is sound"
        );
        assert!(!hold.is_held());
    }

    #[test]
    fn releasing_an_unarmed_hold_is_a_no_op() {
        let hold = FlushHold::new();
        assert!(!hold.release());
        assert!(!hold.is_held());
        // Idempotent release: a guard that releases explicitly *and* on drop
        // must not underflow the arm count into a permanent suppression.
        hold.arm(clock::now() + Duration::from_secs(60));
        assert!(!hold.release());
        assert!(!hold.release());
        assert!(!hold.is_held());
    }

    #[test]
    fn the_deadline_expires_the_hold_and_marks_it_breached() {
        let hold = FlushHold::new();
        hold.arm(clock::now() - Duration::from_millis(1));
        assert!(
            !hold.is_held(),
            "a lapsed deadline reads as released without anybody touching it"
        );
        assert!(
            hold.release(),
            "the capture that armed it must learn its floor is unsafe"
        );
    }

    #[test]
    fn wait_for_release_returns_on_the_deadline() {
        let hold = FlushHold::new();
        hold.arm(clock::now() + Duration::from_millis(30));
        // No releaser exists: the only way out is the deadline.
        hold.wait_for_release();
        assert!(!hold.is_held());
        assert!(hold.release(), "the timed-out wait is a breach");
    }

    #[test]
    fn wait_for_release_returns_when_a_releaser_lifts_the_hold() {
        let hold = Arc::new(FlushHold::new());
        hold.arm(clock::now() + Duration::from_secs(60));
        let releaser = {
            let hold = Arc::clone(&hold);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                hold.release()
            })
        };
        hold.wait_for_release();
        assert!(
            !releaser.join().expect("releaser thread"),
            "an in-deadline release is not a breach"
        );
        assert!(!hold.is_held());
    }

    #[test]
    fn arms_nest_and_only_the_last_release_lifts_the_hold() {
        let hold = FlushHold::new();
        hold.arm(clock::now() + Duration::from_secs(60));
        hold.arm(clock::now() + Duration::from_secs(120));
        assert!(!hold.release());
        assert!(hold.is_held(), "the second capture still needs the hold");
        assert!(!hold.release());
        assert!(!hold.is_held());
    }

    #[test]
    fn a_breach_is_reported_to_every_live_arm() {
        let hold = FlushHold::new();
        hold.arm(clock::now() - Duration::from_millis(1));
        hold.arm(clock::now() - Duration::from_millis(1));
        assert!(!hold.is_held());
        assert!(hold.release(), "first capture learns its floor is unsafe");
        assert!(hold.release(), "so does the second");
    }
}
