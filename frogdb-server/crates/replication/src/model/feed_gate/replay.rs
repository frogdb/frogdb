//! What the feed-gate model found, replayed deterministically against the real
//! gate.
//!
//! Same discipline as `frogdb-cluster`'s model replays: a model result is only
//! worth something if it survives translation, so the paths the checker
//! discovers are re-stated here as plain calls on
//! [`ReplicaFeedGate`](crate::feed_gate::ReplicaFeedGate) with no `stateright`
//! in the loop. A model tick is 100 ms of a paused runtime, so the sequences
//! read the same as the model's actions.
//!
//! The pair below is the level-3 evidence for the replication-correctness
//! PRD's retro-validation revert (d): the first test *is* the pre-`8d55cc4f`
//! tree — a session that ships without consulting the gate — and it shows the
//! frame going out inside a barrier window, exactly the counterexample
//! [`super::tests::a_feed_that_ignores_the_gate_ships_inside_a_barrier_window`]
//! discovers. The second runs the same sequence through the shipped write task
//! and shows the gate refusing it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use frogdb_types::clock;

use crate::feed_gate::{ReplicaFeedGate, decide_feed_hold_until};

/// A model tick, as wall time on a paused runtime.
const TICK: Duration = Duration::from_millis(100);

fn ticks(n: u64) -> Duration {
    TICK * u32::try_from(n).expect("tick counts are small")
}

/// The slot-handoff side: pause entries plus the republish every mutation of
/// them performs (`ClientRegistry::publish_pause_derived_state`), over the
/// production derivation.
struct Handoff {
    gate: Arc<ReplicaFeedGate>,
    armed: Vec<Option<Instant>>,
}

impl Handoff {
    fn new(barriers: usize) -> Self {
        Self {
            gate: ReplicaFeedGate::open(),
            armed: vec![None; barriers],
        }
    }

    /// `pause_slot`: arm barrier `b` for `ttl` ticks, then republish.
    fn arm(&mut self, b: usize, ttl: u64) {
        self.armed[b] = Some(clock::now() + ticks(ttl));
        self.republish();
    }

    /// `unpause_slot`: drop barrier `b`'s entry, then republish.
    fn release(&mut self, b: usize) {
        self.armed[b] = None;
        self.republish();
    }

    fn republish(&self) {
        self.gate
            .publish(decide_feed_hold_until(self.armed.iter().flatten().copied()));
    }

    /// Whether a barrier is armed and in force — read off the pause entries,
    /// not off the gate, so the two cannot agree by construction.
    fn barrier_active(&self) -> bool {
        let now = clock::now();
        self.armed.iter().flatten().any(|&d| now < d)
    }
}

/// The streaming session's write task, with the one assumption the model
/// withdraws left as a parameter: `honour_the_gate` false is the tree before
/// `8d55cc4f`. Returns whether the frame reached the wire while a slot barrier
/// was armed.
async fn ship_a_buffered_frame(handoff: &Handoff, honour_the_gate: bool) -> bool {
    if honour_the_gate {
        handoff.gate.released().await;
    }
    handoff.barrier_active()
}

/// The tree before `8d55cc4f`, on the model's counterexample path — `Arm(0, 2)`,
/// `Write`, `Ship(0)` — translated one action at a time: the barrier is up, the
/// gate is holding, and the session that never asks puts the frame on the wire
/// anyway. The anomaly FM-CLUSTER-097 closes.
// FM-CLUSTER-097
#[tokio::test(start_paused = true)]
async fn the_pre_fix_write_task_ships_inside_the_barrier_window() {
    let mut handoff = Handoff::new(1);
    handoff.arm(0, 2);
    assert!(
        handoff.gate.is_held(),
        "the gate is holding: the pre-fix session simply never asked"
    );
    assert!(
        ship_a_buffered_frame(&handoff, false).await,
        "the pre-fix tree puts a frame on the wire while the slot barrier is armed"
    );
}

/// The shipped tree, on the same sequence: the write task waits on the gate, so
/// the frame cannot leave until the hold is over — and the hold ends on the
/// deadline the arm carried, with nobody clearing it.
// FM-CLUSTER-097
#[tokio::test(start_paused = true)]
async fn the_shipped_write_task_refuses_that_counterexample() {
    let mut handoff = Handoff::new(1);
    let started = clock::now();
    handoff.arm(0, 2);

    assert!(
        !ship_a_buffered_frame(&handoff, true).await,
        "the frame reached the wire only after the barrier window ended"
    );
    assert!(
        clock::elapsed(started) >= ticks(2),
        "the session served out the barrier's own deadline"
    );
}

/// The compose rule under two overlapping handoffs, replayed against the real
/// gate: the shorter barrier ends first, and its `unpause_slot` republish must
/// not open a feed the longer one is still holding. This is the model's
/// `the_gate_agrees_with_the_pause_state` clause at the one interleaving that
/// makes it non-trivial.
// FM-CLUSTER-097
#[tokio::test(start_paused = true)]
async fn a_release_does_not_open_a_feed_another_barrier_still_holds() {
    let mut handoff = Handoff::new(2);
    let started = clock::now();
    handoff.arm(0, 3);
    handoff.arm(1, 1);
    let long_deadline = handoff.armed[0].expect("barrier 0 is armed");

    handoff.release(1);
    assert_eq!(
        handoff.gate.hold_deadline(),
        Some(long_deadline),
        "the ended barrier's release left the feed held to the live barrier's deadline"
    );

    handoff.gate.released().await;
    assert!(
        !handoff.barrier_active(),
        "and the feed opened only once the last barrier's window was over"
    );
    assert!(
        clock::elapsed(started) >= ticks(3),
        "the feed served out the longer barrier's window, not the released one's"
    );
}
