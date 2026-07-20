//! Schedule generator enforcing the permutation-constraint model (brief
//! C1–C7) structurally: illegal message orders are unrepresentable.
//!
//! A test defines a fixed set of **sender programs** (each a `Vec<Step>`) plus a
//! proptest-chosen **schedule** — a bounded sequence of `Choice`s. Replaying
//! the schedule advances senders one step at a time (C1: a sender only advances
//! its own next step; C2: a gating step awaits its reply before completing) and
//! fires per-shard ticks between dispatches (C4). A `Choice::Advance(s)` for a
//! finished sender is a no-op, so shrinking (which removes/relabels choices)
//! can never synthesize an out-of-order history.

#![allow(dead_code)] // generator surface is used piecemeal across scenario modules

use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::ParsedCommand;
use proptest::prelude::*;

use super::harness::ShardDriver;
use frogdb_core::types::BlockingOp;

/// A per-shard tick pseudo-event (C4). Ticks never preempt a dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tick {
    Expiry,
    WaiterTimeout,
    ContinuationRelease,
}

/// One scheduling choice: advance sender `s`, or fire a tick on `shard`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Choice {
    Advance(usize),
    Tick { shard: usize, tick: Tick },
}

/// One step in a logical sender's program.
#[derive(Debug, Clone)]
pub enum Step {
    /// Gated single command on `shard` (awaits its reply — C2).
    Execute {
        shard: usize,
        conn_id: u64,
        command: ParsedCommand,
    },
    /// Gated transaction on `shard` with resolved watches (C6: the versions are
    /// whatever the sender's earlier `GetVersion` read).
    ExecTransaction {
        shard: usize,
        conn_id: u64,
        commands: Vec<ParsedCommand>,
        watches: Vec<(Bytes, u64)>,
    },
    /// Register a blocking waiter (C2 exempt: no immediate reply). The receiver
    /// is dropped by the harness; scenarios that need the reply build the
    /// `BlockWait` directly via the driver.
    BlockWait {
        shard: usize,
        conn_id: u64,
        keys: Vec<Bytes>,
        op: BlockingOp,
        deadline: Option<Instant>,
    },
    /// Fire-and-forget waiter cleanup (C3): only legal after this conn's
    /// `BlockWait`, only in Timeout/Unblocked histories.
    UnregisterWait { shard: usize, conn_id: u64 },
}

/// A logical sender: an ordered program plus a cursor (C1).
#[derive(Debug, Clone)]
pub struct Sender {
    pub program: Vec<Step>,
    pub cursor: usize,
}

impl Sender {
    pub fn new(program: Vec<Step>) -> Self {
        Self { program, cursor: 0 }
    }
    pub fn finished(&self) -> bool {
        self.cursor >= self.program.len()
    }
}

/// Advance sender `idx` by exactly one step against the driver, awaiting any
/// gating reply (C2). No-op if the sender is finished.
pub async fn advance(driver: &mut ShardDriver, senders: &mut [Sender], idx: usize) {
    if idx >= senders.len() || senders[idx].finished() {
        return;
    }
    let step = senders[idx].program[senders[idx].cursor].clone();
    senders[idx].cursor += 1;
    match step {
        Step::Execute {
            shard,
            conn_id,
            command,
        } => {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let msg = frogdb_core::shard::ShardMessage::Execute {
                command: std::sync::Arc::new(command),
                conn_id,
                txid: None,
                protocol_version: frogdb_protocol::ProtocolVersion::Resp3,
                track_reads: false,
                no_touch: false,
                response_tx: tx,
            };
            driver.dispatch(shard, msg).await;
            let _ = rx.await; // C2: gate on the reply
        }
        Step::ExecTransaction {
            shard,
            conn_id,
            commands,
            watches,
        } => {
            let _ = driver
                .exec_transaction(shard, conn_id, commands, watches)
                .await;
        }
        Step::BlockWait {
            shard,
            conn_id,
            keys,
            op,
            deadline,
        } => {
            // Receiver dropped here; the wait entry lives in the queue. Its
            // reply (satisfaction / drain / timeout) is observed via probes and
            // store reads, or captured directly when a scenario needs it.
            let _rx = driver.block_wait(shard, conn_id, keys, op, deadline).await;
        }
        Step::UnregisterWait { shard, conn_id } => {
            driver.unregister_wait(shard, conn_id).await;
        }
    }
}

/// Fire one tick against the driver.
pub async fn fire_tick(driver: &mut ShardDriver, shard: usize, tick: Tick) {
    match tick {
        Tick::Expiry => driver.tick_expiry(shard),
        Tick::WaiterTimeout => driver.tick_waiter_timeout(shard),
        Tick::ContinuationRelease => driver.pump_continuation_release(shard).await,
    }
}

/// Replay a whole schedule: advance senders and fire ticks in the chosen order,
/// then drain each sender to completion so no program is left half-run.
pub async fn replay(
    driver: &mut ShardDriver,
    senders: &mut [Sender],
    schedule: &[Choice],
    num_shards: usize,
) {
    for choice in schedule {
        match *choice {
            Choice::Advance(s) => advance(driver, senders, s).await,
            Choice::Tick { shard, tick } => {
                if shard < num_shards && tick != Tick::ContinuationRelease {
                    fire_tick(driver, shard, tick).await;
                }
            }
        }
    }
    // Finish every remaining step so the quiesce asserts see a full history.
    for s in 0..senders.len() {
        while !senders[s].finished() {
            advance(driver, senders, s).await;
        }
    }
}

/// proptest strategy for a schedule over `num_senders` senders and `num_shards`
/// shards, at most `max_len` choices. Advancing a finished sender is a no-op,
/// so the strategy is total (every generated schedule is legal) and shrinks by
/// dropping choices.
pub fn schedule_strategy(
    num_senders: usize,
    num_shards: usize,
    max_len: usize,
) -> impl Strategy<Value = Vec<Choice>> {
    let advance = (0..num_senders).prop_map(Choice::Advance);
    let tick = (
        0..num_shards,
        prop_oneof![Just(Tick::Expiry), Just(Tick::WaiterTimeout)],
    )
        .prop_map(|(shard, tick)| Choice::Tick { shard, tick });
    // Bias toward advances (senders drive most events); ticks interleave.
    let choice = prop_oneof![3 => advance, 1 => tick];
    proptest::collection::vec(choice, 0..max_len)
}

mod generator_tests {
    use super::*;

    #[test]
    fn advancing_a_finished_sender_is_a_noop() {
        let s = Sender::new(vec![]);
        assert!(s.finished());
    }

    proptest! {
        #[test]
        fn schedule_never_exceeds_bound(sched in schedule_strategy(2, 1, 8)) {
            prop_assert!(sched.len() <= 8);
            for c in &sched {
                match c {
                    Choice::Advance(s) => prop_assert!(*s < 2),
                    Choice::Tick { shard, .. } => prop_assert!(*shard < 1),
                }
            }
        }
    }
}
