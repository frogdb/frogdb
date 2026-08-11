//! The **replication arm** of the seed-driven fault scheduler for the turmoil
//! sims (replication-correctness PRD §3 W4, issue 12).
//!
//! # Where the seam is
//!
//! The seed→schedule derivation, the fingerprint, the regression/muzzle file
//! machinery and generic fault application are topology-agnostic and live in
//! [`super::schedule`] (issue 11); `super::scheduler` is the cluster arm over
//! that seam and this module is its replication sibling. What is supplied here
//! is this topology's half of the [`Arm`] contract — its [`Family`] and [`Op`]
//! vocabularies, its [`Budget`], how its hosts are spawned, and the cross-node
//! checks a single node's state cannot express. Per §8 D8 the ~50-line
//! [`run_seed`] driver shape is duplicated rather than genericized.
//!
//! # The topology
//!
//! One primary and two replicas, **no Raft**: [`REPLICATION_HOSTS`]`[0]` boots
//! as the primary and the other two as replicas of it. The [`LEADER`] sentinel
//! therefore binds to *the primary*, not to an elected leader, and the
//! schedule's `election_timeout_ms` / `heartbeat_interval_ms` draws are inert
//! here — they still happen, because the draw order is shared and moving it
//! would move every arm's seeds at once, but nothing consumes them. Which node
//! *is* primary changes during a run: `PromotionMidStream` promotes a replica
//! and demotes the ex-primary toward it, and the driver tracks that.
//!
//! # The four families beside the reusable three
//!
//! - [`Family::LinkDrop`] — the primary↔replica edge held and healed at a
//!   deliberately chosen partial-sync boundary ([`Boundary`]): inside the
//!   backlog window (a reconnect should be granted `+CONTINUE`), outside it
//!   (`+FULLRESYNC`), and straddling an eviction, where either grant is legal
//!   and the claim is only that the replica converges. The boundary is chosen
//!   by *sizing the backlog*, not by counting bytes: a ring of 8192 entries
//!   cannot evict anything this workload writes, a ring of 2 evicts on the
//!   first write of any hold, and a ring of 24 is comparable to what a hold
//!   covers.
//! - [`Family::PromotionMidStream`] — `REPLICAOF NO ONE` on a replica with
//!   frames in flight, then the ex-primary demoted toward it and re-syncing.
//!   The level-4 witness for retro-validation revert (b).
//! - [`Family::SlowReplica`] — one edge slowed past the lag-disconnect and
//!   `min-replicas-max-lag` windows so the self-fence and
//!   `min-replicas-to-write` arm and disarm under backpressure.
//! - [`Family::FullSyncInterrupt`] — the link dies mid-payload, in both payload
//!   shapes ([`PayloadShape`]: a staged RocksDB checkpoint and the live
//!   dataset a persistence-disabled primary serializes), including the
//!   after-the-trailer/before-the-install case. That instant is not externally
//!   observable, so [`InterruptPoint::PostTrailer`] does not schedule it — it
//!   chops the link repeatedly across the whole sync window and lets the sweep
//!   find it, which is the same "search rather than enumerate" trade the whole
//!   scheduler makes.
//!
//! # What is checked
//!
//! Three layers, all reported as [`Violation`] so the shape is identical to the
//! catalog's:
//!
//! 1. **The invariant catalog on every surviving node**, via `DEBUG REPLICATION
//!    CHECK` (issue 03) at quiesce. That command reports every tier, so
//!    [`hard_violations`] drops the catalog's DOCUMENTED-EXCEPTION ids — those
//!    are rulings, not defects.
//! 2. **Client-visible assertions**: convergence of every touched key across
//!    every surviving node, and no acked-write loss.
//! 3. **Cross-node checks a single-node view cannot express**
//!    ([`check_cross_node`]): `XREPL-1`, `XREPL-2` and `XREPL-3`.
//!
//! # No regression-seed file yet — PRD §8 D9
//!
//! There is deliberately **no `replication-regression-seeds.txt` and no
//! `EXPECTED-FAILURE` muzzle** in this arm. A muzzle is a claim about
//! reproducibility, and cluster-correctness issue 23 (same-seed fingerprint
//! diverges under host load) is open: until it closes, a committed muzzle could
//! be recording a nightly failure that does not replay locally. Exit criterion 5
//! of issue 12 is what blocks on 23; everything else here does not. The shared
//! machinery for the file already exists in [`super::schedule`] — when 23
//! closes, this arm adds the file and the two functions that read it, exactly as
//! the cluster arm does.
//!
//! # Durability faults are out of scope
//!
//! turmoil has no disk model, so lose-unsynced-writes-on-kill rides campaign
//! 2's `CrashTestHarness`, not this scheduler. [`FaultKind::CrashRestart`] here
//! is a process death and restart against an intact data directory.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use frogdb_replication::invariants::CATALOG;
use frogdb_types::{Tier, Violation};
use rand::{RngExt, rngs::StdRng};
use turmoil::Builder;

use super::schedule::{
    self, Arm, Budget, FaultEpisode, FaultKind, LEADER, RunOutcome, Span, apply_fault,
    assert_fingerprints_equal, distinct, env_u64, episode, hard_violations, parse_check_entry,
    prune_concurrent_crashes,
};
use super::{REPLICATION_HOSTS, RespConn, RespValue};
use crate::common::sim_helpers::{
    ReplicationNodeParams, SERVER_PORT, real_frogdb_replication_node,
};

/// Nodes in every scheduled replication topology: one primary and two replicas.
///
/// Two replicas rather than one is what makes the interesting claims sayable at
/// all — `XREPL-1` needs a *surviving sibling* to hold a write the promoted node
/// must also hold, and `XREPL-3` needs a `WAIT` answer that can be wrong by more
/// than one.
pub const NODE_COUNT: usize = REPLICATION_HOSTS.len();

/// Host index the topology boots with as its primary. Also what [`LEADER`]
/// binds to, and the origin of the demote/promote dance.
pub const BOOT_PRIMARY: usize = 0;

/// Replicas at boot.
pub const REPLICA_COUNT: usize = NODE_COUNT - 1;

/// The workload key pool. Small on purpose: overwriting the same handful of keys
/// is what makes a divergent replica visible as a *wrong value* rather than as a
/// missing one.
const KEYS: [&str; 5] = ["alpha", "bravo", "charlie", "delta", "echo"];

/// The id a `DEBUG REPLICATION CHECK` entry is surfaced under when the catalog
/// does not define it — never dropped, because an unknown id is itself news.
const UNKNOWN_CHECK_ID: &str = "XREPL-CHECK-2";

// =============================================================================
// The replication arm: this topology's half of the shared derivation
// =============================================================================

/// The shape of fault a schedule injects.
///
/// Drawn from the seed *first* so a sweep covers every shape by construction
/// rather than by luck: at 500 seeds each family gets roughly 71 runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Family {
    /// No faults, latency skew only — the steady-state stream.
    Healthy,
    /// A primary↔replica edge held and healed at a partial-sync boundary.
    LinkDrop,
    /// `REPLICAOF NO ONE` on a replica with frames in flight, then the
    /// ex-primary demoted toward it.
    PromotionMidStream,
    /// One replica's edge slowed past the lag-disconnect and freshness windows,
    /// with the fence and `min-replicas-to-write` engaged.
    SlowReplica,
    /// The link dies during a full sync, in both payload shapes.
    FullSyncInterrupt,
    /// One node SIGKILLed mid-workload and restarted on the same data directory.
    CrashRestart,
    /// Two or three episodes of any kind, possibly overlapping.
    Mixed,
}

impl Family {
    /// Every family, in the order the seed selects from. Changing this list
    /// renumbers which family a seed maps to.
    pub const ALL: [Family; 7] = [
        Family::Healthy,
        Family::LinkDrop,
        Family::PromotionMidStream,
        Family::SlowReplica,
        Family::FullSyncInterrupt,
        Family::CrashRestart,
        Family::Mixed,
    ];

    /// Stable token used in fingerprints and failure messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Family::Healthy => "healthy",
            Family::LinkDrop => "link-drop",
            Family::PromotionMidStream => "promotion-mid-stream",
            Family::SlowReplica => "slow-replica",
            Family::FullSyncInterrupt => "full-sync-interrupt",
            Family::CrashRestart => "crash-restart",
            Family::Mixed => "mixed",
        }
    }
}

/// Which side of the partial-sync boundary a [`Family::LinkDrop`] reconnect
/// lands on, expressed as a backlog capacity rather than as a byte count.
///
/// The backlog is a ring of *entries*, so sizing it against the number of writes
/// a hold covers is exact in a way that sizing it in bytes is not: the workload
/// draws its values from the schedule, and their encoded length is not something
/// a seed should have to reason about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Boundary {
    /// A ring nothing this workload can evict: the reconnect is inside the
    /// window and should be granted `+CONTINUE`.
    InsideWindow,
    /// A ring of two entries: the first write of any hold evicts the resume
    /// point, so the reconnect is outside the window and must be refused into a
    /// `+FULLRESYNC`.
    OutsideWindow,
    /// A ring comparable to what a hold covers, so the reconnect straddles the
    /// eviction and either grant is legal. The claim is convergence, not which
    /// grant was issued.
    StraddlesEviction,
}

impl Boundary {
    /// Backlog entry cap realizing this boundary.
    pub fn backlog_size(self) -> usize {
        match self {
            Boundary::InsideWindow => 8_192,
            Boundary::OutsideWindow => 2,
            Boundary::StraddlesEviction => 24,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Boundary::InsideWindow => "inside-window",
            Boundary::OutsideWindow => "outside-window",
            Boundary::StraddlesEviction => "straddles-eviction",
        }
    }
}

/// Which full-sync payload a primary ships, chosen by whether it has a RocksDB
/// store to checkpoint (`replica_session::run_full_sync`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadShape {
    /// `persistence.enabled = true`: the primary drains its shard WALs, cuts a
    /// RocksDB checkpoint, and ships it as a multi-file `FROGDB_CHECKPOINT`
    /// stream the replica stages to disk and installs.
    StagedCheckpoint,
    /// `persistence.enabled = false`: the primary serializes its live keyspace
    /// straight into a `FROGDB_SNAPSHOT` envelope.
    LiveDataset,
}

impl PayloadShape {
    fn persistence(self) -> bool {
        matches!(self, PayloadShape::StagedCheckpoint)
    }

    fn as_str(self) -> &'static str {
        match self {
            PayloadShape::StagedCheckpoint => "staged-checkpoint",
            PayloadShape::LiveDataset => "live-dataset",
        }
    }
}

/// Where in a full sync the link is cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterruptPoint {
    /// One hold long enough to sit across the payload transfer.
    MidPayload,
    /// A train of short holds spread across the whole sync-and-install window.
    ///
    /// The trailer-received/dataset-installed instant is a replica-internal
    /// transition with no client-visible edge, so it cannot be *scheduled* from
    /// outside. Chopping the link repeatedly across the window searches for it
    /// instead, which is the same trade the rest of the scheduler makes.
    PostTrailer,
}

impl InterruptPoint {
    fn as_str(self) -> &'static str {
        match self {
            InterruptPoint::MidPayload => "mid-payload",
            InterruptPoint::PostTrailer => "post-trailer",
        }
    }
}

/// One client-workload step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// `SET` against the node the driver currently believes is primary,
    /// followed by one `WAIT` sample. A `+OK` is an acked write; the `WAIT`
    /// answer records how many replicas had it, which is what scopes
    /// [`check_cross_node`]'s `XREPL-1`.
    Write { key: usize },
    /// `GET` against one named node. Reading a *replica* is what feeds the
    /// value-provenance half of `XREPL-2`.
    Read { key: usize, node: usize },
    /// A bare `WAIT <numreplicas> <timeout>`, bracketed by `connected_slaves`
    /// samples for `XREPL-3`.
    Wait { numreplicas: u32, timeout_ms: u64 },
    /// `REPLICAOF NO ONE` on a replica, which becomes the driver's new primary.
    Promote { node: usize },
    /// `REPLICAOF <primary>` on a node, pointing it at the current primary.
    Demote { node: usize },
    /// Sample `INFO replication` from every node, feeding the cross-node
    /// history.
    Observe,
}

impl Op {
    fn render(self) -> String {
        match self {
            Op::Write { key } => format!("write {}", KEYS[key]),
            Op::Read { key, node } => format!("read {}@{node}", KEYS[key]),
            Op::Wait {
                numreplicas,
                timeout_ms,
            } => format!("wait {numreplicas} {timeout_ms}"),
            Op::Promote { node } => format!("promote {node}"),
            Op::Demote { node } => format!("demote {node}"),
            Op::Observe => "observe".to_string(),
        }
    }
}

/// The replication arm's marker type: one primary plus two replicas, no Raft.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationArm;

/// This arm's schedule.
pub type Schedule = schedule::Schedule<ReplicationArm>;

/// The replication arm's per-run knobs, drawn between the latency and the
/// faults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Toggles {
    /// Which side of the partial-sync boundary a reconnect lands on.
    pub boundary: Boundary,
    /// Which full-sync payload the primary ships.
    pub payload: PayloadShape,
    /// Where in a full sync the link is cut.
    pub interrupt: InterruptPoint,
    /// Wires `self-fence-on-replica-loss`.
    pub self_fence: bool,
    /// Wires `min-replicas-to-write`.
    pub min_replicas_to_write: u32,
}

impl Toggles {
    /// The backlog cap this run's nodes are built with.
    ///
    /// `FullSyncInterrupt` needs a full sync to interrupt, so it forces the
    /// narrowest ring regardless of the boundary draw — a reconnect that gets
    /// `+CONTINUE` never reaches the payload path the family is about.
    pub fn backlog_size(&self, family: Family) -> usize {
        match family {
            Family::FullSyncInterrupt => Boundary::OutsideWindow.backlog_size(),
            _ => self.boundary.backlog_size(),
        }
    }
}

impl Arm for ReplicationArm {
    type Family = Family;
    type Toggles = Toggles;
    type Op = Op;

    const HOSTS: &'static [&'static str] = &REPLICATION_HOSTS;

    /// The windows this arm's schedules are drawn inside.
    ///
    /// `base_election_ms` / `election_step_ms` / `heartbeat_interval_ms` are
    /// inert here — there is no election in a replication topology — but the
    /// shared derivation draws them for every arm, and skipping the draw is not
    /// on offer without moving every seed in every arm. They are given the
    /// cluster arm's windows so the numbers in a fingerprint are not
    /// conspicuous nonsense.
    ///
    /// The holds are longer than the cluster arm's: a held replication edge has
    /// to outlive the replica's connect and handshake timeouts before the link
    /// actually breaks and a reconnect is attempted, and a hold that heals first
    /// tests nothing but latency.
    const BUDGET: Budget = Budget {
        heartbeat_interval_ms: Span::new(40, 60),
        base_election_ms: Span::new(280, 360),
        election_step_ms: Span::new(30, 70),
        base_latency_ms: Span::new(1, 8),
        extra_latency_ms: Span::new(1, 20),
        op_gap_ms: Span::new(60, 200),
        quiesce_tail_ms: 2_000,
        op_count: (14, 60),
        sim_duration: Duration::from_secs(300),
        min_arm_ms: 400,
        arm_jitter_ms: Span::new(0, 1_200),
        hold_ms: Span::new(1_500, 6_000),
    };

    fn families() -> &'static [Family] {
        &Family::ALL
    }

    fn family_token(family: Family) -> &'static str {
        family.as_str()
    }

    fn derive_toggles(family: Family, rng: &mut StdRng) -> Toggles {
        let boundary = match rng.random_range(0..3u32) {
            0 => Boundary::InsideWindow,
            1 => Boundary::OutsideWindow,
            _ => Boundary::StraddlesEviction,
        };
        let payload = if rng.random_range(0..2u32) == 0 {
            PayloadShape::StagedCheckpoint
        } else {
            PayloadShape::LiveDataset
        };
        let interrupt = if rng.random_range(0..2u32) == 0 {
            InterruptPoint::MidPayload
        } else {
            InterruptPoint::PostTrailer
        };
        // The fence and `min-replicas-to-write` are what `SlowReplica` exists to
        // put under backpressure, so that family always engages them; elsewhere
        // they are drawn, because a fence that only ever arms in one family is
        // a fence one family's worth of schedules has tested.
        let self_fence = family == Family::SlowReplica || rng.random_range(0..4u32) == 0;
        let min_replicas_to_write = match family {
            Family::SlowReplica => 1,
            _ if rng.random_range(0..5u32) == 0 => 1,
            _ => 0,
        };
        Toggles {
            boundary,
            payload,
            interrupt,
            self_fence,
            min_replicas_to_write,
        }
    }

    fn render_toggles(toggles: &Toggles) -> Vec<String> {
        vec![
            format!("boundary {}", toggles.boundary.as_str()),
            format!("payload {}", toggles.payload.as_str()),
            format!("interrupt {}", toggles.interrupt.as_str()),
            format!("self_fence {}", toggles.self_fence),
            format!("min_replicas_to_write {}", toggles.min_replicas_to_write),
        ]
    }

    fn derive_faults(family: Family, rng: &mut StdRng) -> Vec<FaultEpisode> {
        derive_faults(family, rng)
    }

    fn derive_ops(family: Family, toggles: &Toggles, count: usize, rng: &mut StdRng) -> Vec<Op> {
        derive_ops(family, toggles, count, rng)
    }

    fn render_op(op: Op) -> String {
        op.render()
    }
}

/// Draw one episode inside this arm's budget.
fn repl_episode(rng: &mut StdRng, kind: FaultKind, out: &mut Vec<FaultEpisode>) {
    episode(&ReplicationArm::BUDGET, rng, kind, out);
}

/// Force two node indices apart over this arm's topology.
fn distinct3(a: usize, b: usize) -> (usize, usize) {
    distinct(a, b, NODE_COUNT)
}

/// A replica index, drawn uniformly. Never the boot primary, which is the one
/// node no `primary↔replica` edge has on both ends.
fn some_replica(rng: &mut StdRng) -> usize {
    1 + rng.random_range(0..REPLICA_COUNT)
}

/// Family-specific fault derivation. Every branch draws from the same `rng`.
///
/// [`LEADER`] travels as the primary here: the schedule is derived before the
/// topology exists, and `Schedule::resolve` binds it to whichever host the
/// driver nominated at setup.
fn derive_faults(family: Family, rng: &mut StdRng) -> Vec<FaultEpisode> {
    let mut faults: Vec<FaultEpisode> = Vec::new();
    match family {
        Family::Healthy => {}
        Family::LinkDrop => {
            let replica = some_replica(rng);
            repl_episode(
                rng,
                FaultKind::HoldEdge {
                    a: LEADER,
                    b: replica,
                },
                &mut faults,
            );
        }
        Family::PromotionMidStream => {
            // The promotion happens as an *op*; the fault isolates the primary
            // so the promotion lands on a node whose upstream is gone, which is
            // the shape a real failover has.
            repl_episode(rng, FaultKind::HoldIsolate { node: LEADER }, &mut faults);
        }
        Family::SlowReplica => {
            let replica = some_replica(rng);
            // Well past `min_replicas_timeout_ms` and the freshness window, so
            // the ACK that would keep the replica "good" cannot arrive in time.
            let latency_ms = rng.random_range(600..=2_500u64);
            repl_episode(
                rng,
                FaultKind::SlowEdge {
                    a: LEADER,
                    b: replica,
                    latency_ms,
                },
                &mut faults,
            );
        }
        Family::FullSyncInterrupt => {
            let replica = some_replica(rng);
            // First: a hold long enough to break the link and, with this
            // family's two-entry backlog, force the reconnect into a
            // `+FULLRESYNC`. Everything after it lands during or just past the
            // resulting payload transfer.
            repl_episode(
                rng,
                FaultKind::HoldEdge {
                    a: LEADER,
                    b: replica,
                },
                &mut faults,
            );
            let chops = rng.random_range(1..=4usize);
            for _ in 0..chops {
                repl_episode(
                    rng,
                    FaultKind::HoldEdge {
                        a: LEADER,
                        b: replica,
                    },
                    &mut faults,
                );
            }
        }
        Family::CrashRestart => {
            let node = rng.random_range(0..NODE_COUNT);
            repl_episode(rng, FaultKind::CrashRestart { node }, &mut faults);
        }
        Family::Mixed => {
            let count = rng.random_range(2..=3usize);
            for _ in 0..count {
                let kind = match rng.random_range(0..4u32) {
                    0 => FaultKind::HoldIsolate {
                        node: rng.random_range(0..NODE_COUNT),
                    },
                    1 => {
                        let (a, b) = distinct3(
                            rng.random_range(0..NODE_COUNT),
                            rng.random_range(0..NODE_COUNT),
                        );
                        FaultKind::HoldEdge { a, b }
                    }
                    2 => {
                        let (a, b) = distinct3(
                            rng.random_range(0..NODE_COUNT),
                            rng.random_range(0..NODE_COUNT),
                        );
                        FaultKind::SlowEdge {
                            a,
                            b,
                            latency_ms: rng.random_range(50..=800u64),
                        }
                    }
                    _ => FaultKind::CrashRestart {
                        node: rng.random_range(0..NODE_COUNT),
                    },
                };
                repl_episode(rng, kind, &mut faults);
            }
        }
    }

    prune_concurrent_crashes(faults)
}

/// Ops that must fit after a promotion: the longest `demote_gap`, one repoint
/// per surviving node, and one slot for the workload to exercise the new
/// primary.
const MAX_PROMOTION_TAIL: usize = 3 + (NODE_COUNT - 1) + 1;

/// Workload derivation. Every family writes, reads and samples; promotions only
/// appear where they mean something.
fn derive_ops(family: Family, toggles: &Toggles, count: usize, rng: &mut StdRng) -> Vec<Op> {
    let promotes = matches!(family, Family::PromotionMidStream)
        || (family == Family::Mixed && rng.random_range(0..3u32) == 0);
    // One promotion per run, placed in the middle third so there is a stream to
    // interrupt before it and a settling window after it. A second promotion
    // would only re-test the first with less time to converge.
    //
    // `MAX_PROMOTION_TAIL` slots have to remain after it for the repointing
    // below plus at least one write against the new primary, so the placement is
    // clamped rather than merely drawn — a promotion whose demotes fell off the
    // end of the run would leave the topology split at quiesce.
    let promote_at = if promotes && count > MAX_PROMOTION_TAIL {
        let latest = count - MAX_PROMOTION_TAIL - 1;
        Some((count / 3 + rng.random_range(0..(count / 3).max(1))).min(latest))
    } else {
        None
    };
    let promote_node = some_replica(rng);
    // How long after the promotion the surviving nodes are pointed at the new
    // primary.
    let demote_gap = rng.random_range(1..=3usize);

    // *Every* other node is repointed, not only the ex-primary. Leaving the
    // sibling replica attached to the demoted ex-primary would build a chain
    // (sibling -> ex-primary -> new primary), and chained replication is a
    // `Tier::DocumentedException` in the catalog (INV-ROLE-1, testing-
    // improvements issue 48) — so the run would be measuring a known gap
    // instead of the promotion this family is about. A real failover controller
    // repoints the whole fleet, and so does this schedule: ex-primary first,
    // because it is the node with writes to discard.
    let demote_nodes: Vec<usize> = (0..NODE_COUNT)
        .filter(|&n| n != promote_node)
        .map(|n| if n == BOOT_PRIMARY { (0, n) } else { (1, n) })
        .collect::<BTreeSet<(usize, usize)>>()
        .into_iter()
        .map(|(_, n)| n)
        .collect();
    let demote_at = |i: usize| -> Option<usize> {
        let p = promote_at?;
        let offset = i.checked_sub(p + demote_gap)?;
        demote_nodes.get(offset).copied()
    };

    let mut ops = Vec::with_capacity(count);
    for i in 0..count {
        if Some(i) == promote_at {
            ops.push(Op::Promote { node: promote_node });
            continue;
        }
        if let Some(node) = demote_at(i) {
            ops.push(Op::Demote { node });
            continue;
        }
        // Every third step samples every node's replication view, so the
        // cross-node history has observations spread across the fault window
        // rather than only at quiesce.
        if i % 3 == 2 {
            ops.push(Op::Observe);
            continue;
        }
        let key = rng.random_range(0..KEYS.len());
        let roll = rng.random_range(0..100u32);
        let op = match roll {
            0..=49 => Op::Write { key },
            50..=74 => Op::Read {
                key,
                node: rng.random_range(0..NODE_COUNT),
            },
            75..=89 if toggles.min_replicas_to_write == 0 => Op::Wait {
                numreplicas: rng.random_range(1..=(REPLICA_COUNT as u32 + 1)),
                timeout_ms: rng.random_range(100..=600u64),
            },
            _ => Op::Write { key },
        };
        ops.push(op);
    }
    ops
}

// =============================================================================
// Cross-node history and the checks a single-node view cannot express
// =============================================================================

/// One node's `INFO replication`, as a client sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
    /// Host index of the node that answered.
    pub observer: usize,
    /// `role:master`.
    pub is_primary: bool,
    /// `master_replid` — the history this node believes it is on.
    pub replid: String,
    /// `master_repl_offset`. On a primary this is the live offset it has
    /// produced; on a replica it is the offset it has applied.
    pub offset: u64,
    /// `connected_slaves`.
    pub connected_slaves: u32,
}

/// One observation round: every node sampled at the same logical point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Round {
    /// Monotonic round number — the history's time axis. Simulated durations are
    /// deliberately excluded so the checks do not depend on sampling noise.
    pub seq: u64,
    pub views: Vec<NodeView>,
}

/// A write the primary acknowledged, and how widely it was confirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckedWrite {
    /// Observation round in effect when the ack landed.
    pub seq: u64,
    pub key: usize,
    pub value: String,
    /// Host index that returned `+OK`.
    pub node: usize,
    /// Replicas that had acked this write by the time the driver moved on — the
    /// answer of the `WAIT` the write op issues straight afterwards.
    ///
    /// A write confirmed by *every* replica is one no promotion may lose, which
    /// is what makes `XREPL-1` a claim rather than a hope: a write only the
    /// primary held was never promised to survive its loss.
    pub confirmed_replicas: u32,
    /// Simulated instant of the ack, used only to scope the readback check.
    pub at: Duration,
}

impl AckedWrite {
    /// Was this write confirmed by every replica in the topology, and so
    /// promised to survive a promotion of any of them?
    pub fn survives_promotion(&self) -> bool {
        self.confirmed_replicas as usize >= REPLICA_COUNT
    }
}

/// One `WAIT` answer with the `connected_slaves` window it was answered inside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitSample {
    pub seq: u64,
    /// Host index the `WAIT` was issued against.
    pub node: usize,
    /// What `WAIT` returned. Signed because a negative answer is exactly one of
    /// the things this sample exists to catch.
    pub answered: i64,
    /// `connected_slaves` read immediately *before* the `WAIT`.
    pub connected_before: u32,
    /// `connected_slaves` read immediately *after* it.
    pub connected_after: u32,
}

impl WaitSample {
    /// The most replicas that could have been connected at any instant inside
    /// the `WAIT`.
    ///
    /// The bracket, not either endpoint: a replica that dropped during the call
    /// legitimately makes `connected_after` smaller than the answer, and one
    /// that attached during it makes `connected_before` smaller. Over-
    /// approximating is the only sound direction — under-approximating would
    /// report a phantom defect on a legitimate race.
    pub fn bound(&self) -> u32 {
        self.connected_before.max(self.connected_after)
    }
}

/// One value a client read back out of a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRead {
    pub seq: u64,
    pub node: usize,
    pub key: usize,
    /// `None` is a nil reply — a key not yet applied, which is a legitimate
    /// prefix and never a violation.
    pub value: Option<String>,
}

/// A promotion, and what the promoted node held afterwards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promotion {
    /// Observation round the promotion was issued in.
    pub seq: u64,
    /// Host index promoted.
    pub node: usize,
}

/// One quiesce readback from a node that was promoted during the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotedReadback {
    pub node: usize,
    pub key: usize,
    /// The value the write was acked with.
    pub expected: String,
    /// What the promoted node answered at quiesce.
    pub got: Option<String>,
}

/// The whole run history the cross-node checks consume.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    pub rounds: Vec<Round>,
    pub writes: Vec<AckedWrite>,
    pub waits: Vec<WaitSample>,
    pub reads: Vec<NodeRead>,
    pub promotions: Vec<Promotion>,
    pub promoted_readbacks: Vec<PromotedReadback>,
    /// Rounds at which the ground moved: a fault armed or healed, or a role
    /// changed. Used to scope the offset-monotonicity claim, which a full resync
    /// onto a shorter history legitimately breaks.
    pub churn: Vec<u64>,
    /// Nodes the schedule SIGKILLed and restarted at some point in the run.
    ///
    /// Read by the [`check_cross_node`] named gap for
    /// [replication-correctness issue 24](../../../../../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md):
    /// a restarted node reboots with the replication id it had before the crash
    /// but without the dataset, so replicas legitimately observe themselves
    /// ahead of it on that id until they resync.
    pub restarted: BTreeSet<usize>,
}

/// Rounds within this distance *after* a churn event are excluded from the
/// era-scoped checks: a resync or a role change is allowed to be observed
/// mid-flight.
const CHURN_SETTLE_ROUNDS: u64 = 2;

impl History {
    /// Record a churn event at a round boundary.
    pub fn churn_at(&mut self, seq: u64) {
        self.churn.push(seq);
    }

    /// Is round `seq` inside the settling window of a churn event?
    fn churned(&self, seq: u64) -> bool {
        self.churn
            .iter()
            .any(|&c| seq >= c && seq <= c + CHURN_SETTLE_ROUNDS)
    }

    /// The last acked write per key, in key order.
    pub fn last_writes(&self) -> BTreeMap<usize, &AckedWrite> {
        let mut last: BTreeMap<usize, &AckedWrite> = BTreeMap::new();
        for w in &self.writes {
            last.insert(w.key, w);
        }
        last
    }

    /// Every value the client ever acked for `key`. A replica whose applied
    /// history is a prefix of the primary's can hold nothing outside this set.
    pub fn written_values(&self, key: usize) -> BTreeSet<&str> {
        self.writes
            .iter()
            .filter(|w| w.key == key)
            .map(|w| w.value.as_str())
            .collect()
    }

    /// Was a write acked while a SIGKILL episode was still open — i.e. may it
    /// legitimately be gone, because these sims run the data plane without
    /// persistence?
    pub fn acked_write_is_checkable(&self, schedule: &Schedule, w: &AckedWrite) -> bool {
        !schedule.crash_open_at(w.at)
    }

    /// Was a node promoted at or after observation round `seq`? A write acked
    /// before a promotion may go down with the primary that acked it, unless
    /// every replica had confirmed it first.
    fn promoted_at_or_after(&self, seq: u64) -> bool {
        self.promotions.iter().any(|p| p.seq >= seq)
    }

    /// Every value `key` may legitimately hold once the topology has settled,
    /// oldest first, or `None` if nothing about the key was ever promised.
    ///
    /// Not simply "the last acked write". A write the primary acked before every
    /// replica had confirmed it ([`AckedWrite::survives_promotion`]) was never
    /// promised to outlive that primary, so once a later promotion moves the
    /// primary elsewhere the topology may legitimately settle on the value
    /// before it — which is exactly how [`check_cross_node`] already scopes the
    /// promoted node's own keyspace. The floor is therefore the newest write
    /// that *must* have survived, and every write after it is accepted on top:
    /// it may or may not have made it across before the promotion, and both
    /// outcomes are correct.
    ///
    /// Sweep seed 317 is the witness for the scoping: `write charlie` was acked
    /// `confirmed=0` on a primary an isolate had already cut off, node 2 was
    /// promoted three ops later, all three nodes settled on the preceding value,
    /// and demanding the newest write reported correct behaviour as an `XREPL-1`
    /// acked-write loss.
    pub fn settleable_values(&self, schedule: &Schedule, key: usize) -> Option<Vec<&str>> {
        let order: Vec<&AckedWrite> = self.writes.iter().filter(|w| w.key == key).collect();
        let floor = order.iter().rposition(|w| {
            self.acked_write_is_checkable(schedule, w)
                && (w.survives_promotion() || !self.promoted_at_or_after(w.seq))
        })?;
        Some(order[floor..].iter().map(|w| w.value.as_str()).collect())
    }
}

/// Cross-node checks over a whole run history — claims about the *relationship*
/// between nodes, which no single node's state can express.
///
/// - `XREPL-1` — **no acked write is absent from the promoted node's
///   keyspace.** Scoped to writes every replica had confirmed before the
///   promotion: a write only the primary held was never promised to survive its
///   loss, and asserting on it would make the sweep red for correct behaviour.
/// - `XREPL-2` — **a replica's applied history is a prefix of the primary's.**
///   Three ways of saying it, because no one of them catches all three shapes of
///   divergence: a replica on the primary's replid never reports an offset
///   *ahead* of the primary's; a node's offset never goes backwards while its
///   replid is unchanged and the topology is settled; and a value read out of
///   any node was, at some point, a value the client wrote to that key.
/// - `XREPL-3` — **`WAIT`'s answer never exceeded `connected_slaves`.** Bounded
///   by the bracket around the call ([`WaitSample::bound`]) and, independently,
///   by the number of replicas the topology contains at all. Closes spec GAP-5
///   at level 4.
///
/// Replication ids a crash-restart put back into circulation heading a
/// **shorter** history than the id already named — the observable signature of
/// [replication-correctness issue 24](../../../../../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md).
/// `replication_state.json` lives in the data dir and is reloaded on every boot
/// whether or not a dataset came back with it, so a SIGKILLed node returns
/// advertising the id it headed before the crash with its offset reset to the
/// bottom. Every node still on that id then reports an offset its head has not
/// produced — the restarted node itself, as a rewind, and every replica that has
/// not resynced yet, as being ahead of its primary.
///
/// Keyed on the **observed rewind**, not on "this node was restarted at some
/// point": a restart that mints a fresh id (what issue 24 asks for) taints
/// nothing, so the day it is fixed this set is empty and both `XREPL-2a` and
/// `XREPL-2b` re-arm on their own with no edit here.
fn restart_tainted_replids(history: &History) -> BTreeSet<String> {
    let mut peak: BTreeMap<(usize, &str), u64> = BTreeMap::new();
    let mut tainted = BTreeSet::new();
    for round in &history.rounds {
        for view in &round.views {
            if !history.restarted.contains(&view.observer) {
                continue;
            }
            let mark = peak
                .entry((view.observer, view.replid.as_str()))
                .or_insert(view.offset);
            if view.offset < *mark {
                tainted.insert(view.replid.clone());
            }
            *mark = (*mark).max(view.offset);
        }
    }
    tainted
}

/// Pure: no I/O, iteration order fixed by `Vec`/`BTreeMap`/`BTreeSet`.
pub fn check_cross_node(history: &History) -> Vec<Violation> {
    let mut violations = Vec::new();
    let tainted = restart_tainted_replids(history);

    // --- XREPL-1: a promotion never rolls an acked write back ----------------
    //
    // Not "the promoted node still reads back exactly this value": the workload
    // keeps writing after the promotion, so the newest value for the key is
    // routinely a *later* one, and demanding equality would report a violation
    // for a correctly-applied stream. The claim is that the promoted node's
    // value is not *older* than the write every replica had confirmed — i.e.
    // the write was not lost, whether by nil or by rollback to a predecessor.
    for rb in &history.promoted_readbacks {
        let order: Vec<&str> = history
            .writes
            .iter()
            .filter(|w| w.key == rb.key)
            .map(|w| w.value.as_str())
            .collect();
        let expected_pos = order.iter().position(|v| *v == rb.expected);
        let detail = match rb.got.as_deref() {
            None => Some("<nil>: the key is absent entirely".to_string()),
            Some(got) => match (order.iter().position(|v| *v == got), expected_pos) {
                (None, _) => Some(format!("{got:?}, a value no client ever wrote to that key")),
                (Some(got_pos), Some(exp_pos)) if got_pos < exp_pos => Some(format!(
                    "{got:?}, which the client wrote {} write(s) *earlier* — the promotion \
                     rolled the key back",
                    exp_pos - got_pos
                )),
                _ => None,
            },
        };
        if let Some(detail) = detail {
            violations.push(Violation {
                id: "XREPL-1",
                detail: format!(
                    "node {} was promoted, but the acked write {}={} that every replica had \
                     confirmed before the promotion reads back as {detail}",
                    rb.node, KEYS[rb.key], rb.expected,
                ),
            });
        }
    }

    // --- XREPL-2a: no replica is ahead of its primary on the same history ----
    //
    // Matched by replid rather than by "the primary of the round": mid-promotion
    // a round legitimately sees two masters — the newly promoted node and the
    // ex-primary not yet demoted toward it — and a replica is only comparable
    // with the one whose stream it is actually applying. A replica on an older
    // lineage counts a different stream and is not "ahead" of anything.
    for round in &history.rounds {
        for view in &round.views {
            if view.is_primary {
                continue;
            }
            let Some(primary) = round
                .views
                .iter()
                .find(|p| p.is_primary && p.replid == view.replid)
            else {
                continue;
            };
            if view.offset <= primary.offset {
                continue;
            }
            // Named gap — replication-correctness issue 24, via
            // [`restart_tainted_replids`]: this id was seen rewinding on a node
            // the schedule restarted, so the id outlived the history it names
            // and every replica still on it is trivially "ahead" of the new
            // head. Nothing wider: a replica ahead of its primary on an id no
            // restart rewound is still reported.
            if tainted.contains(&view.replid) {
                continue;
            }
            violations.push(Violation {
                id: "XREPL-2",
                detail: format!(
                    "round {}: replica {} has applied offset {} on replid {} but primary {} has \
                     only produced {} — the replica's history is not a prefix of the primary's",
                    round.seq,
                    view.observer,
                    view.offset,
                    view.replid,
                    primary.observer,
                    primary.offset
                ),
            });
        }
    }

    // --- XREPL-2b: an offset never rewinds under one replid ------------------
    //
    // Against a high-water mark rather than the previous sample, so one dip is
    // reported once instead of re-arming on every later sample. Keyed by
    // `(node, replid)`: adopting a new replid is a new history, and starting it
    // lower is a full resync, not a rewind.
    let mut peak: BTreeMap<(usize, String), (u64, u64)> = BTreeMap::new();
    for round in &history.rounds {
        if history.churned(round.seq) {
            continue;
        }
        for view in &round.views {
            // Same named gap as `XREPL-2a` — replication-correctness issue 24.
            // A rewound-and-reissued id drags every follower back with it on the
            // next resync, so the rewind shows up on nodes the schedule never
            // touched; the exemption is on the *id*, which is where the defect
            // is, and not on the node.
            if tainted.contains(&view.replid) {
                continue;
            }
            let key = (view.observer, view.replid.clone());
            match peak.get_mut(&key) {
                Some(mark) if view.offset < mark.1 => violations.push(Violation {
                    id: "XREPL-2",
                    detail: format!(
                        "node {}'s offset on replid {} went backwards: {} at round {} -> {} at \
                         round {}, with no fault or role change in between",
                        view.observer, view.replid, mark.1, mark.0, view.offset, round.seq
                    ),
                }),
                Some(mark) => {
                    if view.offset > mark.1 {
                        *mark = (round.seq, view.offset);
                    }
                }
                None => {
                    peak.insert(key, (round.seq, view.offset));
                }
            }
        }
    }

    // --- XREPL-2c: every value read back was written by somebody -------------
    for r in &history.reads {
        let Some(value) = r.value.as_deref() else {
            continue;
        };
        if history.written_values(r.key).contains(value) {
            continue;
        }
        violations.push(Violation {
            id: "XREPL-2",
            detail: format!(
                "round {}: node {} returned {}={value}, a value no client ever wrote to that key",
                r.seq, r.node, KEYS[r.key]
            ),
        });
    }

    // --- XREPL-3: WAIT never over-counts (spec GAP-5) ------------------------
    for w in &history.waits {
        if w.answered < 0 {
            violations.push(Violation {
                id: "XREPL-3",
                detail: format!(
                    "round {}: WAIT on node {} answered {}, which is not a replica count",
                    w.seq, w.node, w.answered
                ),
            });
            continue;
        }
        let answered = w.answered as u32;
        if answered > w.bound() {
            violations.push(Violation {
                id: "XREPL-3",
                detail: format!(
                    "round {}: WAIT on node {} answered {answered} but connected_slaves was {} \
                     before the call and {} after it — it counted a replica that was not there",
                    w.seq, w.node, w.connected_before, w.connected_after
                ),
            });
        }
        if answered as usize > REPLICA_COUNT {
            violations.push(Violation {
                id: "XREPL-3",
                detail: format!(
                    "round {}: WAIT on node {} answered {answered} in a topology with only \
                     {REPLICA_COUNT} replicas",
                    w.seq, w.node
                ),
            });
        }
    }

    violations
}

/// Every id the replication catalog defines — the vocabulary a `DEBUG
/// REPLICATION CHECK` reply is read against.
fn catalog_ids() -> Vec<&'static str> {
    CATALOG.iter().map(|inv| inv.id).collect()
}

/// The ids the catalog *deliberates*.
///
/// `DEBUG REPLICATION CHECK` is the reporting view (`check_all`), so it includes
/// `Tier::DocumentedException` entries — `INV-OFFSET-2` (issue 17) and
/// `INV-ROLE-1` (chained replication). Those are rulings, not defects; asserting
/// on them would make the sweep red for states the catalog blesses.
fn excepted_catalog_ids() -> BTreeSet<&'static str> {
    CATALOG
        .iter()
        .filter(|inv| !inv.is_hard())
        .map(|inv| inv.id)
        .collect()
}

/// The catalog ids the scheduler asserts on.
pub fn asserted_catalog_ids() -> Vec<&'static str> {
    CATALOG
        .iter()
        .filter(|inv| matches!(inv.tier, Tier::Hard))
        .map(|inv| inv.id)
        .collect()
}

// =============================================================================
// Runner
// =============================================================================

/// Per-op outcome, recorded as an outcome *class* rather than a raw reply: retry
/// counts and error strings carry sim-timing noise, the class carries the
/// behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OpOutcome {
    Ok,
    Value(String),
    Missing,
    Count(i64),
    /// A refusal the server *chose* — `-READONLY`, `-NOREPLICAS`, `-SELFFENCE`.
    /// Distinguished from `Unreachable` because a fence engaging is behaviour,
    /// not a broken link.
    Refused(String),
    Unreachable,
}

impl OpOutcome {
    fn render(&self) -> String {
        match self {
            OpOutcome::Ok => "ok".to_string(),
            OpOutcome::Value(v) => format!("value={v}"),
            OpOutcome::Missing => "missing".to_string(),
            OpOutcome::Count(n) => format!("count={n}"),
            // The error *word*, not the message: the message carries offsets and
            // addresses that move with sim timing.
            OpOutcome::Refused(e) => {
                format!("refused:{}", e.split_whitespace().next().unwrap_or("ERR"))
            }
            OpOutcome::Unreachable => "unreachable".to_string(),
        }
    }
}

/// Shared between the in-sim driver client and the out-of-sim step loop.
#[derive(Default)]
struct Shared {
    history: History,
    /// One line per op, in order.
    op_lines: Vec<String>,
    /// Quiesce findings, already filtered to hard violations.
    violations: Vec<Violation>,
    /// Final converged value per touched key, for the fingerprint.
    final_values: BTreeMap<usize, String>,
    /// Lifetime PSYNC tallies per node, sampled at quiesce.
    sync_counts: SyncCounts,
    /// Set once the driver has finished setup, so no fault arms against a
    /// topology whose replicas have not linked yet.
    ready: bool,
    /// Set if the driver returned an error rather than completing.
    driver_error: Option<String>,
}

/// How each `PSYNC` in a run resolved, summed over every node: `INFO stats`'
/// `sync_full` / `sync_partial_ok` / `sync_partial_err`.
///
/// The *realized* outcome of the boundary a schedule asked for. A run can size
/// its backlog for a `+CONTINUE` and still take a full resync — the reconnect
/// may simply have arrived later than the schedule imagined — so a claim to
/// have covered the partial-sync boundaries has to be read off the servers'
/// own tallies, not off the config that was meant to produce them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncCounts {
    pub full: u64,
    pub partial_ok: u64,
    pub partial_err: u64,
}

impl SyncCounts {
    /// Full syncs beyond the one each replica necessarily does at boot.
    pub fn resyncs(&self) -> u64 {
        self.full.saturating_sub(REPLICA_COUNT as u64)
    }
}

/// Run one seed end to end, returning its outcome (violations included, not
/// asserted).
pub fn run_seed(seed: u64) -> RunOutcome {
    run_seed_instrumented(seed, Duration::ZERO).0
}

/// [`run_seed`], also reporting how the run's `PSYNC`s actually resolved.
///
/// Kept off [`RunOutcome`] (which [`super::schedule`] owns and both arms share)
/// and out of the fingerprint: the tallies are *coverage* evidence, not part of
/// the run's identity, and folding a counter into the fingerprint would make
/// every reconnect-count wobble read as a determinism failure.
pub fn run_seed_instrumented(seed: u64, real_step_stretch: Duration) -> (RunOutcome, SyncCounts) {
    run_seed_inner(seed, real_step_stretch)
}

/// [`run_seed`], with each `sim.step()` followed by `real_step_stretch` of
/// *real* sleeping.
///
/// The stretch never touches the simulated clock, so a run's outcome must not
/// move when it changes: every deadline the servers evaluate is supposed to be
/// read off turmoil's virtual clock. What it does change is the ratio between
/// real and simulated time, which is exactly what a loaded host does to the
/// sweep — the divergence cluster-correctness issue 23 is about. Running the
/// replication topology under the same stretch is the second data point D9 asks
/// for.
pub fn run_seed_stretched(seed: u64, real_step_stretch: Duration) -> RunOutcome {
    run_seed_inner(seed, real_step_stretch).0
}

/// Is `REPLICATION_SEED_TRACE` set? The single reader of the variable.
fn seed_trace_enabled() -> bool {
    std::env::var_os("REPLICATION_SEED_TRACE").is_some()
}

fn run_seed_inner(seed: u64, real_step_stretch: Duration) -> (RunOutcome, SyncCounts) {
    let schedule = Schedule::from_seed(seed);

    // The schedule half of the fingerprint, printed *before* the sim runs. A
    // seed that panics inside the sim never reaches the dump at the bottom of
    // this function, and that is exactly the seed whose family, boundary and
    // fault list you need in order to know where to look. It is derived from
    // the seed alone, so printing it early adds nothing and hides nothing.
    if seed_trace_enabled() {
        eprintln!("--- seed {seed} schedule ---");
        for line in schedule.render() {
            eprintln!("{line}");
        }
    }

    let mut sim = Builder::new()
        .simulation_duration(schedule.sim_duration)
        .min_message_latency(Duration::from_millis(schedule.base_latency_ms))
        .max_message_latency(Duration::from_millis(schedule.max_latency_ms))
        // Same turmoil-0.7.1 port-budget rationale as the cluster arm: a held
        // edge parks in-flight dials, and each parked dial holds an ephemeral
        // port until it completes on release.
        .ephemeral_ports(2048..=65535)
        .tcp_capacity(65536)
        .rng_seed(seed)
        .enable_random_order()
        .build();

    let dirs = spawn_scheduled_hosts(&mut sim, &schedule);

    let shared = Arc::new(Mutex::new(Shared::default()));
    let driver_shared = shared.clone();
    let driver_schedule = schedule.clone();

    sim.client("driver", async move {
        if let Err(e) = drive(driver_schedule, driver_shared.clone()).await {
            driver_shared.lock().expect("shared").driver_error = Some(e.to_string());
        }
        Ok::<(), DriverError>(())
    });

    step_with_faults(&mut sim, &schedule, &shared, real_step_stretch);
    drop(dirs);

    let mut state = std::mem::take(&mut *shared.lock().expect("shared"));
    // Which nodes the schedule SIGKILLed, resolved the same way the step loop
    // resolved them, so `LEADER` maps to the same host the faults were applied
    // to. `check_cross_node`'s issue-21 gap keys off this.
    state.history.restarted = schedule
        .resolve(BOOT_PRIMARY)
        .faults
        .iter()
        .filter_map(|f| match f.kind {
            FaultKind::CrashRestart { node } => Some(node),
            _ => None,
        })
        .collect();

    let mut violations = state.violations;
    violations.extend(check_cross_node(&state.history));
    if let Some(err) = &state.driver_error {
        violations.push(Violation {
            id: "XREPL-DRIVER-1",
            detail: format!("driver client aborted before quiesce: {err}"),
        });
    }

    let mut fingerprint = schedule.render();
    fingerprint.extend(state.op_lines);
    for (key, value) in &state.final_values {
        fingerprint.push(format!("final {}={value}", KEYS[*key]));
    }
    for v in &violations {
        fingerprint.push(format!("violation {v}"));
    }

    // Triage affordance: a sweep reports only the seed and its violations (500
    // fingerprints would drown the failure), so `REPLICATION_SEED_TRACE=1`
    // replays one seed with its whole fingerprint on stderr.
    if seed_trace_enabled() {
        eprintln!("--- seed {seed} fingerprint ---");
        for line in &fingerprint {
            eprintln!("{line}");
        }
        // Per-round node views, trace-only: they are the raw material for every
        // XREPL-2 verdict, but they stay out of the fingerprint because a
        // reconnect-count wobble in them would read as a determinism failure.
        for round in &state.history.rounds {
            for v in &round.views {
                eprintln!(
                    "  view round={} node={} role={} replid={} offset={} slaves={}",
                    round.seq,
                    v.observer,
                    if v.is_primary { "master" } else { "slave" },
                    v.replid,
                    v.offset,
                    v.connected_slaves,
                );
            }
        }
        eprintln!("--- end seed {seed} ---");
    }

    (
        RunOutcome {
            seed,
            fingerprint,
            violations,
        },
        state.sync_counts,
    )
}

/// Run one seed and panic on any violation. The form every test uses.
pub fn assert_seed_clean(seed: u64) -> RunOutcome {
    let outcome = run_seed(seed);
    assert_clean(seed, &outcome);
    outcome
}

/// Panic with the seed's whole schedule if `outcome` reported anything.
fn assert_clean(seed: u64, outcome: &RunOutcome) {
    if outcome.violations.is_empty() {
        return;
    }
    let schedule = Schedule::from_seed(seed);
    panic!(
        "seed {seed} (family {}) violated {} invariant(s):\n{}\n\nschedule:\n{}\n",
        schedule.family.as_str(),
        outcome.violations.len(),
        outcome
            .violations
            .iter()
            .map(|v| format!("  - {v}"))
            .collect::<Vec<_>>()
            .join("\n"),
        schedule
            .render()
            .iter()
            .map(|l| format!("  {l}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

/// Register the primary and its replicas with the schedule's per-run knobs.
fn spawn_scheduled_hosts(
    sim: &mut turmoil::Sim<'_>,
    schedule: &Schedule,
) -> Vec<tempfile::TempDir> {
    let dirs: Vec<tempfile::TempDir> = (0..NODE_COUNT)
        .map(|_| tempfile::tempdir().expect("replication node data dir"))
        .collect();

    let toggles = schedule.toggles;
    let backlog_size = toggles.backlog_size(schedule.family);

    for (idx, host) in REPLICATION_HOSTS.iter().enumerate() {
        let host = host.to_string();
        // One level down inside the node's own tempdir, never the tempdir
        // itself: a full sync stages its payload in a `checkpoint_ready`
        // directory that is a *sibling* of the data dir
        // (`persistence::rocks::staged::STAGED_CHECKPOINT_DIR`), so a data dir
        // sitting directly in `$TMPDIR` makes every node in every concurrently
        // running seed stage into one shared path. That is what the sweep's
        // first run hit: "staged checkpoint ... is incomplete (missing CURRENT
        // manifest)" plus cross-run keyspace bleed. Same reason
        // `test-harness`'s `create_temp_dir` nests.
        let path = dirs[idx].path().join("data");
        std::fs::create_dir_all(&path).expect("replication node data dir");
        sim.host(host.clone(), move || {
            let path = path.clone();
            let host = host.clone();
            async move {
                let params = ReplicationNodeParams {
                    num_shards: 1,
                    primary_ip: (idx != BOOT_PRIMARY)
                        .then(|| turmoil::lookup(REPLICATION_HOSTS[BOOT_PRIMARY])),
                    persistence: toggles.payload.persistence(),
                    backlog_size,
                    min_replicas_to_write: toggles.min_replicas_to_write,
                    // Tight enough that a `SlowReplica` edge actually pushes the
                    // replica out of the good set within one hold.
                    min_replicas_timeout_ms: 500,
                    self_fence_on_replica_loss: toggles.self_fence,
                    replica_freshness_timeout_ms: 500,
                    replication_lag_threshold_secs: 1,
                };
                if let Err(e) = real_frogdb_replication_node(params, path).await {
                    eprintln!("replication node {host} exited with error: {e}");
                    return Err(e);
                }
                Ok(())
            }
        });
    }
    dirs
}

/// The manual step loop: arm and heal every episode at its simulated instant.
///
/// Fault timing is read off `sim.elapsed()`, never a host clock, so a replay of
/// the same seed injects the same fault at the same simulated instant. The
/// origin is the moment the replicas linked rather than `t = 0`: a fault landing
/// before the first stream exists is a different (and far less interesting)
/// scenario than the one the schedule describes.
fn step_with_faults(
    sim: &mut turmoil::Sim<'_>,
    schedule: &Schedule,
    shared: &Arc<Mutex<Shared>>,
    real_step_stretch: Duration,
) {
    // The primary is host `BOOT_PRIMARY` by construction, so unlike the cluster
    // arm there is nothing to discover — only readiness to wait for.
    let faults = schedule.resolve(BOOT_PRIMARY).faults;
    let mut armed = vec![false; faults.len()];
    let mut healed = vec![false; faults.len()];
    let mut origin: Option<Duration> = None;
    let mut steps: u64 = 0;

    loop {
        let finished = sim.step().expect("turmoil step");
        if !real_step_stretch.is_zero() {
            std::thread::sleep(real_step_stretch);
        }

        if origin.is_none() && shared.lock().expect("shared").ready {
            origin = Some(sim.elapsed());
        }

        if let Some(origin) = origin {
            let now = sim.elapsed().saturating_sub(origin);
            for (i, f) in faults.iter().enumerate() {
                if !armed[i] && now >= f.arm_at {
                    apply_fault(
                        sim,
                        &REPLICATION_HOSTS,
                        schedule.base_latency_ms,
                        f.kind,
                        true,
                    );
                    armed[i] = true;
                }
                if armed[i] && !healed[i] && now >= f.heal_at {
                    apply_fault(
                        sim,
                        &REPLICATION_HOSTS,
                        schedule.base_latency_ms,
                        f.kind,
                        false,
                    );
                    healed[i] = true;
                }
            }
        }

        if finished {
            break;
        }
        steps += 1;
        assert!(
            steps < 20_000_000,
            "seed {}: scheduled replication sim did not finish",
            schedule.seed
        );
    }
}

// -----------------------------------------------------------------------------
// The driver client
// -----------------------------------------------------------------------------

type DriverError = Box<dyn std::error::Error + 'static>;

/// Bound on any convergence poll, in 100ms simulated steps.
const POLL_STEPS: usize = 300;

/// How long the driver waits for the boot topology to link before giving up.
const LINK_ATTEMPTS: u32 = 160;

async fn drive(schedule: Schedule, shared: Arc<Mutex<Shared>>) -> Result<(), DriverError> {
    // --- Setup: wait for both replicas to stream -----------------------------
    let mut linked = false;
    for _ in 0..LINK_ATTEMPTS {
        if connected_slaves(BOOT_PRIMARY).await.unwrap_or(0) as usize >= REPLICA_COUNT {
            linked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    if !linked {
        return Err(format!(
            "seed {}: replicas never linked to the boot primary",
            schedule.seed
        )
        .into());
    }

    let resolved = schedule.resolve(BOOT_PRIMARY);
    let start = tokio::time::Instant::now();
    shared.lock().expect("shared").ready = true;

    let mut primary = BOOT_PRIMARY;
    let mut round_seq: u64 = 0;
    let mut touched: BTreeSet<usize> = BTreeSet::new();
    let mut churn_marks = 0usize;

    for (i, op) in schedule.ops.iter().enumerate() {
        // Mark a churn event the first time each fault's arm or heal instant
        // passes, so the offset-monotonicity check knows the ground moved.
        let now = start.elapsed();
        let passed = resolved
            .faults
            .iter()
            .flat_map(|f| [f.arm_at, f.heal_at])
            .filter(|t| *t <= now)
            .count();
        if passed > churn_marks {
            churn_marks = passed;
            shared.lock().expect("shared").history.churn_at(round_seq);
        }

        let line = match *op {
            Op::Observe => {
                round_seq += 1;
                let round = observe_round(round_seq).await;
                shared.lock().expect("shared").history.rounds.push(round);
                format!("op[{i}] observe round={round_seq}")
            }
            Op::Write { key } => {
                let value = format!("v{i}");
                let outcome =
                    exec_on(primary, &[b"SET", KEYS[key].as_bytes(), value.as_bytes()]).await;
                let mut confirmed = 0u32;
                if outcome == OpOutcome::Ok {
                    touched.insert(key);
                    // One `WAIT` straight after the ack, both to learn how
                    // widely the write landed (which scopes XREPL-1) and to
                    // sample WAIT against `connected_slaves` (XREPL-3).
                    let sample = wait_sample(primary, round_seq, REPLICA_COUNT as u32, 400).await;
                    confirmed = sample.answered.max(0) as u32;
                    let mut guard = shared.lock().expect("shared");
                    guard.history.waits.push(sample);
                    guard.history.writes.push(AckedWrite {
                        seq: round_seq,
                        key,
                        value: value.clone(),
                        node: primary,
                        confirmed_replicas: confirmed,
                        at: start.elapsed(),
                    });
                }
                format!(
                    "op[{i}] write {} -> {} confirmed={confirmed}",
                    KEYS[key],
                    outcome.render()
                )
            }
            Op::Read { key, node } => {
                let outcome = exec_on(node, &[b"GET", KEYS[key].as_bytes()]).await;
                let value = match &outcome {
                    OpOutcome::Value(v) => Some(v.clone()),
                    _ => None,
                };
                shared.lock().expect("shared").history.reads.push(NodeRead {
                    seq: round_seq,
                    node,
                    key,
                    value,
                });
                format!("op[{i}] read {}@{node} -> {}", KEYS[key], outcome.render())
            }
            Op::Wait {
                numreplicas,
                timeout_ms,
            } => {
                let sample = wait_sample(primary, round_seq, numreplicas, timeout_ms).await;
                shared.lock().expect("shared").history.waits.push(sample);
                // The *count* depends on ACK timing the schedule does not pin,
                // so the fingerprint records only that the call answered.
                let class = if sample.answered >= 0 {
                    "count"
                } else {
                    "no-answer"
                };
                format!("op[{i}] wait n={numreplicas} -> {class}")
            }
            Op::Promote { node } => {
                let outcome = exec_on(node, &[b"REPLICAOF", b"NO", b"ONE"]).await;
                if outcome == OpOutcome::Ok {
                    primary = node;
                    let mut guard = shared.lock().expect("shared");
                    guard.history.promotions.push(Promotion {
                        seq: round_seq,
                        node,
                    });
                    guard.history.churn_at(round_seq);
                }
                format!("op[{i}] promote {node} -> {}", outcome.render())
            }
            Op::Demote { node } => {
                if node == primary {
                    // Demoting the current primary toward itself is a no-op the
                    // schedule can express (the promotion it follows may have
                    // been refused) and the server would reject.
                    format!("op[{i}] demote {node} -> skipped")
                } else {
                    let ip = turmoil::lookup(REPLICATION_HOSTS[primary]).to_string();
                    let port = SERVER_PORT.to_string();
                    let outcome =
                        exec_on(node, &[b"REPLICAOF", ip.as_bytes(), port.as_bytes()]).await;
                    if outcome == OpOutcome::Ok {
                        shared.lock().expect("shared").history.churn_at(round_seq);
                    }
                    format!("op[{i}] demote {node} -> {}", outcome.render())
                }
            }
        };
        shared.lock().expect("shared").op_lines.push(line);
        tokio::time::sleep(Duration::from_millis(schedule.op_gap_ms)).await;
    }

    // --- Quiesce -------------------------------------------------------------
    //
    // Wait past the last heal, then let the topology reconverge. Everything
    // below is an assertion about the *settled* system.
    let deadline = resolved.last_heal() + Duration::from_millis(500);
    while start.elapsed() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut findings: Vec<Violation> = Vec::new();
    let mut final_values: BTreeMap<usize, String> = BTreeMap::new();

    // 1. No acked-write loss on the node that is primary at quiesce, and
    //    convergence of every other reachable node onto the same value.
    let checkable: Vec<(usize, Vec<String>)> = {
        let guard = shared.lock().expect("shared");
        guard
            .history
            .last_writes()
            .keys()
            .copied()
            .filter_map(|key| {
                guard.history.settleable_values(&schedule, key).map(|vs| {
                    (
                        key,
                        vs.into_iter().map(str::to_string).collect::<Vec<String>>(),
                    )
                })
            })
            .collect()
    };
    for (key, accepted) in &checkable {
        let mut settled = None;
        for _ in 0..POLL_STEPS {
            if let Some(v) = converged_value(*key).await
                && accepted.contains(&v)
            {
                settled = Some(v);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        match settled {
            Some(v) => {
                final_values.insert(*key, v);
            }
            None => findings.push(Violation {
                id: "XREPL-1",
                detail: format!(
                    "{} never settled on any value the topology was allowed to end on ({}) \
                     after every fault healed; nodes replied: {}",
                    KEYS[*key],
                    accepted.join(" | "),
                    probe_replies(*key).await
                ),
            }),
        }
    }

    // 2. The promoted node's own keyspace, for every write every replica had
    //    confirmed before the promotion. Recorded rather than asserted here —
    //    `check_cross_node` turns a mismatch into `XREPL-1`, so the claim is
    //    unit-testable without a sim.
    {
        let promotions: Vec<Promotion> = shared.lock().expect("shared").history.promotions.clone();
        for promotion in promotions {
            let expected: Vec<(usize, String)> = {
                let guard = shared.lock().expect("shared");
                guard
                    .history
                    .writes
                    .iter()
                    .filter(|w| w.seq < promotion.seq && w.survives_promotion())
                    .filter(|w| guard.history.acked_write_is_checkable(&schedule, w))
                    .map(|w| (w.key, w.value.clone()))
                    // The last such write per key is the one the promoted node
                    // must hold; earlier ones were legitimately overwritten.
                    .collect::<BTreeMap<usize, String>>()
                    .into_iter()
                    .collect()
            };
            for (key, value) in expected {
                let got = match exec_on(promotion.node, &[b"GET", KEYS[key].as_bytes()]).await {
                    OpOutcome::Value(v) => Some(v),
                    _ => None,
                };
                shared
                    .lock()
                    .expect("shared")
                    .history
                    .promoted_readbacks
                    .push(PromotedReadback {
                        node: promotion.node,
                        key,
                        expected: value,
                        got,
                    });
            }
        }
    }

    // 3. The invariant catalog on every surviving node (issue 03's
    //    `DEBUG REPLICATION CHECK`). A node that cannot be reached at all is
    //    reported rather than skipped: after every heal, every node is up.
    for idx in 0..NODE_COUNT {
        match debug_replication_check(idx).await {
            Ok(reported) => {
                for v in hard_violations(reported, &excepted_catalog_ids()) {
                    findings.push(Violation {
                        id: v.id,
                        detail: format!("node {idx}: {}", v.detail),
                    });
                }
            }
            Err(e) => findings.push(Violation {
                id: "XREPL-CHECK-1",
                detail: format!(
                    "node {idx} did not answer DEBUG REPLICATION CHECK at quiesce: {e}"
                ),
            }),
        }
    }

    // 4. A final observation round, so the cross-node history always ends with a
    //    post-heal sample of the settled topology, plus the PSYNC tallies that
    //    say which partial-sync boundary the run actually landed on.
    round_seq += 1;
    let round = observe_round(round_seq).await;
    let sync_counts = sync_counts().await;
    {
        let mut guard = shared.lock().expect("shared");
        guard.history.rounds.push(round);
        guard.violations = findings;
        guard.final_values = final_values;
        guard.sync_counts = sync_counts;
    }

    Ok(())
}

/// One `WAIT`, bracketed by `connected_slaves` reads.
///
/// The bracket is the whole point: `WAIT`'s answer is only checkable against the
/// set of replicas that could have been connected *while it ran*, and either
/// endpoint alone would be a race.
async fn wait_sample(node: usize, seq: u64, numreplicas: u32, timeout_ms: u64) -> WaitSample {
    let connected_before = connected_slaves(node).await.unwrap_or(0);
    let n = numreplicas.to_string();
    let t = timeout_ms.to_string();
    let answered = match exec_on(node, &[b"WAIT", n.as_bytes(), t.as_bytes()]).await {
        OpOutcome::Count(n) => n,
        // A `WAIT` that never answered has nothing to over-count with. Recorded
        // as zero rather than skipped so the sample count stays a function of
        // the schedule.
        _ => 0,
    };
    let connected_after = connected_slaves(node).await.unwrap_or(0);
    WaitSample {
        seq,
        node,
        answered,
        connected_before,
        connected_after,
    }
}

/// `connected_slaves` from one node's `INFO replication`.
async fn connected_slaves(node: usize) -> Option<u32> {
    info_replication(node)
        .await?
        .get("connected_slaves")?
        .parse()
        .ok()
}

/// One `INFO <section>` from one node, parsed into its `field: value` map.
async fn info_section(node: usize, section: &[u8]) -> Option<BTreeMap<String, String>> {
    let ip = turmoil::lookup(REPLICATION_HOSTS[node]);
    let mut conn = RespConn::connect((ip, SERVER_PORT)).await.ok()?;
    let RespValue::Bulk(Some(bytes)) = conn.cmd(&[b"INFO", section]).await.ok()? else {
        return None;
    };
    Some(
        String::from_utf8_lossy(&bytes)
            .lines()
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
            .collect(),
    )
}

/// One node's `INFO replication`.
async fn info_replication(node: usize) -> Option<BTreeMap<String, String>> {
    info_section(node, b"replication").await
}

/// Sum every node's lifetime `PSYNC` tallies from `INFO stats`.
///
/// Summed rather than kept per-node because which node served a resync moves
/// with the promotions, and the question these answer — did this run realize a
/// `+CONTINUE`, a `+FULLRESYNC`, a refusal — is about the run, not the node.
async fn sync_counts() -> SyncCounts {
    let mut total = SyncCounts::default();
    for idx in 0..NODE_COUNT {
        let Some(fields) = info_section(idx, b"stats").await else {
            continue;
        };
        let read = |name: &str| -> u64 {
            fields
                .get(name)
                .and_then(|v| v.parse().ok())
                .unwrap_or_default()
        };
        total.full += read("sync_full");
        total.partial_ok += read("sync_partial_ok");
        total.partial_err += read("sync_partial_err");
    }
    total
}

/// Sample `INFO replication` from every host.
///
/// A round is not an atomic snapshot: each node costs a connect and a command,
/// and the primary keeps producing offsets in between — replication pings alone
/// advance `master_repl_offset`. So a replica read *after* the primary can
/// honestly report an offset above the primary's already-stale sample, which is
/// indistinguishable from the real defect `XREPL-2a` looks for. Sampling the
/// primary a second time, after every other node, removes the ambiguity in the
/// only direction that matters: its offset is then read strictly later than
/// every replica's, so it bounds them, and any remaining excess is the replica
/// holding a history the primary never produced.
///
/// The re-read replaces the first sample rather than being maxed with it, so a
/// primary that rewinds mid-round is still reported rewound.
async fn observe_round(seq: u64) -> Round {
    let mut views = Vec::new();
    for idx in 0..NODE_COUNT {
        if let Some(view) = observe_node(idx).await {
            views.push(view);
        }
    }
    let primaries: Vec<usize> = views
        .iter()
        .filter(|v| v.is_primary)
        .map(|v| v.observer)
        .collect();
    for idx in primaries {
        if let Some(fresh) = observe_node(idx).await
            && let Some(slot) = views.iter_mut().find(|v| v.observer == idx)
        {
            *slot = fresh;
        }
    }
    Round { seq, views }
}

async fn observe_node(idx: usize) -> Option<NodeView> {
    let fields = info_replication(idx).await?;
    Some(NodeView {
        observer: idx,
        is_primary: fields.get("role").map(String::as_str) == Some("master"),
        replid: fields.get("master_replid").cloned().unwrap_or_default(),
        offset: fields
            .get("master_repl_offset")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        connected_slaves: fields
            .get("connected_slaves")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
    })
}

/// Execute `parts` against one named node, classifying the reply.
///
/// No redirect following: this is a standalone replication topology, so the node
/// a command is sent to is the node that answers it or refuses it.
async fn exec_on(node: usize, parts: &[&[u8]]) -> OpOutcome {
    let ip = turmoil::lookup(REPLICATION_HOSTS[node]);
    let Ok(mut conn) = RespConn::connect((ip, SERVER_PORT)).await else {
        return OpOutcome::Unreachable;
    };
    match conn.cmd(parts).await {
        Err(_) => OpOutcome::Unreachable,
        Ok(RespValue::Simple(_)) => OpOutcome::Ok,
        Ok(RespValue::Bulk(Some(b))) => OpOutcome::Value(String::from_utf8_lossy(&b).into_owned()),
        Ok(RespValue::Bulk(None)) => OpOutcome::Missing,
        Ok(RespValue::Int(n)) => OpOutcome::Count(n),
        Ok(RespValue::Error(e)) => OpOutcome::Refused(e),
        Ok(_) => OpOutcome::Refused("ERR unexpected reply shape".to_string()),
    }
}

/// The value every reachable node agrees `key` holds, or `None` if they do not
/// agree — or if any of them cannot be reached, which a settled topology never
/// is.
async fn converged_value(key: usize) -> Option<String> {
    let mut agreed: Option<String> = None;
    for idx in 0..NODE_COUNT {
        match exec_on(idx, &[b"GET", KEYS[key].as_bytes()]).await {
            OpOutcome::Value(v) => match &agreed {
                Some(seen) if *seen != v => return None,
                Some(_) => {}
                None => agreed = Some(v),
            },
            _ => return None,
        }
    }
    agreed
}

/// One `GET` plus role per node, rendered — the diagnostic behind a convergence
/// failure. [`converged_value`] answers only "agreed / not agreed", so without
/// this a failing seed reports that the nodes disagreed but not *how*.
async fn probe_replies(key: usize) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(NODE_COUNT);
    for idx in 0..NODE_COUNT {
        let value = exec_on(idx, &[b"GET", KEYS[key].as_bytes()]).await.render();
        let role = match info_replication(idx).await {
            Some(fields) => format!(
                "role={} replid={} offset={}",
                fields.get("role").cloned().unwrap_or_default(),
                fields
                    .get("master_replid")
                    .map(|r| r.chars().take(8).collect::<String>())
                    .unwrap_or_default(),
                fields
                    .get("master_repl_offset")
                    .cloned()
                    .unwrap_or_default(),
            ),
            None => "<no INFO>".to_string(),
        };
        parts.push(format!("n{idx}={value} [{role}]"));
    }
    parts.join("  ")
}

/// `DEBUG REPLICATION CHECK` against one host, parsed back into violations.
async fn debug_replication_check(node: usize) -> std::io::Result<Vec<Violation>> {
    let ip = turmoil::lookup(REPLICATION_HOSTS[node]);
    let mut conn = RespConn::connect((ip, SERVER_PORT)).await?;
    match conn.cmd(&[b"DEBUG", b"REPLICATION", b"CHECK"]).await? {
        RespValue::Array(Some(items)) => {
            let known = catalog_ids();
            Ok(items
                .iter()
                .filter_map(|item| parse_check_entry(item, &known, UNKNOWN_CHECK_ID))
                .collect())
        }
        RespValue::Error(e) => Err(std::io::Error::other(e)),
        other => Err(std::io::Error::other(format!(
            "DEBUG REPLICATION CHECK returned {other:?}"
        ))),
    }
}

// =============================================================================
// Tests
// =============================================================================

/// Seeds the default suite always runs: a small smoke sweep, so the arm itself
/// cannot rot between nightlies.
///
/// One seed per family, the lowest that derives it.
/// `test_replication_smoke_seeds_cover_every_family` asserts the coverage and
/// prints the replacement list, so a change to the derivation that shifts family
/// assignment fails loudly instead of quietly narrowing coverage.
const SMOKE_SEEDS: [u64; 7] = [1, 2, 3, 5, 7, 16, 17];

#[test]
fn test_replication_derivation_is_pure() {
    for seed in 0..64u64 {
        let a = Schedule::from_seed(seed);
        let b = Schedule::from_seed(seed);
        assert_eq!(a, b, "Schedule::from_seed({seed}) is not a pure function");
        assert_eq!(a.render(), b.render());
    }
}

#[test]
fn test_replication_distinct_seeds_give_distinct_schedules() {
    // Not a uniqueness guarantee — a collision in the derivation is allowed —
    // but a sweep whose seeds nearly all collapsed onto one schedule would
    // search nothing, so hold the line at "mostly distinct".
    let rendered: BTreeSet<Vec<String>> = (0..200u64)
        .map(|s| Schedule::from_seed(s).render())
        .collect();
    assert!(
        rendered.len() > 180,
        "200 seeds produced only {} distinct schedules",
        rendered.len()
    );
}

#[test]
fn test_replication_smoke_seeds_cover_every_family() {
    let covered: BTreeSet<Family> = SMOKE_SEEDS
        .iter()
        .map(|&s| Schedule::from_seed(s).family)
        .collect();
    let missing: Vec<&str> = Family::ALL
        .iter()
        .filter(|f| !covered.contains(f))
        .map(|f| f.as_str())
        .collect();
    // Regenerate by scanning upward: for each family, the lowest seed >= 1 that
    // derives it.
    let suggestion: Vec<u64> = Family::ALL
        .iter()
        .map(|f| {
            (1u64..10_000)
                .find(|&s| Schedule::from_seed(s).family == *f)
                .unwrap_or(0)
        })
        .collect();
    assert!(
        missing.is_empty(),
        "SMOKE_SEEDS no longer cover {missing:?}; set it to {suggestion:?}"
    );
}

#[test]
fn test_replication_faults_stay_inside_their_budget() {
    for seed in 0..500u64 {
        let s = Schedule::from_seed(seed);
        for f in &s.faults {
            assert!(
                f.arm_at >= Duration::from_millis(ReplicationArm::BUDGET.min_arm_ms),
                "seed {seed}"
            );
            assert!(f.heal_at > f.arm_at, "seed {seed}: empty fault window");
            assert!(
                f.heal_at - f.arm_at
                    <= Duration::from_millis(ReplicationArm::BUDGET.max_fault_ms()),
                "seed {seed}: fault window exceeds the budget"
            );
            if let FaultKind::HoldEdge { a, b } | FaultKind::SlowEdge { a, b, .. } = f.kind {
                assert_ne!(a, b, "seed {seed}: self-edge fault would be a no-op");
            }
        }
        let crashes: Vec<&FaultEpisode> = s.faults.iter().filter(|f| f.kind.is_crash()).collect();
        for (i, a) in crashes.iter().enumerate() {
            for b in &crashes[i + 1..] {
                assert!(
                    a.heal_at <= b.arm_at || b.heal_at <= a.arm_at,
                    "seed {seed}: overlapping crash episodes"
                );
            }
        }
    }
}

/// Every family's own knob has to actually be reachable across a sweep-sized
/// seed range, or a family is a name for a scenario nothing runs.
#[test]
fn test_replication_sweep_reaches_every_boundary_and_payload_shape() {
    let mut boundaries: BTreeSet<&str> = BTreeSet::new();
    let mut payloads: BTreeSet<&str> = BTreeSet::new();
    let mut interrupts: BTreeSet<&str> = BTreeSet::new();
    let mut link_drop_boundaries: BTreeSet<&str> = BTreeSet::new();
    let mut full_sync_payloads: BTreeSet<&str> = BTreeSet::new();
    let mut full_sync_interrupts: BTreeSet<&str> = BTreeSet::new();
    for seed in 0..500u64 {
        let s = Schedule::from_seed(seed);
        boundaries.insert(s.toggles.boundary.as_str());
        payloads.insert(s.toggles.payload.as_str());
        interrupts.insert(s.toggles.interrupt.as_str());
        if s.family == Family::LinkDrop {
            link_drop_boundaries.insert(s.toggles.boundary.as_str());
        }
        if s.family == Family::FullSyncInterrupt {
            full_sync_payloads.insert(s.toggles.payload.as_str());
            full_sync_interrupts.insert(s.toggles.interrupt.as_str());
        }
    }
    assert_eq!(boundaries.len(), 3, "{boundaries:?}");
    assert_eq!(payloads.len(), 2, "{payloads:?}");
    assert_eq!(interrupts.len(), 2, "{interrupts:?}");
    // The three partial-sync boundaries and the two payload shapes are
    // acceptance criteria of issue 12, so assert they are reached *in the family
    // that is about them*, not merely somewhere in the sweep.
    assert_eq!(
        link_drop_boundaries.len(),
        3,
        "LinkDrop never reaches all three partial-sync boundaries: {link_drop_boundaries:?}"
    );
    assert_eq!(
        full_sync_payloads.len(),
        2,
        "FullSyncInterrupt never reaches both payload shapes: {full_sync_payloads:?}"
    );
    assert_eq!(
        full_sync_interrupts.len(),
        2,
        "FullSyncInterrupt never reaches the post-trailer case: {full_sync_interrupts:?}"
    );
}

/// `FullSyncInterrupt` needs a full sync to interrupt: a reconnect granted
/// `+CONTINUE` never reaches the payload path, so the family forces the
/// narrowest ring whatever the boundary draw said.
#[test]
fn test_full_sync_interrupt_always_forces_a_resync_window() {
    for seed in 0..500u64 {
        let s = Schedule::from_seed(seed);
        if s.family != Family::FullSyncInterrupt {
            continue;
        }
        assert_eq!(
            s.toggles.backlog_size(s.family),
            Boundary::OutsideWindow.backlog_size(),
            "seed {seed}: FullSyncInterrupt must size the backlog out of the window"
        );
    }
    // And the other families keep the boundary they drew.
    let inside = (0..500u64)
        .map(Schedule::from_seed)
        .find(|s| s.family == Family::LinkDrop && s.toggles.boundary == Boundary::InsideWindow)
        .expect("a LinkDrop seed inside the window");
    assert_eq!(
        inside.toggles.backlog_size(inside.family),
        Boundary::InsideWindow.backlog_size()
    );
}

/// The promotion families schedule exactly one promotion, and the demote that
/// follows it points at a *different* node — a schedule that demoted the node it
/// just promoted would test nothing.
#[test]
fn test_promotion_schedules_one_promotion_followed_by_a_demote() {
    let mut seen = 0;
    for seed in 0..500u64 {
        let s = Schedule::from_seed(seed);
        if s.family != Family::PromotionMidStream {
            continue;
        }
        let promotes: Vec<usize> = s
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::Promote { node } => Some(*node),
                _ => None,
            })
            .collect();
        assert_eq!(promotes.len(), 1, "seed {seed}: {:?}", s.ops);
        assert_ne!(
            promotes[0], BOOT_PRIMARY,
            "seed {seed}: promoting the boot primary is a no-op"
        );
        // Every node but the promoted one is repointed, ex-primary first, so the
        // topology after the promotion is a star and never a chain.
        let demotes: Vec<usize> = s
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::Demote { node } => Some(*node),
                _ => None,
            })
            .collect();
        let mut expected: Vec<usize> = (0..NODE_COUNT).filter(|n| *n != promotes[0]).collect();
        expected.sort_by_key(|n| (*n != BOOT_PRIMARY, *n));
        assert_eq!(demotes, expected, "seed {seed}");
        // And the whole promotion tail fits inside the run.
        let last_demote = s
            .ops
            .iter()
            .rposition(|op| matches!(op, Op::Demote { .. }))
            .expect("a demote");
        assert!(
            last_demote < s.ops.len() - 1,
            "seed {seed}: the run ends on a repoint, leaving no op against the new primary"
        );
        seen += 1;
    }
    assert!(seen > 20, "only {seen} promotion seeds in 500");
}

// --- the pure cross-node checks ---

#[test]
fn test_cross_node_accepts_a_clean_history() {
    let history = History {
        rounds: vec![
            Round {
                seq: 1,
                views: vec![view(0, true, "A", 100, 2), view(1, false, "A", 80, 0)],
            },
            Round {
                seq: 2,
                views: vec![view(0, true, "A", 140, 2), view(1, false, "A", 140, 0)],
            },
        ],
        writes: vec![acked(1, 0, "v1", 2)],
        reads: vec![NodeRead {
            seq: 2,
            node: 1,
            key: 0,
            value: Some("v1".to_string()),
        }],
        waits: vec![wait(1, 0, 2, 2, 2)],
        ..History::default()
    };
    assert!(
        check_cross_node(&history).is_empty(),
        "{:?}",
        check_cross_node(&history)
    );
}

#[test]
fn test_xrepl_1_catches_a_confirmed_write_missing_from_the_promoted_node() {
    // alpha was written v1, then v7, then v9.
    let writes = || {
        vec![
            acked(1, 0, "v1", 2),
            acked(2, 0, "v7", 2),
            acked(3, 0, "v9", 2),
        ]
    };
    let readback = |got: Option<&str>| History {
        writes: writes(),
        promoted_readbacks: vec![PromotedReadback {
            node: 1,
            key: 0,
            expected: "v7".to_string(),
            got: got.map(str::to_string),
        }],
        ..History::default()
    };

    // Gone entirely.
    let found = check_cross_node(&readback(None));
    assert_eq!(
        found.iter().map(|v| v.id).collect::<Vec<_>>(),
        vec!["XREPL-1"]
    );
    assert!(found[0].detail.contains("alpha"), "{found:?}");
    assert!(found[0].detail.contains("<nil>"), "{found:?}");

    // Rolled back to the value the confirmed write replaced.
    let rolled = check_cross_node(&readback(Some("v1")));
    assert_eq!(
        rolled.iter().map(|v| v.id).collect::<Vec<_>>(),
        vec!["XREPL-1"]
    );
    assert!(rolled[0].detail.contains("earlier"), "{rolled:?}");

    // The confirmed value itself is fine.
    assert!(check_cross_node(&readback(Some("v7"))).is_empty());
}

/// The workload keeps writing after a promotion, so the promoted node holding a
/// *later* value for the key is the expected steady state, not a lost write.
/// Demanding equality here would make every promotion seed red.
#[test]
fn test_xrepl_1_accepts_a_write_that_was_legitimately_overwritten() {
    let history = History {
        writes: vec![acked(1, 0, "v7", 2), acked(2, 0, "v9", 2)],
        promoted_readbacks: vec![PromotedReadback {
            node: 1,
            key: 0,
            expected: "v7".to_string(),
            got: Some("v9".to_string()),
        }],
        ..History::default()
    };
    assert!(
        check_cross_node(&history).is_empty(),
        "{:?}",
        check_cross_node(&history)
    );
}

/// A write only the primary held is not promised to survive a promotion, so
/// only the fully-confirmed ones are readback candidates at all.
#[test]
fn test_only_fully_confirmed_writes_are_promotion_safe() {
    assert!(acked(1, 0, "v", REPLICA_COUNT as u32).survives_promotion());
    assert!(!acked(1, 0, "v", 0).survives_promotion());
    assert!(!acked(1, 0, "v", REPLICA_COUNT as u32 - 1).survives_promotion());
}

#[test]
fn test_xrepl_2_catches_a_replica_ahead_of_its_primary() {
    let history = History {
        rounds: vec![Round {
            seq: 3,
            views: vec![view(0, true, "A", 100, 1), view(1, false, "A", 101, 0)],
        }],
        ..History::default()
    };
    let found = check_cross_node(&history);
    assert_eq!(
        found.iter().map(|v| v.id).collect::<Vec<_>>(),
        vec!["XREPL-2"]
    );
    assert!(found[0].detail.contains("not a prefix"), "{found:?}");
}

/// The named gap for replication-correctness issue 24, pinned on the side that
/// makes it a gap: an id a restarted node was *observed* rewinding under is
/// exempt, in both the "replica ahead of its primary" and the "offset went
/// backwards" shapes, and on every node that id reaches — not only the one the
/// schedule killed.
#[test]
fn test_xrepl_2_exempts_a_replid_a_restart_rewound() {
    let rewound = History {
        rounds: vec![
            Round {
                seq: 1,
                views: vec![
                    view(0, true, "A", 240, 2),
                    view(1, false, "A", 240, 0),
                    view(2, false, "A", 240, 0),
                ],
            },
            // Node 0 was SIGKILLed and came back on the same id at the bottom;
            // node 2 has resynced onto the short history, node 1 has not yet.
            Round {
                seq: 5,
                views: vec![
                    view(0, true, "A", 132, 1),
                    view(1, false, "A", 240, 0),
                    view(2, false, "A", 132, 0),
                ],
            },
        ],
        restarted: BTreeSet::from([0]),
        ..History::default()
    };
    assert!(
        check_cross_node(&rewound).is_empty(),
        "the whole shadow of issue 24 is one gap: {:?}",
        check_cross_node(&rewound)
    );
}

/// The other side of the same pin: the gap keys on an *observed rewind*, not on
/// "this node was restarted at some point", so a live primary a replica has
/// genuinely overtaken is still reported even in a run that contained a restart.
#[test]
fn test_xrepl_2_gap_does_not_cover_a_primary_that_never_rewound() {
    let live_again = History {
        rounds: vec![Round {
            seq: 3,
            views: vec![view(0, true, "A", 12, 1), view(1, false, "A", 240, 0)],
        }],
        restarted: BTreeSet::from([0]),
        ..History::default()
    };
    assert_eq!(
        check_cross_node(&live_again)
            .iter()
            .map(|v| v.id)
            .collect::<Vec<_>>(),
        vec!["XREPL-2"],
        "the gap must not widen to any node that was ever restarted"
    );

    // And a rewind on a node the schedule never touched is not issue 24 either.
    let untouched = History {
        rounds: vec![
            Round {
                seq: 1,
                views: vec![view(0, true, "A", 240, 1), view(1, false, "A", 240, 0)],
            },
            Round {
                seq: 5,
                views: vec![view(0, true, "A", 240, 1), view(1, false, "A", 132, 0)],
            },
        ],
        restarted: BTreeSet::from([0]),
        ..History::default()
    };
    assert_eq!(
        check_cross_node(&untouched)
            .iter()
            .map(|v| v.id)
            .collect::<Vec<_>>(),
        vec!["XREPL-2"],
        "a replica rewinding under an id no restart rewound is a fresh finding"
    );
}

/// The panic-shaped named gap for replication-correctness issue 21, pinned both
/// ways: only `INV-OFFSET-3`'s acked-past-live branch is a known gap, and the
/// generic runtime message a task panic unwinds as is not — otherwise the gap
/// would swallow every panic the sweep can produce.
#[test]
fn test_known_panic_gap_matches_only_the_filed_signature() {
    assert!(known_panic_gap("INV-OFFSET-3: replica 2 acked 307 past live 278").is_some());
    assert!(
        known_panic_gap("INV-OFFSET-3: replica 2 resumes from 307 past live 278").is_none(),
        "the resume-floor branch is a different claim and is not filed"
    );
    assert!(
        known_panic_gap(
            "a spawned task panicked and the runtime is configured to shut down on unhandled panic"
        )
        .is_none()
    );
    assert!(known_panic_gap("INV-OFFSET-1: live offset went backwards").is_none());
}

/// The quiesce convergence claim is scoped to what the topology actually
/// promised. A write only the primary held is not promised to outlive a later
/// promotion, so the value written before it is a correct place to settle —
/// while with no promotion in the run the newest acked write is the only one.
#[test]
fn test_settleable_values_scopes_to_writes_a_promotion_could_not_drop() {
    // A schedule with no crash episode: the crash scoping is a separate claim
    // (`acked_write_is_checkable`) and this test is about the promotion one.
    // Asserted below rather than assumed.
    let schedule = Schedule::from_seed(317);
    let history = History {
        writes: vec![
            acked(2, 0, "v1", REPLICA_COUNT as u32),
            acked(6, 0, "v16", 0),
        ],
        promotions: vec![Promotion { seq: 9, node: 2 }],
        ..History::default()
    };
    assert!(
        history
            .writes
            .iter()
            .all(|w| history.acked_write_is_checkable(&schedule, w)),
        "no crash may be open over these writes, or the test measures the wrong scope"
    );

    assert_eq!(
        history.settleable_values(&schedule, 0),
        Some(vec!["v1", "v16"]),
        "a write no replica confirmed may be dropped by the promotion after it"
    );

    let no_promotion = History {
        promotions: Vec::new(),
        ..history.clone()
    };
    assert_eq!(
        no_promotion.settleable_values(&schedule, 0),
        Some(vec!["v16"]),
        "with no promotion, the newest acked write is the only settling place"
    );

    assert_eq!(
        history.settleable_values(&schedule, 1),
        None,
        "a key nobody wrote promises nothing"
    );
}

/// A replica still on an older replication id counts a *different* stream, so
/// its offset is not comparable with the current primary's.
#[test]
fn test_xrepl_2_ignores_a_replica_on_another_replid() {
    let history = History {
        rounds: vec![Round {
            seq: 3,
            views: vec![view(0, true, "B", 10, 1), view(1, false, "A", 900, 0)],
        }],
        ..History::default()
    };
    assert!(check_cross_node(&history).is_empty());
}

#[test]
fn test_xrepl_2_catches_an_offset_rewind_under_one_replid() {
    let history = History {
        rounds: vec![
            Round {
                seq: 1,
                views: vec![view(0, true, "A", 500, 1)],
            },
            Round {
                seq: 2,
                views: vec![view(0, true, "A", 400, 1)],
            },
        ],
        ..History::default()
    };
    let found = check_cross_node(&history);
    assert_eq!(
        found.iter().map(|v| v.id).collect::<Vec<_>>(),
        vec!["XREPL-2"]
    );
    assert!(found[0].detail.contains("went backwards"), "{found:?}");
}

/// A new replid is a new history: starting it lower is a full resync, not a
/// rewind. And a rewind inside a churn settling window is a resync in flight.
#[test]
fn test_xrepl_2_allows_a_rewind_across_a_new_replid_or_a_churn_event() {
    let new_lineage = History {
        rounds: vec![
            Round {
                seq: 1,
                views: vec![view(0, false, "A", 500, 0)],
            },
            Round {
                seq: 2,
                views: vec![view(0, false, "B", 12, 0)],
            },
        ],
        ..History::default()
    };
    assert!(check_cross_node(&new_lineage).is_empty());

    let across_churn = History {
        rounds: vec![
            Round {
                seq: 1,
                views: vec![view(0, true, "A", 500, 1)],
            },
            Round {
                seq: 3,
                views: vec![view(0, true, "A", 400, 1)],
            },
        ],
        churn: vec![2],
        ..History::default()
    };
    assert!(check_cross_node(&across_churn).is_empty());
}

#[test]
fn test_xrepl_2_catches_a_value_nobody_wrote() {
    let history = History {
        writes: vec![acked(1, 0, "v1", 2)],
        reads: vec![NodeRead {
            seq: 2,
            node: 1,
            key: 0,
            value: Some("phantom".to_string()),
        }],
        ..History::default()
    };
    let found = check_cross_node(&history);
    assert_eq!(
        found.iter().map(|v| v.id).collect::<Vec<_>>(),
        vec!["XREPL-2"]
    );
    assert!(
        found[0].detail.contains("no client ever wrote"),
        "{found:?}"
    );

    // A nil read is a legitimate prefix — the replica simply has not applied it.
    let nil = History {
        writes: vec![acked(1, 0, "v1", 2)],
        reads: vec![NodeRead {
            seq: 2,
            node: 1,
            key: 0,
            value: None,
        }],
        ..History::default()
    };
    assert!(check_cross_node(&nil).is_empty());
}

/// Spec GAP-5 at level 4: `WAIT` may never answer with more replicas than were
/// connected, and never with more than the topology contains.
#[test]
fn test_xrepl_3_catches_wait_over_counting_connected_slaves() {
    let history = History {
        waits: vec![wait(1, 0, 2, 1, 1)],
        ..History::default()
    };
    let found = check_cross_node(&history);
    assert_eq!(
        found.iter().map(|v| v.id).collect::<Vec<_>>(),
        vec!["XREPL-3"]
    );
    assert!(found[0].detail.contains("not there"), "{found:?}");
}

/// A replica that dropped *during* the call legitimately leaves the after-sample
/// below the answer, and one that attached during it leaves the before-sample
/// below. The bracket, not either endpoint, is the bound.
#[test]
fn test_xrepl_3_accepts_a_replica_that_joined_or_left_inside_the_call() {
    let left = History {
        waits: vec![wait(1, 0, 2, 2, 0)],
        ..History::default()
    };
    assert!(check_cross_node(&left).is_empty());
    let joined = History {
        waits: vec![wait(1, 0, 2, 0, 2)],
        ..History::default()
    };
    assert!(check_cross_node(&joined).is_empty());
}

#[test]
fn test_xrepl_3_catches_an_answer_beyond_the_whole_topology() {
    // Both endpoints wide, so only the topology bound can catch it — which is
    // the point: a `connected_slaves` that is itself wrong cannot launder a
    // `WAIT` answer past the number of replicas that exist.
    let history = History {
        waits: vec![wait(1, 0, REPLICA_COUNT as i64 + 1, 9, 9)],
        ..History::default()
    };
    let found = check_cross_node(&history);
    assert_eq!(
        found.iter().map(|v| v.id).collect::<Vec<_>>(),
        vec!["XREPL-3"]
    );
    assert!(found[0].detail.contains("only"), "{found:?}");
}

#[test]
fn test_xrepl_3_catches_a_negative_answer() {
    let history = History {
        waits: vec![wait(1, 0, -1, 2, 2)],
        ..History::default()
    };
    let found = check_cross_node(&history);
    assert_eq!(
        found.iter().map(|v| v.id).collect::<Vec<_>>(),
        vec!["XREPL-3"]
    );
    assert!(found[0].detail.contains("not a replica count"), "{found:?}");
}

#[test]
fn test_hard_violations_drops_the_catalogs_documented_exceptions() {
    let excepted = excepted_catalog_ids();
    assert!(
        !excepted.is_empty(),
        "the replication catalog has no DOCUMENTED-EXCEPTION entries; this filter would be \
         dead code"
    );
    let hard = asserted_catalog_ids();
    let mut reported: Vec<Violation> = excepted
        .iter()
        .map(|id| Violation {
            id,
            detail: "excepted".to_string(),
        })
        .collect();
    reported.push(Violation {
        id: hard[0],
        detail: "hard".to_string(),
    });
    let kept = hard_violations(reported, &excepted);
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].id, hard[0]);
}

#[test]
fn test_acked_writes_are_unchecked_only_while_a_crash_is_open() {
    // These sims run the data plane without persistence unless the payload
    // shape asks for it, so a write acked while a SIGKILL episode is open may
    // legitimately vanish — but one acked after the last restart may not.
    let mut schedule = Schedule::from_seed(0);
    schedule.faults = vec![FaultEpisode {
        arm_at: Duration::from_millis(1_000),
        heal_at: Duration::from_millis(3_000),
        kind: FaultKind::CrashRestart { node: 1 },
    }];
    let history = History::default();
    let during = acked_at(Duration::from_millis(2_000));
    let after = acked_at(Duration::from_millis(4_000));
    assert!(!history.acked_write_is_checkable(&schedule, &during));
    assert!(history.acked_write_is_checkable(&schedule, &after));
}

#[test]
fn test_fault_resolve_binds_the_primary_sentinel() {
    let resolved = FaultKind::HoldIsolate { node: LEADER }.resolve(BOOT_PRIMARY, NODE_COUNT);
    assert_eq!(resolved, FaultKind::HoldIsolate { node: BOOT_PRIMARY });
    // Binding must never produce a self-edge, which turmoil would drop.
    let edge = FaultKind::HoldEdge {
        a: LEADER,
        b: BOOT_PRIMARY,
    }
    .resolve(BOOT_PRIMARY, NODE_COUNT);
    let FaultKind::HoldEdge { a, b } = edge else {
        panic!("kind changed under resolve");
    };
    assert_ne!(a, b);
}

#[test]
fn test_parse_check_entry_reads_a_replication_catalog_id() {
    let known = asserted_catalog_ids()[0];
    let entry = RespValue::Array(Some(vec![
        RespValue::Bulk(Some(b"id".to_vec())),
        RespValue::Bulk(Some(known.as_bytes().to_vec())),
        RespValue::Bulk(Some(b"detail".to_vec())),
        RespValue::Bulk(Some(b"the window is half-cleared".to_vec())),
    ]));
    let v = parse_check_entry(&entry, &catalog_ids(), UNKNOWN_CHECK_ID).expect("a violation");
    assert_eq!(v.id, known);
    assert_eq!(v.detail, "the window is half-cleared");

    // An id the catalog does not know is surfaced, never dropped.
    let unknown = RespValue::Array(Some(vec![
        RespValue::Bulk(Some(b"id".to_vec())),
        RespValue::Bulk(Some(b"INV-NOPE-9".to_vec())),
        RespValue::Bulk(Some(b"detail".to_vec())),
        RespValue::Bulk(Some(b"?".to_vec())),
    ]));
    assert_eq!(
        parse_check_entry(&unknown, &catalog_ids(), UNKNOWN_CHECK_ID)
            .expect("surfaced")
            .id,
        UNKNOWN_CHECK_ID
    );
}

/// PRD §8 D9: no regression-seed file and no `EXPECTED-FAILURE` muzzle may be
/// committed for this arm while cluster-correctness issue 23 is open, because a
/// muzzle is a claim about reproducibility. Asserted rather than left to a
/// reviewer, so adding the file without reading D9 is a red test rather than a
/// quiet policy break.
#[test]
fn test_no_regression_seed_file_is_committed_while_cluster_issue_23_is_open() {
    let path = std::path::Path::new(file!())
        .parent()
        .expect("the simulation directory")
        .join("replication-regression-seeds.txt");
    assert!(
        !path.exists(),
        "{} exists, but replication-correctness PRD §8 D9 holds the regression file until \
         cluster-correctness issue 23 (same-seed fingerprint diverges under host load) closes. \
         When it closes, delete this test along with the hold.",
        path.display()
    );
}

// --- helpers for the pure tests above ---

fn view(
    observer: usize,
    is_primary: bool,
    replid: &str,
    offset: u64,
    connected_slaves: u32,
) -> NodeView {
    NodeView {
        observer,
        is_primary,
        replid: replid.to_string(),
        offset,
        connected_slaves,
    }
}

fn acked(seq: u64, key: usize, value: &str, confirmed_replicas: u32) -> AckedWrite {
    AckedWrite {
        seq,
        key,
        value: value.to_string(),
        node: 0,
        confirmed_replicas,
        at: Duration::from_millis(seq * 100),
    }
}

fn acked_at(at: Duration) -> AckedWrite {
    AckedWrite {
        seq: 0,
        key: 0,
        value: "v".to_string(),
        node: 0,
        confirmed_replicas: 0,
        at,
    }
}

fn wait(seq: u64, node: usize, answered: i64, before: u32, after: u32) -> WaitSample {
    WaitSample {
        seq,
        node,
        answered,
        connected_before: before,
        connected_after: after,
    }
}

// --- the sim-backed tests ---

/// Real time added per simulated step in the determinism replay.
///
/// A step simulates 1ms and costs a few hundred microseconds of real time, so
/// half a millisecond of sleep is enough to invert the ratio: real time now
/// outruns simulated time, and any duration measured off the OS clock reads
/// several times larger in the replay than in the first run.
const REPLAY_REAL_STRETCH: Duration = Duration::from_micros(500);

/// Same seed → same run. The arm's core contract: two runs of one seed produce
/// byte-identical fingerprints, so a failing seed replays exactly.
///
/// The replay runs *slowed down in real time* ([`run_seed_stretched`]) while the
/// first run does not, so the two disagree about wall clock and agree about
/// simulated time — the only difference a busy CI box makes to a turmoil sweep,
/// and the difference cluster-correctness issue 23 is about. Running it on a
/// second topology is the evidence PRD §8 D9 asks for.
#[test]
fn test_replication_scheduler_same_seed_same_run() {
    let seed = SMOKE_SEEDS[0];
    let a = run_seed(seed);
    let b = run_seed_stretched(seed, REPLAY_REAL_STRETCH);
    assert_fingerprints_equal(seed, &a.fingerprint, &b.fingerprint);
}

/// Seed 204 read a replica 66 offsets *ahead of its primary* and called it
/// `XREPL-2a`, because [`observe_round`] sampled the primary first and the
/// replica three connects later. The gap was sampling skew, not a lost prefix.
/// Pinned here rather than left to the nightly budget: the ordering rule in
/// `observe_round` is invisible at the call site, and a round that samples the
/// primary early again fails this in the default suite instead of a month later.
#[test]
fn test_sampling_skew_does_not_read_as_a_replica_ahead_of_its_primary() {
    let outcome = run_seed(SAMPLING_SKEW_SEED);
    assert_clean(SAMPLING_SKEW_SEED, &outcome);
}

/// See [`test_sampling_skew_does_not_read_as_a_replica_ahead_of_its_primary`].
const SAMPLING_SKEW_SEED: u64 = 204;

/// The default-suite smoke sweep: one seed per fault family, so the arm cannot
/// rot between nightly sweeps.
///
/// It also witnesses that the partial-sync boundaries are *reached*, not merely
/// configured. `test_replication_sweep_reaches_every_boundary_and_payload_shape`
/// is a claim about the derivation — which backlog size a seed asks for — and a
/// run can size its backlog for a `+CONTINUE` and still take a full resync. The
/// tallies below are the servers' own, so they say which grant actually
/// happened; without them "LinkDrop covers three partial-sync boundaries" would
/// be a statement about a config field.
#[test]
fn test_replication_scheduler_smoke_sweep() {
    let mut total = SyncCounts::default();
    let mut per_seed: Vec<(u64, Family, SyncCounts)> = Vec::new();
    for seed in SMOKE_SEEDS {
        let (outcome, counts) = run_seed_instrumented(seed, Duration::ZERO);
        assert_clean(seed, &outcome);
        total.full += counts.full;
        total.partial_ok += counts.partial_ok;
        total.partial_err += counts.partial_err;
        per_seed.push((seed, Schedule::from_seed(seed).family, counts));
    }

    let report = || {
        per_seed
            .iter()
            .map(|(seed, family, c)| {
                format!(
                    "  seed {seed} ({}): full={} partial_ok={} partial_err={}",
                    family.as_str(),
                    c.full,
                    c.partial_ok,
                    c.partial_err
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    // Every replica takes one full sync at boot, so `full` is only evidence of a
    // *re*sync past that floor.
    assert!(
        total.resyncs() > 0,
        "no smoke seed ever forced a full resync, so the outside-the-window \
         partial-sync boundary and both FullSyncInterrupt payload shapes went \
         untested:\n{}",
        report()
    );
    assert!(
        total.partial_ok > 0,
        "no smoke seed was ever granted a +CONTINUE, so the inside-the-window \
         partial-sync boundary went untested — either the backlog sizing no longer \
         reaches it or every reconnect is being refused:\n{}",
        report()
    );
}

thread_local! {
    /// The first panic message seen on this thread since [`take_first_panic`]
    /// last cleared it.
    static FIRST_PANIC: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

/// Install the panic hook that feeds [`take_first_panic`], once per process.
///
/// An invariant hook inside a server task panics on the seed's own thread, and
/// the sim's tokio runtime is configured to shut down on an unhandled task
/// panic — so what finally unwinds out of `sim.step()` is the generic "a spawned
/// task panicked and the runtime is configured to shut down on unhandled panic".
/// The message that names the invariant is gone by then, and a panicking seed
/// reports nothing anybody can act on. This keeps the *first* message per
/// thread, which is the original one: the `RefCell already borrowed` cascade
/// that follows a panic inside a current-thread runtime is all downstream of it.
/// Seeds run one at a time per worker thread, so there is no cross-seed bleed.
///
/// Chained onto the default hook rather than replacing it, so the raw panic
/// output still reaches stderr.
fn arm_panic_capture() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let payload = info.payload();
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()));
            if let Some(message) = message {
                // `try_with`: a panic during thread teardown finds the local
                // already destroyed, and an `AccessError` there would abort the
                // process instead of reporting the panic.
                let _ = FIRST_PANIC.try_with(|slot| {
                    let mut slot = slot.borrow_mut();
                    if slot.is_none() {
                        *slot = Some(message);
                    }
                });
            }
            default(info);
        }));
    });
}

/// Take and clear this thread's first captured panic message.
fn take_first_panic() -> Option<String> {
    FIRST_PANIC
        .try_with(|slot| slot.borrow_mut().take())
        .ok()
        .flatten()
}

/// Panic signatures a **filed, open** defect produces — the panic form of
/// [`check_cross_node`]'s named gaps, for a defect that aborts the run instead
/// of leaving a violation behind to be reported.
///
/// One entry, as narrow as the message allows: `INV-OFFSET-3`'s
/// acked-past-live branch, which is
/// [replication-correctness issue 21](../../../../../.scratch/replication-correctness/issues/open/21-ack-above-live-head.md).
/// The catalog hook that raises it is `#[cfg(any(test, debug_assertions))]`, so
/// this is a debug-build assertion on a Hard-tier invariant rather than a
/// release crash.
///
/// This is not an `EXPECTED-FAILURE` seed muzzle — PRD §8 D9 holds those until
/// the *cluster-correctness* campaign's issue 23 closes. It names a *signature*,
/// not a seed: it cannot hide a seed that fails some other way, and the day
/// replication issue 21 is fixed the signature stops occurring and this function
/// stops matching anything.
fn known_panic_gap(message: &str) -> Option<&'static str> {
    (message.contains("INV-OFFSET-3") && message.contains("acked") && message.contains("past live"))
        .then_some(
            "replication-correctness issue 21 (a replica's REPLCONF ACK is credited past the \
         primary's live offset)",
        )
}

/// The seeded sweep (`just replication-seeds`). `#[ignore]`d so the default
/// suite runs only the smoke sweep above; the nightly workflow runs this one
/// with `--run-ignored all`.
///
/// Budget comes from `REPLICATION_SEEDS` (count) and `REPLICATION_SEEDS_START`
/// (offset); worker threads from `REPLICATION_SEEDS_JOBS`. All three have laptop
/// defaults, so the recipe works with no environment at all.
///
/// There is no muzzle list to skip (PRD §8 D9), so every failing seed is
/// reported. When cluster-correctness issue 23 closes and the regression file
/// lands, the skip goes here, exactly as in the cluster arm.
#[test]
#[ignore = "sweep: run via `just replication-seeds`"]
fn test_replication_scheduler_seed_sweep() {
    let count: u64 = env_u64("REPLICATION_SEEDS", 500);
    let start: u64 = env_u64("REPLICATION_SEEDS_START", 1);
    let jobs: u64 = env_u64("REPLICATION_SEEDS_JOBS", 4).max(1);

    arm_panic_capture();
    let failures: Arc<Mutex<Vec<(u64, String)>>> = Arc::new(Mutex::new(Vec::new()));
    // Seeds that reached a known, filed defect that aborts the run
    // ([`known_panic_gap`]). Reported, not asserted on: the sweep exists to
    // surface what is *not* already filed.
    let gapped: Arc<Mutex<Vec<(u64, &'static str)>>> = Arc::new(Mutex::new(Vec::new()));
    std::thread::scope(|scope| {
        for worker in 0..jobs {
            let failures = failures.clone();
            let gapped = gapped.clone();
            scope.spawn(move || {
                let mut i = worker;
                while i < count {
                    let seed = start + i;
                    // Each run brings up three real servers; a panic in one seed
                    // must not abort the sweep, so the failure is captured and
                    // reported alongside every other failing seed at the end.
                    let _ = take_first_panic();
                    match std::panic::catch_unwind(|| run_seed(seed)) {
                        Ok(outcome) if outcome.violations.is_empty() => {}
                        Ok(outcome) => {
                            let detail = outcome
                                .violations
                                .iter()
                                .map(|v| v.to_string())
                                .collect::<Vec<_>>()
                                .join("; ");
                            failures.lock().expect("failures").push((seed, detail));
                        }
                        Err(panic) => {
                            // The hook's message in preference to the payload:
                            // the payload is the runtime's generic
                            // shutdown-on-task-panic text, the hook's is the
                            // panic that caused it.
                            let detail = take_first_panic()
                                .or_else(|| panic.downcast_ref::<String>().cloned())
                                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                                .unwrap_or_else(|| "<non-string panic>".to_string());
                            match known_panic_gap(&detail) {
                                Some(gap) => gapped.lock().expect("gapped").push((seed, gap)),
                                None => failures
                                    .lock()
                                    .expect("failures")
                                    .push((seed, format!("panic: {detail}"))),
                            }
                        }
                    }
                    i += jobs;
                }
            });
        }
    });

    let mut gapped = gapped.lock().expect("gapped").clone();
    gapped.sort_by_key(|(seed, _)| *seed);
    if !gapped.is_empty() {
        let mut by_gap: BTreeMap<&'static str, Vec<u64>> = BTreeMap::new();
        for (seed, gap) in &gapped {
            by_gap.entry(gap).or_default().push(*seed);
        }
        for (gap, seeds) in by_gap {
            eprintln!(
                "{} of {count} seeds stopped at a known gap — {gap}: seeds {}",
                seeds.len(),
                seeds
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }

    let mut failures = failures.lock().expect("failures").clone();
    failures.sort_by_key(|(seed, _)| *seed);
    assert!(
        failures.is_empty(),
        "{} of {count} seeds failed (from {start}); replay one with \
         REPLICATION_SEED_TRACE=1:\n{}",
        failures.len(),
        failures
            .iter()
            .map(|(seed, detail)| format!("  seed {seed}: {detail}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
