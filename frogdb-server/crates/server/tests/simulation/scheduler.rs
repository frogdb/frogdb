//! Seed-driven fault scheduler for the turmoil cluster sims (cluster-correctness
//! PRD §3 W4, issue 09).
//!
//! # What this replaces
//!
//! The cluster sims used to be hand-scripted scenarios: each hard-coded which
//! edge to hold, when to hold it, and what to assert. A scripted scenario can
//! only find the bug it was written for. Here a single `u64` derives the *whole*
//! run — the fault family, which links are held or slowed and when, which node
//! is SIGKILLed and when it restarts, the per-node Raft timer skew, and the
//! client workload — so the space of schedules is searched rather than
//! enumerated by hand.
//!
//! # Determinism is the contract
//!
//! Same seed → same run. Everything a schedule contains is drawn from one
//! [`StdRng::seed_from_u64`] in a fixed order; nothing reads the wall clock, and
//! every collection the derivation or the checkers iterate is ordered
//! (`Vec`/`BTreeMap`/`BTreeSet`, never `HashMap`). Faults are applied from the
//! manual `sim.step()` loop against `sim.elapsed()`, the *simulated* clock, so a
//! replay arms them at the same simulated instant.
//!
//! One input a seed could not pin had to be closed for this to be true:
//! openraft draws each election timeout from `rand::thread_rng()`, seeded from
//! OS entropy. `cluster_init.rs` collapses the election-timeout window to a
//! single value under the `turmoil` feature so that draw is constant, and the
//! schedule skews `election_timeout_ms` *per node* instead — deterministic
//! tie-breaking rather than unseeded jitter.
//!
//! [`RunOutcome::fingerprint`] is the assertable form of the contract: a
//! canonical line-per-event rendering of a run, compared by
//! `test_cluster_scheduler_same_seed_same_run`.
//!
//! # What is checked
//!
//! Three layers, all reported as [`Violation`] so the shape is identical to the
//! catalog's:
//!
//! 1. **The invariant catalog on every surviving node**, via `DEBUG CLUSTER
//!    CHECK` (issue 06) at quiesce. That command reports every tier, so
//!    [`hard_violations`] drops the catalog's own DOCUMENTED-EXCEPTION ids — a
//!    documented exception is a ruling, not a defect.
//! 2. **Client-visible assertions**: single-owner convergence for every touched
//!    slot after heal, and no acked-write loss (scoped by
//!    [`History::acked_write_is_checkable`] — the data plane runs without
//!    persistence in these sims, so a SIGKILL of the owner legitimately loses
//!    its keys, and a slot migration moves ownership without moving keys).
//! 3. **Cross-node checks a single-state catalog cannot express**
//!    ([`check_cross_node`]): pairwise epoch monotonicity as observed by
//!    clients, and single-writer-per-slot over the whole run history.
//!
//! # Durability faults are out of scope
//!
//! turmoil has no disk model, so lose-unsynced-writes-on-kill rides campaign
//! 2's crash harness, not this scheduler (issue 09). `CrashRestart` here is a
//! process death and restart against an intact data directory.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use frogdb_cluster::{Tier, Violation, invariants::CATALOG};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use turmoil::Builder;

use super::{
    CLUSTER_HOSTS, RespConn, RespValue, attach_cluster_replica, node_id_of, owner_host_of,
    parse_redirect, single_owner_soft, wait_cluster_ready,
};
use crate::common::sim_helpers::{
    CLUSTER_BUS_PORT, ClusterNodeParams, SERVER_PORT, real_frogdb_cluster_node_with,
};

/// Nodes in every scheduled cluster. Three: the smallest topology with a real
/// Raft majority, which is what makes "the isolated side cannot commit" a
/// meaningful fault rather than a total outage.
pub const NODE_COUNT: usize = CLUSTER_HOSTS.len();

/// The workload key pool, spread over the slot space so a flat three-way split
/// puts at least one key on each node.
const KEYS: [&str; 6] = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];

/// Sentinel node index meaning "whichever node is the Raft leader at setup".
/// Bound by [`FaultKind::resolve`]; never appears in a resolved schedule.
pub const LEADER: usize = usize::MAX;

/// Longest a schedule may hold a fault. Short on purpose: every held edge parks
/// in-flight dials on turmoil ephemeral ports for the duration.
const MAX_FAULT_MS: u64 = 5_000;
/// Earliest a fault may arm, measured from the moment the cluster is ready.
const MIN_ARM_MS: u64 = 300;

// =============================================================================
// Schedule: the pure, seed-derived description of a run
// =============================================================================

/// The shape of fault a schedule injects. One family per scripted scenario the
/// scheduler subsumes, plus `Mixed`, which composes them.
///
/// Drawn from the seed *first* so a sweep covers every shape by construction
/// rather than by luck: at 500 seeds each family gets roughly 83 runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Family {
    /// No faults, latency skew only. The MOVED/ASK redirect-convergence
    /// scenario (former `test_cluster_moved_redirect_convergence`).
    Healthy,
    /// The Raft leader is held away from both peers, typically while a slot
    /// migration is in flight (former
    /// `test_cluster_leader_partition_mid_migration_converges`).
    LeaderIsolation,
    /// A single `leader ↔ victim` edge is held with `auto_failover` on, so both
    /// sides keep quorum through the third node while the leader's probe of a
    /// still-client-reachable peer times out (shape of
    /// `test_cluster_asymmetric_partition_false_failover`).
    AsymmetricEdge,
    /// A PSYNC replica is attached and its data-plane edge to the primary is
    /// held while `WAIT` runs (shape of the two `test_cluster_wait_*` sims).
    ReplicaPartition,
    /// One node is SIGKILLed mid-workload and restarted on the same data
    /// directory.
    CrashRestart,
    /// Two or three episodes of any kind, possibly overlapping — schedules no
    /// scripted sim ever wrote down.
    Mixed,
}

impl Family {
    /// Every family, in the order the seed selects from. Changing this list
    /// renumbers which family a seed maps to, so the regression-seed file
    /// records each seed's family alongside it.
    pub const ALL: [Family; 6] = [
        Family::Healthy,
        Family::LeaderIsolation,
        Family::AsymmetricEdge,
        Family::ReplicaPartition,
        Family::CrashRestart,
        Family::Mixed,
    ];

    /// Stable token used in fingerprints, failure messages and the seed file.
    pub fn as_str(self) -> &'static str {
        match self {
            Family::Healthy => "healthy",
            Family::LeaderIsolation => "leader-isolation",
            Family::AsymmetricEdge => "asymmetric-edge",
            Family::ReplicaPartition => "replica-partition",
            Family::CrashRestart => "crash-restart",
            Family::Mixed => "mixed",
        }
    }
}

/// One injected fault. Node positions are *indices into [`CLUSTER_HOSTS`]*, not
/// Raft node ids: a schedule is derived before the cluster exists, so "the
/// leader" travels as [`LEADER`] until [`FaultKind::resolve`] binds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultKind {
    /// Queue traffic on the `a`↔`b` edge (turmoil `hold`/`release`).
    ///
    /// `hold` rather than `partition` throughout: turmoil 0.7.1 leaks an
    /// ephemeral port for every *cancelled* dial, which a sustained partition
    /// exhausts, whereas a held dial stays pending and completes on release.
    /// The peer is still starved of heartbeats past its election timeout, which
    /// is the property under test.
    HoldEdge { a: usize, b: usize },
    /// Queue traffic between `node` and both peers.
    HoldIsolate { node: usize },
    /// Raise the `a`↔`b` link latency for the window, healed back to the
    /// schedule's baseline.
    SlowEdge { a: usize, b: usize, latency_ms: u64 },
    /// SIGKILL `node` (turmoil `crash`) and restart it at the heal (`bounce`),
    /// reusing the same data directory so the Raft log survives.
    CrashRestart { node: usize },
}

impl FaultKind {
    /// Bind the [`LEADER`] sentinel to the discovered leader index, collapsing
    /// any self-edge onto a distinct peer.
    fn resolve(self, leader: usize) -> Self {
        let fix = |i: usize| if i == LEADER { leader } else { i };
        match self {
            FaultKind::HoldEdge { a, b } => {
                let (a, b) = distinct(fix(a), fix(b));
                FaultKind::HoldEdge { a, b }
            }
            FaultKind::HoldIsolate { node } => FaultKind::HoldIsolate { node: fix(node) },
            FaultKind::SlowEdge { a, b, latency_ms } => {
                let (a, b) = distinct(fix(a), fix(b));
                FaultKind::SlowEdge { a, b, latency_ms }
            }
            FaultKind::CrashRestart { node } => FaultKind::CrashRestart { node: fix(node) },
        }
    }

    /// Stable rendering for fingerprints.
    fn render(self) -> String {
        match self {
            FaultKind::HoldEdge { a, b } => format!("hold-edge {a}-{b}"),
            FaultKind::HoldIsolate { node } => format!("hold-isolate {node}"),
            FaultKind::SlowEdge { a, b, latency_ms } => format!("slow-edge {a}-{b} {latency_ms}ms"),
            FaultKind::CrashRestart { node } => format!("crash-restart {node}"),
        }
    }

    /// True for the kinds that take a node's process down.
    fn is_crash(self) -> bool {
        matches!(self, FaultKind::CrashRestart { .. })
    }
}

/// Force two node indices apart so an edge fault is never a self-loop, which
/// turmoil treats as a no-op and would silently drop.
fn distinct(a: usize, b: usize) -> (usize, usize) {
    if a == b {
        (a, (a + 1) % NODE_COUNT)
    } else {
        (a, b)
    }
}

/// One fault episode: armed at `arm_at`, healed at `heal_at`, both measured in
/// simulated time from the instant the cluster became ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultEpisode {
    pub arm_at: Duration,
    pub heal_at: Duration,
    pub kind: FaultKind,
}

/// Per-node Raft timers. Skewing these across nodes is what makes leader
/// election deterministic once the jitter window is collapsed (module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeTimers {
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
}

/// One client-workload step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// `SET` following redirects; a `+OK` is an acked write.
    Write { key: usize },
    /// `GET` following redirects.
    Read { key: usize },
    /// Move the key's slot to another node through the three-step
    /// `CLUSTER SETSLOT IMPORTING`/`MIGRATING`/`NODE` dance.
    Migrate { key: usize },
    /// `WAIT <numreplicas> <timeout>`.
    Wait { numreplicas: u32, timeout_ms: u64 },
    /// Sample `CLUSTER NODES` + `CLUSTER INFO` from every node, feeding the
    /// cross-node history.
    Observe,
}

impl Op {
    fn render(self) -> String {
        match self {
            Op::Write { key } => format!("write {}", KEYS[key]),
            Op::Read { key } => format!("read {}", KEYS[key]),
            Op::Migrate { key } => format!("migrate {}", KEYS[key]),
            Op::Wait {
                numreplicas,
                timeout_ms,
            } => format!("wait {numreplicas} {timeout_ms}"),
            Op::Observe => "observe".to_string(),
        }
    }
}

/// The complete, seed-derived description of a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    pub seed: u64,
    pub family: Family,
    pub auto_failover: bool,
    /// Attach the last host as a PSYNC replica before the workload starts.
    pub attach_replica: bool,
    pub timers: [NodeTimers; NODE_COUNT],
    /// Baseline link latency for the whole simulated network.
    pub base_latency_ms: u64,
    pub max_latency_ms: u64,
    /// Faults, sorted by `(arm_at, heal_at, kind)`. May overlap (`Mixed`).
    pub faults: Vec<FaultEpisode>,
    pub ops: Vec<Op>,
    /// Simulated gap between successive workload ops.
    pub op_gap_ms: u64,
    /// turmoil's wall for the whole run — the hang guard, not the expected
    /// length.
    pub sim_duration: Duration,
}

impl Schedule {
    /// Derive the whole schedule from `seed`.
    ///
    /// Draw order is fixed and every branch consumes from the same [`StdRng`],
    /// so this is a pure function of the seed — asserted by
    /// `test_scheduler_derivation_is_pure`.
    pub fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);

        let family = Family::ALL[rng.random_range(0..Family::ALL.len())];

        // Timer skew: a distinct election timeout per node, all well above the
        // heartbeat interval, so the lowest-timeout reachable node wins any
        // election without an unseeded jitter draw.
        let heartbeat_interval_ms = rng.random_range(40..=60u64);
        let base_election_ms = rng.random_range(280..=360u64);
        let step_ms = rng.random_range(30..=70u64);
        let mut timers = [NodeTimers {
            election_timeout_ms: base_election_ms,
            heartbeat_interval_ms,
        }; NODE_COUNT];
        for (i, t) in timers.iter_mut().enumerate() {
            t.election_timeout_ms = base_election_ms + (i as u64) * step_ms;
        }

        let base_latency_ms = rng.random_range(1..=8u64);
        let max_latency_ms = base_latency_ms + rng.random_range(1..=20u64);

        let auto_failover = matches!(family, Family::AsymmetricEdge | Family::ReplicaPartition)
            || (family == Family::Mixed && rng.random_range(0..2u32) == 0);
        let attach_replica = family == Family::ReplicaPartition
            || (family == Family::Mixed && rng.random_range(0..5u32) < 2);

        let faults = derive_faults(family, &mut rng);

        // The workload must outlive the last heal, so every run ends with a
        // quiesce window in which the network is whole again.
        let fault_end_ms = faults
            .iter()
            .map(|f| f.heal_at.as_millis() as u64)
            .max()
            .unwrap_or(0);
        let op_gap_ms = rng.random_range(60..=200u64);
        let op_count = ((fault_end_ms + 2_000) / op_gap_ms).clamp(12, 60) as usize;
        let ops = derive_ops(family, attach_replica, op_count, &mut rng);

        Schedule {
            seed,
            family,
            auto_failover,
            attach_replica,
            timers,
            base_latency_ms,
            max_latency_ms,
            faults,
            ops,
            op_gap_ms,
            sim_duration: Duration::from_secs(300),
        }
    }

    /// Simulated instant after which no fault is armed, relative to readiness.
    pub fn last_heal(&self) -> Duration {
        self.faults
            .iter()
            .map(|f| f.heal_at)
            .max()
            .unwrap_or(Duration::ZERO)
    }

    /// True if a SIGKILL episode is still open at `at` — i.e. a write acked
    /// then may legitimately be lost, because these sims run with the data
    /// plane's persistence disabled.
    pub fn crash_open_at(&self, at: Duration) -> bool {
        self.faults
            .iter()
            .any(|f| f.kind.is_crash() && f.heal_at >= at)
    }

    /// Bind the [`LEADER`] sentinel against the discovered leader index.
    pub fn resolve(&self, leader: usize) -> Self {
        let mut resolved = self.clone();
        for f in &mut resolved.faults {
            f.kind = f.kind.resolve(leader);
        }
        resolved
    }

    /// Canonical rendering, one line per field — the opening block of every run
    /// fingerprint, so a *schedule* divergence is reported before any run
    /// divergence.
    pub fn render(&self) -> Vec<String> {
        let mut lines = vec![
            format!("seed {}", self.seed),
            format!("family {}", self.family.as_str()),
            format!("auto_failover {}", self.auto_failover),
            format!("attach_replica {}", self.attach_replica),
            format!(
                "latency base={}ms max={}ms",
                self.base_latency_ms, self.max_latency_ms
            ),
            format!("op_gap {}ms", self.op_gap_ms),
        ];
        for (i, t) in self.timers.iter().enumerate() {
            lines.push(format!(
                "timers[{i}] election={}ms heartbeat={}ms",
                t.election_timeout_ms, t.heartbeat_interval_ms
            ));
        }
        for (i, f) in self.faults.iter().enumerate() {
            lines.push(format!(
                "fault[{i}] {} arm={}ms heal={}ms",
                f.kind.render(),
                f.arm_at.as_millis(),
                f.heal_at.as_millis()
            ));
        }
        for (i, op) in self.ops.iter().enumerate() {
            lines.push(format!("op[{i}] {}", op.render()));
        }
        lines
    }
}

/// Family-specific fault derivation. Every branch draws from the same `rng`.
fn derive_faults(family: Family, rng: &mut StdRng) -> Vec<FaultEpisode> {
    fn episode(rng: &mut StdRng, kind: FaultKind, out: &mut Vec<FaultEpisode>) {
        let arm = MIN_ARM_MS + rng.random_range(0..=1_200u64);
        let hold = rng.random_range(1_200..=MAX_FAULT_MS);
        out.push(FaultEpisode {
            arm_at: Duration::from_millis(arm),
            heal_at: Duration::from_millis(arm + hold),
            kind,
        });
    }

    let mut faults: Vec<FaultEpisode> = Vec::new();
    match family {
        Family::Healthy => {}
        Family::LeaderIsolation => {
            episode(rng, FaultKind::HoldIsolate { node: LEADER }, &mut faults);
        }
        Family::AsymmetricEdge => {
            let victim = rng.random_range(0..NODE_COUNT);
            episode(
                rng,
                FaultKind::HoldEdge {
                    a: LEADER,
                    b: victim,
                },
                &mut faults,
            );
        }
        Family::ReplicaPartition => {
            // The replica is always the last host (see `drive`); hold its
            // data-plane edge to one of the other two, which is where its
            // primary lives.
            let primary = rng.random_range(0..NODE_COUNT - 1);
            episode(
                rng,
                FaultKind::HoldEdge {
                    a: primary,
                    b: NODE_COUNT - 1,
                },
                &mut faults,
            );
        }
        Family::CrashRestart => {
            let node = rng.random_range(0..NODE_COUNT);
            episode(rng, FaultKind::CrashRestart { node }, &mut faults);
        }
        Family::Mixed => {
            let count = rng.random_range(2..=3usize);
            for _ in 0..count {
                let kind = match rng.random_range(0..4u32) {
                    0 => FaultKind::HoldIsolate {
                        node: rng.random_range(0..NODE_COUNT),
                    },
                    1 => {
                        let (a, b) = distinct(
                            rng.random_range(0..NODE_COUNT),
                            rng.random_range(0..NODE_COUNT),
                        );
                        FaultKind::HoldEdge { a, b }
                    }
                    2 => {
                        let (a, b) = distinct(
                            rng.random_range(0..NODE_COUNT),
                            rng.random_range(0..NODE_COUNT),
                        );
                        FaultKind::SlowEdge {
                            a,
                            b,
                            latency_ms: rng.random_range(50..=400u64),
                        }
                    }
                    _ => FaultKind::CrashRestart {
                        node: rng.random_range(0..NODE_COUNT),
                    },
                };
                episode(rng, kind, &mut faults);
            }
        }
    }

    prune_concurrent_crashes(faults)
}

/// A `Mixed` schedule can draw two crashes with overlapping windows, which
/// takes a three-node cluster below quorum and turns the run into a timeout
/// rather than a test. Drop any crash episode overlapping one already kept.
fn prune_concurrent_crashes(faults: Vec<FaultEpisode>) -> Vec<FaultEpisode> {
    let mut kept: Vec<FaultEpisode> = Vec::new();
    for f in faults {
        let clashes = f.kind.is_crash()
            && kept
                .iter()
                .any(|k| k.kind.is_crash() && f.arm_at < k.heal_at && k.arm_at < f.heal_at);
        if !clashes {
            kept.push(f);
        }
    }
    kept.sort_by_key(|f| (f.arm_at, f.heal_at, f.kind));
    kept
}

/// Workload derivation. Every family writes and reads; migrations and `WAIT`
/// only appear where they mean something.
fn derive_ops(family: Family, attach_replica: bool, count: usize, rng: &mut StdRng) -> Vec<Op> {
    let mut ops = Vec::with_capacity(count);
    for i in 0..count {
        // Every fourth step samples the cluster's view, so the cross-node
        // history has observations spread across the fault window rather than
        // only at quiesce.
        if i % 4 == 3 {
            ops.push(Op::Observe);
            continue;
        }
        let key = rng.random_range(0..KEYS.len());
        let roll = rng.random_range(0..100u32);
        let op = match roll {
            0..=44 => Op::Write { key },
            45..=79 => Op::Read { key },
            80..=88 if family != Family::Healthy || i % 2 == 0 => Op::Migrate { key },
            89..=96 if attach_replica => Op::Wait {
                numreplicas: rng.random_range(1..=2u32),
                timeout_ms: rng.random_range(100..=600u64),
            },
            _ => Op::Write { key },
        };
        ops.push(op);
    }
    ops
}

// =============================================================================
// Cross-node history and the checks a single-state catalog cannot express
// =============================================================================

/// One node's `CLUSTER NODES` + `CLUSTER INFO` reply, as a client sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
    /// Host index of the node that answered.
    pub observer: usize,
    /// `cluster_state:ok`. A node saying otherwise is telling the client its
    /// view is not authoritative, so the cross-node checks skip it.
    pub state_ok: bool,
    /// `cluster_current_epoch`.
    pub current_epoch: u64,
    /// The observer's view of each subject node, keyed by host index.
    pub epochs: BTreeMap<usize, u64>,
    /// Slots the observer believes *it itself* owns. Two `state_ok` nodes
    /// claiming one slot are two live writers for it.
    pub self_slots: BTreeSet<u16>,
}

/// One observation round: every node sampled at the same logical point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Round {
    /// Monotonic round number — the history's time axis. Simulated durations
    /// are deliberately excluded so the checks do not depend on sampling noise.
    pub seq: u64,
    pub views: Vec<NodeView>,
}

/// A write the server acknowledged, and who acknowledged it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckedWrite {
    /// Observation round in effect when the ack landed.
    pub seq: u64,
    pub slot: u16,
    pub key: usize,
    pub value: String,
    /// Host index that returned `+OK` (after redirect following).
    pub node: usize,
    /// Simulated instant of the ack, used only to scope the readback check.
    pub at: Duration,
}

/// Something that legitimately moves slot ownership. Eras between churn events
/// are where single-writer-per-slot is a claim rather than a race.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Churn {
    /// A slot migration was initiated for this slot.
    Slot(u16),
    /// Topology-wide: a fault armed or healed, so failover and self-fencing are
    /// both in play.
    All,
}

/// A churn event at a round boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChurnEvent {
    pub seq: u64,
    pub scope: Churn,
}

/// The whole run history the cross-node checks consume.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    pub rounds: Vec<Round>,
    pub writes: Vec<AckedWrite>,
    pub churn: Vec<ChurnEvent>,
}

/// Rounds within this distance *after* a churn event are excluded from the
/// era-scoped single-writer check: a migration or a heal is allowed to be
/// observed mid-flight.
const CHURN_SETTLE_ROUNDS: u64 = 2;

impl History {
    /// Record a topology-wide churn event.
    pub fn churn_all(&mut self, seq: u64) {
        self.churn.push(ChurnEvent {
            seq,
            scope: Churn::All,
        });
    }

    /// Record a slot-scoped churn event.
    pub fn churn_slot(&mut self, seq: u64, slot: u16) {
        self.churn.push(ChurnEvent {
            seq,
            scope: Churn::Slot(slot),
        });
    }

    /// Was `w` still expected to be readable at quiesce?
    ///
    /// Two carve-outs, both about the *harness*, not about correctness:
    ///
    /// 1. These sims run with `persistence.enabled = false` on the data plane,
    ///    so a SIGKILL discards whatever the node held. A write acked while a
    ///    crash episode is still open is therefore legitimately gone; anything
    ///    acked after the last restart is not.
    /// 2. `CLUSTER SETSLOT ... NODE` transfers *ownership*, not keys — the same
    ///    split Redis has, where the operator moves the payload with `MIGRATE`
    ///    and SETSLOT only publishes the result. FrogDB has no `MIGRATE`, so
    ///    the scheduler's migrate op cannot carry the data across, and a key
    ///    whose slot was reassigned after the write is expected to read as
    ///    missing on the new owner. Scoped per slot rather than globally, so a
    ///    migration of one slot does not blind the check for the other five.
    pub fn acked_write_is_checkable(&self, schedule: &Schedule, w: &AckedWrite) -> bool {
        !schedule.crash_open_at(w.at) && !self.slot_reassigned_since(w.slot, w.seq)
    }

    /// Did a slot migration start at or after round `seq` for `slot`?
    ///
    /// `>=` rather than `>`: a migrate and a write inside the same round are not
    /// ordered by anything the history records, so the same-round case has to be
    /// treated as possibly-migrated. Over-approximating drops a checkable write;
    /// under-approximating would report a phantom loss.
    fn slot_reassigned_since(&self, slot: u16, seq: u64) -> bool {
        self.churn
            .iter()
            .any(|c| matches!(c.scope, Churn::Slot(s) if s == slot) && c.seq >= seq)
    }

    /// The last acked write per key, in key order.
    pub fn last_writes(&self) -> BTreeMap<usize, &AckedWrite> {
        let mut last: BTreeMap<usize, &AckedWrite> = BTreeMap::new();
        for w in &self.writes {
            last.insert(w.key, w);
        }
        last
    }

    fn touches(scope: Churn, slot: u16) -> bool {
        match scope {
            Churn::All => true,
            Churn::Slot(s) => s == slot,
        }
    }

    /// Is round `seq` inside the settling window of a churn event for `slot`?
    fn churned(&self, slot: u16, seq: u64) -> bool {
        self.churn.iter().any(|c| {
            Self::touches(c.scope, slot) && seq >= c.seq && seq <= c.seq + CHURN_SETTLE_ROUNDS
        })
    }

    /// The most recent churn event at or before `seq` touching `slot`, which
    /// identifies the era `seq` belongs to. `None` is the opening era.
    fn era_of(&self, slot: u16, seq: u64) -> Option<u64> {
        self.churn
            .iter()
            .filter(|c| Self::touches(c.scope, slot) && c.seq <= seq)
            .map(|c| c.seq)
            .max()
    }
}

/// Cross-node checks over a whole run history — claims about the *relationship*
/// between nodes, which no single node's state can express.
///
/// - `XNODE-EPOCH-1` — **pairwise epoch monotonicity as observed by clients.**
///   For every (observer, subject) pair the config epoch the observer reports
///   for that subject never decreases. A regression means a node's view rolled
///   back, which is how a stale configuration wins a conflict it should lose.
/// - `XNODE-EPOCH-2` — the same for each observer's own
///   `cluster_current_epoch`.
/// - `XNODE-SLOT-1` — **no two authoritative nodes claim one slot.** Two nodes
///   both reporting `cluster_state:ok` and both listing the slot as their own
///   are two live writers for it. Checked in every round, faults included: a
///   node that lost quorum reports `cluster_state:fail` and is excluded, so a
///   partition alone cannot produce this.
/// - `XNODE-SLOT-2` — **single-writer-per-slot over the whole run history.**
///   Within an era (delimited by churn: a migration of that slot, or any fault
///   arming or healing) at most one node may acknowledge writes for a slot.
///
/// Pure: no I/O, iteration order fixed by `Vec`/`BTreeMap`/`BTreeSet`.
pub fn check_cross_node(history: &History) -> Vec<Violation> {
    let mut violations = Vec::new();

    // --- XNODE-EPOCH-1 / -2: monotonicity of client-observed epochs ---
    //
    // Compared against a high-water mark rather than the previous sample, so a
    // single dip is reported once instead of re-arming on every later sample.
    let mut peak_pair: BTreeMap<(usize, usize), (u64, u64)> = BTreeMap::new();
    let mut peak_current: BTreeMap<usize, (u64, u64)> = BTreeMap::new();

    for round in &history.rounds {
        for view in &round.views {
            if !view.state_ok {
                continue;
            }
            for (&subject, &epoch) in &view.epochs {
                let key = (view.observer, subject);
                match peak_pair.get_mut(&key) {
                    Some(peak) if epoch < peak.1 => violations.push(Violation {
                        id: "XNODE-EPOCH-1",
                        detail: format!(
                            "node {} saw node {subject}'s config epoch go backwards: {} at round {} -> {epoch} at round {}",
                            view.observer, peak.1, peak.0, round.seq
                        ),
                    }),
                    Some(peak) => {
                        if epoch > peak.1 {
                            *peak = (round.seq, epoch);
                        }
                    }
                    None => {
                        peak_pair.insert(key, (round.seq, epoch));
                    }
                }
            }

            match peak_current.get_mut(&view.observer) {
                Some(peak) if view.current_epoch < peak.1 => violations.push(Violation {
                    id: "XNODE-EPOCH-2",
                    detail: format!(
                        "node {}'s cluster_current_epoch went backwards: {} at round {} -> {} at round {}",
                        view.observer, peak.1, peak.0, view.current_epoch, round.seq
                    ),
                }),
                Some(peak) => {
                    if view.current_epoch > peak.1 {
                        *peak = (round.seq, view.current_epoch);
                    }
                }
                None => {
                    peak_current.insert(view.observer, (round.seq, view.current_epoch));
                }
            }
        }
    }

    // --- XNODE-SLOT-1: concurrent authoritative claims on one slot ---
    for round in &history.rounds {
        let mut claims: BTreeMap<u16, BTreeSet<usize>> = BTreeMap::new();
        for view in &round.views {
            if !view.state_ok {
                continue;
            }
            for &slot in &view.self_slots {
                claims.entry(slot).or_default().insert(view.observer);
            }
        }
        for (slot, owners) in claims {
            if owners.len() > 1 {
                violations.push(Violation {
                    id: "XNODE-SLOT-1",
                    detail: format!(
                        "round {}: slot {slot} is claimed by {} nodes at once ({owners:?}), all reporting cluster_state:ok",
                        round.seq,
                        owners.len()
                    ),
                });
            }
        }
    }

    // --- XNODE-SLOT-2: single writer per slot, per era, over the history ---
    let mut writers: BTreeMap<(u16, Option<u64>), BTreeSet<usize>> = BTreeMap::new();
    for w in &history.writes {
        if history.churned(w.slot, w.seq) {
            continue;
        }
        let era = history.era_of(w.slot, w.seq);
        writers.entry((w.slot, era)).or_default().insert(w.node);
    }
    for ((slot, era), nodes) in writers {
        if nodes.len() > 1 {
            let era_label =
                era.map_or_else(|| "opening".to_string(), |s| format!("after round {s}"));
            violations.push(Violation {
                id: "XNODE-SLOT-2",
                detail: format!(
                    "slot {slot} accepted writes at {} different nodes ({nodes:?}) inside one era ({era_label}) with no intervening migration or fault",
                    nodes.len()
                ),
            });
        }
    }

    violations
}

/// Drop the violations the catalog deliberates.
///
/// `DEBUG CLUSTER CHECK` is the reporting view (`check_all`), so it includes
/// `Tier::DocumentedException` entries. Those are ruled reachable by a cited
/// failure-mode row or issue and are not defects; asserting on them would make
/// the sweep red for a state the catalog blesses.
pub fn hard_violations(reported: Vec<Violation>) -> Vec<Violation> {
    let excepted: BTreeSet<&'static str> = CATALOG
        .iter()
        .filter(|inv| !inv.is_hard())
        .map(|inv| inv.id)
        .collect();
    reported
        .into_iter()
        .filter(|v| !excepted.contains(v.id))
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

/// Everything a finished run reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOutcome {
    pub seed: u64,
    /// Canonical, line-per-event rendering. Two runs of one seed produce equal
    /// fingerprints; when they do not, the first differing line names the event
    /// that diverged.
    pub fingerprint: Vec<String>,
    /// Catalog + cross-node violations. Non-empty fails the run.
    pub violations: Vec<Violation>,
}

/// Per-op outcome, recorded as an outcome *class* rather than a raw reply:
/// retry counts and error strings carry sim-timing noise, the class carries the
/// behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OpOutcome {
    Ok { node: usize },
    Value { node: usize, value: String },
    Missing { node: usize },
    Count { node: usize },
    Refused,
    Unreachable,
}

impl OpOutcome {
    fn render(&self) -> String {
        match self {
            OpOutcome::Ok { node } => format!("ok@{node}"),
            OpOutcome::Value { node, value } => format!("value@{node}={value}"),
            OpOutcome::Missing { node } => format!("missing@{node}"),
            OpOutcome::Count { node } => format!("count@{node}"),
            OpOutcome::Refused => "refused".to_string(),
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
    /// Final converged owner per touched slot, for the fingerprint.
    final_owners: BTreeMap<u16, usize>,
    /// Discovered leader index; the step loop waits for it before binding
    /// [`LEADER`]-relative faults.
    leader: Option<usize>,
    /// Set once the driver has finished setup, so no fault arms against a
    /// cluster that has not elected a leader yet.
    ready: bool,
    /// Set if the driver returned an error rather than completing.
    driver_error: Option<String>,
}

/// Run one seed end to end, returning its outcome (violations included, not
/// asserted).
pub fn run_seed(seed: u64) -> RunOutcome {
    let schedule = Schedule::from_seed(seed);

    let mut sim = Builder::new()
        .simulation_duration(schedule.sim_duration)
        .min_message_latency(Duration::from_millis(schedule.base_latency_ms))
        .max_message_latency(Duration::from_millis(schedule.max_latency_ms))
        // Same turmoil-0.7.1 port-budget rationale as the scripted sims: a held
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

    step_with_faults(&mut sim, &schedule, &shared);
    drop(dirs);

    let state = std::mem::take(&mut *shared.lock().expect("shared"));

    let mut violations = state.violations;
    violations.extend(check_cross_node(&state.history));
    if let Some(err) = &state.driver_error {
        violations.push(Violation {
            id: "XNODE-DRIVER-1",
            detail: format!("driver client aborted before quiesce: {err}"),
        });
    }

    let mut fingerprint = schedule.render();
    fingerprint.extend(state.op_lines);
    for (slot, node) in &state.final_owners {
        fingerprint.push(format!("final-owner slot={slot} node={node}"));
    }
    for v in &violations {
        fingerprint.push(format!("violation {v}"));
    }

    // Triage affordance: a sweep reports only the seed and its violations (500
    // fingerprints would drown the failure), so `CLUSTER_SEED_TRACE=1` replays
    // one seed with its whole fingerprint on stderr — schedule, every op's
    // outcome, the settled owners. Off by default; the fingerprint is a run
    // output either way, so this only chooses whether to print it.
    if std::env::var_os("CLUSTER_SEED_TRACE").is_some() {
        eprintln!("--- seed {seed} fingerprint ---");
        for line in &fingerprint {
            eprintln!("{line}");
        }
        eprintln!("--- end seed {seed} ---");
    }

    RunOutcome {
        seed,
        fingerprint,
        violations,
    }
}

/// Run one seed and panic on any violation. The form every test uses.
pub fn assert_seed_clean(seed: u64) -> RunOutcome {
    let outcome = run_seed(seed);
    if !outcome.violations.is_empty() {
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
    outcome
}

/// Register the cluster hosts with the schedule's per-node timers.
fn spawn_scheduled_hosts(
    sim: &mut turmoil::Sim<'_>,
    schedule: &Schedule,
) -> Vec<tempfile::TempDir> {
    let dirs: Vec<tempfile::TempDir> = (0..NODE_COUNT)
        .map(|_| tempfile::tempdir().expect("cluster node data dir"))
        .collect();

    for (idx, host) in CLUSTER_HOSTS.iter().enumerate() {
        let host = host.to_string();
        let path = dirs[idx].path().to_path_buf();
        let params = ClusterNodeParams {
            num_shards: 1,
            auto_failover: schedule.auto_failover,
            election_timeout_ms: schedule.timers[idx].election_timeout_ms,
            heartbeat_interval_ms: schedule.timers[idx].heartbeat_interval_ms,
        };
        sim.host(host.clone(), move || {
            let path = path.clone();
            let host = host.clone();
            async move {
                let own_ip = turmoil::lookup(host.as_str());
                let initial_nodes: Vec<String> = CLUSTER_HOSTS
                    .iter()
                    .map(|peer| format!("{}:{}", turmoil::lookup(*peer), CLUSTER_BUS_PORT))
                    .collect();
                if let Err(e) =
                    real_frogdb_cluster_node_with(params, own_ip, initial_nodes, path).await
                {
                    eprintln!("cluster node {host} exited with error: {e}");
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
/// origin is the moment the cluster became ready rather than `t = 0`: bootstrap
/// cost varies with the timer skew, and a fault landing before the first leader
/// exists is a different (and far less interesting) scenario than the one the
/// schedule describes.
fn step_with_faults(sim: &mut turmoil::Sim<'_>, schedule: &Schedule, shared: &Arc<Mutex<Shared>>) {
    let mut armed = vec![false; schedule.faults.len()];
    let mut healed = vec![false; schedule.faults.len()];
    let mut resolved: Option<Vec<FaultEpisode>> = None;
    let mut origin = Duration::ZERO;
    let mut steps: u64 = 0;

    loop {
        let finished = sim.step().expect("turmoil step");

        if resolved.is_none() {
            let discovered = {
                let guard = shared.lock().expect("shared");
                guard.ready.then_some(guard.leader).flatten()
            };
            if let Some(leader) = discovered {
                resolved = Some(schedule.resolve(leader).faults);
                origin = sim.elapsed();
            }
        }

        if let Some(faults) = resolved.as_ref() {
            let now = sim.elapsed().saturating_sub(origin);
            for (i, f) in faults.iter().enumerate() {
                if !armed[i] && now >= f.arm_at {
                    apply_fault(sim, schedule, f.kind, true);
                    armed[i] = true;
                }
                if armed[i] && !healed[i] && now >= f.heal_at {
                    apply_fault(sim, schedule, f.kind, false);
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
            "seed {}: scheduled cluster sim did not finish",
            schedule.seed
        );
    }
}

/// Arm (`arm = true`) or heal a single fault.
fn apply_fault(sim: &mut turmoil::Sim<'_>, schedule: &Schedule, kind: FaultKind, arm: bool) {
    match kind {
        FaultKind::HoldEdge { a, b } => {
            if arm {
                sim.hold(CLUSTER_HOSTS[a], CLUSTER_HOSTS[b]);
            } else {
                sim.release(CLUSTER_HOSTS[a], CLUSTER_HOSTS[b]);
            }
        }
        FaultKind::HoldIsolate { node } => {
            for (i, peer) in CLUSTER_HOSTS.iter().enumerate() {
                if i == node {
                    continue;
                }
                if arm {
                    sim.hold(CLUSTER_HOSTS[node], *peer);
                } else {
                    sim.release(CLUSTER_HOSTS[node], *peer);
                }
            }
        }
        FaultKind::SlowEdge { a, b, latency_ms } => {
            let value = if arm {
                Duration::from_millis(latency_ms)
            } else {
                Duration::from_millis(schedule.base_latency_ms)
            };
            sim.set_link_latency(CLUSTER_HOSTS[a], CLUSTER_HOSTS[b], value);
        }
        FaultKind::CrashRestart { node } => {
            if arm {
                sim.crash(CLUSTER_HOSTS[node]);
            } else {
                sim.bounce(CLUSTER_HOSTS[node]);
            }
        }
    }
}

// -----------------------------------------------------------------------------
// The driver client
// -----------------------------------------------------------------------------

type DriverError = Box<dyn std::error::Error + 'static>;

/// Bound on any convergence poll, in 100ms simulated steps.
const POLL_STEPS: usize = 300;

async fn drive(schedule: Schedule, shared: Arc<Mutex<Shared>>) -> Result<(), DriverError> {
    let entry = turmoil::lookup(CLUSTER_HOSTS[0]);
    wait_cluster_ready(entry).await?;

    let mut ids = [0u64; NODE_COUNT];
    for (idx, host) in CLUSTER_HOSTS.iter().enumerate() {
        ids[idx] = node_id_of(host).await?;
    }
    // Node ids hash the cluster-bus address, so for a fixed turmoil topology
    // the lowest-id node is the deterministic bootstrap leader (same reasoning
    // as the scripted sims).
    let leader = (0..NODE_COUNT)
        .min_by_key(|&i| ids[i])
        .expect("a leader index");
    let id_to_index: BTreeMap<u64, usize> = (0..NODE_COUNT).map(|i| (ids[i], i)).collect();

    if schedule.attach_replica {
        // The last host becomes a PSYNC replica of a primary that is not the
        // Raft leader, matching the scripted WAIT sims: the metadata plane stays
        // whole while the data plane is what a fault can break. Tolerated on
        // failure — a replica that never attaches only means the WAIT ops report
        // a lower count, which the checks accept.
        let replica = NODE_COUNT - 1;
        let primary = (0..NODE_COUNT)
            .find(|&i| i != replica && i != leader)
            .unwrap_or(leader);
        let _ =
            attach_cluster_replica(CLUSTER_HOSTS[replica], CLUSTER_HOSTS[primary], ids[primary])
                .await;
    }

    let resolved = schedule.resolve(leader);
    let start = tokio::time::Instant::now();
    {
        let mut guard = shared.lock().expect("shared");
        guard.leader = Some(leader);
        guard.ready = true;
    }

    let mut round_seq: u64 = 0;
    let mut touched: BTreeSet<u16> = BTreeSet::new();
    let mut churn_marks = 0usize;

    for (i, op) in schedule.ops.iter().enumerate() {
        // Mark a topology-wide churn event the first time each fault's arm or
        // heal instant passes, so the era-scoped single-writer check knows the
        // ground moved under it.
        let now = start.elapsed();
        let passed = resolved
            .faults
            .iter()
            .flat_map(|f| [f.arm_at, f.heal_at])
            .filter(|t| *t <= now)
            .count();
        if passed > churn_marks {
            churn_marks = passed;
            shared.lock().expect("shared").history.churn_all(round_seq);
        }

        let line = match *op {
            Op::Observe => {
                round_seq += 1;
                let round = observe_round(round_seq, &id_to_index).await;
                shared.lock().expect("shared").history.rounds.push(round);
                format!("op[{i}] observe round={round_seq}")
            }
            Op::Write { key } => {
                let value = format!("v{i}");
                let slot = frogdb_core::slot_for_key(KEYS[key].as_bytes());
                let outcome =
                    exec_anywhere(&[b"SET", KEYS[key].as_bytes(), value.as_bytes()]).await;
                if let OpOutcome::Ok { node } = outcome {
                    touched.insert(slot);
                    shared
                        .lock()
                        .expect("shared")
                        .history
                        .writes
                        .push(AckedWrite {
                            seq: round_seq,
                            slot,
                            key,
                            value,
                            node,
                            at: start.elapsed(),
                        });
                }
                format!("op[{i}] write {} -> {}", KEYS[key], outcome.render())
            }
            Op::Read { key } => {
                let outcome = exec_anywhere(&[b"GET", KEYS[key].as_bytes()]).await;
                format!("op[{i}] read {} -> {}", KEYS[key], outcome.render())
            }
            Op::Migrate { key } => {
                let slot = frogdb_core::slot_for_key(KEYS[key].as_bytes());
                touched.insert(slot);
                shared
                    .lock()
                    .expect("shared")
                    .history
                    .churn_slot(round_seq, slot);
                let result = migrate_slot(KEYS[key].as_bytes(), slot, &ids, leader).await;
                format!("op[{i}] migrate slot={slot} -> {result}")
            }
            Op::Wait {
                numreplicas,
                timeout_ms,
            } => {
                let n = numreplicas.to_string();
                let t = timeout_ms.to_string();
                let outcome = exec_anywhere(&[b"WAIT", n.as_bytes(), t.as_bytes()]).await;
                // The *count* WAIT returns depends on PSYNC ack timing, which
                // the schedule does not pin; what the fingerprint records is
                // that the call returned at all rather than hanging past its
                // deadline.
                let class = match outcome {
                    OpOutcome::Count { .. } => "count",
                    OpOutcome::Refused => "refused",
                    OpOutcome::Unreachable => "unreachable",
                    _ => "other",
                };
                format!("op[{i}] wait n={numreplicas} -> {class}")
            }
        };
        shared.lock().expect("shared").op_lines.push(line);
        tokio::time::sleep(Duration::from_millis(schedule.op_gap_ms)).await;
    }

    // --- Quiesce ---------------------------------------------------------
    //
    // Wait past the last heal, then let the cluster reconverge. Everything
    // below is an assertion about the *settled* system.
    let deadline = resolved.last_heal() + Duration::from_millis(500);
    while start.elapsed() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = wait_cluster_ready(entry).await;

    let mut findings: Vec<Violation> = Vec::new();
    let mut final_owners: BTreeMap<u16, usize> = BTreeMap::new();

    // 1. Client-visible convergence: exactly one owner per touched slot, with
    //    every other node redirecting to it.
    for name in KEYS {
        let slot = frogdb_core::slot_for_key(name.as_bytes());
        if !touched.contains(&slot) {
            continue;
        }
        let mut owner = None;
        for _ in 0..POLL_STEPS {
            if let Ok(Some(idx)) = single_owner_soft(name.as_bytes()).await {
                owner = Some(idx);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        match owner {
            Some(idx) => {
                final_owners.insert(slot, idx);
            }
            // `single_owner_soft` collapses every kind of disagreement to
            // `None` — the right shape for a poll loop, the wrong one for a
            // failure report — so re-probe each node and name what they
            // actually said.
            None => findings.push(Violation {
                id: "XNODE-SLOT-1",
                detail: format!(
                    "slot {slot} ({name}) never converged on a single agreed owner \
                     after every fault healed; nodes replied: {}",
                    probe_owner_replies(name.as_bytes()).await
                ),
            }),
        }
    }

    // 2. No acked-write loss, scoped past the crash faults that legitimately
    //    discard a node's (non-persistent) data plane.
    {
        let checkable: Vec<(usize, String)> = {
            let guard = shared.lock().expect("shared");
            guard
                .history
                .last_writes()
                .into_iter()
                .filter(|(_, w)| guard.history.acked_write_is_checkable(&schedule, w))
                .map(|(key, w)| (key, w.value.clone()))
                .collect()
        };
        for (key, value) in checkable {
            let outcome = exec_anywhere(&[b"GET", KEYS[key].as_bytes()]).await;
            let matched = matches!(&outcome, OpOutcome::Value { value: got, .. } if *got == value);
            if !matched {
                findings.push(Violation {
                    id: "XNODE-ACK-1",
                    detail: format!(
                        "acked write {}={value} is not readable after quiesce (got {})",
                        KEYS[key],
                        outcome.render()
                    ),
                });
            }
        }
    }

    // 3. The invariant catalog on every surviving node (issue 06's
    //    `DEBUG CLUSTER CHECK`). A node that cannot be reached at all is
    //    reported rather than skipped: after every heal, every node is up.
    for (idx, host) in CLUSTER_HOSTS.iter().enumerate() {
        match debug_cluster_check(host).await {
            Ok(reported) => {
                for v in hard_violations(reported) {
                    findings.push(Violation {
                        id: v.id,
                        detail: format!("node {idx}: {}", v.detail),
                    });
                }
            }
            Err(e) => findings.push(Violation {
                id: "XNODE-CHECK-1",
                detail: format!("node {idx} did not answer DEBUG CLUSTER CHECK at quiesce: {e}"),
            }),
        }
    }

    // 4. A final observation round, so the cross-node history always ends with
    //    a post-heal sample of the settled topology.
    round_seq += 1;
    let round = observe_round(round_seq, &id_to_index).await;
    {
        let mut guard = shared.lock().expect("shared");
        guard.history.rounds.push(round);
        guard.violations = findings;
        guard.final_owners = final_owners;
    }

    Ok(())
}

/// Sample `CLUSTER NODES` + `CLUSTER INFO` from every host.
async fn observe_round(seq: u64, id_to_index: &BTreeMap<u64, usize>) -> Round {
    let mut views = Vec::new();
    for (idx, host) in CLUSTER_HOSTS.iter().enumerate() {
        if let Some(view) = observe_node(idx, host, id_to_index).await {
            views.push(view);
        }
    }
    Round { seq, views }
}

async fn observe_node(
    idx: usize,
    host: &str,
    id_to_index: &BTreeMap<u64, usize>,
) -> Option<NodeView> {
    let ip = turmoil::lookup(host);
    let mut conn = RespConn::connect((ip, SERVER_PORT)).await.ok()?;
    let nodes = match conn.cmd(&[b"CLUSTER", b"NODES"]).await.ok()? {
        RespValue::Bulk(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        _ => return None,
    };
    let info = match conn.cmd(&[b"CLUSTER", b"INFO"]).await.ok()? {
        RespValue::Bulk(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        _ => return None,
    };

    let (epochs, self_slots) = parse_cluster_nodes(&nodes, id_to_index);
    let field = |name: &str| -> Option<String> {
        info.lines()
            .find_map(|l| l.split_once(':').filter(|(k, _)| *k == name))
            .map(|(_, v)| v.trim().to_string())
    };
    let state_ok = field("cluster_state").as_deref() == Some("ok");
    let current_epoch = field("cluster_current_epoch")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    Some(NodeView {
        observer: idx,
        state_ok,
        current_epoch,
        epochs,
        self_slots,
    })
}

/// Parse a `CLUSTER NODES` payload into `(subject -> config epoch, my slots)`.
///
/// Line shape (`wire.rs`):
/// `<id> <ip:port@cport> <flags> <master> <ping> <pong> <config_epoch> <link> [slots…]`,
/// where slot tokens are `N` or `LO-HI` and migration markers start with `[`.
fn parse_cluster_nodes(
    nodes: &str,
    id_to_index: &BTreeMap<u64, usize>,
) -> (BTreeMap<usize, u64>, BTreeSet<u16>) {
    let mut epochs = BTreeMap::new();
    let mut self_slots = BTreeSet::new();

    for line in nodes.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 8 {
            continue;
        }
        let Ok(raw_id) = u128::from_str_radix(fields[0], 16) else {
            continue;
        };
        let Ok(epoch) = fields[6].parse::<u64>() else {
            continue;
        };
        let Some(&subject) = id_to_index.get(&(raw_id as u64)) else {
            continue;
        };
        epochs.insert(subject, epoch);

        if fields[2].split(',').any(|f| f == "myself") {
            for token in &fields[8..] {
                // Migration markers (`[100->-<id>]`) are not ownership.
                if token.starts_with('[') {
                    continue;
                }
                match token.split_once('-') {
                    Some((lo, hi)) => {
                        if let (Ok(lo), Ok(hi)) = (lo.parse::<u16>(), hi.parse::<u16>()) {
                            for s in lo..=hi {
                                self_slots.insert(s);
                            }
                        }
                    }
                    None => {
                        if let Ok(s) = token.parse::<u16>() {
                            self_slots.insert(s);
                        }
                    }
                }
            }
        }
    }

    (epochs, self_slots)
}

/// Which cluster host has this IP?
fn host_index_of(ip: std::net::IpAddr) -> Option<usize> {
    CLUSTER_HOSTS.iter().position(|h| turmoil::lookup(*h) == ip)
}

/// Execute `parts` starting at host `start`, following `MOVED`/`ASK` redirects
/// and tracking which node ultimately served the command.
///
/// A tracked variant of `simulation::exec_following_redirects`: the cross-node
/// single-writer check is about *which node* acked a write, so the serving node
/// has to come back with the reply.
async fn exec_tracking(
    start: usize,
    parts: &[&[u8]],
    max_hops: usize,
) -> std::io::Result<(RespValue, usize)> {
    let mut idx = start;
    let mut conn = RespConn::connect((turmoil::lookup(CLUSTER_HOSTS[idx]), SERVER_PORT)).await?;
    for _ in 0..max_hops {
        let reply = conn.cmd(parts).await?;
        if let RespValue::Error(e) = &reply {
            if let Some((is_ask, target)) = parse_redirect(e) {
                idx = host_index_of(target.ip()).unwrap_or(idx);
                conn = RespConn::connect((target.ip(), target.port())).await?;
                if is_ask {
                    let _ = conn.cmd(&[b"ASKING"]).await?;
                }
                continue;
            }
            if e.starts_with("CLUSTERDOWN") || e.starts_with("TRYAGAIN") {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        }
        return Ok((reply, idx));
    }
    Err(std::io::Error::other("too many redirects"))
}

/// Execute a command from whichever node answers, in host order.
///
/// Host order, not a random start: a crashed entry point must not mask the whole
/// op as unreachable, and the choice has to be reproducible.
async fn exec_anywhere(parts: &[&[u8]]) -> OpOutcome {
    for start in 0..NODE_COUNT {
        let Ok((reply, node)) = exec_tracking(start, parts, 16).await else {
            continue;
        };
        return match reply {
            RespValue::Simple(_) => OpOutcome::Ok { node },
            RespValue::Bulk(Some(b)) => OpOutcome::Value {
                node,
                value: String::from_utf8_lossy(&b).into_owned(),
            },
            RespValue::Bulk(None) => OpOutcome::Missing { node },
            RespValue::Int(_) => OpOutcome::Count { node },
            _ => OpOutcome::Refused,
        };
    }
    OpOutcome::Unreachable
}

/// One `GET` per node, rendered — the diagnostic behind an `XNODE-SLOT-1`
/// convergence failure. [`single_owner_soft`] answers only "agreed / not
/// agreed", so without this a failing seed reports that the cluster disagreed
/// but not *how*, which is the whole content of the defect.
async fn probe_owner_replies(key: &[u8]) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(NODE_COUNT);
    for (idx, host) in CLUSTER_HOSTS.iter().enumerate() {
        let ip = turmoil::lookup(*host);
        let rendered = match RespConn::connect((ip, SERVER_PORT)).await {
            Err(e) => format!("unreachable({})", e.kind()),
            Ok(mut conn) => {
                let reply = match conn.cmd(&[b"GET", key]).await {
                    Err(e) => format!("io({})", e.kind()),
                    Ok(RespValue::Error(e)) => format!("err({e})"),
                    Ok(RespValue::Bulk(Some(v))) => {
                        format!("value({})", String::from_utf8_lossy(&v))
                    }
                    Ok(RespValue::Bulk(None)) => "missing".to_string(),
                    Ok(other) => format!("other({other:?})"),
                };
                // The data-plane reply alone cannot distinguish "this node
                // believes it owns the slot" from "this node is answering as
                // somebody's replica", and those are different defects. Its
                // own topology view is what separates them.
                let view = match conn.cmd(&[b"CLUSTER", b"NODES"]).await {
                    Ok(RespValue::Bulk(Some(b))) => String::from_utf8_lossy(&b)
                        .lines()
                        .map(str::trim)
                        .filter(|l| !l.is_empty())
                        .collect::<Vec<_>>()
                        .join(" / "),
                    _ => "<no CLUSTER NODES>".to_string(),
                };
                format!("{reply} view[{view}]")
            }
        };
        parts.push(format!("n{idx}={rendered}"));
    }
    parts.join("  ")
}

/// Move `slot` (represented by `key`) to a node that does not currently own it,
/// through the `IMPORTING`/`MIGRATING`/`NODE` dance.
///
/// Best-effort by design: under a fault the commit may not land, which is a
/// legitimate outcome — the slot stays where it was, and the returned token
/// records which step stopped.
async fn migrate_slot(
    key: &[u8],
    slot: u16,
    ids: &[u64; NODE_COUNT],
    leader: usize,
) -> &'static str {
    let Ok(Some(source)) = owner_host_of(key).await else {
        return "no-owner";
    };
    let Some(target) = (0..NODE_COUNT).find(|&i| i != source && i != leader) else {
        return "no-target";
    };
    let slot_s = slot.to_string();
    let source_hex = format!("{:040x}", ids[source]);
    let target_hex = format!("{:040x}", ids[target]);

    if !setslot(
        target,
        &[
            b"CLUSTER",
            b"SETSLOT",
            slot_s.as_bytes(),
            b"IMPORTING",
            source_hex.as_bytes(),
        ],
    )
    .await
    {
        return "importing-refused";
    }
    if !setslot(
        source,
        &[
            b"CLUSTER",
            b"SETSLOT",
            slot_s.as_bytes(),
            b"MIGRATING",
            target_hex.as_bytes(),
        ],
    )
    .await
    {
        return "migrating-refused";
    }

    for _ in 0..30 {
        if setslot(
            target,
            &[
                b"CLUSTER",
                b"SETSLOT",
                slot_s.as_bytes(),
                b"NODE",
                target_hex.as_bytes(),
            ],
        )
        .await
        {
            return "committed";
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    "commit-pending"
}

/// One `CLUSTER SETSLOT` against `host`; true iff it returned `+OK`.
async fn setslot(host: usize, parts: &[&[u8]]) -> bool {
    let ip = turmoil::lookup(CLUSTER_HOSTS[host]);
    let Ok(mut conn) = RespConn::connect((ip, SERVER_PORT)).await else {
        return false;
    };
    matches!(conn.cmd(parts).await, Ok(RespValue::Simple(_)))
}

/// `DEBUG CLUSTER CHECK` against one host, parsed back into violations.
async fn debug_cluster_check(host: &str) -> std::io::Result<Vec<Violation>> {
    let ip = turmoil::lookup(host);
    let mut conn = RespConn::connect((ip, SERVER_PORT)).await?;
    match conn.cmd(&[b"DEBUG", b"CLUSTER", b"CHECK"]).await? {
        RespValue::Array(Some(items)) => Ok(items.iter().filter_map(parse_check_entry).collect()),
        RespValue::Error(e) => Err(std::io::Error::other(e)),
        other => Err(std::io::Error::other(format!(
            "DEBUG CLUSTER CHECK returned {other:?}"
        ))),
    }
}

/// One `{id, detail}` map from `DEBUG CLUSTER CHECK`, which RESP2 flattens to
/// `[id, <v>, detail, <v>]`.
fn parse_check_entry(item: &RespValue) -> Option<Violation> {
    let RespValue::Array(Some(kv)) = item else {
        return None;
    };
    let mut id = String::new();
    let mut detail = String::new();
    for pair in kv.chunks(2) {
        let [RespValue::Bulk(Some(k)), RespValue::Bulk(Some(v))] = pair else {
            continue;
        };
        match k.as_slice() {
            b"id" => id = String::from_utf8_lossy(v).into_owned(),
            b"detail" => detail = String::from_utf8_lossy(v).into_owned(),
            _ => {}
        }
    }
    // The catalog's ids are `&'static str`; recover the static so a reported
    // violation has the same shape as a locally produced one. An id the catalog
    // does not know is surfaced rather than dropped.
    let id = CATALOG
        .iter()
        .map(|inv| inv.id)
        .find(|c| *c == id)
        .unwrap_or("XNODE-CHECK-2");
    Some(Violation { id, detail })
}

// =============================================================================
// Tests
// =============================================================================

/// The committed regression-seed list: seeds that once failed, replayed forever.
///
/// Format, one entry per line, `#` comments and blank lines ignored:
/// `<seed> <family> [EXPECTED-FAILURE:<issue>] <why>`. The family is recorded
/// so a change to [`Schedule::from_seed`]'s draw order is caught (the seed
/// would map somewhere else) rather than silently replaying a different
/// scenario.
const REGRESSION_SEEDS: &str = include_str!("cluster-regression-seeds.txt");

/// Marker opening the optional muzzle column.
const EXPECTED_FAILURE: &str = "EXPECTED-FAILURE:";

/// One line of [`REGRESSION_SEEDS`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct RegressionSeed {
    seed: u64,
    /// The family this seed derived when it was recorded.
    family: String,
    /// `Some(issue)` while the defect this seed found is still open. Such a
    /// seed is expected to *fail*: the replay test asserts it still reproduces,
    /// so the muzzle turns itself off when the fix lands instead of quietly
    /// outliving it.
    expected_failure: Option<String>,
    note: String,
}

/// Parse [`REGRESSION_SEEDS`].
fn regression_seeds() -> Vec<RegressionSeed> {
    REGRESSION_SEEDS
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(parse_regression_seed)
        .collect()
}

fn parse_regression_seed(line: &str) -> RegressionSeed {
    let mut parts = line.splitn(3, char::is_whitespace);
    let seed: u64 = parts
        .next()
        .expect("a seed column")
        .parse()
        .unwrap_or_else(|e| panic!("bad seed in cluster-regression-seeds.txt: {line:?} ({e})"));
    let family = parts.next().unwrap_or("").trim().to_string();
    let rest = parts.next().unwrap_or("").trim();

    let (expected_failure, note) = match rest.strip_prefix(EXPECTED_FAILURE) {
        Some(tail) => {
            let (issue, note) = tail.split_once(char::is_whitespace).unwrap_or((tail, ""));
            assert!(
                !issue.is_empty(),
                "{EXPECTED_FAILURE} needs an issue after it: {line:?}"
            );
            (Some(issue.trim().to_string()), note.trim().to_string())
        }
        None => (None, rest.to_string()),
    };

    RegressionSeed {
        seed,
        family,
        expected_failure,
        note,
    }
}

/// Seeds muzzled against an open issue, which the sweeps skip: a known-open
/// defect must not drown out the *new* failures a sweep exists to find.
/// `test_cluster_scheduler_regression_seeds` still replays each one and asserts
/// it reproduces, so nothing stops being checked.
fn muzzled_seeds() -> BTreeMap<u64, String> {
    regression_seeds()
        .into_iter()
        .filter_map(|r| r.expected_failure.map(|issue| (r.seed, issue)))
        .collect()
}

/// Seeds the default suite always runs: a small smoke sweep, so the scheduler
/// itself cannot rot between nightlies.
///
/// One seed per family, found by scanning upward from 1 — cheap to recompute,
/// and asserted by `test_scheduler_smoke_seeds_cover_every_family` so a change
/// to the derivation that shifts family assignment fails loudly instead of
/// quietly narrowing coverage.
const SMOKE_SEEDS: [u64; 6] = [1, 2, 3, 5, 7, 14];

#[test]
fn test_scheduler_derivation_is_pure() {
    for seed in 0..64u64 {
        let a = Schedule::from_seed(seed);
        let b = Schedule::from_seed(seed);
        assert_eq!(a, b, "Schedule::from_seed({seed}) is not a pure function");
        assert_eq!(a.render(), b.render());
    }
}

#[test]
fn test_scheduler_distinct_seeds_give_distinct_schedules() {
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
fn test_scheduler_smoke_seeds_cover_every_family() {
    let covered: BTreeSet<Family> = SMOKE_SEEDS
        .iter()
        .map(|&s| Schedule::from_seed(s).family)
        .collect();
    let missing: Vec<&str> = Family::ALL
        .iter()
        .filter(|f| !covered.contains(f))
        .map(|f| f.as_str())
        .collect();
    assert!(
        missing.is_empty(),
        "SMOKE_SEEDS no longer cover {missing:?}; recompute them by scanning \
         Schedule::from_seed upward from 1"
    );
}

#[test]
fn test_scheduler_faults_stay_inside_their_budget() {
    for seed in 0..500u64 {
        let s = Schedule::from_seed(seed);
        for f in &s.faults {
            assert!(f.arm_at >= Duration::from_millis(MIN_ARM_MS), "seed {seed}");
            assert!(f.heal_at > f.arm_at, "seed {seed}: empty fault window");
            assert!(
                f.heal_at - f.arm_at <= Duration::from_millis(MAX_FAULT_MS),
                "seed {seed}: fault window exceeds the budget"
            );
            if let FaultKind::HoldEdge { a, b } | FaultKind::SlowEdge { a, b, .. } = f.kind {
                assert_ne!(a, b, "seed {seed}: self-edge fault would be a no-op");
            }
        }
        // Concurrent crashes would take a 3-node cluster below quorum.
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

#[test]
fn test_scheduler_regression_seed_file_parses() {
    let entries = regression_seeds();
    assert!(
        !entries.is_empty(),
        "cluster-regression-seeds.txt parsed to nothing"
    );
    for entry in entries {
        let derived = Schedule::from_seed(entry.seed).family.as_str();
        assert_eq!(
            derived, entry.family,
            "seed {} is recorded as family {:?} but now derives {derived:?} — \
             Schedule::from_seed's draw order changed and the file must be re-derived",
            entry.seed, entry.family
        );
        assert!(
            !entry.note.is_empty(),
            "seed {} has no diagnosis; every regression seed records why it is here",
            entry.seed
        );
    }
}

#[test]
fn test_scheduler_regression_seed_lines_parse_the_muzzle_column() {
    assert_eq!(
        parse_regression_seed("2 healthy subsumes the scripted MOVED sim"),
        RegressionSeed {
            seed: 2,
            family: "healthy".to_string(),
            expected_failure: None,
            note: "subsumes the scripted MOVED sim".to_string(),
        }
    );
    assert_eq!(
        parse_regression_seed("3 replica-partition EXPECTED-FAILURE:issue-17 split brain"),
        RegressionSeed {
            seed: 3,
            family: "replica-partition".to_string(),
            expected_failure: Some("issue-17".to_string()),
            note: "split brain".to_string(),
        }
    );
}

#[test]
fn test_cross_node_accepts_a_clean_history() {
    let history = History {
        rounds: vec![
            Round {
                seq: 1,
                views: vec![
                    view(0, true, 3, &[(0, 1), (1, 2)], &[0, 1]),
                    view(1, true, 3, &[(0, 1), (1, 2)], &[2]),
                ],
            },
            Round {
                seq: 2,
                views: vec![
                    view(0, true, 4, &[(0, 1), (1, 2)], &[0, 1]),
                    view(1, true, 4, &[(0, 1), (1, 2)], &[2]),
                ],
            },
        ],
        writes: vec![acked(1, 0, 0), acked(2, 0, 0)],
        churn: vec![],
    };
    assert!(check_cross_node(&history).is_empty());
}

#[test]
fn test_cross_node_catches_epoch_regression() {
    let history = History {
        rounds: vec![
            Round {
                seq: 1,
                views: vec![view(0, true, 5, &[(1, 7)], &[])],
            },
            Round {
                seq: 2,
                views: vec![view(0, true, 5, &[(1, 6)], &[])],
            },
        ],
        ..History::default()
    };
    let ids: Vec<&str> = check_cross_node(&history).iter().map(|v| v.id).collect();
    assert_eq!(ids, vec!["XNODE-EPOCH-1"]);
}

#[test]
fn test_cross_node_catches_current_epoch_regression() {
    let history = History {
        rounds: vec![
            Round {
                seq: 1,
                views: vec![view(0, true, 9, &[], &[])],
            },
            Round {
                seq: 2,
                views: vec![view(0, true, 8, &[], &[])],
            },
        ],
        ..History::default()
    };
    let ids: Vec<&str> = check_cross_node(&history).iter().map(|v| v.id).collect();
    assert_eq!(ids, vec!["XNODE-EPOCH-2"]);
}

#[test]
fn test_cross_node_ignores_epochs_from_a_non_ok_node() {
    // A node reporting `cluster_state:fail` is telling the client its view is
    // not authoritative, so a stale epoch there is not a regression.
    let history = History {
        rounds: vec![
            Round {
                seq: 1,
                views: vec![view(0, true, 5, &[(1, 7)], &[0])],
            },
            Round {
                seq: 2,
                views: vec![view(0, false, 1, &[(1, 1)], &[0])],
            },
        ],
        ..History::default()
    };
    assert!(check_cross_node(&history).is_empty());
}

#[test]
fn test_cross_node_catches_two_nodes_claiming_one_slot() {
    let history = History {
        rounds: vec![Round {
            seq: 1,
            views: vec![view(0, true, 1, &[], &[42]), view(1, true, 1, &[], &[42])],
        }],
        ..History::default()
    };
    let ids: Vec<&str> = check_cross_node(&history).iter().map(|v| v.id).collect();
    assert_eq!(ids, vec!["XNODE-SLOT-1"]);
}

#[test]
fn test_cross_node_catches_two_writers_in_one_era() {
    let history = History {
        writes: vec![acked(1, 0, 0), acked(2, 0, 1)],
        ..History::default()
    };
    let ids: Vec<&str> = check_cross_node(&history).iter().map(|v| v.id).collect();
    assert_eq!(ids, vec!["XNODE-SLOT-2"]);
}

#[test]
fn test_cross_node_allows_two_writers_across_a_migration() {
    // The same two writes, with a migration of that slot between them and the
    // second write past the settling window: ownership legitimately moved.
    let history = History {
        writes: vec![acked(1, 0, 0), acked(6, 0, 1)],
        churn: vec![ChurnEvent {
            seq: 2,
            scope: Churn::Slot(0),
        }],
        ..History::default()
    };
    assert!(check_cross_node(&history).is_empty());
}

#[test]
fn test_cross_node_suppresses_writes_inside_the_settling_window() {
    // A write landing one round after churn is mid-flight, not a second writer.
    let history = History {
        writes: vec![acked(1, 0, 0), acked(3, 0, 1)],
        churn: vec![ChurnEvent {
            seq: 2,
            scope: Churn::All,
        }],
        ..History::default()
    };
    assert!(check_cross_node(&history).is_empty());
}

#[test]
fn test_hard_violations_drops_documented_exceptions() {
    let excepted: Vec<&'static str> = CATALOG
        .iter()
        .filter(|inv| !inv.is_hard())
        .map(|inv| inv.id)
        .collect();
    assert!(
        !excepted.is_empty(),
        "the catalog has no DOCUMENTED-EXCEPTION entries; this filter would be dead code"
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
    let kept = hard_violations(reported);
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].id, hard[0]);
}

#[test]
fn test_parse_cluster_nodes_reads_epochs_slots_and_skips_migrations() {
    let id_to_index: BTreeMap<u64, usize> = BTreeMap::from([(0x11, 0), (0x22, 1)]);
    let nodes = "\
0000000000000000000000000000000000000011 10.0.0.1:6379@16379 myself,master - 0 0 7 connected 0-9 42 [100->-0000000000000000000000000000000000000022]
0000000000000000000000000000000000000022 10.0.0.2:6379@16379 master - 0 0 9 connected 200-201
";
    let (epochs, slots) = parse_cluster_nodes(nodes, &id_to_index);
    assert_eq!(epochs, BTreeMap::from([(0, 7), (1, 9)]));
    // `myself` owns 0-9 and 42; the `[100->-…]` marker is a migration, not
    // ownership, and node 1's slots are not `myself`'s.
    assert_eq!(
        slots,
        BTreeSet::from([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42]),
        "migration markers or another node's slots leaked into self_slots"
    );
}

#[test]
fn test_parse_check_entry_reads_a_catalog_id() {
    let known = asserted_catalog_ids()[0];
    let entry = RespValue::Array(Some(vec![
        RespValue::Bulk(Some(b"id".to_vec())),
        RespValue::Bulk(Some(known.as_bytes().to_vec())),
        RespValue::Bulk(Some(b"detail".to_vec())),
        RespValue::Bulk(Some(b"slot 5 dangles".to_vec())),
    ]));
    let v = parse_check_entry(&entry).expect("a violation");
    assert_eq!(v.id, known);
    assert_eq!(v.detail, "slot 5 dangles");

    // An id the catalog does not know is surfaced, never dropped.
    let unknown = RespValue::Array(Some(vec![
        RespValue::Bulk(Some(b"id".to_vec())),
        RespValue::Bulk(Some(b"INV-NOPE-9".to_vec())),
        RespValue::Bulk(Some(b"detail".to_vec())),
        RespValue::Bulk(Some(b"?".to_vec())),
    ]));
    assert_eq!(
        parse_check_entry(&unknown).expect("surfaced").id,
        "XNODE-CHECK-2"
    );
}

#[test]
fn test_acked_writes_are_unchecked_only_while_a_crash_is_open() {
    // These sims run the data plane without persistence, so a write acked while
    // a SIGKILL episode is open may legitimately vanish — but one acked after
    // the last restart may not.
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
fn test_acked_writes_are_unchecked_once_their_slot_migrates() {
    // `CLUSTER SETSLOT ... NODE` publishes new ownership; it does not carry the
    // keys (FrogDB has no `MIGRATE`, same split as Redis). So a write is only
    // expected back if its own slot has not been reassigned since — a migration
    // of some *other* slot must leave it checkable.
    let mut schedule = Schedule::from_seed(0);
    schedule.faults = Vec::new();
    let mut history = History::default();
    history.churn_slot(5, 42);

    let before = AckedWrite {
        seq: 4,
        slot: 42,
        ..acked_at(Duration::ZERO)
    };
    let same_round = AckedWrite {
        seq: 5,
        slot: 42,
        ..acked_at(Duration::ZERO)
    };
    let after = AckedWrite {
        seq: 6,
        slot: 42,
        ..acked_at(Duration::ZERO)
    };
    let other_slot = AckedWrite {
        seq: 4,
        slot: 43,
        ..acked_at(Duration::ZERO)
    };

    assert!(!history.acked_write_is_checkable(&schedule, &before));
    assert!(!history.acked_write_is_checkable(&schedule, &same_round));
    assert!(history.acked_write_is_checkable(&schedule, &after));
    assert!(history.acked_write_is_checkable(&schedule, &other_slot));
}

#[test]
fn test_fault_resolve_binds_the_leader_sentinel() {
    let resolved = FaultKind::HoldIsolate { node: LEADER }.resolve(2);
    assert_eq!(resolved, FaultKind::HoldIsolate { node: 2 });
    // Binding must never produce a self-edge, which turmoil would drop.
    let edge = FaultKind::HoldEdge { a: LEADER, b: 2 }.resolve(2);
    let FaultKind::HoldEdge { a, b } = edge else {
        panic!("kind changed under resolve");
    };
    assert_ne!(a, b);
}

// --- helpers for the pure tests above ---

fn view(
    observer: usize,
    state_ok: bool,
    current_epoch: u64,
    epochs: &[(usize, u64)],
    self_slots: &[u16],
) -> NodeView {
    NodeView {
        observer,
        state_ok,
        current_epoch,
        epochs: epochs.iter().copied().collect(),
        self_slots: self_slots.iter().copied().collect(),
    }
}

fn acked(seq: u64, slot: u16, node: usize) -> AckedWrite {
    AckedWrite {
        seq,
        slot,
        key: 0,
        value: format!("v{seq}"),
        node,
        at: Duration::from_millis(seq * 100),
    }
}

fn acked_at(at: Duration) -> AckedWrite {
    AckedWrite {
        seq: 0,
        slot: 0,
        key: 0,
        value: "v".to_string(),
        node: 0,
        at,
    }
}

// --- the sim-backed tests ---

/// Same seed → same run. The scheduler's core contract: two runs of one seed
/// produce byte-identical fingerprints, so a failing seed replays exactly.
#[test]
fn test_cluster_scheduler_same_seed_same_run() {
    let seed = SMOKE_SEEDS[0];
    let a = run_seed(seed);
    let b = run_seed(seed);
    assert_fingerprints_equal(seed, &a.fingerprint, &b.fingerprint);
}

/// The default-suite smoke sweep: one seed per fault family, so the scheduler
/// cannot rot between nightly sweeps.
///
/// Muzzled seeds are skipped here and replayed by
/// `test_cluster_scheduler_regression_seeds` instead, which asserts they still
/// reproduce — the family stays covered either way.
#[test]
fn test_cluster_scheduler_smoke_sweep() {
    let muzzled = muzzled_seeds();
    for seed in SMOKE_SEEDS {
        if muzzled.contains_key(&seed) {
            continue;
        }
        assert_seed_clean(seed);
    }
}

/// Every seed in the committed regression list, replayed forever.
///
/// A seed muzzled against an open issue is asserted to *still fail*: that is
/// what makes the muzzle self-expiring. Fixing the issue turns this test red
/// with an instruction to delete the muzzle, rather than leaving a stale
/// EXPECTED-FAILURE marker hiding a seed that has quietly started passing.
#[test]
fn test_cluster_scheduler_regression_seeds() {
    for entry in regression_seeds() {
        let RegressionSeed {
            seed,
            family,
            expected_failure,
            note,
        } = entry;
        let outcome = run_seed(seed);
        let rendered = outcome
            .violations
            .iter()
            .map(|v| format!("  - {v}"))
            .collect::<Vec<_>>()
            .join("\n");
        match expected_failure {
            None => assert!(
                outcome.violations.is_empty(),
                "regression seed {seed} ({family}) failed again — {note}\n{rendered}"
            ),
            Some(issue) => assert!(
                !outcome.violations.is_empty(),
                "regression seed {seed} ({family}) is muzzled as EXPECTED-FAILURE against \
                 {issue} but now passes — if {issue} is fixed, delete the \
                 {EXPECTED_FAILURE}{issue} marker from cluster-regression-seeds.txt so the \
                 seed is checked clean from here on"
            ),
        }
    }
}

/// The seeded sweep (`just cluster-seeds`). `#[ignore]`d so the default suite
/// runs only the smoke sweep above; the nightly workflow runs this one with
/// `--run-ignored all`.
///
/// Budget comes from `CLUSTER_SEEDS` (count) and `CLUSTER_SEEDS_START`
/// (offset); worker threads from `CLUSTER_SEEDS_JOBS`. All three have laptop
/// defaults, so the recipe works with no environment at all.
///
/// Seeds muzzled as EXPECTED-FAILURE are skipped, so a re-run of a range that
/// contains one reports only what is new. The muzzle is per-seed, not
/// per-defect: another seed hitting the same open issue still fails the sweep
/// and is triaged against the issue by hand, which is the right default — the
/// alternative is matching violations by text and silently swallowing a
/// different defect that happens to render alike.
#[test]
#[ignore = "sweep: run via `just cluster-seeds`"]
fn test_cluster_scheduler_seed_sweep() {
    let count: u64 = env_u64("CLUSTER_SEEDS", 500);
    let start: u64 = env_u64("CLUSTER_SEEDS_START", 1);
    let jobs: u64 = env_u64("CLUSTER_SEEDS_JOBS", 4).max(1);
    let muzzled = muzzled_seeds();

    let failures: Arc<Mutex<Vec<(u64, String)>>> = Arc::new(Mutex::new(Vec::new()));
    std::thread::scope(|scope| {
        for worker in 0..jobs {
            let failures = failures.clone();
            let muzzled = &muzzled;
            scope.spawn(move || {
                let mut i = worker;
                while i < count {
                    let seed = start + i;
                    if muzzled.contains_key(&seed) {
                        i += jobs;
                        continue;
                    }
                    // Each run brings up three real servers; a panic in one seed
                    // must not abort the sweep, so the failure is captured and
                    // reported alongside every other failing seed at the end.
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
                            let detail = panic
                                .downcast_ref::<String>()
                                .cloned()
                                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                                .unwrap_or_else(|| "<non-string panic>".to_string());
                            failures
                                .lock()
                                .expect("failures")
                                .push((seed, format!("panic: {detail}")));
                        }
                    }
                    i += jobs;
                }
            });
        }
    });

    let mut failures = failures.lock().expect("failures").clone();
    failures.sort_by_key(|(seed, _)| *seed);
    assert!(
        failures.is_empty(),
        "{} of {count} seeds failed (from {start}); add each to \
         tests/simulation/cluster-regression-seeds.txt with its diagnosis:\n{}",
        failures.len(),
        failures
            .iter()
            .map(|(seed, detail)| format!("  seed {seed}: {detail}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// Compare two run fingerprints, reporting the *first* divergence with context
/// rather than dumping two multi-hundred-line vectors.
fn assert_fingerprints_equal(seed: u64, a: &[String], b: &[String]) {
    if a == b {
        return;
    }
    let first = a
        .iter()
        .zip(b.iter())
        .position(|(x, y)| x != y)
        .unwrap_or_else(|| a.len().min(b.len()));
    let context_start = first.saturating_sub(3);
    let mut msg = format!(
        "seed {seed} did not reproduce: run A has {} lines, run B has {}; first difference at line {first}\n",
        a.len(),
        b.len()
    );
    for (i, line) in a.iter().enumerate().take(first).skip(context_start) {
        msg.push_str(&format!("  both[{i}] {line}\n"));
    }
    msg.push_str(&format!(
        "     A[{first}] {}\n     B[{first}] {}\n",
        a.get(first).map_or("<end of run>", String::as_str),
        b.get(first).map_or("<end of run>", String::as_str),
    ));
    panic!("{msg}");
}
