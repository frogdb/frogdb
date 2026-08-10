//! The topology-agnostic half of the seeded fault scheduler
//! (replication-correctness PRD §8 D8, issue 11).
//!
//! # What lives here and what does not
//!
//! One `u64` derives a whole simulated run. *How* that derivation is shaped —
//! which fault families exist, what the client workload can do, how wide each
//! draw window is — is per-arm; the derivation *itself* is not. This module
//! holds the part that is not:
//!
//! - [`Schedule::from_seed`], the fixed draw order, with the per-arm vocabulary
//!   supplied through the [`Arm`] trait and the per-arm numbers through
//!   [`Budget`];
//! - [`FaultKind`] / [`FaultEpisode`] and [`apply_fault`], which speak turmoil's
//!   `hold`/`release`/`set_link_latency`/`crash`/`bounce` over host *indices*
//!   and so mean the same thing in any topology;
//! - [`RunOutcome`] and [`assert_fingerprints_equal`], the assertable form of
//!   "same seed, same run";
//! - the regression-seed file machinery ([`regression_seeds`],
//!   [`muzzled_seeds`]) including the self-expiring `EXPECTED-FAILURE:<issue>`
//!   marker;
//! - [`parse_check_entry`] and [`hard_violations`], typed over the shared
//!   catalog vocabulary in `frogdb-types` rather than any one area's catalog.
//!
//! What stays in an arm: how its hosts are spawned, what its `DEBUG … CHECK`
//! command is called, and the cross-node checks a single-node view cannot
//! express. D8 also rules that the ~50-line `run_seed` driver shape is
//! **duplicated per arm rather than genericized** — the shape is short and the
//! per-arm bodies share almost nothing, so a generic driver would cost more in
//! parameters than it saves in lines.
//!
//! # Determinism is the contract
//!
//! Same seed → same run. Everything a schedule contains is drawn from one
//! [`StdRng::seed_from_u64`] in a fixed order; nothing reads the wall clock,
//! and every collection the derivation iterates is ordered (`Vec`/`BTreeMap`/
//! `BTreeSet`, never `HashMap`). An arm's hooks are called from
//! [`Schedule::from_seed`] at fixed points in that order, so adding an arm
//! cannot perturb another arm's seeds — but *reordering the hooks* would move
//! every seed at once, which is what the regression file's family column
//! exists to catch.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;

use frogdb_types::Violation;
use rand::{RngExt, SeedableRng, rngs::StdRng};

use super::RespValue;

/// Sentinel node index meaning "whichever node the arm nominates at setup" —
/// the Raft leader in the cluster arm, the primary in the replication arm.
/// Bound by [`FaultKind::resolve`]; never appears in a resolved schedule.
pub const LEADER: usize = usize::MAX;

// =============================================================================
// The per-arm vocabulary
// =============================================================================

/// An inclusive draw window.
///
/// A plain struct rather than a `RangeInclusive` so a [`Budget`] can be an
/// associated `const`. [`Span::draw`] is the only way to consume one, which is
/// what keeps every arm drawing in the same shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub lo: u64,
    pub hi: u64,
}

impl Span {
    pub const fn new(lo: u64, hi: u64) -> Self {
        Self { lo, hi }
    }

    /// One draw, both ends inclusive.
    pub fn draw(self, rng: &mut StdRng) -> u64 {
        rng.random_range(self.lo..=self.hi)
    }
}

/// The numeric envelope an arm's schedules are drawn inside.
///
/// Separated from the draw *order* on purpose: an arm may widen a window or
/// lengthen its quiesce tail without touching [`Schedule::from_seed`], and the
/// order — which is what a seed's identity rests on — stays in one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    /// Shared heartbeat interval, drawn once for every node.
    pub heartbeat_interval_ms: Span,
    /// Node 0's election timeout; each later node adds [`Budget::election_step_ms`].
    pub base_election_ms: Span,
    pub election_step_ms: Span,
    /// Baseline latency for the whole simulated network.
    pub base_latency_ms: Span,
    /// Added to the baseline to get the maximum message latency.
    pub extra_latency_ms: Span,
    /// Simulated gap between successive workload ops.
    pub op_gap_ms: Span,
    /// Simulated time the workload must keep running past the last heal, so
    /// every run ends with a quiesce window in which the network is whole.
    pub quiesce_tail_ms: u64,
    /// Clamp on the op count the tail implies: `(min, max)`.
    pub op_count: (usize, usize),
    /// turmoil's wall for the whole run — the hang guard, not the expected
    /// length.
    pub sim_duration: Duration,
    /// Earliest a fault may arm, measured from the moment the arm reported
    /// ready.
    pub min_arm_ms: u64,
    /// Jitter drawn on top of [`Budget::min_arm_ms`].
    pub arm_jitter_ms: Span,
    /// How long an episode is held. Short on purpose: every held edge parks
    /// in-flight dials on turmoil ephemeral ports for the duration.
    pub hold_ms: Span,
}

impl Budget {
    /// Longest a schedule may hold a fault — the top of [`Budget::hold_ms`].
    pub const fn max_fault_ms(&self) -> u64 {
        self.hold_ms.hi
    }
}

/// One arm of the seeded scheduler: a topology plus the vocabulary its
/// schedules are written in.
///
/// Implemented by a unit marker type (`ClusterArm`, and later `ReplicationArm`)
/// rather than carried as a value, so [`Schedule`] stays a plain data type and
/// an arm's constants are visible to the type system.
pub trait Arm: Debug + Clone + Copy + PartialEq + Eq {
    /// The shape of fault this arm injects. Drawn from the seed *first*, so a
    /// sweep covers every family by construction rather than by luck.
    type Family: Copy + Ord + Hash + Debug + 'static;
    /// Per-arm booleans and knobs drawn between the latency and the faults.
    type Toggles: Clone + PartialEq + Eq + Debug;
    /// One client-workload step.
    type Op: Copy + PartialEq + Eq + Debug;

    /// This arm's hosts, in index order. Every node index in a [`FaultKind`] is
    /// an index into this slice.
    const HOSTS: &'static [&'static str];
    const BUDGET: Budget;

    /// Every family, in the order the seed selects from. Changing this list
    /// renumbers which family a seed maps to, which is why the regression-seed
    /// file records each seed's family alongside it.
    fn families() -> &'static [Self::Family];

    /// Stable token used in fingerprints, failure messages and the seed file.
    fn family_token(family: Self::Family) -> &'static str;

    fn derive_toggles(family: Self::Family, rng: &mut StdRng) -> Self::Toggles;

    /// The toggle lines of the fingerprint, in a fixed order.
    fn render_toggles(toggles: &Self::Toggles) -> Vec<String>;

    /// Family-specific fault derivation. Draw every episode through
    /// [`episode`] so the budget is honoured, and finish through
    /// [`prune_concurrent_crashes`].
    fn derive_faults(family: Self::Family, rng: &mut StdRng) -> Vec<FaultEpisode>;

    fn derive_ops(
        family: Self::Family,
        toggles: &Self::Toggles,
        count: usize,
        rng: &mut StdRng,
    ) -> Vec<Self::Op>;

    /// Stable rendering for fingerprints.
    fn render_op(op: Self::Op) -> String;

    /// Nodes in this arm's topology.
    fn node_count() -> usize {
        Self::HOSTS.len()
    }
}

// =============================================================================
// Faults
// =============================================================================

/// One injected fault. Node positions are *indices into [`Arm::HOSTS`]*, not
/// node ids: a schedule is derived before the topology exists, so "the leader"
/// travels as [`LEADER`] until [`FaultKind::resolve`] binds it.
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
    /// Queue traffic between `node` and every peer.
    HoldIsolate { node: usize },
    /// Raise the `a`↔`b` link latency for the window, healed back to the
    /// schedule's baseline.
    SlowEdge { a: usize, b: usize, latency_ms: u64 },
    /// SIGKILL `node` (turmoil `crash`) and restart it at the heal (`bounce`),
    /// reusing the same data directory so its log survives.
    CrashRestart { node: usize },
}

impl FaultKind {
    /// Bind the [`LEADER`] sentinel to the discovered leader index, collapsing
    /// any self-edge onto a distinct peer.
    pub fn resolve(self, leader: usize, node_count: usize) -> Self {
        let fix = |i: usize| if i == LEADER { leader } else { i };
        match self {
            FaultKind::HoldEdge { a, b } => {
                let (a, b) = distinct(fix(a), fix(b), node_count);
                FaultKind::HoldEdge { a, b }
            }
            FaultKind::HoldIsolate { node } => FaultKind::HoldIsolate { node: fix(node) },
            FaultKind::SlowEdge { a, b, latency_ms } => {
                let (a, b) = distinct(fix(a), fix(b), node_count);
                FaultKind::SlowEdge { a, b, latency_ms }
            }
            FaultKind::CrashRestart { node } => FaultKind::CrashRestart { node: fix(node) },
        }
    }

    /// Stable rendering for fingerprints.
    pub fn render(self) -> String {
        match self {
            FaultKind::HoldEdge { a, b } => format!("hold-edge {a}-{b}"),
            FaultKind::HoldIsolate { node } => format!("hold-isolate {node}"),
            FaultKind::SlowEdge { a, b, latency_ms } => format!("slow-edge {a}-{b} {latency_ms}ms"),
            FaultKind::CrashRestart { node } => format!("crash-restart {node}"),
        }
    }

    /// True for the kinds that take a node's process down.
    pub fn is_crash(self) -> bool {
        matches!(self, FaultKind::CrashRestart { .. })
    }
}

/// Force two node indices apart so an edge fault is never a self-loop, which
/// turmoil treats as a no-op and would silently drop.
pub fn distinct(a: usize, b: usize, node_count: usize) -> (usize, usize) {
    if a == b {
        (a, (a + 1) % node_count)
    } else {
        (a, b)
    }
}

/// One fault episode: armed at `arm_at`, healed at `heal_at`, both measured in
/// simulated time from the instant the arm reported ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultEpisode {
    pub arm_at: Duration,
    pub heal_at: Duration,
    pub kind: FaultKind,
}

/// Draw one episode of `kind` inside `budget` and append it.
///
/// The one place an arm's `derive_faults` is allowed to invent timings, so
/// "every episode obeys the budget" is a property of this function rather than
/// of every call site.
pub fn episode(budget: &Budget, rng: &mut StdRng, kind: FaultKind, out: &mut Vec<FaultEpisode>) {
    let arm = budget.min_arm_ms + budget.arm_jitter_ms.draw(rng);
    let hold = budget.hold_ms.draw(rng);
    out.push(FaultEpisode {
        arm_at: Duration::from_millis(arm),
        heal_at: Duration::from_millis(arm + hold),
        kind,
    });
}

/// A composite schedule can draw two crashes with overlapping windows, which
/// takes a quorum-sized topology below quorum and turns the run into a timeout
/// rather than a test. Drop any crash episode overlapping one already kept,
/// then sort into the canonical `(arm_at, heal_at, kind)` order.
pub fn prune_concurrent_crashes(faults: Vec<FaultEpisode>) -> Vec<FaultEpisode> {
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

/// Arm (`arm = true`) or heal a single fault against `hosts`.
///
/// Topology-agnostic: every kind is expressed in turmoil primitives over host
/// indices, so the same call means the same thing whether the nodes behind the
/// names are Raft peers or a primary and its replicas.
pub fn apply_fault(
    sim: &mut turmoil::Sim<'_>,
    hosts: &[&'static str],
    base_latency_ms: u64,
    kind: FaultKind,
    arm: bool,
) {
    match kind {
        FaultKind::HoldEdge { a, b } => {
            if arm {
                sim.hold(hosts[a], hosts[b]);
            } else {
                sim.release(hosts[a], hosts[b]);
            }
        }
        FaultKind::HoldIsolate { node } => {
            for (i, peer) in hosts.iter().enumerate() {
                if i == node {
                    continue;
                }
                if arm {
                    sim.hold(hosts[node], *peer);
                } else {
                    sim.release(hosts[node], *peer);
                }
            }
        }
        FaultKind::SlowEdge { a, b, latency_ms } => {
            let value = if arm {
                Duration::from_millis(latency_ms)
            } else {
                Duration::from_millis(base_latency_ms)
            };
            sim.set_link_latency(hosts[a], hosts[b], value);
        }
        FaultKind::CrashRestart { node } => {
            if arm {
                sim.crash(hosts[node]);
            } else {
                sim.bounce(hosts[node]);
            }
        }
    }
}

// =============================================================================
// Schedule: the pure, seed-derived description of a run
// =============================================================================

/// Per-node timers. Skewing these across nodes is what makes leader election
/// deterministic once an implementation's own jitter window is collapsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeTimers {
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
}

/// The complete, seed-derived description of a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule<A: Arm> {
    pub seed: u64,
    pub family: A::Family,
    /// The arm's own booleans, drawn between the latency and the faults.
    pub toggles: A::Toggles,
    /// One entry per host, in [`Arm::HOSTS`] order.
    pub timers: Vec<NodeTimers>,
    /// Baseline link latency for the whole simulated network.
    pub base_latency_ms: u64,
    pub max_latency_ms: u64,
    /// Faults, sorted by `(arm_at, heal_at, kind)`. May overlap.
    pub faults: Vec<FaultEpisode>,
    pub ops: Vec<A::Op>,
    /// Simulated gap between successive workload ops.
    pub op_gap_ms: u64,
    /// turmoil's wall for the whole run — the hang guard, not the expected
    /// length.
    pub sim_duration: Duration,
}

impl<A: Arm> Schedule<A> {
    /// Derive the whole schedule from `seed`.
    ///
    /// The draw order is fixed here and every branch — including the arm's own
    /// hooks — consumes from the same [`StdRng`], so this is a pure function of
    /// the seed. A seed's identity *is* this order: moving a draw moves every
    /// seed's schedule at once.
    pub fn from_seed(seed: u64) -> Self {
        let budget = A::BUDGET;
        let mut rng = StdRng::seed_from_u64(seed);

        let families = A::families();
        let family = families[rng.random_range(0..families.len())];

        // Timer skew: a distinct election timeout per node, all well above the
        // heartbeat interval, so the lowest-timeout reachable node wins any
        // election without an unseeded jitter draw.
        let heartbeat_interval_ms = budget.heartbeat_interval_ms.draw(&mut rng);
        let base_election_ms = budget.base_election_ms.draw(&mut rng);
        let step_ms = budget.election_step_ms.draw(&mut rng);
        let timers: Vec<NodeTimers> = (0..A::node_count())
            .map(|i| NodeTimers {
                election_timeout_ms: base_election_ms + (i as u64) * step_ms,
                heartbeat_interval_ms,
            })
            .collect();

        let base_latency_ms = budget.base_latency_ms.draw(&mut rng);
        let max_latency_ms = base_latency_ms + budget.extra_latency_ms.draw(&mut rng);

        let toggles = A::derive_toggles(family, &mut rng);
        let faults = A::derive_faults(family, &mut rng);

        // The workload must outlive the last heal, so every run ends with a
        // quiesce window in which the network is whole again.
        let fault_end_ms = faults
            .iter()
            .map(|f| f.heal_at.as_millis() as u64)
            .max()
            .unwrap_or(0);
        let op_gap_ms = budget.op_gap_ms.draw(&mut rng);
        let (min_ops, max_ops) = budget.op_count;
        let op_count = ((fault_end_ms + budget.quiesce_tail_ms) / op_gap_ms)
            .clamp(min_ops as u64, max_ops as u64) as usize;
        let ops = A::derive_ops(family, &toggles, op_count, &mut rng);

        Schedule {
            seed,
            family,
            toggles,
            timers,
            base_latency_ms,
            max_latency_ms,
            faults,
            ops,
            op_gap_ms,
            sim_duration: budget.sim_duration,
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
            f.kind = f.kind.resolve(leader, A::node_count());
        }
        resolved
    }

    /// Canonical rendering, one line per field — the opening block of every run
    /// fingerprint, so a *schedule* divergence is reported before any run
    /// divergence.
    pub fn render(&self) -> Vec<String> {
        let mut lines = vec![
            format!("seed {}", self.seed),
            format!("family {}", A::family_token(self.family)),
        ];
        lines.extend(A::render_toggles(&self.toggles));
        lines.push(format!(
            "latency base={}ms max={}ms",
            self.base_latency_ms, self.max_latency_ms
        ));
        lines.push(format!("op_gap {}ms", self.op_gap_ms));
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
            lines.push(format!("op[{i}] {}", A::render_op(*op)));
        }
        lines
    }
}

// =============================================================================
// Run outcome and the fingerprint
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

/// Compare two run fingerprints, reporting the *first* divergence with context
/// rather than dumping two multi-hundred-line vectors.
pub fn assert_fingerprints_equal(seed: u64, a: &[String], b: &[String]) {
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

/// Read a `u64` budget knob from the environment, falling back to `default`, so
/// a sweep recipe works with no environment at all.
pub fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

// =============================================================================
// The regression-seed file and its self-expiring muzzle
// =============================================================================

/// Marker opening the optional muzzle column.
pub const EXPECTED_FAILURE: &str = "EXPECTED-FAILURE:";

/// One line of an arm's regression-seed file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegressionSeed {
    pub seed: u64,
    /// The family this seed derived when it was recorded.
    pub family: String,
    /// `Some(issue)` while the defect this seed found is still open. Such a
    /// seed is expected to *fail*: the replay test asserts it still reproduces,
    /// so the muzzle turns itself off when the fix lands instead of quietly
    /// outliving it.
    pub expected_failure: Option<String>,
    pub note: String,
}

/// Parse an arm's regression-seed file.
///
/// Format, one entry per line, `#` comments and blank lines ignored:
/// `<seed> <family> [EXPECTED-FAILURE:<issue>] <why>`. The family is recorded
/// so a change to [`Schedule::from_seed`]'s draw order is caught (the seed
/// would map somewhere else) rather than silently replaying a different
/// scenario.
pub fn regression_seeds(text: &str) -> Vec<RegressionSeed> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(parse_regression_seed)
        .collect()
}

pub fn parse_regression_seed(line: &str) -> RegressionSeed {
    let mut parts = line.splitn(3, char::is_whitespace);
    let seed: u64 = parts
        .next()
        .expect("a seed column")
        .parse()
        .unwrap_or_else(|e| panic!("bad seed in a regression-seed file: {line:?} ({e})"));
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
/// defect must not drown out the *new* failures a sweep exists to find. The
/// arm's regression replay still replays each one and asserts it reproduces, so
/// nothing stops being checked.
pub fn muzzled_seeds(text: &str) -> BTreeMap<u64, String> {
    regression_seeds(text)
        .into_iter()
        .filter_map(|r| r.expected_failure.map(|issue| (r.seed, issue)))
        .collect()
}

// =============================================================================
// Reading an arm's `DEBUG … CHECK` reply
// =============================================================================

/// Drop the violations a catalog deliberates.
///
/// A `DEBUG … CHECK` command is the *reporting* view, so it includes the
/// catalog's `DOCUMENTED-EXCEPTION` entries. Those are ruled reachable by a
/// cited failure-mode row or issue and are not defects; asserting on them would
/// make a sweep red for a state the catalog blesses. `excepted` is the arm's
/// own set of such ids.
pub fn hard_violations(
    reported: Vec<Violation>,
    excepted: &BTreeSet<&'static str>,
) -> Vec<Violation> {
    reported
        .into_iter()
        .filter(|v| !excepted.contains(v.id))
        .collect()
}

/// One `{id, detail}` map from a `DEBUG … CHECK` reply, which RESP2 flattens to
/// `[id, <v>, detail, <v>]`.
///
/// `known_ids` is the arm's catalog vocabulary: an id it contains is recovered
/// as the `&'static str` the catalog owns, so a reported violation has the same
/// shape as a locally produced one. An id the catalog does not know is
/// surfaced under `unknown_id` rather than dropped.
pub fn parse_check_entry(
    item: &RespValue,
    known_ids: &[&'static str],
    unknown_id: &'static str,
) -> Option<Violation> {
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
    let id = known_ids
        .iter()
        .copied()
        .find(|c| *c == id)
        .unwrap_or(unknown_id);
    Some(Violation { id, detail })
}

// =============================================================================
// Tests for the shared half
// =============================================================================

#[test]
fn test_regression_seed_lines_parse_the_muzzle_column() {
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
        parse_regression_seed("3 replica-partition EXPECTED-FAILURE:issue-20 split brain"),
        RegressionSeed {
            seed: 3,
            family: "replica-partition".to_string(),
            expected_failure: Some("issue-20".to_string()),
            note: "split brain".to_string(),
        }
    );
}

#[test]
fn test_regression_seed_file_comments_and_blanks_are_ignored() {
    let text = "# a comment\n\n  7 healthy why it is here\n";
    let entries = regression_seeds(text);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seed, 7);
    assert!(muzzled_seeds(text).is_empty());
    assert_eq!(
        muzzled_seeds("9 mixed EXPECTED-FAILURE:issue-3 open"),
        BTreeMap::from([(9, "issue-3".to_string())])
    );
}

#[test]
fn test_distinct_never_returns_a_self_edge() {
    assert_eq!(distinct(1, 1, 3), (1, 2));
    assert_eq!(distinct(2, 2, 3), (2, 0));
    assert_eq!(distinct(0, 2, 3), (0, 2));
}

#[test]
fn test_prune_concurrent_crashes_drops_overlaps_and_sorts() {
    let ep = |arm: u64, heal: u64, kind: FaultKind| FaultEpisode {
        arm_at: Duration::from_millis(arm),
        heal_at: Duration::from_millis(heal),
        kind,
    };
    let kept = prune_concurrent_crashes(vec![
        ep(100, 500, FaultKind::CrashRestart { node: 0 }),
        // Overlaps the first crash: would take the topology below quorum.
        ep(200, 600, FaultKind::CrashRestart { node: 1 }),
        // Disjoint from it, so it survives.
        ep(600, 900, FaultKind::CrashRestart { node: 2 }),
        // Not a crash, so overlap is fine.
        ep(50, 400, FaultKind::HoldEdge { a: 0, b: 1 }),
    ]);
    assert_eq!(kept.len(), 3);
    assert_eq!(kept[0].kind, FaultKind::HoldEdge { a: 0, b: 1 });
    assert_eq!(kept[1].kind, FaultKind::CrashRestart { node: 0 });
    assert_eq!(kept[2].kind, FaultKind::CrashRestart { node: 2 });
}

#[test]
fn test_parse_check_entry_recovers_a_known_id_and_surfaces_an_unknown_one() {
    let entry = |id: &str| {
        RespValue::Array(Some(vec![
            RespValue::Bulk(Some(b"id".to_vec())),
            RespValue::Bulk(Some(id.as_bytes().to_vec())),
            RespValue::Bulk(Some(b"detail".to_vec())),
            RespValue::Bulk(Some(b"why".to_vec())),
        ]))
    };
    let known: [&'static str; 2] = ["INV-A-1", "INV-B-2"];
    let v = parse_check_entry(&entry("INV-B-2"), &known, "UNKNOWN").expect("a violation");
    assert_eq!(v.id, "INV-B-2");
    assert_eq!(v.detail, "why");
    assert_eq!(
        parse_check_entry(&entry("INV-NOPE-9"), &known, "UNKNOWN")
            .expect("surfaced")
            .id,
        "UNKNOWN"
    );
    // Anything that is not a flat `{id, detail}` map is not an entry.
    assert!(parse_check_entry(&RespValue::Bulk(None), &known, "UNKNOWN").is_none());
}

#[test]
fn test_hard_violations_drops_only_the_excepted_ids() {
    let excepted: BTreeSet<&'static str> = BTreeSet::from(["INV-B-2"]);
    let reported = vec![
        Violation {
            id: "INV-A-1",
            detail: "hard".to_string(),
        },
        Violation {
            id: "INV-B-2",
            detail: "excepted".to_string(),
        },
    ];
    let kept = hard_violations(reported, &excepted);
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].id, "INV-A-1");
}
