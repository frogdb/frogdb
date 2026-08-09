//! Model-checking runs. See the [module docs](super) for the scopes.

use std::time::Instant;

use stateright::{Checker, Model};

use super::{Handoff, cross_slot_scope, deep_scope, smoke_scope, unbounded_lag_scope};

/// Guards against a scope edit that quietly shrinks the explored space: a
/// green run over three states proves nothing. Set just under the measured
/// counts recorded in the [module docs](super).
const MIN_SMOKE_STATES: usize = 30_000;
const MIN_CROSS_SLOT_STATES: usize = 1_250_000;
const MIN_DEEP_STATES: usize = 11_000_000;

fn report<M: Model>(label: &str, checker: &impl Checker<M>, elapsed: std::time::Duration) -> usize {
    let states = checker.unique_state_count();
    println!(
        "stateright handoff model [{label}]: {states} unique states, depth {}, in {:.1}s",
        checker.max_depth(),
        elapsed.as_secs_f64()
    );
    states
}

fn num_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

/// Bounded-depth smoke configuration, in the default suite.
///
/// FM-CLUSTER-084 FM-CLUSTER-086 FM-CLUSTER-100
#[test]
fn handoff_model_smoke() {
    check("smoke", smoke_scope(), MIN_SMOKE_STATES);
}

/// Full budget, breadth: both handoffs in flight at once, so node 2 is the
/// target of one and the source of the other. Nightly
/// (`cluster-model-nightly`) and `just model-check`.
///
/// FM-CLUSTER-084 FM-CLUSTER-086 FM-CLUSTER-088 FM-CLUSTER-100
#[test]
#[ignore = "full model-checking budget: nightly / `just model-check`"]
fn handoff_model_full_cross_slot() {
    check("full/cross-slot", cross_slot_scope(), MIN_CROSS_SLOT_STATES);
}

/// Full budget, depth: one handoff, three attempts, duplicated drain
/// acknowledgements and two leader changes. Nightly and `just model-check`.
///
/// FM-CLUSTER-084 FM-CLUSTER-085 FM-CLUSTER-086 FM-CLUSTER-100
#[test]
#[ignore = "full model-checking budget: nightly / `just model-check`"]
fn handoff_model_full_deep() {
    check("full/deep", deep_scope(), MIN_DEEP_STATES);
}

fn check(label: &str, scope: super::Scope, min_states: usize) {
    let started = Instant::now();
    let checker = Handoff::new(scope)
        .checker()
        .threads(num_threads())
        .spawn_bfs()
        .join();
    let states = report(label, &checker, started.elapsed());
    assert!(
        states >= min_states,
        "the {label} scope collapsed to {states} states (floor {min_states}) — a scope edit \
         shrank the explored space, so a green run here proves less than it used to"
    );
    checker.assert_properties();
}

/// Withdrawing the bounded-apply-lag assumption exposes a stale source.
///
/// The handoff design leans on a node applying a committed entry within
/// `barrier_ms` of applying the prepare; drop that and a source whose local
/// pause has lapsed keeps serving a slot the target already owns. This is a
/// *characterization* test: it asserts the counterexample is still there, so a
/// fix (issue 16) fails it loudly instead of leaving a dead scope behind.
/// [`super::replay`] re-runs the trace against the state machine directly.
#[test]
fn stale_source_admits_writes_after_ownership_moves() {
    let started = Instant::now();
    let checker = Handoff::new(unbounded_lag_scope())
        .checker()
        .spawn_bfs()
        .join();
    report("unbounded-lag", &checker, started.elapsed());
    let path = checker
        .discovery("single_writer_per_slot")
        .expect("issue 16 is open: the unbounded-lag scope must still expose two writers");
    println!("counterexample: {:?}", path.into_actions());
}
