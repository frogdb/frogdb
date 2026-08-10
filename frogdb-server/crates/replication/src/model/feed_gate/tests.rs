//! Model-checking runs for the feed gate. See the [module docs](super) for
//! the scopes and the measured budgets.

use std::time::Instant;

use stateright::{Checker, Model};

use super::{FeedGate, Scope, churn_scope, overlapping_scope, smoke_scope, unheld_feed_scope};

/// Guards against a scope edit that quietly shrinks the explored space: a
/// green run over three states proves nothing. Set just under the measured
/// counts recorded in the [module docs](super).
const MIN_SMOKE_STATES: usize = 20_000;
const MIN_OVERLAPPING_STATES: usize = 3_900_000;
const MIN_CHURN_STATES: usize = 2_600_000;

fn report<M: Model>(label: &str, checker: &impl Checker<M>, elapsed: std::time::Duration) -> usize {
    let states = checker.unique_state_count();
    println!(
        "stateright feed-gate model [{label}]: {states} unique states, depth {}, in {:.1}s",
        checker.max_depth(),
        elapsed.as_secs_f64()
    );
    states
}

fn num_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

fn explore(label: &str, scope: Scope) -> impl Checker<FeedGate> {
    let started = Instant::now();
    let checker = FeedGate::new(scope)
        .checker()
        .threads(num_threads())
        .spawn_bfs()
        .join();
    report(label, &checker, started.elapsed());
    checker
}

fn check(label: &str, scope: Scope, min_states: usize) {
    let checker = explore(label, scope);
    let states = checker.unique_state_count();
    assert!(
        states >= min_states,
        "the {label} scope collapsed to {states} states (floor {min_states}) — a scope edit \
         shrank the explored space, so a green run here proves less than it used to"
    );
    checker.assert_properties();
}

/// Bounded smoke configuration, in the default suite.
// FM-CLUSTER-097
#[test]
fn feed_gate_model_smoke() {
    check("smoke", smoke_scope(), MIN_SMOKE_STATES);
}

/// Full budget, depth: two barriers that overlap and two sessions over a
/// longer horizon. Nightly (`replication-model-nightly`) and
/// `just replication-model-check`.
// FM-CLUSTER-097
#[test]
#[ignore = "full model-checking budget: nightly / `just replication-model-check`"]
fn feed_gate_model_full_overlapping() {
    check(
        "full/overlapping",
        overlapping_scope(),
        MIN_OVERLAPPING_STATES,
    );
}

/// Full budget, breadth: three sessions and a primary stint that ends inside a
/// barrier window. Nightly and `just replication-model-check`.
// FM-CLUSTER-097
#[test]
#[ignore = "full model-checking budget: nightly / `just replication-model-check`"]
fn feed_gate_model_full_churn() {
    check("full/churn", churn_scope(), MIN_CHURN_STATES);
}

/// Level-3 evidence for the replication-correctness PRD's retro-validation
/// revert (d), obtained without a reverted tree: withdraw the assumption that
/// the sessions consult the gate — the tree before `8d55cc4f` — and the model
/// puts a frame on the wire inside a barrier window.
///
/// The counterexample this finds is re-run against the real gate by
/// [`super::replay`], which shows the shipped-tree gate refusing it.
// FM-CLUSTER-097
#[test]
fn a_feed_that_ignores_the_gate_ships_inside_a_barrier_window() {
    let checker = explore("unheld-feed", unheld_feed_scope());
    let path = checker
        .discovery("no_frame_ships_while_a_barrier_is_armed")
        .expect(
            "a session that never consults the gate must be able to ship inside a barrier \
             window — if this scope has gone green, the model has stopped exercising the \
             behaviour `8d55cc4f` fixed",
        );
    println!("counterexample: {:?}", path.into_actions());
}
