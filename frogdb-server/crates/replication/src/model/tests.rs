//! Model-checking runs for the promotion / resume composite. See the [module
//! docs](super) for the scopes and the measured budget.

use std::time::Instant;

use stateright::{Checker, Model};

use super::{Promotion, Scope, deep_scope, smoke_scope, strand_scope, two_primary_scope};

/// Guards against a scope edit that quietly shrinks the explored space: a green
/// run over three states proves nothing. Set just under the measured counts
/// recorded in the [module docs](super).
const MIN_SMOKE_STATES: usize = 120_000;
const MIN_DEEP_STATES: usize = 3_000_000;
const MIN_TWO_PRIMARY_STATES: usize = 35_000_000;

fn report<M: Model>(label: &str, checker: &impl Checker<M>, elapsed: std::time::Duration) -> usize {
    let states = checker.unique_state_count();
    println!(
        "stateright promotion model [{label}]: {states} unique states, depth {}, in {:.1}s",
        checker.max_depth(),
        elapsed.as_secs_f64()
    );
    states
}

fn num_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

fn check(label: &str, scope: Scope, min_states: usize) {
    let started = Instant::now();
    let checker = Promotion::new(scope)
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

/// Bounded smoke configuration, in the default suite.
// FM-REPLICATION-019
#[test]
fn promotion_model_smoke() {
    check("smoke", smoke_scope(), MIN_SMOKE_STATES);
}

/// The known exposure (issue 16) in its smallest scope, in the default suite:
/// the model's reachability witness for what [`super::replay`] re-runs against
/// real state.
///
/// A *characterization* test. When the exposure is fixed the `sometimes`
/// property goes unwitnessed and this fails, which is the signal to flip it to
/// an `always` — see the property's own note.
// FM-REPLICATION-020
#[test]
fn a_failed_promotion_strands_the_node() {
    let started = Instant::now();
    let checker = Promotion::new(strand_scope())
        .checker()
        .threads(num_threads())
        .spawn_bfs()
        .join();
    report("strand", &checker, started.elapsed());
    checker.assert_properties();
}

/// Full budget, depth: two nodes, two promotions, a demotion, and enough write
/// depth that a second stint's failover window sits above the first's. Nightly
/// (`replication-model-nightly`) and `just replication-model-check`.
// FM-REPLICATION-019
#[test]
#[ignore = "full model-checking budget: nightly / `just replication-model-check`"]
fn promotion_model_full_deep() {
    check("full/deep", deep_scope(), MIN_DEEP_STATES);
}

/// Full budget, breadth: three nodes and two promotions with no demotion, so
/// two would-be primaries serve resumes at once and a surviving replica can be
/// moved between them. Nightly and `just replication-model-check`.
// FM-REPLICATION-013
#[test]
#[ignore = "full model-checking budget: nightly / `just replication-model-check`"]
fn promotion_model_full_two_primary() {
    check(
        "full/two-primary",
        two_primary_scope(),
        MIN_TWO_PRIMARY_STATES,
    );
}
