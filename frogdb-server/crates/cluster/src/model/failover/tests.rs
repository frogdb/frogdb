//! Model-checking runs for the failover composite. See the [module
//! docs](super) for the scopes.

use std::time::Instant;

use stateright::{Checker, Model};

use super::{
    Failover, Scope, absorb_scope, smoke_scope, stranded_scope, two_leader_scope,
    unjustified_promotion_scope,
};

/// Guards against a scope edit that quietly shrinks the explored space: a
/// green run over three states proves nothing. Set just under the measured
/// counts recorded in the [module docs](super).
const MIN_SMOKE_STATES: usize = 500_000;
const MIN_TWO_LEADER_STATES: usize = 23_000_000;
const MIN_ABSORB_STATES: usize = 1_700_000;

fn report<M: Model>(label: &str, checker: &impl Checker<M>, elapsed: std::time::Duration) -> usize {
    let states = checker.unique_state_count();
    println!(
        "stateright failover model [{label}]: {states} unique states, depth {}, in {:.1}s",
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
    let checker = Failover::new(scope)
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
#[test]
fn failover_model_smoke() {
    check("smoke", smoke_scope(), MIN_SMOKE_STATES);
}

/// Full budget, depth: two would-be leaders, a second round of verdicts, two
/// manual takeovers and two leader changes. Nightly
/// (`cluster-model-nightly`) and `just model-check`.
#[test]
#[ignore = "full model-checking budget: nightly / `just model-check`"]
fn failover_model_full_two_leader() {
    check("full/two-leader", two_leader_scope(), MIN_TWO_LEADER_STATES);
}

/// Full budget, breadth: two slots and a successor that is already a primary,
/// so the absorb path (slots move, nobody is promoted) is covered alongside
/// the promotion path. Nightly and `just model-check`.
#[test]
#[ignore = "full model-checking budget: nightly / `just model-check`"]
fn failover_model_full_absorb() {
    check("full/absorb", absorb_scope(), MIN_ABSORB_STATES);
}

/// The smallest witness for issue 18, and the one [`super::replay`] re-runs
/// against the state machine directly.
///
/// One detector, on the node that ends up as the healthy replica. It fails the
/// original primary over onto its peer, then flags that peer — and by the time
/// it looks for a successor its own snapshot still predates the failover, so
/// it sees a replica rather than a primary and returns. Nothing re-arms it,
/// and the cluster comes to rest with the slot on a node it has flagged FAIL.
///
/// A *characterization* test: it asserts the counterexample is still there, so
/// a fix fails it loudly instead of leaving a dead scope behind.
#[test]
fn a_slot_strands_on_a_primary_the_cluster_has_failed() {
    let started = Instant::now();
    let checker = Failover::new(stranded_scope())
        .checker()
        .threads(num_threads())
        .spawn_bfs()
        .join();
    report("stranded", &checker, started.elapsed());
    let path = checker
        .discovery("a_slot_strands_on_a_failed_primary")
        .expect("issue 18 is open: the minimal scope must still strand a slot");
    println!("counterexample: {:?}", path.into_actions());
}

/// The smallest witness for issue 19, and the one [`super::replay`] re-runs
/// against the state machine directly.
///
/// After the primary is failed over onto node 2, node 3 is reparented onto
/// node 2 — but node 3 has not applied that yet, so an operator's `CLUSTER
/// FAILOVER FORCE` there names the departed primary as the node to take over
/// from. `force` waives the members-check, and node 3 is promoted out of a
/// replication link that is still live, taking the cluster's highest config
/// epoch with it and leaving node 2 without a replica.
///
/// A *characterization* test: see above.
#[test]
fn a_promotion_can_move_nothing() {
    let started = Instant::now();
    let checker = Failover::new(unjustified_promotion_scope())
        .checker()
        .threads(num_threads())
        .spawn_bfs()
        .join();
    report("unjustified-promotion", &checker, started.elapsed());
    let path = checker
        .discovery("a_promotion_moves_nothing")
        .expect("issue 19 is open: the minimal scope must still promote an unrelated node");
    println!("counterexample: {:?}", path.into_actions());
}
