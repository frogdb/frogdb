//! Pub/sub oracle seed sweep (Phase 4c): drive seeded [`PubSubWorkload`]s
//! against the real server under turmoil, then run the pub/sub conservation +
//! order checkers. Pub/sub is the audit's weakest area (zero concurrency-tooling
//! coverage before this phase); it is not per-key-linearizability-checkable, so
//! it has its own oracle (`frogdb_testing::pubsub_oracle`).
//!
//! Turmoil-only: gated at the `mod concurrency_pubsub;` declaration in
//! `tests/main.rs`.

use frogdb_testing::{
    PubSubHistory, PubSubWorkload, check_pubsub_conservation, check_pubsub_order,
};
// The fault-injection helpers live in the oracle module.
use frogdb_testing::pubsub_oracle::{
    drop_delivery, duplicate_delivery, phantom_delivery, reorder_delivery,
};

use crate::common::pubsub_runner::run_pubsub_workload;

/// Run both pub/sub checkers over a captured history, returning the violations.
fn check(history: &PubSubHistory) -> Vec<String> {
    let mut v = Vec::new();
    if let Err(e) = check_pubsub_conservation(history) {
        v.push(format!("conservation: {e}"));
    }
    if let Err(e) = check_pubsub_order(history) {
        v.push(format!("order: {e}"));
    }
    v
}

/// Generate → run against the real server → check the pub/sub oracle.
fn run_and_check(
    seed: u64,
    num_subscribers: usize,
    num_publishers: usize,
    msgs_per_publisher: usize,
    num_shards: usize,
) -> (PubSubHistory, Vec<String>) {
    let workload =
        PubSubWorkload::generate(seed, num_subscribers, num_publishers, msgs_per_publisher);
    let history = run_pubsub_workload(&workload, num_shards);
    let violations = check(&history);
    (history, violations)
}

/// Full pub/sub seed sweep (CI per-PR tier). 20 seeds × small pub/sub workloads
/// against a 2-shard server (regular PUBLISH funnels to the coordinator shard 0,
/// so this also exercises the cross-shard subscriber→publisher path). Each run
/// asserts (a) the reader actually captured deliveries — a broken frame reader
/// that recorded nothing would silently green the conservation checker — and
/// (b) no conservation/order violation.
#[test]
fn seed_sweep_pubsub() {
    for seed in 0..20u64 {
        let (history, violations) = run_and_check(seed, 3, 3, 8, 2);
        assert!(
            history.publish_count() > 0,
            "seed {seed}: no publishes recorded — runner is broken"
        );
        assert!(
            history.receive_count() > 0,
            "seed {seed}: no deliveries captured — the frame reader is silently \
             disabled (conservation checker would false-pass)"
        );
        assert!(
            violations.is_empty(),
            "seed {seed}: pub/sub oracle violated: {violations:?}\n{}",
            history.to_json()
        );
    }
}

/// Single-shard variant: the `Local` keyspace/pubsub topology (num_shards == 1),
/// the byte-for-byte synchronous delivery path. Kept small and separate so a
/// single-shard-only regression is distinguishable from a cross-shard one.
#[test]
fn pubsub_single_shard_is_clean() {
    let (history, violations) = run_and_check(0, 3, 2, 6, 1);
    assert!(history.receive_count() > 0, "no deliveries captured");
    assert!(
        violations.is_empty(),
        "single-shard pub/sub violated: {violations:?}\n{}",
        history.to_json()
    );
}

/// Harness self-test (silent-green guard): a real captured history must pass,
/// and each deliberately-corrupted copy (drop / duplicate / phantom / reorder a
/// delivery) must be caught by the checkers. This is the pub/sub arm of the
/// spec's "Harness self-tests" requirement — it proves the sweep above is not
/// green because the checkers are inert.
#[test]
fn pubsub_fault_injection_is_caught() {
    // A history rich enough to have firmly-in-window deliveries and at least one
    // multi-message publisher stream (so reorder has two receives to swap).
    let workload = PubSubWorkload::generate(0, 3, 3, 8);
    let history = run_pubsub_workload(&workload, 2);
    assert!(
        history.receive_count() >= 2,
        "need ≥2 deliveries for the reorder fault; got {}",
        history.receive_count()
    );

    // The genuine history is clean.
    assert!(
        check(&history).is_empty(),
        "baseline pub/sub history must be clean: {:?}",
        check(&history)
    );

    // Each corruption is caught. `drop`/`duplicate`/`phantom` trip
    // conservation; `reorder` trips order. (Aliased imports keep both the
    // re-exported and module paths exercised.)
    let dropped = drop_delivery(&history);
    assert!(
        !check(&dropped).is_empty(),
        "dropped delivery must be caught"
    );
    let dup = duplicate_delivery(&history);
    assert!(!check(&dup).is_empty(), "duplicate delivery must be caught");
    let phantom = phantom_delivery(&history);
    assert!(
        !check(&phantom).is_empty(),
        "phantom delivery must be caught"
    );
    let reordered = reorder_delivery(&history);
    assert!(
        check_pubsub_order(&reordered).is_err(),
        "reordered delivery must be caught by the order checker"
    );
}

/// Determinism: same seed ⇒ identical captured deliveries. A flapping pub/sub
/// history would make repro files worthless.
#[test]
fn pubsub_run_is_deterministic() {
    let workload = PubSubWorkload::generate(3, 2, 2, 6);
    let a = run_pubsub_workload(&workload, 2);
    let b = run_pubsub_workload(&workload, 2);
    assert_eq!(
        a.receive_count(),
        b.receive_count(),
        "delivery count must be reproducible for a fixed seed"
    );
    assert_eq!(a.publish_count(), b.publish_count());
}

/// Pinned regressions: one named test per confirmed pub/sub bug, carrying its
/// hardcoded failing seed. Copy the template when the sweep trips a real bug.
mod regressions {
    use super::*;

    /// Template. Seed 0 (2 shards) exercises channel + pattern subscribers
    /// against three publishers; it passes today. When the sweep trips a real
    /// bug, copy this, name it for the bug, hardcode the seed, and land it with
    /// the fix (must FAIL before, PASS after).
    #[test]
    fn regression_template_seed_0() {
        let (history, violations) = run_and_check(0, 3, 3, 8, 2);
        assert!(
            violations.is_empty(),
            "pinned pub/sub seed regressed: {violations:?}\n{}",
            history.to_json()
        );
    }
}
