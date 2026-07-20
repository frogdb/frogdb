//! Generated-workload seed sweep: drive seeded workloads against the real
//! server under turmoil, run the invariant pipeline, and emit a repro file on
//! any failure. See `docs/superpowers/plans/concurrency-phase3-bug-workflow.md`.
//!
//! Turmoil-only: gated at the `mod concurrency_workload;` declaration in
//! `tests/main.rs` (`#[cfg(feature = "turmoil")]`), so no inner `#![cfg]` here
//! (that would trip `clippy::duplicated_attributes`).

use std::path::PathBuf;

use frogdb_testing::{Profile, Workload};

use crate::common::invariants::{InvariantReport, check_all};
use crate::common::repro::{ReproFile, read_repro, repro_path, write_repro};
use crate::common::workload_runner::run_workload_capturing;

/// Generate → run against the real server (fake persistence) → check invariants.
fn run_and_check(
    seed: u64,
    profile: Profile,
    num_clients: usize,
    ops_per_client: usize,
    num_shards: usize,
) -> InvariantReport {
    let workload = Workload::generate(seed, profile, num_clients, ops_per_client);
    let run = run_workload_capturing(&workload, num_shards, true);
    let report = check_all(&run.history, &run.final_elements, Some(&run.quiescence));
    if !report.passed() {
        eprintln!("seed {seed} ({profile:?}) FAILED: {:?}", report.violations);
    }
    report
}

/// Write a repro file for a failing seed, returning its path.
fn write_repro_for(
    seed: u64,
    profile: Profile,
    num_clients: usize,
    ops_per_client: usize,
    num_shards: usize,
) -> PathBuf {
    let path = repro_path(seed);
    let repro = ReproFile {
        seed,
        profile,
        num_clients,
        ops_per_client,
        num_shards,
    };
    write_repro(&path, &repro).expect("write repro file");
    path
}

// Full-size generated seed sweep (CI per-PR tier), BlockingHeavy + Mixed
// profiles. This is real, enabled coverage: it drives seeded workloads against
// the real multi-shard server and runs the full invariant pipeline.
//
// Previously the whole sweep was `#[ignore]`d because `to_testing_history`
// collapsed every op into a zero-width point at completion time (manufacturing
// wholesale false non-linearizability under real concurrency). That collapse is
// fixed (plus the aborted-EXEC-as-`Some("")` recorder defect and `KVModel::exec`
// rejecting nil aborts), so the List/Stream/Hash/ZSet/plain-KV vocabulary now
// linearizes correctly.
//
// TxHeavy is intentionally excluded here and deferred to the `#[ignore]`d
// `seed_sweep_txheavy` below — see its comment for the specific remaining
// harness gap (cross-slot WATCH/EXEC generation vs. the KV model).
#[test]
fn seed_sweep_short_workloads() {
    // ~20 seeds x short workloads (CI per-PR tier), alternating the two
    // fully-supported profiles.
    for seed in 0..20u64 {
        let profile = if seed % 2 == 0 {
            Profile::Mixed
        } else {
            Profile::BlockingHeavy
        };
        let report = run_and_check(seed, profile, 4, 30, 2);
        if !report.passed() {
            let path = write_repro_for(seed, profile, 4, 30, 2);
            panic!(
                "seed {seed} ({profile:?}) violated invariants: {:?}\nrepro: {}",
                report.violations,
                path.display()
            );
        }
    }
}

// The tier-4 quiescence stage: run one small workload and assert the DEBUG
// introspection probes ran (LOCKTABLE / WAITQUEUE / MEMORY-CHECK /
// EXPIRY-INDEX-CHECK), and that a drained, quiesced server reports no
// quiescence violation (empty lock table + wait queue, consistent memory and
// expiry index). This is the live-wiring smoke test for the probe→snapshot
// adapter; the full sweep above exercises it every seed.
#[test]
fn quiescence_stage_runs_and_is_clean() {
    let report = run_and_check(0, Profile::Mixed, 2, 8, 2);
    assert!(
        report.quiescence_checked,
        "quiescence stage must run (DEBUG snapshots supplied)"
    );
    let quiescence_violations: Vec<_> = report
        .violations
        .iter()
        .filter(|v| v.starts_with("quiescence: "))
        .collect();
    assert!(
        quiescence_violations.is_empty(),
        "a quiesced server must report no quiescence violation: {quiescence_violations:?}"
    );
    assert!(
        report.passed(),
        "small clean workload must pass all stages: {:?}",
        report.violations
    );
}

// TxHeavy seed sweep — DEFERRED (documented harness/model gap, not a server bug).
//
// Root cause (seed 0, verified by dumping the per-key Kv sub-histories): the
// TxHeavy generator emits standalone `watch` ops on independently-chosen keys
// from a two-slot key family ({t0}kv0, {t1}kv1) plus `exec` transactions whose
// sub-commands also pick keys independently. Because a connection's WATCH set
// accumulates until an EXEC/DISCARD clears it, many EXECs end up with a watched
// set (and/or sub-command keys) that span two hash slots. The multi-shard server
// then *correctly* rejects those EXECs with `-CROSSSLOT` (and returns empty-array
// aborts in related cases). `KVModel::exec` has no encoding for a CROSSSLOT/abort
// EXEC result, so it treats every such result as a value mismatch and rejects it,
// poisoning the entire per-key Kv sub-history (every op flagged). The plain-KV
// ops themselves are linearizable, and the non-TxHeavy sweep above is green — so
// this is a generator+model gap, not a FrogDB bug.
//
// Fixing it properly requires either (a) generating coherent single-slot
// transactions (all watched + exec'd keys in one slot), or (b) teaching the
// recorder/model that a CROSSSLOT/abort EXEC is a legal no-op (state unchanged).
// Neither is a small change; tracked as the #1 phase-3 follow-up. Run manually
// with `--run-ignored all` to reproduce.
#[test]
#[ignore = "harness/model gap (not a server bug): TxHeavy generates cross-slot WATCH/EXEC combos the multi-shard server correctly rejects with CROSSSLOT; KVModel does not encode CROSSSLOT/abort EXEC results. See comment + final report."]
fn seed_sweep_txheavy() {
    for seed in 0..20u64 {
        let report = run_and_check(seed, Profile::TxHeavy, 4, 30, 2);
        if !report.passed() {
            let path = write_repro_for(seed, Profile::TxHeavy, 4, 30, 2);
            panic!(
                "seed {seed} (TxHeavy) violated invariants: {:?}\nrepro: {}",
                report.violations,
                path.display()
            );
        }
    }
}

#[test]
#[ignore = "replay a single repro file via `just concurrency-repro <file>`"]
fn replay_repro() {
    let path = std::env::var("REPRO_FILE").expect("set REPRO_FILE");
    let r = read_repro(&path);
    let report = run_and_check(
        r.seed,
        r.profile,
        r.num_clients,
        r.ops_per_client,
        r.num_shards,
    );
    assert!(
        report.passed(),
        "repro {} still fails: {:?}",
        path,
        report.violations
    );
}

/// Pinned regressions: one named test per confirmed bug, carrying its
/// hardcoded failing seed so it can never silently regress. See
/// `docs/superpowers/plans/concurrency-phase3-bug-workflow.md`.
mod regressions {
    use super::*;

    /// Template. When the sweep trips a real bug, copy this, name it for the
    /// bug, hardcode the failing seed/profile/config, and land it alongside the
    /// fix. It must FAIL before the fix and PASS after.
    ///
    /// Seed 0 (Mixed) exercises the Kv/List/Stream families under heavy
    /// concurrency; it passes now that the `to_testing_history` collapse and the
    /// EXEC-abort encoding defects are fixed (see `seed_sweep_short_workloads`).
    #[test]
    fn regression_template_seed_0() {
        let report = run_and_check(0, Profile::Mixed, 4, 30, 2);
        assert!(
            report.passed(),
            "pinned seed regressed: {:?}",
            report.violations
        );
    }
}
