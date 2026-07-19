//! Generated-workload seed sweep: drive seeded workloads against the real
//! server under turmoil, run the invariant pipeline, and emit a repro file on
//! any failure. See `docs/superpowers/plans/concurrency-phase3-bug-workflow.md`.
#![cfg(feature = "turmoil")]

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
    let (history, final_elements) = run_workload_capturing(&workload, num_shards, true);
    let report = check_all(&history, &final_elements);
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

#[test]
fn seed_sweep_short_workloads() {
    // ~20 seeds x short workloads (CI per-PR tier).
    for seed in 0..20u64 {
        let profile = match seed % 3 {
            0 => Profile::TxHeavy,
            1 => Profile::BlockingHeavy,
            _ => Profile::Mixed,
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
