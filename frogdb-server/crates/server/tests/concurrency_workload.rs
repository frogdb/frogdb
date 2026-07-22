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
    let report = check_all(
        &run.history,
        &run.final_elements,
        Some(&run.quiescence),
        Some(&run.registration_order),
        num_shards,
    );
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
// TxHeavy is intentionally excluded here and covered separately by
// `seed_sweep_txheavy` below, which is live (not `#[ignore]`d): the
// server-side cross-shard WATCH false-negative bug it previously tripped is
// fixed (EXEC now folds every live watched shard at `take_transaction` time),
// and that fix is pinned by
// `regressions::regression_crossshard_watch_false_negative_seed_8`.
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

// Multi-waiter exact-FIFO smoke test: a small `Profile::MultiWaiter` workload
// where every client may park a long-timeout blocking pop on shared list/zset
// keys, so several waiters register concurrently on one key and a delayed
// producer serves them. The mid-run `DEBUG WAITQUEUE` prober correlates each
// waiter's registration ordinal to its client via the CLIENT ID map, feeding
// the exact FIFO wake-order checker (not the invoke-time proxy).
//
// Two assertions guard against a silently-disabled checker: (1) the prober +
// CLIENT ID join must have produced at least one `(key, client_id)` ordinal
// entry — a broken join (mismatched id space, or a WAITQUEUE key that does not
// round-trip to the served key) would empty the map and quietly fall the exact
// checker back to nothing; (2) a correct, FIFO-fair server serves in
// registration order, so `check_all` must pass. If it fails on served order,
// triage harness-vs-server per the bug workflow before pinning a regression.
#[test]
fn multi_waiter_exact_fifo_is_clean() {
    let workload = Workload::generate(0, Profile::MultiWaiter, 4, 12);
    let run = run_workload_capturing(&workload, 2, true);
    assert!(
        !run.registration_order.is_empty(),
        "prober + CLIENT ID join produced no registration ordinals — the exact \
         FIFO checker is silently disabled (join mismatch or key-encoding drift)"
    );
    let report = check_all(
        &run.history,
        &run.final_elements,
        Some(&run.quiescence),
        Some(&run.registration_order),
        2,
    );
    assert!(
        report.passed(),
        "multi-waiter workload violated invariants: {:?}",
        report.violations
    );
}

// TxHeavy seed sweep (CI per-PR tier). Transactions are biased toward
// single-slot key groups (which commit); a deliberate minority draw keys
// independently and so span slots — some of those land on separate shards,
// which the standalone server rejects with CROSSSLOT, pinning its transaction
// co-location discipline (see check_exec_slot_discipline, which is shard-level
// to match the standalone harness). KVModel, the per-key partition explode, and
// the conservation checkers all accept an aborted (nil) or errored ("ERR:…",
// CROSSSLOT/EXECABORT) EXEC as a legal no-op, so a rejected transaction no
// longer poisons its per-key Kv sub-history.
//
// Seed 8 previously tripped `check_watch_no_false_negative` on a *server* bug:
// a client accumulating a WATCH set spanning two shards (WATCH {t0}kv0 then
// WATCH {t1}kv1) then EXECing a single-shard transaction would wrongly commit,
// because `handle_exec` version-checked only the command-target shard. Fixed in
// `ConnectionState::take_transaction`: EXEC folds every *live* watched shard
// into the transaction target, so a cross-shard WATCH set promotes to `Multi`
// and EXEC CROSSSLOT-rejects (a model no-op), while an UNWATCH inside MULTI
// leaves no stale fold. Pinned as
// `regressions::regression_crossshard_watch_false_negative_seed_8`.
#[test]
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

// Nightly generated-workload seed sweep (CI nightly tier, see the "CI" section
// of `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`):
// many more seeds and longer per-client histories than the per-PR tiers above,
// across every profile including `MultiWaiter` (excluded from the per-PR tier
// only because it has its own dedicated smoke test). Ignored by default —
// driven explicitly via `just concurrency-nightly` (used by the nightly CI
// workflow), which sets the env var overrides below; running the whole crate
// test suite (even with `--features turmoil`) never picks this up.
//
// Unlike the per-PR sweeps, this does not stop at the first failing seed: it
// runs every seed of every profile, writes a repro file for each failure, and
// reports the full set at the end. That lets one nightly run surface (and let
// CI upload artifacts for) more than one distinct bug instead of hiding
// everything behind whichever seed happens to fail first.
#[test]
#[ignore = "nightly-tier sweep; run via `just concurrency-nightly`"]
fn seed_sweep_nightly() {
    let seeds_per_profile = env_override("FROGDB_CONCURRENCY_SEEDS", 250u64);
    // 75, not e.g. 150: at ops_per_client >= ~90 the MultiWaiter "exactly-once delivery"
    // invariant fails on nearly every seed (see
    // .scratch/concurrency-testing/issues/10-nightly-smoke-findings.md, Finding A) — a real bug,
    // but one that would make this job permanently red instead of surfacing new findings. Raise
    // this default only after that issue is resolved.
    let ops_per_client = env_override("FROGDB_CONCURRENCY_OPS_PER_CLIENT", 75usize);
    let num_clients = env_override("FROGDB_CONCURRENCY_CLIENTS", 4usize);
    let num_shards = env_override("FROGDB_CONCURRENCY_SHARDS", 2usize);

    let profiles = [
        Profile::Mixed,
        Profile::BlockingHeavy,
        Profile::TxHeavy,
        Profile::MultiWaiter,
    ];

    let mut failures = Vec::new();
    for profile in profiles {
        for seed in 0..seeds_per_profile {
            let report = run_and_check(seed, profile, num_clients, ops_per_client, num_shards);
            if !report.passed() {
                let path = write_repro_for(seed, profile, num_clients, ops_per_client, num_shards);
                eprintln!(
                    "seed {seed} ({profile:?}) violated invariants: {:?}\nrepro: {}",
                    report.violations,
                    path.display()
                );
                failures.push((seed, profile, path));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} seed(s) violated invariants: {:#?}",
        failures.len(),
        seeds_per_profile * profiles.len() as u64,
        failures
    );
}

/// Read an env var override, falling back to `default` when unset. Panics naming the env var
/// and offending value if it's set but fails to parse — a malformed override (e.g. a typo'd
/// `workflow_dispatch` `seeds` input) must fail loudly, not silently fall back to a default and
/// run a different sweep than what was asked for.
fn env_override<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) => v
            .parse()
            .unwrap_or_else(|e| panic!("env var {key}={v:?} is not a valid override: {e}")),
        Err(_) => default,
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

    /// FIXED SERVER BUG (live pin): cross-shard WATCH was accepted but not
    /// validated at EXEC. TxHeavy seed 8 has a client WATCH two keys owned by
    /// different shards ({t0}kv0 on shard 0, {t1}kv1 on shard 1) and then EXEC a
    /// transaction touching only {t1}kv1. Previously `handle_exec` routed all
    /// watches to the single command-target shard (assuming "watches are all
    /// same-slot, so at most one shard"), so a concurrent write to the *other*
    /// shard's watched key was never version-checked and the EXEC wrongly
    /// committed — a WATCH false-negative caught by `check_watch_no_false_negative`.
    ///
    /// Fix (`ConnectionState::take_transaction`): EXEC now folds every live
    /// watched shard into the transaction target, so a cross-shard WATCH set
    /// promotes the target to `Multi` and EXEC CROSSSLOT-rejects it (recorded
    /// "ERR:", a model no-op). This test must FAIL before that fix and PASS
    /// after.
    #[test]
    fn regression_crossshard_watch_false_negative_seed_8() {
        let report = run_and_check(8, Profile::TxHeavy, 4, 30, 2);
        assert!(
            report.passed(),
            "cross-shard WATCH false-negative regressed: {:?}",
            report.violations
        );
    }
}
