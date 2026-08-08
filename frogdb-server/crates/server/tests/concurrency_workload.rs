//! Generated-workload seed sweep: drive seeded workloads against the real
//! server under turmoil, run the invariant pipeline, and emit a repro file on
//! any failure. Bug workflow: repro file → pinned regression test below
//! (design docs retired; see `.scratch/concurrency-testing/` and git history).
//!
//! Turmoil-only: gated at the `mod concurrency_workload;` declaration in
//! `tests/main.rs` (`#[cfg(feature = "turmoil")]`), so no inner `#![cfg]` here
//! (that would trip `clippy::duplicated_attributes`).

use std::path::PathBuf;

use frogdb_testing::{Profile, Workload};

use crate::common::invariants::{
    InvariantReport, MAX_OPS_PER_KEY, MAX_WGL_STATES, MAX_WGL_STATES_NIGHTLY, check_all_with,
};
use crate::common::repro::{ReproFile, read_repro, repro_path, write_repro};
use crate::common::sweep_summary::{
    SweepSummary, fail_ratio_override, fifo_ratio_override, warn_ratio_override,
};
use crate::common::workload_runner::run_workload_capturing;

/// Generate → run against the real server (fake persistence) → check invariants.
///
/// `max_states` is the WGL bounded-search budget (see `invariants::MAX_WGL_STATES` /
/// `MAX_WGL_STATES_NIGHTLY`): per-PR tiers use the smaller default budget, while the
/// nightly tier raises it to make state-bound downgrades rarer (issue 41).
fn run_and_check(
    seed: u64,
    profile: Profile,
    num_clients: usize,
    ops_per_client: usize,
    num_shards: usize,
    max_states: u64,
) -> InvariantReport {
    let workload = Workload::generate(seed, profile, num_clients, ops_per_client);
    let run = run_workload_capturing(&workload, num_shards, true);
    let report = check_all_with(
        &run.history,
        &run.final_elements,
        Some(&run.quiescence),
        Some(&run.registration_order),
        num_shards,
        MAX_OPS_PER_KEY,
        max_states,
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
    let path = repro_path(seed, profile, ops_per_client);
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
    let mut summary = SweepSummary::default();
    for seed in 0..20u64 {
        let profile = if seed % 2 == 0 {
            Profile::Mixed
        } else {
            Profile::BlockingHeavy
        };
        let report = run_and_check(seed, profile, 4, 30, 2, MAX_WGL_STATES);
        summary.record(&report);
        if !report.passed() {
            let path = write_repro_for(seed, profile, 4, 30, 2);
            panic!(
                "seed {seed} ({profile:?}) violated invariants: {:?}\nrepro: {}",
                report.violations,
                path.display()
            );
        }
    }
    // Surface the WGL downgrade ratio in the sweep's own output (issue 41):
    // previously this was only visible per-key via an `eprintln!` buried in
    // `invariants.rs`, so a sweep where every key downgraded to
    // conservation-only checking still reported a silent clean pass.
    eprintln!("{}", summary.report_line("seed_sweep_short_workloads"));
    summary.warn_if_over("seed_sweep_short_workloads", warn_ratio_override());
    eprintln!("{}", summary.fifo_report_line("seed_sweep_short_workloads"));
}

// The tier-4 quiescence stage: run one small workload and assert the DEBUG
// introspection probes ran (LOCKTABLE / WAITQUEUE / MEMORY-CHECK /
// EXPIRY-INDEX-CHECK), and that a drained, quiesced server reports no
// quiescence violation (empty lock table + wait queue, consistent memory and
// expiry index). This is the live-wiring smoke test for the probe→snapshot
// adapter; the full sweep above exercises it every seed.
#[test]
fn quiescence_stage_runs_and_is_clean() {
    let report = run_and_check(0, Profile::Mixed, 2, 8, 2, MAX_WGL_STATES);
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
// producer serves them. The post-drain `DEBUG WAITQUEUE-LOG` journal correlates
// each registration's ordinal to its client via the CLIENT ID map, feeding the
// exact FIFO wake-order checker. There is no fallback proxy: an empty or
// ambiguous journal means "nothing judged", so the checker cannot be silently
// disabled without the coverage assertions below noticing.
//
// Three assertions guard the checker: (1) the journal must be whole and the
// CLIENT ID join must have produced ordinals — a broken join (mismatched id
// space, or a journal key that does not round-trip to the served key) would
// empty the map; (2) coverage must reach the comparison stage, i.e. at least
// one key had two judged waiters to order against each other, so the test
// fails if capture ever regresses to the near-zero coverage of the old polling
// prober (issue 16); (3) a correct, FIFO-fair server serves in registration
// order, so `check_all` must pass. If it fails on served order, triage
// harness-vs-server per the bug workflow before pinning a regression.
#[test]
fn multi_waiter_exact_fifo_is_clean() {
    let workload = Workload::generate(0, Profile::MultiWaiter, 4, 12);
    let run = run_workload_capturing(&workload, 2, true);
    assert!(
        run.registration_order.is_complete(),
        "wait-queue registration journal was truncated — the exact FIFO checker \
         refuses to judge a truncated journal"
    );
    assert!(
        !run.registration_order.is_empty(),
        "journal + CLIENT ID join produced no registration ordinals — the exact \
         FIFO checker has nothing to judge (join mismatch or key-encoding drift)"
    );
    let report = check_all_with(
        &run.history,
        &run.final_elements,
        Some(&run.quiescence),
        Some(&run.registration_order),
        2,
        MAX_OPS_PER_KEY,
        MAX_WGL_STATES,
    );
    assert!(
        report.passed(),
        "multi-waiter workload violated invariants: {:?}",
        report.violations
    );
    // Not every served pop is judgeable: a pop that finds data already present
    // never enters the wait queue, so it has no ordinal by construction. The
    // honest coverage target is therefore *journaled registrations*, i.e. the
    // pops that provably parked — every one of those must be attributed to an
    // operation. On top of that, real ordering comparisons must have happened.
    // This seed measures 5 served pops / 4 registrations / 4 attributed / 3
    // pairs compared; the polling prober it replaced typically captured nothing
    // at all.
    let cov = report.fifo_coverage;
    assert_eq!(
        cov.attributed, cov.registrations,
        "exact FIFO checker left journaled registrations unattributed \
         (coverage {cov:?}) — the (key, client) join has drifted"
    );
    assert!(
        cov.registrations >= 1 && cov.pairs_compared >= 1,
        "exact FIFO checker compared no waiter pairs (coverage {cov:?}) — \
         registration capture regressed to the useless sampling of issue 16"
    );
}

// High-contention exact-FIFO smoke test: the same guarantees as
// `multi_waiter_exact_fifo_is_clean`, plus the one it deliberately does not
// make — that most *served* blocking pops are actually judged.
//
// `MultiWaiter` and `BlockingHeavy` judge only the pops that parked, and most
// of their pops find data already present (BlockingHeavy: ~7% of served pops
// judged at nightly length). `Profile::HighContention` is built so that
// essentially every pop parks — one hot key, consumers outnumbering producers,
// pushes under-supplied — so a judged fraction far below 1.0 here means pops
// stopped parking (or the (key, client) join drifted), not that the workload
// happens to be easy.
#[test]
fn high_contention_exact_fifo_judges_most_served_pops() {
    let workload = Workload::generate(0, Profile::HighContention, 4, 20);
    let run = run_workload_capturing(&workload, 2, true);
    assert!(
        run.registration_order.is_complete(),
        "wait-queue registration journal was truncated — the exact FIFO checker \
         refuses to judge a truncated journal"
    );
    let report = check_all_with(
        &run.history,
        &run.final_elements,
        Some(&run.quiescence),
        Some(&run.registration_order),
        2,
        MAX_OPS_PER_KEY,
        MAX_WGL_STATES,
    );
    assert!(
        report.passed(),
        "high-contention workload violated invariants: {:?}",
        report.violations
    );
    let cov = report.fifo_coverage;
    assert_eq!(
        cov.attributed, cov.registrations,
        "exact FIFO checker left journaled registrations unattributed \
         (coverage {cov:?}) — the (key, client) join has drifted"
    );
    assert_eq!(
        cov.unattributed_missing_pops, 0,
        "the shards journaled parks with no matching blocking pop (coverage \
         {cov:?}) — a wake-accounting or capture-join defect, not coverage loss"
    );
    assert!(
        cov.pairs_compared >= 2,
        "exact FIFO checker compared fewer than two waiter pairs (coverage \
         {cov:?}) — the profile stopped stacking waiters on the hot key"
    );
    // The point of the profile. Kept well below the measured value so ordinary
    // scheduling jitter cannot flake it, but far above the ~7% the pre-existing
    // blocking profiles reach.
    assert!(
        cov.judged_pops * 2 >= cov.served_pops,
        "only {}/{} served blocking pops were judged (coverage {cov:?}) — pops \
         stopped parking, so this profile no longer buys the FIFO coverage it \
         exists for",
        cov.judged_pops,
        cov.served_pops
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
    let mut summary = SweepSummary::default();
    for seed in 0..20u64 {
        let report = run_and_check(seed, Profile::TxHeavy, 4, 30, 2, MAX_WGL_STATES);
        summary.record(&report);
        if !report.passed() {
            let path = write_repro_for(seed, Profile::TxHeavy, 4, 30, 2);
            panic!(
                "seed {seed} (TxHeavy) violated invariants: {:?}\nrepro: {}",
                report.violations,
                path.display()
            );
        }
    }
    eprintln!("{}", summary.report_line("seed_sweep_txheavy"));
    summary.warn_if_over("seed_sweep_txheavy", warn_ratio_override());
    eprintln!("{}", summary.fifo_report_line("seed_sweep_txheavy"));
}

// Nightly generated-workload seed sweep (CI nightly tier — see
// `.github/workflows/workflow_gen/src/workflow_gen/workflows/concurrency_nightly.py`):
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
    // Back to the original 150 (held at 75 while the workload runner's final-state readback
    // raced long client scripts and reported phantom "exactly-once delivery" loss above ~90
    // ops — see .scratch/concurrency-testing/issues/11, Finding A;
    // fixed by latching the readback to client completion, pinned by
    // `regressions::regression_drain_capture_race_multiwaiter_ops_110_seed_0`).
    let ops_per_client = env_override("FROGDB_CONCURRENCY_OPS_PER_CLIENT", 150usize);
    let num_clients = env_override("FROGDB_CONCURRENCY_CLIENTS", 4usize);
    let num_shards = env_override("FROGDB_CONCURRENCY_SHARDS", 2usize);
    // Nightly can afford a far larger WGL state-search budget than a per-PR
    // sweep, so the default is raised from MAX_WGL_STATES to
    // MAX_WGL_STATES_NIGHTLY (issue 41: a bigger budget makes state-bound
    // downgrades to conservation-only checking rarer). Still overridable for
    // ad hoc tuning.
    let max_states = env_override("FROGDB_CONCURRENCY_MAX_STATES", MAX_WGL_STATES_NIGHTLY);

    let profiles = [
        Profile::Mixed,
        Profile::BlockingHeavy,
        Profile::TxHeavy,
        Profile::MultiWaiter,
        // Added for the FIFO *judged* fraction: the other blocking profiles
        // judge only the minority of pops that park (~7% of served pops at this
        // length), so wake order was being checked on a thin slice of what the
        // sweep actually ran. See `Profile::HighContention`.
        Profile::HighContention,
    ];

    let mut failures = Vec::new();
    let mut summary = SweepSummary::default();
    for profile in profiles {
        // Per-profile aggregate as well as the pooled one: pooled coverage hides
        // *which* profile stopped contributing, and the profiles differ by an order
        // of magnitude in how often their pops park.
        let mut profile_summary = SweepSummary::default();
        for seed in 0..seeds_per_profile {
            let report = run_and_check(
                seed,
                profile,
                num_clients,
                ops_per_client,
                num_shards,
                max_states,
            );
            profile_summary.record(&report);
            summary.record(&report);
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
        eprintln!(
            "{}",
            profile_summary.fifo_report_line(&format!("seed_sweep_nightly {profile:?}"))
        );
    }

    // Surface + threshold the WGL downgrade ratio at the sweep-summary level
    // (issue 41): the nightly tier is where this must be a hard CI-failing
    // signal, since it's the tier without a human reviewing every run — a
    // sweep where every key downgraded to conservation-only checking must not
    // be allowed to report a silent, technically-clean pass.
    eprintln!("{}", summary.report_line("seed_sweep_nightly"));
    summary.warn_if_over("seed_sweep_nightly", warn_ratio_override());
    let downgrade_threshold_result =
        summary.check_threshold("seed_sweep_nightly", fail_ratio_override());

    // Same treatment for exact-FIFO coverage (issue 16): the checker has no
    // fallback, so a capture regression turns every FIFO verdict into "no
    // verdict" — which without this threshold reads as a clean pass.
    eprintln!("{}", summary.fifo_report_line("seed_sweep_nightly"));
    let fifo_coverage_result =
        summary.check_fifo_coverage("seed_sweep_nightly", fifo_ratio_override());

    assert!(
        failures.is_empty(),
        "{} of {} seed(s) violated invariants: {:#?}",
        failures.len(),
        seeds_per_profile * profiles.len() as u64,
        failures
    );
    if let Err(e) = downgrade_threshold_result {
        panic!("{e}");
    }
    if let Err(e) = fifo_coverage_result {
        panic!("{e}");
    }
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
        MAX_WGL_STATES,
    );
    assert!(
        report.passed(),
        "repro {} still fails: {:?}",
        path,
        report.violations
    );
}

/// The harness's core contract: a run is a pure function of
/// `(seed, profile, num_clients, ops_per_client, num_shards)`.
///
/// Everything else in this file checks that a run *satisfies its invariants*; these tests
/// check that a run *is the same run twice*. Without that, a seed sweep's verdict is a
/// sample rather than a fact: a "failing seed" cannot be replayed, a passing seed proves
/// nothing about the next execution of the same seed, and a bisection over checker output
/// is measuring scheduling noise. See
/// `.scratch/concurrency-testing/issues/open/14-workload-harness-not-reproducible.md` and
/// the determinism audit it links.
///
/// Both runs happen in one process, back to back, so the only thing that can differ is
/// state the harness itself carries across runs (process-global counters, unseeded RNG,
/// hash-map iteration order, real-clock readings taken under a paused virtual clock).
mod determinism {
    use super::*;
    use crate::common::run_digest::{assert_digests_equal, digest_fingerprint, run_digest};
    use crate::common::workload_runner::CapturedRun;

    /// Generate and run the same configuration twice, then require byte-identical digests.
    /// Returns the first run so a caller can additionally assert on what it exercised.
    ///
    /// The workload is regenerated (not cloned) for the second run so workload *generation*
    /// is covered too, not only execution.
    fn assert_run_is_reproducible(
        seed: u64,
        profile: Profile,
        num_clients: usize,
        ops_per_client: usize,
        num_shards: usize,
    ) -> CapturedRun {
        let label = format!("seed {seed} ({profile:?}, {ops_per_client} ops/client)");

        let first = Workload::generate(seed, profile, num_clients, ops_per_client);
        let run_a = run_workload_capturing(&first, num_shards, true);
        let digest_a = run_digest(&run_a);

        let second = Workload::generate(seed, profile, num_clients, ops_per_client);
        let digest_b = run_digest(&run_workload_capturing(&second, num_shards, true));

        // Guard against a vacuous pass: two empty digests are trivially equal, which would
        // let a harness that silently recorded nothing satisfy this test forever.
        assert!(
            digest_a.len() > ops_per_client,
            "{label}: digest has only {} lines — the run recorded almost nothing, so \
             comparing it proves nothing",
            digest_a.len()
        );

        assert_digests_equal(&label, &digest_a, &digest_b);
        run_a
    }

    // These were `#[ignore]`d on arrival, per the audit's R0 note: the assertion has to exist
    // before the remediation steps have a pass/fail signal, but a red test in `just concurrency`
    // would block every unrelated change until the last step lands. Each is un-ignored by the
    // step that makes it pass.
    //
    // The two profiles that generate streams were ignored for audit item A15: `XADD *` minted
    // its ID from `SystemTime::now()`, which nothing in the codebase virtualized, so every
    // stream entry carried a real-time millisecond into the reply. `crate::clock::system_now()`
    // (frogdb-types) now closes that gap — see
    // `.scratch/concurrency-testing/issues/done/17-virtual-wall-clock-for-stream-ids.md`.

    /// Mixed: the broadest command vocabulary (all six type families), including the
    /// TTL-bearing commands whose expiry decisions read the clock.
    #[test]
    fn run_is_reproducible_mixed_seed_0() {
        assert_run_is_reproducible(0, Profile::Mixed, 4, 30, 2);
    }

    /// MultiWaiter: concurrent blocking pops on shared keys. The wake order is decided by
    /// waiter registration and timeout expiry, i.e. entirely by the clock and the shard
    /// event loop's scheduling — the configuration whose invariant verdict was observed to
    /// flap run to run. Generates no streams, so it is free of the wall-clock ID problem
    /// and holds the line for R3 (blocking deadlines) and R4 (expiry clock).
    #[test]
    fn run_is_reproducible_multiwaiter_seed_10() {
        assert_run_is_reproducible(10, Profile::MultiWaiter, 4, 30, 2);
    }

    /// TxHeavy at a longer script length: WATCH/EXEC version checks plus enough sim time
    /// for TTLs to actually elapse, so the active-expiry cycle participates in the run.
    #[test]
    fn run_is_reproducible_txheavy_seed_3() {
        assert_run_is_reproducible(3, Profile::TxHeavy, 4, 60, 2);
    }

    /// BlockingHeavy: the profile the other three pins left uncovered. Its blocking pops
    /// carry *short* finite timeouts, so which waiters get served and which expire is
    /// decided by the deadline arithmetic rather than by a producer that always arrives —
    /// the same clock-dependence as MultiWaiter, sampled at the opposite end of the
    /// timeout range, plus the BLMOVE path no other profile generates.
    ///
    /// The registration assertion is the anti-vacuity guard that matters here: a run in
    /// which nothing ever parked would still be trivially reproducible while covering none
    /// of the blocking machinery this pin exists for.
    #[test]
    fn run_is_reproducible_blockingheavy_seed_1() {
        let run = assert_run_is_reproducible(1, Profile::BlockingHeavy, 4, 60, 2);
        assert!(
            run.registration_order.list_pop_registrations() > 0,
            "seed 1 (BlockingHeavy) parked no waiter at all, so this pin covers none of \
             the blocking-path nondeterminism it exists for"
        );
    }

    /// The configuration the cross-process pin runs on both sides of the process boundary.
    fn cross_process_config() -> (u64, Profile, usize, usize, usize) {
        (0, Profile::Mixed, 4, 30, 2)
    }

    /// Prefix the child prints its fingerprint under.
    const FINGERPRINT_PREFIX: &str = "RUN_DIGEST_FINGERPRINT ";

    /// Libtest filter selecting [`cross_process_digest_child`] in the child binary. A
    /// substring, not `--exact`: the exact name carries the integration binary's own module
    /// prefix (`concurrency_workload::…`), which is easy to get wrong and fails as silently
    /// as "no such test".
    const CHILD_TEST: &str = "determinism::cross_process_digest_child";

    /// Runs [`cross_process_config`] and prints its digest fingerprint. Ignored so it only
    /// executes when [`run_digest_is_stable_across_processes`] spawns it by exact name.
    #[test]
    #[ignore = "child half of run_is_reproducible_across_processes; spawned by that test"]
    fn cross_process_digest_child() {
        let (seed, profile, clients, ops, shards) = cross_process_config();
        let workload = Workload::generate(seed, profile, clients, ops);
        let digest = run_digest(&run_workload_capturing(&workload, shards, true));
        println!("{FINGERPRINT_PREFIX}{}", digest_fingerprint(&digest));
    }

    /// Same seed ⇒ same digest in a *different process*.
    ///
    /// The three pins above run a configuration twice inside one process, which cannot see
    /// the drift that is per-process by construction: `RandomState`'s per-process hash seed
    /// leaking into an iteration order, ASLR-dependent pointer ordering, an environment- or
    /// PID-derived value reaching a reply. This spawns the test binary again and compares
    /// fingerprints across the boundary.
    ///
    /// A committed golden constant would be the cheaper assertion and was rejected: CI runs
    /// `ubuntu-latest` (x86_64 Linux) while this is developed on aarch64 macOS, so a
    /// constant recorded here cannot be validated for the platform that would enforce it,
    /// and the first CI run would either fail for a reason unrelated to determinism or
    /// force the constant to be re-recorded from CI output — pinning whatever CI happened
    /// to produce rather than anything anyone verified. Self-spawning asserts the same
    /// property (identical inputs, different process ⇒ identical run) on whatever platform
    /// it runs, at the cost of one extra run of a 30-op configuration.
    #[test]
    fn run_is_reproducible_across_processes() {
        let (seed, profile, clients, ops, shards) = cross_process_config();
        let workload = Workload::generate(seed, profile, clients, ops);
        let local = digest_fingerprint(&run_digest(&run_workload_capturing(
            &workload, shards, true,
        )));

        let exe = std::env::current_exe().expect("path to this test binary");
        let output = std::process::Command::new(&exe)
            .args(["--ignored", "--nocapture", CHILD_TEST])
            .output()
            .unwrap_or_else(|e| panic!("failed to spawn {} as a child: {e}", exe.display()));
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "child run of {CHILD_TEST} failed ({}):\n--- stdout ---\n{stdout}\n--- stderr ---\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        let child = stdout
            .lines()
            .find_map(|line| line.strip_prefix(FINGERPRINT_PREFIX))
            .map(str::trim)
            .unwrap_or_else(|| {
                panic!(
                    "child printed no {FINGERPRINT_PREFIX}line — it was filtered out rather \
                     than run:\n{stdout}"
                )
            });

        assert_eq!(
            child, local,
            "seed {seed} ({profile:?}) produced a different run in a second process \
             (this process {local}, child {child}): something in the run depends on \
             per-process state, not on the seed"
        );
    }
}

/// Pinned regressions: one named test per confirmed bug, carrying its
/// hardcoded failing seed so it can never silently regress.
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
        let report = run_and_check(0, Profile::Mixed, 4, 30, 2, MAX_WGL_STATES);
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
        let report = run_and_check(8, Profile::TxHeavy, 4, 30, 2, MAX_WGL_STATES);
        assert!(
            report.passed(),
            "cross-shard WATCH false-negative regressed: {:?}",
            report.violations
        );
    }

    /// FIXED HARNESS BUG (live pin): the final-state readback used to race a
    /// still-running workload. `workload_runner`'s drainer slept a fixed 30
    /// sim-seconds from sim start and then LRANGE'd every key as "final state";
    /// a `MultiWaiter` client spends 120–400 sim-ms of think time before each
    /// producer push, so once `ops_per_client` passed ~90 the scripts ran past
    /// 30s and the capture happened *mid-workload*. Every element pushed after
    /// the readback was then neither delivered nor in final state, which
    /// `check_exactly_once_delivery` correctly reported as a lost element —
    /// the entirety of `.scratch/concurrency-testing/issues/11`
    /// Finding A, which reproduced on nearly every seed above the threshold and
    /// held the nightly `ops_per_client` cap at 75.
    ///
    /// Fix: the drainer keys off a client-completion latch instead of a
    /// wall-clock deadline, so the capture is always taken after the last
    /// client script finished.
    ///
    /// This seed/config is one of the reproducers from the original bisection.
    /// It must FAIL before that fix and PASS after; `ops_per_client = 110` is
    /// load-bearing — at the per-PR tier's 30 the scripts finish well inside
    /// the old 30s window and the bug is invisible.
    ///
    /// The FIFO/exactly-once *product* properties this ultimately protects are
    /// specced in `.scratch/hardening/specs/blocking-failure-modes.md`; this
    /// test is not cited there because it is turmoil-gated (see that spec's
    /// scope note).
    #[test]
    fn regression_drain_capture_race_multiwaiter_ops_110_seed_0() {
        let report = run_and_check(0, Profile::MultiWaiter, 4, 110, 2, MAX_WGL_STATES);
        assert!(
            report.passed(),
            "final-state capture raced the workload again: {:?}",
            report.violations
        );
    }
}
