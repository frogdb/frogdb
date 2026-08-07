//! Sweep-level aggregation of the WGL downgrade ratio (issue 41).
//!
//! `invariants::InvariantReport` already tracks, per seed, how many
//! WGL-eligible keys got downgraded to conservation-only checking (state
//! budget hit, or over the per-key op cap) versus fully linearizability-
//! checked. That per-seed number was previously visible only via an
//! `eprintln!` inside `invariants.rs` and asserted nowhere above the unit
//! level — a sweep where *every* key downgraded still reported a clean pass,
//! having linearizability-checked nothing.
//!
//! [`SweepSummary`] accumulates that ratio across an entire sweep run (all
//! seeds of a profile, or all profiles of a tier) so it can be reported in the
//! sweep's own output, warned on loudly past a soft threshold, and hard-failed
//! past a stricter one (the nightly tier, which can also afford to raise
//! `invariants::MAX_WGL_STATES_NIGHTLY` to make the downgrade rarer in the
//! first place).

#![allow(dead_code)]

use super::invariants::InvariantReport;

/// Env var overriding the soft warn threshold (fraction, e.g. `0.1` for 10%).
pub const WARN_RATIO_ENV: &str = "FROGDB_WGL_DOWNGRADE_WARN_RATIO";
/// Env var overriding the hard fail threshold (nightly tier only).
pub const FAIL_RATIO_ENV: &str = "FROGDB_WGL_DOWNGRADE_FAIL_RATIO";

/// Default soft warn threshold: flag (but do not fail) once more than 5% of
/// WGL-eligible keys, across the whole sweep, were downgraded.
pub const DEFAULT_WARN_RATIO: f64 = 0.05;
/// Default hard fail threshold (nightly tier): fail once more than 25% of
/// WGL-eligible keys were downgraded — a run this degraded is not a
/// meaningful linearizability pass, regardless of whether any violation
/// fired.
pub const DEFAULT_FAIL_RATIO: f64 = 0.25;

/// Env var overriding the minimum share of *journaled* blocking-pop
/// registrations the exact FIFO checker must have attributed to an operation.
pub const FIFO_RATIO_ENV: &str = "FROGDB_FIFO_ATTRIBUTED_MIN_RATIO";

/// Default floor on the sweep-wide share of journaled registrations tied to a
/// specific operation.
///
/// The denominator is registrations, not served pops: a blocking pop that finds
/// data already present never enters the wait queue, so it has no ordinal *by
/// construction* — measured sweeps serve ~370 blocking pops of which only ~20
/// ever park, and judging the other 350 is not a thing any capture could do.
/// Against registrations the achievable target is 1.0, and the floor exists to
/// catch issue 16's failure mode: capture (or the `(key, client_id)` join)
/// regressing toward zero while the sweep still reports a clean pass, which is
/// how an unsound proxy went unnoticed. It sits below 1.0 only because a client
/// whose pop count on a key disagrees with its registration count there is
/// deliberately left unattributed rather than guessed at.
pub const DEFAULT_FIFO_ATTRIBUTED_MIN_RATIO: f64 = 0.9;

/// Aggregate downgrade/coverage counters across every seed in a sweep run.
#[derive(Debug, Default, Clone, Copy)]
pub struct SweepSummary {
    /// Number of `InvariantReport`s folded in (one per seed run).
    pub seeds_run: u64,
    /// Sum of `InvariantReport::keys_checked` across every seed.
    pub keys_checked: usize,
    /// Sum of `InvariantReport::downgraded_keys.len()` across every seed.
    pub downgraded_keys: usize,
    /// Blocking-pop registrations the shards journaled, sweep-wide: the pops
    /// that provably parked.
    pub fifo_registrations: usize,
    /// Of those, how many the checker tied to a specific operation.
    pub fifo_attributed: usize,
    /// Registrations left unattributed because the client's blocking pops on
    /// the key outnumbered its journaled parks there. Benign by construction —
    /// see `frogdb_testing::conservation::FifoCoverage` — and the only reason
    /// the attributed ratio can sit below 1.0.
    pub fifo_unattributed_extra_pops: usize,
    /// Registrations left unattributed because the shards journaled *more*
    /// parks than the history has blocking pops. Nothing in the blocking path
    /// can produce this, so a non-zero count is a defect, not coverage loss,
    /// and [`Self::check_fifo_coverage`] fails on it outright.
    pub fifo_unattributed_missing_pops: usize,
    /// Served blocking pops the exact FIFO checker considered, sweep-wide.
    /// Mostly pops that never parked, so this is context, not a target.
    pub fifo_served_pops: usize,
    /// Of those, how many carried an unambiguous registration ordinal.
    pub fifo_judged_pops: usize,
    /// Serve-order pairs actually compared, sweep-wide. Zero means the sweep
    /// verified nothing at all about wake ordering.
    pub fifo_pairs_compared: usize,
    /// Seeds whose registration journal was truncated (nothing judged).
    pub fifo_incomplete_runs: u64,
}

impl SweepSummary {
    /// Fold one seed's report into the running aggregate.
    pub fn record(&mut self, report: &InvariantReport) {
        self.seeds_run += 1;
        self.keys_checked += report.keys_checked;
        self.downgraded_keys += report.downgraded_keys.len();
        let cov = report.fifo_coverage;
        self.fifo_registrations += cov.registrations;
        self.fifo_attributed += cov.attributed;
        self.fifo_unattributed_extra_pops += cov.unattributed_extra_pops;
        self.fifo_unattributed_missing_pops += cov.unattributed_missing_pops;
        self.fifo_served_pops += cov.served_pops;
        self.fifo_judged_pops += cov.judged_pops;
        self.fifo_pairs_compared += cov.pairs_compared;
        if !cov.complete {
            self.fifo_incomplete_runs += 1;
        }
    }

    /// Sweep-wide share of journaled registrations the checker attributed to an
    /// operation. `1.0` (not `0.0`) when nothing ever parked — nothing was
    /// missed, so the floor must not fire on a workload that does not block.
    pub fn fifo_attributed_ratio(&self) -> f64 {
        if self.fifo_registrations == 0 {
            1.0
        } else {
            self.fifo_attributed as f64 / self.fifo_registrations as f64
        }
    }

    /// Sweep-wide share of *served* blocking pops that carried an unambiguous
    /// registration ordinal and so could be ordered. Unlike
    /// [`Self::fifo_attributed_ratio`] this is capped by how often the workload
    /// actually makes pops park, which is what the high-contention profile
    /// exists to raise. `0.0` when no blocking pop was served.
    pub fn fifo_judged_pop_ratio(&self) -> f64 {
        if self.fifo_served_pops == 0 {
            0.0
        } else {
            self.fifo_judged_pops as f64 / self.fifo_served_pops as f64
        }
    }

    /// Fraction of WGL-eligible keys, across the whole sweep, downgraded to
    /// conservation-only checking. `0.0` (never NaN) when the sweep checked
    /// no WGL-eligible keys at all.
    pub fn downgrade_ratio(&self) -> f64 {
        if self.keys_checked == 0 {
            0.0
        } else {
            self.downgraded_keys as f64 / self.keys_checked as f64
        }
    }

    /// One-line human-readable summary suitable for sweep output/CI logs —
    /// the observable surface the downgrade ratio previously lacked.
    pub fn report_line(&self, label: &str) -> String {
        format!(
            "{label}: {} seed(s), {}/{} WGL-eligible key(s) downgraded to \
             conservation-only (downgrade ratio {:.4})",
            self.seeds_run,
            self.downgraded_keys,
            self.keys_checked,
            self.downgrade_ratio()
        )
    }

    /// One-line summary of exact-FIFO coverage, the counterpart of
    /// [`Self::report_line`] for issue 16's collapse mode.
    pub fn fifo_report_line(&self, label: &str) -> String {
        format!(
            "{label}: exact FIFO attributed {}/{} journaled registration(s) \
             (ratio {:.4}), judged {} of {} served blocking pop(s) (judged ratio \
             {:.4}), {} pair(s) compared, {} run(s) with a truncated registration \
             journal, unattributed {} extra-pop / {} missing-pop",
            self.fifo_attributed,
            self.fifo_registrations,
            self.fifo_attributed_ratio(),
            self.fifo_judged_pops,
            self.fifo_served_pops,
            self.fifo_judged_pop_ratio(),
            self.fifo_pairs_compared,
            self.fifo_incomplete_runs,
            self.fifo_unattributed_extra_pops,
            self.fifo_unattributed_missing_pops,
        )
    }

    /// Hard-threshold check on exact-FIFO coverage: `Err` when the journal was
    /// truncated anywhere, when blocking pops were served but nothing parked
    /// (capture is dead), when a registration was lost in the *defect*
    /// direction (more journaled parks than blocking pops), when too few
    /// journaled registrations were attributed, or when no serve-order pair was
    /// compared at all. Any of the five means the FIFO verdict is not evidence
    /// of anything.
    pub fn check_fifo_coverage(&self, label: &str, min_ratio: f64) -> Result<(), String> {
        if self.fifo_incomplete_runs > 0 {
            return Err(format!(
                "{} — a truncated journal makes a missing ordinal stop implying \
                 \"never parked\", so the checker judged nothing for those runs",
                self.fifo_report_line(label)
            ));
        }
        if self.fifo_served_pops > 0 && self.fifo_registrations == 0 {
            return Err(format!(
                "{} — blocking pops were served but the shards journaled no \
                 registration at all: capture is dead (issue 16)",
                self.fifo_report_line(label)
            ));
        }
        if self.fifo_unattributed_missing_pops > 0 {
            return Err(format!(
                "{} — the shards journaled more parks for some (key, client) than \
                 the history has blocking pops for it. A BLPOP registers at most \
                 once and only when it parks, so this is a wake-accounting or \
                 capture-join defect, not coverage loss",
                self.fifo_report_line(label)
            ));
        }
        if self.fifo_attributed_ratio() < min_ratio {
            return Err(format!(
                "{} — below the minimum attributed ratio {min_ratio:.4}: \
                 registration capture or the (key, client) join has regressed \
                 (issue 16)",
                self.fifo_report_line(label)
            ));
        }
        if self.fifo_served_pops > 0 && self.fifo_pairs_compared == 0 {
            return Err(format!(
                "{} — blocking pops were served but no two judged waiters ever \
                 shared a key, so wake order was never actually checked",
                self.fifo_report_line(label)
            ));
        }
        Ok(())
    }

    /// Log a loud warning (stderr) when the ratio exceeds `warn_ratio`.
    /// Never fails the sweep on its own. Returns whether it warned.
    pub fn warn_if_over(&self, label: &str, warn_ratio: f64) -> bool {
        let over = self.downgrade_ratio() > warn_ratio;
        if over {
            eprintln!(
                "WARNING: {} — exceeds warn threshold {:.4}. The sweep may still \
                 pass, but a growing share of it is conservation-only, not \
                 linearizability-checked.",
                self.report_line(label),
                warn_ratio
            );
        }
        over
    }

    /// Hard-threshold check for tiers that must fail on excessive downgrade
    /// (the nightly tier): `Err` describing the breach when the ratio exceeds
    /// `fail_ratio`, `Ok(())` otherwise.
    pub fn check_threshold(&self, label: &str, fail_ratio: f64) -> Result<(), String> {
        if self.downgrade_ratio() > fail_ratio {
            Err(format!(
                "{} — exceeds fail threshold {:.4}: too many keys never got a \
                 real linearizability check for this sweep result to be trusted",
                self.report_line(label),
                fail_ratio
            ))
        } else {
            Ok(())
        }
    }
}

/// Read `key` as an env-overridable ratio, falling back to `default` when
/// unset. Panics naming the env var and offending value if it's set but
/// fails to parse — a malformed override must fail loudly, not silently fall
/// back to a default threshold different from what was asked for.
pub fn env_ratio(key: &str, default: f64) -> f64 {
    match std::env::var(key) {
        Ok(v) => parse_ratio(key, &v),
        Err(_) => default,
    }
}

fn parse_ratio(key: &str, value: &str) -> f64 {
    value
        .parse()
        .unwrap_or_else(|e| panic!("env var {key}={value:?} is not a valid ratio: {e}"))
}

/// The warn threshold, honoring [`WARN_RATIO_ENV`].
pub fn warn_ratio_override() -> f64 {
    env_ratio(WARN_RATIO_ENV, DEFAULT_WARN_RATIO)
}

/// The nightly fail threshold, honoring [`FAIL_RATIO_ENV`].
pub fn fail_ratio_override() -> f64 {
    env_ratio(FAIL_RATIO_ENV, DEFAULT_FAIL_RATIO)
}

/// The minimum exact-FIFO attributed ratio, honoring [`FIFO_RATIO_ENV`].
pub fn fifo_ratio_override() -> f64 {
    env_ratio(FIFO_RATIO_ENV, DEFAULT_FIFO_ATTRIBUTED_MIN_RATIO)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report_with(keys_checked: usize, downgraded: usize) -> InvariantReport {
        InvariantReport {
            violations: Vec::new(),
            downgraded_keys: (0..downgraded).map(|i| format!("k{i}")).collect(),
            keys_checked,
            quiescence_checked: false,
            ..InvariantReport::default()
        }
    }

    fn report_with_fifo(
        registrations: usize,
        attributed: usize,
        served: usize,
        judged: usize,
        pairs: usize,
        complete: bool,
    ) -> InvariantReport {
        InvariantReport {
            fifo_coverage: frogdb_testing::FifoCoverage {
                registrations,
                attributed,
                served_pops: served,
                judged_pops: judged,
                pairs_compared: pairs,
                complete,
                // The direction split is exercised by its own test below; the
                // benign bucket absorbs whatever these helpers left unattributed
                // so the defect check never fires incidentally.
                unattributed_extra_pops: registrations.saturating_sub(attributed),
                unattributed_missing_pops: 0,
            },
            ..InvariantReport::default()
        }
    }

    #[test]
    fn fifo_ratio_is_one_when_nothing_ever_parked() {
        // No waiter parked, so nothing was missed — the floor must not fire on
        // a workload that simply does not block.
        let summary = SweepSummary::default();
        assert_eq!(summary.fifo_attributed_ratio(), 1.0);
        assert!(summary.check_fifo_coverage("empty", 0.9).is_ok());
    }

    #[test]
    fn fifo_coverage_accumulates_and_passes_when_healthy() {
        let mut summary = SweepSummary::default();
        summary.record(&report_with_fifo(4, 4, 50, 4, 3, true));
        summary.record(&report_with_fifo(4, 4, 50, 4, 3, true));
        assert_eq!(summary.fifo_registrations, 8);
        assert_eq!(summary.fifo_attributed, 8);
        assert_eq!(summary.fifo_served_pops, 100);
        assert_eq!(summary.fifo_judged_pops, 8);
        assert_eq!(summary.fifo_pairs_compared, 6);
        assert_eq!(summary.fifo_incomplete_runs, 0);
        assert!(summary.check_fifo_coverage("healthy", 0.9).is_ok());
    }

    #[test]
    fn fifo_dead_capture_fails() {
        // Issue 16's exact failure mode: waiters were served, the journal came
        // back empty, no violation was reported — which must NOT read as a
        // clean pass.
        let mut summary = SweepSummary::default();
        summary.record(&report_with_fifo(0, 0, 20, 0, 0, true));
        let err = summary
            .check_fifo_coverage("dead", 0.9)
            .expect_err("zero capture must fail");
        assert!(err.contains("capture is dead"), "{err}");
    }

    #[test]
    fn fifo_no_pair_compared_fails_even_with_ordinals() {
        // Every parked pop was attributed, but no key ever had two of them, so
        // no ordering was actually verified.
        let mut summary = SweepSummary::default();
        summary.record(&report_with_fifo(4, 4, 4, 4, 0, true));
        assert!(summary.check_fifo_coverage("no pairs", 0.9).is_err());
    }

    #[test]
    fn fifo_truncated_journal_fails() {
        let mut summary = SweepSummary::default();
        summary.record(&report_with_fifo(10, 0, 10, 0, 0, false));
        let err = summary
            .check_fifo_coverage("truncated", 0.9)
            .expect_err("a truncated journal must fail");
        assert!(err.contains("truncated"), "{err}");
    }

    #[test]
    fn fifo_low_attribution_fails() {
        // Registrations were captured but most could not be tied to an op:
        // the join has regressed even though capture itself works.
        let mut summary = SweepSummary::default();
        summary.record(&report_with_fifo(100, 10, 100, 10, 4, true));
        let err = summary
            .check_fifo_coverage("low", 0.9)
            .expect_err("10% attribution must fail a 90% floor");
        assert!(err.contains("minimum attributed ratio"), "{err}");
    }

    #[test]
    fn fifo_missing_pop_attribution_loss_fails_even_above_the_ratio_floor() {
        // 99% attributed — comfortably over the floor — but one registration
        // was lost in the direction the blocking path cannot produce. That is a
        // defect signal and must fail rather than be absorbed as coverage loss.
        let mut summary = SweepSummary::default();
        let mut report = report_with_fifo(100, 99, 100, 99, 20, true);
        report.fifo_coverage.unattributed_extra_pops = 0;
        report.fifo_coverage.unattributed_missing_pops = 1;
        summary.record(&report);
        let err = summary
            .check_fifo_coverage("missing pops", 0.9)
            .expect_err("a park with no matching pop must fail");
        assert!(err.contains("journaled more parks"), "{err}");
    }

    #[test]
    fn fifo_judged_pop_ratio_reports_parking_density() {
        let mut summary = SweepSummary::default();
        summary.record(&report_with_fifo(4, 4, 200, 4, 2, true));
        assert_eq!(summary.fifo_judged_pop_ratio(), 0.02);
        assert_eq!(
            SweepSummary::default().fifo_judged_pop_ratio(),
            0.0,
            "no served pops must not divide by zero"
        );
    }

    #[test]
    fn fifo_served_pops_that_never_parked_do_not_drag_the_ratio() {
        // 4 registrations, all attributed, but 200 served pops that found data
        // present and never parked. The old served-pop denominator read this
        // as 2% coverage; against registrations it is a clean 100%.
        let mut summary = SweepSummary::default();
        summary.record(&report_with_fifo(4, 4, 200, 4, 2, true));
        assert_eq!(summary.fifo_attributed_ratio(), 1.0);
        assert!(summary.check_fifo_coverage("sparse parking", 0.9).is_ok());
    }

    #[test]
    fn empty_summary_has_zero_ratio() {
        let summary = SweepSummary::default();
        assert_eq!(summary.seeds_run, 0);
        assert_eq!(summary.downgrade_ratio(), 0.0);
    }

    #[test]
    fn record_accumulates_across_seeds() {
        let mut summary = SweepSummary::default();
        summary.record(&report_with(10, 1)); // seed 1: 1/10 downgraded
        summary.record(&report_with(10, 4)); // seed 2: 4/10 downgraded
        assert_eq!(summary.seeds_run, 2);
        assert_eq!(summary.keys_checked, 20);
        assert_eq!(summary.downgraded_keys, 5);
        assert!((summary.downgrade_ratio() - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn all_keys_downgraded_is_ratio_one() {
        // The exact scenario the issue calls out: every key downgraded, so a
        // "clean pass" (no violations) must still surface ratio 1.0.
        let mut summary = SweepSummary::default();
        summary.record(&report_with(5, 5));
        assert_eq!(summary.downgrade_ratio(), 1.0);
    }

    #[test]
    fn report_line_includes_seed_count_and_ratio() {
        let mut summary = SweepSummary::default();
        summary.record(&report_with(4, 1));
        let line = summary.report_line("test sweep");
        assert!(line.contains("test sweep"));
        assert!(line.contains("1 seed"));
        assert!(line.contains("1/4"));
        assert!(line.contains("0.2500"));
    }

    #[test]
    fn warn_if_over_flags_past_threshold_not_at_or_under() {
        let mut over = SweepSummary::default();
        over.record(&report_with(10, 6)); // ratio 0.6
        assert!(over.warn_if_over("sweep", 0.5));

        let mut at = SweepSummary::default();
        at.record(&report_with(10, 5)); // ratio 0.5, exactly at threshold
        assert!(
            !at.warn_if_over("sweep", 0.5),
            "exactly at threshold must not warn (strictly greater-than)"
        );

        let mut under = SweepSummary::default();
        under.record(&report_with(10, 4)); // ratio 0.4
        assert!(!under.warn_if_over("sweep", 0.5));
    }

    #[test]
    fn check_threshold_errors_past_fail_ratio() {
        let mut summary = SweepSummary::default();
        summary.record(&report_with(10, 3)); // ratio 0.3

        assert!(
            summary.check_threshold("sweep", 0.5).is_ok(),
            "under threshold must pass"
        );
        let err = summary
            .check_threshold("sweep", 0.2)
            .expect_err("over threshold must fail");
        assert!(
            err.contains("0.2000"),
            "error must cite the threshold: {err}"
        );
        assert!(err.contains("sweep"));
    }

    #[test]
    fn check_threshold_catches_all_keys_downgraded() {
        // The headline scenario from issue 41: 100% downgraded, zero
        // violations found. `passed()` on the underlying report would say
        // true, but the sweep-level threshold must still catch it.
        let mut summary = SweepSummary::default();
        summary.record(&report_with(50, 50));
        assert!(
            summary
                .check_threshold("nightly sweep", DEFAULT_FAIL_RATIO)
                .is_err()
        );
    }

    #[test]
    fn zero_keys_checked_never_trips_warn_or_fail() {
        // A sweep with no WGL-eligible keys at all (e.g. every history was
        // empty) must not be misreported as 100% downgraded.
        let mut summary = SweepSummary::default();
        summary.record(&report_with(0, 0));
        assert!(!summary.warn_if_over("sweep", 0.0));
        assert!(summary.check_threshold("sweep", 0.0).is_ok());
    }

    #[test]
    fn env_ratio_falls_back_to_default_when_unset() {
        let key = "FROGDB_WGL_DOWNGRADE_TEST_UNSET_RATIO_41";
        // SAFETY: test-only env var, scoped to this key, no other thread in
        // this test relies on its value.
        unsafe {
            std::env::remove_var(key);
        }
        assert_eq!(env_ratio(key, 0.42), 0.42);
    }

    #[test]
    fn parse_ratio_panics_on_malformed_value() {
        let result = std::panic::catch_unwind(|| parse_ratio("SOME_ENV", "not-a-number"));
        assert!(
            result.is_err(),
            "a malformed ratio override must panic loudly, not silently fall back"
        );
    }

    #[test]
    fn parse_ratio_accepts_valid_value() {
        assert_eq!(parse_ratio("SOME_ENV", "0.1"), 0.1);
    }
}
