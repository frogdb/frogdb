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

/// Aggregate downgrade/coverage counters across every seed in a sweep run.
#[derive(Debug, Default, Clone, Copy)]
pub struct SweepSummary {
    /// Number of `InvariantReport`s folded in (one per seed run).
    pub seeds_run: u64,
    /// Sum of `InvariantReport::keys_checked` across every seed.
    pub keys_checked: usize,
    /// Sum of `InvariantReport::downgraded_keys.len()` across every seed.
    pub downgraded_keys: usize,
}

impl SweepSummary {
    /// Fold one seed's report into the running aggregate.
    pub fn record(&mut self, report: &InvariantReport) {
        self.seeds_run += 1;
        self.keys_checked += report.keys_checked;
        self.downgraded_keys += report.downgraded_keys.len();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn report_with(keys_checked: usize, downgraded: usize) -> InvariantReport {
        InvariantReport {
            violations: Vec::new(),
            downgraded_keys: (0..downgraded).map(|i| format!("k{i}")).collect(),
            keys_checked,
            quiescence_checked: false,
        }
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
