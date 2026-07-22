//! S2 — WATCH vs expiry vs unrelated write (characterization). WATCH is a
//! per-shard version (worker.rs:459-469): any same-shard write or active-expiry
//! removal bumps the version and aborts EXEC. Invariant: zero false negatives
//! (a genuine change to the watched key MUST abort). Over-aborts are legal and
//! characterized. F3 (lazy-expiry false negative on the watcher's own
//! EXEC-time purge) is closed and pinned (`s2_f3_lazy_expiry_watched_key_aborts`).
//! Lazy-expiry parity closes two further gaps at this seam: gap 3 — a THIRD
//! party's lazy value read of a watched key now bumps the shard version via
//! `apply_lazy_purge_effects`, so the watcher's EXEC aborts
//! (`regression_gap3_third_party_lazy_read_aborts_watch`) — and gap 4 — a
//! second watcher who watched the key while live still aborts on the first
//! watcher's no-bump WATCH-time purge, via the per-key `live_at_watch` clause
//! in `check_watches` (`regression_gap4_second_watcher_aborts`), which a coarse
//! per-shard bump alone cannot express.

use std::time::Duration;

use bytes::Bytes;

use super::harness::{ShardDriver, cmd};
use frogdb_core::shard::WatchEntry;
use frogdb_core::shard::types::TransactionResult;
use frogdb_core::store::Store;

/// What happens in the WATCH→EXEC gap.
#[derive(Debug, Clone, Copy)]
enum Interleave {
    None,
    /// A third connection READS the watched key while it is live. A read must
    /// not bump the version or purge, so EXEC must commit — the arm that pins
    /// "a read never aborts a watch" (catches a bump-version-on-read regression).
    ReadWatchedKey,
    UnrelatedWrite,
    WatchedWrite,
    ActiveExpiryUnrelated,
    ActiveExpiryWatched,
}

/// The pinned outcome for a fixed schedule, with the abort *attributed* to its
/// cause. Pinning the cause (not just the abort bit) is what lets a spurious
/// abort fail loudly instead of folding into a `>= 1` floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// EXEC commits: the watched key was untouched AND the shard version never
    /// bumped, so there is nothing that could legitimately abort.
    Commit,
    /// EXEC aborts because the watched key itself genuinely changed (a write to
    /// `k`, or `k`'s expiry). A required abort — the zero-false-negatives floor.
    TrueAbort,
    /// EXEC aborts *only* because the coarse per-shard version bumped from a
    /// write/expiry of a DIFFERENT key on the same shard. Legal but
    /// characterized: a per-key version scheme would let this arm commit, so a
    /// partial coarseness regression flips it to `Commit` and fails the pin.
    OverAbort,
}

/// Everything an arm's EXEC observed, enough to attribute the abort.
struct CaseObservation {
    aborted: bool,
    /// Model truth: did the watched key's value/existence change during the gap?
    watched_changed: bool,
    /// Did the shard version the watch is validated against differ from the
    /// snapshot `v0` (read non-destructively just before EXEC)? This is the
    /// exact signal that drives a coarse over-abort.
    version_bumped: bool,
}

async fn run_case(interleave: Interleave) -> CaseObservation {
    let mut d = ShardDriver::new(1);

    // Seed the watched key and an unrelated key.
    let _ = d.execute(0, "SET", &["k", "v0"]).await;
    let _ = d.execute(0, "SET", &["u", "u0"]).await;

    // Conn A reads the pre-gap version and watches k at it.
    let v0 = d.get_version(0).await;

    let mut watched_changed = false;
    match interleave {
        Interleave::None => {}
        Interleave::ReadWatchedKey => {
            // A pure read of the still-live watched key: must not bump/purge.
            let _ = d.execute_conn(0, 2, "GET", &["k"]).await;
        }
        Interleave::UnrelatedWrite => {
            let _ = d.execute_conn(0, 2, "SET", &["u", "u1"]).await;
        }
        Interleave::WatchedWrite => {
            let _ = d.execute_conn(0, 2, "SET", &["k", "v1"]).await;
            watched_changed = true;
        }
        Interleave::ActiveExpiryUnrelated => {
            // Seed an unrelated key with an already-elapsed TTL, then sweep.
            let _ = d.execute(0, "SET", &["e", "e0"]).await;
            let _ = d.execute(0, "PEXPIRE", &["e", "1"]).await;
            tokio::time::sleep(Duration::from_millis(3)).await;
            d.tick_expiry(0).await;
        }
        Interleave::ActiveExpiryWatched => {
            let _ = d.execute(0, "PEXPIRE", &["k", "1"]).await;
            tokio::time::sleep(Duration::from_millis(3)).await;
            d.tick_expiry(0).await;
            watched_changed = true;
        }
    }

    // The version EXEC will validate the watch against, read non-destructively
    // (empty-key GetVersion => no lazy purge) just before EXEC. `!= v0` is
    // exactly the coarse-bump signal `check_watches` compares on.
    let version_bumped = d.get_version(0).await != v0;

    // Conn A runs EXEC watching k at v0.
    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["k", "x"])],
            vec![WatchEntry {
                key: Bytes::from_static(b"k"),
                version: v0,
                live_at_watch: true,
            }],
        )
        .await;
    let aborted = matches!(result, TransactionResult::WatchAborted);
    CaseObservation {
        aborted,
        watched_changed,
        version_bumped,
    }
}

/// Characterize the S2 WATCH model with an *attributed* partition rather than a
/// coarse `over_aborts >= 1` floor. For each fixed schedule we pin the exact
/// outcome AND its cause, and assert the whole partition (N true + M over,
/// exact). Two properties fall out that the floor could not catch:
///   * a partial coarseness regression (an over-abort arm starts committing)
///     shifts the exact `over_aborts` count and fails;
///   * a spurious abort — one attributable to neither a real watched-key change
///     nor a coarse version bump — fails loudly at its arm instead of silently
///     inflating the count (the bug the floor masked).
#[tokio::test]
async fn s2_zero_false_negatives_and_over_abort_characterized() {
    let schedule = [
        (Interleave::None, Outcome::Commit),
        (Interleave::ReadWatchedKey, Outcome::Commit),
        (Interleave::UnrelatedWrite, Outcome::OverAbort),
        (Interleave::WatchedWrite, Outcome::TrueAbort),
        (Interleave::ActiveExpiryUnrelated, Outcome::OverAbort),
        (Interleave::ActiveExpiryWatched, Outcome::TrueAbort),
    ];

    let mut true_aborts = 0u32;
    let mut over_aborts = 0u32;

    for (interleave, expected) in schedule {
        let obs = run_case(interleave).await;

        // (1) The abort decision matches the pinned expectation exactly. This is
        // the zero-false-negatives floor AND its converse: a Commit arm that
        // aborts (spurious) fails here too.
        let should_abort = expected != Outcome::Commit;
        assert_eq!(
            obs.aborted, should_abort,
            "{interleave:?}: pinned {expected:?} (abort={should_abort}), \
             got aborted={} (changed={}, version_bumped={})",
            obs.aborted, obs.watched_changed, obs.version_bumped
        );

        // (2) Attribute the outcome to its cause — the abort bit alone is not
        // enough; the *reason* must match the pin.
        match expected {
            Outcome::Commit => {
                assert!(
                    !obs.watched_changed,
                    "{interleave:?}: Commit arm must not change the watched key"
                );
                assert!(
                    !obs.version_bumped,
                    "{interleave:?}: Commit arm must not bump the shard version \
                     (a read/no-op must be version-neutral)"
                );
            }
            Outcome::TrueAbort => {
                assert!(
                    obs.watched_changed,
                    "{interleave:?}: TrueAbort must be caused by a real change to \
                     the watched key, not coarseness"
                );
                true_aborts += 1;
            }
            Outcome::OverAbort => {
                // The precise coarseness signature: the watched key was NOT
                // touched, yet an unrelated same-shard write/expiry bumped the
                // per-shard version and forced the abort.
                assert!(
                    !obs.watched_changed,
                    "{interleave:?}: OverAbort must not touch the watched key \
                     (else it is a true abort, not coarseness)"
                );
                assert!(
                    obs.version_bumped,
                    "{interleave:?}: OverAbort must be attributable to a coarse \
                     per-shard version bump from another key"
                );
                over_aborts += 1;
            }
        }

        // (3) Attribution completeness: any abort must be explained by EITHER a
        // real watched-key change OR a coarse version bump. An abort with
        // neither is spurious and fails here — the case the old floor folded
        // into its count.
        if obs.aborted {
            assert!(
                obs.watched_changed || obs.version_bumped,
                "{interleave:?}: spurious abort — EXEC aborted with no watched-key \
                 change and no shard-version bump"
            );
        }
    }

    // (4) Pin the exact partition for this fixed schedule: exactly two required
    // (true) aborts and exactly two coarse over-aborts. A partial coarseness
    // regression (an over-abort arm that starts committing) or an extra spurious
    // abort shifts one of these exact counts and fails.
    assert_eq!(
        true_aborts, 2,
        "expected exactly two required (true) aborts: WatchedWrite + ActiveExpiryWatched"
    );
    assert_eq!(
        over_aborts, 2,
        "expected exactly two coarse over-aborts: UnrelatedWrite + ActiveExpiryUnrelated"
    );
}

/// F3: a watched key whose TTL elapses with no active sweep and no touch until
/// EXEC. Lazy expiry removed it (observed by the EXEC's own watch validation),
/// so the watch MUST abort — Redis/Valkey/Dragonfly all abort here (redis PR
/// #7920 / issue #7918: an expired watched key counts as modified at EXEC).
///
/// Ordering (adjudicated): PEXPIRE runs BEFORE the watch snapshot because
/// PEXPIRE on a live key is itself a write that bumps the shard version. If we
/// snapshotted `v0` before PEXPIRE, EXEC watching `(k, v0)` would abort on the
/// PEXPIRE bump alone — masking the lazy-expiry seam and passing even without
/// the fix (wrong seam). So we SET -> PEXPIRE -> snapshot v0 (post-PEXPIRE) ->
/// let it expire lazily -> EXEC. The ONLY thing that can invalidate the watch
/// in the window is the lazy-expiry purge under test, mirroring the Task-8
/// turmoil ordering.
#[tokio::test]
async fn s2_f3_lazy_expiry_watched_key_aborts() {
    let mut d = ShardDriver::new(1);
    let _ = d.execute(0, "SET", &["k", "v0"]).await;

    // Set a 1ms TTL FIRST (this write bumps the version), then snapshot the
    // post-PEXPIRE version as the watch baseline. Wait past the TTL. Do NOT
    // tick active expiry and do NOT touch k — the key is only observed
    // lazily-expired at EXEC time.
    let _ = d.execute(0, "PEXPIRE", &["k", "1"]).await;
    let v0 = d.get_version(0).await;
    tokio::time::sleep(Duration::from_millis(3)).await;

    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["k", "x"])],
            vec![WatchEntry {
                key: Bytes::from_static(b"k"),
                version: v0,
                live_at_watch: true,
            }],
        )
        .await;

    assert!(
        matches!(result, TransactionResult::WatchAborted),
        "F3: an expired watched key touched only at EXEC must abort the transaction, got {result:?}"
    );
}

/// Regression pin (gap 3): a watched key lazily purged by a THIRD party's value
/// read bumps the shard version, so the watcher's EXEC aborts — previously the
/// read-path lazy purge was version-ignorant (under-abort). This is the S2 lazy
/// arm the proposal calls for.
///
/// Ordering mirrors F3: SET -> PEXPIRE -> snapshot v0 (post-PEXPIRE, watching the
/// still-live key) -> elapse the TTL. The distinguishing move is that a THIRD
/// connection's `GET k` physically purges the expired key BEFORE EXEC runs. That
/// lazy purge now reports to the worker (`apply_lazy_purge_effects`) and bumps
/// the version, so EXEC's watch no longer matches its snapshot and aborts —
/// EXEC's own `purge_expired_watches` finds nothing to purge, so the abort comes
/// solely from the third-party read's bump. Redis/Valkey/Dragonfly abort here
/// (`expireIfNeeded` -> `keyModified` -> `touchWatchedKey`, redis PR #7920).
#[tokio::test]
async fn regression_gap3_third_party_lazy_read_aborts_watch() {
    let mut d = ShardDriver::new(1);
    let _ = d.execute(0, "SET", &["k", "v0"]).await;
    // Set the TTL FIRST (this write bumps the version), then snapshot the
    // post-PEXPIRE version as the watch baseline (watching the still-live key).
    let _ = d.execute(0, "PEXPIRE", &["k", "1"]).await;
    let v0 = d.get_version(0).await;
    tokio::time::sleep(Duration::from_millis(3)).await;

    // Third conn's lazy read physically purges k. Version-ignorant before the
    // fix; now bumps the shard version at the point of removal.
    let _ = d.execute_conn(0, 2, "GET", &["k"]).await;

    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["k", "x"])],
            vec![WatchEntry {
                key: Bytes::from_static(b"k"),
                version: v0,
                live_at_watch: true,
            }],
        )
        .await;
    assert!(
        matches!(result, TransactionResult::WatchAborted),
        "gap 3: a third-party lazy read of an expired watched key must abort EXEC, got {result:?}"
    );
}

/// Regression pin (gap 4): B watches k while live; k expires; A's WATCH-time
/// no-bump purge removes k; B's EXEC must still abort (per-key `live_at_watch`),
/// even though no version bump reached B. The coarse per-shard version cannot
/// distinguish B (must abort) from A (must not) — only per-key expired-watch
/// state can. Redis records `wk->expired` at WATCH time and fires
/// `touchWatchedKey` on the lazy removal (redis PR #7920 / issue #7918).
///
/// The full two-watcher interleave (ruling 4): B snapshots version+liveness via
/// `watch_keys` while k is live (real liveness, not a hardcoded `true`); the TTL
/// elapses on the real clock; A's keyed `watch_keys` call is the purge-trigger —
/// the `GetVersion` handler runs the WATCH-time no-bump purge that physically
/// removes k (asserted via raw store access); B's EXEC then aborts through the
/// new `live_at_watch` clause, with no version bump reaching B.
#[tokio::test]
async fn regression_gap4_second_watcher_aborts() {
    let mut d = ShardDriver::new(1);
    let _ = d.execute(0, "SET", &["k", "v0"]).await;
    let _ = d.execute(0, "PEXPIRE", &["k", "1"]).await;
    // B watches k LIVE: real version + liveness snapshot via watch_keys (not a
    // hardcoded `true` — watch_keys reports the actual computed live_at_watch).
    let (vb, live) = d.watch_keys(0, &["k"]).await;
    assert_eq!(
        live,
        vec![true],
        "gap 4 setup: k must be live when B watches it"
    );
    tokio::time::sleep(Duration::from_millis(3)).await; // elapse the real-clock TTL

    // A watches k already-expired: this keyed GetVersion is A's purge-trigger —
    // it fires the WATCH-time no-bump purge (dispatch_core.rs GetVersion handler:
    // `for key in &keys { self.store.purge_if_expired(key); }`), which is
    // deliberately version-ignorant (F3). A's own liveness snapshot is false.
    let (_va, a_live) = d.watch_keys(0, &["k"]).await;
    assert_eq!(
        a_live,
        vec![false],
        "gap 4: A watches k already-expired, so its live_at_watch snapshot is false"
    );

    // Prove the purge physically fired: k must be absent from the raw store (not
    // merely logically expired) before B's EXEC runs.
    assert!(
        !d.worker(0).store.contains(b"k"),
        "gap 4: A's WATCH-time purge must have physically removed k before B's EXEC"
    );

    // B's EXEC watching k live at vb must abort — via the new live_at_watch
    // clause alone (key physically absent, version unchanged since vb).
    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["k", "x"])],
            vec![WatchEntry {
                key: Bytes::from_static(b"k"),
                version: vb,
                live_at_watch: true,
            }],
        )
        .await;
    assert!(
        matches!(result, TransactionResult::WatchAborted),
        "gap 4: second (live) watcher must abort after the first watcher's no-bump purge, got {result:?}"
    );

    // A-side non-abort (the other half of the Redis wk->expired semantics): a
    // watcher that saw k already-dead (live_at_watch == false) commits when k
    // stays gone — no version bump, no live->gone transition for A. Same shard,
    // same absent key: proves the clause is per-key, not a shard-wide broadcast.
    let a_result = d
        .exec_transaction(
            0,
            2,
            vec![cmd("SET", &["other", "y"])],
            vec![WatchEntry {
                key: Bytes::from_static(b"k"),
                version: vb,
                live_at_watch: false,
            }],
        )
        .await;
    assert!(
        matches!(a_result, TransactionResult::Success(_)),
        "gap 4: stale watcher A (watched k already-dead) must COMMIT when k stays gone, got {a_result:?}"
    );
}
