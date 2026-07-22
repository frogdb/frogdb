//! S2 — WATCH vs expiry vs unrelated write (characterization). WATCH is a
//! per-SLOT version (proposal 18, worker.rs `get_key_version`/`SlotVersions`): a
//! write or active-expiry removal bumps only the affected key's Hash Slot
//! version and aborts EXEC only for watches on THAT slot. Invariant: zero false
//! negatives (a genuine change to the watched key MUST abort). A same-shard
//! write to a DIFFERENT slot no longer aborts (the proposal-18 fix); a same-slot
//! sibling (hash-tag colocated) still does — the residual coarseness, legal and
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
use frogdb_core::types::BlockingOp;

/// What happens in the WATCH→EXEC gap.
#[derive(Debug, Clone, Copy)]
enum Interleave {
    None,
    /// A third connection READS the watched key while it is live. A read must
    /// not bump the version or purge, so EXEC must commit — the arm that pins
    /// "a read never aborts a watch" (catches a bump-version-on-read regression).
    ReadWatchedKey,
    /// A write to an unrelated key in a DIFFERENT slot on the same shard. Under
    /// slot-granular WATCH (proposal 18) this no longer bumps `k`'s slot, so
    /// EXEC COMMITS (it over-aborted under the old coarse per-shard version).
    UnrelatedWrite,
    /// A write to a sibling key colocated on `k`'s slot via a hash tag. Still
    /// bumps `k`'s slot (slot granularity is coarser than Redis's per-key), so
    /// EXEC over-aborts — the residual coarseness the design deliberately keeps.
    SameSlotWrite,
    WatchedWrite,
    /// Active expiry of an unrelated key in a DIFFERENT slot. Whole-key expiry
    /// bumps only that key's slot via the removal pipeline, so `k`'s watch
    /// survives and EXEC COMMITS (over-aborted under the old coarse version).
    ActiveExpiryUnrelated,
    ActiveExpiryWatched,
}

/// The pinned outcome for a fixed schedule, with the abort *attributed* to its
/// cause. Pinning the cause (not just the abort bit) is what lets a spurious
/// abort fail loudly instead of folding into a `>= 1` floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// EXEC commits: the watched key was untouched AND its slot version never
    /// bumped, so there is nothing that could legitimately abort.
    Commit,
    /// EXEC aborts because the watched key itself genuinely changed (a write to
    /// `k`, or `k`'s expiry). A required abort — the zero-false-negatives floor.
    TrueAbort,
    /// EXEC aborts *only* because `k`'s slot version bumped from a write/expiry
    /// of a DIFFERENT key that shares `k`'s Hash Slot (a hash-tag sibling).
    /// Legal but characterized: Redis's per-key WATCH would let this commit, so
    /// this is the residual slot-granular coarseness. A regression that widened
    /// the bump back to shard-scope would flip a Commit arm here and fail.
    OverAbort,
}

/// Everything an arm's EXEC observed, enough to attribute the abort.
struct CaseObservation {
    aborted: bool,
    /// Model truth: did the watched key's value/existence change during the gap?
    watched_changed: bool,
    /// Did `k`'s OWN slot version (the value `check_watches` compares) differ
    /// from the snapshot `v0`, read non-destructively just before EXEC? This is
    /// the exact signal that drives an abort under slot-granular WATCH.
    version_bumped: bool,
}

async fn run_case(interleave: Interleave) -> CaseObservation {
    let mut d = ShardDriver::new(1);

    // Seed the watched key and an unrelated key.
    let _ = d.execute(0, "SET", &["k", "v0"]).await;
    let _ = d.execute(0, "SET", &["u", "u0"]).await;

    // Conn A reads the pre-gap version OF THE WATCHED KEY and watches k at it.
    let v0 = d.key_version(0, "k").await;

    let mut watched_changed = false;
    match interleave {
        Interleave::None => {}
        Interleave::ReadWatchedKey => {
            // A pure read of the still-live watched key: must not bump/purge.
            let _ = d.execute_conn(0, 2, "GET", &["k"]).await;
        }
        Interleave::UnrelatedWrite => {
            // `u` is a different slot from `k` (same shard): no bump to k's slot.
            let _ = d.execute_conn(0, 2, "SET", &["u", "u1"]).await;
        }
        Interleave::SameSlotWrite => {
            // `{k}sib` shares k's slot via the `{k}` hash tag → bumps k's slot.
            let _ = d.execute_conn(0, 2, "SET", &["{k}sib", "s1"]).await;
        }
        Interleave::WatchedWrite => {
            let _ = d.execute_conn(0, 2, "SET", &["k", "v1"]).await;
            watched_changed = true;
        }
        Interleave::ActiveExpiryUnrelated => {
            // Seed an unrelated key (different slot) with an elapsed TTL, sweep.
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

    // Whether k's OWN slot version moved since the snapshot — the exact signal
    // `check_watches` compares on. Read non-destructively via `key_version`
    // (direct `get_key_version`, no GetVersion round-trip, so no lazy purge).
    let version_bumped = d.key_version(0, "k").await != v0;

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
        // Proposal 18 flips: unrelated different-slot write/expiry now COMMIT.
        (Interleave::UnrelatedWrite, Outcome::Commit),
        (Interleave::ActiveExpiryUnrelated, Outcome::Commit),
        // Residual slot-granular over-abort: same-slot sibling still aborts.
        (Interleave::SameSlotWrite, Outcome::OverAbort),
        (Interleave::WatchedWrite, Outcome::TrueAbort),
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
                    "{interleave:?}: Commit arm must not bump k's slot version \
                     (a read/no-op/different-slot write must be version-neutral for k)"
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
                // touched, yet a same-SLOT sibling write bumped k's slot version
                // and forced the abort.
                assert!(
                    !obs.watched_changed,
                    "{interleave:?}: OverAbort must not touch the watched key \
                     (else it is a true abort, not coarseness)"
                );
                assert!(
                    obs.version_bumped,
                    "{interleave:?}: OverAbort must be attributable to a same-slot \
                     sibling bumping k's slot version"
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
                 change and no slot-version bump"
            );
        }
    }

    // (4) Pin the exact partition for this fixed schedule: exactly two required
    // (true) aborts and exactly ONE residual slot-granular over-abort. If the
    // bump regressed back to shard-scope, `UnrelatedWrite`/`ActiveExpiryUnrelated`
    // would over-abort again and inflate this count; if the slot bump broke,
    // `SameSlotWrite` would stop aborting and drop it.
    assert_eq!(
        true_aborts, 2,
        "expected exactly two required (true) aborts: WatchedWrite + ActiveExpiryWatched"
    );
    assert_eq!(
        over_aborts, 1,
        "expected exactly one slot-granular over-abort: SameSlotWrite"
    );
}

/// Headline (red-green pin for proposal 18): `WATCH a; SET b; EXEC` where `a`
/// and `b` are on the same Internal Shard but DIFFERENT hash slots must COMMIT —
/// the watched key `a` never changed, so a slot-granular WATCH does not abort.
/// Under the pre-proposal coarse per-shard version this over-aborted.
#[tokio::test]
async fn watch_unrelated_slot_write_commits() {
    let mut d = ShardDriver::new(1);
    let _ = d.execute(0, "SET", &["a", "v0"]).await;
    // a's watch-time version (per-key under Design A; shard-wide before it).
    let v0 = d.key_version(0, "a").await;
    // An unrelated write to a DIFFERENT slot on the same shard.
    let _ = d.execute_conn(0, 2, "SET", &["b", "vb"]).await;
    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["a", "x"])],
            vec![WatchEntry {
                key: Bytes::from_static(b"a"),
                version: v0,
                live_at_watch: true,
            }],
        )
        .await;
    assert!(
        matches!(result, TransactionResult::Success(_)),
        "WATCH a; SET b (different slot, same shard); EXEC must COMMIT, got {result:?}"
    );
}

/// Slot-granularity control (proposal 18 caveat — coarser than Redis): two keys
/// colocated on the SAME hash slot via a hash tag still cross-abort. `WATCH
/// {t}a; SET {t}b; EXEC` aborts because both keys share slot `slot_for_key("t")`.
#[tokio::test]
async fn watch_same_slot_write_aborts() {
    let mut d = ShardDriver::new(1);
    let _ = d.execute(0, "SET", &["{t}a", "v0"]).await;
    let v0 = d.key_version(0, "{t}a").await;
    // Same-slot sibling (shared hash tag `{t}`).
    let _ = d.execute_conn(0, 2, "SET", &["{t}b", "vb"]).await;
    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["{t}a", "x"])],
            vec![WatchEntry {
                key: Bytes::from_static(b"{t}a"),
                version: v0,
                live_at_watch: true,
            }],
        )
        .await;
    assert!(
        matches!(result, TransactionResult::WatchAborted),
        "WATCH {{t}}a; SET {{t}}b (same slot); EXEC must abort (slot granularity), got {result:?}"
    );
}

/// Over-abort regression (proposal 18): a blocking-waiter wake bumps only the
/// woken key's slot. `WATCH a` must survive a BLPOP waiter on a DIFFERENT-slot
/// key `b` being satisfied by an `LPUSH b` — the wake path (`blocking.rs`
/// `bumps_version`) is slot-local, not shard-wide.
#[tokio::test]
async fn watch_survives_waiter_wake_on_different_slot() {
    use bytes::Bytes;

    let mut d = ShardDriver::new(1);
    let _ = d.execute(0, "SET", &["a", "v0"]).await;
    let v0 = d.key_version(0, "a").await;

    // Block a BLPOP waiter on `b` (a different slot from `a`, same shard).
    let _rx = d
        .block_wait(
            0,
            2,
            vec![Bytes::from_static(b"b")],
            BlockingOp::BLPop,
            None,
        )
        .await;
    // LPUSH b wakes the waiter → pops the element → bumps b's slot (a wake that
    // `bumps_version`), never a's slot.
    let _ = d.execute_conn(0, 3, "LPUSH", &["b", "x"]).await;

    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["a", "y"])],
            vec![WatchEntry {
                key: Bytes::from_static(b"a"),
                version: v0,
                live_at_watch: true,
            }],
        )
        .await;
    assert!(
        matches!(result, TransactionResult::Success(_)),
        "WATCH a must survive a BLPOP waiter-wake on a different-slot key b, got {result:?}"
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
    let v0 = d.key_version(0, "k").await;
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
    let v0 = d.key_version(0, "k").await;
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
    let (vb_versions, live) = d.watch_keys(0, &["k"]).await;
    let vb = vb_versions[0];
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
