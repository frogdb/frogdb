//! S2 — WATCH vs expiry vs unrelated write (characterization). WATCH is a
//! per-shard version (worker.rs:459-469): any same-shard write or active-expiry
//! removal bumps the version and aborts EXEC. Invariant: zero false negatives
//! (a genuine change to the watched key MUST abort). Over-aborts are legal and
//! characterized. F3 (lazy-expiry false negative) is covered in Tasks 8-9.

use std::time::Duration;

use bytes::Bytes;

use super::harness::{ShardDriver, cmd};
use frogdb_core::shard::WatchEntry;
use frogdb_core::shard::types::TransactionResult;

/// What happens in the WATCH→EXEC gap.
#[derive(Debug, Clone, Copy)]
enum Interleave {
    None,
    UnrelatedWrite,
    WatchedWrite,
    ActiveExpiryUnrelated,
    ActiveExpiryWatched,
}

/// Returns (aborted, watched_key_changed). `watched_key_changed` is the model
/// truth: did the watched key's value/existence change due to a write or
/// active expiry during the gap?
async fn run_case(interleave: Interleave) -> (bool, bool) {
    let mut d = ShardDriver::new(1);

    // Seed the watched key and an unrelated key.
    let _ = d.execute(0, "SET", &["k", "v0"]).await;
    let _ = d.execute(0, "SET", &["u", "u0"]).await;

    // Conn A reads the pre-gap version and watches k at it.
    let v0 = d.get_version(0).await;

    let mut watched_changed = false;
    match interleave {
        Interleave::None => {}
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
            d.tick_expiry(0);
        }
        Interleave::ActiveExpiryWatched => {
            let _ = d.execute(0, "PEXPIRE", &["k", "1"]).await;
            tokio::time::sleep(Duration::from_millis(3)).await;
            d.tick_expiry(0);
            watched_changed = true;
        }
    }

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
    (aborted, watched_changed)
}

#[tokio::test]
async fn s2_zero_false_negatives_and_over_abort_characterized() {
    let cases = [
        Interleave::None,
        Interleave::UnrelatedWrite,
        Interleave::WatchedWrite,
        Interleave::ActiveExpiryUnrelated,
        Interleave::ActiveExpiryWatched,
    ];
    let mut over_aborts = 0u32;
    for c in cases {
        let (aborted, changed) = run_case(c).await;
        // Zero false negatives: a real change must abort.
        if changed {
            assert!(
                aborted,
                "false negative: {c:?} changed the watched key but EXEC committed"
            );
        }
        // Characterize over-aborts (legal, per the pinned per-shard-version
        // divergence) — count, do not assert.
        if aborted && !changed {
            over_aborts += 1;
        }
    }
    // Documented over-abort sources: unrelated same-shard write + unrelated
    // active-expiry removal both bump shard_version.
    eprintln!("S2 over-aborts (legal, characterized): {over_aborts}");
    assert!(
        over_aborts >= 1,
        "expected the per-shard-version over-abort to be observable"
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
