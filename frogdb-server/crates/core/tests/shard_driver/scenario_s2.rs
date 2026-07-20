//! S2 — WATCH vs expiry vs unrelated write (characterization). WATCH is a
//! per-shard version (worker.rs:459-469): any same-shard write or active-expiry
//! removal bumps the version and aborts EXEC. Invariant: zero false negatives
//! (a genuine change to the watched key MUST abort). Over-aborts are legal and
//! characterized. F3 (lazy-expiry false negative) is covered in Tasks 8-9.

use std::time::Duration;

use bytes::Bytes;

use super::harness::{ShardDriver, cmd};
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
            vec![(Bytes::from_static(b"k"), v0)],
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
