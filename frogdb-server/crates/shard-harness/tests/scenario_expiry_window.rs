//! Issue 57 — commands must not read *through* the sampled-expiry window.
//!
//! Active expiry is sampled, so a key whose deadline has passed can still be
//! physically present until the sweeper reaches it. Every command that inspects
//! a key has to apply the logical-expiry check itself; the ones that did not
//! observed a dead key as alive.
//!
//! `PERSIST` was the worst of them: it cleared `expires_at` **and** removed the
//! key from the expiry index, so a key that was already logically dead became
//! **permanently** immortal — nothing could ever expire it again. That also
//! diverges a primary from its replica, which expires independently.
//!
//! Level 3 (shard driver): these need the real store's expiry index and the
//! shard's expiry tick, which a leaked `HashMapStore` has neither of. Nothing
//! about sockets, connections or routing is involved, so a server integration
//! test would only add latency.
//!
//! The window is entered deterministically through the production
//! `DEBUG EXPIRE-BACKDATE` seam ([`ShardDriver::backdate_expiry`]) — **never a
//! sleep** — and no test below ticks active expiry before the command under
//! test runs, so the key is guaranteed to still be physically present.
//!
//! One `#[test]` per command, so a partial fix cannot pass.

use bytes::Bytes;
use frogdb_protocol::{Response, SafeStatus};

use frogdb_core::store::BackdateExpiryResult;
use frogdb_shard_harness::harness::ShardDriver;

/// Seed `key` on shard 0 with a live TTL, then backdate the deadline into the
/// past. The key is left physically present and logically expired.
async fn seed_past_deadline(d: &mut ShardDriver, key: &str) {
    assert!(matches!(
        d.execute(0, "SET", &[key, "v"]).await,
        Response::Simple(_)
    ));
    assert_eq!(
        d.execute(0, "EXPIRE", &[key, "100"]).await,
        Response::Integer(1)
    );
    assert_eq!(
        d.backdate_expiry(0, key, 1_000).await,
        BackdateExpiryResult::Backdated
    );
}

/// PERSIST on a past-deadline key must delete it, not strip the deadline.
///
/// Pre-fix this replied `1` and left a key with no expiry at all — immortal.
#[tokio::test]
async fn persist_on_expired_key_does_not_immortalize() {
    let mut d = ShardDriver::new(1);
    seed_past_deadline(&mut d, "k").await;

    assert_eq!(
        d.execute(0, "PERSIST", &["k"]).await,
        Response::Integer(0),
        "PERSIST on a logically dead key must report 0"
    );

    // The key must be gone for good: an expiry tick cannot rescue a key whose
    // deadline PERSIST already threw away, so this is the assertion that pins
    // the "permanently immortal" outcome.
    d.tick_expiry(0).await;
    assert_eq!(
        d.execute(0, "EXISTS", &["k"]).await,
        Response::Integer(0),
        "the key must be gone after the sweep"
    );

    // Regression guard, not the failing signal: the pre-fix state was
    // *self-consistent* (PERSIST cleared the deadline AND the index entry), so
    // this check reports zero anomalies both before and after the fix. It is
    // here to catch a future fix that deletes the entry but leaks its index row.
    assert!(
        d.expiry_index_check(0).await.anomalies.is_empty(),
        "expiry index must stay consistent"
    );
}

/// RENAME must treat a past-deadline source as "no such key", and must not
/// carry a dead value (or its stale deadline) over to the destination.
#[tokio::test]
async fn rename_of_expired_source_is_no_such_key() {
    let mut d = ShardDriver::new(1);
    seed_past_deadline(&mut d, "k").await;

    match d.execute(0, "RENAME", &["k", "k2"]).await {
        Response::Error(msg) => assert!(
            String::from_utf8_lossy(&msg)
                .to_ascii_lowercase()
                .contains("no such key"),
            "expected 'no such key', got {msg:?}"
        ),
        other => panic!("RENAME of a past-deadline key must error, got {other:?}"),
    }
    assert_eq!(
        d.execute(0, "EXISTS", &["k2"]).await,
        Response::Integer(0),
        "a dead value must not be resurrected under the new name"
    );
    assert!(d.expiry_index_check(0).await.anomalies.is_empty());
}

/// RENAMENX has two reads through the window: the destination existence probe
/// and the source value read. Both must see a past-deadline key as gone.
#[tokio::test]
async fn renamenx_sees_expired_source_and_destination_as_gone() {
    let mut d = ShardDriver::new(1);

    // Arm 1 — expired *destination*: the NX probe must not block the rename.
    assert!(matches!(
        d.execute(0, "SET", &["src", "fresh"]).await,
        Response::Simple(_)
    ));
    seed_past_deadline(&mut d, "dst").await;
    assert_eq!(
        d.execute(0, "RENAMENX", &["src", "dst"]).await,
        Response::Integer(1),
        "a past-deadline destination does not exist, so RENAMENX must proceed"
    );
    assert_eq!(
        d.execute(0, "GET", &["dst"]).await,
        Response::Bulk(Some(Bytes::from_static(b"fresh")))
    );

    // Arm 2 — expired *source*: "no such key", nothing written.
    seed_past_deadline(&mut d, "k").await;
    match d.execute(0, "RENAMENX", &["k", "k2"]).await {
        Response::Error(msg) => assert!(
            String::from_utf8_lossy(&msg)
                .to_ascii_lowercase()
                .contains("no such key"),
            "expected 'no such key', got {msg:?}"
        ),
        other => panic!("RENAMENX of a past-deadline key must error, got {other:?}"),
    }
    assert_eq!(d.execute(0, "EXISTS", &["k2"]).await, Response::Integer(0));
    assert!(d.expiry_index_check(0).await.anomalies.is_empty());
}

/// TYPE must report `none` for a past-deadline key — and must stay
/// non-destructive (a metadata probe never physically purges).
#[tokio::test]
async fn type_of_expired_key_is_none() {
    let mut d = ShardDriver::new(1);
    seed_past_deadline(&mut d, "k").await;

    assert_eq!(
        d.execute(0, "TYPE", &["k"]).await,
        Response::Simple(SafeStatus::from_static("none")),
        "TYPE must not report the type of a logically dead key"
    );
}

/// EXISTS must not count a past-deadline key.
#[tokio::test]
async fn exists_does_not_count_expired_key() {
    let mut d = ShardDriver::new(1);
    seed_past_deadline(&mut d, "k").await;
    assert!(matches!(
        d.execute(0, "SET", &["live", "v"]).await,
        Response::Simple(_)
    ));

    assert_eq!(
        d.execute(0, "EXISTS", &["k"]).await,
        Response::Integer(0),
        "a past-deadline key must not be counted"
    );
    assert_eq!(
        d.execute(0, "EXISTS", &["k", "live"]).await,
        Response::Integer(1),
        "only the live key counts in a multi-key EXISTS"
    );
}

/// EXPIRETIME/PEXPIRETIME must reply `-2` (no such key) for a past-deadline
/// key, matching the guard TTL/PTTL already carry — not the stale absolute
/// timestamp of a deadline that has already gone by.
#[tokio::test]
async fn expiretime_of_expired_key_is_minus_two() {
    let mut d = ShardDriver::new(1);
    seed_past_deadline(&mut d, "k").await;

    assert_eq!(
        d.execute(0, "EXPIRETIME", &["k"]).await,
        Response::Integer(-2),
        "EXPIRETIME on a logically dead key must report -2"
    );
    assert_eq!(
        d.execute(0, "PEXPIRETIME", &["k"]).await,
        Response::Integer(-2),
        "PEXPIRETIME on a logically dead key must report -2"
    );
    // TTL/PTTL already had the guard; pinned here so the four stay aligned.
    assert_eq!(d.execute(0, "TTL", &["k"]).await, Response::Integer(-2));
    assert_eq!(d.execute(0, "PTTL", &["k"]).await, Response::Integer(-2));
}
