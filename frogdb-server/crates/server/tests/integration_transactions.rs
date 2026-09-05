//! Integration tests for transaction commands (MULTI, EXEC, DISCARD, WATCH, UNWATCH).

use crate::common::test_server::{TestServer, TestServerConfig};
use bytes::Bytes;
use frogdb_protocol::Response;

/// Find two plain (non-hash-tagged) keys that hash to different shards, so a
/// MULTI spanning them is a genuine cross-internal-shard transaction. Mirrors
/// the helper in `integration_client.rs`; duplicated here to keep this test file
/// self-contained.
fn cross_shard_key_pair(num_shards: usize) -> (String, String) {
    let mut first: Option<(usize, String)> = None;
    for i in 0..100_000 {
        let key = format!("txnkey:{i}");
        let shard = frogdb_core::shard_for_key(key.as_bytes(), num_shards);
        match &first {
            None => first = Some((shard, key)),
            Some((s0, k0)) if *s0 != shard => return (k0.clone(), key),
            _ => {}
        }
    }
    panic!("could not find a cross-shard key pair for {num_shards} shards");
}

// FM-TXN-035
#[tokio::test]
async fn test_multi_exec_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Queue commands (use hash tags to colocate keys on same shard)
    let response = client.command(&["SET", "{k}key1", "value1"]).await;
    assert_eq!(response, Response::queued());

    let response = client.command(&["SET", "{k}key2", "value2"]).await;
    assert_eq!(response, Response::queued());

    let response = client.command(&["GET", "{k}key1"]).await;
    assert_eq!(response, Response::queued());

    // Execute transaction
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Response::ok());
            assert_eq!(results[1], Response::ok());
            assert_eq!(results[2], Response::Bulk(Some(Bytes::from("value1"))));
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify values are persisted
    let response = client.command(&["GET", "{k}key1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    let response = client.command(&["GET", "{k}key2"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value2"))));

    server.shutdown().await;
}

// FM-TXN-018
#[tokio::test]
async fn test_multi_exec_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Execute with no commands
    let response = client.command(&["EXEC"]).await;
    assert_eq!(response, Response::Array(vec![]));

    server.shutdown().await;
}

// FM-TXN-004
#[tokio::test]
async fn test_multi_discard() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set initial value
    client.command(&["SET", "foo", "original"]).await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Queue a command
    let response = client.command(&["SET", "foo", "modified"]).await;
    assert_eq!(response, Response::queued());

    // Discard transaction
    let response = client.command(&["DISCARD"]).await;
    assert_eq!(response, Response::ok());

    // Verify value was not modified
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("original"))));

    server.shutdown().await;
}

// FM-TXN-002
#[tokio::test]
async fn test_exec_without_multi() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["EXEC"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR EXEC without MULTI")));

    server.shutdown().await;
}

// FM-TXN-003
#[tokio::test]
async fn test_discard_without_multi() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["DISCARD"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR DISCARD without MULTI")));

    server.shutdown().await;
}

// FM-TXN-001
#[tokio::test]
async fn test_nested_multi() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Try to start another transaction
    let response = client.command(&["MULTI"]).await;
    assert!(
        matches!(response, Response::Error(e) if e.starts_with(b"ERR MULTI calls can not be nested"))
    );

    // Discard to clean up
    client.command(&["DISCARD"]).await;

    server.shutdown().await;
}

// FM-TXN-033
#[tokio::test]
async fn test_watch_exec_success() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set initial value
    client.command(&["SET", "watched_key", "initial"]).await;

    // Watch the key
    let response = client.command(&["WATCH", "watched_key"]).await;
    assert_eq!(response, Response::ok());

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Queue commands
    let response = client.command(&["SET", "watched_key", "updated"]).await;
    assert_eq!(response, Response::queued());

    // Execute (should succeed since no one else modified the key)
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Response::ok());
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify value was updated
    let response = client.command(&["GET", "watched_key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("updated"))));

    server.shutdown().await;
}

// FM-TXN-033
#[tokio::test]
async fn test_watch_exec_abort() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Set initial value
    client1.command(&["SET", "watched_key", "initial"]).await;

    // Client 1 watches the key
    let response = client1.command(&["WATCH", "watched_key"]).await;
    assert_eq!(response, Response::ok());

    // Client 2 modifies the key
    client2
        .command(&["SET", "watched_key", "modified_by_client2"])
        .await;

    // Client 1 starts transaction
    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Queue commands
    let response = client1
        .command(&["SET", "watched_key", "modified_by_client1"])
        .await;
    assert_eq!(response, Response::queued());

    // Execute (should fail/abort because the watched key was modified)
    let response = client1.command(&["EXEC"]).await;
    assert_eq!(response, Response::Bulk(None)); // Nil response on WATCH abort

    // Verify value is still what client2 set (client1's transaction was aborted)
    let response = client1.command(&["GET", "watched_key"]).await;
    assert_eq!(
        response,
        Response::Bulk(Some(Bytes::from("modified_by_client2")))
    );

    server.shutdown().await;
}

// FM-TXN-050
/// Re-`WATCH`ing a key this connection already watches must not re-arm the CAS
/// against a fresher version: the write that landed between the two `WATCH`es
/// still has to abort the `EXEC`. Redis' `watchForKey` returns early for an
/// already-watched key and nothing but `EXEC`/`DISCARD`/`UNWATCH`/`RESET`
/// clears `CLIENT_DIRTY_CAS`.
#[tokio::test]
async fn test_rewatch_does_not_rearm_a_dirty_watch() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    client1.command(&["SET", "rewatched", "initial"]).await;

    // First WATCH: the CAS snapshot client1 must be held to.
    let response = client1.command(&["WATCH", "rewatched"]).await;
    assert_eq!(response, Response::ok());

    // Another client dirties the watch.
    client2.command(&["SET", "rewatched", "by_client2"]).await;

    // Second WATCH of the same key — must be a no-op for the snapshot.
    let response = client1.command(&["WATCH", "rewatched"]).await;
    assert_eq!(response, Response::ok());

    client1.command(&["MULTI"]).await;
    let response = client1.command(&["SET", "rewatched", "by_client1"]).await;
    assert_eq!(response, Response::queued());

    let response = client1.command(&["EXEC"]).await;
    assert_eq!(
        response,
        Response::Bulk(None),
        "the write between the two WATCHes must still abort the EXEC"
    );
    let response = client1.command(&["GET", "rewatched"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("by_client2"))));

    // The aborted EXEC cleared the watch set, so a fresh WATCH re-arms normally
    // and an undisturbed transaction commits.
    client1.command(&["WATCH", "rewatched"]).await;
    client1.command(&["WATCH", "rewatched"]).await;
    client1.command(&["MULTI"]).await;
    client1.command(&["SET", "rewatched", "by_client1"]).await;
    let response = client1.command(&["EXEC"]).await;
    assert_eq!(
        response,
        Response::Array(vec![Response::ok()]),
        "a re-WATCH of an untouched key must not abort either"
    );

    server.shutdown().await;
}

#[cfg(feature = "cmd-hyperloglog")]
/// A duplicate PFADD moves no register (no-op write), so it must not bump the
/// watched key's version: a WATCH over it must survive and EXEC must succeed.
#[tokio::test]
async fn test_noop_pfadd_does_not_bump_watch_version() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Seed the HLL so a subsequent duplicate PFADD is a genuine no-op.
    client2.command(&["PFADD", "{t}hll", "a"]).await;

    // Client 1 watches the HLL key.
    let response = client1.command(&["WATCH", "{t}hll"]).await;
    assert_eq!(response, Response::ok());

    // Client 2 issues a duplicate PFADD: no register moves, so the watched
    // key's version must NOT be bumped.
    let response = client2.command(&["PFADD", "{t}hll", "a"]).await;
    assert_eq!(response, Response::Integer(0));

    // Client 1 runs a transaction touching a colocated key.
    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    let response = client1.command(&["SET", "{t}x", "1"]).await;
    assert_eq!(response, Response::queued());

    // EXEC must succeed (array response, not nil) because the no-op PFADD did
    // not invalidate the WATCH.
    let response = client1.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Response::ok());
        }
        other => panic!("Expected array response from EXEC, got {:?}", other),
    }

    server.shutdown().await;
}

#[cfg(feature = "cmd-hyperloglog")]
/// Positive control: a PFADD that DOES move a register bumps the watched key's
/// version, so a WATCH over it is invalidated and EXEC aborts (nil).
#[tokio::test]
async fn test_changing_pfadd_aborts_watch() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Seed the HLL.
    client2.command(&["PFADD", "{t}hll", "a"]).await;

    // Client 1 watches the HLL key.
    let response = client1.command(&["WATCH", "{t}hll"]).await;
    assert_eq!(response, Response::ok());

    // Client 2 adds a new element: a register moves, so the version is bumped.
    let response = client2.command(&["PFADD", "{t}hll", "b"]).await;
    assert_eq!(response, Response::Integer(1));

    // Client 1 runs a transaction touching a colocated key.
    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    let response = client1.command(&["SET", "{t}x", "1"]).await;
    assert_eq!(response, Response::queued());

    // EXEC must abort (nil) because the watched key changed.
    let response = client1.command(&["EXEC"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

// FM-TXN-011
#[tokio::test]
async fn test_watch_inside_multi_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Try to WATCH inside MULTI (should error)
    let response = client.command(&["WATCH", "somekey"]).await;
    assert!(
        matches!(response, Response::Error(e) if e.starts_with(b"ERR WATCH inside MULTI is not allowed"))
    );

    // Discard to clean up
    client.command(&["DISCARD"]).await;

    server.shutdown().await;
}

// FM-TXN-013
#[tokio::test]
async fn test_unwatch() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Set initial value
    client1.command(&["SET", "key", "initial"]).await;

    // Client 1 watches the key
    let response = client1.command(&["WATCH", "key"]).await;
    assert_eq!(response, Response::ok());

    // Client 1 unwatches
    let response = client1.command(&["UNWATCH"]).await;
    assert_eq!(response, Response::ok());

    // Client 2 modifies the key
    client2.command(&["SET", "key", "modified"]).await;

    // Client 1 starts transaction (should still succeed because UNWATCH cleared watches)
    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    let response = client1.command(&["SET", "key", "from_client1"]).await;
    assert_eq!(response, Response::queued());

    let response = client1.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Response::ok());
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify client1's transaction succeeded
    let response = client1.command(&["GET", "key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("from_client1"))));

    server.shutdown().await;
}

// FM-TXN-013
// Regression (reviewer, fix round 2): UNWATCH inside MULTI clears the live watch
// set, so it must leave no stale cross-shard watch fold that would spuriously
// CROSSSLOT-reject an otherwise single-shard EXEC. The watch shards are folded at
// EXEC time (`ConnectionState::take_transaction`) from the *live* watch set, so a
// cleared set contributes nothing. `{t0}kv0` and `{t1}kv1` own different shards
// (4-shard standalone); the control case below proves it by CROSSSLOT-rejecting
// the same setup without the UNWATCH, which also pins the a2f3eef9 cross-shard
// WATCH false-negative protection.
#[tokio::test]
async fn test_unwatch_in_multi_clears_stale_cross_shard_watch_fold() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // --- Control: cross-shard WATCH set + single-shard EXEC must CROSSSLOT. ---
    // (Confirms the two keys really are on different shards AND that a live
    // cross-shard watch set still promotes the target to Multi.)
    // The watched key is seeded first: only a *live* watch folds its shard
    // (FM-TXN-020), so watching an absent key here would commit instead.
    client.command(&["SET", "{t0}kv0", "seed"]).await;
    let response = client.command(&["WATCH", "{t0}kv0"]).await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["SET", "{t1}kv1", "v"]).await;
    assert_eq!(response, Response::queued());
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Error(e) => assert!(
            e.starts_with(b"CROSSSLOT"),
            "cross-shard WATCH set must CROSSSLOT at EXEC, got {:?}",
            String::from_utf8_lossy(&e)
        ),
        other => panic!("expected CROSSSLOT error, got {other:?}"),
    }

    // --- Regression: same setup but UNWATCH inside MULTI must let EXEC commit. ---
    let response = client.command(&["WATCH", "{t0}kv0"]).await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    // UNWATCH inside MULTI executes immediately, clearing the watch set.
    let response = client.command(&["UNWATCH"]).await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["SET", "{t1}kv1", "v"]).await;
    assert_eq!(response, Response::queued());
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1, "one queued command");
            assert_eq!(results[0], Response::ok());
        }
        other => panic!("EXEC after UNWATCH must commit (no stale CROSSSLOT fold), got {other:?}"),
    }
    // The write actually landed.
    let response = client.command(&["GET", "{t1}kv1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("v"))));

    server.shutdown().await;
}

// FM-TXN-020
/// The canonical create-if-absent CAS, across shards: `WATCH counter` while
/// `counter` does not exist, then a `MULTI` whose queued write lives on another
/// shard. A dead watch has no data on its shard to be atomic with, so it must
/// not promote the target to `Multi` — Redis commits this; FrogDB used to
/// answer `-CROSSSLOT`.
#[tokio::test]
async fn test_dead_cross_shard_watch_commits_instead_of_crossslot() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let (absent, written) = cross_shard_key_pair(4);

    let response = client.command(&["WATCH", &absent]).await;
    assert_eq!(response, Response::ok());
    client.command(&["MULTI"]).await;
    client.command(&["SET", &written, "v"]).await;

    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1, "one queued command");
            assert_eq!(results[0], Response::ok());
        }
        other => panic!("a dead cross-shard watch must not CROSSSLOT, got {other:?}"),
    }
    let response = client.command(&["GET", &written]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("v"))));

    server.shutdown().await;
}

// FM-TXN-020, FM-TXN-033
/// The safety half of the same rule: the dead watch's shard is *not* folded into
/// the target, so the version check has to reach it on a round-trip of its own.
/// Another client creating the watched key before `EXEC` breaks the CAS, and
/// missing it would be a silent WATCH false negative — the shape FM-TXN-020
/// exists to forbid. Also FM-TXN-033's case (i): the key was absent at `WATCH`
/// and is later created — `live_at_watch = false`, so this abort comes from the
/// off-target round-trip's own slot-version check, not from FM-TXN-033's
/// `live_at_watch && !exists_unexpired` clause.
#[tokio::test]
async fn test_dead_cross_shard_watch_still_aborts_when_the_key_is_created() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;
    let (absent, written) = cross_shard_key_pair(4);

    let response = client1.command(&["WATCH", &absent]).await;
    assert_eq!(response, Response::ok());

    // Another client creates the watched-nonexistent key on its own shard.
    client2.command(&["SET", &absent, "raced"]).await;

    client1.command(&["MULTI"]).await;
    client1.command(&["SET", &written, "v"]).await;

    let response = client1.command(&["EXEC"]).await;
    assert_eq!(
        response,
        Response::Bulk(None),
        "creating the watched key must abort the CAS even with its shard unfolded"
    );
    // The batch never ran.
    let response = client1.command(&["GET", &written]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

// FM-TXN-049
/// One `WATCH` naming keys on two different internal shards must be accepted.
/// Watch sets are not co-location-constrained (only the queued batch is), so a
/// client that batches its watches cannot be refused where a client issuing one
/// `WATCH` per key succeeds. FrogDB used to answer `-CROSSSLOT` here, in every
/// mode, from an internal-shard pre-check.
#[tokio::test]
async fn test_batched_cross_shard_watch_is_not_crossslot() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let (first, second) = cross_shard_key_pair(4);

    let response = client.command(&["WATCH", &first, &second]).await;
    assert_eq!(
        response,
        Response::ok(),
        "a watch set spanning internal shards must be accepted"
    );

    // Neither watched key exists, so neither folds its shard into the target
    // (FM-TXN-020) and the EXEC commits.
    client.command(&["MULTI"]).await;
    client.command(&["SET", &first, "v"]).await;
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => assert_eq!(results.len(), 1, "the queued SET ran"),
        other => panic!("batched cross-shard WATCH must reach a normal EXEC, got {other:?}"),
    }

    server.shutdown().await;
}

// FM-TXN-049
/// The batched path must build the *same* watch set the sequential path builds,
/// which means each key's version is probed on the shard that owns it. Probing
/// both keys on one shard would register the second key against a version
/// counter its own shard never moves — a CAS that silently never fires.
#[tokio::test]
async fn test_batched_cross_shard_watch_probes_each_key_on_its_own_shard() {
    let server = TestServer::start_standalone().await;
    let mut watcher = server.connect().await;
    let mut writer = server.connect().await;
    let (first, second) = cross_shard_key_pair(4);

    // Both keys are absent, so neither folds its shard into the target
    // (FM-TXN-020) and the EXEC below is a single-shard batch on `first`'s
    // shard — `second`'s watch is checked by a round-trip of its own.
    let response = watcher.command(&["WATCH", &first, &second]).await;
    assert_eq!(response, Response::ok());

    // Dirty only the second key — the one whose shard is not the batch's target.
    writer.command(&["SET", &second, "created"]).await;

    watcher.command(&["MULTI"]).await;
    watcher.command(&["SET", &first, "v"]).await;
    let response = watcher.command(&["EXEC"]).await;
    assert_eq!(
        response,
        Response::Bulk(None),
        "the second key's watch must have been taken on its own shard"
    );

    server.shutdown().await;
}

// FM-TXN-020
/// Packing does not change EXEC semantics either: two *live* watched keys on
/// different shards still resolve to `Multi` and CROSSSLOT at EXEC — the same
/// answer two sequential `WATCH`es produce — but the refusal comes from EXEC,
/// not from `WATCH`.
#[tokio::test]
async fn test_batched_live_cross_shard_watch_defers_crossslot_to_exec() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let (first, second) = cross_shard_key_pair(4);
    client.command(&["SET", &first, "seed"]).await;
    client.command(&["SET", &second, "seed"]).await;

    let response = client.command(&["WATCH", &first, &second]).await;
    assert_eq!(response, Response::ok());

    client.command(&["MULTI"]).await;
    client.command(&["SET", &first, "v"]).await;
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Error(text) => assert!(
            String::from_utf8_lossy(&text).starts_with("CROSSSLOT"),
            "live cross-shard watch set must CROSSSLOT at EXEC, got {text:?}"
        ),
        other => panic!("expected a CROSSSLOT error at EXEC, got {other:?}"),
    }

    server.shutdown().await;
}

// FM-TXN-036
#[tokio::test]
async fn test_transaction_with_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set a string key (use hash tags to colocate keys on same shard)
    client.command(&["SET", "{k}mystring", "hello"]).await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Queue a command that will succeed
    let response = client.command(&["SET", "{k}foo", "bar"]).await;
    assert_eq!(response, Response::queued());

    // Queue a command that will fail at runtime (LPUSH on a string)
    let response = client.command(&["LPUSH", "{k}mystring", "value"]).await;
    assert_eq!(response, Response::queued());

    // Queue another command that will succeed
    let response = client.command(&["SET", "{k}baz", "qux"]).await;
    assert_eq!(response, Response::queued());

    // Execute - all commands run, one returns error
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Response::ok()); // SET {k}foo bar
            assert!(matches!(results[1], Response::Error(ref e) if e.starts_with(b"WRONGTYPE"))); // LPUSH {k}mystring
            assert_eq!(results[2], Response::ok()); // SET {k}baz qux
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify the successful commands did execute
    let response = client.command(&["GET", "{k}foo"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("bar"))));

    let response = client.command(&["GET", "{k}baz"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("qux"))));

    server.shutdown().await;
}

// FM-TXN-005
/// An unknown command inside MULTI poisons the queue exactly like a bad arity
/// does: the reply is the unknown-command error rather than `+QUEUED`, and EXEC
/// answers EXECABORT without running the commands that did queue. Redis calls
/// this `flagTransaction`; the queue is never "best effort".
#[tokio::test]
async fn test_unknown_command_in_multi_aborts_the_transaction() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    let response = client.command(&["SET", "unknowncmdkey", "bar"]).await;
    assert_eq!(response, Response::queued());

    let response = client.command(&["NOSUCHCOMMAND", "a", "b"]).await;
    assert!(
        matches!(&response, Response::Error(e) if e.starts_with(b"ERR unknown command")),
        "an unknown command inside MULTI must fail as unknown, got {response:?}"
    );

    let response = client.command(&["EXEC"]).await;
    assert!(
        matches!(&response, Response::Error(e) if e.starts_with(b"EXECABORT")),
        "a poisoned queue must abort at EXEC, got {response:?}"
    );

    // The queued SET must not have run.
    let response = client.command(&["GET", "unknowncmdkey"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

// FM-TXN-006
#[tokio::test]
async fn test_transaction_syntax_error_aborts() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Queue a valid command
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert_eq!(response, Response::queued());

    // Queue a command with wrong number of arguments (syntax error)
    let response = client.command(&["GET"]).await; // GET requires 1 argument
    assert!(
        matches!(response, Response::Error(e) if e.starts_with(b"ERR wrong number of arguments"))
    );

    // Execute - should abort due to syntax error during queuing
    let response = client.command(&["EXEC"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"EXECABORT")));

    // Verify the first command was NOT executed
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

/// The wrong-arity error raised while *queuing* inside MULTI must render the
/// command name lowercase, exactly like the non-MULTI path does. Redis stores
/// `c->cmd->fullname` lowercase and replies `'get'` / `'eval_ro'`; FrogDB's
/// registry entries carry the uppercase spec names, so the queue path used to
/// leak `'GET'` / `'EVAL_RO'` to clients that parse the error text.
///
/// Deliberately untagged: FM-TXN-006 already forces the abort *outcome* of this
/// path, and its Observable does not reach the error text. See the follow-up
/// note at the fix site in `connection/guards.rs::queue_command`.
#[tokio::test]
async fn test_multi_wrong_arity_error_names_command_lowercase() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // A generic shard command and a connection-level scripting command, typed
    // in upper *and* lower case, all report the canonical lowercase name.
    for (cmd, expected) in [
        (
            vec!["GET"],
            "ERR wrong number of arguments for 'get' command",
        ),
        (
            vec!["get"],
            "ERR wrong number of arguments for 'get' command",
        ),
        (
            vec!["EVAL_RO", "return 1"],
            "ERR wrong number of arguments for 'eval_ro' command",
        ),
        (
            vec!["eval_ro", "return 1"],
            "ERR wrong number of arguments for 'eval_ro' command",
        ),
    ] {
        let response = client.command(&["MULTI"]).await;
        assert_eq!(response, Response::ok());

        let response = client.command(&cmd).await;
        assert_eq!(
            response,
            Response::Error(Bytes::from(expected)),
            "queuing {cmd:?} inside MULTI must report the lowercase command name"
        );

        // The rejection still poisons the transaction (FM-TXN-006 behavior).
        let response = client.command(&["EXEC"]).await;
        assert!(
            matches!(&response, Response::Error(e) if e.starts_with(b"EXECABORT")),
            "a wrong-arity rejection must still abort at EXEC, got {response:?}"
        );
    }

    // Outside MULTI the same commands already rendered lowercase — pin it so the
    // two paths cannot drift apart again.
    let response = client.command(&["GET"]).await;
    assert_eq!(
        response,
        Response::Error(Bytes::from(
            "ERR wrong number of arguments for 'get' command"
        ))
    );
    let response = client.command(&["EVAL_RO", "return 1"]).await;
    assert_eq!(
        response,
        Response::Error(Bytes::from(
            "ERR wrong number of arguments for 'eval_ro' command"
        ))
    );

    server.shutdown().await;
}

// FM-TXN-037
/// Connection-level commands (CONFIG, INFO, ...) are deferred out of the shard
/// transaction and merged back afterwards. Their results must land at their
/// original queue positions — first, middle, and last.
#[tokio::test]
async fn test_transaction_connection_level_merge_order() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Interleave connection-level (CONFIG GET) with shard commands.
    for cmd in [
        vec!["CONFIG", "GET", "maxmemory"], // index 0: deferred
        vec!["SET", "{k}merge", "v1"],      // index 1: shard
        vec!["CONFIG", "GET", "maxmemory"], // index 2: deferred
        vec!["INCR", "{k}counter"],         // index 3: shard
        vec!["CONFIG", "GET", "maxmemory"], // index 4: deferred
    ] {
        let response = client.command(&cmd).await;
        assert_eq!(response, Response::queued());
    }

    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 5);
            // CONFIG GET replies are key/value arrays (or maps in RESP3).
            for i in [0, 2, 4] {
                assert!(
                    matches!(results[i], Response::Array(_) | Response::Map(_)),
                    "expected CONFIG GET reply at index {i}, got {:?}",
                    results[i]
                );
            }
            assert_eq!(results[1], Response::ok());
            assert_eq!(results[3], Response::Integer(1));
        }
        other => panic!("Expected array response from EXEC, got {other:?}"),
    }

    server.shutdown().await;
}

// FM-TXN-034
/// A transaction whose queue is entirely connection-level still checks watches
/// via an empty shard round-trip: success yields the merged results...
#[tokio::test]
async fn test_watch_with_only_connection_level_commands_success() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "watched_key", "initial"]).await;
    let response = client.command(&["WATCH", "watched_key"]).await;
    assert_eq!(response, Response::ok());

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["CONFIG", "GET", "maxmemory"]).await;
    assert_eq!(response, Response::queued());

    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1);
            assert!(matches!(results[0], Response::Array(_) | Response::Map(_)));
        }
        other => panic!("Expected array response from EXEC, got {other:?}"),
    }

    server.shutdown().await;
}

// FM-TXN-034
/// ...and a modified watched key still aborts the transaction with nil.
#[tokio::test]
async fn test_watch_with_only_connection_level_commands_abort() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    client1.command(&["SET", "watched_key", "initial"]).await;
    let response = client1.command(&["WATCH", "watched_key"]).await;
    assert_eq!(response, Response::ok());

    // Another client touches the watched key before EXEC.
    client2.command(&["SET", "watched_key", "modified"]).await;

    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    let response = client1.command(&["CONFIG", "GET", "maxmemory"]).await;
    assert_eq!(response, Response::queued());

    // Nil reply: the deferred commands never ran.
    let response = client1.command(&["EXEC"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

// FM-TXN-034
/// The genuinely-empty arm of the same rule: `WATCH k`, another client writes
/// `k`, then `MULTI; EXEC` with *nothing* queued at all. The empty-queue fast
/// path used to skip the shard round-trip and answer `*0` — a committed
/// transaction whose CAS precondition was already broken. Redis's `execCommand`
/// checks `CLIENT_DIRTY_CAS` before queue length and answers nil; so do we.
#[tokio::test]
async fn test_watch_then_empty_multi_exec_aborts_when_the_key_was_written() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    client1.command(&["SET", "watched_key", "initial"]).await;
    let response = client1.command(&["WATCH", "watched_key"]).await;
    assert_eq!(response, Response::ok());

    // Another client dirties the watch before EXEC.
    client2.command(&["SET", "watched_key", "modified"]).await;

    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Nothing queued, but the CAS still has to be answered.
    let response = client1.command(&["EXEC"]).await;
    assert_eq!(
        response,
        Response::Bulk(None),
        "an empty EXEC on a dirtied watch must abort, not commit `*0`"
    );

    // The watch set was consumed either way: a fresh empty EXEC now commits.
    client1.command(&["MULTI"]).await;
    let response = client1.command(&["EXEC"]).await;
    assert_eq!(response, Response::Array(vec![]));

    server.shutdown().await;
}

/// Extract a RESP2 flat map/array's key names (even indices), in order.
fn resp2_flat_keys(resp: &Response) -> Vec<Vec<u8>> {
    match resp {
        Response::Array(items) => items
            .iter()
            .step_by(2)
            .map(|k| match k {
                Response::Bulk(Some(b)) => b.to_vec(),
                other => panic!("expected bulk key, got {other:?}"),
            })
            .collect(),
        other => panic!("expected Array, got {other:?}"),
    }
}

// FM-TXN-037
/// Regression: connection-level commands `HOTKEYS` and `FT.CURSOR` must be
/// deferred out of the shard transaction and *really executed* by the
/// registry-union EXEC path — not silently treated as a no-op (their old
/// behavior inside `MULTI`). Both queue as `+QUEUED`, and `EXEC` returns their
/// genuine replies at their queue positions.
#[tokio::test]
async fn test_transaction_conn_command_hotkeys_ftcursor_execute() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start a hotkeys session *outside* the transaction so that HOTKEYS GET has
    // a deterministic, non-nil map reply to produce inside EXEC.
    let started = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "cpu"])
        .await;
    assert_eq!(started, Response::ok());

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Index 0: HOTKEYS GET — a connection-level command.
    let response = client.command(&["HOTKEYS", "GET"]).await;
    assert_eq!(response, Response::queued());

    // Index 1: FT.CURSOR DEL on a nonexistent cursor (id 0) — deterministic
    // "Cursor not found" reply, another connection-level command.
    let response = client.command(&["FT.CURSOR", "DEL", "idx", "0"]).await;
    assert_eq!(response, Response::queued());

    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 2);

            // HOTKEYS GET executed: real map reply (RESP2 flat array of the four
            // unconditional fields), NOT an error and NOT a nil no-op.
            assert!(
                !matches!(results[0], Response::Error(_)),
                "HOTKEYS GET must execute, not error: {:?}",
                results[0]
            );
            assert!(
                matches!(results[0], Response::Array(_)),
                "HOTKEYS GET must return its real map/array shape, got {:?}",
                results[0]
            );
            assert_eq!(
                resp2_flat_keys(&results[0]),
                vec![
                    b"metrics".to_vec(),
                    b"count".to_vec(),
                    b"duration".to_vec(),
                    b"hotkeys".to_vec(),
                ],
                "HOTKEYS GET reply carries its real fields (proves execution, not no-op)"
            );

            // FT.CURSOR DEL executed: its genuine deterministic reply for a
            // missing cursor, proving it ran rather than being a no-op.
            assert!(
                matches!(&results[1], Response::Error(e) if e.starts_with(b"ERR Cursor not found")),
                "FT.CURSOR DEL must execute (Cursor not found), got {:?}",
                results[1]
            );
        }
        other => panic!("Expected array response from EXEC, got {other:?}"),
    }

    server.shutdown().await;
}

// FM-TXN-035
#[tokio::test]
async fn test_transaction_increments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set initial counter
    client.command(&["SET", "counter", "0"]).await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // Queue multiple increments
    for _ in 0..5 {
        let response = client.command(&["INCR", "counter"]).await;
        assert_eq!(response, Response::queued());
    }

    // Execute
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 5);
            assert_eq!(results[0], Response::Integer(1));
            assert_eq!(results[1], Response::Integer(2));
            assert_eq!(results[2], Response::Integer(3));
            assert_eq!(results[3], Response::Integer(4));
            assert_eq!(results[4], Response::Integer(5));
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify final value
    let response = client.command(&["GET", "counter"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5"))));

    server.shutdown().await;
}

// FM-TXN-033
/// A write performed inside an EVAL script bumps the WATCH version exactly
/// like a direct write: a transaction watching the key must abort after a
/// scripted SET modifies it (proposal 46 item 2 — the scripting seam used to
/// skip the whole write-effect pipeline, including the version increment).
#[tokio::test]
async fn test_scripted_write_dirties_watch() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    client1.command(&["SET", "evwatch", "initial"]).await;

    let response = client1.command(&["WATCH", "evwatch"]).await;
    assert_eq!(response, Response::ok());

    // Client 2 modifies the watched key via a script.
    let response = client2
        .command(&[
            "EVAL",
            "return redis.call('SET', KEYS[1], ARGV[1])",
            "1",
            "evwatch",
            "scripted",
        ])
        .await;
    assert_eq!(response, Response::ok());

    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    let response = client1.command(&["SET", "evwatch", "txn"]).await;
    assert_eq!(response, Response::queued());

    // EXEC must abort: the scripted write dirtied the watched key.
    let response = client1.command(&["EXEC"]).await;
    assert_eq!(
        response,
        Response::Bulk(None),
        "a scripted write must invalidate a WATCH on the written key"
    );

    let response = client1.command(&["GET", "evwatch"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("scripted"))));

    server.shutdown().await;
}

// ============================================================================
// Server-wide commands inside MULTI/EXEC
//
// Server-wide commands (KEYS, SCAN, FLUSHDB, FT.*, ...) queued in a MULTI are
// deferred past the shard transaction and fan out to ALL shards via
// `dispatch_server_wide` — exactly like the direct (non-transactional) path.
// Before this deferral existed, they executed on the single transaction shard,
// silently returning partial results (KEYS/SCAN), clearing one shard only
// (FLUSHDB), or replying from a do-nothing stub (FT.*).
// ============================================================================

// FM-TXN-039
/// `MULTI; KEYS *; EXEC` must return keys from ALL shards. The test server
/// runs 4 shards; 20 distinct keys deterministically hash across several of
/// them, so a single-shard execution could only ever see a strict subset.
#[tokio::test]
async fn test_multi_exec_keys_spans_all_shards() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..20 {
        let key = format!("mxkeys:{i}");
        let response = client.command(&["SET", &key, "v"]).await;
        assert_eq!(response, Response::ok());
    }

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["KEYS", "mxkeys:*"]).await;
    assert_eq!(response, Response::queued());

    let response = client.command(&["EXEC"]).await;
    let results = match response {
        Response::Array(results) => results,
        other => panic!("Expected array response from EXEC, got {other:?}"),
    };
    assert_eq!(results.len(), 1);
    let keys = match &results[0] {
        Response::Array(keys) => keys,
        other => panic!("Expected KEYS reply array, got {other:?}"),
    };
    assert_eq!(
        keys.len(),
        20,
        "KEYS inside MULTI must return keys from all shards, got {keys:?}"
    );

    server.shutdown().await;
}

// FM-TXN-039
/// `MULTI; FLUSHDB; EXEC` must clear ALL shards, not just the transaction's
/// target shard. Verified via DBSIZE == 0 afterwards.
#[tokio::test]
async fn test_multi_exec_flushdb_clears_all_shards() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..20 {
        let key = format!("mxflush:{i}");
        client.command(&["SET", &key, "v"]).await;
    }
    let response = client.command(&["DBSIZE"]).await;
    assert_eq!(response, Response::Integer(20));

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["FLUSHDB"]).await;
    assert_eq!(response, Response::queued());

    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Response::ok());
        }
        other => panic!("Expected array response from EXEC, got {other:?}"),
    }

    let response = client.command(&["DBSIZE"]).await;
    assert_eq!(
        response,
        Response::Integer(0),
        "FLUSHDB inside MULTI must clear every shard"
    );

    server.shutdown().await;
}

// FM-TXN-039
/// `MULTI; SCAN 0; EXEC` must return a proper `[cursor, [keys...]]` reply that
/// walks the whole (multi-shard) keyspace, not a single shard's slice.
#[tokio::test]
async fn test_multi_exec_scan_returns_full_cursor_reply() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..20 {
        let key = format!("mxscan:{i}");
        client.command(&["SET", &key, "v"]).await;
    }

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["SCAN", "0", "COUNT", "100"]).await;
    assert_eq!(response, Response::queued());

    let response = client.command(&["EXEC"]).await;
    let results = match response {
        Response::Array(results) => results,
        other => panic!("Expected array response from EXEC, got {other:?}"),
    };
    assert_eq!(results.len(), 1);

    // Collect keys from the EXEC-embedded SCAN reply, then follow the cursor
    // (outside the transaction) until exhaustion.
    let mut collected: Vec<Bytes> = Vec::new();
    let mut reply = results[0].clone();
    loop {
        let parts = match reply {
            Response::Array(parts) => parts,
            other => panic!("Expected [cursor, keys] SCAN reply, got {other:?}"),
        };
        assert_eq!(parts.len(), 2, "SCAN reply must be [cursor, keys]");
        let cursor = match &parts[0] {
            Response::Bulk(Some(c)) => String::from_utf8_lossy(c).to_string(),
            other => panic!("Expected bulk cursor, got {other:?}"),
        };
        match &parts[1] {
            Response::Array(keys) => {
                for key in keys {
                    match key {
                        Response::Bulk(Some(k)) => collected.push(k.clone()),
                        other => panic!("Expected bulk key, got {other:?}"),
                    }
                }
            }
            other => panic!("Expected key array, got {other:?}"),
        }
        if cursor == "0" {
            break;
        }
        reply = client.command(&["SCAN", &cursor, "COUNT", "100"]).await;
    }

    collected.sort();
    collected.dedup();
    assert_eq!(
        collected.len(),
        20,
        "SCAN started inside MULTI must cover all shards, got {collected:?}"
    );

    server.shutdown().await;
}

// FM-TXN-036
/// `MULTI; FT.SEARCH <nonexistent>; EXEC` must run the real server-wide
/// FT.SEARCH (which errors on an unknown index), not the shard-side stub
/// (which used to fabricate an empty result).
#[tokio::test]
async fn test_multi_exec_ft_search_unknown_index_errors() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    let response = client
        .command(&["FT.SEARCH", "mx-nonexistent-index", "x"])
        .await;
    assert_eq!(response, Response::queued());

    let response = client.command(&["EXEC"]).await;
    let results = match response {
        Response::Array(results) => results,
        other => panic!("Expected array response from EXEC, got {other:?}"),
    };
    assert_eq!(results.len(), 1);
    match &results[0] {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("no such index"),
                "expected unknown-index error, got {msg:?}"
            );
        }
        other => panic!("FT.SEARCH on unknown index inside MULTI must error, got {other:?}"),
    }

    server.shutdown().await;
}

// FM-TXN-039
/// EXEC replies must appear at their queued positions when shard, server-wide,
/// and shard commands interleave: `SET k v; KEYS k*; GET k` → `[OK, [k], v]`.
#[tokio::test]
async fn test_multi_exec_server_wide_reply_ordering() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    for cmd in [
        &["SET", "mxorder:key", "value1"][..],
        &["KEYS", "mxorder:*"][..],
        &["GET", "mxorder:key"][..],
    ] {
        let response = client.command(cmd).await;
        assert_eq!(response, Response::queued());
    }

    let response = client.command(&["EXEC"]).await;
    let results = match response {
        Response::Array(results) => results,
        other => panic!("Expected array response from EXEC, got {other:?}"),
    };
    assert_eq!(results.len(), 3, "one reply per queued command");
    assert_eq!(results[0], Response::ok());
    assert_eq!(
        results[1],
        Response::Array(vec![Response::Bulk(Some(Bytes::from("mxorder:key")))]),
        "KEYS (deferred server-wide) reply must sit at its queued position and \
         see the transaction's write"
    );
    assert_eq!(results[2], Response::Bulk(Some(Bytes::from("value1"))));

    server.shutdown().await;
}

// FM-TXN-045
/// Regression: a nested `Response::NullArray` inside an EXEC reply array must
/// encode over RESP2 as a nested null (`$-1\r\n`), not panic the encoder.
///
/// `ZRANK nokey nomember WITHSCORE` on a missing key returns `Response::NullArray`
/// (the `*-1` top-level shape). Wrapped in EXEC it becomes
/// `Response::Array([NullArray])`, which `to_resp2_frame` must recurse into. The
/// top-level codec diversion only fires at the outermost level, so the nested
/// NullArray reaches `to_resp2_frame`'s arm — which previously `unreachable!`'d
/// and panicked the connection. The correct RESP2 shape is `*1\r\n$-1\r\n`, i.e.
/// a one-element array whose sole element is a null bulk.
#[tokio::test]
async fn test_exec_nested_null_array_encodes_as_nested_null_resp2() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());

    // ZRANK on a missing key WITHSCORE queues and yields Response::NullArray.
    let response = client
        .command(&["ZRANK", "nokey", "nomember", "WITHSCORE"])
        .await;
    assert_eq!(response, Response::queued());

    // EXEC wraps the reply in an array: Response::Array([NullArray]). The RESP2
    // encoder must not panic; the wire shape `*1\r\n$-1\r\n` decodes to a
    // one-element array containing a null bulk.
    let response = client.command(&["EXEC"]).await;
    assert_eq!(
        response,
        Response::Array(vec![Response::Bulk(None)]),
        "nested NullArray must encode as RESP2 nested null ($-1), yielding *1\\r\\n$-1\\r\\n"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Cross-shard MULTI/EXEC invariant (testing-gap issue #19)
//
// `allow_cross_slot_standalone` lets *single* multi-key commands (MGET/MSET/DEL)
// scatter across internal shards, backed by VLL execution atomicity. That
// support is deliberately withheld from MULTI/EXEC transactions, which have no
// cross-shard failure-atomicity / rollback story. A shard-spanning transaction
// must therefore ALWAYS reject with CROSSSLOT at EXEC, whether the config flag
// is on or off. These tests pin that invariant: a future refactor threading the
// flag into `fold_transaction_keys` / `TransactionTarget::resolve` (state.rs)
// would permit a non-atomic partial-commit and must fail here.
// ---------------------------------------------------------------------------

/// Assert an EXEC (or queued-command) response is a CROSSSLOT error.
fn assert_crossslot(response: &Response, context: &str) {
    match response {
        Response::Error(e) => assert!(
            e.starts_with(b"CROSSSLOT"),
            "{context}: expected CROSSSLOT error, got {:?}",
            String::from_utf8_lossy(e)
        ),
        other => panic!("{context}: expected CROSSSLOT error, got {other:?}"),
    }
}

// FM-TXN-019
/// Baseline (default config: `allow_cross_slot_standalone=false`): a plain-key,
/// no-WATCH MULTI over keys on different internal shards CROSSSLOTs at EXEC.
/// Previously the only standalone cross-shard-MULTI CROSSSLOT coverage was the
/// WATCH-fold control case; this pins the plain queued-key path.
#[tokio::test]
async fn test_multi_cross_shard_plain_keys_crossslot_default_config() {
    // Default standalone server uses 4 shards.
    let (k1, k2) = cross_shard_key_pair(4);

    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["SET", k1.as_str(), "v1"]).await;
    assert_eq!(response, Response::queued());
    let response = client.command(&["SET", k2.as_str(), "v2"]).await;
    assert_eq!(response, Response::queued());
    let response = client.command(&["EXEC"]).await;
    assert_crossslot(
        &response,
        "cross-shard plain-key MULTI under default config",
    );

    // Neither write landed (transaction rejected atomically, not partially).
    assert_eq!(
        client.command(&["GET", k1.as_str()]).await,
        Response::Bulk(None)
    );
    assert_eq!(
        client.command(&["GET", k2.as_str()]).await,
        Response::Bulk(None)
    );

    server.shutdown().await;
}

// FM-TXN-021
/// Core invariant: with `allow_cross_slot_standalone=true`, single multi-key
/// commands scatter across shards (proven here by an out-of-transaction MSET
/// returning OK), yet a MULTI spanning the same two shards STILL CROSSSLOTs at
/// EXEC. The successful scatter MSET proves the flag is genuinely effective, so
/// the CROSSSLOT is the transaction restriction — not the flag silently no-oping.
#[tokio::test]
async fn test_multi_cross_shard_crossslot_with_allow_cross_slot_standalone() {
    let (k1, k2) = cross_shard_key_pair(4);

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        allow_cross_slot_standalone: true,
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    // Sanity: with the flag on, a single cross-shard MSET scatters and succeeds.
    let response = client
        .command(&["MSET", k1.as_str(), "seed1", k2.as_str(), "seed2"])
        .await;
    assert_eq!(
        response,
        Response::ok(),
        "cross-shard MSET must scatter-succeed when allow_cross_slot_standalone=true"
    );

    // But a MULTI over the same two shards must still CROSSSLOT at EXEC.
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["SET", k1.as_str(), "tx1"]).await;
    assert_eq!(response, Response::queued());
    let response = client.command(&["SET", k2.as_str(), "tx2"]).await;
    assert_eq!(response, Response::queued());
    let response = client.command(&["EXEC"]).await;
    assert_crossslot(
        &response,
        "cross-shard MULTI with allow_cross_slot_standalone=true",
    );

    // The transaction writes did not land: the seed values from MSET survive
    // unchanged (no partial commit of tx1/tx2).
    assert_eq!(
        client.command(&["GET", k1.as_str()]).await,
        Response::Bulk(Some(Bytes::from("seed1")))
    );
    assert_eq!(
        client.command(&["GET", k2.as_str()]).await,
        Response::Bulk(Some(Bytes::from("seed2")))
    );

    server.shutdown().await;
}

// FM-TXN-021
/// Regression baseline mirror: `allow_cross_slot_standalone=false` (explicit)
/// also CROSSSLOTs a cross-shard MULTI. Together with the `true` case above,
/// this proves the fold path is config-independent.
#[tokio::test]
async fn test_multi_cross_shard_crossslot_with_flag_disabled() {
    let (k1, k2) = cross_shard_key_pair(4);

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        allow_cross_slot_standalone: false,
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["SET", k1.as_str(), "v1"]).await;
    assert_eq!(response, Response::queued());
    let response = client.command(&["SET", k2.as_str(), "v2"]).await;
    assert_eq!(response, Response::queued());
    let response = client.command(&["EXEC"]).await;
    assert_crossslot(
        &response,
        "cross-shard MULTI with allow_cross_slot_standalone=false",
    );

    server.shutdown().await;
}

// FM-TXN-021
/// Boundary: enabling `allow_cross_slot_standalone` must NOT break a legitimate
/// single-shard transaction. Two distinct keys sharing a hash tag are colocated
/// on one slot/shard, so the MULTI commits normally with the flag on.
#[tokio::test]
async fn test_multi_single_shard_commits_with_allow_cross_slot_standalone() {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        allow_cross_slot_standalone: true,
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::ok());
    // Distinct keys, same hash tag -> same slot -> same shard (colocated).
    let response = client.command(&["SET", "{tag}foo", "a"]).await;
    assert_eq!(response, Response::queued());
    let response = client.command(&["SET", "{tag}bar", "b"]).await;
    assert_eq!(response, Response::queued());
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 2, "both queued commands run");
            assert_eq!(results[0], Response::ok());
            assert_eq!(results[1], Response::ok());
        }
        other => panic!("hash-tag-colocated MULTI must commit, got {other:?}"),
    }

    assert_eq!(
        client.command(&["GET", "{tag}foo"]).await,
        Response::Bulk(Some(Bytes::from("a")))
    );
    assert_eq!(
        client.command(&["GET", "{tag}bar"]).await,
        Response::Bulk(Some(Bytes::from("b")))
    );

    server.shutdown().await;
}

// FM-TXN-054
/// The transaction buffer bound as a client sees it: the command that would
/// take the queued `MULTI` past `txn-buffer-limit` is refused with the OOM
/// text in place of `+QUEUED`, `EXEC` then aborts, and nothing was applied.
#[tokio::test]
async fn test_multi_past_the_txn_buffer_limit_is_refused_then_execaborts() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // The floor: 1 MiB per core.
    let response = client
        .command(&["CONFIG", "SET", "txn-buffer-limit", "1048576"])
        .await;
    assert_eq!(response, Response::ok());
    let response = client.command(&["CONFIG", "GET", "txn-buffer-limit"]).await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("txn-buffer-limit"))),
            Response::Bulk(Some(Bytes::from("1048576"))),
        ])
    );

    let small = "v".repeat(1024);
    let big = "x".repeat(1024 * 1024);

    assert_eq!(client.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        client.command(&["SET", "txnbudget:a", &small]).await,
        Response::queued()
    );
    let response = client.command(&["SET", "txnbudget:b", &big]).await;
    assert!(
        matches!(&response, Response::Error(e) if e.as_ref() == b"OOM transaction buffer limit exceeded"),
        "the command that crosses the limit is refused, got {response:?}"
    );
    // The transaction is poisoned exactly as a bad arity poisons it.
    let response = client.command(&["EXEC"]).await;
    assert!(
        matches!(&response, Response::Error(e) if e.starts_with(b"EXECABORT")),
        "EXEC must abort a poisoned transaction, got {response:?}"
    );
    assert_eq!(
        client.command(&["GET", "txnbudget:a"]).await,
        Response::Bulk(None),
        "a command queued before the refusal must not have run"
    );
    assert_eq!(
        client.command(&["GET", "txnbudget:b"]).await,
        Response::Bulk(None)
    );

    // Outside a transaction the same write is fine: the bound is on what a
    // transaction holds, not on a command's size.
    assert_eq!(
        client.command(&["SET", "txnbudget:b", &big]).await,
        Response::ok()
    );

    server.shutdown().await;
}

// FM-TXN-054
/// Every way a transaction ends releases its charge. With the limit at the
/// floor and each transaction holding more than half of it, a second
/// transaction only fits if the first one's bytes were given back — after
/// `DISCARD`, after `RESET`, and after `EXEC`.
#[tokio::test]
async fn test_discard_and_reset_release_the_txn_buffer_charge() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["CONFIG", "SET", "txn-buffer-limit", "1048576"])
        .await;
    assert_eq!(response, Response::ok());
    // More than half the limit: two of these never fit together.
    let value = "v".repeat(600 * 1024);

    // DISCARD releases.
    assert_eq!(client.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        client.command(&["SET", "txnbudget:k", &value]).await,
        Response::queued()
    );
    assert_eq!(client.command(&["DISCARD"]).await, Response::ok());

    assert_eq!(client.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        client.command(&["SET", "txnbudget:k", &value]).await,
        Response::queued(),
        "DISCARD must have released the previous transaction's bytes"
    );
    // RESET releases.
    let response = client.command(&["RESET"]).await;
    assert!(matches!(&response, Response::Simple(s) if s.as_ref() == b"RESET"));

    assert_eq!(client.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        client.command(&["SET", "txnbudget:k", &value]).await,
        Response::queued(),
        "RESET must have released the previous transaction's bytes"
    );
    // EXEC releases once the batch has run.
    let response = client.command(&["EXEC"]).await;
    assert!(
        matches!(&response, Response::Array(r) if r.len() == 1),
        "got {response:?}"
    );

    assert_eq!(client.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        client.command(&["SET", "txnbudget:k", &value]).await,
        Response::queued(),
        "EXEC must have released the previous transaction's bytes"
    );
    assert_eq!(client.command(&["DISCARD"]).await, Response::ok());

    server.shutdown().await;
}
