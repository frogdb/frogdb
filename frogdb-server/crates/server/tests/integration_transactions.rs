//! Integration tests for transaction commands (MULTI, EXEC, DISCARD, WATCH, UNWATCH).

use crate::common::test_server::TestServer;
use bytes::Bytes;
use frogdb_protocol::Response;

#[tokio::test]
async fn test_multi_exec_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue commands (use hash tags to colocate keys on same shard)
    let response = client.command(&["SET", "{k}key1", "value1"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    let response = client.command(&["SET", "{k}key2", "value2"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    let response = client.command(&["GET", "{k}key1"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Execute transaction
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Response::Simple(Bytes::from("OK")));
            assert_eq!(results[1], Response::Simple(Bytes::from("OK")));
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

#[tokio::test]
async fn test_multi_exec_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Execute with no commands
    let response = client.command(&["EXEC"]).await;
    assert_eq!(response, Response::Array(vec![]));

    server.shutdown().await;
}

#[tokio::test]
async fn test_multi_discard() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set initial value
    client.command(&["SET", "foo", "original"]).await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue a command
    let response = client.command(&["SET", "foo", "modified"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Discard transaction
    let response = client.command(&["DISCARD"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify value was not modified
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("original"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_exec_without_multi() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["EXEC"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR EXEC without MULTI")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_discard_without_multi() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["DISCARD"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR DISCARD without MULTI")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_nested_multi() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Try to start another transaction
    let response = client.command(&["MULTI"]).await;
    assert!(
        matches!(response, Response::Error(e) if e.starts_with(b"ERR MULTI calls can not be nested"))
    );

    // Discard to clean up
    client.command(&["DISCARD"]).await;

    server.shutdown().await;
}

#[tokio::test]
async fn test_watch_exec_success() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set initial value
    client.command(&["SET", "watched_key", "initial"]).await;

    // Watch the key
    let response = client.command(&["WATCH", "watched_key"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue commands
    let response = client.command(&["SET", "watched_key", "updated"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Execute (should succeed since no one else modified the key)
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Response::Simple(Bytes::from("OK")));
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify value was updated
    let response = client.command(&["GET", "watched_key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("updated"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_watch_exec_abort() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Set initial value
    client1.command(&["SET", "watched_key", "initial"]).await;

    // Client 1 watches the key
    let response = client1.command(&["WATCH", "watched_key"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Client 2 modifies the key
    client2
        .command(&["SET", "watched_key", "modified_by_client2"])
        .await;

    // Client 1 starts transaction
    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue commands
    let response = client1
        .command(&["SET", "watched_key", "modified_by_client1"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

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
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Client 2 issues a duplicate PFADD: no register moves, so the watched
    // key's version must NOT be bumped.
    let response = client2.command(&["PFADD", "{t}hll", "a"]).await;
    assert_eq!(response, Response::Integer(0));

    // Client 1 runs a transaction touching a colocated key.
    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client1.command(&["SET", "{t}x", "1"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // EXEC must succeed (array response, not nil) because the no-op PFADD did
    // not invalidate the WATCH.
    let response = client1.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Response::Simple(Bytes::from("OK")));
        }
        other => panic!("Expected array response from EXEC, got {:?}", other),
    }

    server.shutdown().await;
}

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
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Client 2 adds a new element: a register moves, so the version is bumped.
    let response = client2.command(&["PFADD", "{t}hll", "b"]).await;
    assert_eq!(response, Response::Integer(1));

    // Client 1 runs a transaction touching a colocated key.
    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client1.command(&["SET", "{t}x", "1"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // EXEC must abort (nil) because the watched key changed.
    let response = client1.command(&["EXEC"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_watch_inside_multi_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Try to WATCH inside MULTI (should error)
    let response = client.command(&["WATCH", "somekey"]).await;
    assert!(
        matches!(response, Response::Error(e) if e.starts_with(b"ERR WATCH inside MULTI is not allowed"))
    );

    // Discard to clean up
    client.command(&["DISCARD"]).await;

    server.shutdown().await;
}

#[tokio::test]
async fn test_unwatch() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Set initial value
    client1.command(&["SET", "key", "initial"]).await;

    // Client 1 watches the key
    let response = client1.command(&["WATCH", "key"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Client 1 unwatches
    let response = client1.command(&["UNWATCH"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Client 2 modifies the key
    client2.command(&["SET", "key", "modified"]).await;

    // Client 1 starts transaction (should still succeed because UNWATCH cleared watches)
    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client1.command(&["SET", "key", "from_client1"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    let response = client1.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Response::Simple(Bytes::from("OK")));
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify client1's transaction succeeded
    let response = client1.command(&["GET", "key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("from_client1"))));

    server.shutdown().await;
}

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
    let response = client.command(&["WATCH", "{t0}kv0"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    let response = client.command(&["SET", "{t1}kv1", "v"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));
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
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    // UNWATCH inside MULTI executes immediately, clearing the watch set.
    let response = client.command(&["UNWATCH"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    let response = client.command(&["SET", "{t1}kv1", "v"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1, "one queued command");
            assert_eq!(results[0], Response::Simple(Bytes::from("OK")));
        }
        other => panic!("EXEC after UNWATCH must commit (no stale CROSSSLOT fold), got {other:?}"),
    }
    // The write actually landed.
    let response = client.command(&["GET", "{t1}kv1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("v"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_transaction_with_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set a string key (use hash tags to colocate keys on same shard)
    client.command(&["SET", "{k}mystring", "hello"]).await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue a command that will succeed
    let response = client.command(&["SET", "{k}foo", "bar"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Queue a command that will fail at runtime (LPUSH on a string)
    let response = client.command(&["LPUSH", "{k}mystring", "value"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Queue another command that will succeed
    let response = client.command(&["SET", "{k}baz", "qux"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Execute - all commands run, one returns error
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Response::Simple(Bytes::from("OK"))); // SET {k}foo bar
            assert!(matches!(results[1], Response::Error(ref e) if e.starts_with(b"WRONGTYPE"))); // LPUSH {k}mystring
            assert_eq!(results[2], Response::Simple(Bytes::from("OK"))); // SET {k}baz qux
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

#[tokio::test]
async fn test_transaction_syntax_error_aborts() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue a valid command
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

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

/// Connection-level commands (CONFIG, INFO, ...) are deferred out of the shard
/// transaction and merged back afterwards. Their results must land at their
/// original queue positions — first, middle, and last.
#[tokio::test]
async fn test_transaction_connection_level_merge_order() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Interleave connection-level (CONFIG GET) with shard commands.
    for cmd in [
        vec!["CONFIG", "GET", "maxmemory"], // index 0: deferred
        vec!["SET", "{k}merge", "v1"],      // index 1: shard
        vec!["CONFIG", "GET", "maxmemory"], // index 2: deferred
        vec!["INCR", "{k}counter"],         // index 3: shard
        vec!["CONFIG", "GET", "maxmemory"], // index 4: deferred
    ] {
        let response = client.command(&cmd).await;
        assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));
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
            assert_eq!(results[1], Response::Simple(Bytes::from("OK")));
            assert_eq!(results[3], Response::Integer(1));
        }
        other => panic!("Expected array response from EXEC, got {other:?}"),
    }

    server.shutdown().await;
}

/// A transaction whose queue is entirely connection-level still checks watches
/// via an empty shard round-trip: success yields the merged results...
#[tokio::test]
async fn test_watch_with_only_connection_level_commands_success() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "watched_key", "initial"]).await;
    let response = client.command(&["WATCH", "watched_key"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    let response = client.command(&["CONFIG", "GET", "maxmemory"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

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

/// ...and a modified watched key still aborts the transaction with nil.
#[tokio::test]
async fn test_watch_with_only_connection_level_commands_abort() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    client1.command(&["SET", "watched_key", "initial"]).await;
    let response = client1.command(&["WATCH", "watched_key"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Another client touches the watched key before EXEC.
    client2.command(&["SET", "watched_key", "modified"]).await;

    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    let response = client1.command(&["CONFIG", "GET", "maxmemory"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Nil reply: the deferred commands never ran.
    let response = client1.command(&["EXEC"]).await;
    assert_eq!(response, Response::Bulk(None));

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
    assert_eq!(started, Response::Simple(Bytes::from("OK")));

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Index 0: HOTKEYS GET — a connection-level command.
    let response = client.command(&["HOTKEYS", "GET"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Index 1: FT.CURSOR DEL on a nonexistent cursor (id 0) — deterministic
    // "Cursor not found" reply, another connection-level command.
    let response = client.command(&["FT.CURSOR", "DEL", "idx", "0"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

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

#[tokio::test]
async fn test_transaction_increments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set initial counter
    client.command(&["SET", "counter", "0"]).await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue multiple increments
    for _ in 0..5 {
        let response = client.command(&["INCR", "counter"]).await;
        assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));
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
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

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
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    let response = client1.command(&["SET", "evwatch", "txn"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

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
        assert_eq!(response, Response::Simple(Bytes::from("OK")));
    }

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    let response = client.command(&["KEYS", "mxkeys:*"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

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
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    let response = client.command(&["FLUSHDB"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Response::Simple(Bytes::from("OK")));
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
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    let response = client.command(&["SCAN", "0", "COUNT", "100"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

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

/// `MULTI; FT.SEARCH <nonexistent>; EXEC` must run the real server-wide
/// FT.SEARCH (which errors on an unknown index), not the shard-side stub
/// (which used to fabricate an empty result).
#[tokio::test]
async fn test_multi_exec_ft_search_unknown_index_errors() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    let response = client
        .command(&["FT.SEARCH", "mx-nonexistent-index", "x"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

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

/// EXEC replies must appear at their queued positions when shard, server-wide,
/// and shard commands interleave: `SET k v; KEYS k*; GET k` → `[OK, [k], v]`.
#[tokio::test]
async fn test_multi_exec_server_wide_reply_ordering() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));
    for cmd in [
        &["SET", "mxorder:key", "value1"][..],
        &["KEYS", "mxorder:*"][..],
        &["GET", "mxorder:key"][..],
    ] {
        let response = client.command(cmd).await;
        assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));
    }

    let response = client.command(&["EXEC"]).await;
    let results = match response {
        Response::Array(results) => results,
        other => panic!("Expected array response from EXEC, got {other:?}"),
    };
    assert_eq!(results.len(), 3, "one reply per queued command");
    assert_eq!(results[0], Response::Simple(Bytes::from("OK")));
    assert_eq!(
        results[1],
        Response::Array(vec![Response::Bulk(Some(Bytes::from("mxorder:key")))]),
        "KEYS (deferred server-wide) reply must sit at its queued position and \
         see the transaction's write"
    );
    assert_eq!(results[2], Response::Bulk(Some(Bytes::from("value1"))));

    server.shutdown().await;
}

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
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // ZRANK on a missing key WITHSCORE queues and yields Response::NullArray.
    let response = client
        .command(&["ZRANK", "nokey", "nomember", "WITHSCORE"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

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
