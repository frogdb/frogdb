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
