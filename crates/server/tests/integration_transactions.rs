//! Integration tests for transaction commands (MULTI, EXEC, DISCARD, WATCH, UNWATCH).

mod common;

use bytes::Bytes;
use common::test_server::TestServer;
use frogdb_protocol::Response;

#[tokio::test]
async fn test_multi_exec_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue commands
    let response = client.command(&["SET", "key1", "value1"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    let response = client.command(&["SET", "key2", "value2"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    let response = client.command(&["GET", "key1"]).await;
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
    let response = client.command(&["GET", "key1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    let response = client.command(&["GET", "key2"]).await;
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
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR MULTI calls can not be nested")));

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
    client2.command(&["SET", "watched_key", "modified_by_client2"]).await;

    // Client 1 starts transaction
    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue commands
    let response = client1.command(&["SET", "watched_key", "modified_by_client1"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Execute (should fail/abort because the watched key was modified)
    let response = client1.command(&["EXEC"]).await;
    assert_eq!(response, Response::Bulk(None)); // Nil response on WATCH abort

    // Verify value is still what client2 set (client1's transaction was aborted)
    let response = client1.command(&["GET", "watched_key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("modified_by_client2"))));

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
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR WATCH inside MULTI is not allowed")));

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

    // Set a string key
    client.command(&["SET", "mystring", "hello"]).await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue a command that will succeed
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Queue a command that will fail at runtime (LPUSH on a string)
    let response = client.command(&["LPUSH", "mystring", "value"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Queue another command that will succeed
    let response = client.command(&["SET", "baz", "qux"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Execute - all commands run, one returns error
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Response::Simple(Bytes::from("OK"))); // SET foo bar
            assert!(matches!(results[1], Response::Error(ref e) if e.starts_with(b"WRONGTYPE"))); // LPUSH mystring
            assert_eq!(results[2], Response::Simple(Bytes::from("OK"))); // SET baz qux
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify the successful commands did execute
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("bar"))));

    let response = client.command(&["GET", "baz"]).await;
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
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR wrong number of arguments")));

    // Execute - should abort due to syntax error during queuing
    let response = client.command(&["EXEC"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"EXECABORT")));

    // Verify the first command was NOT executed
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(None));

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
