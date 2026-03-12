//! Integration tests for database-specifying commands (SELECT, SWAPDB, MOVE).
//!
//! FrogDB is single-database-per-instance. These commands return
//! `DatabaseNotSupported` errors (except SELECT 0 which is a no-op).

use crate::common::test_server::TestServer;
use bytes::Bytes;
use frogdb_protocol::Response;

// =============================================================================
// SELECT
// =============================================================================

#[tokio::test]
async fn test_select_zero_returns_ok() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["SELECT", "0"]).await;
    assert_eq!(response, Response::ok());

    server.shutdown().await;
}

#[tokio::test]
async fn test_select_nonzero_returns_database_not_supported() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for db in &["1", "15"] {
        let response = client.command(&["SELECT", db]).await;
        match &response {
            Response::Error(err) => {
                let msg = String::from_utf8_lossy(err);
                assert!(
                    msg.contains("not supported"),
                    "Expected 'not supported' in error for SELECT {db}: {msg}"
                );
                assert!(
                    msg.contains("single database"),
                    "Expected 'single database' in error for SELECT {db}: {msg}"
                );
            }
            other => panic!("Expected error for SELECT {db}, got: {other:?}"),
        }
    }

    server.shutdown().await;
}

// =============================================================================
// SWAPDB
// =============================================================================

#[tokio::test]
async fn test_swapdb_returns_database_not_supported() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["SWAPDB", "0", "1"]).await;
    match &response {
        Response::Error(err) => {
            let msg = String::from_utf8_lossy(err);
            assert!(
                msg.contains("not supported"),
                "Expected 'not supported' in error: {msg}"
            );
            assert!(
                msg.contains("single database"),
                "Expected 'single database' in error: {msg}"
            );
        }
        other => panic!("Expected error response, got: {other:?}"),
    }

    server.shutdown().await;
}

// =============================================================================
// MOVE
// =============================================================================

#[tokio::test]
async fn test_move_returns_database_not_supported() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set a key first
    client.command(&["SET", "mykey", "hello"]).await;

    let response = client.command(&["MOVE", "mykey", "1"]).await;
    match &response {
        Response::Error(err) => {
            let msg = String::from_utf8_lossy(err);
            assert!(
                msg.contains("not supported"),
                "Expected 'not supported' in error: {msg}"
            );
            assert!(
                msg.contains("single database"),
                "Expected 'single database' in error: {msg}"
            );
        }
        other => panic!("Expected error response, got: {other:?}"),
    }

    // Verify key still exists (was not moved)
    let val = client.command(&["GET", "mykey"]).await;
    assert_eq!(val, Response::Bulk(Some(Bytes::from("hello"))));

    server.shutdown().await;
}
