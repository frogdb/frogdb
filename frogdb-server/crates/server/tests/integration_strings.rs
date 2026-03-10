//! Integration tests for string commands (LCS).


use bytes::Bytes;
use crate::common::test_server::TestServer;
use frogdb_protocol::Response;

// ============================================================================
// LCS Tests
// ============================================================================

#[tokio::test]
async fn test_lcs_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}a", "ohmytext"]).await;
    client.command(&["SET", "{t}b", "mynewtext"]).await;

    let response = client.command(&["LCS", "{t}a", "{t}b"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("mytext"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_len() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}a", "ohmytext"]).await;
    client.command(&["SET", "{t}b", "mynewtext"]).await;

    let response = client.command(&["LCS", "{t}a", "{t}b", "LEN"]).await;
    assert_eq!(response, Response::Integer(6));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_idx() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}a", "ohmytext"]).await;
    client.command(&["SET", "{t}b", "mynewtext"]).await;

    let response = client.command(&["LCS", "{t}a", "{t}b", "IDX"]).await;
    match response {
        Response::Array(arr) => {
            // Should have "matches" and "len" keys
            assert!(!arr.is_empty(), "IDX response should not be empty");
        }
        _ => panic!("Expected array response for IDX mode, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_minmatchlen() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}a", "ohmytext"]).await;
    client.command(&["SET", "{t}b", "mynewtext"]).await;

    // With MINMATCHLEN 4, should filter short matches
    let response = client
        .command(&["LCS", "{t}a", "{t}b", "IDX", "MINMATCHLEN", "4"])
        .await;
    match response {
        Response::Array(_) => {}
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_withmatchlen() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}a", "ohmytext"]).await;
    client.command(&["SET", "{t}b", "mynewtext"]).await;

    let response = client
        .command(&["LCS", "{t}a", "{t}b", "IDX", "WITHMATCHLEN"])
        .await;
    match response {
        Response::Array(_) => {}
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_nonexistent_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Non-existent keys treated as empty strings
    let response = client
        .command(&["LCS", "{t}nonexistent1", "{t}nonexistent2"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from(""))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_identical_strings() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}a", "identical"]).await;
    client.command(&["SET", "{t}b", "identical"]).await;

    let response = client.command(&["LCS", "{t}a", "{t}b"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("identical"))));

    let response = client.command(&["LCS", "{t}a", "{t}b", "LEN"]).await;
    assert_eq!(response, Response::Integer(9));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_no_common() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}a", "abc"]).await;
    client.command(&["SET", "{t}b", "xyz"]).await;

    let response = client.command(&["LCS", "{t}a", "{t}b"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from(""))));

    let response = client.command(&["LCS", "{t}a", "{t}b", "LEN"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}
