//! Integration tests for COPY and RANDOMKEY commands.


use bytes::Bytes;
use crate::common::test_server::TestServer;
use frogdb_protocol::Response;

// =============================================================================
// COPY command tests
// =============================================================================

#[tokio::test]
async fn test_copy_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SET source key
    client.command(&["SET", "src", "hello"]).await;

    // COPY to new destination
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify both keys have the same value
    let src_val = client.command(&["GET", "src"]).await;
    let dst_val = client.command(&["GET", "dst"]).await;
    assert_eq!(src_val, Response::Bulk(Some(Bytes::from("hello"))));
    assert_eq!(dst_val, Response::Bulk(Some(Bytes::from("hello"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_source_not_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // COPY nonexistent source
    let response = client.command(&["COPY", "{k}nonexistent", "{k}dst"]).await;
    assert_eq!(response, Response::Integer(0));

    // Verify destination was not created
    let dst_val = client.command(&["GET", "{k}dst"]).await;
    assert_eq!(dst_val, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_destination_exists_without_replace() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SET both keys
    client.command(&["SET", "src", "source_value"]).await;
    client.command(&["SET", "dst", "original_value"]).await;

    // COPY without REPLACE should return 0
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(0));

    // Verify destination still has original value
    let dst_val = client.command(&["GET", "dst"]).await;
    assert_eq!(dst_val, Response::Bulk(Some(Bytes::from("original_value"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_destination_exists_with_replace() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SET both keys
    client.command(&["SET", "src", "new_value"]).await;
    client.command(&["SET", "dst", "old_value"]).await;

    // COPY with REPLACE should succeed
    let response = client.command(&["COPY", "src", "dst", "REPLACE"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination has new value
    let dst_val = client.command(&["GET", "dst"]).await;
    assert_eq!(dst_val, Response::Bulk(Some(Bytes::from("new_value"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_with_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SET source key with TTL
    client.command(&["SET", "src", "hello", "EX", "100"]).await;

    // COPY to destination
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination has TTL
    let ttl = client.command(&["TTL", "dst"]).await;
    match ttl {
        Response::Integer(t) => assert!(t > 0 && t <= 100, "TTL should be between 1 and 100"),
        _ => panic!("Expected integer response for TTL"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_hash() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create hash
    client
        .command(&["HSET", "src", "field1", "value1", "field2", "value2"])
        .await;

    // COPY hash
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination is a hash with same values
    let field1 = client.command(&["HGET", "dst", "field1"]).await;
    let field2 = client.command(&["HGET", "dst", "field2"]).await;
    assert_eq!(field1, Response::Bulk(Some(Bytes::from("value1"))));
    assert_eq!(field2, Response::Bulk(Some(Bytes::from("value2"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create list
    client.command(&["RPUSH", "src", "a", "b", "c"]).await;

    // COPY list
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination has same elements
    let len = client.command(&["LLEN", "dst"]).await;
    assert_eq!(len, Response::Integer(3));

    let range = client.command(&["LRANGE", "dst", "0", "-1"]).await;
    match range {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("a"))));
            assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("b"))));
            assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("c"))));
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create set
    client
        .command(&["SADD", "src", "member1", "member2", "member3"])
        .await;

    // COPY set
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination has same members
    let card = client.command(&["SCARD", "dst"]).await;
    assert_eq!(card, Response::Integer(3));

    let is_member = client.command(&["SISMEMBER", "dst", "member1"]).await;
    assert_eq!(is_member, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_sorted_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create sorted set
    client
        .command(&["ZADD", "src", "1", "one", "2", "two", "3", "three"])
        .await;

    // COPY sorted set
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination has same members with scores
    let card = client.command(&["ZCARD", "dst"]).await;
    assert_eq!(card, Response::Integer(3));

    let score = client.command(&["ZSCORE", "dst", "two"]).await;
    match score {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert_eq!(s, "2");
        }
        _ => panic!("Expected bulk response for ZSCORE"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_db_option_rejected() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SET source key
    client.command(&["SET", "src", "hello"]).await;

    // COPY with DB option should be rejected (single database per instance)
    let response = client.command(&["COPY", "src", "dst", "DB", "1"]).await;
    match &response {
        Response::Error(err) => {
            let msg = String::from_utf8_lossy(err);
            assert!(msg.contains("not supported"), "Expected 'not supported' in error: {msg}");
            assert!(msg.contains("single database"), "Expected 'single database' in error: {msg}");
        }
        other => panic!("Expected error response, got: {other:?}"),
    }

    server.shutdown().await;
}

// ============================================================================
// RANDOMKEY Tests
// ============================================================================

#[tokio::test]
async fn test_randomkey_empty_database() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Empty database should return nil
    let response = client.command(&["RANDOMKEY"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_randomkey_single_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "only-key", "value"]).await;

    // Should always return the only key
    let response = client.command(&["RANDOMKEY"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("only-key"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_randomkey_multiple_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add multiple keys of different types
    client.command(&["SET", "str-key", "value"]).await;
    client
        .command(&["HSET", "hash-key", "field", "value"])
        .await;
    client.command(&["LPUSH", "list-key", "item"]).await;
    client.command(&["SADD", "set-key", "member"]).await;
    client.command(&["ZADD", "zset-key", "1", "member"]).await;

    let valid_keys = ["str-key", "hash-key", "list-key", "set-key", "zset-key"];

    // Run multiple times to verify randomness returns valid keys
    for _ in 0..10 {
        let response = client.command(&["RANDOMKEY"]).await;
        match response {
            Response::Bulk(Some(key)) => {
                let key_str = String::from_utf8_lossy(&key);
                assert!(
                    valid_keys.contains(&key_str.as_ref()),
                    "RANDOMKEY returned unexpected key: {}",
                    key_str
                );
            }
            _ => panic!("Expected bulk response, got {:?}", response),
        }
    }

    server.shutdown().await;
}
