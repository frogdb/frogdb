//! Integration tests for Top-K (TOPK.*) commands.

use crate::common::response_helpers::{
    assert_error_prefix, assert_ok, unwrap_array, unwrap_bulk, unwrap_integer,
};
use crate::common::test_server::TestServer;
use frogdb_protocol::Response;

// ============================================================================
// TOPK.RESERVE tests
// ============================================================================

#[tokio::test]
async fn test_topk_reserve_defaults() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TOPK.RESERVE", "{tk}key", "10"]).await;
    assert_ok(&resp);

    // Verify with INFO
    let resp = client.command(&["TOPK.INFO", "{tk}key"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_bulk(&arr[0]), b"k");
    assert_eq!(unwrap_integer(&arr[1]), 10);
    assert_eq!(unwrap_bulk(&arr[2]), b"width");
    assert_eq!(unwrap_integer(&arr[3]), 80); // 10 * 8
    assert_eq!(unwrap_bulk(&arr[4]), b"depth");
    assert_eq!(unwrap_integer(&arr[5]), 7);
    assert_eq!(unwrap_bulk(&arr[6]), b"decay");
    assert_eq!(unwrap_bulk(&arr[7]), b"0.9");

    server.shutdown().await;
}

#[tokio::test]
async fn test_topk_reserve_custom_params() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["TOPK.RESERVE", "{tk}key", "5", "50", "3", "0.92"])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["TOPK.INFO", "{tk}key"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[1]), 5);
    assert_eq!(unwrap_integer(&arr[3]), 50);
    assert_eq!(unwrap_integer(&arr[5]), 3);
    assert_eq!(unwrap_bulk(&arr[7]), b"0.92");

    server.shutdown().await;
}

#[tokio::test]
async fn test_topk_reserve_duplicate_key_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TOPK.RESERVE", "{tk}key", "5"]).await;
    assert_ok(&resp);

    let resp = client.command(&["TOPK.RESERVE", "{tk}key", "10"]).await;
    assert_error_prefix(&resp, "ERR");

    server.shutdown().await;
}

// ============================================================================
// TOPK.ADD tests
// ============================================================================

#[tokio::test]
async fn test_topk_add_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "{tk}key", "3"]).await;

    let resp = client
        .command(&["TOPK.ADD", "{tk}key", "apple", "banana", "cherry"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 3);
    // Each result is either null (no expulsion) or a bulk string (expelled item)
    for item in &arr {
        assert!(
            matches!(item, Response::Null | Response::Bulk(_)),
            "unexpected response: {item:?}"
        );
    }

    // All three items should be queryable (they fit in k=3)
    let resp = client
        .command(&["TOPK.QUERY", "{tk}key", "apple", "banana", "cherry"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);
    assert_eq!(unwrap_integer(&arr[1]), 1);
    assert_eq!(unwrap_integer(&arr[2]), 1);

    server.shutdown().await;
}

#[tokio::test]
async fn test_topk_add_missing_key_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TOPK.ADD", "{tk}nokey", "item"]).await;
    assert_error_prefix(&resp, "ERR");

    server.shutdown().await;
}

#[tokio::test]
async fn test_topk_add_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{tk}str", "hello"]).await;

    let resp = client.command(&["TOPK.ADD", "{tk}str", "item"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");

    server.shutdown().await;
}

// ============================================================================
// TOPK.QUERY and TOPK.COUNT tests
// ============================================================================

#[tokio::test]
async fn test_topk_query_and_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "{tk}key", "5"]).await;
    client
        .command(&["TOPK.ADD", "{tk}key", "apple", "banana", "apple"])
        .await;

    // Query
    let resp = client
        .command(&["TOPK.QUERY", "{tk}key", "apple", "banana", "missing"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1); // apple in top-k
    assert_eq!(unwrap_integer(&arr[1]), 1); // banana in top-k
    assert_eq!(unwrap_integer(&arr[2]), 0); // missing not in top-k

    // Count
    let resp = client
        .command(&["TOPK.COUNT", "{tk}key", "apple", "banana", "missing"])
        .await;
    let arr = unwrap_array(resp);
    assert!(unwrap_integer(&arr[0]) >= 2); // apple added twice
    assert!(unwrap_integer(&arr[1]) >= 1); // banana added once
    assert_eq!(unwrap_integer(&arr[2]), 0); // missing

    server.shutdown().await;
}

#[tokio::test]
async fn test_topk_query_missing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TOPK.QUERY", "{tk}nokey", "a", "b"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 0);
    assert_eq!(unwrap_integer(&arr[1]), 0);

    let resp = client.command(&["TOPK.COUNT", "{tk}nokey", "a", "b"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 0);
    assert_eq!(unwrap_integer(&arr[1]), 0);

    server.shutdown().await;
}

// ============================================================================
// TOPK.INCRBY tests
// ============================================================================

#[tokio::test]
async fn test_topk_incrby() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "{tk}key", "5"]).await;

    let resp = client
        .command(&["TOPK.INCRBY", "{tk}key", "apple", "10", "banana", "5"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);

    // Verify counts
    let resp = client
        .command(&["TOPK.COUNT", "{tk}key", "apple", "banana"])
        .await;
    let arr = unwrap_array(resp);
    assert!(unwrap_integer(&arr[0]) >= 10);
    assert!(unwrap_integer(&arr[1]) >= 5);

    server.shutdown().await;
}

#[tokio::test]
async fn test_topk_incrby_invalid_increment() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "{tk}key", "5"]).await;

    // Zero increment
    let resp = client
        .command(&["TOPK.INCRBY", "{tk}key", "apple", "0"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Too large
    let resp = client
        .command(&["TOPK.INCRBY", "{tk}key", "apple", "100001"])
        .await;
    assert_error_prefix(&resp, "ERR");

    server.shutdown().await;
}

// ============================================================================
// TOPK.LIST tests
// ============================================================================

#[tokio::test]
async fn test_topk_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "{tk}key", "5"]).await;
    client
        .command(&["TOPK.INCRBY", "{tk}key", "a", "10", "b", "5", "c", "1"])
        .await;

    // Without WITHCOUNT
    let resp = client.command(&["TOPK.LIST", "{tk}key"]).await;
    let arr = unwrap_array(resp);
    assert!(!arr.is_empty());

    // With WITHCOUNT
    let resp = client.command(&["TOPK.LIST", "{tk}key", "WITHCOUNT"]).await;
    let arr = unwrap_array(resp);
    // Should be pairs: item, count, item, count, ...
    assert!(arr.len() >= 2);
    assert!(arr.len() % 2 == 0);

    server.shutdown().await;
}

#[tokio::test]
async fn test_topk_list_missing_key_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TOPK.LIST", "{tk}nokey"]).await;
    assert_error_prefix(&resp, "ERR");

    server.shutdown().await;
}

// ============================================================================
// TOPK.INFO tests
// ============================================================================

#[tokio::test]
async fn test_topk_info_missing_key_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TOPK.INFO", "{tk}nokey"]).await;
    assert_error_prefix(&resp, "ERR");

    server.shutdown().await;
}

// ============================================================================
// Expulsion behavior
// ============================================================================

#[tokio::test]
async fn test_topk_expulsion_behavior() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Small k to force expulsions
    client
        .command(&["TOPK.RESERVE", "{tk}key", "2", "8", "4", "0.9"])
        .await;

    // Add two high-frequency items
    for _ in 0..20 {
        client.command(&["TOPK.ADD", "{tk}key", "high"]).await;
    }
    for _ in 0..15 {
        client.command(&["TOPK.ADD", "{tk}key", "medium"]).await;
    }

    // Both should be in the top-k
    let resp = client
        .command(&["TOPK.QUERY", "{tk}key", "high", "medium"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);
    assert_eq!(unwrap_integer(&arr[1]), 1);

    // Add a low-frequency third item — shouldn't displace the top 2
    client.command(&["TOPK.ADD", "{tk}key", "low"]).await;

    let resp = client
        .command(&["TOPK.QUERY", "{tk}key", "high", "medium"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);
    assert_eq!(unwrap_integer(&arr[1]), 1);

    server.shutdown().await;
}
