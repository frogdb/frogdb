//! Integration tests for Count-Min Sketch (CMS.*) commands.

use bytes::Bytes;
use crate::common::response_helpers::{assert_error_prefix, assert_ok, unwrap_array, unwrap_bulk, unwrap_integer};
use crate::common::test_server::TestServer;
use frogdb_protocol::Response;

// ============================================================================
// CMS.INITBYDIM tests
// ============================================================================

#[tokio::test]
async fn test_cms_initbydim_and_info() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CMS.INITBYDIM", "{cms}key", "100", "5"]).await;
    assert_ok(&resp);

    let resp = client.command(&["CMS.INFO", "{cms}key"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_bulk(&arr[0]), b"width");
    assert_eq!(unwrap_integer(&arr[1]), 100);
    assert_eq!(unwrap_bulk(&arr[2]), b"depth");
    assert_eq!(unwrap_integer(&arr[3]), 5);
    assert_eq!(unwrap_bulk(&arr[4]), b"count");
    assert_eq!(unwrap_integer(&arr[5]), 0);

    server.shutdown().await;
}

#[tokio::test]
async fn test_cms_initbydim_duplicate_key_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CMS.INITBYDIM", "{cms}key", "100", "5"]).await;
    assert_ok(&resp);

    let resp = client.command(&["CMS.INITBYDIM", "{cms}key", "200", "3"]).await;
    assert_error_prefix(&resp, "ERR");

    server.shutdown().await;
}

// ============================================================================
// CMS.INITBYPROB tests
// ============================================================================

#[tokio::test]
async fn test_cms_initbyprob() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CMS.INITBYPROB", "{cms}key", "0.01", "0.001"])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["CMS.INFO", "{cms}key"]).await;
    let arr = unwrap_array(resp);
    // width = ceil(e / 0.01) = 272
    assert_eq!(unwrap_integer(&arr[1]), 272);
    // depth = ceil(ln(1/0.001)) = 7
    assert_eq!(unwrap_integer(&arr[3]), 7);
    assert_eq!(unwrap_integer(&arr[5]), 0);

    server.shutdown().await;
}

#[tokio::test]
async fn test_cms_initbyprob_invalid_params() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // error >= 1
    let resp = client
        .command(&["CMS.INITBYPROB", "{cms}k1", "1.0", "0.01"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // error <= 0
    let resp = client
        .command(&["CMS.INITBYPROB", "{cms}k2", "0", "0.01"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // probability >= 1
    let resp = client
        .command(&["CMS.INITBYPROB", "{cms}k3", "0.01", "1.0"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // probability <= 0
    let resp = client
        .command(&["CMS.INITBYPROB", "{cms}k4", "0.01", "0"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // negative values
    let resp = client
        .command(&["CMS.INITBYPROB", "{cms}k5", "-0.01", "0.01"])
        .await;
    assert_error_prefix(&resp, "ERR");

    server.shutdown().await;
}

// ============================================================================
// CMS.INCRBY tests
// ============================================================================

#[tokio::test]
async fn test_cms_incrby_and_query_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "{cms}key", "100", "5"])
        .await;

    let resp = client
        .command(&["CMS.INCRBY", "{cms}key", "apple", "3", "banana", "7"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    // Returns updated counts
    assert!(unwrap_integer(&arr[0]) >= 3);
    assert!(unwrap_integer(&arr[1]) >= 7);

    // Query
    let resp = client
        .command(&["CMS.QUERY", "{cms}key", "apple", "banana", "nonexistent"])
        .await;
    let arr = unwrap_array(resp);
    assert!(unwrap_integer(&arr[0]) >= 3);
    assert!(unwrap_integer(&arr[1]) >= 7);
    assert_eq!(unwrap_integer(&arr[2]), 0);

    // Increment again
    let resp = client
        .command(&["CMS.INCRBY", "{cms}key", "apple", "2"])
        .await;
    let arr = unwrap_array(resp);
    assert!(unwrap_integer(&arr[0]) >= 5);

    server.shutdown().await;
}

#[tokio::test]
async fn test_cms_incrby_missing_key_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CMS.INCRBY", "{cms}missing", "item", "1"])
        .await;
    assert_error_prefix(&resp, "ERR");

    server.shutdown().await;
}

#[tokio::test]
async fn test_cms_incrby_wrong_type_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{cms}str", "hello"]).await;

    let resp = client
        .command(&["CMS.INCRBY", "{cms}str", "item", "1"])
        .await;
    assert_error_prefix(&resp, "WRONGTYPE");

    server.shutdown().await;
}

#[tokio::test]
async fn test_cms_incrby_odd_args_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "{cms}key", "100", "5"])
        .await;

    let resp = client
        .command(&["CMS.INCRBY", "{cms}key", "apple", "3", "banana"])
        .await;
    assert_error_prefix(&resp, "ERR");

    server.shutdown().await;
}

// ============================================================================
// CMS.QUERY tests
// ============================================================================

#[tokio::test]
async fn test_cms_query_missing_key_returns_zeros() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CMS.QUERY", "{cms}missing", "a", "b", "c"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 3);
    assert_eq!(unwrap_integer(&arr[0]), 0);
    assert_eq!(unwrap_integer(&arr[1]), 0);
    assert_eq!(unwrap_integer(&arr[2]), 0);

    server.shutdown().await;
}

// ============================================================================
// CMS.MERGE tests
// ============================================================================

#[tokio::test]
async fn test_cms_merge_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create two sketches with same dimensions
    client
        .command(&["CMS.INITBYDIM", "{cms}src1", "100", "5"])
        .await;
    client
        .command(&["CMS.INITBYDIM", "{cms}src2", "100", "5"])
        .await;

    // Add data
    client
        .command(&["CMS.INCRBY", "{cms}src1", "apple", "3"])
        .await;
    client
        .command(&["CMS.INCRBY", "{cms}src2", "apple", "7"])
        .await;
    client
        .command(&["CMS.INCRBY", "{cms}src2", "banana", "5"])
        .await;

    // Merge into dest
    let resp = client
        .command(&["CMS.MERGE", "{cms}dest", "2", "{cms}src1", "{cms}src2"])
        .await;
    assert_ok(&resp);

    // Query merged result
    let resp = client
        .command(&["CMS.QUERY", "{cms}dest", "apple", "banana"])
        .await;
    let arr = unwrap_array(resp);
    assert!(unwrap_integer(&arr[0]) >= 10); // 3 + 7
    assert!(unwrap_integer(&arr[1]) >= 5);

    server.shutdown().await;
}

#[tokio::test]
async fn test_cms_merge_with_weights() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "{cms}src1", "100", "5"])
        .await;
    client
        .command(&["CMS.INITBYDIM", "{cms}src2", "100", "5"])
        .await;

    client
        .command(&["CMS.INCRBY", "{cms}src1", "apple", "3"])
        .await;
    client
        .command(&["CMS.INCRBY", "{cms}src2", "apple", "7"])
        .await;

    // Merge with weights: src1*2 + src2*3
    let resp = client
        .command(&[
            "CMS.MERGE",
            "{cms}dest",
            "2",
            "{cms}src1",
            "{cms}src2",
            "WEIGHTS",
            "2",
            "3",
        ])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["CMS.QUERY", "{cms}dest", "apple"]).await;
    let arr = unwrap_array(resp);
    // 3*2 + 7*3 = 6 + 21 = 27
    assert!(unwrap_integer(&arr[0]) >= 27);

    server.shutdown().await;
}

#[tokio::test]
async fn test_cms_merge_dimension_mismatch_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "{cms}src1", "100", "5"])
        .await;
    client
        .command(&["CMS.INITBYDIM", "{cms}src2", "200", "5"])
        .await;

    let resp = client
        .command(&["CMS.MERGE", "{cms}dest", "2", "{cms}src1", "{cms}src2"])
        .await;
    assert_error_prefix(&resp, "ERR");

    server.shutdown().await;
}

#[tokio::test]
async fn test_cms_merge_dest_is_source() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "{cms}key1", "100", "5"])
        .await;
    client
        .command(&["CMS.INITBYDIM", "{cms}key2", "100", "5"])
        .await;

    client
        .command(&["CMS.INCRBY", "{cms}key1", "apple", "3"])
        .await;
    client
        .command(&["CMS.INCRBY", "{cms}key2", "apple", "7"])
        .await;

    // Merge into key1 (which is also a source)
    let resp = client
        .command(&["CMS.MERGE", "{cms}key1", "2", "{cms}key1", "{cms}key2"])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["CMS.QUERY", "{cms}key1", "apple"]).await;
    let arr = unwrap_array(resp);
    assert!(unwrap_integer(&arr[0]) >= 10); // 3 + 7

    server.shutdown().await;
}

// ============================================================================
// CMS.INFO tests
// ============================================================================

#[tokio::test]
async fn test_cms_info_missing_key_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CMS.INFO", "{cms}missing"]).await;
    assert_error_prefix(&resp, "ERR");

    server.shutdown().await;
}

// ============================================================================
// TYPE command test
// ============================================================================

#[tokio::test]
async fn test_cms_type_command() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "{cms}key", "100", "5"])
        .await;

    let resp = client.command(&["TYPE", "{cms}key"]).await;
    assert_eq!(resp, Response::Simple(Bytes::from("cms")));

    server.shutdown().await;
}
