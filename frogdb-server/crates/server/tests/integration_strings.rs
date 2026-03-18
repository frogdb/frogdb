//! Integration tests for string commands (LCS, DIGEST, DELEX, MSETEX).

use crate::common::test_server::TestServer;
use bytes::Bytes;
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

// ============================================================================
// DIGEST Tests
// ============================================================================

#[tokio::test]
async fn test_digest_returns_hex_hash() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    let response = client.command(&["DIGEST", "mykey"]).await;

    // Should return a 16-char hex string
    match &response {
        Response::Bulk(Some(b)) => {
            assert_eq!(b.len(), 16, "digest should be 16 hex chars");
            assert!(
                b.iter().all(|&c| c.is_ascii_hexdigit()),
                "digest should be hex"
            );
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_digest_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["DIGEST", "nokey"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_digest_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["LPUSH", "mylist", "a"]).await;
    let response = client.command(&["DIGEST", "mylist"]).await;
    match response {
        Response::Error(_) => {}
        _ => panic!("Expected error for wrong type, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_digest_deterministic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "k1", "testvalue"]).await;
    let r1 = client.command(&["DIGEST", "k1"]).await;

    // Set same value again and verify same digest
    client.command(&["SET", "k2", "testvalue"]).await;
    let r2 = client.command(&["DIGEST", "k2"]).await;

    assert_eq!(r1, r2, "same value should produce same digest");

    server.shutdown().await;
}

// ============================================================================
// DELEX Tests
// ============================================================================

#[tokio::test]
async fn test_delex_no_condition() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    let response = client.command(&["DELEX", "mykey"]).await;
    assert_eq!(response, Response::Integer(1));

    // Key should be gone
    let response = client.command(&["GET", "mykey"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_delex_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["DELEX", "nokey"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_delex_no_condition_any_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Should work on non-string types when no condition is given
    client.command(&["LPUSH", "mylist", "a"]).await;
    let response = client.command(&["DELEX", "mylist"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_delex_ifeq_match() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    let response = client.command(&["DELEX", "mykey", "IFEQ", "hello"]).await;
    assert_eq!(response, Response::Integer(1));

    let response = client.command(&["GET", "mykey"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_delex_ifeq_no_match() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    let response = client.command(&["DELEX", "mykey", "IFEQ", "world"]).await;
    assert_eq!(response, Response::Integer(0));

    // Key should still exist
    let response = client.command(&["GET", "mykey"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("hello"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_delex_ifne_match() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    let response = client.command(&["DELEX", "mykey", "IFNE", "world"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_delex_ifdeq_match() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;

    // Get the digest first
    let digest_resp = client.command(&["DIGEST", "mykey"]).await;
    let digest = match &digest_resp {
        Response::Bulk(Some(b)) => std::str::from_utf8(b).unwrap().to_string(),
        _ => panic!("Expected bulk digest"),
    };

    let response = client
        .command(&["DELEX", "mykey", "IFDEQ", &digest])
        .await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_delex_ifdne_match() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    let response = client
        .command(&["DELEX", "mykey", "IFDNE", "0000000000000000"])
        .await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_delex_wrong_type_with_condition() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["LPUSH", "mylist", "a"]).await;
    let response = client
        .command(&["DELEX", "mylist", "IFEQ", "a"])
        .await;
    match response {
        Response::Error(_) => {}
        _ => panic!("Expected WRONGTYPE error, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// MSETEX Tests
// ============================================================================

#[tokio::test]
async fn test_msetex_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["MSETEX", "2", "{t}k1", "v1", "{t}k2", "v2", "EX", "100"])
        .await;
    assert_eq!(response, Response::Integer(1));

    let r1 = client.command(&["GET", "{t}k1"]).await;
    assert_eq!(r1, Response::Bulk(Some(Bytes::from("v1"))));
    let r2 = client.command(&["GET", "{t}k2"]).await;
    assert_eq!(r2, Response::Bulk(Some(Bytes::from("v2"))));

    // Should have TTLs
    let ttl1 = client.command(&["TTL", "{t}k1"]).await;
    match ttl1 {
        Response::Integer(t) => assert!(t > 0 && t <= 100, "TTL should be positive, got {t}"),
        _ => panic!("Expected integer TTL"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_msetex_nx_none_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["MSETEX", "2", "{t}k1", "v1", "{t}k2", "v2", "NX"])
        .await;
    assert_eq!(response, Response::Integer(1));

    let r1 = client.command(&["GET", "{t}k1"]).await;
    assert_eq!(r1, Response::Bulk(Some(Bytes::from("v1"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_msetex_nx_some_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}k1", "existing"]).await;

    let response = client
        .command(&["MSETEX", "2", "{t}k1", "v1", "{t}k2", "v2", "NX"])
        .await;
    assert_eq!(response, Response::Integer(0));

    // k1 should be unchanged, k2 should not exist
    let r1 = client.command(&["GET", "{t}k1"]).await;
    assert_eq!(r1, Response::Bulk(Some(Bytes::from("existing"))));
    let r2 = client.command(&["GET", "{t}k2"]).await;
    assert_eq!(r2, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_msetex_xx_all_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}k1", "old1"]).await;
    client.command(&["SET", "{t}k2", "old2"]).await;

    let response = client
        .command(&["MSETEX", "2", "{t}k1", "new1", "{t}k2", "new2", "XX"])
        .await;
    assert_eq!(response, Response::Integer(1));

    let r1 = client.command(&["GET", "{t}k1"]).await;
    assert_eq!(r1, Response::Bulk(Some(Bytes::from("new1"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_msetex_xx_some_missing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}k1", "old1"]).await;

    let response = client
        .command(&["MSETEX", "2", "{t}k1", "new1", "{t}k2", "new2", "XX"])
        .await;
    assert_eq!(response, Response::Integer(0));

    // k1 should be unchanged
    let r1 = client.command(&["GET", "{t}k1"]).await;
    assert_eq!(r1, Response::Bulk(Some(Bytes::from("old1"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_msetex_px_milliseconds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["MSETEX", "1", "k1", "v1", "PX", "100000"])
        .await;
    assert_eq!(response, Response::Integer(1));

    let pttl = client.command(&["PTTL", "k1"]).await;
    match pttl {
        Response::Integer(t) => {
            assert!(t > 0 && t <= 100000, "PTTL should be positive, got {t}")
        }
        _ => panic!("Expected integer PTTL"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_msetex_keepttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set with a TTL
    client.command(&["SET", "k1", "old", "EX", "200"]).await;

    // MSETEX with KEEPTTL should preserve the TTL
    let response = client
        .command(&["MSETEX", "1", "k1", "new", "KEEPTTL"])
        .await;
    assert_eq!(response, Response::Integer(1));

    let r1 = client.command(&["GET", "k1"]).await;
    assert_eq!(r1, Response::Bulk(Some(Bytes::from("new"))));

    let ttl = client.command(&["TTL", "k1"]).await;
    match ttl {
        Response::Integer(t) => assert!(t > 0 && t <= 200, "TTL should be preserved, got {t}"),
        _ => panic!("Expected integer TTL"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_msetex_wrong_numkeys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // numkeys=3 but only 2 key-value pairs provided
    let response = client
        .command(&["MSETEX", "3", "{t}k1", "v1", "{t}k2", "v2"])
        .await;
    match response {
        Response::Error(_) => {}
        _ => panic!("Expected error for wrong numkeys, got {:?}", response),
    }

    server.shutdown().await;
}
