//! Integration tests for FT.* (full-text search) commands.

use crate::common::response_helpers::{assert_ok, unwrap_array, unwrap_bulk, unwrap_integer};
use crate::common::test_server::{TestServer, TestServerConfig};
use frogdb_protocol::Response;
use std::time::Duration;

async fn start_server_no_persist() -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await
}

async fn start_multi_shard_server() -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(4),
        persistence: true,
        ..Default::default()
    })
    .await
}

// ============================================================================
// FT.CREATE
// ============================================================================

#[tokio::test]
async fn test_ft_create_basic() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
            "age",
            "NUMERIC",
            "tags",
            "TAG",
        ])
        .await;
    assert_ok(&response);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_create_duplicate() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    assert_ok(&response);

    // Creating same index again should fail
    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_create_invalid_schema() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Missing SCHEMA keyword
    let response = client
        .command(&["FT.CREATE", "idx", "ON", "HASH", "name", "TEXT"])
        .await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

// ============================================================================
// FT.SEARCH — text queries
// ============================================================================

#[tokio::test]
async fn test_ft_search_text() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Create docs first, then index
    client
        .command(&["HSET", "user:1", "name", "Alice Smith", "age", "30"])
        .await;
    client
        .command(&["HSET", "user:2", "name", "Bob Jones", "age", "25"])
        .await;
    client
        .command(&["HSET", "user:3", "name", "Alice Jones", "age", "35"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
            "age",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);

    // Wait for background indexing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Search for "Alice"
    let response = client.command(&["FT.SEARCH", "idx", "Alice"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 2, "Should find 2 docs matching 'Alice'");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_field_specific() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "user:1", "name", "Alice", "bio", "Engineer"])
        .await;
    client
        .command(&["HSET", "user:2", "name", "Bob", "bio", "Alice fan"])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
            "bio",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Search for Alice only in name field
    let response = client.command(&["FT.SEARCH", "idx", "@name:Alice"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1, "Only 1 doc has Alice in the name field");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_tag() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "item:1", "name", "Widget", "color", "red"])
        .await;
    client
        .command(&["HSET", "item:2", "name", "Gadget", "color", "blue"])
        .await;
    client
        .command(&["HSET", "item:3", "name", "Doohickey", "color", "red"])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "item:",
            "SCHEMA",
            "name",
            "TEXT",
            "color",
            "TAG",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Search for red items
    let response = client.command(&["FT.SEARCH", "idx", "@color:{red}"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 2, "Should find 2 red items");

    // Search for red OR blue
    let response = client
        .command(&["FT.SEARCH", "idx", "@color:{red|blue}"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 3, "Should find all 3 items");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_numeric_range() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "product:1", "name", "Cheap", "price", "10"])
        .await;
    client
        .command(&["HSET", "product:2", "name", "Mid", "price", "50"])
        .await;
    client
        .command(&["HSET", "product:3", "name", "Expensive", "price", "100"])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "product:",
            "SCHEMA",
            "name",
            "TEXT",
            "price",
            "NUMERIC",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Range [10 50]
    let response = client
        .command(&["FT.SEARCH", "idx", "@price:[10 50]"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 2, "Should find 2 products in price range [10, 50]");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_empty_result() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "user:1", "name", "Alice"]).await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client.command(&["FT.SEARCH", "idx", "nonexistent"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 0, "No matches expected");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_negation() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "user:1", "name", "Alice Smith"])
        .await;
    client
        .command(&["HSET", "user:2", "name", "Bob Smith"])
        .await;
    client
        .command(&["HSET", "user:3", "name", "Alice Jones"])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Search for Smith but NOT Alice
    let response = client.command(&["FT.SEARCH", "idx", "Smith -Alice"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1, "Should find only Bob Smith");

    server.shutdown().await;
}

// ============================================================================
// FT.SEARCH — options (NOCONTENT, WITHSCORES, LIMIT, RETURN)
// ============================================================================

#[tokio::test]
async fn test_ft_search_nocontent() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "user:1", "name", "Alice"]).await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .command(&["FT.SEARCH", "idx", "Alice", "NOCONTENT"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1);
    // With NOCONTENT, response is [total, key1, key2, ...] — no field arrays
    assert_eq!(arr.len(), 2); // total + 1 key
    let key = unwrap_bulk(&arr[1]);
    assert_eq!(key, b"user:1");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_limit() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    for i in 0..10 {
        client
            .command(&[
                "HSET",
                &format!("user:{}", i),
                "name",
                &format!("User{}", i),
                "group",
                "testgroup",
            ])
            .await;
    }

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
            "group",
            "TAG",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Search with LIMIT 0 3 — first 3 results
    let response = client
        .command(&["FT.SEARCH", "idx", "@group:{testgroup}", "LIMIT", "0", "3"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 10, "Total should report all matches");
    // arr = [total, key1, fields1, key2, fields2, key3, fields3]
    // That's 1 + 3*2 = 7 elements
    assert_eq!(arr.len(), 7, "Should have 3 results with fields");

    server.shutdown().await;
}

// ============================================================================
// FT.DROPINDEX
// ============================================================================

#[tokio::test]
async fn test_ft_dropindex() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;

    let response = client.command(&["FT.DROPINDEX", "idx"]).await;
    assert_ok(&response);

    // Searching dropped index should error
    let response = client.command(&["FT.SEARCH", "idx", "hello"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

// ============================================================================
// FT.INFO
// ============================================================================

#[tokio::test]
async fn test_ft_info() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "user:1", "name", "Alice", "age", "30"])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
            "age",
            "NUMERIC",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client.command(&["FT.INFO", "idx"]).await;
    let arr = unwrap_array(response);
    // Should contain index_name, index_definition, attributes, num_docs
    assert!(arr.len() >= 4, "FT.INFO should return key-value pairs");

    // First pair should be index_name
    assert_eq!(unwrap_bulk(&arr[0]), b"index_name");
    assert_eq!(unwrap_bulk(&arr[1]), b"idx");

    server.shutdown().await;
}

// ============================================================================
// FT._LIST
// ============================================================================

#[tokio::test]
async fn test_ft_list() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Initially empty
    let response = client.command(&["FT._LIST"]).await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 0);

    // Create two indexes
    client
        .command(&[
            "FT.CREATE",
            "idx1",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    client
        .command(&[
            "FT.CREATE",
            "idx2",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "product:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;

    let response = client.command(&["FT._LIST"]).await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 2, "Should have 2 indexes");

    let names: Vec<&[u8]> = arr.iter().map(|r| unwrap_bulk(r)).collect();
    assert!(names.contains(&b"idx1".as_slice()));
    assert!(names.contains(&b"idx2".as_slice()));

    server.shutdown().await;
}

// ============================================================================
// Write-path / live indexing
// ============================================================================

#[tokio::test]
async fn test_ft_live_index_hset() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Create index first, then add docs
    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;

    // HSET after FT.CREATE — should be live-indexed
    client.command(&["HSET", "user:1", "name", "Alice"]).await;
    // Wait for search commit
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let response = client.command(&["FT.SEARCH", "idx", "Alice"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1, "Live-indexed doc should be searchable");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_live_index_del() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "user:1", "name", "Alice"]).await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify indexed
    let response = client.command(&["FT.SEARCH", "idx", "Alice"]).await;
    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 1);

    // Delete the key
    client.command(&["DEL", "user:1"]).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Should no longer be found
    let response = client.command(&["FT.SEARCH", "idx", "Alice"]).await;
    let arr = unwrap_array(response);
    assert_eq!(
        unwrap_integer(&arr[0]),
        0,
        "Deleted doc should not be searchable"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_live_index_rename() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "user:1", "name", "Alice"]).await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Rename outside prefix — old key should be gone from index
    client.command(&["RENAME", "user:1", "other:1"]).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let response = client.command(&["FT.SEARCH", "idx", "Alice"]).await;
    let arr = unwrap_array(response);
    assert_eq!(
        unwrap_integer(&arr[0]),
        0,
        "Renamed-away doc should not be searchable"
    );

    server.shutdown().await;
}

// ============================================================================
// Background indexing (existing docs)
// ============================================================================

#[tokio::test]
async fn test_ft_create_indexes_existing() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Create 20 docs first
    for i in 0..20 {
        client
            .command(&[
                "HSET",
                &format!("user:{}", i),
                "name",
                &format!("User{}", i),
            ])
            .await;
    }

    // Now create the index — should background-index all 20
    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Search for a specific user
    let response = client.command(&["FT.SEARCH", "idx", "User5"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert!(
        total >= 1,
        "User5 should be found in background-indexed data"
    );

    server.shutdown().await;
}

// ============================================================================
// Multi-shard tests
// ============================================================================

#[tokio::test]
async fn test_ft_search_multi_shard() {
    let server = start_multi_shard_server().await;
    let mut client = server.connect().await;

    // Create many docs that will distribute across 4 shards
    for i in 0..20 {
        client
            .command(&[
                "HSET",
                &format!("user:{}", i),
                "name",
                "searchable",
                "id",
                &format!("{}", i),
            ])
            .await;
    }

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
            "id",
            "TAG",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Search should merge results from all shards
    let response = client
        .command(&["FT.SEARCH", "idx", "searchable", "LIMIT", "0", "100"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 20, "All 20 docs should be found across shards");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_multi_shard_limit() {
    let server = start_multi_shard_server().await;
    let mut client = server.connect().await;

    for i in 0..20 {
        client
            .command(&["HSET", &format!("user:{}", i), "name", "common"])
            .await;
    }

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // LIMIT 0 5 across shards
    let response = client
        .command(&["FT.SEARCH", "idx", "common", "LIMIT", "0", "5"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 20, "Total should reflect all matches");
    // 1 (total) + 5 * 2 (key + fields) = 11
    assert_eq!(arr.len(), 11, "Should return exactly 5 results");

    server.shutdown().await;
}

// ============================================================================
// Persistence / recovery
// ============================================================================

#[tokio::test]
async fn test_ft_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let config = TestServerConfig {
        persistence: true,
        data_dir: Some(tmp.path().to_path_buf()),
        num_shards: Some(1),
        ..Default::default()
    };

    // First boot: create index and add data
    {
        let server = TestServer::start_standalone_with_config(config.clone()).await;
        let mut client = server.connect().await;

        client.command(&["HSET", "user:1", "name", "Alice"]).await;

        client
            .command(&[
                "FT.CREATE",
                "idx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "user:",
                "SCHEMA",
                "name",
                "TEXT",
            ])
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify it works
        let response = client.command(&["FT.SEARCH", "idx", "Alice"]).await;
        let arr = unwrap_array(response);
        assert_eq!(unwrap_integer(&arr[0]), 1);

        drop(client);
        server.shutdown().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Second boot: verify index survived
    {
        let config = TestServerConfig {
            persistence: true,
            data_dir: Some(tmp.path().to_path_buf()),
            num_shards: Some(1),
            ..Default::default()
        };
        let server = TestServer::start_standalone_with_config(config).await;
        let mut client = server.connect().await;

        // Index should exist
        let response = client.command(&["FT._LIST"]).await;
        let arr = unwrap_array(response);
        assert_eq!(arr.len(), 1);
        assert_eq!(unwrap_bulk(&arr[0]), b"idx");

        // Data should still be searchable
        let response = client.command(&["FT.SEARCH", "idx", "Alice"]).await;
        let arr = unwrap_array(response);
        assert_eq!(
            unwrap_integer(&arr[0]),
            1,
            "Search index should survive restart"
        );

        server.shutdown().await;
    }
}

// ============================================================================
// Edge cases
// ============================================================================

#[tokio::test]
async fn test_ft_search_nonexistent_index() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client.command(&["FT.SEARCH", "nonexistent", "hello"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_multiple_indexes() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "user:1", "name", "Alice"]).await;
    client
        .command(&["HSET", "product:1", "name", "Widget"])
        .await;

    // Two indexes with different prefixes
    client
        .command(&[
            "FT.CREATE",
            "user_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    client
        .command(&[
            "FT.CREATE",
            "product_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "product:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // user_idx should only find Alice
    let response = client.command(&["FT.SEARCH", "user_idx", "Alice"]).await;
    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 1);

    // user_idx should NOT find Widget
    let response = client.command(&["FT.SEARCH", "user_idx", "Widget"]).await;
    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 0);

    // product_idx should find Widget
    let response = client
        .command(&["FT.SEARCH", "product_idx", "Widget"])
        .await;
    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 1);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_boolean_or() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "user:1", "name", "Alice"]).await;
    client.command(&["HSET", "user:2", "name", "Bob"]).await;
    client.command(&["HSET", "user:3", "name", "Charlie"]).await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "user:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // OR query
    let response = client.command(&["FT.SEARCH", "idx", "(Alice | Bob)"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 2, "Should find Alice and Bob");

    server.shutdown().await;
}
