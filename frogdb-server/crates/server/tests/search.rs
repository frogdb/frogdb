//! Integration tests for FT.* (full-text search) commands.

use crate::common::response_helpers::{assert_ok, unwrap_array, unwrap_bulk, unwrap_integer};
use crate::common::test_server::{TestServer, TestServerConfig};
use bytes::Bytes;
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
// FT.ALTER
// ============================================================================

#[tokio::test]
async fn test_ft_alter_add_field() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Create index with just name TEXT
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

    // Add data with a field not yet in the schema
    client
        .command(&["HSET", "user:1", "name", "Alice", "age", "30"])
        .await;

    // Alter index to add age NUMERIC SORTABLE
    let response = client
        .command(&[
            "FT.ALTER", "idx", "SCHEMA", "ADD", "age", "NUMERIC", "SORTABLE",
        ])
        .await;
    assert_ok(&response);

    // Wait for re-indexing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Search by numeric range on the new field
    let response = client.command(&["FT.SEARCH", "idx", "@age:[25 35]"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1);

    // FT.INFO should show both fields
    let response = client.command(&["FT.INFO", "idx"]).await;
    let arr = unwrap_array(response);
    // Find attributes
    let mut found_attrs = false;
    for i in 0..arr.len() {
        if let Response::Bulk(Some(ref b)) = arr[i]
            && b.as_ref() == b"attributes"
            && i + 1 < arr.len()
            && let Response::Array(ref attrs) = arr[i + 1]
        {
            assert_eq!(attrs.len(), 2); // name + age
            found_attrs = true;
        }
    }
    assert!(found_attrs, "Should find attributes in FT.INFO");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_alter_nonexistent_index() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["FT.ALTER", "noindex", "SCHEMA", "ADD", "field", "TEXT"])
        .await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_alter_duplicate_field() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "name", "TEXT"])
        .await;
    assert_ok(&response);

    // Try to add a field that already exists
    let response = client
        .command(&["FT.ALTER", "idx", "SCHEMA", "ADD", "name", "TAG"])
        .await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_alter_multi_shard() {
    let server = start_multi_shard_server().await;
    let mut client = server.connect().await;

    // Create index
    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;
    assert_ok(&response);

    // Add docs across shards
    for i in 0..10 {
        client
            .command(&[
                "HSET",
                &format!("doc:{}", i),
                "title",
                &format!("document {}", i),
                "category",
                "books",
            ])
            .await;
    }

    // Alter to add category TAG
    let response = client
        .command(&["FT.ALTER", "idx", "SCHEMA", "ADD", "category", "TAG"])
        .await;
    assert_ok(&response);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Search by new field
    let response = client
        .command(&["FT.SEARCH", "idx", "@category:{books}", "LIMIT", "0", "20"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 10);

    server.shutdown().await;
}

// ============================================================================
// FT.SEARCH — HIGHLIGHT
// ============================================================================

#[tokio::test]
async fn test_ft_search_highlight_default_tags() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "hello world greeting"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;
    assert_ok(&response);

    let response = client
        .command(&["FT.SEARCH", "idx", "hello", "HIGHLIGHT"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1);

    // Check that the title field contains <b> tags
    if let Response::Array(ref fields) = arr[2] {
        let field_name = unwrap_bulk(&fields[0]);
        assert_eq!(field_name, b"title");
        let field_val = unwrap_bulk(&fields[1]);
        let val_str = std::str::from_utf8(field_val).unwrap();
        assert!(
            val_str.contains("<b>"),
            "Expected <b> in highlighted output, got: {}",
            val_str
        );
    } else {
        panic!("Expected field array");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_highlight_custom_tags() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "hello world greeting"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;
    assert_ok(&response);

    let response = client
        .command(&["FT.SEARCH", "idx", "hello", "HIGHLIGHT", "TAGS", "[", "]"])
        .await;
    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 1);

    if let Response::Array(ref fields) = arr[2] {
        let field_val = unwrap_bulk(&fields[1]);
        let val_str = std::str::from_utf8(field_val).unwrap();
        assert!(
            val_str.contains("[") && val_str.contains("]"),
            "Expected custom tags in highlighted output, got: {}",
            val_str
        );
        assert!(
            !val_str.contains("<b>"),
            "Should not contain default <b> tags, got: {}",
            val_str
        );
    } else {
        panic!("Expected field array");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_highlight_specific_fields() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "HSET",
            "doc:1",
            "title",
            "hello world",
            "body",
            "hello there",
        ])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "body",
            "TEXT",
        ])
        .await;
    assert_ok(&response);

    // Only highlight the title field
    let response = client
        .command(&[
            "FT.SEARCH",
            "idx",
            "hello",
            "HIGHLIGHT",
            "FIELDS",
            "1",
            "title",
        ])
        .await;
    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 1);

    if let Response::Array(ref fields) = arr[2] {
        // Find title and body values
        let mut title_highlighted = false;
        let mut body_not_highlighted = false;
        for chunk in fields.chunks(2) {
            let name = std::str::from_utf8(unwrap_bulk(&chunk[0])).unwrap();
            let val = std::str::from_utf8(unwrap_bulk(&chunk[1])).unwrap();
            if name == "title" {
                title_highlighted = val.contains("<b>");
            }
            if name == "body" {
                body_not_highlighted = !val.contains("<b>");
            }
        }
        assert!(title_highlighted, "title should be highlighted");
        assert!(body_not_highlighted, "body should NOT be highlighted");
    } else {
        panic!("Expected field array");
    }

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

    let names: Vec<&[u8]> = arr.iter().map(unwrap_bulk).collect();
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
        tokio::time::sleep(Duration::from_millis(500)).await;
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
// Snapshot coordination
// ============================================================================

/// Test that BGSAVE can be triggered while search indexes exist and that
/// the search data remains intact after the snapshot process runs.
#[tokio::test]
async fn test_ft_search_bgsave_flushes_search() {
    let tmp = tempfile::tempdir().unwrap();
    let config = TestServerConfig {
        persistence: true,
        data_dir: Some(tmp.path().to_path_buf()),
        num_shards: Some(1),
        ..Default::default()
    };

    let server = TestServer::start_standalone_with_config(config.clone()).await;
    let mut client = server.connect().await;

    // Create index and add data
    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;
    client
        .command(&["HSET", "doc:1", "title", "snapshot search test"])
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify search works
    let response = client.command(&["FT.SEARCH", "idx", "snapshot"]).await;
    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 1);

    // Trigger BGSAVE — should flush search indexes before snapshotting
    let bgsave_resp = client.command(&["BGSAVE"]).await;
    match &bgsave_resp {
        Response::Simple(msg) => {
            let msg_str = String::from_utf8_lossy(msg);
            assert!(
                msg_str.contains("Background saving started")
                    || msg_str.contains("already in progress"),
                "Unexpected BGSAVE response: {msg_str}"
            );
        }
        _ => panic!("Expected simple string response for BGSAVE, got: {bgsave_resp:?}"),
    }

    // Wait for snapshot to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Search should still work after BGSAVE
    let response = client.command(&["FT.SEARCH", "idx", "snapshot"]).await;
    let arr = unwrap_array(response);
    assert_eq!(
        unwrap_integer(&arr[0]),
        1,
        "Search should still work after BGSAVE"
    );

    // Verify the live search index directory exists in data_dir
    let search_dir = tmp.path().join("search").join("idx").join("shard_0");
    assert!(
        search_dir.exists(),
        "Live search index should exist at {}",
        search_dir.display()
    );

    drop(client);
    server.shutdown().await;
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

// ============================================================================
// Phase 2: SORTBY
// ============================================================================

#[tokio::test]
async fn test_ft_search_sortby_numeric_asc() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "product:1", "name", "Widget", "price", "50"])
        .await;
    client
        .command(&["HSET", "product:2", "name", "Gadget", "price", "10"])
        .await;
    client
        .command(&["HSET", "product:3", "name", "Doohickey", "price", "100"])
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
            "SORTABLE",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .command(&["FT.SEARCH", "idx", "*", "SORTBY", "price", "ASC"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 3);

    // First result should be cheapest (price=10)
    let key1 = unwrap_bulk(&arr[1]);
    assert_eq!(key1, b"product:2", "Cheapest product should be first");

    // Last result should be most expensive (price=100)
    let key3 = unwrap_bulk(&arr[5]);
    assert_eq!(key3, b"product:3", "Most expensive product should be last");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_sortby_numeric_desc() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "product:1", "name", "Widget", "price", "50"])
        .await;
    client
        .command(&["HSET", "product:2", "name", "Gadget", "price", "10"])
        .await;
    client
        .command(&["HSET", "product:3", "name", "Doohickey", "price", "100"])
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
            "SORTABLE",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .command(&["FT.SEARCH", "idx", "*", "SORTBY", "price", "DESC"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 3);

    // First result should be most expensive (price=100)
    let key1 = unwrap_bulk(&arr[1]);
    assert_eq!(key1, b"product:3", "Most expensive product should be first");

    // Last result should be cheapest (price=10)
    let key3 = unwrap_bulk(&arr[5]);
    assert_eq!(key3, b"product:2", "Cheapest product should be last");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_sortby_multi_shard() {
    let server = start_multi_shard_server().await;
    let mut client = server.connect().await;

    for i in 0..12 {
        client
            .command(&[
                "HSET",
                &format!("item:{}", i),
                "name",
                "product",
                "score",
                &format!("{}", i * 10),
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
            "item:",
            "SCHEMA",
            "name",
            "TEXT",
            "score",
            "NUMERIC",
            "SORTABLE",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .command(&[
            "FT.SEARCH",
            "idx",
            "*",
            "SORTBY",
            "score",
            "ASC",
            "LIMIT",
            "0",
            "12",
        ])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 12);

    // Verify ascending order by checking first and last keys' score fields
    // arr layout: [total, key1, fields1, key2, fields2, ...]
    // Extract score from first result's fields
    let fields1 = unwrap_array(arr[2].clone());
    let mut score_first = None;
    for i in (0..fields1.len()).step_by(2) {
        if unwrap_bulk(&fields1[i]) == b"score" {
            score_first = Some(
                std::str::from_utf8(unwrap_bulk(&fields1[i + 1]))
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
            );
        }
    }
    let fields_last = unwrap_array(arr[24].clone());
    let mut score_last = None;
    for i in (0..fields_last.len()).step_by(2) {
        if unwrap_bulk(&fields_last[i]) == b"score" {
            score_last = Some(
                std::str::from_utf8(unwrap_bulk(&fields_last[i + 1]))
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
            );
        }
    }
    assert!(
        score_first.unwrap() <= score_last.unwrap(),
        "Results should be in ascending score order"
    );

    server.shutdown().await;
}

// ============================================================================
// Phase 2: Phrase queries
// ============================================================================

#[tokio::test]
async fn test_ft_search_phrase() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "quick brown fox"])
        .await;
    client
        .command(&["HSET", "doc:2", "title", "brown quick fox"])
        .await;
    client
        .command(&["HSET", "doc:3", "title", "the quick brown fox jumps"])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Exact phrase "quick brown" should match doc:1 and doc:3 (both have "quick brown" in order)
    let response = client
        .command(&["FT.SEARCH", "idx", "\"quick brown\""])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(
        total, 2,
        "Phrase 'quick brown' should match 2 docs (not 'brown quick')"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_phrase_field_specific() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "HSET",
            "doc:1",
            "title",
            "quick brown fox",
            "body",
            "something else",
        ])
        .await;
    client
        .command(&[
            "HSET",
            "doc:2",
            "title",
            "other title",
            "body",
            "quick brown fox",
        ])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "body",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Phrase restricted to title field
    let response = client
        .command(&["FT.SEARCH", "idx", "@title:\"quick brown\""])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1, "Only doc:1 has phrase in title field");

    server.shutdown().await;
}

// ============================================================================
// Phase 2: Prefix matching
// ============================================================================

#[tokio::test]
async fn test_ft_search_prefix() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "name", "helicopter"])
        .await;
    client.command(&["HSET", "doc:2", "name", "hello"]).await;
    client.command(&["HSET", "doc:3", "name", "world"]).await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client.command(&["FT.SEARCH", "idx", "hel*"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 2, "Prefix 'hel*' should match helicopter and hello");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_search_prefix_field_specific() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "name", "helicopter", "tag", "heavy"])
        .await;
    client
        .command(&["HSET", "doc:2", "name", "world", "tag", "hello"])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "name",
            "TEXT",
            "tag",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Prefix restricted to name field
    let response = client.command(&["FT.SEARCH", "idx", "@name:hel*"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1, "Only doc:1 has 'hel*' in name field");

    server.shutdown().await;
}

// ============================================================================
// Phase 2: Fuzzy matching
// ============================================================================

#[tokio::test]
async fn test_ft_search_fuzzy() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "doc:1", "name", "hello"]).await;
    client.command(&["HSET", "doc:2", "name", "world"]).await;
    client.command(&["HSET", "doc:3", "name", "hallo"]).await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Fuzzy distance 1: %hello% should match "hello" exactly and "hallo" (1 edit)
    let response = client.command(&["FT.SEARCH", "idx", "%hello%"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert!(total >= 2, "Fuzzy should match 'hello' and 'hallo'");

    server.shutdown().await;
}

// ============================================================================
// Phase 2: Exclusive numeric ranges
// ============================================================================

#[tokio::test]
async fn test_ft_search_exclusive_numeric_range() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "item:1", "name", "A", "score", "10"])
        .await;
    client
        .command(&["HSET", "item:2", "name", "B", "score", "50"])
        .await;
    client
        .command(&["HSET", "item:3", "name", "C", "score", "100"])
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
            "score",
            "NUMERIC",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Exclusive min: (10 100] — should exclude score=10
    let response = client
        .command(&["FT.SEARCH", "idx", "@score:[(10 100]"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(
        total, 2,
        "Exclusive min (10 should exclude item with score=10"
    );

    // Exclusive max: [10 (100] — should exclude score=100
    let response = client
        .command(&["FT.SEARCH", "idx", "@score:[10 (100]"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(
        total, 2,
        "Exclusive max (100 should exclude item with score=100"
    );

    // Both exclusive: (10 (100] — should only match score=50
    let response = client
        .command(&["FT.SEARCH", "idx", "@score:[(10 (100]"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1, "Both exclusive should only match score=50");

    server.shutdown().await;
}

// ============================================================================
// Phase 2: INFIELDS
// ============================================================================

#[tokio::test]
async fn test_ft_search_infields() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "HSET",
            "doc:1",
            "title",
            "hello world",
            "body",
            "something else",
        ])
        .await;
    client
        .command(&[
            "HSET",
            "doc:2",
            "title",
            "other title",
            "body",
            "hello world",
        ])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "body",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Without INFIELDS — should match both
    let response = client.command(&["FT.SEARCH", "idx", "hello"]).await;
    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 2);

    // With INFIELDS 1 title — should only match doc:1
    let response = client
        .command(&["FT.SEARCH", "idx", "hello", "INFIELDS", "1", "title"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1, "INFIELDS title should only match doc:1");

    server.shutdown().await;
}

// ============================================================================
// Phase 2: NOSTEM
// ============================================================================

#[tokio::test]
async fn test_ft_search_nostem() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "doc:1", "title", "running"]).await;
    client.command(&["HSET", "doc:2", "title", "runner"]).await;

    // Create index with NOSTEM on title
    client
        .command(&[
            "FT.CREATE",
            "idx_nostem",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "NOSTEM",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // With NOSTEM, searching "running" should only match exact token "running", not "runner"
    let response = client
        .command(&["FT.SEARCH", "idx_nostem", "running"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(
        total, 1,
        "NOSTEM should prevent stemming — only exact match"
    );

    server.shutdown().await;
}

// ============================================================================
// Phase 2: Weight boosting
// ============================================================================

#[tokio::test]
async fn test_ft_search_weight_boosting() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // doc:1 has "searchterm" only in body (WEIGHT 1.0)
    client
        .command(&[
            "HSET",
            "doc:1",
            "title",
            "unrelated content",
            "body",
            "searchterm appears here",
        ])
        .await;
    // doc:2 has "searchterm" only in title (WEIGHT 5.0)
    client
        .command(&[
            "HSET",
            "doc:2",
            "title",
            "searchterm appears here",
            "body",
            "unrelated content",
        ])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "WEIGHT",
            "5.0",
            "body",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .command(&["FT.SEARCH", "idx", "searchterm", "WITHSCORES"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 2);

    // With WITHSCORES the layout is [total, key, score, fields, key, score, fields, ...]
    // First result should be doc:2 (higher weight on title)
    let first_key = unwrap_bulk(&arr[1]);
    assert_eq!(
        first_key, b"doc:2",
        "Doc with match in WEIGHT 5.0 field should rank first"
    );

    server.shutdown().await;
}

// ============================================================================
// Phase 2: Expiry hooks
// ============================================================================

#[tokio::test]
async fn test_ft_search_expiry_removes_from_index() {
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
            "doc:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;

    // Set a key with a short TTL
    client
        .command(&["HSET", "doc:1", "name", "ephemeral"])
        .await;
    client.command(&["PEXPIRE", "doc:1", "200"]).await;
    // Also set a permanent key
    client
        .command(&["HSET", "doc:2", "name", "ephemeral permanent"])
        .await;

    tokio::time::sleep(Duration::from_millis(1500)).await;

    // After expiry, only the permanent key should remain
    let response = client.command(&["FT.SEARCH", "idx", "ephemeral"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1, "Expired key should be removed from search index");
    let key = unwrap_bulk(&arr[1]);
    assert_eq!(key, b"doc:2");

    server.shutdown().await;
}

// ============================================================================
// FT.SYNUPDATE / FT.SYNDUMP
// ============================================================================

#[tokio::test]
async fn test_ft_synupdate_and_syndump() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Create index
    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;

    // Add synonym group
    let response = client
        .command(&[
            "FT.SYNUPDATE",
            "idx",
            "vehicles",
            "car",
            "automobile",
            "vehicle",
        ])
        .await;
    assert_ok(&response);

    // Dump synonyms
    let response = client.command(&["FT.SYNDUMP", "idx"]).await;
    let arr = unwrap_array(response);
    // Should have 6 entries: 3 terms × (term + [group_id])
    assert_eq!(arr.len(), 6);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_synonym_search_expansion() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Create index
    client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;

    // Add documents
    client
        .command(&["HSET", "doc:1", "title", "buy a new car today"])
        .await;
    client
        .command(&["HSET", "doc:2", "title", "automobile insurance rates"])
        .await;
    client
        .command(&["HSET", "doc:3", "title", "vehicle maintenance tips"])
        .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Without synonyms, "car" should find only doc:1
    let response = client.command(&["FT.SEARCH", "idx", "car"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1);

    // Add synonym group
    client
        .command(&[
            "FT.SYNUPDATE",
            "idx",
            "vehicles",
            "car",
            "automobile",
            "vehicle",
        ])
        .await;

    // Now "car" should find all three docs via synonym expansion
    let response = client.command(&["FT.SEARCH", "idx", "car"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(
        total, 3,
        "Synonym expansion should find all three documents"
    );

    // "automobile" should also find all three
    let response = client.command(&["FT.SEARCH", "idx", "automobile"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(
        total, 3,
        "Synonym expansion should work for any term in the group"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_synupdate_unknown_index() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["FT.SYNUPDATE", "nonexistent", "grp1", "term1", "term2"])
        .await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_syndump_unknown_index() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client.command(&["FT.SYNDUMP", "nonexistent"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_synupdate_multiple_groups() {
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
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;

    // Add two different synonym groups
    client
        .command(&["FT.SYNUPDATE", "idx", "vehicles", "car", "automobile"])
        .await;
    client
        .command(&["FT.SYNUPDATE", "idx", "colors", "red", "crimson"])
        .await;

    // Dump should contain entries from both groups
    let response = client.command(&["FT.SYNDUMP", "idx"]).await;
    let arr = unwrap_array(response);
    // vehicles: car + [vehicles], automobile + [vehicles] = 4
    // colors: red + [colors], crimson + [colors] = 4
    assert_eq!(arr.len(), 8);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_synupdate_with_skipinitialscan() {
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
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;

    // SKIPINITIALSCAN should be accepted (and ignored — just skip the keyword)
    let response = client
        .command(&[
            "FT.SYNUPDATE",
            "idx",
            "vehicles",
            "SKIPINITIALSCAN",
            "car",
            "automobile",
        ])
        .await;
    assert_ok(&response);

    let response = client.command(&["FT.SYNDUMP", "idx"]).await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 4); // 2 terms × 2 entries each

    server.shutdown().await;
}

// ============================================================================
// GEO field type
// ============================================================================

#[tokio::test]
async fn test_ft_geo_create_and_search() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Create index with GEO field
    let response = client
        .command(&[
            "FT.CREATE",
            "geo_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "place:",
            "SCHEMA",
            "name",
            "TEXT",
            "location",
            "GEO",
        ])
        .await;
    assert_ok(&response);

    // Add places
    let response = client
        .command(&[
            "HSET",
            "place:1",
            "name",
            "Central Park",
            "location",
            "-73.9654,40.7829",
        ])
        .await;
    unwrap_integer(&response);

    let response = client
        .command(&[
            "HSET",
            "place:2",
            "name",
            "Times Square",
            "location",
            "-73.9855,40.7580",
        ])
        .await;
    unwrap_integer(&response);

    let response = client
        .command(&[
            "HSET",
            "place:3",
            "name",
            "Statue of Liberty",
            "location",
            "-74.0445,40.6892",
        ])
        .await;
    unwrap_integer(&response);

    // Wait for search index commit (interval is 1s)
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 5km radius from Central Park: Central Park + Times Square
    let response = client
        .command(&["FT.SEARCH", "geo_idx", "@location:[-73.9654 40.7829 5 km]"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 2);

    // 500m radius: only Central Park
    let response = client
        .command(&["FT.SEARCH", "geo_idx", "@location:[-73.9654 40.7829 500 m]"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1);

    // 20km radius: all three
    let response = client
        .command(&["FT.SEARCH", "geo_idx", "@location:[-73.9654 40.7829 20 km]"])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 3);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_geo_info_shows_type() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client
        .command(&[
            "FT.CREATE",
            "geo_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "place:",
            "SCHEMA",
            "location",
            "GEO",
        ])
        .await;
    assert_ok(&response);

    let response = client.command(&["FT.INFO", "geo_idx"]).await;
    let arr = unwrap_array(response);
    // Find the "attributes" section and verify GEO type is shown
    let info_str = format!("{:?}", arr);
    assert!(info_str.contains("GEO"), "FT.INFO should show GEO type");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_geo_with_text_filter() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client
        .command(&[
            "FT.CREATE",
            "geo_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "place:",
            "SCHEMA",
            "name",
            "TEXT",
            "location",
            "GEO",
        ])
        .await;
    assert_ok(&response);

    let response = client
        .command(&[
            "HSET",
            "place:1",
            "name",
            "Central Park",
            "location",
            "-73.9654,40.7829",
        ])
        .await;
    unwrap_integer(&response);

    let response = client
        .command(&[
            "HSET",
            "place:2",
            "name",
            "Times Square",
            "location",
            "-73.9855,40.7580",
        ])
        .await;
    unwrap_integer(&response);

    // Wait for search index commit (interval is 1s)
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Combine text + geo: "park" within 5km radius
    let response = client
        .command(&[
            "FT.SEARCH",
            "geo_idx",
            "park @location:[-73.9654 40.7829 5 km]",
        ])
        .await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 1);
    let key = unwrap_bulk(&arr[1]);
    assert_eq!(key, b"place:1");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_geo_alter_add_field() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Create index with TEXT only
    let response = client
        .command(&[
            "FT.CREATE",
            "geo_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "place:",
            "SCHEMA",
            "name",
            "TEXT",
        ])
        .await;
    assert_ok(&response);

    // Add GEO field via ALTER
    let response = client
        .command(&["FT.ALTER", "geo_idx", "SCHEMA", "ADD", "location", "GEO"])
        .await;
    assert_ok(&response);

    // Verify field was added
    let response = client.command(&["FT.INFO", "geo_idx"]).await;
    let info_str = format!("{:?}", unwrap_array(response));
    assert!(
        info_str.contains("GEO"),
        "FT.INFO should show GEO type after ALTER"
    );

    server.shutdown().await;
}

// ============================================================================
// FT.SUGADD / FT.SUGGET / FT.SUGDEL / FT.SUGLEN
// ============================================================================

#[tokio::test]
async fn test_ft_sugadd_and_suglen() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Add suggestions
    let response = client
        .command(&["FT.SUGADD", "autocomplete", "hello world", "1.0"])
        .await;
    assert_eq!(unwrap_integer(&response), 1);

    let response = client
        .command(&["FT.SUGADD", "autocomplete", "hello there", "2.0"])
        .await;
    assert_eq!(unwrap_integer(&response), 2);

    let response = client
        .command(&["FT.SUGADD", "autocomplete", "help me", "0.5"])
        .await;
    assert_eq!(unwrap_integer(&response), 3);

    // Check length
    let response = client.command(&["FT.SUGLEN", "autocomplete"]).await;
    assert_eq!(unwrap_integer(&response), 3);

    // Nonexistent key returns 0
    let response = client.command(&["FT.SUGLEN", "nonexistent"]).await;
    assert_eq!(unwrap_integer(&response), 0);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_sugadd_incr() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Add initial suggestion
    let response = client.command(&["FT.SUGADD", "ac", "hello", "1.0"]).await;
    assert_eq!(unwrap_integer(&response), 1);

    // Increment score
    let response = client
        .command(&["FT.SUGADD", "ac", "hello", "2.5", "INCR"])
        .await;
    assert_eq!(unwrap_integer(&response), 1); // still 1 suggestion

    // Check score via WITHSCORES
    let response = client
        .command(&["FT.SUGGET", "ac", "hel", "WITHSCORES"])
        .await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 2); // suggestion + score
    assert_eq!(unwrap_bulk(&arr[0]), b"hello");
    let score_str = String::from_utf8_lossy(unwrap_bulk(&arr[1]));
    let score: f64 = score_str.parse().unwrap();
    assert!(
        (score - 3.5).abs() < 0.01,
        "Score should be 3.5, got {score}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_sugget_basic() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Add suggestions with different scores
    client
        .command(&["FT.SUGADD", "ac", "hello world", "1.0"])
        .await;
    client
        .command(&["FT.SUGADD", "ac", "hello there", "3.0"])
        .await;
    client.command(&["FT.SUGADD", "ac", "help me", "2.0"]).await;
    client.command(&["FT.SUGADD", "ac", "goodbye", "5.0"]).await;

    // Get suggestions for "hel" prefix — should return 3 results, sorted by score desc
    let response = client.command(&["FT.SUGGET", "ac", "hel"]).await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 3);
    // Highest score first
    assert_eq!(unwrap_bulk(&arr[0]), b"hello there");
    assert_eq!(unwrap_bulk(&arr[1]), b"help me");
    assert_eq!(unwrap_bulk(&arr[2]), b"hello world");

    // MAX limits results
    let response = client
        .command(&["FT.SUGGET", "ac", "hel", "MAX", "2"])
        .await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 2);

    // No match returns null
    let response = client.command(&["FT.SUGGET", "ac", "xyz"]).await;
    assert!(matches!(response, Response::Bulk(None)));

    // Nonexistent key returns null
    let response = client.command(&["FT.SUGGET", "nonexistent", "abc"]).await;
    assert!(matches!(response, Response::Bulk(None)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_sugget_with_payloads() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["FT.SUGADD", "ac", "hello", "1.0", "PAYLOAD", "extra-data"])
        .await;
    client
        .command(&["FT.SUGADD", "ac", "help", "2.0"]) // no payload
        .await;

    // Get with payloads
    let response = client
        .command(&["FT.SUGGET", "ac", "hel", "WITHPAYLOADS"])
        .await;
    let arr = unwrap_array(response);
    // Each entry: suggestion + payload (or null)
    assert_eq!(arr.len(), 4); // 2 suggestions × (suggestion + payload)
    assert_eq!(unwrap_bulk(&arr[0]), b"help"); // higher score first
    assert!(matches!(arr[1], Response::Bulk(None))); // no payload for "help"
    assert_eq!(unwrap_bulk(&arr[2]), b"hello");
    assert_eq!(unwrap_bulk(&arr[3]), b"extra-data");

    // With both scores and payloads
    let response = client
        .command(&["FT.SUGGET", "ac", "hel", "WITHSCORES", "WITHPAYLOADS"])
        .await;
    let arr = unwrap_array(response);
    // Each entry: suggestion + score + payload
    assert_eq!(arr.len(), 6); // 2 × (suggestion + score + payload)

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_sugget_fuzzy() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client.command(&["FT.SUGADD", "ac", "hello", "1.0"]).await;
    client.command(&["FT.SUGADD", "ac", "hallo", "2.0"]).await;
    client.command(&["FT.SUGADD", "ac", "world", "3.0"]).await;

    // Without FUZZY, "hel" only matches "hello"
    let response = client.command(&["FT.SUGGET", "ac", "hel"]).await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 1);
    assert_eq!(unwrap_bulk(&arr[0]), b"hello");

    // With FUZZY, "hel" should also match "hallo" (levenshtein distance 1 on prefix)
    let response = client.command(&["FT.SUGGET", "ac", "hel", "FUZZY"]).await;
    let arr = unwrap_array(response);
    assert!(arr.len() >= 2, "FUZZY should match more suggestions");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_sugdel() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client.command(&["FT.SUGADD", "ac", "hello", "1.0"]).await;
    client.command(&["FT.SUGADD", "ac", "help", "2.0"]).await;

    // Delete existing
    let response = client.command(&["FT.SUGDEL", "ac", "hello"]).await;
    assert_eq!(unwrap_integer(&response), 1);

    // Delete nonexistent
    let response = client.command(&["FT.SUGDEL", "ac", "hello"]).await;
    assert_eq!(unwrap_integer(&response), 0);

    // Verify length
    let response = client.command(&["FT.SUGLEN", "ac"]).await;
    assert_eq!(unwrap_integer(&response), 1);

    // Verify only "help" remains
    let response = client.command(&["FT.SUGGET", "ac", "hel"]).await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 1);
    assert_eq!(unwrap_bulk(&arr[0]), b"help");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_sugdel_nonexistent_key() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client.command(&["FT.SUGDEL", "nonexistent", "hello"]).await;
    assert_eq!(unwrap_integer(&response), 0);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_sugadd_payload_survives_delete() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Add with payload
    client
        .command(&["FT.SUGADD", "ac", "hello", "1.0", "PAYLOAD", "my-payload"])
        .await;

    // Verify payload present
    let response = client
        .command(&["FT.SUGGET", "ac", "hel", "WITHPAYLOADS"])
        .await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 2);
    assert_eq!(unwrap_bulk(&arr[1]), b"my-payload");

    // Delete — should also remove payload
    let response = client.command(&["FT.SUGDEL", "ac", "hello"]).await;
    assert_eq!(unwrap_integer(&response), 1);

    // Length should be 0 (payload entry also gone)
    let response = client.command(&["FT.SUGLEN", "ac"]).await;
    assert_eq!(unwrap_integer(&response), 0);

    server.shutdown().await;
}

// ============================================================================
// FT.AGGREGATE
// ============================================================================

#[tokio::test]
async fn test_ft_aggregate_groupby_count() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Create data
    client
        .command(&["HSET", "doc:1", "category", "electronics", "price", "100"])
        .await;
    client
        .command(&["HSET", "doc:2", "category", "electronics", "price", "200"])
        .await;
    client
        .command(&["HSET", "doc:3", "category", "books", "price", "15"])
        .await;
    client
        .command(&["HSET", "doc:4", "category", "books", "price", "25"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "category",
            "TAG",
            "price",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // GROUPBY category REDUCE COUNT 0 AS cnt
    let response = client
        .command(&[
            "FT.AGGREGATE",
            "idx",
            "*",
            "GROUPBY",
            "1",
            "@category",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "cnt",
        ])
        .await;

    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 2, "Should have 2 groups");

    // Collect groups into a map for order-independent checking
    let mut groups = std::collections::HashMap::new();
    for i in 1..=2 {
        let row = unwrap_array(arr[i].clone());
        let mut category = String::new();
        let mut count = String::new();
        let mut j = 0;
        while j < row.len() {
            let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
            let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
            if key == "category" {
                category = val;
            } else if key == "cnt" {
                count = val;
            }
            j += 2;
        }
        groups.insert(category, count);
    }
    assert_eq!(groups.get("electronics").map(String::as_str), Some("2"));
    assert_eq!(groups.get("books").map(String::as_str), Some("2"));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_groupby_sum() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "category", "electronics", "price", "100"])
        .await;
    client
        .command(&["HSET", "doc:2", "category", "electronics", "price", "200"])
        .await;
    client
        .command(&["HSET", "doc:3", "category", "books", "price", "15"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "category",
            "TAG",
            "price",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .command(&[
            "FT.AGGREGATE",
            "idx",
            "*",
            "GROUPBY",
            "1",
            "@category",
            "REDUCE",
            "SUM",
            "1",
            "@price",
            "AS",
            "total",
        ])
        .await;

    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 2);

    let mut groups = std::collections::HashMap::new();
    for i in 1..=2 {
        let row = unwrap_array(arr[i].clone());
        let mut category = String::new();
        let mut total_val = String::new();
        let mut j = 0;
        while j < row.len() {
            let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
            let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
            if key == "category" {
                category = val;
            } else if key == "total" {
                total_val = val;
            }
            j += 2;
        }
        groups.insert(category, total_val);
    }
    assert_eq!(groups.get("electronics").map(String::as_str), Some("300"));
    assert_eq!(groups.get("books").map(String::as_str), Some("15"));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_sortby_and_limit() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "category", "a", "price", "10"])
        .await;
    client
        .command(&["HSET", "doc:2", "category", "b", "price", "30"])
        .await;
    client
        .command(&["HSET", "doc:3", "category", "c", "price", "20"])
        .await;
    client
        .command(&["HSET", "doc:4", "category", "d", "price", "40"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "category",
            "TAG",
            "price",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // GROUPBY category, REDUCE SUM price, SORTBY DESC, LIMIT 0 2
    let response = client
        .command(&[
            "FT.AGGREGATE",
            "idx",
            "*",
            "GROUPBY",
            "1",
            "@category",
            "REDUCE",
            "SUM",
            "1",
            "@price",
            "AS",
            "total",
            "SORTBY",
            "2",
            "@total",
            "DESC",
            "LIMIT",
            "0",
            "2",
        ])
        .await;

    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 2, "LIMIT 0 2 should return 2 rows");

    // First row should be highest total (d=40)
    let row1 = unwrap_array(arr[1].clone());
    let val1 = String::from_utf8(unwrap_bulk(&row1[3]).to_vec()).unwrap();
    assert_eq!(val1, "40");

    // Second row should be b=30
    let row2 = unwrap_array(arr[2].clone());
    let val2 = String::from_utf8(unwrap_bulk(&row2[3]).to_vec()).unwrap();
    assert_eq!(val2, "30");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_avg_reducer() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "category", "x", "score", "10"])
        .await;
    client
        .command(&["HSET", "doc:2", "category", "x", "score", "20"])
        .await;
    client
        .command(&["HSET", "doc:3", "category", "x", "score", "30"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "category",
            "TAG",
            "score",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .command(&[
            "FT.AGGREGATE",
            "idx",
            "*",
            "GROUPBY",
            "1",
            "@category",
            "REDUCE",
            "AVG",
            "1",
            "@score",
            "AS",
            "avg_score",
        ])
        .await;

    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 1);

    let row = unwrap_array(arr[1].clone());
    // Find the avg_score value
    let mut avg_val = String::new();
    let mut j = 0;
    while j < row.len() {
        let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
        if key == "avg_score" {
            avg_val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
            break;
        }
        j += 2;
    }
    let avg: f64 = avg_val.parse().unwrap();
    assert!(
        (avg - 20.0).abs() < 0.01,
        "Average of 10,20,30 should be 20, got {avg}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_multi_shard() {
    let server = start_multi_shard_server().await;
    let mut client = server.connect().await;

    // Use different hash tags so docs spread across shards
    client
        .command(&["HSET", "item:1", "type", "fruit", "qty", "10"])
        .await;
    client
        .command(&["HSET", "item:2", "type", "fruit", "qty", "20"])
        .await;
    client
        .command(&["HSET", "item:3", "type", "veggie", "qty", "5"])
        .await;
    client
        .command(&["HSET", "item:4", "type", "veggie", "qty", "15"])
        .await;
    client
        .command(&["HSET", "item:5", "type", "fruit", "qty", "30"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "item:",
            "SCHEMA",
            "type",
            "TAG",
            "qty",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // GROUPBY type, REDUCE SUM qty, REDUCE COUNT
    let response = client
        .command(&[
            "FT.AGGREGATE",
            "idx",
            "*",
            "GROUPBY",
            "1",
            "@type",
            "REDUCE",
            "SUM",
            "1",
            "@qty",
            "AS",
            "total_qty",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "cnt",
        ])
        .await;

    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 2);

    let mut groups = std::collections::HashMap::new();
    for i in 1..=2 {
        let row = unwrap_array(arr[i].clone());
        let mut typ = String::new();
        let mut total = String::new();
        let mut cnt = String::new();
        let mut j = 0;
        while j < row.len() {
            let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
            let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
            match key.as_str() {
                "type" => typ = val,
                "total_qty" => total = val,
                "cnt" => cnt = val,
                _ => {}
            }
            j += 2;
        }
        groups.insert(typ, (total, cnt));
    }

    assert_eq!(groups["fruit"].0, "60");
    assert_eq!(groups["fruit"].1, "3");
    assert_eq!(groups["veggie"].0, "20");
    assert_eq!(groups["veggie"].1, "2");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_min_max_reducers() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "group", "a", "val", "5"])
        .await;
    client
        .command(&["HSET", "doc:2", "group", "a", "val", "15"])
        .await;
    client
        .command(&["HSET", "doc:3", "group", "a", "val", "10"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "group",
            "TAG",
            "val",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .command(&[
            "FT.AGGREGATE",
            "idx",
            "*",
            "GROUPBY",
            "1",
            "@group",
            "REDUCE",
            "MIN",
            "1",
            "@val",
            "AS",
            "min_val",
            "REDUCE",
            "MAX",
            "1",
            "@val",
            "AS",
            "max_val",
        ])
        .await;

    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 1);

    let row = unwrap_array(arr[1].clone());
    let mut min_val = String::new();
    let mut max_val = String::new();
    let mut j = 0;
    while j < row.len() {
        let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
        let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
        if key == "min_val" {
            min_val = val;
        } else if key == "max_val" {
            max_val = val;
        }
        j += 2;
    }
    assert_eq!(min_val, "5");
    assert_eq!(max_val, "15");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_with_query_filter() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "status", "active", "amount", "100"])
        .await;
    client
        .command(&["HSET", "doc:2", "status", "active", "amount", "200"])
        .await;
    client
        .command(&["HSET", "doc:3", "status", "inactive", "amount", "50"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "status",
            "TAG",
            "amount",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Filter to only active docs, then aggregate
    let response = client
        .command(&[
            "FT.AGGREGATE",
            "idx",
            "@status:{active}",
            "GROUPBY",
            "1",
            "@status",
            "REDUCE",
            "SUM",
            "1",
            "@amount",
            "AS",
            "total",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "cnt",
        ])
        .await;

    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 1, "Only 1 group (active)");

    let row = unwrap_array(arr[1].clone());
    let mut total = String::new();
    let mut cnt = String::new();
    let mut j = 0;
    while j < row.len() {
        let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
        let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
        if key == "total" {
            total = val;
        } else if key == "cnt" {
            cnt = val;
        }
        j += 2;
    }
    assert_eq!(total, "300");
    assert_eq!(cnt, "2");

    server.shutdown().await;
}

// ============================================================================
// VECTOR (KNN search)
// ============================================================================

/// Helper to create a f32 vector as raw little-endian bytes.
fn vec_to_blob(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

#[tokio::test]
async fn test_ft_vector_create_and_info() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client
        .command(&[
            "FT.CREATE",
            "vecidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "embedding",
            "VECTOR",
            "FLAT",
            "6",
            "DIM",
            "4",
            "DISTANCE_METRIC",
            "COSINE",
            "TYPE",
            "FLOAT32",
        ])
        .await;
    assert_ok(&response);

    // FT.INFO should show the vector field
    let info = client.command(&["FT.INFO", "vecidx"]).await;
    let info_str = format!("{:?}", info);
    assert!(
        info_str.contains("embedding"),
        "Should list embedding field"
    );
    assert!(info_str.contains("VECTOR"), "Should show VECTOR type");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_vector_knn_search_cosine() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Insert documents FIRST (before creating index), then create index
    // This tests the initial bulk-indexing path.
    let b_cmd = Bytes::from_static(b"HSET");
    let b_title = Bytes::from_static(b"title");
    let b_emb = Bytes::from_static(b"embedding");

    // doc:1 = [1, 0, 0]
    let b_key1 = Bytes::from_static(b"doc:1");
    let b_title1 = Bytes::from_static(b"first");
    let b_blob1 = Bytes::from(vec_to_blob(&[1.0, 0.0, 0.0]));
    client
        .command_raw(&[&b_cmd, &b_key1, &b_title, &b_title1, &b_emb, &b_blob1])
        .await;

    // doc:2 = [0.9, 0.1, 0] — very close to doc:1
    let b_key2 = Bytes::from_static(b"doc:2");
    let b_title2 = Bytes::from_static(b"second");
    let b_blob2 = Bytes::from(vec_to_blob(&[0.9, 0.1, 0.0]));
    client
        .command_raw(&[&b_cmd, &b_key2, &b_title, &b_title2, &b_emb, &b_blob2])
        .await;

    // doc:3 = [0, 0, 1] — orthogonal to doc:1
    let b_key3 = Bytes::from_static(b"doc:3");
    let b_title3 = Bytes::from_static(b"third");
    let b_blob3 = Bytes::from(vec_to_blob(&[0.0, 0.0, 1.0]));
    client
        .command_raw(&[&b_cmd, &b_key3, &b_title, &b_title3, &b_emb, &b_blob3])
        .await;

    // Create index AFTER docs exist
    let response = client
        .command(&[
            "FT.CREATE",
            "vecidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "embedding",
            "VECTOR",
            "FLAT",
            "6",
            "DIM",
            "3",
            "DISTANCE_METRIC",
            "COSINE",
            "TYPE",
            "FLOAT32",
        ])
        .await;
    assert_ok(&response);

    // Wait for initial indexing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // KNN search for vector closest to [1, 0, 0], top 2
    let query_blob = Bytes::from(vec_to_blob(&[1.0, 0.0, 0.0]));
    let b_search = Bytes::from_static(b"FT.SEARCH");
    let b_idx = Bytes::from_static(b"vecidx");
    let b_query = Bytes::from_static(b"*=>[KNN 2 @embedding $BLOB]");
    let b_params = Bytes::from_static(b"PARAMS");
    let b_two = Bytes::from_static(b"2");
    let b_blob_name = Bytes::from_static(b"BLOB");

    let response = client
        .command_raw(&[
            &b_search,
            &b_idx,
            &b_query,
            &b_params,
            &b_two,
            &b_blob_name,
            &query_blob,
        ])
        .await;

    let arr = unwrap_array(response);
    // First element is the count
    let count = unwrap_integer(&arr[0]);
    assert_eq!(count, 2, "Should return 2 nearest neighbors");

    // Results should include doc:1 and doc:2 (closest by cosine), NOT doc:3
    let key1 = String::from_utf8(unwrap_bulk(&arr[1]).to_vec()).unwrap();
    let key2 = String::from_utf8(unwrap_bulk(&arr[3]).to_vec()).unwrap();
    let keys: Vec<&str> = vec![&key1, &key2];
    assert!(keys.contains(&"doc:1"), "doc:1 should be in top 2");
    assert!(keys.contains(&"doc:2"), "doc:2 should be in top 2");

    // Check that __vec_score field is included
    let fields1 = unwrap_array(arr[2].clone());
    let mut has_score = false;
    for chunk in fields1.chunks(2) {
        if let Ok(name) = std::str::from_utf8(&unwrap_bulk(&chunk[0])) {
            if name == "__vec_score" {
                has_score = true;
            }
        }
    }
    assert!(has_score, "Should include __vec_score field");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_vector_knn_search_l2() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Insert docs first
    let b_cmd = Bytes::from_static(b"HSET");
    let b_emb = Bytes::from_static(b"embedding");
    let b_ka = Bytes::from_static(b"doc:a");
    let b_kb = Bytes::from_static(b"doc:b");
    let b_kc = Bytes::from_static(b"doc:c");
    let blob_a = Bytes::from(vec_to_blob(&[0.0, 0.0]));
    let blob_b = Bytes::from(vec_to_blob(&[1.0, 1.0]));
    let blob_c = Bytes::from(vec_to_blob(&[10.0, 10.0]));

    client.command_raw(&[&b_cmd, &b_ka, &b_emb, &blob_a]).await;
    client.command_raw(&[&b_cmd, &b_kb, &b_emb, &blob_b]).await;
    client.command_raw(&[&b_cmd, &b_kc, &b_emb, &blob_c]).await;

    // Create index with L2 distance metric
    let response = client
        .command(&[
            "FT.CREATE",
            "vecidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "embedding",
            "VECTOR",
            "FLAT",
            "6",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "L2",
            "TYPE",
            "FLOAT32",
        ])
        .await;
    assert_ok(&response);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // KNN 2 nearest to [0.5, 0.5]
    let query_blob = Bytes::from(vec_to_blob(&[0.5, 0.5]));
    let b_search = Bytes::from_static(b"FT.SEARCH");
    let b_idx = Bytes::from_static(b"vecidx");
    let b_query = Bytes::from_static(b"*=>[KNN 2 @embedding $VEC]");
    let b_params = Bytes::from_static(b"PARAMS");
    let b_two = Bytes::from_static(b"2");
    let b_vec = Bytes::from_static(b"VEC");

    let response = client
        .command_raw(&[
            &b_search,
            &b_idx,
            &b_query,
            &b_params,
            &b_two,
            &b_vec,
            &query_blob,
        ])
        .await;

    let arr = unwrap_array(response);
    let count = unwrap_integer(&arr[0]);
    assert_eq!(count, 2);

    // doc:a and doc:b should be closest to [0.5, 0.5]
    let key1 = String::from_utf8(unwrap_bulk(&arr[1]).to_vec()).unwrap();
    let key2 = String::from_utf8(unwrap_bulk(&arr[3]).to_vec()).unwrap();
    let keys: Vec<&str> = vec![&key1, &key2];
    assert!(keys.contains(&"doc:a"), "doc:a should be in top 2");
    assert!(keys.contains(&"doc:b"), "doc:b should be in top 2");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_vector_missing_param() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client
        .command(&[
            "FT.CREATE",
            "vecidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "embedding",
            "VECTOR",
            "FLAT",
            "6",
            "DIM",
            "3",
            "DISTANCE_METRIC",
            "COSINE",
            "TYPE",
            "FLOAT32",
        ])
        .await;
    assert_ok(&response);

    // Search without providing the required PARAMS
    let response = client
        .command(&["FT.SEARCH", "vecidx", "*=>[KNN 2 @embedding $BLOB]"])
        .await;

    // Should return error about missing parameter
    match &response {
        Response::Error(msg) => {
            let msg_str = std::str::from_utf8(msg).unwrap_or("");
            assert!(
                msg_str.contains("No such parameter"),
                "Error should mention missing parameter, got: {}",
                msg_str
            );
        }
        other => panic!("Expected error response, got: {:?}", other),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_vector_with_text_fields() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Insert items FIRST
    let b_cmd = Bytes::from_static(b"HSET");
    let b_name = Bytes::from_static(b"name");
    let b_emb = Bytes::from_static(b"embedding");

    let b_k1 = Bytes::from_static(b"item:1");
    let b_n1 = Bytes::from_static(b"apple");
    let blob1 = Bytes::from(vec_to_blob(&[1.0, 0.0]));
    client
        .command_raw(&[&b_cmd, &b_k1, &b_name, &b_n1, &b_emb, &blob1])
        .await;

    let b_k2 = Bytes::from_static(b"item:2");
    let b_n2 = Bytes::from_static(b"banana");
    let blob2 = Bytes::from(vec_to_blob(&[0.0, 1.0]));
    client
        .command_raw(&[&b_cmd, &b_k2, &b_name, &b_n2, &b_emb, &blob2])
        .await;

    // Create index with both TEXT and VECTOR
    let response = client
        .command(&[
            "FT.CREATE",
            "hybridx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "item:",
            "SCHEMA",
            "name",
            "TEXT",
            "embedding",
            "VECTOR",
            "FLAT",
            "6",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "COSINE",
            "TYPE",
            "FLOAT32",
        ])
        .await;
    assert_ok(&response);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Regular text search should still work
    let text_resp = client.command(&["FT.SEARCH", "hybridx", "apple"]).await;
    let text_arr = unwrap_array(text_resp);
    assert_eq!(unwrap_integer(&text_arr[0]), 1);

    // KNN search should also work
    let query_blob = Bytes::from(vec_to_blob(&[1.0, 0.0]));
    let b_search = Bytes::from_static(b"FT.SEARCH");
    let b_idx = Bytes::from_static(b"hybridx");
    let b_query = Bytes::from_static(b"*=>[KNN 1 @embedding $V]");
    let b_params = Bytes::from_static(b"PARAMS");
    let b_two = Bytes::from_static(b"2");
    let b_v = Bytes::from_static(b"V");

    let vec_resp = client
        .command_raw(&[
            &b_search,
            &b_idx,
            &b_query,
            &b_params,
            &b_two,
            &b_v,
            &query_blob,
        ])
        .await;

    let vec_arr = unwrap_array(vec_resp);
    assert_eq!(
        unwrap_integer(&vec_arr[0]),
        1,
        "KNN 1 should return 1 result"
    );
    let key = String::from_utf8(unwrap_bulk(&vec_arr[1]).to_vec()).unwrap();
    assert_eq!(key, "item:1", "Closest to [1,0] should be item:1");

    server.shutdown().await;
}

// ============================================================================
// KNN multi-shard sort order
// ============================================================================

#[tokio::test]
async fn test_ft_vector_knn_multi_shard_sort_order() {
    let server = start_multi_shard_server().await;
    let mut client = server.connect().await;

    let b_cmd = Bytes::from_static(b"HSET");
    let b_emb = Bytes::from_static(b"embedding");
    let b_name = Bytes::from_static(b"name");

    // Spread docs across shards with distinct distances from query [1,0,0]
    for (i, (vec, name)) in [
        ([1.0f32, 0.0, 0.0], "exact"), // distance ~0
        ([0.9, 0.1, 0.0], "close"),    // small distance
        ([0.5, 0.5, 0.0], "medium"),   // medium distance
        ([0.0, 0.0, 1.0], "far"),      // large distance
    ]
    .iter()
    .enumerate()
    {
        let key = format!("vdoc:{i}");
        let blob = Bytes::from(vec_to_blob(vec));
        let b_key = Bytes::from(key);
        let b_name_val = Bytes::from(*name);
        client
            .command_raw(&[&b_cmd, &b_key, &b_name, &b_name_val, &b_emb, &blob])
            .await;
    }

    let response = client
        .command(&[
            "FT.CREATE",
            "vidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "vdoc:",
            "SCHEMA",
            "name",
            "TEXT",
            "embedding",
            "VECTOR",
            "FLAT",
            "6",
            "DIM",
            "3",
            "DISTANCE_METRIC",
            "L2",
            "TYPE",
            "FLOAT32",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // KNN 4: should return all 4 sorted by ascending distance
    let query_blob = Bytes::from(vec_to_blob(&[1.0, 0.0, 0.0]));
    let b_search = Bytes::from_static(b"FT.SEARCH");
    let b_idx = Bytes::from_static(b"vidx");
    let b_query = Bytes::from_static(b"*=>[KNN 4 @embedding $V]");
    let b_params = Bytes::from_static(b"PARAMS");
    let b_two = Bytes::from_static(b"2");
    let b_v = Bytes::from_static(b"V");

    let response = client
        .command_raw(&[
            &b_search,
            &b_idx,
            &b_query,
            &b_params,
            &b_two,
            &b_v,
            &query_blob,
        ])
        .await;

    let arr = unwrap_array(response);
    let count = unwrap_integer(&arr[0]);
    assert_eq!(count, 4);

    // First result should be the exact match (distance 0)
    let key0 = String::from_utf8(unwrap_bulk(&arr[1]).to_vec()).unwrap();
    assert_eq!(key0, "vdoc:0", "First KNN result should be exact match");

    // Last result should be the farthest
    let key3 = String::from_utf8(unwrap_bulk(&arr[7]).to_vec()).unwrap();
    assert_eq!(key3, "vdoc:3", "Last KNN result should be farthest");

    server.shutdown().await;
}

// ============================================================================
// FT.AGGREGATE APPLY / FILTER / new reducers
// ============================================================================

#[tokio::test]
async fn test_ft_aggregate_apply_before_groupby() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "prod:1", "category", "A", "price", "10", "qty", "3"])
        .await;
    client
        .command(&["HSET", "prod:2", "category", "A", "price", "20", "qty", "2"])
        .await;
    client
        .command(&["HSET", "prod:3", "category", "B", "price", "5", "qty", "4"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "pidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "prod:",
            "SCHEMA",
            "category",
            "TAG",
            "price",
            "NUMERIC",
            "qty",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // APPLY @price*@qty AS total, then GROUPBY category SUM total
    let response = client
        .command(&[
            "FT.AGGREGATE",
            "pidx",
            "*",
            "APPLY",
            "@price*@qty",
            "AS",
            "total",
            "GROUPBY",
            "1",
            "@category",
            "REDUCE",
            "SUM",
            "1",
            "@total",
            "AS",
            "sum_total",
            "SORTBY",
            "2",
            "@category",
            "ASC",
        ])
        .await;

    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 2);

    // Parse results
    let mut groups = std::collections::HashMap::new();
    for i in 1..=2 {
        let row = unwrap_array(arr[i].clone());
        let mut cat = String::new();
        let mut total = String::new();
        let mut j = 0;
        while j < row.len() {
            let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
            let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
            match key.as_str() {
                "category" => cat = val,
                "sum_total" => total = val,
                _ => {}
            }
            j += 2;
        }
        groups.insert(cat, total);
    }

    assert_eq!(groups["A"], "70"); // 10*3 + 20*2
    assert_eq!(groups["B"], "20"); // 5*4

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_filter() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "emp:1", "dept", "eng", "age", "25"])
        .await;
    client
        .command(&["HSET", "emp:2", "dept", "eng", "age", "15"])
        .await;
    client
        .command(&["HSET", "emp:3", "dept", "sales", "age", "30"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "eidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "emp:",
            "SCHEMA",
            "dept",
            "TAG",
            "age",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // FILTER @age>=18, then GROUPBY dept COUNT
    let response = client
        .command(&[
            "FT.AGGREGATE",
            "eidx",
            "*",
            "FILTER",
            "@age>=18",
            "GROUPBY",
            "1",
            "@dept",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "cnt",
        ])
        .await;

    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 2);

    // eng should have 1 (only age=25 passes), sales has 1 (age=30)
    let mut groups = std::collections::HashMap::new();
    for i in 1..=2 {
        let row = unwrap_array(arr[i].clone());
        let mut dept = String::new();
        let mut cnt = String::new();
        let mut j = 0;
        while j < row.len() {
            let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
            let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
            match key.as_str() {
                "dept" => dept = val,
                "cnt" => cnt = val,
                _ => {}
            }
            j += 2;
        }
        groups.insert(dept, cnt);
    }

    assert_eq!(groups["eng"], "1");
    assert_eq!(groups["sales"], "1");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_count_distinct() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "cd:1", "city", "NYC", "color", "red"])
        .await;
    client
        .command(&["HSET", "cd:2", "city", "NYC", "color", "blue"])
        .await;
    client
        .command(&["HSET", "cd:3", "city", "NYC", "color", "red"])
        .await;
    client
        .command(&["HSET", "cd:4", "city", "LA", "color", "green"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "cdidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "cd:",
            "SCHEMA",
            "city",
            "TAG",
            "color",
            "TAG",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .command(&[
            "FT.AGGREGATE",
            "cdidx",
            "*",
            "GROUPBY",
            "1",
            "@city",
            "REDUCE",
            "COUNT_DISTINCT",
            "1",
            "@color",
            "AS",
            "uniq",
            "SORTBY",
            "2",
            "@city",
            "ASC",
        ])
        .await;

    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 2);

    let mut groups = std::collections::HashMap::new();
    for i in 1..=2 {
        let row = unwrap_array(arr[i].clone());
        let mut city = String::new();
        let mut uniq = String::new();
        let mut j = 0;
        while j < row.len() {
            let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
            let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
            match key.as_str() {
                "city" => city = val,
                "uniq" => uniq = val,
                _ => {}
            }
            j += 2;
        }
        groups.insert(city, uniq);
    }

    assert_eq!(groups["NYC"], "2"); // red, blue
    assert_eq!(groups["LA"], "1"); // green

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_tolist() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "tl:1", "city", "NYC", "name", "Alice"])
        .await;
    client
        .command(&["HSET", "tl:2", "city", "NYC", "name", "Bob"])
        .await;
    client
        .command(&["HSET", "tl:3", "city", "LA", "name", "Carol"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "tlidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "tl:",
            "SCHEMA",
            "city",
            "TAG",
            "name",
            "TEXT",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .command(&[
            "FT.AGGREGATE",
            "tlidx",
            "*",
            "GROUPBY",
            "1",
            "@city",
            "REDUCE",
            "TOLIST",
            "1",
            "@name",
            "AS",
            "names",
            "SORTBY",
            "2",
            "@city",
            "ASC",
        ])
        .await;

    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 2);

    let mut groups = std::collections::HashMap::new();
    for i in 1..=2 {
        let row = unwrap_array(arr[i].clone());
        let mut city = String::new();
        let mut names = String::new();
        let mut j = 0;
        while j < row.len() {
            let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
            let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
            match key.as_str() {
                "city" => city = val,
                "names" => names = val,
                _ => {}
            }
            j += 2;
        }
        groups.insert(city, names);
    }

    let nyc_names = &groups["NYC"];
    assert!(
        nyc_names.contains("Alice"),
        "NYC tolist should contain Alice"
    );
    assert!(nyc_names.contains("Bob"), "NYC tolist should contain Bob");
    assert_eq!(groups["LA"], "Carol");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_stddev() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Values: 2, 4, 4, 4, 5, 5, 7, 9 → stddev = 2.0
    for (i, v) in [2, 4, 4, 4, 5, 5, 7, 9].iter().enumerate() {
        client
            .command(&["HSET", &format!("sd:{i}"), "g", "a", "x", &v.to_string()])
            .await;
    }

    let response = client
        .command(&[
            "FT.CREATE",
            "sdidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "sd:",
            "SCHEMA",
            "g",
            "TAG",
            "x",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .command(&[
            "FT.AGGREGATE",
            "sdidx",
            "*",
            "GROUPBY",
            "1",
            "@g",
            "REDUCE",
            "STDDEV",
            "1",
            "@x",
            "AS",
            "sd",
        ])
        .await;

    let arr = unwrap_array(response);
    let row = unwrap_array(arr[1].clone());
    let mut sd_val = String::new();
    let mut j = 0;
    while j < row.len() {
        let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
        let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
        if key == "sd" {
            sd_val = val;
        }
        j += 2;
    }

    let sd: f64 = sd_val.parse().unwrap();
    assert!((sd - 2.0).abs() < 0.1, "stddev should be ~2.0, got {sd}");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_multi_groupby() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "mg:1", "city", "NYC", "dept", "eng"])
        .await;
    client
        .command(&["HSET", "mg:2", "city", "NYC", "dept", "eng"])
        .await;
    client
        .command(&["HSET", "mg:3", "city", "NYC", "dept", "sales"])
        .await;
    client
        .command(&["HSET", "mg:4", "city", "LA", "dept", "eng"])
        .await;
    client
        .command(&["HSET", "mg:5", "city", "LA", "dept", "eng"])
        .await;
    client
        .command(&["HSET", "mg:6", "city", "LA", "dept", "eng"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "mgidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "mg:",
            "SCHEMA",
            "city",
            "TAG",
            "dept",
            "TAG",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // First GROUPBY city+dept COUNT, then second GROUPBY count to find how many groups have each count
    let response = client
        .command(&[
            "FT.AGGREGATE",
            "mgidx",
            "*",
            "GROUPBY",
            "2",
            "@city",
            "@dept",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "cnt",
            "GROUPBY",
            "1",
            "@cnt",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "num_groups",
            "SORTBY",
            "2",
            "@cnt",
            "ASC",
        ])
        .await;

    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert!(total >= 2, "Should have at least 2 count values");

    // Groups: NYC/eng=2, NYC/sales=1, LA/eng=3
    // Second GROUPBY: cnt=1 -> 1 group, cnt=2 -> 1 group, cnt=3 -> 1 group
    let mut groups = std::collections::HashMap::new();
    for i in 1..=(total as usize) {
        let row = unwrap_array(arr[i].clone());
        let mut cnt = String::new();
        let mut num = String::new();
        let mut j = 0;
        while j < row.len() {
            let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
            let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
            match key.as_str() {
                "cnt" => cnt = val,
                "num_groups" => num = val,
                _ => {}
            }
            j += 2;
        }
        groups.insert(cnt, num);
    }

    assert_eq!(groups.get("1").map(String::as_str), Some("1")); // NYC/sales
    assert_eq!(groups.get("2").map(String::as_str), Some("1")); // NYC/eng
    assert_eq!(groups.get("3").map(String::as_str), Some("1")); // LA/eng

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_load() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Store extra fields NOT in the index schema
    client
        .command(&["HSET", "lo:1", "city", "NYC", "extra", "hello"])
        .await;
    client
        .command(&["HSET", "lo:2", "city", "LA", "extra", "world"])
        .await;

    // Only index "city" — "extra" is NOT in the schema
    let response = client
        .command(&[
            "FT.CREATE",
            "loidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "lo:",
            "SCHEMA",
            "city",
            "TAG",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // LOAD the non-indexed "extra" field, then GROUPBY city, TOLIST extra
    let response = client
        .command(&[
            "FT.AGGREGATE",
            "loidx",
            "*",
            "LOAD",
            "1",
            "@extra",
            "GROUPBY",
            "1",
            "@city",
            "REDUCE",
            "TOLIST",
            "1",
            "@extra",
            "AS",
            "extras",
            "SORTBY",
            "2",
            "@city",
            "ASC",
        ])
        .await;

    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 2);

    let mut groups = std::collections::HashMap::new();
    for i in 1..=2 {
        let row = unwrap_array(arr[i].clone());
        let mut city = String::new();
        let mut extras = String::new();
        let mut j = 0;
        while j < row.len() {
            let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
            let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
            match key.as_str() {
                "city" => city = val,
                "extras" => extras = val,
                _ => {}
            }
            j += 2;
        }
        groups.insert(city, extras);
    }

    assert_eq!(groups["LA"], "world");
    assert_eq!(groups["NYC"], "hello");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_apply_after_groupby() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "aa:1", "city", "NYC", "sales", "100"])
        .await;
    client
        .command(&["HSET", "aa:2", "city", "NYC", "sales", "200"])
        .await;
    client
        .command(&["HSET", "aa:3", "city", "LA", "sales", "50"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "aaidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "aa:",
            "SCHEMA",
            "city",
            "TAG",
            "sales",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // GROUPBY city, SUM + COUNT, then APPLY total/cnt AS avg
    let response = client
        .command(&[
            "FT.AGGREGATE",
            "aaidx",
            "*",
            "GROUPBY",
            "1",
            "@city",
            "REDUCE",
            "SUM",
            "1",
            "@sales",
            "AS",
            "total",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "cnt",
            "APPLY",
            "@total/@cnt",
            "AS",
            "avg_sale",
            "SORTBY",
            "2",
            "@city",
            "ASC",
        ])
        .await;

    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 2);

    let mut groups = std::collections::HashMap::new();
    for i in 1..=2 {
        let row = unwrap_array(arr[i].clone());
        let mut city = String::new();
        let mut avg = String::new();
        let mut j = 0;
        while j < row.len() {
            let key = String::from_utf8(unwrap_bulk(&row[j]).to_vec()).unwrap();
            let val = String::from_utf8(unwrap_bulk(&row[j + 1]).to_vec()).unwrap();
            match key.as_str() {
                "city" => city = val,
                "avg_sale" => avg = val,
                _ => {}
            }
            j += 2;
        }
        groups.insert(city, avg);
    }

    assert_eq!(groups["NYC"], "150"); // 300/2
    assert_eq!(groups["LA"], "50"); // 50/1

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_aggregate_filter_after_groupby() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "fa:1", "city", "NYC", "val", "1"])
        .await;
    client
        .command(&["HSET", "fa:2", "city", "NYC", "val", "1"])
        .await;
    client
        .command(&["HSET", "fa:3", "city", "NYC", "val", "1"])
        .await;
    client
        .command(&["HSET", "fa:4", "city", "LA", "val", "1"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "faidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "fa:",
            "SCHEMA",
            "city",
            "TAG",
            "val",
            "NUMERIC",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // GROUPBY city COUNT, then FILTER count > 2
    let response = client
        .command(&[
            "FT.AGGREGATE",
            "faidx",
            "*",
            "GROUPBY",
            "1",
            "@city",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "cnt",
            "FILTER",
            "@cnt>2",
        ])
        .await;

    let arr = unwrap_array(response);
    assert_eq!(unwrap_integer(&arr[0]), 1); // Only NYC has count > 2

    let row = unwrap_array(arr[1].clone());
    let city_val = String::from_utf8(unwrap_bulk(&row[1]).to_vec()).unwrap();
    assert_eq!(city_val, "NYC");

    server.shutdown().await;
}

// ============================================================================
// FT.ALIASADD, FT.ALIASDEL, FT.ALIASUPDATE
// ============================================================================

#[tokio::test]
async fn test_ft_alias_add_search_delete() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Add docs first, then create index (existing test pattern)
    client
        .command(&["HSET", "doc:1", "title", "hello world"])
        .await;

    let response = client
        .command(&[
            "FT.CREATE",
            "myidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;
    assert_ok(&response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Add alias
    let response = client.command(&["FT.ALIASADD", "myalias", "myidx"]).await;
    assert_ok(&response);

    // Search via alias
    let response = client.command(&["FT.SEARCH", "myalias", "hello"]).await;
    let arr = unwrap_array(response);
    let count = unwrap_integer(&arr[0]);
    assert!(
        count >= 1,
        "Expected at least 1 result via alias, got {count}"
    );

    // Delete alias
    let response = client.command(&["FT.ALIASDEL", "myalias"]).await;
    assert_ok(&response);

    // Search via deleted alias should fail
    let response = client.command(&["FT.SEARCH", "myalias", "hello"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "Expected error after alias deletion"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_alias_add_duplicate_error() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["FT.CREATE", "idx1", "ON", "HASH", "SCHEMA", "title", "TEXT"])
        .await;

    let response = client.command(&["FT.ALIASADD", "al1", "idx1"]).await;
    assert_ok(&response);

    // Adding same alias again should fail
    let response = client.command(&["FT.ALIASADD", "al1", "idx1"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "Expected error for duplicate alias"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_alias_del_nonexistent_error() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client.command(&["FT.ALIASDEL", "nonexistent"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "Expected error for nonexistent alias"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_alias_update() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Add docs first
    client.command(&["HSET", "a:1", "title", "alpha"]).await;
    client.command(&["HSET", "b:1", "title", "beta"]).await;

    // Create two indexes
    client
        .command(&[
            "FT.CREATE",
            "idx1",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "a:",
            "SCHEMA",
            "title",
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
            "b:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create alias pointing to idx1
    client.command(&["FT.ALIASADD", "al", "idx1"]).await;

    // Search should find "alpha"
    let response = client.command(&["FT.SEARCH", "al", "alpha"]).await;
    let arr = unwrap_array(response);
    assert!(unwrap_integer(&arr[0]) >= 1);

    // Update alias to point to idx2
    let response = client.command(&["FT.ALIASUPDATE", "al", "idx2"]).await;
    assert_ok(&response);

    // Search should now find "beta"
    let response = client.command(&["FT.SEARCH", "al", "beta"]).await;
    let arr = unwrap_array(response);
    assert!(unwrap_integer(&arr[0]) >= 1);

    server.shutdown().await;
}

// ============================================================================
// FT.TAGVALS
// ============================================================================

#[tokio::test]
async fn test_ft_tagvals_basic() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Add docs first
    client
        .command(&["HSET", "doc:1", "color", "red,blue"])
        .await;
    client
        .command(&["HSET", "doc:2", "color", "green,red"])
        .await;
    client.command(&["HSET", "doc:3", "color", "blue"]).await;

    client
        .command(&[
            "FT.CREATE",
            "tvidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "color",
            "TAG",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client.command(&["FT.TAGVALS", "tvidx", "color"]).await;
    let arr = unwrap_array(response);

    let values: Vec<String> = arr
        .iter()
        .filter_map(|r| {
            if let Response::Bulk(Some(b)) = r {
                Some(String::from_utf8_lossy(b).to_string())
            } else {
                None
            }
        })
        .collect();

    assert!(values.contains(&"red".to_string()));
    assert!(values.contains(&"blue".to_string()));
    assert!(values.contains(&"green".to_string()));
    assert_eq!(values.len(), 3);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_tagvals_nonexistent_index() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client.command(&["FT.TAGVALS", "nope", "field"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_tagvals_non_tag_field() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "FT.CREATE",
            "tvidx2",
            "ON",
            "HASH",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;

    let response = client.command(&["FT.TAGVALS", "tvidx2", "title"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

// ============================================================================
// FT.DICTADD, FT.DICTDEL, FT.DICTDUMP
// ============================================================================

#[tokio::test]
async fn test_ft_dict_add_dump_del() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Add terms
    let response = client
        .command(&["FT.DICTADD", "mydict", "hello", "world", "foo"])
        .await;
    let count = unwrap_integer(&response);
    assert_eq!(count, 3);

    // Dump
    let response = client.command(&["FT.DICTDUMP", "mydict"]).await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 3);

    // Add duplicate — only new ones counted
    let response = client
        .command(&["FT.DICTADD", "mydict", "hello", "bar"])
        .await;
    let count = unwrap_integer(&response);
    assert_eq!(count, 1); // only "bar" is new

    // Delete terms
    let response = client
        .command(&["FT.DICTDEL", "mydict", "hello", "nonexistent"])
        .await;
    let count = unwrap_integer(&response);
    assert_eq!(count, 1); // only "hello" was actually removed

    // Dump again — should have 3 remaining (world, foo, bar)
    let response = client.command(&["FT.DICTDUMP", "mydict"]).await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 3);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_dictdump_nonexistent() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Dumping nonexistent dict should return empty array
    let response = client.command(&["FT.DICTDUMP", "nodict"]).await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 0);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_dictdel_nonexistent_dict() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Deleting from nonexistent dict should return 0
    let response = client.command(&["FT.DICTDEL", "nodict", "term1"]).await;
    let count = unwrap_integer(&response);
    assert_eq!(count, 0);

    server.shutdown().await;
}

// ============================================================================
// FT.CONFIG GET/SET
// ============================================================================

#[tokio::test]
async fn test_ft_config_get_defaults() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client.command(&["FT.CONFIG", "GET", "*"]).await;
    let arr = unwrap_array(response);
    // Should have 4 config entries (MINPREFIX, MAXEXPANSIONS, TIMEOUT, DEFAULT_DIALECT)
    assert_eq!(arr.len(), 4);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_config_set_and_get() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    // Set a config
    let response = client
        .command(&["FT.CONFIG", "SET", "TIMEOUT", "1000"])
        .await;
    assert_ok(&response);

    // Get it back
    let response = client.command(&["FT.CONFIG", "GET", "TIMEOUT"]).await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 1);

    let pair = unwrap_array(arr[0].clone());
    let key = String::from_utf8(unwrap_bulk(&pair[0]).to_vec()).unwrap();
    let val = String::from_utf8(unwrap_bulk(&pair[1]).to_vec()).unwrap();
    assert_eq!(key, "TIMEOUT");
    assert_eq!(val, "1000");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_config_get_nonexistent() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client.command(&["FT.CONFIG", "GET", "NOSUCHPARAM"]).await;
    let arr = unwrap_array(response);
    assert_eq!(arr.len(), 0);

    server.shutdown().await;
}

// ============================================================================
// FT.SPELLCHECK
// ============================================================================

#[tokio::test]
async fn test_ft_spellcheck_basic() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "hello world"])
        .await;
    client
        .command(&["HSET", "doc:2", "title", "help wanted"])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "spidx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Spellcheck a misspelled term
    let response = client.command(&["FT.SPELLCHECK", "spidx", "helo"]).await;
    let arr = unwrap_array(response);
    // Should have at least 1 term entry
    assert!(
        !arr.is_empty(),
        "Expected spellcheck suggestions for 'helo'"
    );

    // Each entry is [TERM, misspelled_term, [[score, suggestion], ...]]
    let entry = unwrap_array(arr[0].clone());
    let marker = String::from_utf8(unwrap_bulk(&entry[0]).to_vec()).unwrap();
    assert_eq!(marker, "TERM");

    let misspelled = String::from_utf8(unwrap_bulk(&entry[1]).to_vec()).unwrap();
    assert_eq!(misspelled, "helo");

    // Should have suggestions
    let suggestions = unwrap_array(entry[2].clone());
    assert!(!suggestions.is_empty(), "Expected suggestions for 'helo'");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_spellcheck_correct_term() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "hello world"])
        .await;

    client
        .command(&[
            "FT.CREATE",
            "spidx2",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Correctly spelled term should return empty
    let response = client.command(&["FT.SPELLCHECK", "spidx2", "hello"]).await;
    let arr = unwrap_array(response);
    assert!(
        arr.is_empty(),
        "Correctly spelled term should have no suggestions"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_spellcheck_with_dict() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "doc:1", "title", "hello"]).await;

    client
        .command(&[
            "FT.CREATE",
            "spidx3",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Add a custom dictionary
    client.command(&["FT.DICTADD", "custom", "helm"]).await;

    // Spellcheck with INCLUDE dict
    let response = client
        .command(&[
            "FT.SPELLCHECK",
            "spidx3",
            "helo",
            "TERMS",
            "INCLUDE",
            "custom",
        ])
        .await;
    let arr = unwrap_array(response);
    assert!(!arr.is_empty());

    // Check suggestions include "helm" from custom dict
    let entry = unwrap_array(arr[0].clone());
    let suggestions = unwrap_array(entry[2].clone());
    let suggestion_words: Vec<String> = suggestions
        .iter()
        .filter_map(|s| {
            let pair = unwrap_array(s.clone());
            if pair.len() >= 2 {
                Some(String::from_utf8(unwrap_bulk(&pair[1]).to_vec()).unwrap())
            } else {
                None
            }
        })
        .collect();
    assert!(
        suggestion_words.contains(&"helm".to_string()),
        "Custom dict term should appear"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_spellcheck_nonexistent_index() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client.command(&["FT.SPELLCHECK", "noidx", "hello"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}
