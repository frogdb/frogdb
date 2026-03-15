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
    let response = client
        .command(&["FT.SEARCH", "idx", "@age:[25 35]"])
        .await;
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
        .command(&[
            "FT.ALTER",
            "noindex",
            "SCHEMA",
            "ADD",
            "field",
            "TEXT",
        ])
        .await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ft_alter_duplicate_field() {
    let server = start_server_no_persist().await;
    let mut client = server.connect().await;

    let response = client
        .command(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "SCHEMA",
            "name",
            "TEXT",
        ])
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
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT",
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
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT",
        ])
        .await;
    assert_ok(&response);

    let response = client
        .command(&[
            "FT.SEARCH",
            "idx",
            "hello",
            "HIGHLIGHT",
            "TAGS",
            "[",
            "]",
        ])
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
            "HSET", "doc:1", "title", "hello world", "body", "hello there",
        ])
        .await;

    let response = client
        .command(&[
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT",
            "body", "TEXT",
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
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT",
        ])
        .await;
    client.command(&["HSET", "doc:1", "title", "snapshot search test"]).await;
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
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT",
        ])
        .await;

    // Add synonym group
    let response = client
        .command(&["FT.SYNUPDATE", "idx", "vehicles", "car", "automobile", "vehicle"])
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
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT",
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
        .command(&["FT.SYNUPDATE", "idx", "vehicles", "car", "automobile", "vehicle"])
        .await;

    // Now "car" should find all three docs via synonym expansion
    let response = client.command(&["FT.SEARCH", "idx", "car"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 3, "Synonym expansion should find all three documents");

    // "automobile" should also find all three
    let response = client.command(&["FT.SEARCH", "idx", "automobile"]).await;
    let arr = unwrap_array(response);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 3, "Synonym expansion should work for any term in the group");

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
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT",
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
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT",
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
