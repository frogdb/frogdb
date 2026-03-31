//! Regression tests for FT.* (RediSearch) commands: index lifecycle, text
//! search, numeric/tag/geo filters, aggregation, suggestions, aliases,
//! dictionaries, synonyms, and vector search.

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Assert the response is an Error (FT.* errors don't always start with "ERR").
fn assert_is_error(resp: &frogdb_protocol::Response) {
    assert!(
        matches!(resp, frogdb_protocol::Response::Error(_)),
        "expected Error, got {resp:?}"
    );
}

/// Find a field value in a flat key-value response array (e.g. FT.INFO).
fn info_field<'a>(
    items: &'a [frogdb_protocol::Response],
    label: &str,
) -> &'a frogdb_protocol::Response {
    for i in (0..items.len()).step_by(2) {
        if let frogdb_protocol::Response::Bulk(Some(b)) = &items[i]
            && std::str::from_utf8(b).unwrap() == label
        {
            return &items[i + 1];
        }
        if let frogdb_protocol::Response::Simple(s) = &items[i]
            && s == label
        {
            return &items[i + 1];
        }
    }
    panic!("field {label:?} not found in response");
}

/// Create an index on HASH with given prefix and schema fields, then wait for
/// background indexing to settle.
async fn create_index_and_wait(
    client: &mut frogdb_test_harness::server::TestClient,
    index: &str,
    prefix: &str,
    schema: &[&str],
) {
    let mut args: Vec<&str> = vec![
        "FT.CREATE",
        index,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
    ];
    args.extend_from_slice(schema);
    assert_ok(&client.command(&args).await);
    tokio::time::sleep(Duration::from_millis(200)).await;
}

/// Extract a field value from the fields sub-array of an FT.SEARCH result.
/// `fields` is the Array([field_name, field_val, ...]) for one document.
fn search_field(fields: &frogdb_protocol::Response, name: &str) -> String {
    if let frogdb_protocol::Response::Array(items) = fields {
        for chunk in items.chunks(2) {
            let fname = std::str::from_utf8(unwrap_bulk(&chunk[0])).unwrap();
            if fname == name {
                return std::str::from_utf8(unwrap_bulk(&chunk[1]))
                    .unwrap()
                    .to_string();
            }
        }
    }
    panic!("field {name:?} not found in search result: {fields:?}");
}

// ===========================================================================
// FT.CREATE — Index Lifecycle
// ===========================================================================

#[tokio::test]
async fn ft_create_hash_schema() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
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
            "score",
            "NUMERIC",
            "tags",
            "TAG",
        ])
        .await;
    assert_ok(&resp);
}

#[tokio::test]
async fn ft_create_duplicate_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "name", "TEXT"])
            .await,
    );

    let resp = client
        .command(&["FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "name", "TEXT"])
        .await;
    assert_is_error(&resp);
}

#[tokio::test]
async fn ft_create_invalid_schema() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Missing SCHEMA keyword
    let resp = client
        .command(&["FT.CREATE", "idx", "ON", "HASH", "name", "TEXT"])
        .await;
    assert_is_error(&resp);
}

// ===========================================================================
// FT.ALTER
// ===========================================================================

#[tokio::test]
async fn ft_alter_add_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
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
            .await,
    );

    client
        .command(&["HSET", "doc:1", "title", "hello", "score", "42"])
        .await;

    assert_ok(
        &client
            .command(&[
                "FT.ALTER", "idx", "SCHEMA", "ADD", "score", "NUMERIC", "SORTABLE",
            ])
            .await,
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    let resp = client
        .command(&["FT.SEARCH", "idx", "@score:[40 50]"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);
}

#[tokio::test]
async fn ft_alter_nonexistent_index() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["FT.ALTER", "nosuchidx", "SCHEMA", "ADD", "f", "TEXT"])
        .await;
    assert_is_error(&resp);
}

// ===========================================================================
// FT.DROPINDEX
// ===========================================================================

#[tokio::test]
async fn ft_dropindex_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "name", "TEXT"])
            .await,
    );

    assert_ok(&client.command(&["FT.DROPINDEX", "idx"]).await);

    // Searching dropped index should error
    let resp = client.command(&["FT.SEARCH", "idx", "*"]).await;
    assert_is_error(&resp);
}

#[tokio::test]
async fn ft_dropindex_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["FT.DROPINDEX", "nosuchidx"]).await;
    assert_is_error(&resp);
}

// ===========================================================================
// FT._LIST
// ===========================================================================

#[tokio::test]
async fn ft_list_indexes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["FT.CREATE", "idx1", "ON", "HASH", "SCHEMA", "a", "TEXT"])
            .await,
    );
    assert_ok(
        &client
            .command(&["FT.CREATE", "idx2", "ON", "HASH", "SCHEMA", "b", "TEXT"])
            .await,
    );

    let resp = client.command(&["FT._LIST"]).await;
    let names = extract_bulk_strings(&resp);
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"idx1".to_string()));
    assert!(names.contains(&"idx2".to_string()));
}

// ===========================================================================
// FT.INFO
// ===========================================================================

#[tokio::test]
async fn ft_info_shows_schema() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
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
                "score",
                "NUMERIC",
            ])
            .await,
    );

    client
        .command(&["HSET", "doc:1", "title", "hello", "score", "10"])
        .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let resp = client.command(&["FT.INFO", "idx"]).await;
    let arr = unwrap_array(resp);

    // Should contain "index_name" → "idx"
    let name = info_field(&arr, "index_name");
    assert_bulk_eq(name, b"idx");

    // Should contain "attributes" with 2 fields
    let attrs = info_field(&arr, "attributes");
    if let frogdb_protocol::Response::Array(items) = attrs {
        assert_eq!(items.len(), 2, "expected 2 schema fields");
    } else {
        panic!("expected attributes to be an Array");
    }
}

#[tokio::test]
async fn ft_info_nonexistent_index() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["FT.INFO", "nosuchidx"]).await;
    assert_is_error(&resp);
}

// ===========================================================================
// FT.SEARCH — text queries
// ===========================================================================

#[tokio::test]
async fn ft_search_text_single_word() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "hello world"])
        .await;
    client
        .command(&["HSET", "doc:2", "title", "goodbye world"])
        .await;

    create_index_and_wait(&mut client, "idx", "doc:", &["title", "TEXT"]).await;

    let resp = client.command(&["FT.SEARCH", "idx", "hello"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);
    assert_bulk_eq(&arr[1], b"doc:1");
}

#[tokio::test]
async fn ft_search_no_matches() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "doc:1", "title", "hello"]).await;
    create_index_and_wait(&mut client, "idx", "doc:", &["title", "TEXT"]).await;

    let resp = client.command(&["FT.SEARCH", "idx", "zzzzz"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 0);
    assert_eq!(arr.len(), 1); // just the total count
}

#[tokio::test]
async fn ft_search_nonexistent_index() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["FT.SEARCH", "nosuchidx", "hello"]).await;
    assert_is_error(&resp);
}

#[tokio::test]
async fn ft_search_field_specific() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "hello", "body", "world"])
        .await;
    client
        .command(&["HSET", "doc:2", "title", "world", "body", "hello"])
        .await;

    create_index_and_wait(
        &mut client,
        "idx",
        "doc:",
        &["title", "TEXT", "body", "TEXT"],
    )
    .await;

    // Search only in title field
    let resp = client.command(&["FT.SEARCH", "idx", "@title:hello"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);
    assert_bulk_eq(&arr[1], b"doc:1");
}

#[tokio::test]
async fn ft_search_wildcard_all() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "doc:1", "name", "Alice"]).await;
    client.command(&["HSET", "doc:2", "name", "Bob"]).await;

    create_index_and_wait(&mut client, "idx", "doc:", &["name", "TEXT"]).await;

    // "*" matches all documents
    let resp = client.command(&["FT.SEARCH", "idx", "*"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 2);
}

// ===========================================================================
// FT.SEARCH — options (NOCONTENT, WITHSCORES, LIMIT, SORTBY, RETURN)
// ===========================================================================

#[tokio::test]
async fn ft_search_nocontent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "doc:1", "title", "hello"]).await;
    client.command(&["HSET", "doc:2", "title", "hello"]).await;

    create_index_and_wait(&mut client, "idx", "doc:", &["title", "TEXT"]).await;

    let resp = client
        .command(&["FT.SEARCH", "idx", "hello", "NOCONTENT"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 2);
    // NOCONTENT: [total, key1, key2] — no field arrays
    assert_eq!(arr.len(), 3);
}

#[tokio::test]
async fn ft_search_limit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..5 {
        client
            .command(&["HSET", &format!("doc:{i}"), "title", "common"])
            .await;
    }

    create_index_and_wait(&mut client, "idx", "doc:", &["title", "TEXT"]).await;

    let resp = client
        .command(&["FT.SEARCH", "idx", "common", "LIMIT", "0", "2"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 5); // total matches
    // But only 2 results returned: [total, key1, fields1, key2, fields2]
    assert_eq!(arr.len(), 5);
}

#[tokio::test]
async fn ft_search_sortby() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "name", "a", "score", "30"])
        .await;
    client
        .command(&["HSET", "doc:2", "name", "b", "score", "10"])
        .await;
    client
        .command(&["HSET", "doc:3", "name", "c", "score", "20"])
        .await;

    create_index_and_wait(
        &mut client,
        "idx",
        "doc:",
        &["name", "TEXT", "score", "NUMERIC", "SORTABLE"],
    )
    .await;

    let resp = client
        .command(&["FT.SEARCH", "idx", "*", "SORTBY", "score", "ASC"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 3);
    // First result should be doc:2 (score=10)
    assert_bulk_eq(&arr[1], b"doc:2");
}

#[tokio::test]
async fn ft_search_return_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "HSET", "doc:1", "title", "hello", "body", "world", "score", "42",
        ])
        .await;

    create_index_and_wait(
        &mut client,
        "idx",
        "doc:",
        &["title", "TEXT", "body", "TEXT", "score", "NUMERIC"],
    )
    .await;

    // Only return title field
    let resp = client
        .command(&["FT.SEARCH", "idx", "hello", "RETURN", "1", "title"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);

    if let frogdb_protocol::Response::Array(ref fields) = arr[2] {
        // Should only have title field (2 items: name + value)
        assert_eq!(fields.len(), 2, "RETURN 1 title should return only title");
        assert_bulk_eq(&fields[0], b"title");
        assert_bulk_eq(&fields[1], b"hello");
    } else {
        panic!("expected fields array");
    }
}

// ===========================================================================
// FT.SEARCH — numeric filter
// ===========================================================================

#[tokio::test]
async fn ft_search_numeric_range() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "name", "a", "age", "25"])
        .await;
    client
        .command(&["HSET", "doc:2", "name", "b", "age", "35"])
        .await;
    client
        .command(&["HSET", "doc:3", "name", "c", "age", "45"])
        .await;

    create_index_and_wait(
        &mut client,
        "idx",
        "doc:",
        &["name", "TEXT", "age", "NUMERIC"],
    )
    .await;

    let resp = client.command(&["FT.SEARCH", "idx", "@age:[30 50]"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 2); // 35 and 45
}

// ===========================================================================
// FT.SEARCH — TAG filter
// ===========================================================================

#[tokio::test]
async fn ft_search_tag_single() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "name", "a", "color", "red"])
        .await;
    client
        .command(&["HSET", "doc:2", "name", "b", "color", "blue"])
        .await;
    client
        .command(&["HSET", "doc:3", "name", "c", "color", "red"])
        .await;

    create_index_and_wait(
        &mut client,
        "idx",
        "doc:",
        &["name", "TEXT", "color", "TAG"],
    )
    .await;

    let resp = client.command(&["FT.SEARCH", "idx", "@color:{red}"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 2);
}

#[tokio::test]
async fn ft_search_tag_multi() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "name", "a", "color", "red"])
        .await;
    client
        .command(&["HSET", "doc:2", "name", "b", "color", "blue"])
        .await;
    client
        .command(&["HSET", "doc:3", "name", "c", "color", "green"])
        .await;

    create_index_and_wait(
        &mut client,
        "idx",
        "doc:",
        &["name", "TEXT", "color", "TAG"],
    )
    .await;

    // red OR blue
    let resp = client
        .command(&["FT.SEARCH", "idx", "@color:{red|blue}"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 2);
}

// ===========================================================================
// FT.SEARCH — HIGHLIGHT
// ===========================================================================

#[tokio::test]
async fn ft_search_highlight() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "hello world"])
        .await;

    create_index_and_wait(&mut client, "idx", "doc:", &["title", "TEXT"]).await;

    let resp = client
        .command(&["FT.SEARCH", "idx", "hello", "HIGHLIGHT"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);

    let title = search_field(&arr[2], "title");
    assert!(
        title.contains("<b>"),
        "expected <b> highlight tag in: {title}"
    );
}

// ===========================================================================
// FT.SEARCH — prefix matching
// ===========================================================================

#[tokio::test]
async fn ft_search_prefix() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "helicopter"])
        .await;
    client.command(&["HSET", "doc:2", "title", "hello"]).await;
    client.command(&["HSET", "doc:3", "title", "world"]).await;

    create_index_and_wait(&mut client, "idx", "doc:", &["title", "TEXT"]).await;

    let resp = client.command(&["FT.SEARCH", "idx", "hel*"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 2); // helicopter + hello
}

// ===========================================================================
// FT.AGGREGATE
// ===========================================================================

#[tokio::test]
async fn ft_aggregate_groupby_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "a", "category", "books"])
        .await;
    client
        .command(&["HSET", "doc:2", "title", "b", "category", "electronics"])
        .await;
    client
        .command(&["HSET", "doc:3", "title", "c", "category", "books"])
        .await;

    create_index_and_wait(
        &mut client,
        "idx",
        "doc:",
        &["title", "TEXT", "category", "TAG"],
    )
    .await;

    let resp = client
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
    let arr = unwrap_array(resp);
    let total = unwrap_integer(&arr[0]);
    assert_eq!(total, 2); // 2 groups: books, electronics
}

#[tokio::test]
async fn ft_aggregate_groupby_sum() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "a", "cat", "A", "price", "10"])
        .await;
    client
        .command(&["HSET", "doc:2", "title", "b", "cat", "A", "price", "20"])
        .await;
    client
        .command(&["HSET", "doc:3", "title", "c", "cat", "B", "price", "30"])
        .await;

    create_index_and_wait(
        &mut client,
        "idx",
        "doc:",
        &["title", "TEXT", "cat", "TAG", "price", "NUMERIC"],
    )
    .await;

    let resp = client
        .command(&[
            "FT.AGGREGATE",
            "idx",
            "*",
            "GROUPBY",
            "1",
            "@cat",
            "REDUCE",
            "SUM",
            "1",
            "@price",
            "AS",
            "total",
            "SORTBY",
            "2",
            "@cat",
            "ASC",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 2);

    // Verify groups exist (TAG values may be lowercased)
    // Find the group where cat = "a" (or "A") and check its total
    let mut found_a = false;
    for item in arr.iter().skip(1) {
        if let frogdb_protocol::Response::Array(group) = item {
            let cat_val = std::str::from_utf8(unwrap_bulk(&group[1]))
                .unwrap()
                .to_lowercase();
            let total_val = std::str::from_utf8(unwrap_bulk(&group[3])).unwrap();
            if cat_val == "a" {
                assert_eq!(total_val, "30", "cat A total should be 30");
                found_a = true;
            }
        }
    }
    assert!(found_a, "expected group with cat=a");
}

#[tokio::test]
async fn ft_aggregate_with_limit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..5 {
        client
            .command(&[
                "HSET",
                &format!("doc:{i}"),
                "title",
                &format!("item{i}"),
                "cat",
                &format!("cat{i}"),
            ])
            .await;
    }

    create_index_and_wait(&mut client, "idx", "doc:", &["title", "TEXT", "cat", "TAG"]).await;

    let resp = client
        .command(&[
            "FT.AGGREGATE",
            "idx",
            "*",
            "GROUPBY",
            "1",
            "@cat",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "cnt",
            "LIMIT",
            "0",
            "2",
        ])
        .await;
    let arr = unwrap_array(resp);
    // Total groups = 5, but LIMIT 0 2 → only 2 groups returned
    // arr = [total, group1, group2]
    assert_eq!(arr.len(), 3);
}

// ===========================================================================
// FT.SUGADD / FT.SUGGET / FT.SUGDEL / FT.SUGLEN
// ===========================================================================

#[tokio::test]
async fn ft_sug_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add suggestions
    let resp = client
        .command(&["FT.SUGADD", "ac", "hello world", "1.0"])
        .await;
    assert_eq!(unwrap_integer(&resp), 1);

    let resp = client
        .command(&["FT.SUGADD", "ac", "hello there", "2.0"])
        .await;
    assert_eq!(unwrap_integer(&resp), 2);

    // Get suggestions for prefix "hel"
    let resp = client.command(&["FT.SUGGET", "ac", "hel"]).await;
    let suggestions = extract_bulk_strings(&resp);
    assert_eq!(suggestions.len(), 2);
    // Higher score should come first
    assert_eq!(suggestions[0], "hello there");
    assert_eq!(suggestions[1], "hello world");
}

#[tokio::test]
async fn ft_suglen() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FT.SUGADD", "ac", "foo", "1"]).await;
    client.command(&["FT.SUGADD", "ac", "bar", "1"]).await;
    client.command(&["FT.SUGADD", "ac", "baz", "1"]).await;

    let resp = client.command(&["FT.SUGLEN", "ac"]).await;
    assert_integer_eq(&resp, 3);
}

#[tokio::test]
async fn ft_sugdel() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FT.SUGADD", "ac", "hello", "1"]).await;
    client.command(&["FT.SUGADD", "ac", "help", "1"]).await;

    let resp = client.command(&["FT.SUGDEL", "ac", "hello"]).await;
    assert_integer_eq(&resp, 1);

    let resp = client.command(&["FT.SUGLEN", "ac"]).await;
    assert_integer_eq(&resp, 1);

    // Delete non-existent
    let resp = client.command(&["FT.SUGDEL", "ac", "nope"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn ft_sugget_fuzzy() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FT.SUGADD", "ac", "hello", "1"]).await;

    // "helo" (typo) with FUZZY should still match "hello"
    let resp = client.command(&["FT.SUGGET", "ac", "helo", "FUZZY"]).await;
    let suggestions = extract_bulk_strings(&resp);
    assert!(
        suggestions.contains(&"hello".to_string()),
        "fuzzy should match 'hello', got: {suggestions:?}"
    );
}

#[tokio::test]
async fn ft_sugget_max() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..10 {
        client
            .command(&["FT.SUGADD", "ac", &format!("word{i}"), "1"])
            .await;
    }

    let resp = client
        .command(&["FT.SUGGET", "ac", "word", "MAX", "3"])
        .await;
    let suggestions = extract_bulk_strings(&resp);
    assert_eq!(suggestions.len(), 3);
}

#[tokio::test]
async fn ft_sugget_withscores() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FT.SUGADD", "ac", "hello", "5.0"]).await;

    let resp = client
        .command(&["FT.SUGGET", "ac", "hel", "WITHSCORES"])
        .await;
    let arr = unwrap_array(resp);
    // [suggestion, score, ...]
    assert!(arr.len() >= 2);
    assert_bulk_eq(&arr[0], b"hello");
    // Score should be present as bulk string
    let _score = std::str::from_utf8(unwrap_bulk(&arr[1])).unwrap();
}

// ===========================================================================
// FT.ALIASADD / FT.ALIASDEL / FT.ALIASUPDATE
// ===========================================================================

#[tokio::test]
async fn ft_alias_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "doc:1", "name", "Alice"]).await;

    create_index_and_wait(&mut client, "idx", "doc:", &["name", "TEXT"]).await;

    // Add alias
    assert_ok(&client.command(&["FT.ALIASADD", "myalias", "idx"]).await);

    // Search via alias
    let resp = client.command(&["FT.SEARCH", "myalias", "Alice"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);
}

#[tokio::test]
async fn ft_alias_update_and_delete() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["FT.CREATE", "idx1", "ON", "HASH", "SCHEMA", "a", "TEXT"])
            .await,
    );
    assert_ok(
        &client
            .command(&["FT.CREATE", "idx2", "ON", "HASH", "SCHEMA", "b", "TEXT"])
            .await,
    );

    assert_ok(&client.command(&["FT.ALIASADD", "al", "idx1"]).await);

    // Update alias to point to idx2
    assert_ok(&client.command(&["FT.ALIASUPDATE", "al", "idx2"]).await);

    // Delete alias
    assert_ok(&client.command(&["FT.ALIASDEL", "al"]).await);

    // Searching via deleted alias should error
    let resp = client.command(&["FT.SEARCH", "al", "*"]).await;
    assert_is_error(&resp);
}

// ===========================================================================
// FT.DICTADD / FT.DICTDEL / FT.DICTDUMP
// ===========================================================================

#[tokio::test]
async fn ft_dict_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["FT.DICTADD", "mydict", "hello", "world", "foo"])
        .await;
    assert_integer_eq(&resp, 3);

    let resp = client.command(&["FT.DICTDUMP", "mydict"]).await;
    let terms = extract_bulk_strings(&resp);
    assert_eq!(terms.len(), 3);
    assert!(terms.contains(&"hello".to_string()));
    assert!(terms.contains(&"world".to_string()));
    assert!(terms.contains(&"foo".to_string()));
}

#[tokio::test]
async fn ft_dictdel() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["FT.DICTADD", "mydict", "a", "b", "c"])
        .await;

    // Delete 1 existing + 1 non-existing
    let resp = client
        .command(&["FT.DICTDEL", "mydict", "a", "nonexistent"])
        .await;
    assert_integer_eq(&resp, 1); // only "a" was removed

    let resp = client.command(&["FT.DICTDUMP", "mydict"]).await;
    let terms = extract_bulk_strings(&resp);
    assert_eq!(terms.len(), 2);
}

#[tokio::test]
async fn ft_dictadd_duplicates() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["FT.DICTADD", "mydict", "hello", "world"])
        .await;

    // Add again with one new term
    let resp = client
        .command(&["FT.DICTADD", "mydict", "hello", "new"])
        .await;
    assert_integer_eq(&resp, 1); // only "new" is added
}

// ===========================================================================
// FT.SYNUPDATE / FT.SYNDUMP
// ===========================================================================

#[tokio::test]
async fn ft_synonym_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "title", "TEXT"])
            .await,
    );

    assert_ok(
        &client
            .command(&[
                "FT.SYNUPDATE",
                "idx",
                "vehicle",
                "car",
                "automobile",
                "truck",
            ])
            .await,
    );

    let resp = client.command(&["FT.SYNDUMP", "idx"]).await;
    let arr = unwrap_array(resp);
    // Should contain entries mapping each term to the group
    assert!(arr.len() >= 6, "expected at least 3 term→group pairs");
}

// ===========================================================================
// FT.TAGVALS
// ===========================================================================

#[tokio::test]
async fn ft_tagvals() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "name", "a", "color", "red"])
        .await;
    client
        .command(&["HSET", "doc:2", "name", "b", "color", "blue"])
        .await;
    client
        .command(&["HSET", "doc:3", "name", "c", "color", "red"])
        .await;

    create_index_and_wait(
        &mut client,
        "idx",
        "doc:",
        &["name", "TEXT", "color", "TAG"],
    )
    .await;

    let resp = client.command(&["FT.TAGVALS", "idx", "color"]).await;
    let vals = extract_bulk_strings(&resp);
    assert_eq!(vals.len(), 2);
    assert!(vals.contains(&"red".to_string()));
    assert!(vals.contains(&"blue".to_string()));
}

// ===========================================================================
// FT.SEARCH — GEO filter
// ===========================================================================

#[tokio::test]
async fn ft_search_geo_filter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Central Park area coordinates
    client
        .command(&[
            "HSET",
            "place:1",
            "name",
            "park",
            "location",
            "-73.9654,40.7829",
        ])
        .await;
    // Far away
    client
        .command(&["HSET", "place:2", "name", "remote", "location", "0.0,0.0"])
        .await;

    create_index_and_wait(
        &mut client,
        "idx",
        "place:",
        &["name", "TEXT", "location", "GEO"],
    )
    .await;

    let resp = client
        .command(&["FT.SEARCH", "idx", "@location:[-73.9654 40.7829 10 km]"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);
    assert_bulk_eq(&arr[1], b"place:1");
}

// ===========================================================================
// FT.SEARCH — WITHSCORES
// ===========================================================================

#[tokio::test]
async fn ft_search_withscores() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "doc:1", "title", "hello world"])
        .await;

    create_index_and_wait(&mut client, "idx", "doc:", &["title", "TEXT"]).await;

    let resp = client
        .command(&["FT.SEARCH", "idx", "hello", "WITHSCORES"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);
    // With scores: [total, key, score, fields]
    assert_bulk_eq(&arr[1], b"doc:1");
    // Score should be a bulk string (float)
    let _score = std::str::from_utf8(unwrap_bulk(&arr[2])).unwrap();
}

// ===========================================================================
// FT.CONFIG
// ===========================================================================

#[tokio::test]
async fn ft_config_get_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // GET a config value
    let resp = client.command(&["FT.CONFIG", "GET", "TIMEOUT"]).await;
    // Should return without error
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "FT.CONFIG GET TIMEOUT should not error: {resp:?}"
    );
}

// ===========================================================================
// FT.EXPLAIN
// ===========================================================================

#[tokio::test]
async fn ft_explain() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "title", "TEXT"])
            .await,
    );

    let resp = client.command(&["FT.EXPLAIN", "idx", "hello world"]).await;
    // Should return a bulk string with the query plan
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "FT.EXPLAIN should not error: {resp:?}"
    );
}

// ===========================================================================
// FT.EXPLAINCLI
// ===========================================================================

#[tokio::test]
async fn ft_explaincli() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "title", "TEXT"])
            .await,
    );

    let resp = client
        .command(&["FT.EXPLAINCLI", "idx", "hello world"])
        .await;
    // Should return an array of strings (CLI-formatted query plan)
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "FT.EXPLAINCLI should not error: {resp:?}"
    );
}

// ===========================================================================
// FT.CURSOR — cursor-based iteration for FT.AGGREGATE results
// ===========================================================================

#[tokio::test]
async fn ft_cursor_read_and_del() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    create_index_and_wait(
        &mut client,
        "idx",
        "doc:",
        &["title", "TEXT", "score", "NUMERIC"],
    )
    .await;

    // Insert enough docs to require cursor pagination
    for i in 0..20 {
        client
            .command(&[
                "HSET",
                &format!("doc:{i}"),
                "title",
                &format!("document {i}"),
                "score",
                &i.to_string(),
            ])
            .await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // FT.AGGREGATE with WITHCURSOR and small COUNT to force pagination
    let resp = client
        .command(&[
            "FT.AGGREGATE",
            "idx",
            "*",
            "LOAD",
            "1",
            "@title",
            "WITHCURSOR",
            "COUNT",
            "5",
        ])
        .await;
    // Response should be [results_array, cursor_id]
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "FT.AGGREGATE WITHCURSOR should not error: {resp:?}"
    );

    let top = unwrap_array(resp);
    assert_eq!(top.len(), 2, "expected [results, cursor_id], got {top:?}");

    let cursor_id_str = match &top[1] {
        frogdb_protocol::Response::Integer(n) => n.to_string(),
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        other => panic!("expected cursor id, got {other:?}"),
    };

    // If cursor_id is non-zero, read next page
    if cursor_id_str != "0" {
        let resp2 = client
            .command(&["FT.CURSOR", "READ", "idx", &cursor_id_str, "COUNT", "5"])
            .await;
        assert!(
            !matches!(resp2, frogdb_protocol::Response::Error(_)),
            "FT.CURSOR READ should not error: {resp2:?}"
        );

        // Delete the cursor
        let del_resp = client
            .command(&["FT.CURSOR", "DEL", "idx", &cursor_id_str])
            .await;
        assert!(
            !matches!(del_resp, frogdb_protocol::Response::Error(_)),
            "FT.CURSOR DEL should not error: {del_resp:?}"
        );
    }
}

// ===========================================================================
// FT.SPELLCHECK — spell checking with custom dictionaries
// ===========================================================================

#[tokio::test]
async fn ft_spellcheck_with_dictionary() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    create_index_and_wait(&mut client, "idx", "doc:", &["title", "TEXT"]).await;

    client
        .command(&["HSET", "doc:1", "title", "hello world"])
        .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Add terms to a custom dictionary
    let resp = client
        .command(&["FT.DICTADD", "mydict", "hello", "helo", "world"])
        .await;
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "FT.DICTADD should not error: {resp:?}"
    );

    // Spellcheck a misspelled query
    let resp = client
        .command(&[
            "FT.SPELLCHECK",
            "idx",
            "helo wrld",
            "TERMS",
            "INCLUDE",
            "mydict",
        ])
        .await;
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "FT.SPELLCHECK should not error: {resp:?}"
    );
}

// ===========================================================================
// FT.PROFILE — query profiling
// ===========================================================================

#[tokio::test]
async fn ft_profile_search() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    create_index_and_wait(&mut client, "idx", "doc:", &["title", "TEXT"]).await;

    client
        .command(&["HSET", "doc:1", "title", "hello world"])
        .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let resp = client
        .command(&["FT.PROFILE", "idx", "SEARCH", "QUERY", "hello"])
        .await;
    // Should return [search_results, profile_info]
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "FT.PROFILE should not error: {resp:?}"
    );
}
