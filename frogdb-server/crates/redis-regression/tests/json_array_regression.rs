//! Regression tests for RedisJSON array commands.

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// JSON.ARRAPPEND
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_arrappend_single_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,2,3]}"#])
            .await,
    );

    // single_or_multi: one match → Integer directly
    let resp = client
        .command(&["JSON.ARRAPPEND", "doc", "$.arr", "4"])
        .await;
    assert_integer_eq(&resp, 4);

    let get = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&get)).unwrap();
    assert!(
        body.contains("[1,2,3,4]"),
        "expected appended array, got {body}"
    );
}

#[tokio::test]
async fn json_arrappend_multiple_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[1]}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.ARRAPPEND", "doc", "$.arr", "2", "3", "4"])
        .await;
    assert_integer_eq(&resp, 4);

    let get = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&get)).unwrap();
    assert!(
        body.contains("[1,2,3,4]"),
        "expected appended array, got {body}"
    );
}

#[tokio::test]
async fn json_arrappend_nested_array() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"data":{"items":[10,20]}}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.ARRAPPEND", "doc", "$.data.items", "30"])
        .await;
    assert_integer_eq(&resp, 3);

    let get = client.command(&["JSON.GET", "doc", "$.data.items"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&get)).unwrap();
    assert!(
        body.contains("[10,20,30]"),
        "expected nested append, got {body}"
    );
}

// ---------------------------------------------------------------------------
// JSON.ARRINDEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_arrindex_found() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":["a","b","c","d"]}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.ARRINDEX", "doc", "$.arr", r#""c""#])
        .await;
    assert_integer_eq(&resp, 2);
}

#[tokio::test]
async fn json_arrindex_not_found() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,2,3]}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.ARRINDEX", "doc", "$.arr", "99"])
        .await;
    assert_integer_eq(&resp, -1);
}

#[tokio::test]
async fn json_arrindex_with_start_stop() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[0,1,2,3,4,5]}"#])
            .await,
    );

    // Search for 3 in range [0, 3) — should NOT find it (exclusive stop)
    let resp = client
        .command(&["JSON.ARRINDEX", "doc", "$.arr", "3", "0", "3"])
        .await;
    assert_integer_eq(&resp, -1);

    // Search for 3 in range [0, 4) — should find it at index 3
    let resp = client
        .command(&["JSON.ARRINDEX", "doc", "$.arr", "3", "0", "4"])
        .await;
    assert_integer_eq(&resp, 3);
}

// ---------------------------------------------------------------------------
// JSON.ARRINSERT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_arrinsert_at_beginning() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[2,3]}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.ARRINSERT", "doc", "$.arr", "0", "1"])
        .await;
    assert_integer_eq(&resp, 3);

    let get = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&get)).unwrap();
    assert!(
        body.contains("[1,2,3]"),
        "expected insert at beginning, got {body}"
    );
}

#[tokio::test]
async fn json_arrinsert_at_middle() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,3]}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.ARRINSERT", "doc", "$.arr", "1", "2"])
        .await;
    assert_integer_eq(&resp, 3);

    let get = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&get)).unwrap();
    assert!(
        body.contains("[1,2,3]"),
        "expected insert at middle, got {body}"
    );
}

#[tokio::test]
async fn json_arrinsert_negative_index() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,2,4]}"#])
            .await,
    );

    // Insert at index -1 (before last element)
    let resp = client
        .command(&["JSON.ARRINSERT", "doc", "$.arr", "-1", "3"])
        .await;
    assert_integer_eq(&resp, 4);

    let get = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&get)).unwrap();
    assert!(
        body.contains("[1,2,3,4]") || body.contains("[1,2,4,3]"),
        "expected insert at negative index, got {body}"
    );
}

// ---------------------------------------------------------------------------
// JSON.ARRLEN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_arrlen_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,2,3,4,5]}"#])
            .await,
    );

    let resp = client.command(&["JSON.ARRLEN", "doc", "$.arr"]).await;
    assert_integer_eq(&resp, 5);
}

#[tokio::test]
async fn json_arrlen_empty_array() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[]}"#])
            .await,
    );

    let resp = client.command(&["JSON.ARRLEN", "doc", "$.arr"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn json_arrlen_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["JSON.ARRLEN", "nonexistent", "$.arr"])
        .await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// JSON.ARRPOP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_arrpop_default_last() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[10,20,30]}"#])
            .await,
    );

    // Pop last element (no index argument) — single match returns Bulk directly
    let resp = client.command(&["JSON.ARRPOP", "doc", "$.arr"]).await;
    let popped = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert_eq!(popped, "30");

    let get = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&get)).unwrap();
    assert!(
        body.contains("[10,20]"),
        "expected remaining array, got {body}"
    );
}

#[tokio::test]
async fn json_arrpop_specific_index() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":["a","b","c"]}"#])
            .await,
    );

    let resp = client.command(&["JSON.ARRPOP", "doc", "$.arr", "1"]).await;
    let popped = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert_eq!(popped, r#""b""#);

    let get = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&get)).unwrap();
    assert!(
        body.contains(r#"["a","c"]"#),
        "expected remaining after pop index 1, got {body}"
    );
}

#[tokio::test]
async fn json_arrpop_empty_array() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[]}"#])
            .await,
    );

    // single match on empty array returns nil
    let resp = client.command(&["JSON.ARRPOP", "doc", "$.arr"]).await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// JSON.ARRTRIM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_arrtrim_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[0,1,2,3,4,5]}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.ARRTRIM", "doc", "$.arr", "1", "3"])
        .await;
    assert_integer_eq(&resp, 3);

    let get = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&get)).unwrap();
    assert!(
        body.contains("[1,2,3]"),
        "expected trimmed array, got {body}"
    );
}

#[tokio::test]
async fn json_arrtrim_out_of_range() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,2,3]}"#])
            .await,
    );

    // start > stop → empty array
    let resp = client
        .command(&["JSON.ARRTRIM", "doc", "$.arr", "2", "0"])
        .await;
    assert_integer_eq(&resp, 0);

    let get = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&get)).unwrap();
    assert!(
        body.contains("[]"),
        "expected empty array after trim, got {body}"
    );
}

// ---------------------------------------------------------------------------
// Wildcard array ops
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_arrlen_wildcard() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "wca",
                "$",
                r#"{"items":[{"tags":["a","b"]},{"tags":["c"]},{"tags":["d","e","f"]}]}"#,
            ])
            .await,
    );

    // Wildcard: get lengths of all nested tag arrays
    let resp = client
        .command(&["JSON.ARRLEN", "wca", "$.items[*].tags"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 3);
    assert_integer_eq(&arr[0], 2); // ["a","b"]
    assert_integer_eq(&arr[1], 1); // ["c"]
    assert_integer_eq(&arr[2], 3); // ["d","e","f"]
}

#[tokio::test]
async fn json_arrappend_wildcard() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "wca2",
                "$",
                r#"{"items":[{"tags":["a"]},{"tags":["b"]}]}"#,
            ])
            .await,
    );

    // Append "z" to all tag arrays
    let resp = client
        .command(&["JSON.ARRAPPEND", "wca2", "$.items[*].tags", r#""z""#])
        .await;
    // Multiple matches → array of new lengths
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    assert_integer_eq(&arr[0], 2); // ["a","z"]
    assert_integer_eq(&arr[1], 2); // ["b","z"]

    // Verify
    let resp = client
        .command(&["JSON.GET", "wca2", "$.items[*].tags"])
        .await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert!(body.contains("z"), "expected 'z' appended, got: {body}");
}

#[tokio::test]
async fn json_arrpop_negative_index() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "neg", "$", r#"{"arr":[10,20,30,40]}"#])
            .await,
    );

    // Pop at index -2 (second from end = 30)
    let resp = client.command(&["JSON.ARRPOP", "neg", "$.arr", "-2"]).await;
    let popped = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert_eq!(popped, "30");

    let get = client.command(&["JSON.GET", "neg", "$.arr"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&get)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([[10, 20, 40]]));
}
