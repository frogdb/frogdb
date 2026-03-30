//! Regression tests for RedisJSON numeric, string, and object commands.

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// JSON.NUMINCRBY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_numincrby_integer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"val":10}"#])
            .await,
    );

    // single_or_multi: one match → Bulk(formatted_float) directly
    let resp = client
        .command(&["JSON.NUMINCRBY", "doc", "$.val", "5"])
        .await;
    assert_bulk_eq(&resp, b"15");
}

#[tokio::test]
async fn json_numincrby_float() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"val":2.5}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.NUMINCRBY", "doc", "$.val", "1.5"])
        .await;
    // 2.5 + 1.5 = 4.0, formatted as integer since no fractional part
    assert_bulk_eq(&resp, b"4");
}

#[tokio::test]
async fn json_numincrby_negative() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"val":100}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.NUMINCRBY", "doc", "$.val", "-30"])
        .await;
    assert_bulk_eq(&resp, b"70");
}

#[tokio::test]
async fn json_numincrby_on_non_number() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"name":"alice"}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.NUMINCRBY", "doc", "$.name", "5"])
        .await;
    // Non-number path match → error
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// JSON.NUMMULTBY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_nummultby_integer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"val":7}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.NUMMULTBY", "doc", "$.val", "3"])
        .await;
    assert_bulk_eq(&resp, b"21");
}

#[tokio::test]
async fn json_nummultby_float() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"val":4}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.NUMMULTBY", "doc", "$.val", "2.5"])
        .await;
    assert_bulk_eq(&resp, b"10");
}

// ---------------------------------------------------------------------------
// JSON.STRAPPEND
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_strappend_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"greeting":"hello"}"#])
            .await,
    );

    // single_or_multi: one match → Integer(new_len) directly
    let resp = client
        .command(&["JSON.STRAPPEND", "doc", "$.greeting", r#"" world""#])
        .await;
    assert_integer_eq(&resp, 11);

    let resp = client.command(&["JSON.GET", "doc", "$.greeting"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert!(
        body.contains("hello world"),
        "expected 'hello world', got {body}"
    );
}

#[tokio::test]
async fn json_strappend_to_nested() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "doc",
                "$",
                r#"{"user":{"first":"Jane","last":"Doe"}}"#,
            ])
            .await,
    );

    let resp = client
        .command(&["JSON.STRAPPEND", "doc", "$.user.first", r#""t""#])
        .await;
    // "Jane" + "t" = "Janet" → length 5
    assert_integer_eq(&resp, 5);

    let resp = client.command(&["JSON.GET", "doc", "$.user.first"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert!(body.contains("Janet"), "expected 'Janet', got {body}");
}

#[tokio::test]
async fn json_strappend_on_non_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"count":42}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.STRAPPEND", "doc", "$.count", r#""extra""#])
        .await;
    // Path matches a number, not a string → error
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// JSON.STRLEN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_strlen_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"msg":"frogdb"}"#])
            .await,
    );

    let resp = client.command(&["JSON.STRLEN", "doc", "$.msg"]).await;
    assert_integer_eq(&resp, 6);
}

#[tokio::test]
async fn json_strlen_empty_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"empty":""}"#])
            .await,
    );

    let resp = client.command(&["JSON.STRLEN", "doc", "$.empty"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn json_strlen_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["JSON.STRLEN", "nosuchkey", "$.field"])
        .await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// JSON.OBJKEYS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_objkeys_root() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "doc",
                "$",
                r#"{"name":"alice","age":30,"active":true}"#,
            ])
            .await,
    );

    // single_or_multi: one object match → Array of key names directly
    let resp = client.command(&["JSON.OBJKEYS", "doc", "$"]).await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&"name".to_string()));
    assert!(keys.contains(&"age".to_string()));
    assert!(keys.contains(&"active".to_string()));
}

#[tokio::test]
async fn json_objkeys_nested() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "doc",
                "$",
                r#"{"info":{"city":"pond","zip":"00000"}}"#,
            ])
            .await,
    );

    let resp = client.command(&["JSON.OBJKEYS", "doc", "$.info"]).await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"city".to_string()));
    assert!(keys.contains(&"zip".to_string()));
}

// ---------------------------------------------------------------------------
// JSON.OBJLEN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_objlen_root() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2,"c":3}"#])
            .await,
    );

    // single_or_multi: one match → Integer directly
    let resp = client.command(&["JSON.OBJLEN", "doc", "$"]).await;
    assert_integer_eq(&resp, 3);
}

#[tokio::test]
async fn json_objlen_nested() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "doc",
                "$",
                r#"{"outer":{"x":1,"y":2}}"#,
            ])
            .await,
    );

    let resp = client.command(&["JSON.OBJLEN", "doc", "$.outer"]).await;
    assert_integer_eq(&resp, 2);
}

#[tokio::test]
async fn json_objlen_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["JSON.OBJLEN", "nosuchkey", "$"]).await;
    assert_nil(&resp);
}
