//! Rust port of Redis 8.6.0 `unit/acl-v2.tcl` test suite.
//!
//! Excludes:
//! - `external:skip` (entire file is tagged, but tests are ported for FrogDB)
//! - ACL LOG tests (not yet supported)
//! - ACL LOAD/SAVE file-based tests (second `start_server` block)
//! - MIGRATE-specific DRYRUN tests (MIGRATE not implemented)
//! - GEORADIUS DRYRUN tests (GEORADIUS not implemented)

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Helper: find a field value in an ACL GETUSER flat key-value array
// ---------------------------------------------------------------------------

/// Given the flat key-value array from ACL GETUSER, extract the value for `field`.
fn getuser_field(items: &[Response], field: &str) -> Response {
    for pair in items.chunks(2) {
        if let Response::Bulk(Some(k)) = &pair[0]
            && k.as_ref() == field.as_bytes()
        {
            return pair[1].clone();
        }
    }
    panic!("field {field:?} not found in ACL GETUSER response");
}

/// Extract the selectors array from an ACL GETUSER response.
fn getuser_selectors(items: &[Response]) -> Vec<Response> {
    unwrap_array(getuser_field(items, "selectors"))
}

/// Given a single selector (itself a flat key-value array), extract a field as a string.
fn selector_field_str(selector: &Response, field: &str) -> String {
    let items = match selector {
        Response::Array(items) => items,
        other => panic!("expected selector to be Array, got {other:?}"),
    };
    let val = getuser_field(items, field);
    match val {
        Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
        Response::Bulk(None) => String::new(),
        other => panic!("expected Bulk for selector field {field:?}, got {other:?}"),
    }
}

/// Extract a string field from the top-level GETUSER response.
fn getuser_field_str(items: &[Response], field: &str) -> String {
    let val = getuser_field(items, field);
    match val {
        Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
        Response::Bulk(None) => String::new(),
        other => panic!("expected Bulk for GETUSER field {field:?}, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Test basic multiple selectors
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_basic_multiple_selectors() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "selector-1",
            "on",
            "-@all",
            "resetkeys",
            "nopass",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(&r2.command(&["AUTH", "selector-1", "password"]).await);

    // No permissions yet
    assert_error_prefix(&r2.command(&["PING"]).await, "NOPERM");
    assert_error_prefix(&r2.command(&["SET", "write::foo", "bar"]).await, "NOPERM");
    assert_error_prefix(&r2.command(&["GET", "read::foo"]).await, "NOPERM");

    // Add selectors
    client
        .command(&[
            "ACL",
            "SETUSER",
            "selector-1",
            "(+@write ~write::*)",
            "(+@read ~read::*)",
        ])
        .await;

    assert_ok(&r2.command(&["SET", "write::foo", "bar"]).await);
    // read::foo does not exist, so GET returns nil (not error)
    assert_nil(&r2.command(&["GET", "read::foo"]).await);

    // Cross-namespace access denied
    assert_error_prefix(&r2.command(&["GET", "write::foo"]).await, "NOPERM");
    assert_error_prefix(&r2.command(&["SET", "read::foo", "bar"]).await, "NOPERM");
}

// ---------------------------------------------------------------------------
// Test ACL selectors by default have no permissions
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_selectors_default_no_permissions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "selector-default", "reset", "()"])
        .await;

    let resp = client
        .command(&["ACL", "GETUSER", "selector-default"])
        .await;
    let items = unwrap_array(resp);
    let selectors = getuser_selectors(&items);
    assert_eq!(selectors.len(), 1, "expected exactly 1 selector");

    let sel = &selectors[0];
    assert_eq!(selector_field_str(sel, "keys"), "");
    assert_eq!(selector_field_str(sel, "channels"), "");
    assert_eq!(selector_field_str(sel, "commands"), "-@all");
}

// ---------------------------------------------------------------------------
// Test deleting selectors
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_deleting_selectors() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "selector-del", "on", "(~added-selector)"])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "selector-del"]).await;
    let items = unwrap_array(resp);
    let selectors = getuser_selectors(&items);
    assert_eq!(selectors.len(), 1);
    assert_eq!(selector_field_str(&selectors[0], "keys"), "~added-selector");

    // Clear selectors
    client
        .command(&["ACL", "SETUSER", "selector-del", "clearselectors"])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "selector-del"]).await;
    let items = unwrap_array(resp);
    let selectors = getuser_selectors(&items);
    assert_eq!(selectors.len(), 0);
}

// ---------------------------------------------------------------------------
// Test selector syntax error reports the error in the selector context
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_selector_syntax_error_reports() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Invalid modifier inside selector
    let resp = client
        .command(&[
            "ACL",
            "SETUSER",
            "selector-syntax",
            "on",
            "(this-is-invalid)",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Adding a pattern after the allchannels pattern
    let resp = client
        .command(&["ACL", "SETUSER", "selector-syntax", "on", "(&* &fail)"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Unmatched parenthesis
    let resp = client
        .command(&[
            "ACL",
            "SETUSER",
            "selector-syntax",
            "on",
            "(+PING",
            "(+SELECT",
            "(+DEL",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");

    // User should not have been created
    assert_nil(&client.command(&["ACL", "GETUSER", "selector-syntax"]).await);
}

// ---------------------------------------------------------------------------
// Test flexible selector definition
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_flexible_selector_definition() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Quoted selectors with various spacing
    client
        .command(&[
            "ACL",
            "SETUSER",
            "selector-2",
            "(~key1 +get )",
            "( ~key2 +get )",
            "( ~key3 +get)",
            "(~key4 +get)",
        ])
        .await;

    // Unquoted selectors
    client
        .command(&[
            "ACL",
            "SETUSER",
            "selector-2",
            "(~key5 +get )",
            "( ~key6 +get )",
            "( ~key7 +get)",
            "(~key8 +get)",
        ])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "selector-2"]).await;
    let items = unwrap_array(resp);
    let selectors = getuser_selectors(&items);
    assert_eq!(selectors.len(), 8);

    for (i, expected) in [
        "~key1", "~key2", "~key3", "~key4", "~key5", "~key6", "~key7", "~key8",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            selector_field_str(&selectors[i], "keys"),
            *expected,
            "selector {i} keys mismatch"
        );
    }

    // Invalid selector syntax
    let resp = client
        .command(&["ACL", "SETUSER", "invalid-selector", " () "])
        .await;
    assert_error_prefix(&resp, "ERR");

    let resp = client
        .command(&["ACL", "SETUSER", "invalid-selector", "("])
        .await;
    assert_error_prefix(&resp, "ERR");

    let resp = client
        .command(&["ACL", "SETUSER", "invalid-selector", ")"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// Test separate read permission (%R~)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_separate_read_permission() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "key-permission-R",
            "on",
            "nopass",
            "%R~read*",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(&r2.command(&["AUTH", "key-permission-R", "password"]).await);

    assert_bulk_eq(&r2.command(&["PING"]).await, b"PONG");

    // Pre-populate via admin
    client.command(&["SET", "readstr", "bar"]).await;

    assert_bulk_eq(&r2.command(&["GET", "readstr"]).await, b"bar");

    // Write denied
    assert_error_prefix(&r2.command(&["SET", "readstr", "bar"]).await, "NOPERM");

    // Key outside pattern denied
    assert_error_prefix(&r2.command(&["GET", "notread"]).await, "NOPERM");
}

// ---------------------------------------------------------------------------
// Test separate write permission (%W~)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_separate_write_permission() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "key-permission-W",
            "on",
            "nopass",
            "%W~write*",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(&r2.command(&["AUTH", "key-permission-W", "password"]).await);

    assert_bulk_eq(&r2.command(&["PING"]).await, b"PONG");

    // LPUSH is write-only, should succeed
    let resp = r2.command(&["LPUSH", "writelist", "10"]).await;
    assert!(
        matches!(resp, Response::Integer(_)),
        "expected integer from LPUSH"
    );

    // GET is read, denied on write-only key
    assert_error_prefix(&r2.command(&["GET", "writestr"]).await, "NOPERM");

    // Key outside pattern denied
    assert_error_prefix(&r2.command(&["LPUSH", "notwrite", "10"]).await, "NOPERM");
}

// ---------------------------------------------------------------------------
// Test separate read and write permissions
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "COPY command may not be implemented"]
async fn tcl_separate_read_and_write_permissions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "key-permission-RW",
            "on",
            "nopass",
            "%R~read*",
            "%W~write*",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(&r2.command(&["AUTH", "key-permission-RW", "password"]).await);

    assert_bulk_eq(&r2.command(&["PING"]).await, b"PONG");

    client.command(&["SET", "read", "bar"]).await;
    assert_ok(&r2.command(&["COPY", "read", "write"]).await);

    assert_error_prefix(&r2.command(&["COPY", "write", "read"]).await, "NOPERM");
}

// ---------------------------------------------------------------------------
// Validate read and write permissions format
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_validate_rw_permissions_empty_permission() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["ACL", "SETUSER", "key-permission-RW", "%~"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_validate_rw_permissions_empty_selector() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["ACL", "SETUSER", "key-permission-RW", "%"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_validate_rw_permissions_empty_pattern() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "key-perm-empty",
            "on",
            "nopass",
            "%RW~",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(&r2.command(&["AUTH", "key-perm-empty", "password"]).await);

    // Empty pattern means no key access
    assert_error_prefix(&r2.command(&["SET", "x", "5"]).await, "NOPERM");
}

#[tokio::test]
async fn tcl_validate_rw_permissions_no_pattern() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "key-perm-nopat",
            "on",
            "nopass",
            "%RW",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(&r2.command(&["AUTH", "key-perm-nopat", "password"]).await);

    // No pattern means no key access
    assert_error_prefix(&r2.command(&["SET", "x", "5"]).await, "NOPERM");
}

// ---------------------------------------------------------------------------
// Test separate R/W permissions on different selectors are not additive
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_rw_selectors_not_additive() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "key-permission-RW-selector",
            "on",
            "nopass",
            "(%R~read* +@all)",
            "(%W~write* +@all)",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(
        &r2.command(&["AUTH", "key-permission-RW-selector", "password"])
            .await,
    );

    assert_bulk_eq(&r2.command(&["PING"]).await, b"PONG");

    // Write selector works
    let resp = r2.command(&["LPUSH", "writelist", "10"]).await;
    assert!(matches!(resp, Response::Integer(_)));
    assert_error_prefix(&r2.command(&["GET", "writestr"]).await, "NOPERM");
    assert_error_prefix(&r2.command(&["LPUSH", "notwrite", "10"]).await, "NOPERM");

    // Read selector works
    client.command(&["SET", "readstr", "bar"]).await;
    assert_bulk_eq(&r2.command(&["GET", "readstr"]).await, b"bar");
    assert_error_prefix(&r2.command(&["SET", "readstr", "bar"]).await, "NOPERM");
    assert_error_prefix(&r2.command(&["GET", "notread"]).await, "NOPERM");
}

// ---------------------------------------------------------------------------
// Test SET with separate read permission
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_set_with_separate_read_permission() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "readstr"]).await;
    client
        .command(&[
            "ACL",
            "SETUSER",
            "set-key-permission-R",
            "on",
            "nopass",
            "%R~read*",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(
        &r2.command(&["AUTH", "set-key-permission-R", "password"])
            .await,
    );

    assert_bulk_eq(&r2.command(&["PING"]).await, b"PONG");
    assert_nil(&r2.command(&["GET", "readstr"]).await);

    // No write permission
    assert_error_prefix(&r2.command(&["SET", "readstr", "bar"]).await, "NOPERM");
    assert_error_prefix(
        &r2.command(&["SET", "readstr", "bar", "GET"]).await,
        "NOPERM",
    );
    assert_error_prefix(
        &r2.command(&["SET", "readstr", "bar", "EX", "100"]).await,
        "NOPERM",
    );
    assert_error_prefix(
        &r2.command(&["SET", "readstr", "bar", "KEEPTTL", "NX"])
            .await,
        "NOPERM",
    );
}

// ---------------------------------------------------------------------------
// Test SET with separate write permission
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_set_with_separate_write_permission() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "writestr"]).await;
    client
        .command(&[
            "ACL",
            "SETUSER",
            "set-key-permission-W",
            "on",
            "nopass",
            "%W~write*",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(
        &r2.command(&["AUTH", "set-key-permission-W", "password"])
            .await,
    );

    assert_bulk_eq(&r2.command(&["PING"]).await, b"PONG");
    assert_ok(&r2.command(&["SET", "writestr", "bar"]).await);
    assert_ok(&r2.command(&["SET", "writestr", "get"]).await);

    // No read permission -- SET with GET requires read
    assert_error_prefix(&r2.command(&["SET", "get", "writestr"]).await, "NOPERM");
    assert_error_prefix(
        &r2.command(&["SET", "writestr", "bar", "GET"]).await,
        "NOPERM",
    );
    assert_error_prefix(
        &r2.command(&["SET", "writestr", "bar", "GET", "EX", "100"])
            .await,
        "NOPERM",
    );
    assert_error_prefix(
        &r2.command(&["SET", "writestr", "bar", "GET", "KEEPTTL", "NX"])
            .await,
        "NOPERM",
    );
    assert_error_prefix(
        &r2.command(&["SET", "writestr", "bar", "EX", "get"]).await,
        "NOPERM",
    );
}

// ---------------------------------------------------------------------------
// Test SET with read and write permissions
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_set_with_rw_permissions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "readwrite_str"]).await;
    client
        .command(&[
            "ACL",
            "SETUSER",
            "set-key-permission-RW-selector",
            "on",
            "nopass",
            "%RW~readwrite*",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(
        &r2.command(&["AUTH", "set-key-permission-RW-selector", "password"])
            .await,
    );

    assert_bulk_eq(&r2.command(&["PING"]).await, b"PONG");
    assert_nil(&r2.command(&["GET", "readwrite_str"]).await);

    // "SET readwrite_str bar EX get" should fail with integer parse error
    assert_error_prefix(
        &r2.command(&["SET", "readwrite_str", "bar", "EX", "get"])
            .await,
        "ERR",
    );

    assert_ok(&r2.command(&["SET", "readwrite_str", "bar"]).await);
    assert_bulk_eq(&r2.command(&["GET", "readwrite_str"]).await, b"bar");

    // SET with GET returns previous value
    assert_bulk_eq(
        &r2.command(&["SET", "readwrite_str", "bar2", "GET"]).await,
        b"bar",
    );
    assert_bulk_eq(&r2.command(&["GET", "readwrite_str"]).await, b"bar2");

    // SET with GET and EX
    assert_bulk_eq(
        &r2.command(&["SET", "readwrite_str", "bar3", "GET", "EX", "10"])
            .await,
        b"bar2",
    );
    assert_bulk_eq(&r2.command(&["GET", "readwrite_str"]).await, b"bar3");

    // TTL should be between 5 and 10
    let ttl = unwrap_integer(&r2.command(&["TTL", "readwrite_str"]).await);
    assert!((5..=10).contains(&ttl), "expected TTL in 5..=10, got {ttl}");
}

// ---------------------------------------------------------------------------
// Test BITFIELD with separate read permission
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "BITFIELD command may not be implemented"]
async fn tcl_bitfield_with_read_permission() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "readstr"]).await;
    client
        .command(&[
            "ACL",
            "SETUSER",
            "bitfield-key-permission-R",
            "on",
            "nopass",
            "%R~read*",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(
        &r2.command(&["AUTH", "bitfield-key-permission-R", "password"])
            .await,
    );

    assert_bulk_eq(&r2.command(&["PING"]).await, b"PONG");

    // Read-only BITFIELD GET
    let resp = r2.command(&["BITFIELD", "readstr", "GET", "u4", "0"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);
    assert_integer_eq(&arr[0], 0);

    // Write ops denied
    assert_error_prefix(
        &r2.command(&["BITFIELD", "readstr", "SET", "u4", "0", "1"])
            .await,
        "NOPERM",
    );
    assert_error_prefix(
        &r2.command(&[
            "BITFIELD", "readstr", "GET", "u4", "0", "SET", "u4", "0", "1",
        ])
        .await,
        "NOPERM",
    );
    assert_error_prefix(
        &r2.command(&["BITFIELD", "readstr", "INCRBY", "u4", "0", "1"])
            .await,
        "NOPERM",
    );
}

// ---------------------------------------------------------------------------
// Test BITFIELD with separate write permission
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "BITFIELD command may not be implemented"]
async fn tcl_bitfield_with_write_permission() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "writestr"]).await;
    client
        .command(&[
            "ACL",
            "SETUSER",
            "bitfield-key-permission-W",
            "on",
            "nopass",
            "%W~write*",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(
        &r2.command(&["AUTH", "bitfield-key-permission-W", "password"])
            .await,
    );

    assert_bulk_eq(&r2.command(&["PING"]).await, b"PONG");

    // All BITFIELD ops require read on write-only key
    assert_error_prefix(
        &r2.command(&["BITFIELD", "writestr", "GET", "u4", "0"])
            .await,
        "NOPERM",
    );
    assert_error_prefix(
        &r2.command(&["BITFIELD", "writestr", "SET", "u4", "0", "1"])
            .await,
        "NOPERM",
    );
    assert_error_prefix(
        &r2.command(&["BITFIELD", "writestr", "INCRBY", "u4", "0", "1"])
            .await,
        "NOPERM",
    );
}

// ---------------------------------------------------------------------------
// Test BITFIELD with read and write permissions
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "BITFIELD command may not be implemented"]
async fn tcl_bitfield_with_rw_permissions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "readwrite_str"]).await;
    client
        .command(&[
            "ACL",
            "SETUSER",
            "bitfield-key-permission-RW-selector",
            "on",
            "nopass",
            "%RW~readwrite*",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(
        &r2.command(&["AUTH", "bitfield-key-permission-RW-selector", "password"])
            .await,
    );

    assert_bulk_eq(&r2.command(&["PING"]).await, b"PONG");

    // GET
    let resp = r2
        .command(&["BITFIELD", "readwrite_str", "GET", "u4", "0"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 0);

    // SET returns old value
    let resp = r2
        .command(&["BITFIELD", "readwrite_str", "SET", "u4", "0", "1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 0);

    // INCRBY
    let resp = r2
        .command(&["BITFIELD", "readwrite_str", "INCRBY", "u4", "0", "1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 2);

    // Verify
    let resp = r2
        .command(&["BITFIELD", "readwrite_str", "GET", "u4", "0"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 2);
}

// ---------------------------------------------------------------------------
// Test ACL GETUSER response information
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_getuser_response_information() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set root permissions
    client
        .command(&[
            "ACL",
            "SETUSER",
            "selector-info",
            "-@all",
            "+get",
            "resetchannels",
            "&channel1",
            "%R~foo1",
            "%W~bar1",
            "~baz1",
        ])
        .await;

    // Add a selector
    client
        .command(&[
            "ACL",
            "SETUSER",
            "selector-info",
            "(-@all +set resetchannels &channel2 %R~foo2 %W~bar2 ~baz2)",
        ])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "selector-info"]).await;
    let items = unwrap_array(resp);

    // Root selector fields
    assert_eq!(getuser_field_str(&items, "keys"), "%R~foo1 %W~bar1 ~baz1");
    assert_eq!(getuser_field_str(&items, "channels"), "&channel1");
    assert_eq!(getuser_field_str(&items, "commands"), "-@all +get");

    // Secondary selector
    let selectors = getuser_selectors(&items);
    assert_eq!(selectors.len(), 1);
    assert_eq!(
        selector_field_str(&selectors[0], "keys"),
        "%R~foo2 %W~bar2 ~baz2"
    );
    assert_eq!(selector_field_str(&selectors[0], "channels"), "&channel2");
    assert_eq!(selector_field_str(&selectors[0], "commands"), "-@all +set");
}

// ---------------------------------------------------------------------------
// Test ACL list idempotency
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_acl_list_idempotency() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "user-idempotency",
            "off",
            "-@all",
            "+get",
            "resetchannels",
            "&channel1",
            "%R~foo1",
            "%W~bar1",
            "~baz1",
            "(-@all +set resetchannels &channel2 %R~foo2 %W~bar2 ~baz2)",
        ])
        .await;

    let resp = client.command(&["ACL", "LIST"]).await;
    let rules = extract_bulk_strings(&resp);

    let entry = rules
        .iter()
        .find(|r| r.contains("user-idempotency"))
        .expect("user-idempotency not found in ACL LIST");

    // Root permissions present before selector
    assert!(entry.contains("-@all"), "missing -@all in {entry}");
    assert!(entry.contains("+get"), "missing +get in {entry}");
    assert!(entry.contains("&channel1"), "missing &channel1 in {entry}");
    assert!(entry.contains("%R~foo1"), "missing %R~foo1 in {entry}");
    assert!(entry.contains("%W~bar1"), "missing %W~bar1 in {entry}");
    assert!(entry.contains("~baz1"), "missing ~baz1 in {entry}");

    // Selector present (in parentheses)
    assert!(
        entry.contains("("),
        "missing selector open paren in {entry}"
    );
    assert!(
        entry.contains(")"),
        "missing selector close paren in {entry}"
    );
}

// ---------------------------------------------------------------------------
// Test R+W is the same as all permissions
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_rw_same_as_all_permissions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "selector-rw-info",
            "%R~foo",
            "%W~foo",
            "%RW~bar",
        ])
        .await;

    let resp = client
        .command(&["ACL", "GETUSER", "selector-rw-info"])
        .await;
    let items = unwrap_array(resp);

    // %R~foo + %W~foo should merge to ~foo (full access)
    assert_eq!(getuser_field_str(&items, "keys"), "~foo ~bar");
}

// ---------------------------------------------------------------------------
// Test basic dry run functionality (ACL DRYRUN)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_basic_dryrun_functionality() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "command-test",
            "+@all",
            "%R~read*",
            "%W~write*",
            "%RW~rw*",
        ])
        .await;

    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "command-test", "GET", "read"])
            .await,
    );

    // Non-existent user
    assert_error_prefix(
        &client
            .command(&["ACL", "DRYRUN", "not-a-user", "GET", "read"])
            .await,
        "ERR",
    );

    // Non-existent command
    assert_error_prefix(
        &client
            .command(&["ACL", "DRYRUN", "command-test", "not-a-command", "read"])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// Test various commands for command permissions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_dryrun_command_permissions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "command-test2",
            "+@all",
            "%R~read*",
            "%W~write*",
            "%RW~rw*",
        ])
        .await;

    // Remove all command permissions
    client
        .command(&["ACL", "SETUSER", "command-test2", "-@all"])
        .await;

    // DRYRUN returns a bulk string describing the denial (not an error)
    let resp = client
        .command(&[
            "ACL",
            "DRYRUN",
            "command-test2",
            "SET",
            "somekey",
            "somevalue",
        ])
        .await;
    assert!(
        matches!(resp, Response::Bulk(Some(_))),
        "DRYRUN should return bulk string for denied command, got {resp:?}"
    );

    let resp = client
        .command(&["ACL", "DRYRUN", "command-test2", "GET", "somekey"])
        .await;
    assert!(
        matches!(resp, Response::Bulk(Some(_))),
        "DRYRUN should return bulk string for denied command, got {resp:?}"
    );
}

// ---------------------------------------------------------------------------
// Test various odd commands for key permissions (DRYRUN)
// -- MIGRATE and GEORADIUS tests excluded (not implemented)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "SORT BY/GET ACL interaction may not be implemented"]
async fn tcl_dryrun_sort_key_permissions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "cmd-sort-test",
            "+@all",
            "%R~read*",
            "%W~write*",
            "%RW~rw*",
        ])
        .await;

    // SORT read STORE write
    assert_ok(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-sort-test",
                "SORT",
                "read",
                "STORE",
                "write",
            ])
            .await,
    );

    // SORT read STORE read -- write denied on read key
    assert_error_prefix(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-sort-test",
                "SORT",
                "read",
                "STORE",
                "read",
            ])
            .await,
        "ERR",
    );

    // SORT write STORE write -- read denied on write key
    assert_error_prefix(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-sort-test",
                "SORT",
                "write",
                "STORE",
                "write",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
#[ignore = "EVAL DRYRUN key handling may differ"]
async fn tcl_dryrun_eval_key_permissions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "cmd-eval-test",
            "+@all",
            "%R~read*",
            "%W~write*",
            "%RW~rw*",
        ])
        .await;

    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-eval-test", "EVAL", "", "1", "rw1"])
            .await,
    );

    assert_error_prefix(
        &client
            .command(&["ACL", "DRYRUN", "cmd-eval-test", "EVAL", "", "1", "read"])
            .await,
        "ERR",
    );

    // EVAL_RO with read key should be OK
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-eval-test", "EVAL_RO", "", "1", "rw1"])
            .await,
    );
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-eval-test", "EVAL_RO", "", "1", "read"])
            .await,
    );

    // 0 keys -- "read" is not a key here
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-eval-test", "EVAL", "", "0", "read"])
            .await,
    );

    // Syntax errors are still OK from ACL perspective
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-eval-test", "EVAL", "", "-1", "read"])
            .await,
    );
    assert_ok(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-eval-test",
                "EVAL",
                "",
                "3",
                "rw",
                "rw",
            ])
            .await,
    );
    assert_ok(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-eval-test",
                "EVAL",
                "",
                "3",
                "rw",
                "read",
            ])
            .await,
    );
}

// ---------------------------------------------------------------------------
// Existence test commands are not marked as access
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_dryrun_existence_commands_not_access() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "cmd-exist-test",
            "+@all",
            "%R~read*",
            "%W~write*",
            "%RW~rw*",
        ])
        .await;

    // HEXISTS: read or write key should work, no-permission key should fail
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-exist-test", "HEXISTS", "read", "foo"])
            .await,
    );
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-exist-test", "HEXISTS", "write", "foo"])
            .await,
    );
    assert_error_prefix(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-exist-test",
                "HEXISTS",
                "nothing",
                "foo",
            ])
            .await,
        "ERR",
    );

    // SISMEMBER
    assert_ok(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-exist-test",
                "SISMEMBER",
                "read",
                "foo",
            ])
            .await,
    );
    assert_ok(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-exist-test",
                "SISMEMBER",
                "write",
                "foo",
            ])
            .await,
    );
    assert_error_prefix(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-exist-test",
                "SISMEMBER",
                "nothing",
                "foo",
            ])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// Intersection cardinality commands are access commands
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "SINTERCARD/ZINTERCARD may not be implemented"]
async fn tcl_dryrun_intersection_cardinality_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "cmd-card-test",
            "+@all",
            "%R~read*",
            "%W~write*",
            "%RW~rw*",
        ])
        .await;

    // SINTERCARD
    assert_ok(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-card-test",
                "SINTERCARD",
                "2",
                "read",
                "read",
            ])
            .await,
    );
    assert_error_prefix(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-card-test",
                "SINTERCARD",
                "2",
                "write",
                "read",
            ])
            .await,
        "ERR",
    );

    // ZCOUNT
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-card-test", "ZCOUNT", "read", "0", "1"])
            .await,
    );
    assert_error_prefix(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-card-test",
                "ZCOUNT",
                "write",
                "0",
                "1",
            ])
            .await,
        "ERR",
    );

    // PFCOUNT
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-card-test", "PFCOUNT", "read", "read"])
            .await,
    );
    assert_error_prefix(
        &client
            .command(&["ACL", "DRYRUN", "cmd-card-test", "PFCOUNT", "write", "read"])
            .await,
        "ERR",
    );

    // ZINTERCARD
    assert_ok(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-card-test",
                "ZINTERCARD",
                "2",
                "read",
                "read",
            ])
            .await,
    );
    assert_error_prefix(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "cmd-card-test",
                "ZINTERCARD",
                "2",
                "write",
                "read",
            ])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// Test general keyspace commands require some type of permission
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_dryrun_general_keyspace_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "cmd-ks-test",
            "+@all",
            "%R~read*",
            "%W~write*",
            "%RW~rw*",
        ])
        .await;

    // TOUCH
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "TOUCH", "read"])
            .await,
    );
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "TOUCH", "write"])
            .await,
    );
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "TOUCH", "rw"])
            .await,
    );
    assert_error_prefix(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "TOUCH", "nothing"])
            .await,
        "ERR",
    );

    // EXISTS
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "EXISTS", "read"])
            .await,
    );
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "EXISTS", "write"])
            .await,
    );
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "EXISTS", "rw"])
            .await,
    );
    assert_error_prefix(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "EXISTS", "nothing"])
            .await,
        "ERR",
    );

    // TYPE
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "TYPE", "read"])
            .await,
    );
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "TYPE", "write"])
            .await,
    );
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "TYPE", "rw"])
            .await,
    );
    assert_error_prefix(
        &client
            .command(&["ACL", "DRYRUN", "cmd-ks-test", "TYPE", "nothing"])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// Cardinality commands require some type of permission
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_dryrun_cardinality_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "cmd-clen-test",
            "+@all",
            "%R~read*",
            "%W~write*",
            "%RW~rw*",
        ])
        .await;

    let commands = ["STRLEN", "HLEN", "LLEN", "SCARD", "ZCARD", "XLEN"];
    for cmd in &commands {
        assert_ok(
            &client
                .command(&["ACL", "DRYRUN", "cmd-clen-test", cmd, "read"])
                .await,
        );
        assert_ok(
            &client
                .command(&["ACL", "DRYRUN", "cmd-clen-test", cmd, "write"])
                .await,
        );
        assert_ok(
            &client
                .command(&["ACL", "DRYRUN", "cmd-clen-test", cmd, "rw"])
                .await,
        );
        assert_error_prefix(
            &client
                .command(&["ACL", "DRYRUN", "cmd-clen-test", cmd, "nothing"])
                .await,
            "ERR",
        );
    }
}

// ---------------------------------------------------------------------------
// Test sharded channel permissions
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_dryrun_sharded_channel_permissions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "test-channels",
            "+@all",
            "resetchannels",
            "&channel",
        ])
        .await;

    assert_ok(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "test-channels",
                "SPUBLISH",
                "channel",
                "foo",
            ])
            .await,
    );
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "test-channels", "SSUBSCRIBE", "channel"])
            .await,
    );
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "test-channels", "SUNSUBSCRIBE"])
            .await,
    );
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "test-channels", "SUNSUBSCRIBE", "channel"])
            .await,
    );
    assert_ok(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "test-channels",
                "SUNSUBSCRIBE",
                "otherchannel",
            ])
            .await,
    );

    // Denied channels
    assert_error_prefix(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "test-channels",
                "SPUBLISH",
                "otherchannel",
                "foo",
            ])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&[
                "ACL",
                "DRYRUN",
                "test-channels",
                "SSUBSCRIBE",
                "otherchannel",
                "foo",
            ])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// Test sort with ACL permissions
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "SORT BY/GET ACL restrictions may not be implemented"]
async fn tcl_sort_with_acl_permissions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "v1", "1"]).await;
    client.command(&["LPUSH", "mylist", "1"]).await;

    // User with sort access only to mylist
    client
        .command(&[
            "ACL",
            "SETUSER",
            "test-sort-acl",
            "on",
            "nopass",
            "(+sort ~mylist)",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(&r2.command(&["AUTH", "test-sort-acl", "nopass"]).await);

    // BY denied without pattern access
    let resp = r2.command(&["SORT", "mylist", "BY", "v*"]).await;
    assert_error_prefix(&resp, "ERR");

    // GET denied without pattern access
    let resp = r2.command(&["SORT", "mylist", "GET", "v*"]).await;
    assert_error_prefix(&resp, "ERR");

    // Add selector with ~v* -- still denied (need %R~)
    client
        .command(&["ACL", "SETUSER", "test-sort-acl", "(+sort ~mylist ~v*)"])
        .await;
    let resp = r2.command(&["SORT", "mylist", "BY", "v*"]).await;
    assert_error_prefix(&resp, "ERR");

    // Add selector with %W~* -- still denied (need read, not write)
    client
        .command(&["ACL", "SETUSER", "test-sort-acl", "(+sort ~mylist %W~*)"])
        .await;
    let resp = r2.command(&["SORT", "mylist", "BY", "v*"]).await;
    assert_error_prefix(&resp, "ERR");

    // Add selector with %R~* -- now BY should work
    client
        .command(&["ACL", "SETUSER", "test-sort-acl", "(+sort ~mylist %R~*)"])
        .await;
    let resp = r2.command(&["SORT", "mylist", "BY", "v*"]).await;
    // Should return "1"
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);

    // Cleanup
    client.command(&["ACL", "DELUSER", "test-sort-acl"]).await;
    client.command(&["DEL", "v1", "mylist"]).await;
}

// ---------------------------------------------------------------------------
// Test DRYRUN with wrong number of arguments
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB ACL selectors not yet implemented"]
async fn tcl_dryrun_wrong_number_of_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "test-dry-run", "+@all", "~v*"])
        .await;

    // Correct usage
    assert_ok(
        &client
            .command(&["ACL", "DRYRUN", "test-dry-run", "SET", "v", "v"])
            .await,
    );

    // Too few args for SET
    assert_error_prefix(
        &client
            .command(&["ACL", "DRYRUN", "test-dry-run", "SET", "v"])
            .await,
        "ERR wrong number of arguments",
    );

    assert_error_prefix(
        &client
            .command(&["ACL", "DRYRUN", "test-dry-run", "SET"])
            .await,
        "ERR wrong number of arguments",
    );
}
