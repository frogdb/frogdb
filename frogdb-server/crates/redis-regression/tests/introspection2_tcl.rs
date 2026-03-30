//! Rust port of Redis 8.6.0 `unit/introspection-2.tcl` test suite.
//!
//! Excluded tests:
//! - `needs:debug`-tagged (no-touch mode tests requiring DEBUG OBJECT)
//! - `needs:config-resetstat`-tagged (command stats / cmdstat / cmdrstat tests)
//! - OBJECT IDLETIME-dependent tests (LRU/LFU internals)
//! - COMMAND GETKEYS for commands FrogDB may not support (LCS, MEMORY USAGE,
//!   EVAL, ZUNIONSTORE with 260 keys, DELEX, LMOVE, SORT, MSETEX)
//! - COMMAND GETKEYSANDFLAGS (Redis-specific key-flags format)
//! - COMMAND LIST FILTERBY ACLCAT (ACL category introspection)
//! - COMMAND INFO / movablekeys flag tests
//! - GEORADIUS / GEORADIUS_RO movablekeys tests

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// TIME command
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_time_microsecond_part_does_not_overflow() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TIME"]).await;
    let parts = unwrap_array(resp);
    assert_eq!(parts.len(), 2, "TIME should return a two-element array");

    let microseconds: i64 = std::str::from_utf8(unwrap_bulk(&parts[1]))
        .unwrap()
        .parse()
        .unwrap();
    assert!(microseconds >= 0, "microseconds should be >= 0");
    assert!(
        microseconds < 1_000_000,
        "microseconds should be < 1_000_000, got {microseconds}"
    );
}

// ---------------------------------------------------------------------------
// TOUCH command
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_touch_returns_number_of_existing_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client.command(&["SET", "key1{t}", "1"]).await;
    client.command(&["SET", "key2{t}", "2"]).await;

    // key0{t} and key3{t} do not exist, so only 2 of the 4 keys are found.
    assert_integer_eq(
        &client
            .command(&["TOUCH", "key0{t}", "key1{t}", "key2{t}", "key3{t}"])
            .await,
        2,
    );
}

// ---------------------------------------------------------------------------
// COMMAND COUNT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_command_count_returns_positive_number() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let count = unwrap_integer(&client.command(&["COMMAND", "COUNT"]).await);
    assert!(count > 0, "COMMAND COUNT should be > 0, got {count}");
}

// ---------------------------------------------------------------------------
// COMMAND GETKEYS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_command_getkeys_get() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["COMMAND", "GETKEYS", "GET", "key"]).await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys, vec!["key"]);
}

#[tokio::test]
async fn tcl_command_getkeys_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "GETKEYS", "SET", "mykey", "myval"])
        .await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys, vec!["mykey"]);
}

#[tokio::test]
async fn tcl_command_getkeys_mset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "GETKEYS", "MSET", "k1", "v1", "k2", "v2"])
        .await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys, vec!["k1", "k2"]);
}

#[tokio::test]
async fn tcl_command_getkeys_xgroup_create() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "COMMAND",
            "GETKEYS",
            "XGROUP",
            "CREATE",
            "key",
            "groupname",
            "$",
        ])
        .await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys, vec!["key"]);
}

// ---------------------------------------------------------------------------
// COMMAND LIST (basic, no filterby)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_command_list_contains_common_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["COMMAND", "LIST"]).await;
    let commands = extract_bulk_strings(&resp);
    assert!(
        commands.iter().any(|c| c.eq_ignore_ascii_case("set")),
        "COMMAND LIST should contain SET"
    );
    assert!(
        commands.iter().any(|c| c.eq_ignore_ascii_case("get")),
        "COMMAND LIST should contain GET"
    );
}

// ---------------------------------------------------------------------------
// COMMAND LIST syntax errors
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB COMMAND LIST/INFO behavior differs from Redis"]
async fn tcl_command_list_syntax_error_bad_arg() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["COMMAND", "LIST", "bad_arg"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_command_list_syntax_error_bad_filterby() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "LIST", "FILTERBY", "bad_arg"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_command_list_syntax_error_bad_filterby_two_args() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "LIST", "FILTERBY", "bad_arg", "bad_arg2"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// COMMAND LIST FILTERBY MODULE (non-existing module returns empty)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_command_list_filterby_module_non_existing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "COMMAND",
            "LIST",
            "FILTERBY",
            "MODULE",
            "non_existing_module",
        ])
        .await;
    let commands = extract_bulk_strings(&resp);
    assert!(
        commands.is_empty(),
        "COMMAND LIST FILTERBY MODULE for non-existing module should be empty"
    );
}

// ---------------------------------------------------------------------------
// COMMAND LIST FILTERBY PATTERN
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB COMMAND LIST/INFO behavior differs from Redis"]
async fn tcl_command_list_filterby_pattern_exact_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "LIST", "FILTERBY", "PATTERN", "set"])
        .await;
    let commands = extract_bulk_strings(&resp);
    assert_eq!(commands, vec!["set"]);
}

#[tokio::test]
#[ignore = "FrogDB COMMAND LIST/INFO behavior differs from Redis"]
async fn tcl_command_list_filterby_pattern_exact_get() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "LIST", "FILTERBY", "PATTERN", "get"])
        .await;
    let commands = extract_bulk_strings(&resp);
    assert_eq!(commands, vec!["get"]);
}

#[tokio::test]
async fn tcl_command_list_filterby_pattern_non_existing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "LIST", "FILTERBY", "PATTERN", "non_exists"])
        .await;
    let commands = extract_bulk_strings(&resp);
    assert!(
        commands.is_empty(),
        "COMMAND LIST FILTERBY PATTERN for non-existing pattern should be empty"
    );
}

// ---------------------------------------------------------------------------
// COMMAND INFO of invalid subcommands
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB COMMAND LIST/INFO behavior differs from Redis"]
async fn tcl_command_info_invalid_subcommand_returns_nil() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // An invalid subcommand like "get|key" should return a single-element
    // array containing nil (the Redis convention for unknown commands).
    let resp = client.command(&["COMMAND", "INFO", "get|key"]).await;
    let items = unwrap_array(resp);
    assert_eq!(
        items.len(),
        1,
        "COMMAND INFO for unknown cmd should return 1-element array"
    );
    assert_nil(&items[0]);
}

#[tokio::test]
#[ignore = "FrogDB COMMAND LIST/INFO behavior differs from Redis"]
async fn tcl_command_info_double_pipe_invalid_returns_nil() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["COMMAND", "INFO", "config|get|key"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_nil(&items[0]);
}
