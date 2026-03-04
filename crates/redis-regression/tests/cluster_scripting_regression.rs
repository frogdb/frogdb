//! Regression tests for the Redis `unit/cluster/scripting` test suite.
//!
//! FrogDB's cluster scripting support:
//! - `no-cluster` flag: Implemented (parsed in FUNCTION LOAD, shebang in EVAL)
//! - `allow-cross-slot-keys` flag: NOT implemented
//! - Shebang parsing (`#!lua`): Implemented
//! - Cross-slot validation: Implemented (via `allow_cross_slot` server config)

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::{TestServer, TestServerConfig};

/// Start a single-shard server for scripting tests that don't need cross-slot.
async fn start_single_shard_server() -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await
}

// ---------------------------------------------------------------------------
// no-cluster flag — FUNCTION LOAD + FCALL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn no_cluster_flag_allows_zero_keys_function() {
    let server = start_single_shard_server().await;
    let mut client = server.connect().await;

    // Load a function with no-cluster flag — it can be called with 0 keys
    let code = r#"#!lua name=noclusterlib
redis.register_function{
    function_name = 'noclusterfn',
    callback = function(keys, args)
        return 'no-cluster-ok'
    end,
    flags = {'no-cluster'}
}
"#;
    let resp = client.command(&["FUNCTION", "LOAD", code]).await;
    assert_bulk_eq(&resp, b"noclusterlib");

    // Call with 0 keys — should succeed because no-cluster allows it
    let resp = client.command(&["FCALL", "noclusterfn", "0"]).await;
    assert_bulk_eq(&resp, b"no-cluster-ok");
}

#[tokio::test]
async fn no_cluster_flag_function_still_works_with_keys() {
    let server = start_single_shard_server().await;
    let mut client = server.connect().await;

    // Load a function with no-cluster flag
    let code = r#"#!lua name=noclusterlib2
redis.register_function{
    function_name = 'noclusterfn2',
    callback = function(keys, args)
        if #keys > 0 then
            return redis.call('GET', keys[1])
        end
        return 'no-keys'
    end,
    flags = {'no-cluster'}
}
"#;
    let resp = client.command(&["FUNCTION", "LOAD", code]).await;
    assert_bulk_eq(&resp, b"noclusterlib2");

    // Set a key, then call the function with it
    assert_ok(&client.command(&["SET", "mykey", "hello"]).await);
    let resp = client
        .command(&["FCALL", "noclusterfn2", "1", "mykey"])
        .await;
    assert_bulk_eq(&resp, b"hello");
}

// ---------------------------------------------------------------------------
// no-cluster flag — EVAL with shebang
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "EVAL does not parse shebangs — shebang support is FUNCTION LOAD only"]
async fn no_cluster_flag_eval_with_shebang() {
    let server = start_single_shard_server().await;
    let mut client = server.connect().await;

    // EVAL with shebang and no-cluster flag, 0 keys
    // In Redis 7+, EVAL accepts shebangs for flag-based scripts.
    // FrogDB only parses shebangs in FUNCTION LOAD, not EVAL.
    let script = "#!lua flags=no-cluster\nreturn 'eval-no-cluster'";
    let resp = client.command(&["EVAL", script, "0"]).await;
    assert_bulk_eq(&resp, b"eval-no-cluster");
}

// ---------------------------------------------------------------------------
// Shebang scripts — cross-slot behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn eval_without_shebang_single_shard_cross_slot() {
    // Old-style EVAL (no shebang) on a single-shard server: cross-slot
    // is irrelevant since all keys hash to the same shard.
    let server = start_single_shard_server().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "key1", "a"]).await);
    assert_ok(&client.command(&["SET", "key2", "b"]).await);

    // Access two keys that would be in different slots on a multi-shard server
    let resp = client
        .command(&[
            "EVAL",
            "return redis.call('GET', KEYS[1]) .. redis.call('GET', KEYS[2])",
            "2",
            "key1",
            "key2",
        ])
        .await;
    assert_bulk_eq(&resp, b"ab");
}

// ---------------------------------------------------------------------------
// allow-cross-slot-keys flag — NOT implemented
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "allow-cross-slot-keys flag not implemented"]
async fn allow_cross_slot_keys_flag() {
    let server = TestServer::start_standalone().await; // 4 shards
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "key1", "a"]).await);
    assert_ok(&client.command(&["SET", "key2", "b"]).await);

    // Shebang script with allow-cross-slot-keys should allow cross-slot access
    let script = "#!lua flags=allow-cross-slot-keys\nreturn redis.call('GET', KEYS[1]) .. redis.call('GET', KEYS[2])";
    let resp = client.command(&["EVAL", script, "2", "key1", "key2"]).await;
    assert_bulk_eq(&resp, b"ab");
}
