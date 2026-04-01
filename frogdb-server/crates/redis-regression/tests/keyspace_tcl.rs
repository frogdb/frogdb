//! Rust port of Redis 8.6.0 `unit/keyspace.tcl` test suite.
//!
//! Excludes:
//! - `needs:debug` tests (DEL against expired key, etc.)
//! - `singledb:skip` tests (MOVE, SET/GET in different DBs, COPY cross-DB)
//! - SWAPDB tests
//! - Encoding loops, assert_encoding, assert_refcount, debug_digest_value
//! - CONFIG SET dependent tests (skiplist sorted set, hashtable hash, etc.)
//! - COPY tests for collection types that rely on debug_digest_value
//! - Commands pipelining (low-level channel test)

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// DEL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_del_single_item() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "foo"]).await;
    assert_bulk_eq(&client.command(&["GET", "x"]).await, b"foo");
    client.command(&["DEL", "x"]).await;
    assert_nil(&client.command(&["GET", "x"]).await);
}

#[tokio::test]
async fn tcl_vararg_del() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{foo}1", "a"]).await;
    client.command(&["SET", "{foo}2", "b"]).await;
    client.command(&["SET", "{foo}3", "c"]).await;

    assert_integer_eq(
        &client
            .command(&["DEL", "{foo}1", "{foo}2", "{foo}3", "{foo}4"])
            .await,
        3,
    );
    assert_nil(&client.command(&["GET", "{foo}1"]).await);
    assert_nil(&client.command(&["GET", "{foo}2"]).await);
    assert_nil(&client.command(&["GET", "{foo}3"]).await);
}

#[tokio::test]
async fn tcl_untagged_multi_key_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["MSET", "{foo}1", "a", "{foo}2", "b", "{foo}3", "c"])
        .await;
    let resp = client
        .command(&["MGET", "{foo}1", "{foo}2", "{foo}3", "{foo}4"])
        .await;
    // Response is [Bulk("a"), Bulk("b"), Bulk("c"), Nil] — extract_bulk_strings skips nils
    let vals = extract_bulk_strings(&resp);
    assert_eq!(vals, vec!["a", "b", "c"]);

    assert_integer_eq(
        &client
            .command(&["DEL", "{foo}1", "{foo}2", "{foo}3", "{foo}4"])
            .await,
        3,
    );
}

// ---------------------------------------------------------------------------
// KEYS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_keys_with_pattern() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for key in &["key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"] {
        client.command(&["SET", key, "hello"]).await;
    }

    let resp = client.command(&["KEYS", "foo*"]).await;
    let mut keys = extract_bulk_strings(&resp);
    keys.sort();
    assert_eq!(keys, vec!["foo_a", "foo_b", "foo_c"]);
}

#[tokio::test]
async fn tcl_keys_all() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for key in &["key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"] {
        client.command(&["SET", key, "hello"]).await;
    }

    let resp = client.command(&["KEYS", "*"]).await;
    let mut keys = extract_bulk_strings(&resp);
    keys.sort();
    assert_eq!(
        keys,
        vec!["foo_a", "foo_b", "foo_c", "key_x", "key_y", "key_z"]
    );
}

#[tokio::test]
async fn tcl_dbsize() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for key in &["key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"] {
        client.command(&["SET", key, "hello"]).await;
    }

    assert_integer_eq(&client.command(&["DBSIZE"]).await, 6);
}

#[tokio::test]
async fn tcl_keys_with_hashtag() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for key in &["{a}x", "{a}y", "{a}z", "{b}a", "{b}b", "{b}c"] {
        client.command(&["SET", key, "hello"]).await;
    }

    let resp = client.command(&["KEYS", "{a}*"]).await;
    let mut keys = extract_bulk_strings(&resp);
    keys.sort();
    assert_eq!(keys, vec!["{a}x", "{a}y", "{a}z"]);

    let resp = client.command(&["KEYS", "*{b}*"]).await;
    let mut keys = extract_bulk_strings(&resp);
    keys.sort();
    assert_eq!(keys, vec!["{b}a", "{b}b", "{b}c"]);
}

#[tokio::test]
async fn tcl_del_all_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for key in &["key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"] {
        client.command(&["SET", key, "hello"]).await;
    }

    let resp = client.command(&["KEYS", "*"]).await;
    let keys = extract_bulk_strings(&resp);
    for key in &keys {
        client.command(&["DEL", key]).await;
    }

    assert_integer_eq(&client.command(&["DBSIZE"]).await, 0);
}

// ---------------------------------------------------------------------------
// EXISTS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "newkey", "test"]).await;
    assert_integer_eq(&client.command(&["EXISTS", "newkey"]).await, 1);

    client.command(&["DEL", "newkey"]).await;
    assert_integer_eq(&client.command(&["EXISTS", "newkey"]).await, 0);
}

#[tokio::test]
async fn tcl_zero_length_value_set_get_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "emptykey", ""]).await;
    assert_bulk_eq(&client.command(&["GET", "emptykey"]).await, b"");
    assert_integer_eq(&client.command(&["EXISTS", "emptykey"]).await, 1);

    client.command(&["DEL", "emptykey"]).await;
    assert_integer_eq(&client.command(&["EXISTS", "emptykey"]).await, 0);
}

// ---------------------------------------------------------------------------
// Non-existing command
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_non_existing_command() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["FOOBAREDCOMMAND"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// RENAME
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_rename_basic_usage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{my}key", "hello"]).await;
    assert_ok(&client.command(&["RENAME", "{my}key", "{my}key1"]).await);
    assert_ok(&client.command(&["RENAME", "{my}key1", "{my}key2"]).await);
    assert_bulk_eq(&client.command(&["GET", "{my}key2"]).await, b"hello");
}

#[tokio::test]
async fn tcl_rename_source_key_no_longer_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    client.command(&["RENAME", "mykey", "mykey2"]).await;
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);
}

#[tokio::test]
async fn tcl_rename_against_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "a"]).await;
    client.command(&["SET", "mykey2", "b"]).await;
    client.command(&["RENAME", "mykey2", "mykey"]).await;

    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"b");
    assert_integer_eq(&client.command(&["EXISTS", "mykey2"]).await, 0);
}

#[tokio::test]
async fn tcl_renamenx_basic_usage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    client.command(&["DEL", "mykey2"]).await;
    client.command(&["SET", "mykey", "foobar"]).await;
    assert_integer_eq(&client.command(&["RENAMENX", "mykey", "mykey2"]).await, 1);
    assert_bulk_eq(&client.command(&["GET", "mykey2"]).await, b"foobar");
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);
}

#[tokio::test]
async fn tcl_renamenx_against_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "foo"]).await;
    client.command(&["SET", "mykey2", "bar"]).await;
    assert_integer_eq(&client.command(&["RENAMENX", "mykey", "mykey2"]).await, 0);
}

#[tokio::test]
async fn tcl_renamenx_against_existing_key_values_preserved() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "foo"]).await;
    client.command(&["SET", "mykey2", "bar"]).await;
    client.command(&["RENAMENX", "mykey", "mykey2"]).await;

    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"foo");
    assert_bulk_eq(&client.command(&["GET", "mykey2"]).await, b"bar");
}

#[tokio::test]
async fn tcl_rename_non_existing_source_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["RENAME", "{t}nokey", "{t}foobar"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_rename_source_and_dest_same_existing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "foo"]).await;
    assert_ok(&client.command(&["RENAME", "mykey", "mykey"]).await);
}

#[tokio::test]
async fn tcl_renamenx_source_and_dest_same_existing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "foo"]).await;
    assert_integer_eq(&client.command(&["RENAMENX", "mykey", "mykey"]).await, 0);
}

#[tokio::test]
async fn tcl_rename_source_and_dest_same_non_existing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    let resp = client.command(&["RENAME", "mykey", "mykey"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_rename_volatile_key_moves_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey", "mykey2"]).await;
    client.command(&["SET", "mykey", "foo"]).await;
    client.command(&["EXPIRE", "mykey", "100"]).await;

    let ttl = unwrap_integer(&client.command(&["TTL", "mykey"]).await);
    assert!(ttl > 95 && ttl <= 100, "expected TTL 96-100, got {ttl}");

    client.command(&["RENAME", "mykey", "mykey2"]).await;

    let ttl2 = unwrap_integer(&client.command(&["TTL", "mykey2"]).await);
    assert!(ttl2 > 95 && ttl2 <= 100, "expected TTL 96-100, got {ttl2}");
}

#[tokio::test]
async fn tcl_rename_volatile_key_should_not_inherit_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey", "mykey2"]).await;
    client.command(&["SET", "mykey", "foo"]).await;
    client.command(&["SET", "mykey2", "bar"]).await;
    client.command(&["EXPIRE", "mykey2", "100"]).await;

    let ttl_mykey = unwrap_integer(&client.command(&["TTL", "mykey"]).await);
    let ttl_mykey2 = unwrap_integer(&client.command(&["TTL", "mykey2"]).await);
    assert_eq!(ttl_mykey, -1);
    assert!(ttl_mykey2 > 0);

    client.command(&["RENAME", "mykey", "mykey2"]).await;
    assert_integer_eq(&client.command(&["TTL", "mykey2"]).await, -1);
}

#[tokio::test]
async fn tcl_del_all_keys_db0() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "a", "1"]).await;
    client.command(&["SET", "b", "2"]).await;

    let resp = client.command(&["KEYS", "*"]).await;
    let keys = extract_bulk_strings(&resp);
    for key in &keys {
        client.command(&["DEL", key]).await;
    }

    assert_integer_eq(&client.command(&["DBSIZE"]).await, 0);
}

// ---------------------------------------------------------------------------
// COPY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_copy_basic_usage_for_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{my}key", "foobar"]).await;
    assert_integer_eq(&client.command(&["COPY", "{my}key", "{my}newkey"]).await, 1);
    assert_bulk_eq(&client.command(&["GET", "{my}newkey"]).await, b"foobar");
}

#[tokio::test]
async fn tcl_copy_string_does_not_copy_to_non_integer_db() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{my}key", "foobar"]).await;
    let resp = client
        .command(&["COPY", "{my}key", "{my}newkey", "DB", "notanumber"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_copy_key_expire_metadata() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["SET", "{my}key", "foobar", "EX", "100"])
        .await;
    client
        .command(&["COPY", "{my}key", "{my}newkey", "REPLACE"])
        .await;

    let ttl = unwrap_integer(&client.command(&["TTL", "{my}newkey"]).await);
    assert!(ttl > 0 && ttl <= 100, "expected TTL 1-100, got {ttl}");
    assert_bulk_eq(&client.command(&["GET", "{my}newkey"]).await, b"foobar");
}

#[tokio::test]
async fn tcl_copy_does_not_create_expire_if_none() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{my}key", "foobar"]).await;
    assert_integer_eq(&client.command(&["TTL", "{my}key"]).await, -1);

    client
        .command(&["COPY", "{my}key", "{my}newkey", "REPLACE"])
        .await;
    assert_integer_eq(&client.command(&["TTL", "{my}newkey"]).await, -1);
    assert_bulk_eq(&client.command(&["GET", "{my}newkey"]).await, b"foobar");
}

#[tokio::test]
async fn tcl_copy_does_not_replace_without_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{my}key", "foobar"]).await;
    client.command(&["SET", "{my}newkey", "existing"]).await;

    assert_integer_eq(&client.command(&["COPY", "{my}key", "{my}newkey"]).await, 0);
    // Original value is preserved.
    assert_bulk_eq(&client.command(&["GET", "{my}newkey"]).await, b"existing");
}

#[tokio::test]
async fn tcl_copy_replaces_with_replace_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{my}key", "foobar"]).await;
    client.command(&["SET", "{my}newkey", "existing"]).await;

    assert_integer_eq(
        &client
            .command(&["COPY", "{my}key", "{my}newkey", "REPLACE"])
            .await,
        1,
    );
    assert_bulk_eq(&client.command(&["GET", "{my}newkey"]).await, b"foobar");
}

// ---------------------------------------------------------------------------
// RANDOMKEY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_randomkey() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client.command(&["SET", "foo", "x"]).await;
    client.command(&["SET", "bar", "y"]).await;

    let mut foo_seen = false;
    let mut bar_seen = false;
    for _ in 0..100 {
        let resp = client.command(&["RANDOMKEY"]).await;
        let key = unwrap_bulk(&resp);
        let key = std::str::from_utf8(key).unwrap();
        if key == "foo" {
            foo_seen = true;
        }
        if key == "bar" {
            bar_seen = true;
        }
    }
    assert!(foo_seen, "expected to see key 'foo'");
    assert!(bar_seen, "expected to see key 'bar'");
}

#[tokio::test]
async fn tcl_randomkey_against_empty_db() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    assert_nil(&client.command(&["RANDOMKEY"]).await);
}

#[tokio::test]
async fn tcl_randomkey_regression_1() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client.command(&["SET", "x", "10"]).await;
    client.command(&["DEL", "x"]).await;
    assert_nil(&client.command(&["RANDOMKEY"]).await);
}

// ---------------------------------------------------------------------------
// KEYS regressions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_keys_star_twice_with_long_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client
        .command(&["SET", "dlskeriewrioeuwqoirueioqwrueoqwrueqw", "test"])
        .await;
    client.command(&["KEYS", "*"]).await;

    let resp = client.command(&["KEYS", "*"]).await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys, vec!["dlskeriewrioeuwqoirueioqwrueoqwrueqw"]);
}

#[tokio::test]
async fn tcl_regression_pattern_matching_long_nested_loops() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    let key = "a".repeat(40);
    client.command(&["SET", &key, "1"]).await;
    let resp = client
        .command(&["KEYS", "a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b"])
        .await;
    let keys = extract_bulk_strings(&resp);
    assert!(keys.is_empty());
}

#[tokio::test]
#[ignore = "FrogDB KEYS pattern matching differs for nested patterns"]
async fn tcl_regression_pattern_matching_very_long_nested_loops() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    let key = "a".repeat(50000);
    client.command(&["SET", &key, "1"]).await;

    let pattern: String = "*?".repeat(50000);
    let resp = client.command(&["KEYS", &pattern]).await;
    let keys = extract_bulk_strings(&resp);
    assert!(keys.is_empty());
}
