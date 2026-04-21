//! Rust port of Redis 8.6.0 `unit/type/hash.tcl` test suite.
//!
//! Excludes: encoding-specific tests (listpack/hashtable), random fuzzing,
//! `needs:repl`, `needs:debug`, DUMP/RESTORE, config-dependent, and
//! chi-square statistical distribution tests.
//!
//! ## Intentional exclusions
//!
//! Encoding-specific tests (FrogDB has a single internal encoding, not listpack/hashtable):
//! - `Is the small hash encoded with a listpack?` — intentional-incompatibility:encoding — internal-encoding
//! - `Is the big hash encoded with an hash table?` — intentional-incompatibility:encoding — internal-encoding
//! - `Is a ziplist encoded Hash promoted on big payload?` — intentional-incompatibility:encoding — internal-encoding
//! - `HGET against the small hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HGET against the big hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HMSET - small hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HMSET - big hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HMGET - small hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HMGET - big hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HKEYS - small hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HKEYS - big hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HVALS - small hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HVALS - big hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HGETALL - small hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HGETALL - big hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HSTRLEN against the small hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HSTRLEN against the big hash` — intentional-incompatibility:encoding — internal-encoding
//! - `HRANDFIELD - $type` — intentional-incompatibility:encoding — internal-encoding
//! - `Stress test the hash ziplist -> hashtable encoding conversion` — intentional-incompatibility:encoding — internal-encoding
//! - `Hash ziplist of various encodings` — intentional-incompatibility:encoding — internal-encoding
//! - `Hash ziplist of various encodings - sanitize dump` — intentional-incompatibility:encoding — internal-encoding
//!
//! Fuzzing/stress tests:
//! (none — Hash fuzzing #1 and #2 are now ported)
//!
//! RESP3 variants:
//! - `HRANDFIELD with RESP3` — intentional-incompatibility:protocol — RESP3-only
//!
//! Replication-propagation tests:
//! - `HGETDEL propagated as HDEL command to replica` — intentional-incompatibility:replication — replication-internal
//!
//! Config-dependent (`allow_access_expired`):
//! - `KEYS command return expired keys when allow_access_expired is 1` — intentional-incompatibility:config — Redis-internal config flag

use std::collections::HashMap;

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

// ---------------------------------------------------------------------------
// HSET / HLEN / HGET basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hset_hlen_small_hash_creation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..8 {
        client
            .command(&["HSET", "smallhash", &format!("key{i}"), &format!("val{i}")])
            .await;
    }
    assert_integer_eq(&client.command(&["HLEN", "smallhash"]).await, 8);
}

#[tokio::test]
async fn tcl_hset_hlen_big_hash_creation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..1024 {
        client
            .command(&["HSET", "bighash", &format!("key{i}"), &format!("val{i}")])
            .await;
    }
    assert_integer_eq(&client.command(&["HLEN", "bighash"]).await, 1024);
}

#[tokio::test]
async fn tcl_hget_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "f1", "v1"]).await;
    assert_nil(&client.command(&["HGET", "myhash", "__123123123__"]).await);
}

#[tokio::test]
async fn tcl_hset_in_update_and_insert_mode() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "existing", "old"]).await;

    // Update returns 0
    assert_integer_eq(
        &client
            .command(&["HSET", "myhash", "existing", "newval"])
            .await,
        0,
    );
    assert_bulk_eq(
        &client.command(&["HGET", "myhash", "existing"]).await,
        b"newval",
    );

    // Insert returns 1
    assert_integer_eq(
        &client
            .command(&["HSET", "myhash", "newfield", "newval"])
            .await,
        1,
    );
}

// ---------------------------------------------------------------------------
// HSETNX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hsetnx_target_key_missing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_integer_eq(&client.command(&["HSETNX", "myhash", "f1", "foo"]).await, 1);
    assert_bulk_eq(&client.command(&["HGET", "myhash", "f1"]).await, b"foo");
}

#[tokio::test]
async fn tcl_hsetnx_target_key_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "f1", "foo"]).await;
    assert_integer_eq(&client.command(&["HSETNX", "myhash", "f1", "bar"]).await, 0);
    assert_bulk_eq(&client.command(&["HGET", "myhash", "f1"]).await, b"foo");
}

// ---------------------------------------------------------------------------
// HSET/HMSET wrong number of args
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hset_hmset_wrong_number_of_args() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(
        &client
            .command(&["HSET", "myhash", "key1", "val1", "key2"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["HMSET", "myhash", "key1", "val1", "key2"])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// HMGET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hmget_against_non_existing_key_and_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Against non-existing hash
    let resp = client
        .command(&["HMGET", "doesntexist", "__123__", "__456__"])
        .await;
    let items = unwrap_array(resp);
    assert_nil(&items[0]);
    assert_nil(&items[1]);

    // Against existing hash with non-existing fields
    client.command(&["HSET", "myhash", "a", "1"]).await;
    let resp = client
        .command(&["HMGET", "myhash", "__123__", "__456__"])
        .await;
    let items = unwrap_array(resp);
    assert_nil(&items[0]);
    assert_nil(&items[1]);
}

// ---------------------------------------------------------------------------
// Hash commands against wrong type
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hash_commands_against_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "wrongtype", "somevalue"]).await;

    assert_error_prefix(
        &client
            .command(&["HMGET", "wrongtype", "field1", "field2"])
            .await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client.command(&["HRANDFIELD", "wrongtype"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client.command(&["HGET", "wrongtype", "field1"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client.command(&["HGETALL", "wrongtype"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client.command(&["HDEL", "wrongtype", "field1"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client
            .command(&["HINCRBY", "wrongtype", "field1", "2"])
            .await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client
            .command(&["HINCRBYFLOAT", "wrongtype", "field1", "2.5"])
            .await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client.command(&["HSTRLEN", "wrongtype", "field1"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(&client.command(&["HVALS", "wrongtype"]).await, "WRONGTYPE");
    assert_error_prefix(&client.command(&["HKEYS", "wrongtype"]).await, "WRONGTYPE");
    assert_error_prefix(
        &client.command(&["HEXISTS", "wrongtype", "field1"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client
            .command(&["HSET", "wrongtype", "field1", "val1"])
            .await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client
            .command(&["HMSET", "wrongtype", "field1", "val1", "field2", "val2"])
            .await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client
            .command(&["HSETNX", "wrongtype", "field1", "val1"])
            .await,
        "WRONGTYPE",
    );
    assert_error_prefix(&client.command(&["HLEN", "wrongtype"]).await, "WRONGTYPE");
    assert_error_prefix(
        &client.command(&["HSCAN", "wrongtype", "0"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client
            .command(&["HGETDEL", "wrongtype", "FIELDS", "1", "a"])
            .await,
        "WRONGTYPE",
    );
}

// ---------------------------------------------------------------------------
// HGETALL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hgetall_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "htest"]).await;
    let resp = client.command(&["HGETALL", "htest"]).await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

// ---------------------------------------------------------------------------
// HDEL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hdel_more_than_a_single_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HMSET", "myhash", "a", "1", "b", "2", "c", "3"])
        .await;
    assert_integer_eq(&client.command(&["HDEL", "myhash", "x", "y"]).await, 0);
    assert_integer_eq(&client.command(&["HDEL", "myhash", "a", "c", "f"]).await, 2);
    let resp = client.command(&["HGETALL", "myhash"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["b", "2"]);
}

#[tokio::test]
async fn tcl_hdel_hash_becomes_empty_before_deleting_all_specified_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HMSET", "myhash", "a", "1", "b", "2", "c", "3"])
        .await;
    assert_integer_eq(
        &client
            .command(&["HDEL", "myhash", "a", "b", "c", "d", "e"])
            .await,
        3,
    );
    assert_integer_eq(&client.command(&["EXISTS", "myhash"]).await, 0);
}

// ---------------------------------------------------------------------------
// HEXISTS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hexists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "f1", "v1"]).await;
    assert_integer_eq(&client.command(&["HEXISTS", "myhash", "f1"]).await, 1);
    assert_integer_eq(&client.command(&["HEXISTS", "myhash", "nokey"]).await, 0);
}

// ---------------------------------------------------------------------------
// HINCRBY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hincrby_against_non_existing_database_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "htest"]).await;
    assert_integer_eq(&client.command(&["HINCRBY", "htest", "foo", "2"]).await, 2);
}

#[tokio::test]
async fn tcl_hincrby_hincrbyfloat_against_non_integer_increment() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "incrhash"]).await;
    client.command(&["HSET", "incrhash", "field", "5"]).await;
    assert_error_prefix(
        &client.command(&["HINCRBY", "incrhash", "field", "v"]).await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["HINCRBYFLOAT", "incrhash", "field", "v"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_hincrby_against_non_existing_hash_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "existing", "1"]).await;
    client.command(&["HDEL", "myhash", "tmp"]).await;
    assert_integer_eq(&client.command(&["HINCRBY", "myhash", "tmp", "2"]).await, 2);
    assert_bulk_eq(&client.command(&["HGET", "myhash", "tmp"]).await, b"2");
}

#[tokio::test]
async fn tcl_hincrby_against_hash_key_created_by_hincrby_itself() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HINCRBY", "myhash", "tmp", "2"]).await;
    assert_integer_eq(&client.command(&["HINCRBY", "myhash", "tmp", "3"]).await, 5);
    assert_bulk_eq(&client.command(&["HGET", "myhash", "tmp"]).await, b"5");
}

#[tokio::test]
async fn tcl_hincrby_against_hash_key_originally_set_with_hset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "tmp", "100"]).await;
    assert_integer_eq(
        &client.command(&["HINCRBY", "myhash", "tmp", "2"]).await,
        102,
    );
}

#[tokio::test]
async fn tcl_hincrby_over_32bit_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "tmp", "17179869184"])
        .await;
    assert_integer_eq(
        &client.command(&["HINCRBY", "myhash", "tmp", "1"]).await,
        17179869185,
    );
}

#[tokio::test]
async fn tcl_hincrby_over_32bit_value_with_over_32bit_increment() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "tmp", "17179869184"])
        .await;
    assert_integer_eq(
        &client
            .command(&["HINCRBY", "myhash", "tmp", "17179869184"])
            .await,
        34359738368,
    );
}

#[tokio::test]
async fn tcl_hincrby_fails_against_hash_value_with_spaces_left() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "str", " 11"]).await;
    assert_error_prefix(
        &client.command(&["HINCRBY", "myhash", "str", "1"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_hincrby_fails_against_hash_value_with_spaces_right() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "str", "11 "]).await;
    assert_error_prefix(
        &client.command(&["HINCRBY", "myhash", "str", "1"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_hincrby_can_detect_overflows() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "hash"]).await;
    client
        .command(&["HSET", "hash", "n", "-9223372036854775484"])
        .await;
    assert_integer_eq(
        &client.command(&["HINCRBY", "hash", "n", "-1"]).await,
        -9223372036854775485,
    );
    assert_error_prefix(
        &client.command(&["HINCRBY", "hash", "n", "-10000"]).await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// HINCRBYFLOAT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hincrbyfloat_against_non_existing_database_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "htest"]).await;
    assert_bulk_eq(
        &client
            .command(&["HINCRBYFLOAT", "htest", "foo", "2.5"])
            .await,
        b"2.5",
    );
}

#[tokio::test]
async fn tcl_hincrbyfloat_against_non_existing_hash_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    assert_bulk_eq(
        &client
            .command(&["HINCRBYFLOAT", "myhash", "tmp", "2.5"])
            .await,
        b"2.5",
    );
    assert_bulk_eq(&client.command(&["HGET", "myhash", "tmp"]).await, b"2.5");
}

#[tokio::test]
async fn tcl_hincrbyfloat_against_hash_key_created_by_hincrbyfloat() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HINCRBYFLOAT", "myhash", "tmp", "2.5"])
        .await;
    assert_bulk_eq(
        &client
            .command(&["HINCRBYFLOAT", "myhash", "tmp", "3.5"])
            .await,
        b"6",
    );
    assert_bulk_eq(&client.command(&["HGET", "myhash", "tmp"]).await, b"6");
}

#[tokio::test]
async fn tcl_hincrbyfloat_against_hash_key_originally_set_with_hset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "tmp", "100"]).await;
    assert_bulk_eq(
        &client
            .command(&["HINCRBYFLOAT", "myhash", "tmp", "2.5"])
            .await,
        b"102.5",
    );
}

#[tokio::test]
async fn tcl_hincrbyfloat_over_32bit_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "tmp", "17179869184"])
        .await;
    assert_bulk_eq(
        &client
            .command(&["HINCRBYFLOAT", "myhash", "tmp", "1"])
            .await,
        b"17179869185",
    );
}

#[tokio::test]
async fn tcl_hincrbyfloat_over_32bit_with_over_32bit_increment() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "tmp", "17179869184"])
        .await;
    assert_bulk_eq(
        &client
            .command(&["HINCRBYFLOAT", "myhash", "tmp", "17179869184"])
            .await,
        b"34359738368",
    );
}

#[tokio::test]
async fn tcl_hincrbyfloat_fails_against_hash_value_with_spaces_left() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "str", " 11"]).await;
    assert_error_prefix(
        &client
            .command(&["HINCRBYFLOAT", "myhash", "str", "1"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_hincrbyfloat_fails_against_hash_value_with_spaces_right() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "str", "11 "]).await;
    assert_error_prefix(
        &client
            .command(&["HINCRBYFLOAT", "myhash", "str", "1"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_hincrbyfloat_does_not_allow_nan_or_infinity() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(
        &client
            .command(&["HINCRBYFLOAT", "hfoo", "field", "+inf"])
            .await,
        "ERR",
    );
    assert_integer_eq(&client.command(&["EXISTS", "hfoo"]).await, 0);
}

#[tokio::test]
async fn tcl_hincrbyfloat_correct_float_representation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    assert_bulk_eq(
        &client
            .command(&["HINCRBYFLOAT", "myhash", "float", "1.23"])
            .await,
        b"1.23",
    );
    assert_bulk_eq(
        &client
            .command(&["HINCRBYFLOAT", "myhash", "float", "0.77"])
            .await,
        b"2",
    );
    assert_bulk_eq(
        &client
            .command(&["HINCRBYFLOAT", "myhash", "float", "-0.1"])
            .await,
        b"1.9",
    );
}

// ---------------------------------------------------------------------------
// HSTRLEN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hstrlen_against_non_existing_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "f1", "v1"]).await;
    assert_integer_eq(
        &client
            .command(&["HSTRLEN", "myhash", "__123123123__"])
            .await,
        0,
    );
}

#[tokio::test]
async fn tcl_hstrlen_corner_cases() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let vals = [
        "-9223372036854775808",
        "9223372036854775807",
        "9223372036854775808",
        "",
        "0",
        "-1",
        "x",
    ];
    for v in &vals {
        client.command(&["HMSET", "myhash", "field", v]).await;
        let expected_len = v.len() as i64;
        assert_integer_eq(
            &client.command(&["HSTRLEN", "myhash", "field"]).await,
            expected_len,
        );
    }
}

// ---------------------------------------------------------------------------
// HRANDFIELD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hrandfield_count_of_0_is_handled_correctly() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "myhash", "a", "1", "b", "2"])
        .await;
    let resp = client.command(&["HRANDFIELD", "myhash", "0"]).await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

#[tokio::test]
async fn tcl_hrandfield_count_overflow() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HMSET", "myhash", "a", "1"]).await;
    assert_error_prefix(
        &client
            .command(&["HRANDFIELD", "myhash", "-9223372036854770000", "WITHVALUES"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["HRANDFIELD", "myhash", "-9223372036854775808", "WITHVALUES"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["HRANDFIELD", "myhash", "-9223372036854775808"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_hrandfield_with_count_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HRANDFIELD", "nonexisting_key", "100"])
        .await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

#[tokio::test]
async fn tcl_hrandfield_negative_count_allows_duplicates() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "myhash", "a", "1", "b", "2", "c", "3"])
        .await;

    // Negative count: always returns exactly |count| items (may have duplicates)
    let resp = client.command(&["HRANDFIELD", "myhash", "-20"]).await;
    assert_array_len(&resp, 20);

    // With WITHVALUES: returns |count|*2 items
    let resp = client
        .command(&["HRANDFIELD", "myhash", "-20", "WITHVALUES"])
        .await;
    assert_array_len(&resp, 40);
}

#[tokio::test]
async fn tcl_hrandfield_positive_count_unique_and_capped() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    for i in 0..10 {
        client
            .command(&["HSET", "myhash", &format!("f{i}"), &format!("v{i}")])
            .await;
    }

    // count >= hash size: returns all elements
    let resp = client.command(&["HRANDFIELD", "myhash", "10"]).await;
    assert_array_len(&resp, 10);
    let resp = client.command(&["HRANDFIELD", "myhash", "20"]).await;
    assert_array_len(&resp, 10);

    // count < hash size: returns exactly count unique elements
    let resp = client.command(&["HRANDFIELD", "myhash", "5"]).await;
    assert_array_len(&resp, 5);

    // with WITHVALUES
    let resp = client
        .command(&["HRANDFIELD", "myhash", "10", "WITHVALUES"])
        .await;
    assert_array_len(&resp, 20);
}

// ---------------------------------------------------------------------------
// HGETDEL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hgetdel_input_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "key1"]).await;
    assert_error_prefix(&client.command(&["HGETDEL"]).await, "ERR");
    assert_error_prefix(&client.command(&["HGETDEL", "key1"]).await, "ERR");
    assert_error_prefix(&client.command(&["HGETDEL", "key1", "FIELDS"]).await, "ERR");
    assert_error_prefix(
        &client.command(&["HGETDEL", "key1", "FIELDS", "0"]).await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["HGETDEL", "key1", "XFIELDX", "1", "a"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["HGETDEL", "key1", "FIELDS", "2", "a"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["HGETDEL", "key1", "FIELDS", "2", "a", "b", "c"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["HGETDEL", "key1", "FIELDS", "0", "a"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["HGETDEL", "key1", "FIELDS", "-1", "a"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_hgetdel_basic_test() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "key1"]).await;
    client
        .command(&[
            "HSET", "key1", "f1", "1", "f2", "2", "f3", "3", "strfield", "strval",
        ])
        .await;

    // Delete f2, should return its value
    let resp = client
        .command(&["HGETDEL", "key1", "FIELDS", "1", "f2"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"2");

    assert_integer_eq(&client.command(&["HLEN", "key1"]).await, 3);
    assert_bulk_eq(&client.command(&["HGET", "key1", "f1"]).await, b"1");
    assert_nil(&client.command(&["HGET", "key1", "f2"]).await);
    assert_bulk_eq(&client.command(&["HGET", "key1", "f3"]).await, b"3");
    assert_bulk_eq(
        &client.command(&["HGET", "key1", "strfield"]).await,
        b"strval",
    );

    // Delete f1
    let resp = client
        .command(&["HGETDEL", "key1", "FIELDS", "1", "f1"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"1");

    // Delete remaining fields
    let resp = client
        .command(&["HGETDEL", "key1", "FIELDS", "1", "f3"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"3");

    let resp = client
        .command(&["HGETDEL", "key1", "FIELDS", "1", "strfield"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"strval");

    // Key should be gone
    assert_integer_eq(&client.command(&["EXISTS", "key1"]).await, 0);
}

#[tokio::test]
async fn tcl_hgetdel_test_with_non_existing_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "key1"]).await;
    client
        .command(&["HSET", "key1", "f1", "1", "f2", "2", "f3", "3"])
        .await;

    // All non-existing fields → all nil
    let resp = client
        .command(&["HGETDEL", "key1", "FIELDS", "4", "x1", "x2", "x3", "x4"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 4);
    for item in &items {
        assert_nil(item);
    }

    // Mix of existing and non-existing
    let resp = client
        .command(&["HGETDEL", "key1", "FIELDS", "4", "x1", "x2", "f3", "x4"])
        .await;
    let items = unwrap_array(resp);
    assert_nil(&items[0]);
    assert_nil(&items[1]);
    assert_bulk_eq(&items[2], b"3");
    assert_nil(&items[3]);

    // Remaining fields
    let resp = client
        .command(&["HGETDEL", "key1", "FIELDS", "3", "f1", "f2", "f3"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"1");
    assert_bulk_eq(&items[1], b"2");
    assert_nil(&items[2]); // f3 already deleted

    // All gone now
    let resp = client
        .command(&["HGETDEL", "key1", "FIELDS", "3", "f1", "f2", "f3"])
        .await;
    let items = unwrap_array(resp);
    for item in &items {
        assert_nil(item);
    }
}

// ---------------------------------------------------------------------------
// Hash ziplist regression test for large keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hash_ziplist_regression_test_for_large_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let long_key = "k".repeat(340);
    client.command(&["HSET", "hash", &long_key, "a"]).await;
    client.command(&["HSET", "hash", &long_key, "b"]).await;
    assert_bulk_eq(&client.command(&["HGET", "hash", &long_key]).await, b"b");
}

// ---------------------------------------------------------------------------
// HGETALL with deterministic data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hgetall_returns_all_fields_and_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "a", "1", "b", "2", "c", "3"])
        .await;
    let resp = client.command(&["HGETALL", "myhash"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items.len(), 6); // 3 field-value pairs
    let mut sorted = items;
    sorted.sort();
    assert_eq!(sorted, vec!["1", "2", "3", "a", "b", "c"]);
}

// ---------------------------------------------------------------------------
// HKEYS / HVALS with deterministic data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hkeys_and_hvals() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "a", "10", "b", "20", "c", "30"])
        .await;

    let mut keys = extract_bulk_strings(&client.command(&["HKEYS", "myhash"]).await);
    keys.sort();
    assert_eq!(keys, vec!["a", "b", "c"]);

    let mut vals = extract_bulk_strings(&client.command(&["HVALS", "myhash"]).await);
    vals.sort();
    assert_eq!(vals, vec!["10", "20", "30"]);
}

// ---------------------------------------------------------------------------
// HMSET / HMGET with deterministic data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hmset_hmget_roundtrip() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["HMSET", "myhash", "a", "1", "b", "2", "c", "3"])
            .await,
    );

    let resp = client.command(&["HMGET", "myhash", "a", "b", "c"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"1");
    assert_bulk_eq(&items[1], b"2");
    assert_bulk_eq(&items[2], b"3");
}

// ---------------------------------------------------------------------------
// HDEL return values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hdel_and_return_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;

    // Delete non-existing field
    assert_integer_eq(&client.command(&["HDEL", "myhash", "nokey"]).await, 0);

    // Delete existing field
    assert_integer_eq(&client.command(&["HDEL", "myhash", "f1"]).await, 1);

    // Double-delete returns 0
    assert_integer_eq(&client.command(&["HDEL", "myhash", "f1"]).await, 0);

    // Field is gone
    assert_nil(&client.command(&["HGET", "myhash", "f1"]).await);
}

// ---------------------------------------------------------------------------
// Hash fuzzing helpers
// ---------------------------------------------------------------------------

/// Generate a random value mimicking Redis's `randomValue` helper: a mix of
/// small integers, large integers, and random alpha strings.
fn hash_random_value(rng: &mut StdRng) -> String {
    match rng.random_range(0..4) {
        0 => {
            // Small signed integer [-999, 999]
            let n: i32 = rng.random_range(-999..1000);
            n.to_string()
        }
        1 => {
            // Large 32-bit range signed integer
            let n: i64 = rng.random_range(-2_000_000_000..2_000_000_001);
            n.to_string()
        }
        2 => {
            // Large 64-bit range signed integer
            let n: i64 = rng.random_range(-1_000_000_000_000..1_000_000_000_001);
            n.to_string()
        }
        _ => {
            // Random alpha string, length 1..64
            let len: usize = rng.random_range(1..64);
            (0..len)
                .map(|_| {
                    let idx = rng.random_range(0..52);
                    if idx < 26 {
                        (b'a' + idx) as char
                    } else {
                        (b'A' + idx - 26) as char
                    }
                })
                .collect()
        }
    }
}

/// Generate a random signed integer in [-max, max) mimicking Redis's
/// `randomSignedInt`.
fn random_signed_int(rng: &mut StdRng, max: i64) -> String {
    let n: i64 = rng.random_range(0..max);
    if rng.random_bool(0.5) {
        (-n).to_string()
    } else {
        n.to_string()
    }
}

// ---------------------------------------------------------------------------
// Hash fuzzing #1 — HSET-only with verification
// ---------------------------------------------------------------------------

/// Port of Redis `Hash fuzzing #1 - 10 fields`.
///
/// Creates random field/value pairs using HSET and verifies that HGET returns
/// the correct value for every field and HLEN matches the reference.
#[tokio::test]
async fn tcl_hash_fuzzing_1_10_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let mut rng = StdRng::seed_from_u64(100);

    for iteration in 0..10 {
        let mut reference: HashMap<String, String> = HashMap::new();
        client.command(&["DEL", "hash"]).await;

        for _ in 0..10 {
            let field = hash_random_value(&mut rng);
            let value = hash_random_value(&mut rng);
            client.command(&["HSET", "hash", &field, &value]).await;
            reference.insert(field, value);
        }

        // Verify every field
        for (k, v) in &reference {
            assert_bulk_eq(&client.command(&["HGET", "hash", k]).await, v.as_bytes());
        }

        // Verify length
        assert_integer_eq(
            &client.command(&["HLEN", "hash"]).await,
            reference.len() as i64,
        );

        // Verify HGETALL consistency
        let resp = client.command(&["HGETALL", "hash"]).await;
        let items = extract_bulk_strings(&resp);
        assert_eq!(
            items.len(),
            reference.len() * 2,
            "HGETALL length mismatch at iteration {iteration}"
        );
        // Parse HGETALL flat array into field/value pairs and verify
        let mut server_hash: HashMap<String, String> = HashMap::new();
        for chunk in items.chunks(2) {
            server_hash.insert(chunk[0].clone(), chunk[1].clone());
        }
        assert_eq!(
            server_hash.len(),
            reference.len(),
            "HGETALL unique field count mismatch at iteration {iteration}"
        );
        for (k, v) in &reference {
            assert_eq!(
                server_hash.get(k).map(|s| s.as_str()),
                Some(v.as_str()),
                "HGETALL value mismatch for field {k:?} at iteration {iteration}"
            );
        }
    }
}

/// Port of Redis `Hash fuzzing #1 - 512 fields`.
///
/// Same as the 10-field variant but exercises larger hashes that exceed the
/// listpack encoding threshold.
#[tokio::test]
async fn tcl_hash_fuzzing_1_512_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let mut rng = StdRng::seed_from_u64(101);

    for iteration in 0..10 {
        let mut reference: HashMap<String, String> = HashMap::new();
        client.command(&["DEL", "hash"]).await;

        for _ in 0..512 {
            let field = hash_random_value(&mut rng);
            let value = hash_random_value(&mut rng);
            client.command(&["HSET", "hash", &field, &value]).await;
            reference.insert(field, value);
        }

        // Verify every field
        for (k, v) in &reference {
            assert_bulk_eq(&client.command(&["HGET", "hash", k]).await, v.as_bytes());
        }

        // Verify length
        assert_integer_eq(
            &client.command(&["HLEN", "hash"]).await,
            reference.len() as i64,
        );

        // Verify HGETALL consistency
        let resp = client.command(&["HGETALL", "hash"]).await;
        let items = extract_bulk_strings(&resp);
        assert_eq!(
            items.len(),
            reference.len() * 2,
            "HGETALL length mismatch at iteration {iteration}"
        );
        let mut server_hash: HashMap<String, String> = HashMap::new();
        for chunk in items.chunks(2) {
            server_hash.insert(chunk[0].clone(), chunk[1].clone());
        }
        assert_eq!(
            server_hash.len(),
            reference.len(),
            "HGETALL unique field count mismatch at iteration {iteration}"
        );
        for (k, v) in &reference {
            assert_eq!(
                server_hash.get(k).map(|s| s.as_str()),
                Some(v.as_str()),
                "HGETALL value mismatch for field {k:?} at iteration {iteration}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Hash fuzzing #2 — HSET + HDEL with verification
// ---------------------------------------------------------------------------

/// Port of Redis `Hash fuzzing #2 - 10 fields`.
///
/// Randomly mixes HSET (with random string or integer field/value) and HDEL
/// operations, then verifies that HGET/HLEN match the in-memory reference.
#[tokio::test]
async fn tcl_hash_fuzzing_2_10_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let mut rng = StdRng::seed_from_u64(200);

    for iteration in 0..10 {
        let mut reference: HashMap<String, String> = HashMap::new();
        client.command(&["DEL", "hash"]).await;

        for _ in 0..10 {
            match rng.random_range(0..3) {
                0 => {
                    // Random string field/value
                    let field = hash_random_value(&mut rng);
                    let value = hash_random_value(&mut rng);
                    client.command(&["HSET", "hash", &field, &value]).await;
                    reference.insert(field, value);
                }
                1 => {
                    // Random integer field/value
                    let field = random_signed_int(&mut rng, 512);
                    let value = random_signed_int(&mut rng, 512);
                    client.command(&["HSET", "hash", &field, &value]).await;
                    reference.insert(field, value);
                }
                _ => {
                    // Delete a random field (string or integer)
                    let field = if rng.random_bool(0.5) {
                        hash_random_value(&mut rng)
                    } else {
                        random_signed_int(&mut rng, 512)
                    };
                    client.command(&["HDEL", "hash", &field]).await;
                    reference.remove(&field);
                }
            }
        }

        // Verify every field via HGET
        for (k, v) in &reference {
            assert_bulk_eq(&client.command(&["HGET", "hash", k]).await, v.as_bytes());
        }

        // Verify length
        assert_integer_eq(
            &client.command(&["HLEN", "hash"]).await,
            reference.len() as i64,
        );

        // Verify HEXISTS for all reference fields
        for k in reference.keys() {
            assert_integer_eq(&client.command(&["HEXISTS", "hash", k]).await, 1);
        }

        // Verify HGETALL consistency
        if !reference.is_empty() {
            let resp = client.command(&["HGETALL", "hash"]).await;
            let items = extract_bulk_strings(&resp);
            assert_eq!(
                items.len(),
                reference.len() * 2,
                "HGETALL length mismatch at iteration {iteration}"
            );
            let mut server_hash: HashMap<String, String> = HashMap::new();
            for chunk in items.chunks(2) {
                server_hash.insert(chunk[0].clone(), chunk[1].clone());
            }
            assert_eq!(
                server_hash.len(),
                reference.len(),
                "HGETALL unique field count mismatch at iteration {iteration}"
            );
            for (k, v) in &reference {
                assert_eq!(
                    server_hash.get(k).map(|s| s.as_str()),
                    Some(v.as_str()),
                    "HGETALL value mismatch for field {k:?} at iteration {iteration}"
                );
            }
        }
    }
}

/// Port of Redis `Hash fuzzing #2 - 512 fields`.
///
/// Same as the 10-field variant but exercises larger hashes with mixed
/// HSET/HDEL operations to stress the hashtable encoding path.
#[tokio::test]
async fn tcl_hash_fuzzing_2_512_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let mut rng = StdRng::seed_from_u64(201);

    for iteration in 0..10 {
        let mut reference: HashMap<String, String> = HashMap::new();
        client.command(&["DEL", "hash"]).await;

        for _ in 0..512 {
            match rng.random_range(0..3) {
                0 => {
                    // Random string field/value
                    let field = hash_random_value(&mut rng);
                    let value = hash_random_value(&mut rng);
                    client.command(&["HSET", "hash", &field, &value]).await;
                    reference.insert(field, value);
                }
                1 => {
                    // Random integer field/value
                    let field = random_signed_int(&mut rng, 512);
                    let value = random_signed_int(&mut rng, 512);
                    client.command(&["HSET", "hash", &field, &value]).await;
                    reference.insert(field, value);
                }
                _ => {
                    // Delete a random field (string or integer)
                    let field = if rng.random_bool(0.5) {
                        hash_random_value(&mut rng)
                    } else {
                        random_signed_int(&mut rng, 512)
                    };
                    client.command(&["HDEL", "hash", &field]).await;
                    reference.remove(&field);
                }
            }
        }

        // Verify every field via HGET
        for (k, v) in &reference {
            assert_bulk_eq(&client.command(&["HGET", "hash", k]).await, v.as_bytes());
        }

        // Verify length
        assert_integer_eq(
            &client.command(&["HLEN", "hash"]).await,
            reference.len() as i64,
        );

        // Verify HEXISTS for all reference fields
        for k in reference.keys() {
            assert_integer_eq(&client.command(&["HEXISTS", "hash", k]).await, 1);
        }

        // Verify HGETALL consistency
        if !reference.is_empty() {
            let resp = client.command(&["HGETALL", "hash"]).await;
            let items = extract_bulk_strings(&resp);
            assert_eq!(
                items.len(),
                reference.len() * 2,
                "HGETALL length mismatch at iteration {iteration}"
            );
            let mut server_hash: HashMap<String, String> = HashMap::new();
            for chunk in items.chunks(2) {
                server_hash.insert(chunk[0].clone(), chunk[1].clone());
            }
            assert_eq!(
                server_hash.len(),
                reference.len(),
                "HGETALL unique field count mismatch at iteration {iteration}"
            );
            for (k, v) in &reference {
                assert_eq!(
                    server_hash.get(k).map(|s| s.as_str()),
                    Some(v.as_str()),
                    "HGETALL value mismatch for field {k:?} at iteration {iteration}"
                );
            }
        }
    }
}
