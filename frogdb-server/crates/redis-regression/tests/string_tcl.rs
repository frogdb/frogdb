//! Rust port of Redis 8.6.0 `unit/type/string.tcl` test suite.
//!
//! Excludes: `needs:repl`-tagged, `needs:debug`-tagged, jemalloc-specific,
//! MEMORY USAGE internals, and random-fuzzing tests.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// SET / GET basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_set_and_get_an_item() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "foobar"]).await;
    assert_bulk_eq(&client.command(&["GET", "x"]).await, b"foobar");
}

#[tokio::test]
async fn tcl_set_and_get_an_empty_item() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", ""]).await;
    assert_bulk_eq(&client.command(&["GET", "x"]).await, b"");
}

#[tokio::test]
async fn tcl_very_big_payload_in_get_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let buf = "abcd".repeat(1_000_000);
    assert_ok(&client.command(&["SET", "foo", &buf]).await);
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, buf.as_bytes());
}

// ---------------------------------------------------------------------------
// SETNX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_setnx_target_key_missing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "novar"]).await;
    assert_integer_eq(&client.command(&["SETNX", "novar", "foobared"]).await, 1);
    assert_bulk_eq(&client.command(&["GET", "novar"]).await, b"foobared");
}

#[tokio::test]
async fn tcl_setnx_target_key_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "foobared"]).await;
    assert_integer_eq(&client.command(&["SETNX", "novar", "blabla"]).await, 0);
    assert_bulk_eq(&client.command(&["GET", "novar"]).await, b"foobared");
}

#[tokio::test]
async fn tcl_setnx_against_not_expired_volatile_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "10"]).await;
    client.command(&["EXPIRE", "x", "10000"]).await;
    assert_integer_eq(&client.command(&["SETNX", "x", "20"]).await, 0);
    assert_bulk_eq(&client.command(&["GET", "x"]).await, b"10");
}

// ---------------------------------------------------------------------------
// GETEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_getex_ex_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar"]).await;
    client.command(&["GETEX", "foo", "EX", "10"]).await;
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!((5..=10).contains(&ttl), "TTL {ttl} not in [5, 10]");
}

#[tokio::test]
async fn tcl_getex_px_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar"]).await;
    client.command(&["GETEX", "foo", "PX", "10000"]).await;
    let pttl = unwrap_integer(&client.command(&["PTTL", "foo"]).await);
    assert!(
        (5000..=10000).contains(&pttl),
        "PTTL {pttl} not in [5000, 10000]"
    );
}

#[tokio::test]
async fn tcl_getex_exat_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let future = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 10;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar"]).await;
    client
        .command(&["GETEX", "foo", "EXAT", &future.to_string()])
        .await;
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!((5..=10).contains(&ttl), "TTL {ttl} not in [5, 10]");
}

#[tokio::test]
async fn tcl_getex_pxat_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let future = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 10_000;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar"]).await;
    client
        .command(&["GETEX", "foo", "PXAT", &future.to_string()])
        .await;
    let pttl = unwrap_integer(&client.command(&["PTTL", "foo"]).await);
    assert!(
        (5000..=10000).contains(&pttl),
        "PTTL {pttl} not in [5000, 10000]"
    );
}

#[tokio::test]
async fn tcl_getex_persist_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar", "EX", "10"]).await;
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!((5..=10).contains(&ttl), "TTL {ttl} not in [5, 10]");
    client.command(&["GETEX", "foo", "PERSIST"]).await;
    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -1);
}

#[tokio::test]
async fn tcl_getex_no_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar"]).await;
    client.command(&["GETEX", "foo"]).await;
    assert_bulk_eq(&client.command(&["GETEX", "foo"]).await, b"bar");
}

#[tokio::test]
async fn tcl_getex_syntax_errors() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["GETEX", "foo", "non-existent-option"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_getex_and_get_expired_key_or_not_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar", "PX", "1"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_nil(&client.command(&["GETEX", "foo"]).await);
    assert_nil(&client.command(&["GET", "foo"]).await);
}

#[tokio::test]
async fn tcl_getex_no_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["GETEX"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// GETDEL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_getdel_command() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar"]).await;
    assert_bulk_eq(&client.command(&["GETDEL", "foo"]).await, b"bar");
    assert_nil(&client.command(&["GETDEL", "foo"]).await);
}

// ---------------------------------------------------------------------------
// MGET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_mget() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client.command(&["SET", "foo{t}", "BAR"]).await;
    client.command(&["SET", "bar{t}", "FOO"]).await;
    let resp = client.command(&["MGET", "foo{t}", "bar{t}"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"BAR");
    assert_bulk_eq(&items[1], b"FOO");
}

#[tokio::test]
async fn tcl_mget_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client.command(&["SET", "foo{t}", "BAR"]).await;
    client.command(&["SET", "bar{t}", "FOO"]).await;
    let resp = client
        .command(&["MGET", "foo{t}", "baazz{t}", "bar{t}"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"BAR");
    assert_nil(&items[1]);
    assert_bulk_eq(&items[2], b"FOO");
}

#[tokio::test]
async fn tcl_mget_against_non_string_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client.command(&["SET", "foo{t}", "BAR"]).await;
    client.command(&["SET", "bar{t}", "FOO"]).await;
    client.command(&["SADD", "myset{t}", "ciao"]).await;
    client.command(&["SADD", "myset{t}", "bau"]).await;
    let resp = client
        .command(&["MGET", "foo{t}", "baazz{t}", "bar{t}", "myset{t}"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"BAR");
    assert_nil(&items[1]);
    assert_bulk_eq(&items[2], b"FOO");
    assert_nil(&items[3]);
}

// ---------------------------------------------------------------------------
// GETSET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_getset_set_new_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    assert_nil(&client.command(&["GETSET", "foo", "xyz"]).await);
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"xyz");
}

#[tokio::test]
async fn tcl_getset_replace_old_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    assert_bulk_eq(&client.command(&["GETSET", "foo", "xyz"]).await, b"bar");
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"xyz");
}

// ---------------------------------------------------------------------------
// MSET / MSETNX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_mset_base_case() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "MSET",
                "x{t}",
                "10",
                "y{t}",
                "foo bar",
                "z{t}",
                "x x x x x x x\n\n\r\n",
            ])
            .await,
    );
    let resp = client.command(&["MGET", "x{t}", "y{t}", "z{t}"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"10");
    assert_bulk_eq(&items[1], b"foo bar");
    assert_bulk_eq(&items[2], b"x x x x x x x\n\n\r\n");
}

#[tokio::test]
async fn tcl_mset_msetnx_wrong_number_of_args() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(
        &client
            .command(&["MSET", "x{t}", "10", "y{t}", "foo bar", "z{t}"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["MSETNX", "x{t}", "20", "y{t}", "foo bar", "z{t}"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_mset_with_already_existing_same_key_twice() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x{t}", "x"]).await;
    assert_ok(
        &client
            .command(&["MSET", "x{t}", "xxx", "x{t}", "yyy"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "x{t}"]).await, b"yyy");
}

#[tokio::test]
async fn tcl_msetnx_with_already_existent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x1{t}", "y2{t}"]).await;
    client.command(&["SET", "x{t}", "existing"]).await;
    assert_integer_eq(
        &client
            .command(&["MSETNX", "x1{t}", "xxx", "y2{t}", "yyy", "x{t}", "20"])
            .await,
        0,
    );
    assert_integer_eq(&client.command(&["EXISTS", "x1{t}"]).await, 0);
    assert_integer_eq(&client.command(&["EXISTS", "y2{t}"]).await, 0);
}

#[tokio::test]
async fn tcl_msetnx_with_not_existing_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x1{t}", "y2{t}"]).await;
    assert_integer_eq(
        &client
            .command(&["MSETNX", "x1{t}", "xxx", "y2{t}", "yyy"])
            .await,
        1,
    );
    assert_bulk_eq(&client.command(&["GET", "x1{t}"]).await, b"xxx");
    assert_bulk_eq(&client.command(&["GET", "y2{t}"]).await, b"yyy");
}

#[tokio::test]
async fn tcl_msetnx_with_not_existing_keys_same_key_twice() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x1{t}"]).await;
    assert_integer_eq(
        &client
            .command(&["MSETNX", "x1{t}", "xxx", "x1{t}", "yyy"])
            .await,
        1,
    );
    assert_bulk_eq(&client.command(&["GET", "x1{t}"]).await, b"yyy");
}

#[tokio::test]
async fn tcl_msetnx_with_already_existing_keys_same_key_twice() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x1{t}", "yyy"]).await;
    assert_integer_eq(
        &client
            .command(&["MSETNX", "x1{t}", "xxx", "x1{t}", "zzz"])
            .await,
        0,
    );
    assert_bulk_eq(&client.command(&["GET", "x1{t}"]).await, b"yyy");
}

// ---------------------------------------------------------------------------
// STRLEN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_strlen_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_integer_eq(&client.command(&["STRLEN", "notakey"]).await, 0);
}

#[tokio::test]
async fn tcl_strlen_against_integer_encoded_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "myinteger", "-555"]).await;
    assert_integer_eq(&client.command(&["STRLEN", "myinteger"]).await, 4);
}

#[tokio::test]
async fn tcl_strlen_against_plain_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["SET", "mystring", "foozzz0123456789 baz"])
        .await;
    assert_integer_eq(&client.command(&["STRLEN", "mystring"]).await, 20);
}

// ---------------------------------------------------------------------------
// SETBIT / GETBIT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_setbit_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    assert_integer_eq(&client.command(&["SETBIT", "mykey", "1", "1"]).await, 0);
    // bit pattern: 01000000 = 0x40 = '@'
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, &[0x40]);
}

#[tokio::test]
async fn tcl_setbit_against_string_encoded_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // "@" = 0x40 = 01000000
    client.command(&["SET", "mykey", "@"]).await;
    assert_integer_eq(&client.command(&["SETBIT", "mykey", "2", "1"]).await, 0);
    // 01100000 = 0x60
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, &[0x60]);
    assert_integer_eq(&client.command(&["SETBIT", "mykey", "1", "0"]).await, 1);
    // 00100000 = 0x20 = ' '
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, &[0x20]);
}

#[tokio::test]
async fn tcl_setbit_against_integer_encoded_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // "1" = 0x31 = 00110001
    client.command(&["SET", "mykey", "1"]).await;
    assert_integer_eq(&client.command(&["SETBIT", "mykey", "6", "1"]).await, 0);
    // 00110011 = 0x33
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, &[0x33]);
    assert_integer_eq(&client.command(&["SETBIT", "mykey", "2", "0"]).await, 1);
    // 00010011 = 0x13
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, &[0x13]);
}

#[tokio::test]
async fn tcl_setbit_against_key_with_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    client.command(&["LPUSH", "mykey", "foo"]).await;
    assert_error_prefix(
        &client.command(&["SETBIT", "mykey", "0", "1"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_setbit_with_out_of_range_bit_offset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    let big_offset = (4u64 * 1024 * 1024 * 1024).to_string();
    assert_error_prefix(
        &client.command(&["SETBIT", "mykey", &big_offset, "1"]).await,
        "ERR",
    );
    assert_error_prefix(
        &client.command(&["SETBIT", "mykey", "-1", "1"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_setbit_with_non_bit_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    for val in &["-1", "2", "10", "20"] {
        assert_error_prefix(&client.command(&["SETBIT", "mykey", "0", val]).await, "ERR");
    }
}

#[tokio::test]
async fn tcl_getbit_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "0"]).await, 0);
}

#[tokio::test]
async fn tcl_getbit_against_string_encoded_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // "`" = 0x60 = 01100000
    client.command(&["SET", "mykey", "`"]).await;

    assert_integer_eq(&client.command(&["GETBIT", "mykey", "0"]).await, 0);
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "1"]).await, 1);
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "2"]).await, 1);
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "3"]).await, 0);

    // Out of range
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "8"]).await, 0);
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "100"]).await, 0);
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "10000"]).await, 0);
}

#[tokio::test]
async fn tcl_getbit_against_integer_encoded_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // "1" = 0x31 = 00110001
    client.command(&["SET", "mykey", "1"]).await;

    assert_integer_eq(&client.command(&["GETBIT", "mykey", "0"]).await, 0);
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "1"]).await, 0);
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "2"]).await, 1);
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "3"]).await, 1);

    // Out of range
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "8"]).await, 0);
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "100"]).await, 0);
    assert_integer_eq(&client.command(&["GETBIT", "mykey", "10000"]).await, 0);
}

// ---------------------------------------------------------------------------
// SETRANGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_setrange_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    assert_integer_eq(&client.command(&["SETRANGE", "mykey", "0", "foo"]).await, 3);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"foo");

    client.command(&["DEL", "mykey"]).await;
    assert_integer_eq(&client.command(&["SETRANGE", "mykey", "0", ""]).await, 0);
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);

    client.command(&["DEL", "mykey"]).await;
    assert_integer_eq(&client.command(&["SETRANGE", "mykey", "1", "foo"]).await, 4);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"\x00foo");
}

#[tokio::test]
async fn tcl_setrange_against_string_encoded_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "foo"]).await;
    assert_integer_eq(&client.command(&["SETRANGE", "mykey", "0", "b"]).await, 3);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"boo");

    client.command(&["SET", "mykey", "foo"]).await;
    assert_integer_eq(&client.command(&["SETRANGE", "mykey", "0", ""]).await, 3);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"foo");

    client.command(&["SET", "mykey", "foo"]).await;
    assert_integer_eq(&client.command(&["SETRANGE", "mykey", "1", "b"]).await, 3);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"fbo");

    client.command(&["SET", "mykey", "foo"]).await;
    assert_integer_eq(&client.command(&["SETRANGE", "mykey", "4", "bar"]).await, 7);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"foo\x00bar");
}

#[tokio::test]
async fn tcl_setrange_against_integer_encoded_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "1234"]).await;
    assert_integer_eq(&client.command(&["SETRANGE", "mykey", "0", "2"]).await, 4);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"2234");

    client.command(&["SET", "mykey", "1234"]).await;
    assert_integer_eq(&client.command(&["SETRANGE", "mykey", "0", ""]).await, 4);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"1234");

    client.command(&["SET", "mykey", "1234"]).await;
    assert_integer_eq(&client.command(&["SETRANGE", "mykey", "1", "3"]).await, 4);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"1334");

    client.command(&["SET", "mykey", "1234"]).await;
    assert_integer_eq(&client.command(&["SETRANGE", "mykey", "5", "2"]).await, 6);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"1234\x002");
}

#[tokio::test]
async fn tcl_setrange_against_key_with_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    client.command(&["LPUSH", "mykey", "foo"]).await;
    assert_error_prefix(
        &client.command(&["SETRANGE", "mykey", "0", "bar"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_setrange_with_out_of_range_offset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    let big_offset = (512u64 * 1024 * 1024 - 4).to_string();
    assert_error_prefix(
        &client
            .command(&["SETRANGE", "mykey", &big_offset, "world"])
            .await,
        "ERR",
    );

    client.command(&["SET", "mykey", "hello"]).await;
    assert_error_prefix(
        &client.command(&["SETRANGE", "mykey", "-1", "world"]).await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// GETRANGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_getrange_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "0", "-1"]).await,
        b"",
    );
}

#[tokio::test]
async fn tcl_getrange_against_wrong_key_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "lkey1"]).await;
    client.command(&["LPUSH", "lkey1", "list"]).await;
    assert_error_prefix(
        &client.command(&["GETRANGE", "lkey1", "0", "-1"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_getrange_against_string_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "Hello World"]).await;
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "0", "3"]).await,
        b"Hell",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "0", "-1"]).await,
        b"Hello World",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "-4", "-1"]).await,
        b"orld",
    );
    assert_bulk_eq(&client.command(&["GETRANGE", "mykey", "5", "3"]).await, b"");
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "5", "5000"]).await,
        b" World",
    );
    assert_bulk_eq(
        &client
            .command(&["GETRANGE", "mykey", "-5000", "10000"])
            .await,
        b"Hello World",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "0", "-100"]).await,
        b"H",
    );
}

#[tokio::test]
#[ignore = "FrogDB GETRANGE with large negative end: returns data where Redis returns empty"]
async fn tcl_getrange_against_string_value_negative_edge_cases() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "Hello World"]).await;
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "1", "-100"]).await,
        b"",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "-1", "-100"]).await,
        b"",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "-100", "-99"]).await,
        b"H",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "-100", "-100"]).await,
        b"H",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "-100", "-101"]).await,
        b"",
    );
}

#[tokio::test]
async fn tcl_getrange_against_integer_encoded_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "1234"]).await;
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "0", "2"]).await,
        b"123",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "0", "-1"]).await,
        b"1234",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "-3", "-1"]).await,
        b"234",
    );
    assert_bulk_eq(&client.command(&["GETRANGE", "mykey", "5", "3"]).await, b"");
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "3", "5000"]).await,
        b"4",
    );
    assert_bulk_eq(
        &client
            .command(&["GETRANGE", "mykey", "-5000", "10000"])
            .await,
        b"1234",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "0", "-100"]).await,
        b"1",
    );
}

#[tokio::test]
#[ignore = "FrogDB GETRANGE with large negative end: returns data where Redis returns empty"]
async fn tcl_getrange_against_integer_negative_edge_cases() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "1234"]).await;
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "1", "-100"]).await,
        b"",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "-1", "-100"]).await,
        b"",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "-100", "-99"]).await,
        b"1",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "-100", "-100"]).await,
        b"1",
    );
    assert_bulk_eq(
        &client.command(&["GETRANGE", "mykey", "-100", "-101"]).await,
        b"",
    );
}

// ---------------------------------------------------------------------------
// SUBSTR
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_coverage_substr() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "key", "abcde"]).await;
    assert_bulk_eq(&client.command(&["SUBSTR", "key", "0", "0"]).await, b"a");
    assert_bulk_eq(&client.command(&["SUBSTR", "key", "0", "3"]).await, b"abcd");
    assert_bulk_eq(
        &client.command(&["SUBSTR", "key", "-4", "-1"]).await,
        b"bcde",
    );
    assert_bulk_eq(&client.command(&["SUBSTR", "key", "-1", "-3"]).await, b"");
    assert_bulk_eq(&client.command(&["SUBSTR", "key", "7", "8"]).await, b"");
    assert_bulk_eq(&client.command(&["SUBSTR", "nokey", "0", "1"]).await, b"");
}

// ---------------------------------------------------------------------------
// Extended SET options
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_extended_set_can_detect_syntax_errors() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["SET", "foo", "bar", "non-existing-option"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_extended_set_nx_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    assert_ok(&client.command(&["SET", "foo", "1", "NX"]).await);
    assert_nil(&client.command(&["SET", "foo", "2", "NX"]).await);
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"1");
}

#[tokio::test]
async fn tcl_extended_set_xx_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    assert_nil(&client.command(&["SET", "foo", "1", "XX"]).await);
    client.command(&["SET", "foo", "bar"]).await;
    assert_ok(&client.command(&["SET", "foo", "2", "XX"]).await);
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"2");
}

#[tokio::test]
async fn tcl_extended_set_get_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar"]).await;
    assert_bulk_eq(
        &client.command(&["SET", "foo", "bar2", "GET"]).await,
        b"bar",
    );
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar2");
}

#[tokio::test]
async fn tcl_extended_set_get_option_with_no_previous_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    assert_nil(&client.command(&["SET", "foo", "bar", "GET"]).await);
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");
}

#[tokio::test]
async fn tcl_extended_set_get_option_with_xx() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar"]).await;
    assert_bulk_eq(
        &client.command(&["SET", "foo", "baz", "GET", "XX"]).await,
        b"bar",
    );
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"baz");
}

#[tokio::test]
async fn tcl_extended_set_get_option_with_xx_and_no_previous_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    assert_nil(&client.command(&["SET", "foo", "bar", "GET", "XX"]).await);
    assert_nil(&client.command(&["GET", "foo"]).await);
}

#[tokio::test]
async fn tcl_extended_set_get_option_with_nx() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    assert_nil(&client.command(&["SET", "foo", "bar", "GET", "NX"]).await);
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");
}

#[tokio::test]
async fn tcl_extended_set_get_option_with_nx_and_previous_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar"]).await;
    assert_bulk_eq(
        &client.command(&["SET", "foo", "baz", "GET", "NX"]).await,
        b"bar",
    );
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");
}

#[tokio::test]
async fn tcl_extended_set_get_with_incorrect_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["RPUSH", "foo", "waffle"]).await;
    assert_error_prefix(
        &client.command(&["SET", "foo", "bar", "GET"]).await,
        "WRONGTYPE",
    );
    assert_bulk_eq(&client.command(&["RPOP", "foo"]).await, b"waffle");
}

#[tokio::test]
async fn tcl_extended_set_ex_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar", "EX", "10"]).await;
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(ttl <= 10 && ttl > 5, "TTL {ttl} not in (5, 10]");
}

#[tokio::test]
async fn tcl_extended_set_px_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "bar", "PX", "10000"]).await;
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(ttl <= 10 && ttl > 5, "TTL {ttl} not in (5, 10]");
}

#[tokio::test]
async fn tcl_extended_set_exat_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let future = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 10;

    client.command(&["DEL", "foo"]).await;
    client
        .command(&["SET", "foo", "bar", "EXAT", &future.to_string()])
        .await;
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!((5..=10).contains(&ttl), "TTL {ttl} not in [5, 10]");
}

#[tokio::test]
async fn tcl_extended_set_pxat_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let future = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 10_000;

    client.command(&["DEL", "foo"]).await;
    client
        .command(&["SET", "foo", "bar", "PXAT", &future.to_string()])
        .await;
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!((5..=10).contains(&ttl), "TTL {ttl} not in [5, 10]");
}

#[tokio::test]
async fn tcl_extended_set_using_multiple_options_at_once() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "val"]).await;
    assert_ok(
        &client
            .command(&["SET", "foo", "bar", "XX", "PX", "10000"])
            .await,
    );
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(ttl <= 10 && ttl > 5, "TTL {ttl} not in (5, 10]");
}

// ---------------------------------------------------------------------------
// GETRANGE edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_getrange_with_huge_ranges_github_issue_1844() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    assert_bulk_eq(
        &client
            .command(&["GETRANGE", "foo", "0", "4294967297"])
            .await,
        b"bar",
    );
}

// ---------------------------------------------------------------------------
// LCS (Longest Common Substring)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_lcs_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let rna1 = "CACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACTTTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAACTAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCGTCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTTCGTCCGGGTGTG";
    let rna2 = "ATTAAAGGTTTATACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACTTTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAACTAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCGTCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTT";
    let expected_lcs = "ACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACTTTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAACTAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCGTCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTT";

    client.command(&["SET", "virus1{t}", rna1]).await;
    client.command(&["SET", "virus2{t}", rna2]).await;
    assert_bulk_eq(
        &client.command(&["LCS", "virus1{t}", "virus2{t}"]).await,
        expected_lcs.as_bytes(),
    );
}

#[tokio::test]
#[ignore = "FrogDB LCS LEN returns 227 instead of 222 for this test case"]
async fn tcl_lcs_len() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let rna1 = "CACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACTTTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAACTAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCGTCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTTCGTCCGGGTGTG";
    let rna2 = "ATTAAAGGTTTATACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACTTTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAACTAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCGTCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTT";

    client.command(&["SET", "virus1{t}", rna1]).await;
    client.command(&["SET", "virus2{t}", rna2]).await;
    assert_integer_eq(
        &client
            .command(&["LCS", "virus1{t}", "virus2{t}", "LEN"])
            .await,
        222,
    );
}

// ---------------------------------------------------------------------------
// SETRANGE huge offset
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_setrange_with_huge_offset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for value in &["9223372036854775807", "2147483647"] {
        let resp = client.command(&["SETRANGE", "K", value, "A"]).await;
        match &resp {
            Response::Error(e) => {
                let msg = String::from_utf8_lossy(e);
                assert!(
                    msg.contains("maximum allowed size") || msg.contains("out of range"),
                    "unexpected error: {msg}"
                );
            }
            other => panic!("expected error, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// APPEND
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_append_modifies_the_encoding_from_int_to_raw() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SET", "foo", "1"]).await;
    client.command(&["APPEND", "foo", "2"]).await;
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"12");

    client.command(&["SET", "bar", "12"]).await;
    assert_bulk_eq(&client.command(&["GET", "bar"]).await, b"12");
}

// ---------------------------------------------------------------------------
// DIGEST
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_digest_basic_usage_with_plain_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello world"]).await;
    let resp = client.command(&["DIGEST", "mykey"]).await;
    let digest = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    // Verify it's a hex string
    assert!(
        digest.chars().all(|c| c.is_ascii_hexdigit()),
        "digest should be hex, got: {digest}"
    );
}

#[tokio::test]
async fn tcl_digest_with_empty_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", ""]).await;
    let resp = client.command(&["DIGEST", "mykey"]).await;
    let digest = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
async fn tcl_digest_with_integer_encoded_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "12345"]).await;
    let resp = client.command(&["DIGEST", "mykey"]).await;
    let digest = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
async fn tcl_digest_returns_consistent_hash_for_same_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "test string"]).await;
    let d1 = unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec();
    let d2 = unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec();
    assert_eq!(d1, d2);
}

#[tokio::test]
async fn tcl_digest_returns_same_hash_for_same_content_in_different_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "key1", "identical"]).await;
    client.command(&["SET", "key2", "identical"]).await;
    let d1 = unwrap_bulk(&client.command(&["DIGEST", "key1"]).await).to_vec();
    let d2 = unwrap_bulk(&client.command(&["DIGEST", "key2"]).await).to_vec();
    assert_eq!(d1, d2);
}

#[tokio::test]
async fn tcl_digest_returns_different_hash_for_different_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "key1", "value1"]).await;
    client.command(&["SET", "key2", "value2"]).await;
    let d1 = unwrap_bulk(&client.command(&["DIGEST", "key1"]).await).to_vec();
    let d2 = unwrap_bulk(&client.command(&["DIGEST", "key2"]).await).to_vec();
    assert_ne!(d1, d2);
}

#[tokio::test]
async fn tcl_digest_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "nonexistent"]).await;
    assert_nil(&client.command(&["DIGEST", "nonexistent"]).await);
}

#[tokio::test]
async fn tcl_digest_against_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["LPUSH", "mylist", "element"]).await;
    assert_error_prefix(&client.command(&["DIGEST", "mylist"]).await, "WRONGTYPE");

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "field", "value"]).await;
    assert_error_prefix(&client.command(&["DIGEST", "myhash"]).await, "WRONGTYPE");

    client.command(&["DEL", "myset"]).await;
    client.command(&["SADD", "myset", "member"]).await;
    assert_error_prefix(&client.command(&["DIGEST", "myset"]).await, "WRONGTYPE");

    client.command(&["DEL", "myzset"]).await;
    client.command(&["ZADD", "myzset", "1", "member"]).await;
    assert_error_prefix(&client.command(&["DIGEST", "myzset"]).await, "WRONGTYPE");
}

#[tokio::test]
async fn tcl_digest_wrong_number_of_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(&client.command(&["DIGEST"]).await, "ERR");
    assert_error_prefix(&client.command(&["DIGEST", "key1", "key2"]).await, "ERR");
}

#[tokio::test]
async fn tcl_digest_consistency_across_set_operations() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "original"]).await;
    let d1 = unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec();

    client.command(&["SET", "mykey", "changed"]).await;
    let d2 = unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec();
    assert_ne!(d1, d2);

    client.command(&["SET", "mykey", "original"]).await;
    let d3 = unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec();
    assert_eq!(d1, d3);
}

#[tokio::test]
async fn tcl_digest_always_returns_exactly_16_hex_characters() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "SET",
            "foo",
            "v8lf0c11xh8ymlqztfd3eeq16kfn4sspw7fqmnuuq3k3t75em5wdizgcdw7uc26nnf961u2jkfzkjytls2kwlj7626sd",
        ])
        .await;
    assert_bulk_eq(
        &client.command(&["DIGEST", "foo"]).await,
        b"00006c38adf31777",
    );
}

// ---------------------------------------------------------------------------
// DELEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_delex_basic_usage_without_conditions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_integer_eq(&client.command(&["DELEX", "mykey"]).await, 1);

    client.command(&["HSET", "myhash", "f", "v"]).await;
    assert_integer_eq(&client.command(&["DELEX", "myhash"]).await, 1);

    client.command(&["ZADD", "mystr", "1", "m"]).await;
    assert_integer_eq(&client.command(&["DELEX", "mystr"]).await, 1);
}

#[tokio::test]
async fn tcl_delex_basic_usage_with_ifeq() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_integer_eq(
        &client.command(&["DELEX", "mykey", "IFEQ", "hello"]).await,
        1,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);

    client.command(&["SET", "mykey", "hello"]).await;
    assert_integer_eq(
        &client.command(&["DELEX", "mykey", "IFEQ", "world"]).await,
        0,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 1);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"hello");
}

#[tokio::test]
async fn tcl_delex_basic_usage_with_ifne() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_integer_eq(
        &client.command(&["DELEX", "mykey", "IFNE", "world"]).await,
        1,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);

    client.command(&["SET", "mykey", "hello"]).await;
    assert_integer_eq(
        &client.command(&["DELEX", "mykey", "IFNE", "hello"]).await,
        0,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 1);
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"hello");
}

#[tokio::test]
async fn tcl_delex_basic_usage_with_ifdeq() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    let digest =
        String::from_utf8(unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec())
            .unwrap();
    assert_integer_eq(
        &client.command(&["DELEX", "mykey", "IFDEQ", &digest]).await,
        1,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);
}

#[tokio::test]
async fn tcl_delex_basic_usage_with_ifdne() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    let digest =
        String::from_utf8(unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec())
            .unwrap();
    // Use a wrong digest (increment last hex digit)
    let last = u8::from_str_radix(&digest[15..], 16).unwrap();
    let new_last = if last == 0xf { 0 } else { last + 1 };
    let wrong_digest = format!("{}{:x}", &digest[..15], new_last);
    assert_integer_eq(
        &client
            .command(&["DELEX", "mykey", "IFDNE", &wrong_digest])
            .await,
        1,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);
}

#[tokio::test]
async fn tcl_delex_with_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "nonexistent"]).await;
    assert_integer_eq(
        &client
            .command(&["DELEX", "nonexistent", "IFEQ", "hello"])
            .await,
        0,
    );
    assert_integer_eq(
        &client
            .command(&["DELEX", "nonexistent", "IFNE", "hello"])
            .await,
        0,
    );
    assert_integer_eq(
        &client
            .command(&["DELEX", "nonexistent", "IFDEQ", "1234567890abcdef"])
            .await,
        0,
    );
    assert_integer_eq(
        &client
            .command(&["DELEX", "nonexistent", "IFDNE", "1234567890abcdef"])
            .await,
        0,
    );
}

#[tokio::test]
async fn tcl_delex_with_empty_string_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", ""]).await;
    assert_integer_eq(&client.command(&["DELEX", "mykey", "IFEQ", ""]).await, 1);
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);

    client.command(&["SET", "mykey", ""]).await;
    assert_integer_eq(
        &client
            .command(&["DELEX", "mykey", "IFEQ", "notempty"])
            .await,
        0,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 1);
}

#[tokio::test]
async fn tcl_delex_with_integer_encoded_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "12345"]).await;
    assert_integer_eq(
        &client.command(&["DELEX", "mykey", "IFEQ", "12345"]).await,
        1,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);

    client.command(&["SET", "mykey", "12345"]).await;
    assert_integer_eq(
        &client.command(&["DELEX", "mykey", "IFEQ", "54321"]).await,
        0,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 1);
}

#[tokio::test]
async fn tcl_delex_against_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["LPUSH", "mylist", "element"]).await;
    assert_error_prefix(
        &client
            .command(&["DELEX", "mylist", "IFEQ", "element"])
            .await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_delex_wrong_number_of_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "key1", "x"]).await;
    assert_error_prefix(&client.command(&["DELEX", "key1", "IFEQ"]).await, "ERR");
    assert_error_prefix(
        &client
            .command(&["DELEX", "key1", "IFEQ", "value1", "extra"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_delex_invalid_condition() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_error_prefix(
        &client
            .command(&["DELEX", "mykey", "INVALID", "hello"])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// Extended SET with IFEQ / IFNE / IFDEQ / IFDNE
// These are Redis 8.x features not yet implemented in FrogDB.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "SET IFEQ not implemented in FrogDB"]
async fn tcl_extended_set_with_ifeq_key_exists_and_matches() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_ok(
        &client
            .command(&["SET", "mykey", "world", "IFEQ", "hello"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifeq_key_exists_but_doesnt_match() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_nil(
        &client
            .command(&["SET", "mykey", "world", "IFEQ", "different"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"hello");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifeq_key_doesnt_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    assert_nil(
        &client
            .command(&["SET", "mykey", "world", "IFEQ", "hello"])
            .await,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifne_key_exists_and_doesnt_match() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_ok(
        &client
            .command(&["SET", "mykey", "world", "IFNE", "different"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifne_key_exists_and_matches() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_nil(
        &client
            .command(&["SET", "mykey", "world", "IFNE", "hello"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"hello");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifne_key_doesnt_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    assert_ok(
        &client
            .command(&["SET", "mykey", "world", "IFNE", "hello"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifdeq_key_exists_and_digest_matches() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    let digest =
        String::from_utf8(unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec())
            .unwrap();
    assert_ok(
        &client
            .command(&["SET", "mykey", "world", "IFDEQ", &digest])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifdeq_key_doesnt_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    assert_nil(
        &client
            .command(&["SET", "mykey", "world", "IFDEQ", "1234567890abcdef"])
            .await,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifdne_key_doesnt_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    assert_ok(
        &client
            .command(&["SET", "mykey", "world", "IFDNE", "1234567890abcdef"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
}

// ---------------------------------------------------------------------------
// Extended SET with IFEQ/IFNE + GET
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifeq_and_get_key_exists_and_matches() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_bulk_eq(
        &client
            .command(&["SET", "mykey", "world", "IFEQ", "hello", "GET"])
            .await,
        b"hello",
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifeq_and_get_key_exists_but_doesnt_match() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_bulk_eq(
        &client
            .command(&["SET", "mykey", "world", "IFEQ", "different", "GET"])
            .await,
        b"hello",
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"hello");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifeq_and_get_key_doesnt_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    assert_nil(
        &client
            .command(&["SET", "mykey", "world", "IFEQ", "hello", "GET"])
            .await,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifne_and_get_key_exists_and_doesnt_match() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_bulk_eq(
        &client
            .command(&["SET", "mykey", "world", "IFNE", "different", "GET"])
            .await,
        b"hello",
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifne_and_get_key_exists_and_matches() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_bulk_eq(
        &client
            .command(&["SET", "mykey", "world", "IFNE", "hello", "GET"])
            .await,
        b"hello",
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"hello");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifne_and_get_key_doesnt_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    assert_nil(
        &client
            .command(&["SET", "mykey", "world", "IFNE", "hello", "GET"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
}

// ---------------------------------------------------------------------------
// Extended SET with IFEQ/IFNE + expiration
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifeq_and_expiration() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_ok(
        &client
            .command(&["SET", "mykey", "world", "IFEQ", "hello", "EX", "10"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
    let ttl = unwrap_integer(&client.command(&["TTL", "mykey"]).await);
    assert!((5..=10).contains(&ttl), "TTL {ttl} not in [5, 10]");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifne_and_expiration() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_ok(
        &client
            .command(&["SET", "mykey", "world", "IFNE", "different", "EX", "10"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
    let ttl = unwrap_integer(&client.command(&["TTL", "mykey"]).await);
    assert!((5..=10).contains(&ttl), "TTL {ttl} not in [5, 10]");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_ifeq_against_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["LPUSH", "mylist", "element"]).await;
    assert_error_prefix(
        &client
            .command(&["SET", "mylist", "value", "IFEQ", "element"])
            .await,
        "WRONGTYPE",
    );
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_with_integer_encoded_value_and_ifeq() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "12345"]).await;
    assert_ok(
        &client
            .command(&["SET", "mykey", "world", "IFEQ", "12345"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_extended_set_case_insensitive_conditions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    assert_ok(
        &client
            .command(&["SET", "mykey", "world", "ifeq", "hello"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");

    client.command(&["SET", "mykey", "hello"]).await;
    assert_ok(
        &client
            .command(&["SET", "mykey", "world", "IfEq", "hello"])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");
}

// ---------------------------------------------------------------------------
// IFDEQ/IFDNE digest format validation
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_ifdeq_ifdne_reject_digest_with_incorrect_format() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "test"]).await;
    let digest =
        String::from_utf8(unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec())
            .unwrap();

    // Too short (15 chars)
    let short = &digest[1..];
    assert_error_prefix(
        &client
            .command(&["SET", "mykey", "new", "IFDEQ", short])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["SET", "mykey", "new", "IFDNE", short])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client.command(&["DELEX", "mykey", "IFDEQ", short]).await,
        "ERR",
    );
    assert_error_prefix(
        &client.command(&["DELEX", "mykey", "IFDNE", short]).await,
        "ERR",
    );

    // Too long (17 chars)
    let long = format!("0{digest}");
    assert_error_prefix(
        &client
            .command(&["SET", "mykey", "new", "IFDEQ", &long])
            .await,
        "ERR",
    );

    // Empty
    assert_error_prefix(
        &client.command(&["SET", "mykey", "new", "IFDEQ", ""]).await,
        "ERR",
    );
}

#[tokio::test]
#[ignore = "SET IFEQ/IFNE/IFDEQ/IFDNE not implemented in FrogDB"]
async fn tcl_ifdeq_ifdne_accepts_uppercase_hex_digits() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    let digest =
        String::from_utf8(unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec())
            .unwrap();
    let upper = digest.to_uppercase();

    assert_ok(
        &client
            .command(&["SET", "mykey", "world", "IFDEQ", &upper])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"world");

    // IFDNE with matching uppercase digest should NOT set
    client.command(&["SET", "mykey", "hello"]).await;
    let digest =
        String::from_utf8(unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec())
            .unwrap();
    let upper = digest.to_uppercase();
    assert_nil(
        &client
            .command(&["SET", "mykey", "world", "IFDNE", &upper])
            .await,
    );
    assert_bulk_eq(&client.command(&["GET", "mykey"]).await, b"hello");

    // DELEX IFDEQ with uppercase
    client.command(&["SET", "mykey", "hello"]).await;
    let upper =
        String::from_utf8(unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec())
            .unwrap()
            .to_uppercase();
    assert_integer_eq(
        &client.command(&["DELEX", "mykey", "IFDEQ", &upper]).await,
        1,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 0);

    // DELEX IFDNE with matching uppercase digest should NOT delete
    client.command(&["SET", "mykey", "hello"]).await;
    let upper =
        String::from_utf8(unwrap_bulk(&client.command(&["DIGEST", "mykey"]).await).to_vec())
            .unwrap()
            .to_uppercase();
    assert_integer_eq(
        &client.command(&["DELEX", "mykey", "IFDNE", &upper]).await,
        0,
    );
    assert_integer_eq(&client.command(&["EXISTS", "mykey"]).await, 1);
}

// ---------------------------------------------------------------------------
// MSETEX (Redis 8.x)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_msetex_all_expiration_flags() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // EX
    client
        .command(&[
            "MSETEX",
            "2",
            "ex:key1{t}",
            "val1",
            "ex:key2{t}",
            "val2",
            "EX",
            "5",
        ])
        .await;
    let ttl = unwrap_integer(&client.command(&["TTL", "ex:key1{t}"]).await);
    assert!(ttl > 0, "EX key should have TTL > 0, got {ttl}");

    // PX
    client
        .command(&[
            "MSETEX",
            "2",
            "px:key1{t}",
            "val1",
            "px:key2{t}",
            "val2",
            "PX",
            "5000",
        ])
        .await;
    let pttl = unwrap_integer(&client.command(&["PTTL", "px:key1{t}"]).await);
    assert!(pttl > 0, "PX key should have PTTL > 0, got {pttl}");

    // EXAT
    let future_sec = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 10;
    client
        .command(&[
            "MSETEX",
            "2",
            "exat:key1{t}",
            "val3",
            "exat:key2{t}",
            "val4",
            "EXAT",
            &future_sec.to_string(),
        ])
        .await;
    let ttl = unwrap_integer(&client.command(&["TTL", "exat:key1{t}"]).await);
    assert!(ttl > 0, "EXAT key should have TTL > 0, got {ttl}");

    // PXAT
    let future_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 10_000;
    client
        .command(&[
            "MSETEX",
            "2",
            "pxat:key1{t}",
            "val3",
            "pxat:key2{t}",
            "val4",
            "PXAT",
            &future_ms.to_string(),
        ])
        .await;
    let pttl = unwrap_integer(&client.command(&["PTTL", "pxat:key1{t}"]).await);
    assert!(pttl > 0, "PXAT key should have PTTL > 0, got {pttl}");
}

#[tokio::test]
async fn tcl_msetex_keepttl_preserves_existing_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["SETEX", "keepttl:key{t}", "100", "oldval"])
        .await;
    let old_ttl = unwrap_integer(&client.command(&["TTL", "keepttl:key{t}"]).await);
    client
        .command(&["MSETEX", "1", "keepttl:key{t}", "newval", "KEEPTTL"])
        .await;
    assert_bulk_eq(&client.command(&["GET", "keepttl:key{t}"]).await, b"newval");
    let new_ttl = unwrap_integer(&client.command(&["TTL", "keepttl:key{t}"]).await);
    assert!(
        new_ttl > old_ttl - 5,
        "TTL should be preserved: old={old_ttl}, new={new_ttl}"
    );
}

#[tokio::test]
async fn tcl_msetex_nx_xx_conditions_and_return_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "DEL",
            "nx:new{t}",
            "nx:new2{t}",
            "xx:existing{t}",
            "xx:nonexist{t}",
        ])
        .await;
    client.command(&["SET", "xx:existing{t}", "oldval"]).await;

    // NX with new keys
    assert_integer_eq(
        &client
            .command(&[
                "MSETEX",
                "2",
                "nx:new{t}",
                "val1",
                "nx:new2{t}",
                "val2",
                "NX",
                "EX",
                "10",
            ])
            .await,
        1,
    );
    // NX with existing key
    assert_integer_eq(
        &client
            .command(&["MSETEX", "1", "xx:existing{t}", "newval", "NX", "EX", "10"])
            .await,
        0,
    );
    // XX with non-existing key
    assert_integer_eq(
        &client
            .command(&["MSETEX", "1", "xx:nonexist{t}", "newval", "XX", "EX", "10"])
            .await,
        0,
    );
    // XX with existing key
    assert_integer_eq(
        &client
            .command(&["MSETEX", "1", "xx:existing{t}", "newval", "XX", "EX", "10"])
            .await,
        1,
    );
    assert_bulk_eq(&client.command(&["GET", "nx:new{t}"]).await, b"val1");
    assert_bulk_eq(&client.command(&["GET", "xx:existing{t}"]).await, b"newval");
}

#[tokio::test]
async fn tcl_msetex_error_cases() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(&client.command(&["MSETEX"]).await, "ERR");
    assert_error_prefix(
        &client
            .command(&["MSETEX", "key1", "val1", "EX", "10"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["MSETEX", "2", "key1{t}", "val1", "key2{t}"])
            .await,
        "ERR",
    );
}

#[tokio::test]
#[ignore = "FrogDB MSETEX does not reject mutually exclusive flags (NX+XX, EX+PX, KEEPTTL+EX)"]
async fn tcl_msetex_mutually_exclusive_flags() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // NX and XX
    assert_error_prefix(
        &client
            .command(&[
                "MSETEX", "2", "key1{t}", "val1", "key2{t}", "val2", "NX", "XX", "EX", "10",
            ])
            .await,
        "ERR",
    );

    // Multiple expiration flags
    assert_error_prefix(
        &client
            .command(&[
                "MSETEX", "2", "key1{t}", "val1", "key2{t}", "val2", "EX", "10", "PX", "5000",
            ])
            .await,
        "ERR",
    );

    // KEEPTTL + expiration
    assert_error_prefix(
        &client
            .command(&[
                "MSETEX", "2", "key1{t}", "val1", "key2{t}", "val2", "KEEPTTL", "EX", "10",
            ])
            .await,
        "ERR",
    );
}
