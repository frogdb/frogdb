//! Rust port of Redis 8.6.0 `unit/type/incr.tcl` test suite.
//!
//! Excludes: `assert_encoding`/`assert_refcount` tests (require DEBUG OBJECT),
//! `needs:debug` tests.

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// INCR basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_incr_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_integer_eq(&client.command(&["INCR", "novar"]).await, 1);
    assert_bulk_eq(&client.command(&["GET", "novar"]).await, b"1");
}

#[tokio::test]
async fn tcl_incr_against_key_created_by_incr_itself() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["INCR", "novar"]).await;
    assert_integer_eq(&client.command(&["INCR", "novar"]).await, 2);
}

#[tokio::test]
async fn tcl_decr_against_key_created_by_incr() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["INCR", "novar"]).await;
    assert_integer_eq(&client.command(&["DECR", "novar"]).await, 0);
}

#[tokio::test]
async fn tcl_decr_against_key_not_exist_and_incr() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "novar_not_exist"]).await;
    assert_integer_eq(&client.command(&["DECR", "novar_not_exist"]).await, -1);
    assert_integer_eq(&client.command(&["INCR", "novar_not_exist"]).await, 0);
}

#[tokio::test]
async fn tcl_incr_against_key_originally_set_with_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "100"]).await;
    assert_integer_eq(&client.command(&["INCR", "novar"]).await, 101);
}

#[tokio::test]
async fn tcl_incr_over_32bit_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "17179869184"]).await;
    assert_integer_eq(&client.command(&["INCR", "novar"]).await, 17179869185);
}

#[tokio::test]
async fn tcl_incrby_over_32bit_value_with_over_32bit_increment() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "17179869184"]).await;
    assert_integer_eq(
        &client.command(&["INCRBY", "novar", "17179869184"]).await,
        34359738368,
    );
}

// ---------------------------------------------------------------------------
// INCR error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_incr_fails_against_key_with_spaces_left() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "    11"]).await;
    assert_error_prefix(&client.command(&["INCR", "novar"]).await, "ERR");
}

#[tokio::test]
async fn tcl_incr_fails_against_key_with_spaces_right() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "11    "]).await;
    assert_error_prefix(&client.command(&["INCR", "novar"]).await, "ERR");
}

#[tokio::test]
async fn tcl_incr_fails_against_key_with_spaces_both() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "    11    "]).await;
    assert_error_prefix(&client.command(&["INCR", "novar"]).await, "ERR");
}

#[tokio::test]
async fn tcl_decrby_negation_overflow() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "0"]).await;
    assert_error_prefix(
        &client
            .command(&["DECRBY", "x", "-9223372036854775808"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_incr_fails_against_key_holding_a_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "1"]).await;
    assert_error_prefix(&client.command(&["INCR", "mylist"]).await, "WRONGTYPE");
    client.command(&["DEL", "mylist"]).await;
}

// ---------------------------------------------------------------------------
// DECRBY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_decrby_over_32bit_value_with_over_32bit_increment_negative_res() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "17179869184"]).await;
    assert_integer_eq(
        &client.command(&["DECRBY", "novar", "17179869185"]).await,
        -1,
    );
}

#[tokio::test]
async fn tcl_decrby_against_key_not_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "key_not_exist"]).await;
    assert_integer_eq(
        &client.command(&["DECRBY", "key_not_exist", "1"]).await,
        -1,
    );
}

// ---------------------------------------------------------------------------
// INCRBYFLOAT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_incrbyfloat_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "novar"]).await;
    assert_bulk_eq(&client.command(&["INCRBYFLOAT", "novar", "1"]).await, b"1");
    assert_bulk_eq(&client.command(&["GET", "novar"]).await, b"1");
    assert_bulk_eq(
        &client.command(&["INCRBYFLOAT", "novar", "0.25"]).await,
        b"1.25",
    );
    assert_bulk_eq(&client.command(&["GET", "novar"]).await, b"1.25");
}

#[tokio::test]
async fn tcl_incrbyfloat_against_key_originally_set_with_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "1.5"]).await;
    assert_bulk_eq(
        &client.command(&["INCRBYFLOAT", "novar", "1.5"]).await,
        b"3",
    );
}

#[tokio::test]
async fn tcl_incrbyfloat_over_32bit_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "17179869184"]).await;
    assert_bulk_eq(
        &client.command(&["INCRBYFLOAT", "novar", "1.5"]).await,
        b"17179869185.5",
    );
}

#[tokio::test]
async fn tcl_incrbyfloat_over_32bit_value_with_over_32bit_increment() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "17179869184"]).await;
    assert_bulk_eq(
        &client
            .command(&["INCRBYFLOAT", "novar", "17179869184"])
            .await,
        b"34359738368",
    );
}

#[tokio::test]
async fn tcl_incrbyfloat_fails_against_key_with_spaces_left() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "    11"]).await;
    assert_error_prefix(
        &client.command(&["INCRBYFLOAT", "novar", "1.0"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_incrbyfloat_fails_against_key_with_spaces_right() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "11    "]).await;
    assert_error_prefix(
        &client.command(&["INCRBYFLOAT", "novar", "1.0"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_incrbyfloat_fails_against_key_with_spaces_both() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", " 11 "]).await;
    assert_error_prefix(
        &client.command(&["INCRBYFLOAT", "novar", "1.0"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_incrbyfloat_fails_against_key_holding_a_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["RPUSH", "mylist", "1"]).await;
    assert_error_prefix(
        &client.command(&["INCRBYFLOAT", "mylist", "1.0"]).await,
        "WRONGTYPE",
    );
    client.command(&["DEL", "mylist"]).await;
}

#[tokio::test]
async fn tcl_incrbyfloat_does_not_allow_nan_or_infinity() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "0"]).await;
    assert_error_prefix(
        &client.command(&["INCRBYFLOAT", "foo", "+inf"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_incrbyfloat_decrement() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "1"]).await;
    let resp = client.command(&["INCRBYFLOAT", "foo", "-1.1"]).await;
    let val = unwrap_bulk(&resp);
    let f: f64 = std::str::from_utf8(val).unwrap().parse().unwrap();
    assert!((f - (-0.1)).abs() < 1e-10, "expected ~-0.1, got {f}");
}

#[tokio::test]
async fn tcl_string_to_double_with_null_terminator() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "1"]).await;
    client.command(&["SETRANGE", "foo", "2", "2"]).await;
    assert_error_prefix(
        &client.command(&["INCRBYFLOAT", "foo", "1"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_no_negative_zero() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    // 1/41 ≈ 0.024390243902439025
    let inc = format!("{}", 1.0_f64 / 41.0);
    let dec = format!("{}", -1.0_f64 / 41.0);
    client.command(&["INCRBYFLOAT", "foo", &inc]).await;
    client.command(&["INCRBYFLOAT", "foo", &dec]).await;
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"0");
}

// ---------------------------------------------------------------------------
// Unhappy paths (wrong number of args, invalid types)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_incrby_incrbyfloat_decrby_unhappy_path() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykeyincr"]).await;

    // Wrong number of arguments
    assert_error_prefix(
        &client.command(&["INCR", "mykeyincr", "v"]).await,
        "ERR wrong number of arguments",
    );
    assert_error_prefix(
        &client.command(&["DECR", "mykeyincr", "v"]).await,
        "ERR wrong number of arguments",
    );

    // Invalid increment values
    assert_error_prefix(
        &client.command(&["INCRBY", "mykeyincr", "v"]).await,
        "ERR value is not an integer",
    );
    assert_error_prefix(
        &client.command(&["INCRBY", "mykeyincr", "1.5"]).await,
        "ERR value is not an integer",
    );
    assert_error_prefix(
        &client.command(&["DECRBY", "mykeyincr", "v"]).await,
        "ERR value is not an integer",
    );
    assert_error_prefix(
        &client.command(&["DECRBY", "mykeyincr", "1.5"]).await,
        "ERR value is not an integer",
    );
    assert_error_prefix(
        &client.command(&["INCRBYFLOAT", "mykeyincr", "v"]).await,
        "ERR value is not a valid float",
    );
}
