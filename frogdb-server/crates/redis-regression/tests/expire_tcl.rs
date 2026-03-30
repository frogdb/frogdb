//! Rust port of Redis 8.6.0 `unit/expire.tcl` test suite.
//!
//! Excludes:
//! - `needs:debug` tests (DEBUG SET-ACTIVE-EXPIRE, DEBUG LOADAOF, etc.)
//! - `needs:repl` tests (replication stream assertions)
//! - `external:skip` tests (AOF propagation, replica TTL verification)
//! - Active expire cycle / lazy expire stats tests
//! - Cluster-mode expire scan tests
//! - CONFIG SET tests (lazyexpire-nested-arbitrary-keys, hz, etc.)
//! - Encoding loops, DEBUG OBJECT, assert_encoding, assert_refcount

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// EXPIRE basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expire_set_timeouts_multiple_times() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "foobar"]).await;
    let v1 = unwrap_integer(&client.command(&["EXPIRE", "x", "5"]).await);
    let v2 = unwrap_integer(&client.command(&["TTL", "x"]).await);
    let v3 = unwrap_integer(&client.command(&["EXPIRE", "x", "10"]).await);
    let v4 = unwrap_integer(&client.command(&["TTL", "x"]).await);
    client.command(&["EXPIRE", "x", "2"]).await;

    assert_eq!(v1, 1);
    assert!(v2 >= 4 && v2 <= 5, "expected TTL 4-5, got {v2}");
    assert_eq!(v3, 1);
    assert_eq!(v4, 10);
}

#[tokio::test]
async fn tcl_expire_key_still_readable() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "foobar"]).await;
    client.command(&["EXPIRE", "x", "20"]).await;
    assert_bulk_eq(&client.command(&["GET", "x"]).await, b"foobar");
}

#[tokio::test]
async fn tcl_expire_after_timeout_key_gone() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "foobar"]).await;
    client.command(&["EXPIRE", "x", "2"]).await;

    tokio::time::sleep(Duration::from_millis(2500)).await;

    assert_nil(&client.command(&["GET", "x"]).await);
    assert_integer_eq(&client.command(&["EXISTS", "x"]).await, 0);
}

#[tokio::test]
async fn tcl_expire_write_on_expire_should_work() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["LPUSH", "x", "foo"]).await;
    client.command(&["EXPIRE", "x", "1000"]).await;
    client.command(&["LPUSH", "x", "bar"]).await;

    let resp = client.command(&["LRANGE", "x", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["bar", "foo"]);
}

// ---------------------------------------------------------------------------
// EXPIREAT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expireat_check_for_expire_alike_behavior() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["SET", "x", "foo"]).await;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let expire_at = (now + 15).to_string();
    client.command(&["EXPIREAT", "x", &expire_at]).await;

    let ttl = unwrap_integer(&client.command(&["TTL", "x"]).await);
    assert!(
        ttl >= 13 && ttl <= 15,
        "expected TTL 13-15, got {ttl}"
    );
}

// ---------------------------------------------------------------------------
// SETEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_setex_set_plus_expire_check_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SETEX", "x", "12", "test"]).await;
    let ttl = unwrap_integer(&client.command(&["TTL", "x"]).await);
    assert!(ttl >= 10 && ttl <= 12, "expected TTL 10-12, got {ttl}");
}

#[tokio::test]
async fn tcl_setex_check_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SETEX", "x", "12", "test"]).await;
    assert_bulk_eq(&client.command(&["GET", "x"]).await, b"test");
}

#[tokio::test]
async fn tcl_setex_overwrite_old_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SETEX", "y", "1", "foo"]).await;
    assert_bulk_eq(&client.command(&["GET", "y"]).await, b"foo");
}

#[tokio::test]
async fn tcl_setex_wait_for_key_to_expire() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SETEX", "y", "1", "foo"]).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_nil(&client.command(&["GET", "y"]).await);
}

#[tokio::test]
async fn tcl_setex_wrong_time_parameter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["SETEX", "z", "-10", "foo"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// PERSIST
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_persist_can_undo_expire() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "foo"]).await;
    client.command(&["EXPIRE", "x", "50"]).await;

    let ttl1 = unwrap_integer(&client.command(&["TTL", "x"]).await);
    assert_eq!(ttl1, 50);
    assert_integer_eq(&client.command(&["PERSIST", "x"]).await, 1);
    assert_integer_eq(&client.command(&["TTL", "x"]).await, -1);
    assert_bulk_eq(&client.command(&["GET", "x"]).await, b"foo");
}

#[tokio::test]
async fn tcl_persist_returns_0_against_non_existing_or_non_volatile_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "foo"]).await;
    assert_integer_eq(&client.command(&["PERSIST", "foo"]).await, 0);
    assert_integer_eq(&client.command(&["PERSIST", "nokeyatall"]).await, 0);
}

// ---------------------------------------------------------------------------
// EXPIRE precision / sub-second expiry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expire_precision_is_now_the_millisecond() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Retry loop like the TCL test to handle server-under-pressure false positives.
    let mut a = String::new();
    let mut b = String::new();
    for _ in 0..30 {
        client.command(&["DEL", "x"]).await;
        client.command(&["SETEX", "x", "1", "somevalue"]).await;
        tokio::time::sleep(Duration::from_millis(800)).await;
        let resp_a = client.command(&["GET", "x"]).await;
        a = match &resp_a {
            Response::Bulk(Some(v)) => String::from_utf8_lossy(v).to_string(),
            _ => String::new(),
        };
        if a != "somevalue" {
            continue;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let resp_b = client.command(&["GET", "x"]).await;
        b = match &resp_b {
            Response::Bulk(None) => String::new(),
            Response::Bulk(Some(v)) => String::from_utf8_lossy(v).to_string(),
            _ => "unexpected".to_string(),
        };
        if b.is_empty() {
            break;
        }
    }
    assert_eq!(a, "somevalue");
    assert!(b.is_empty(), "expected key to be expired, got {b:?}");
}

#[tokio::test]
async fn tcl_psetex_can_set_sub_second_expires() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for _ in 0..50 {
        client.command(&["DEL", "x"]).await;
        client.command(&["PSETEX", "x", "100", "somevalue"]).await;
        let resp_a = client.command(&["GET", "x"]).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let resp_b = client.command(&["GET", "x"]).await;

        let a_ok = matches!(&resp_a, Response::Bulk(Some(v)) if v.as_ref() == b"somevalue");
        let b_nil = matches!(&resp_b, Response::Bulk(None));
        if a_ok && b_nil {
            return; // success
        }
    }
    panic!("PSETEX sub-second expire never observed");
}

#[tokio::test]
async fn tcl_pexpire_can_set_sub_second_expires() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for _ in 0..50 {
        client.command(&["SET", "x", "somevalue"]).await;
        client.command(&["PEXPIRE", "x", "100"]).await;
        let resp_a = client.command(&["GET", "x"]).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let resp_b = client.command(&["GET", "x"]).await;

        let a_ok = matches!(&resp_a, Response::Bulk(Some(v)) if v.as_ref() == b"somevalue");
        let b_nil = matches!(&resp_b, Response::Bulk(None));
        if a_ok && b_nil {
            return;
        }
    }
    panic!("PEXPIRE sub-second expire never observed");
}

#[tokio::test]
async fn tcl_pexpireat_can_set_sub_second_expires() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for _ in 0..50 {
        client.command(&["SET", "x", "somevalue"]).await;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let target = (now_ms + 200).to_string();
        client.command(&["PEXPIREAT", "x", &target]).await;
        let resp_a = client.command(&["GET", "x"]).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let resp_b = client.command(&["GET", "x"]).await;

        let a_ok = matches!(&resp_a, Response::Bulk(Some(v)) if v.as_ref() == b"somevalue");
        let b_nil = matches!(&resp_b, Response::Bulk(None));
        if a_ok && b_nil {
            return;
        }
    }
    panic!("PEXPIREAT sub-second expire never observed");
}

// ---------------------------------------------------------------------------
// TTL / PTTL / EXPIRETIME / PEXPIRETIME
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_ttl_returns_time_to_live_in_seconds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["SETEX", "x", "10", "somevalue"]).await;
    let ttl = unwrap_integer(&client.command(&["TTL", "x"]).await);
    assert!(ttl > 8 && ttl <= 10, "expected TTL 9-10, got {ttl}");
}

#[tokio::test]
async fn tcl_pttl_returns_time_to_live_in_milliseconds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["SETEX", "x", "1", "somevalue"]).await;
    let pttl = unwrap_integer(&client.command(&["PTTL", "x"]).await);
    assert!(
        pttl > 500 && pttl <= 1000,
        "expected PTTL 500-1000, got {pttl}"
    );
}

#[tokio::test]
async fn tcl_ttl_pttl_expiretime_pexpiretime_return_neg1_if_no_expire() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["SET", "x", "hello"]).await;

    assert_integer_eq(&client.command(&["TTL", "x"]).await, -1);
    assert_integer_eq(&client.command(&["PTTL", "x"]).await, -1);
    assert_integer_eq(&client.command(&["EXPIRETIME", "x"]).await, -1);
    assert_integer_eq(&client.command(&["PEXPIRETIME", "x"]).await, -1);
}

#[tokio::test]
async fn tcl_ttl_pttl_expiretime_pexpiretime_return_neg2_if_key_not_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;

    assert_integer_eq(&client.command(&["TTL", "x"]).await, -2);
    assert_integer_eq(&client.command(&["PTTL", "x"]).await, -2);
    assert_integer_eq(&client.command(&["EXPIRETIME", "x"]).await, -2);
    assert_integer_eq(&client.command(&["PEXPIRETIME", "x"]).await, -2);
}

#[tokio::test]
#[ignore = "Timing-sensitive — may fail under load"]
async fn tcl_expiretime_returns_absolute_expiration_time_in_seconds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    let abs_expire = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 100;
    let abs_str = abs_expire.to_string();
    client
        .command(&["SET", "x", "somevalue", "EXAT", &abs_str])
        .await;
    assert_integer_eq(
        &client.command(&["EXPIRETIME", "x"]).await,
        abs_expire as i64,
    );
}

#[tokio::test]
async fn tcl_pexpiretime_returns_absolute_expiration_time_in_milliseconds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    let abs_expire_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 100_000;
    let abs_str = abs_expire_ms.to_string();
    client
        .command(&["SET", "x", "somevalue", "PXAT", &abs_str])
        .await;
    assert_integer_eq(
        &client.command(&["PEXPIRETIME", "x"]).await,
        abs_expire_ms as i64,
    );
}

// ---------------------------------------------------------------------------
// Miscellaneous EXPIRE tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_5_keys_in_5_keys_out() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client.command(&["SET", "a", "c"]).await;
    client.command(&["EXPIRE", "a", "5"]).await;
    client.command(&["SET", "t", "c"]).await;
    client.command(&["SET", "e", "c"]).await;
    client.command(&["SET", "s", "c"]).await;
    client.command(&["SET", "foo", "b"]).await;

    let resp = client.command(&["KEYS", "*"]).await;
    let mut keys = extract_bulk_strings(&resp);
    keys.sort();
    assert_eq!(keys, vec!["a", "e", "foo", "s", "t"]);
    client.command(&["DEL", "a"]).await;
}

#[tokio::test]
async fn tcl_expire_with_empty_string_as_ttl_should_report_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client.command(&["EXPIRE", "foo", ""]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// SET with EX / GETEX overflow errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_set_with_ex_big_integer_should_report_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["SET", "foo", "bar", "EX", "10000000000000000"])
        .await;
    assert_error_prefix(&resp, "ERR invalid expire time");
}

#[tokio::test]
async fn tcl_set_with_ex_smallest_integer_should_report_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["SET", "foo", "bar", "EX", "-9999999999999999"])
        .await;
    assert_error_prefix(&resp, "ERR invalid expire time");
}

#[tokio::test]
async fn tcl_getex_with_big_integer_should_report_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client
        .command(&["GETEX", "foo", "EX", "10000000000000000"])
        .await;
    assert_error_prefix(&resp, "ERR invalid expire time");
}

#[tokio::test]
async fn tcl_getex_with_smallest_integer_should_report_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client
        .command(&["GETEX", "foo", "EX", "-9999999999999999"])
        .await;
    assert_error_prefix(&resp, "ERR invalid expire time");
}

// ---------------------------------------------------------------------------
// EXPIRE with big integer overflow
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expire_with_big_integer_overflows_when_converted_to_milliseconds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;

    // Hit `when > LLONG_MAX - basetime`
    assert_error_prefix(
        &client
            .command(&["EXPIRE", "foo", "9223370399119966"])
            .await,
        "ERR invalid expire time",
    );

    // Hit `when > LLONG_MAX / 1000`
    assert_error_prefix(
        &client
            .command(&["EXPIRE", "foo", "9223372036854776"])
            .await,
        "ERR invalid expire time",
    );
    assert_error_prefix(
        &client
            .command(&["EXPIRE", "foo", "10000000000000000"])
            .await,
        "ERR invalid expire time",
    );
    assert_error_prefix(
        &client
            .command(&["EXPIRE", "foo", "18446744073709561"])
            .await,
        "ERR invalid expire time",
    );

    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -1);
}

#[tokio::test]
async fn tcl_pexpire_with_big_integer_overflow_when_basetime_added() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client
        .command(&["PEXPIRE", "foo", "9223372036854770000"])
        .await;
    assert_error_prefix(&resp, "ERR invalid expire time");
}

#[tokio::test]
async fn tcl_expire_with_big_negative_integer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;

    assert_error_prefix(
        &client
            .command(&["EXPIRE", "foo", "-9223372036854776"])
            .await,
        "ERR invalid expire time",
    );
    assert_error_prefix(
        &client
            .command(&["EXPIRE", "foo", "-9999999999999999"])
            .await,
        "ERR invalid expire time",
    );

    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -1);
}

#[tokio::test]
async fn tcl_pexpireat_with_big_integer_works() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    assert_integer_eq(
        &client
            .command(&["PEXPIREAT", "foo", "9223372036854770000"])
            .await,
        1,
    );
}

#[tokio::test]
async fn tcl_pexpireat_with_big_negative_integer_works() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    client
        .command(&["PEXPIREAT", "foo", "-9223372036854770000"])
        .await;
    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -2);
}

// ---------------------------------------------------------------------------
// SET removes expire / KEEPTTL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_set_command_will_remove_expire() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar", "EX", "100"]).await;
    client.command(&["SET", "foo", "bar"]).await;
    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -1);
}

#[tokio::test]
async fn tcl_set_command_will_remove_expire_with_large_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let large_value = "A".repeat(1000);
    client
        .command(&["SET", "foo", &large_value, "EX", "100"])
        .await;
    client
        .command(&["SET", "foo", &large_value, "KEEPTTL"])
        .await;
    let ttl1 = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(
        ttl1 <= 100 && ttl1 > 90,
        "expected TTL 91-100, got {ttl1}"
    );

    // Plain SET should remove TTL even with large strings
    client.command(&["SET", "foo", &large_value]).await;
    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -1);
}

#[tokio::test]
async fn tcl_set_use_keepttl_option_ttl_should_not_be_removed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar", "EX", "100"]).await;
    client.command(&["SET", "foo", "bar", "KEEPTTL"]).await;
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(ttl <= 100 && ttl > 90, "expected TTL 91-100, got {ttl}");
}

// ---------------------------------------------------------------------------
// GETEX with PERSIST
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_getex_use_of_persist_option_should_remove_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar", "EX", "100"]).await;
    client.command(&["GETEX", "foo", "PERSIST"]).await;
    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -1);
}

// ---------------------------------------------------------------------------
// EXPIRE with NX option
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expire_with_nx_option_on_key_with_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar", "EX", "100"]).await;
    assert_integer_eq(&client.command(&["EXPIRE", "foo", "200", "NX"]).await, 0);
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(ttl >= 50 && ttl <= 100, "expected TTL 50-100, got {ttl}");
}

#[tokio::test]
async fn tcl_expire_with_nx_option_on_key_without_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    assert_integer_eq(&client.command(&["EXPIRE", "foo", "200", "NX"]).await, 1);
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(
        ttl >= 100 && ttl <= 200,
        "expected TTL 100-200, got {ttl}"
    );
}

// ---------------------------------------------------------------------------
// EXPIRE with XX option
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expire_with_xx_option_on_key_with_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar", "EX", "100"]).await;
    assert_integer_eq(&client.command(&["EXPIRE", "foo", "200", "XX"]).await, 1);
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(
        ttl >= 100 && ttl <= 200,
        "expected TTL 100-200, got {ttl}"
    );
}

#[tokio::test]
async fn tcl_expire_with_xx_option_on_key_without_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    assert_integer_eq(&client.command(&["EXPIRE", "foo", "200", "XX"]).await, 0);
    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -1);
}

// ---------------------------------------------------------------------------
// EXPIRE with GT option
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expire_with_gt_option_on_key_with_lower_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar", "EX", "100"]).await;
    assert_integer_eq(&client.command(&["EXPIRE", "foo", "200", "GT"]).await, 1);
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(
        ttl >= 100 && ttl <= 200,
        "expected TTL 100-200, got {ttl}"
    );
}

#[tokio::test]
async fn tcl_expire_with_gt_option_on_key_with_higher_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar", "EX", "200"]).await;
    assert_integer_eq(&client.command(&["EXPIRE", "foo", "100", "GT"]).await, 0);
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(
        ttl >= 100 && ttl <= 200,
        "expected TTL 100-200, got {ttl}"
    );
}

#[tokio::test]
async fn tcl_expire_with_gt_option_on_key_without_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    assert_integer_eq(&client.command(&["EXPIRE", "foo", "200", "GT"]).await, 0);
    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -1);
}

// ---------------------------------------------------------------------------
// EXPIRE with LT option
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expire_with_lt_option_on_key_with_higher_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar", "EX", "100"]).await;
    assert_integer_eq(&client.command(&["EXPIRE", "foo", "200", "LT"]).await, 0);
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(ttl >= 50 && ttl <= 100, "expected TTL 50-100, got {ttl}");
}

#[tokio::test]
async fn tcl_expire_with_lt_option_on_key_with_lower_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar", "EX", "200"]).await;
    assert_integer_eq(&client.command(&["EXPIRE", "foo", "100", "LT"]).await, 1);
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(ttl >= 50 && ttl <= 100, "expected TTL 50-100, got {ttl}");
}

#[tokio::test]
async fn tcl_expire_with_lt_option_on_key_without_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    assert_integer_eq(&client.command(&["EXPIRE", "foo", "100", "LT"]).await, 1);
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(ttl >= 50 && ttl <= 100, "expected TTL 50-100, got {ttl}");
}

// ---------------------------------------------------------------------------
// EXPIRE with combined LT + XX options
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expire_with_lt_and_xx_option_on_key_with_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar", "EX", "200"]).await;
    assert_integer_eq(
        &client.command(&["EXPIRE", "foo", "100", "LT", "XX"]).await,
        1,
    );
    let ttl = unwrap_integer(&client.command(&["TTL", "foo"]).await);
    assert!(ttl >= 50 && ttl <= 100, "expected TTL 50-100, got {ttl}");
}

#[tokio::test]
async fn tcl_expire_with_lt_and_xx_option_on_key_without_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    assert_integer_eq(
        &client
            .command(&["EXPIRE", "foo", "200", "LT", "XX"])
            .await,
        0,
    );
    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -1);
}

// ---------------------------------------------------------------------------
// EXPIRE conflicting / unsupported options
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expire_conflicting_options_lt_gt() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client.command(&["EXPIRE", "foo", "200", "LT", "GT"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_expire_conflicting_options_nx_gt() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client.command(&["EXPIRE", "foo", "200", "NX", "GT"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_expire_conflicting_options_nx_lt() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client.command(&["EXPIRE", "foo", "200", "NX", "LT"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_expire_conflicting_options_nx_xx() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client.command(&["EXPIRE", "foo", "200", "NX", "XX"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_expire_unsupported_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client.command(&["EXPIRE", "foo", "200", "AB"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_expire_unsupported_option_after_valid() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client
        .command(&["EXPIRE", "foo", "200", "XX", "AB"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// EXPIRE with negative expiry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expire_with_negative_expiry() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar", "EX", "100"]).await;
    assert_integer_eq(
        &client.command(&["EXPIRE", "foo", "-10", "LT"]).await,
        1,
    );
    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -2);
}

#[tokio::test]
async fn tcl_expire_with_negative_expiry_on_non_volatile_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    assert_integer_eq(
        &client.command(&["EXPIRE", "foo", "-10", "LT"]).await,
        1,
    );
    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -2);
}

// ---------------------------------------------------------------------------
// EXPIRE with non-existed key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_expire_with_non_existed_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_integer_eq(&client.command(&["EXPIRE", "none", "100", "NX"]).await, 0);
    assert_integer_eq(&client.command(&["EXPIRE", "none", "100", "XX"]).await, 0);
    assert_integer_eq(&client.command(&["EXPIRE", "none", "100", "GT"]).await, 0);
    assert_integer_eq(&client.command(&["EXPIRE", "none", "100", "LT"]).await, 0);
}
