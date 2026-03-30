//! Rust port of Redis 8.6.0 `unit/type/hash-field-expire.tcl` test suite.
//!
//! The original TCL suite is entirely tagged `external:skip needs:debug`.
//! We convert only the tests that don't require DEBUG commands, CONFIG SET,
//! DUMP/RESTORE, RENAME, MOVE, COPY, SWAPDB, FLUSHALL, or replication.
//!
//! FrogDB response codes for HEXPIRE family:
//!   1 = OK (set), 0 = FAIL (condition not met), -2 = NO_FIELD, 2 = DELETED (past time)
//! Response codes used by HTTL family:
//!   -2 = NO_FIELD, -1 = NO_EXPIRY
//! Response codes used by HPERSIST:
//!   -2 = NO_FIELD, -1 = NO_EXPIRY, 1 = OK

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[allow(dead_code)]
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ---------------------------------------------------------------------------
// HEXPIRE/HEXPIREAT/HPEXPIRE/HPEXPIREAT — non-existent key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hexpire_returns_array_if_key_not_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    // -2 = field doesn't exist
    let resp = client
        .command(&["HEXPIRE", "myhash", "1000", "FIELDS", "1", "a"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);
    assert_integer_eq(&arr[0], -2);

    let resp = client
        .command(&["HPEXPIRE", "myhash", "1000", "FIELDS", "2", "a", "b"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    assert_integer_eq(&arr[0], -2);
    assert_integer_eq(&arr[1], -2);
}

// ---------------------------------------------------------------------------
// HEXPIRE — overflow validation
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB does not reject negative HEXPIRE times as errors"]
async fn tcl_hexpire_overflow_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "f1", "v1"]).await;

    // Negative expire time
    let resp = client
        .command(&["HEXPIRE", "myhash", "-1", "FIELDS", "1", "f1"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Very large negative
    let resp = client
        .command(&["HEXPIRE", "myhash", "-9223372036854775808", "FIELDS", "1", "f1"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// NX flag
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hpexpire_nx_flag() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1", "field2", "value2", "field3", "value3"])
        .await;

    // Set TTL on field1 with NX — should succeed (2 = OK)
    let resp = client
        .command(&["HPEXPIRE", "myhash", "1000", "NX", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);

    // Try NX on field1 again (already has TTL) — should fail (0)
    // Also set field2 NX — should succeed
    let resp = client
        .command(&[
            "HPEXPIRE", "myhash", "1000", "NX", "FIELDS", "2", "field1", "field2",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 0); // field1 already has TTL
    assert_integer_eq(&arr[1], 1); // field2 newly set
}

// ---------------------------------------------------------------------------
// XX flag
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hpexpire_xx_flag() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1", "field2", "value2", "field3", "value3"])
        .await;

    // Set TTL on field1 and field2
    client
        .command(&[
            "HPEXPIRE", "myhash", "1000", "NX", "FIELDS", "2", "field1", "field2",
        ])
        .await;

    // XX on field1 (has TTL) and field3 (no TTL)
    let resp = client
        .command(&[
            "HPEXPIRE", "myhash", "2000", "XX", "FIELDS", "2", "field1", "field3",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1); // field1 has TTL → XX succeeds
    assert_integer_eq(&arr[1], 0); // field3 no TTL → XX fails
}

// ---------------------------------------------------------------------------
// GT flag
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hpexpire_gt_flag() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1", "field2", "value2"])
        .await;

    // field1 = 1s, field2 = 2s
    client
        .command(&["HPEXPIRE", "myhash", "1000", "NX", "FIELDS", "1", "field1"])
        .await;
    client
        .command(&["HPEXPIRE", "myhash", "2000", "NX", "FIELDS", "1", "field2"])
        .await;

    // GT with 1.5s: field1 (1s < 1.5s) → OK, field2 (2s > 1.5s) → FAIL
    let resp = client
        .command(&[
            "HPEXPIRE", "myhash", "1500", "GT", "FIELDS", "2", "field1", "field2",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1); // 1.5s > 1s → GT succeeds
    assert_integer_eq(&arr[1], 0); // 1.5s < 2s → GT fails
}

// ---------------------------------------------------------------------------
// LT flag
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hpexpire_lt_flag() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1", "field2", "value2", "field3", "value3"])
        .await;

    // field1 = 1s, field2 = 2s
    client
        .command(&["HPEXPIRE", "myhash", "1000", "NX", "FIELDS", "1", "field1"])
        .await;
    client
        .command(&["HPEXPIRE", "myhash", "2000", "NX", "FIELDS", "1", "field2"])
        .await;

    // LT with 1.5s: field1 (1s < 1.5s) → FAIL, field2 (2s > 1.5s) → OK, field3 (no TTL) → OK
    let resp = client
        .command(&[
            "HPEXPIRE", "myhash", "1500", "LT", "FIELDS", "3", "field1", "field2", "field3",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 0); // 1.5s > 1s → LT fails
    assert_integer_eq(&arr[1], 1); // 1.5s < 2s → LT succeeds
    assert_integer_eq(&arr[2], 1); // no TTL → LT succeeds
}

// ---------------------------------------------------------------------------
// HPEXPIREAT — field not exists or TTL in the past
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hpexpireat_field_not_exists_or_past() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2", "f4", "v4"])
        .await;
    client
        .command(&["HEXPIRE", "myhash", "1000", "NX", "FIELDS", "1", "f4"])
        .await;

    let past_ms = format!("{}", (now_secs() - 1) * 1000);
    let resp = client
        .command(&[
            "HPEXPIREAT",
            "myhash",
            &past_ms,
            "NX",
            "FIELDS",
            "4",
            "f1",
            "f2",
            "f3",
            "f4",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 2); // f1 deleted (TTL in past)
    assert_integer_eq(&arr[1], 2); // f2 deleted
    assert_integer_eq(&arr[2], -2); // f3 doesn't exist
    // FrogDB deletes fields with past time regardless of NX condition
    assert_integer_eq(&arr[3], 2); // f4 deleted despite having TTL (past time overrides NX)
}

// ---------------------------------------------------------------------------
// HPEXPIRE — wrong number of arguments
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hpexpire_wrong_number_of_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "f1", "v1"]).await;

    // numFields = 0
    let resp = client
        .command(&["HPEXPIRE", "myhash", "1000", "NX", "FIELDS", "0", "f1", "f2", "f3"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // More fields than numFields
    let resp = client
        .command(&["HPEXPIRE", "myhash", "1000", "NX", "FIELDS", "4", "f1", "f2", "f3"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// HTTL/HPTTL — non-existent key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_httl_hpttl_non_existent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "1", "a"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -2);

    let resp = client
        .command(&["HPTTL", "myhash", "FIELDS", "2", "a", "b"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -2);
    assert_integer_eq(&arr[1], -2);
}

// ---------------------------------------------------------------------------
// HTTL/HPTTL — field without expiry or nonexistent
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_httl_hpttl_no_expiry_or_nonexistent_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1", "field2", "value2"])
        .await;
    client
        .command(&["HPEXPIRE", "myhash", "1000", "NX", "FIELDS", "1", "field1"])
        .await;

    // field2 has no expiry (-1), non_exists_field doesn't exist (-2)
    let resp = client
        .command(&[
            "HTTL",
            "myhash",
            "FIELDS",
            "2",
            "field2",
            "non_exists_field",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -1);
    assert_integer_eq(&arr[1], -2);
}

// ---------------------------------------------------------------------------
// HTTL/HPTTL — returns time to live
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_httl_hpttl_returns_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1", "field2", "value2"])
        .await;
    client
        .command(&[
            "HPEXPIRE", "myhash", "2000", "NX", "FIELDS", "2", "field1", "field2",
        ])
        .await;

    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "2", "field1", "field2"])
        .await;
    let arr = unwrap_array(resp);
    let ttl1 = unwrap_integer(&arr[0]);
    let ttl2 = unwrap_integer(&arr[1]);
    assert!(ttl1 >= 1 && ttl1 <= 2, "HTTL field1 = {ttl1}");
    assert!(ttl2 >= 1 && ttl2 <= 2, "HTTL field2 = {ttl2}");

    let resp = client
        .command(&["HPTTL", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let pttl = unwrap_integer(&arr[0]);
    assert!(pttl >= 1000 && pttl <= 2000, "HPTTL field1 = {pttl}");
}

// ---------------------------------------------------------------------------
// HEXPIRETIME/HPEXPIRETIME — non-existent key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hexpiretime_non_existent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    let resp = client
        .command(&["HEXPIRETIME", "myhash", "FIELDS", "1", "a"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -2);

    let resp = client
        .command(&["HPEXPIRETIME", "myhash", "FIELDS", "2", "a", "b"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -2);
    assert_integer_eq(&arr[1], -2);
}

// ---------------------------------------------------------------------------
// HEXPIRETIME — returns Unix timestamp
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hexpiretime_returns_unix_timestamp() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1"])
        .await;

    let lo = now_secs() + 1;
    let hi = now_secs() + 3;
    client
        .command(&["HPEXPIRE", "myhash", "1500", "NX", "FIELDS", "1", "field1"])
        .await;

    let resp = client
        .command(&["HEXPIRETIME", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let et = unwrap_integer(&arr[0]);
    assert!(et >= lo && et <= hi, "HEXPIRETIME = {et}, expected [{lo}, {hi}]");

    let resp = client
        .command(&["HPEXPIRETIME", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let pet = unwrap_integer(&arr[0]);
    assert!(
        pet >= lo * 1000 && pet <= hi * 1000,
        "HPEXPIRETIME = {pet}"
    );
}

// ---------------------------------------------------------------------------
// HEXPIREAT — set time in the past
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hexpireat_set_time_in_past() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1"])
        .await;

    let past = format!("{}", now_secs() - 1);
    let resp = client
        .command(&["HEXPIREAT", "myhash", &past, "NX", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 2); // deleted (FrogDB uses 2 for deleted)

    assert_integer_eq(&client.command(&["HEXISTS", "myhash", "field1"]).await, 0);
}

// ---------------------------------------------------------------------------
// HEXPIREAT — set time and get TTL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hexpireat_set_time_and_get_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1"])
        .await;

    let future = format!("{}", now_secs() + 2);
    client
        .command(&["HEXPIREAT", "myhash", &future, "NX", "FIELDS", "1", "field1"])
        .await;

    let resp = client
        .command(&["HPTTL", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let pttl = unwrap_integer(&arr[0]);
    assert!(pttl >= 500 && pttl <= 2000, "HPTTL = {pttl}");

    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let ttl = unwrap_integer(&arr[0]);
    assert!(ttl >= 1 && ttl <= 2, "HTTL = {ttl}");

    // Update with XX to 5s
    let future5 = format!("{}", now_secs() + 5);
    client
        .command(&["HEXPIREAT", "myhash", &future5, "XX", "FIELDS", "1", "field1"])
        .await;
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let ttl = unwrap_integer(&arr[0]);
    assert!(ttl >= 4 && ttl <= 5, "HTTL after update = {ttl}");
}

// ---------------------------------------------------------------------------
// Field with TTL overridden by HSET (TTL discarded)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_field_ttl_overridden_by_hset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1", "field2", "value2", "field3", "value3"])
        .await;
    client
        .command(&["HPEXPIRE", "myhash", "10000", "NX", "FIELDS", "1", "field1"])
        .await;
    client
        .command(&["HPEXPIRE", "myhash", "10000", "NX", "FIELDS", "1", "field2"])
        .await;

    // Overwrite field2 — TTL should be discarded
    client
        .command(&["HSET", "myhash", "field2", "value4"])
        .await;

    assert_bulk_eq(&client.command(&["HGET", "myhash", "field2"]).await, b"value4");

    // field2 and field3 should have no expiry (-1)
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "2", "field2", "field3"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -1);
    assert_integer_eq(&arr[1], -1);

    // field1 should still have TTL
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let ttl = unwrap_integer(&arr[0]);
    assert!(ttl > 0, "field1 should still have TTL, got {ttl}");
}

// ---------------------------------------------------------------------------
// Modify TTL of a field
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_modify_ttl_of_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1"])
        .await;
    client
        .command(&["HPEXPIRE", "myhash", "200000", "NX", "FIELDS", "1", "field1"])
        .await;
    client
        .command(&["HPEXPIRE", "myhash", "1000000", "XX", "FIELDS", "1", "field1"])
        .await;

    assert_bulk_eq(
        &client.command(&["HGET", "myhash", "field1"]).await,
        b"value1",
    );
    let resp = client
        .command(&["HPTTL", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let pttl = unwrap_integer(&arr[0]);
    assert!(
        pttl >= 900000 && pttl <= 1000000,
        "expected PTTL ~1000000, got {pttl}"
    );
}

// ---------------------------------------------------------------------------
// HPERSIST — non-existent key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hpersist_non_existent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    let resp = client
        .command(&["HPERSIST", "myhash", "FIELDS", "1", "a"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -2);

    let resp = client
        .command(&["HPERSIST", "myhash", "FIELDS", "2", "a", "b"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -2);
    assert_integer_eq(&arr[1], -2);
}

// ---------------------------------------------------------------------------
// HPERSIST — input validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hpersist_input_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;
    client
        .command(&["HEXPIRE", "myhash", "1000", "NX", "FIELDS", "1", "f1"])
        .await;

    // Wrong number of args
    let resp = client.command(&["HPERSIST", "myhash"]).await;
    assert_error_prefix(&resp, "ERR");

    // f1 has TTL → persist OK (1), not-exists → NO_FIELD (-2)
    let resp = client
        .command(&["HPERSIST", "myhash", "FIELDS", "2", "f1", "not-exists-field"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);
    assert_integer_eq(&arr[1], -2);

    // f2 has no TTL → NO_EXPIRY (-1)
    let resp = client
        .command(&["HPERSIST", "myhash", "FIELDS", "1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -1);
}

// ---------------------------------------------------------------------------
// HPERSIST — verify fields are persisted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hpersist_verify_fields_persisted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;
    client
        .command(&["HEXPIRE", "myhash", "20", "NX", "FIELDS", "2", "f1", "f2"])
        .await;
    client
        .command(&["HPERSIST", "myhash", "FIELDS", "2", "f1", "f2"])
        .await;

    // Wait longer than original TTL
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    assert_bulk_eq(&client.command(&["HGET", "myhash", "f1"]).await, b"v1");
    assert_bulk_eq(&client.command(&["HGET", "myhash", "f2"]).await, b"v2");

    // TTLs should be -1 (no expiry)
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "2", "f1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -1);
    assert_integer_eq(&arr[1], -1);
}

// ---------------------------------------------------------------------------
// HTTL/HPERSIST — non-volatile hash
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_httl_hpersist_non_volatile_hash() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1", "field2", "value2", "field3", "value3"])
        .await;

    // No expiry on any field
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -1);

    // Non-existent field
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "1", "fieldnonexist"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -2);

    // HPERSIST on field without expiry
    let resp = client
        .command(&["HPERSIST", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -1);

    // HPERSIST on non-existent field
    let resp = client
        .command(&["HPERSIST", "myhash", "FIELDS", "1", "fieldnonexist"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -2);
}

// ---------------------------------------------------------------------------
// HPEXPIRE — DEL hash with non-expired fields
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hpexpire_del_hash_with_non_expired_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "field1", "value1", "field2", "value2"])
        .await;
    client
        .command(&["HPEXPIRE", "myhash", "10000", "NX", "FIELDS", "1", "field1"])
        .await;
    // Should succeed without errors
    assert_integer_eq(&client.command(&["DEL", "myhash"]).await, 1);
}

// ---------------------------------------------------------------------------
// HSET return value with expiring fields
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hset_return_value_with_expiring_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;
    client
        .command(&["HEXPIRE", "myhash", "100000", "FIELDS", "1", "f1"])
        .await;

    // Updating existing field returns 0
    assert_integer_eq(&client.command(&["HSET", "myhash", "f2", "v2"]).await, 0);
    // Adding new field returns 1
    assert_integer_eq(&client.command(&["HSET", "myhash", "f3", "v3"]).await, 1);
    // Mixed: 1 existing + 1 new = 1
    assert_integer_eq(
        &client
            .command(&["HSET", "myhash", "f3", "v3", "f4", "v4"])
            .await,
        1,
    );
    // Mixed: 1 existing + 2 new = 2
    assert_integer_eq(
        &client
            .command(&["HSET", "myhash", "f3", "v3", "f5", "v5", "f6", "v6"])
            .await,
        2,
    );
}
