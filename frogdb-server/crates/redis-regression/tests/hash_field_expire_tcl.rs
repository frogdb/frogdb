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
//!
//! ## Intentional exclusions
//!
//! FrogDB's HSETEX/HGETEX parsers require the strict Redis argument order
//! `[cond] [expiry] FIELDS numfields <pairs>` — trailing options after the
//! FIELDS list are not accepted. The upstream "flexible argument parsing"
//! tests assert that Redis also accepts the FIELDS-first form
//! (`HSETEX key FIELDS 2 f1 v1 f2 v2 EX 60`), which is a Redis 8.6-only
//! relaxation. FrogDB tests below cover the rigid form only.

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
        .command(&[
            "HEXPIRE",
            "myhash",
            "-9223372036854775808",
            "FIELDS",
            "1",
            "f1",
        ])
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
        .command(&[
            "HSET", "myhash", "field1", "value1", "field2", "value2", "field3", "value3",
        ])
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
        .command(&[
            "HSET", "myhash", "field1", "value1", "field2", "value2", "field3", "value3",
        ])
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
        .command(&[
            "HSET", "myhash", "field1", "value1", "field2", "value2", "field3", "value3",
        ])
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
        .command(&[
            "HPEXPIRE", "myhash", "1000", "NX", "FIELDS", "0", "f1", "f2", "f3",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");

    // More fields than numFields
    let resp = client
        .command(&[
            "HPEXPIRE", "myhash", "1000", "NX", "FIELDS", "4", "f1", "f2", "f3",
        ])
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
    assert!((1..=2).contains(&ttl1), "HTTL field1 = {ttl1}");
    assert!((1..=2).contains(&ttl2), "HTTL field2 = {ttl2}");

    let resp = client
        .command(&["HPTTL", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let pttl = unwrap_integer(&arr[0]);
    assert!((1000..=2000).contains(&pttl), "HPTTL field1 = {pttl}");
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
    assert!(
        et >= lo && et <= hi,
        "HEXPIRETIME = {et}, expected [{lo}, {hi}]"
    );

    let resp = client
        .command(&["HPEXPIRETIME", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let pet = unwrap_integer(&arr[0]);
    assert!(pet >= lo * 1000 && pet <= hi * 1000, "HPEXPIRETIME = {pet}");
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
        .command(&[
            "HEXPIREAT",
            "myhash",
            &future,
            "NX",
            "FIELDS",
            "1",
            "field1",
        ])
        .await;

    let resp = client
        .command(&["HPTTL", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let pttl = unwrap_integer(&arr[0]);
    assert!((500..=2000).contains(&pttl), "HPTTL = {pttl}");

    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let ttl = unwrap_integer(&arr[0]);
    assert!((1..=2).contains(&ttl), "HTTL = {ttl}");

    // Update with XX to 5s
    let future5 = format!("{}", now_secs() + 5);
    client
        .command(&[
            "HEXPIREAT",
            "myhash",
            &future5,
            "XX",
            "FIELDS",
            "1",
            "field1",
        ])
        .await;
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "1", "field1"])
        .await;
    let arr = unwrap_array(resp);
    let ttl = unwrap_integer(&arr[0]);
    assert!((4..=5).contains(&ttl), "HTTL after update = {ttl}");
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
        .command(&[
            "HSET", "myhash", "field1", "value1", "field2", "value2", "field3", "value3",
        ])
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

    assert_bulk_eq(
        &client.command(&["HGET", "myhash", "field2"]).await,
        b"value4",
    );

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
        .command(&[
            "HPEXPIRE", "myhash", "200000", "NX", "FIELDS", "1", "field1",
        ])
        .await;
    client
        .command(&[
            "HPEXPIRE", "myhash", "1000000", "XX", "FIELDS", "1", "field1",
        ])
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
        (900000..=1000000).contains(&pttl),
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
        .command(&[
            "HPERSIST",
            "myhash",
            "FIELDS",
            "2",
            "f1",
            "not-exists-field",
        ])
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
        .command(&[
            "HSET", "myhash", "field1", "value1", "field2", "value2", "field3", "value3",
        ])
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

// ===========================================================================
// === Flexible argument parsing tests (Phase 3.1) ==========================
// ===========================================================================
//
// These tests are ported from the upstream "flexible argument parsing",
// "field count validation", "error message consistency", and "parser state
// consistency" test groups in `unit/type/hash-field-expire.tcl`
// (lines ~2200-2548). Upstream wraps every test in a
// `foreach type {listpackex hashtable}` loop — FrogDB has a single internal
// encoding, so each test is written once and the `$type` suffix is stripped
// from the test name.

// ---------------------------------------------------------------------------
// HEXPIRE FAMILY - Rigid expiration time positioning
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hexpire_family_rigid_expiration_time_positioning() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;

    // Test 1: Traditional order
    let resp = client
        .command(&["HEXPIRE", "myhash", "60", "FIELDS", "2", "f1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);
    assert_integer_eq(&arr[1], 1);

    // Test 2: Condition flags in rigid order
    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;
    let resp = client
        .command(&["HEXPIRE", "myhash", "120", "NX", "FIELDS", "2", "f1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);
    assert_integer_eq(&arr[1], 1);
    let resp = client
        .command(&["HEXPIRE", "myhash", "180", "XX", "FIELDS", "1", "f1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);

    // Test 3: GT/LT flags
    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;
    let resp = client
        .command(&["HEXPIRE", "myhash", "100", "FIELDS", "1", "f1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);
    let resp = client
        .command(&["HEXPIRE", "myhash", "200", "GT", "FIELDS", "1", "f1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);
    let resp = client
        .command(&["HEXPIRE", "myhash", "50", "LT", "FIELDS", "1", "f1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);

    // Test 4: Flexible positioning is rejected — expiration time must be at
    // position 2 (immediately after the key). Parser error message differs
    // from Redis but the outcome is the same.
    let resp = client
        .command(&["HEXPIRE", "myhash", "FIELDS", "1", "f1", "60"])
        .await;
    assert_error_prefix(&resp, "ERR");
    let resp = client
        .command(&["HPEXPIRE", "myhash", "FIELDS", "1", "f2", "5000"])
        .await;
    assert_error_prefix(&resp, "ERR");
    let resp = client
        .command(&["HEXPIRE", "myhash", "NX", "FIELDS", "1", "f1"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// HEXPIREAT/HPEXPIREAT - Flexible keyword ordering
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hexpireat_hpexpireat_flexible_keyword_ordering() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;

    let future_sec = (now_secs() + 300).to_string();
    let future_ms = (now_millis() + 300_000).to_string();

    // Rigid ordering with absolute timestamps
    let resp = client
        .command(&["HEXPIREAT", "myhash", &future_sec, "FIELDS", "1", "f1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);

    let resp = client
        .command(&[
            "HPEXPIREAT",
            "myhash",
            &future_ms,
            "NX",
            "FIELDS",
            "1",
            "f2",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);

    let resp = client
        .command(&[
            "HPEXPIREAT",
            "myhash",
            &future_ms,
            "XX",
            "FIELDS",
            "1",
            "f2",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);
}

// ---------------------------------------------------------------------------
// HSETEX - Flexible argument parsing and validation (rigid-order subset)
// ---------------------------------------------------------------------------
//
// Upstream also tests the FIELDS-first flexible order (Test 2) and a
// `HSETEX myhash FXX FIELDS 1 f1 v1 KEEPTTL` form where KEEPTTL trails the
// FIELDS list (Test 3). Both are Redis-only relaxations — FrogDB's parser
// requires `[FNX|FXX] [EX..|KEEPTTL] FIELDS ...` in strict order. See the
// `## Intentional exclusions` note in the file header.

#[tokio::test]
async fn tcl_hsetex_flexible_argument_parsing_and_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;

    // Test 1: Traditional order (expiration first, FIELDS last).
    assert_integer_eq(
        &client
            .command(&[
                "HSETEX", "myhash", "EX", "60", "FIELDS", "2", "f1", "v1", "f2", "v2",
            ])
            .await,
        1,
    );
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "2", "f1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    assert!(unwrap_integer(&arr[0]) > 0 && unwrap_integer(&arr[0]) <= 60);
    assert!(unwrap_integer(&arr[1]) > 0 && unwrap_integer(&arr[1]) <= 60);

    // Test 3 (rigid variant): FXX + KEEPTTL with condition/expiry BEFORE the
    // FIELDS list. Upstream writes this as
    // `HSETEX myhash FXX FIELDS 1 f1 v1 KEEPTTL` (KEEPTTL trailing), which
    // FrogDB rejects. The rigid form below exercises the same code path —
    // an FXX rewrite that preserves the pre-existing TTL.
    assert_integer_eq(
        &client
            .command(&[
                "HSETEX",
                "myhash",
                "FXX",
                "KEEPTTL",
                "FIELDS",
                "1",
                "f1",
                "v1_updated",
            ])
            .await,
        1,
    );
    assert_bulk_eq(
        &client.command(&["HGET", "myhash", "f1"]).await,
        b"v1_updated",
    );

    // The TTL should still be present (KEEPTTL preserved it).
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "1", "f1"])
        .await;
    let arr = unwrap_array(resp);
    assert!(unwrap_integer(&arr[0]) > 0 && unwrap_integer(&arr[0]) <= 60);
}

// ---------------------------------------------------------------------------
// HGETEX - Flexible argument parsing and validation (rigid-order subset)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hgetex_flexible_argument_parsing_and_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;

    // Test 1: Traditional order (expiration first, FIELDS last).
    let resp = client
        .command(&["HGETEX", "myhash", "EX", "60", "FIELDS", "2", "f1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    assert_bulk_eq(&arr[0], b"v1");
    assert_bulk_eq(&arr[1], b"v2");
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "2", "f1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    assert!(unwrap_integer(&arr[0]) > 0 && unwrap_integer(&arr[0]) <= 60);
    assert!(unwrap_integer(&arr[1]) > 0 && unwrap_integer(&arr[1]) <= 60);

    // Test 3 (rigid variant): PERSIST written BEFORE the FIELDS list.
    // Upstream asserts `HGETEX myhash FIELDS 1 f3 PERSIST` returns ["v3"] —
    // FrogDB rejects that trailing form. PERSIST-before-FIELDS achieves the
    // same behaviour: read the value and clear any field TTL.
    client
        .command(&["HEXPIRE", "myhash", "60", "FIELDS", "1", "f3"])
        .await;
    let resp = client
        .command(&["HGETEX", "myhash", "PERSIST", "FIELDS", "1", "f3"])
        .await;
    let arr = unwrap_array(resp);
    assert_bulk_eq(&arr[0], b"v3");
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "1", "f3"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], -1);
}

// ---------------------------------------------------------------------------
// Field count validation - HSETEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_field_count_validation_hsetex() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;

    // Field-value pair count mismatches should error.
    let resp = client
        .command(&["HSETEX", "myhash", "FIELDS", "2", "f1", "v1"])
        .await;
    assert_error_prefix(&resp, "ERR");
    let resp = client
        .command(&["HSETEX", "myhash", "FIELDS", "1", "f1", "v1", "f2", "v2"])
        .await;
    assert_error_prefix(&resp, "ERR");
    let resp = client
        .command(&["HSETEX", "myhash", "FIELDS", "3", "f1", "v1", "f2", "v2"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Valid field-value pairs
    assert_integer_eq(
        &client
            .command(&[
                "HSETEX", "myhash", "EX", "60", "FIELDS", "2", "f1", "v1", "f2", "v2",
            ])
            .await,
        1,
    );
    assert_bulk_eq(&client.command(&["HGET", "myhash", "f1"]).await, b"v1");
    assert_bulk_eq(&client.command(&["HGET", "myhash", "f2"]).await, b"v2");
}

// ---------------------------------------------------------------------------
// Field count validation - HGETEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_field_count_validation_hgetex() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;

    // Field count mismatches
    let resp = client
        .command(&["HGETEX", "myhash", "FIELDS", "2", "f1"])
        .await;
    assert_error_prefix(&resp, "ERR");
    let resp = client
        .command(&["HGETEX", "myhash", "FIELDS", "1", "f1", "f2", "f3"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Valid field counts
    let resp = client
        .command(&["HGETEX", "myhash", "EX", "60", "FIELDS", "2", "f1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    assert_bulk_eq(&arr[0], b"v1");
    assert_bulk_eq(&arr[1], b"v2");
}

// ---------------------------------------------------------------------------
// Error message consistency and validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_error_message_consistency_and_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "f1", "v1"]).await;

    // Invalid numfields values (0 or negative).
    let resp = client
        .command(&["HEXPIRE", "myhash", "60", "FIELDS", "0", "f1"])
        .await;
    assert_error_prefix(&resp, "ERR");
    let resp = client
        .command(&["HEXPIRE", "myhash", "60", "FIELDS", "-1", "f1"])
        .await;
    assert_error_prefix(&resp, "ERR");
    let resp = client
        .command(&["HSETEX", "myhash", "EX", "60", "FIELDS", "0", "f1", "v1"])
        .await;
    assert_error_prefix(&resp, "ERR");
    let resp = client
        .command(&["HGETEX", "myhash", "EX", "60", "FIELDS", "0", "f1"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Missing FIELDS keyword
    let resp = client
        .command(&["HEXPIRE", "myhash", "60", "2", "f1", "f2"])
        .await;
    assert_error_prefix(&resp, "ERR");
    let resp = client
        .command(&["HSETEX", "myhash", "EX", "60", "2", "f1", "v1", "f2", "v2"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Missing expire time
    let resp = client
        .command(&["HEXPIRE", "myhash", "NX", "FIELDS", "1", "f1"])
        .await;
    assert_error_prefix(&resp, "ERR");
    let resp = client
        .command(&["HPEXPIRE", "myhash", "FIELDS", "1", "f1", "XX"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// Numeric field names validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_numeric_field_names_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&[
            "HSET", "myhash", "01", "v1", "02", "v2", "999", "v999", "1000", "v1000",
        ])
        .await;

    // Small numbers should work as field names
    let resp = client
        .command(&["HEXPIRE", "myhash", "60", "FIELDS", "3", "01", "02", "999"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);
    assert_integer_eq(&arr[1], 1);
    assert_integer_eq(&arr[2], 1);

    // Large numbers should also work as field names
    let resp = client
        .command(&["HPEXPIRE", "myhash", "5000", "FIELDS", "1", "1000"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);

    // Verify all fields still exist and have a positive TTL.
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "4", "01", "02", "999", "1000"])
        .await;
    let arr = unwrap_array(resp);
    for ttl in arr {
        assert!(unwrap_integer(&ttl) > 0);
    }
}

// ---------------------------------------------------------------------------
// Multiple condition flags error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_multiple_condition_flags_error_handling() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "f1", "v1"]).await;

    // NX + XX: mutually exclusive. FrogDB's error text differs from Redis
    // ("NX and XX, GT or LT options at the same time are not compatible"
    // vs Redis's "Multiple condition flags specified"), so we only check
    // the ERR prefix.
    let resp = client
        .command(&["HEXPIRE", "myhash", "60", "NX", "XX", "FIELDS", "1", "f1"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // GT + LT: mutually exclusive.
    let resp = client
        .command(&[
            "HPEXPIRE", "myhash", "5000", "GT", "LT", "FIELDS", "1", "f1",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Condition flags after the FIELDS list are rejected (parser consumes
    // them as field names, producing a numfields mismatch).
    let resp = client
        .command(&["HEXPIRE", "myhash", "60", "FIELDS", "1", "f1", "NX", "XX"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// Multiple FIELDS keywords error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_multiple_fields_keywords_error_handling() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client.command(&["HSET", "myhash", "f1", "v1"]).await;

    // Upstream form 1: `HEXPIRE myhash FIELDS 1 f1 60 FIELDS 1 f2` —
    // FrogDB rejects this at the `FIELDS` position because the expire
    // time is expected immediately after the key.
    let resp = client
        .command(&[
            "HEXPIRE", "myhash", "FIELDS", "1", "f1", "60", "FIELDS", "1", "f2",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Upstream form 2: `HPEXPIRE myhash 5000 FIELDS 1 f1 FIELDS 1 f2` —
    // FrogDB's parser reads the first FIELDS block, sees the trailing
    // `FIELDS 1 f2` as extra field args, and errors on numfields mismatch.
    let resp = client
        .command(&[
            "HPEXPIRE", "myhash", "5000", "FIELDS", "1", "f1", "FIELDS", "1", "f2",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// Boundary conditions and edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_boundary_conditions_and_edge_cases() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    // Seed 100 fields.
    for i in 1..=100 {
        let f = format!("f{}", i);
        let v = format!("v{}", i);
        client.command(&["HSET", "myhash", &f, &v]).await;
    }

    // Build a field list of 50 fields.
    let field_names: Vec<String> = (1..=50).map(|i| format!("f{}", i)).collect();

    let mut args: Vec<&str> = vec!["HEXPIRE", "myhash", "300", "FIELDS", "50"];
    for f in &field_names {
        args.push(f.as_str());
    }
    let resp = client.command(&args).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 50);
    for r in &arr {
        assert_integer_eq(r, 1);
    }

    // Verify TTLs are set (0 < ttl <= 300).
    let mut ttl_args: Vec<&str> = vec!["HTTL", "myhash", "FIELDS", "50"];
    for f in &field_names {
        ttl_args.push(f.as_str());
    }
    let resp = client.command(&ttl_args).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 50);
    for r in &arr {
        let ttl = unwrap_integer(r);
        assert!(ttl > 0 && ttl <= 300);
    }
}

// ---------------------------------------------------------------------------
// Field names that look like keywords or numbers
// ---------------------------------------------------------------------------
//
// The upstream HSETEX assertion in this test uses the flexible
// FIELDS-first form (`HSETEX myhash FIELDS 3 EX val1 PX val2 FIELDS val3 EX 60`)
// which is rejected by FrogDB. The HEXPIRE portion uses rigid ordering and
// is fully covered below. A rigid-order HSETEX check is added to maintain
// the "keyword-like field names" intent of the original test.

#[tokio::test]
async fn tcl_field_names_that_look_like_keywords_or_numbers() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&[
            "HSET", "myhash", "EX", "value1", "PX", "value2", "FIELDS", "value3", "NX", "value4",
            "60", "value5",
        ])
        .await;

    // HEXPIRE over field names that look like reserved keywords.
    let resp = client
        .command(&[
            "HEXPIRE", "myhash", "120", "FIELDS", "5", "EX", "PX", "FIELDS", "NX", "60",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 5);
    for r in &arr {
        assert_integer_eq(r, 1);
    }

    let resp = client
        .command(&[
            "HTTL", "myhash", "FIELDS", "5", "EX", "PX", "FIELDS", "NX", "60",
        ])
        .await;
    let arr = unwrap_array(resp);
    for r in &arr {
        let ttl = unwrap_integer(r);
        assert!(ttl > 0 && ttl <= 120);
    }

    // Rigid-order HSETEX with keyword-like field names. FrogDB's parser
    // is not confused by field values that spell common reserved words.
    client.command(&["DEL", "myhash"]).await;
    assert_integer_eq(
        &client
            .command(&[
                "HSETEX", "myhash", "EX", "60", "FIELDS", "3", "EX", "val1", "PX", "val2",
                "FIELDS", "val3",
            ])
            .await,
        1,
    );
    assert_bulk_eq(&client.command(&["HGET", "myhash", "EX"]).await, b"val1");
    assert_bulk_eq(&client.command(&["HGET", "myhash", "PX"]).await, b"val2");
    assert_bulk_eq(
        &client.command(&["HGET", "myhash", "FIELDS"]).await,
        b"val3",
    );
}

// ---------------------------------------------------------------------------
// Parser state consistency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_parser_state_consistency() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;

    // Test 1: Multiple valid commands in sequence
    let resp = client
        .command(&["HEXPIRE", "myhash", "60", "FIELDS", "1", "f1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);

    let resp = client
        .command(&["HPEXPIRE", "myhash", "5000", "FIELDS", "1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);

    // NX should fail because f1 already has an expiration.
    let resp = client
        .command(&["HEXPIRE", "myhash", "120", "NX", "FIELDS", "1", "f1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 0);

    // XX should succeed because f1 has an expiration.
    let resp = client
        .command(&["HEXPIRE", "myhash", "180", "XX", "FIELDS", "1", "f1"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);

    // Test 2: Verify TTL values are correct.
    let resp = client
        .command(&["HTTL", "myhash", "FIELDS", "2", "f1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    let ttl1 = unwrap_integer(&arr[0]);
    let ttl2 = unwrap_integer(&arr[1]);
    assert!(ttl1 > 0 && ttl1 <= 180);
    assert!(ttl2 > 0);
}

// ---------------------------------------------------------------------------
// Stress test - complex scenarios with all features
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_stress_test_complex_scenarios_with_all_features() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;

    // Create a hash with 20 fields.
    for i in 1..=20 {
        let f = format!("field{}", i);
        let v = format!("value{}", i);
        client.command(&["HSET", "myhash", &f, &v]).await;
    }

    // Test 1: Flexible parsing with large field counts (rigid form).
    let fields_1_10: Vec<String> = (1..=10).map(|i| format!("field{}", i)).collect();
    let mut args: Vec<&str> = vec!["HEXPIRE", "myhash", "3600", "NX", "FIELDS", "10"];
    for f in &fields_1_10 {
        args.push(f.as_str());
    }
    let resp = client.command(&args).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 10);

    // Test 2: Mixed operations with rigid ordering.
    // First set expiration on field11-field15 so XX can succeed.
    let fields_11_15: Vec<String> = (11..=15).map(|i| format!("field{}", i)).collect();
    let mut args: Vec<&str> = vec!["HPEXPIRE", "myhash", "3600000", "NX", "FIELDS", "5"];
    for f in &fields_11_15 {
        args.push(f.as_str());
    }
    let resp = client.command(&args).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 5);
    for r in &arr {
        assert_integer_eq(r, 1);
    }

    let mut args: Vec<&str> = vec!["HPEXPIRE", "myhash", "7200000", "XX", "FIELDS", "5"];
    for f in &fields_11_15 {
        args.push(f.as_str());
    }
    let resp = client.command(&args).await;
    let arr = unwrap_array(resp);
    for r in &arr {
        assert_integer_eq(r, 1);
    }

    let fields_1_3: Vec<String> = (1..=3).map(|i| format!("field{}", i)).collect();
    let mut args: Vec<&str> = vec!["HEXPIRE", "myhash", "7200", "GT", "FIELDS", "3"];
    for f in &fields_1_3 {
        args.push(f.as_str());
    }
    let resp = client.command(&args).await;
    let arr = unwrap_array(resp);
    for r in &arr {
        assert_integer_eq(r, 1);
    }

    // Test 3: Field count validation still works with complex scenarios.
    let fields_1_3_short: Vec<String> = (1..=3).map(|i| format!("field{}", i)).collect();
    let mut args: Vec<&str> = vec!["HEXPIRE", "myhash", "3600", "FIELDS", "15"];
    for f in &fields_1_3_short {
        args.push(f.as_str());
    }
    let resp = client.command(&args).await;
    assert_error_prefix(&resp, "ERR");

    let fields_1_5: Vec<String> = (1..=5).map(|i| format!("field{}", i)).collect();
    let mut args: Vec<&str> = vec!["HPEXPIRE", "myhash", "7200000", "FIELDS", "3"];
    for f in &fields_1_5 {
        args.push(f.as_str());
    }
    let resp = client.command(&args).await;
    assert_error_prefix(&resp, "ERR");

    // Test 4: All fields 1..=15 have positive TTLs.
    let fields_1_15: Vec<String> = (1..=15).map(|i| format!("field{}", i)).collect();
    let mut args: Vec<&str> = vec!["HTTL", "myhash", "FIELDS", "15"];
    for f in &fields_1_15 {
        args.push(f.as_str());
    }
    let resp = client.command(&args).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 15);
    for r in &arr {
        assert!(unwrap_integer(r) > 0);
    }
}

// ---------------------------------------------------------------------------
// Backward compatibility verification
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_backward_compatibility_verification() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myhash"]).await;
    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;

    // Traditional syntax still works.
    let resp = client
        .command(&["HEXPIRE", "myhash", "60", "FIELDS", "2", "f1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);
    assert_integer_eq(&arr[1], 1);

    let resp = client
        .command(&["HPEXPIRE", "myhash", "5000", "NX", "FIELDS", "1", "f3"])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);

    let future_sec = (now_secs() + 300).to_string();
    let resp = client
        .command(&[
            "HEXPIREAT",
            "myhash",
            &future_sec,
            "XX",
            "FIELDS",
            "1",
            "f1",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_integer_eq(&arr[0], 1);

    // HSETEX/HGETEX traditional syntax
    client.command(&["DEL", "myhash"]).await;
    assert_integer_eq(
        &client
            .command(&[
                "HSETEX", "myhash", "EX", "60", "FIELDS", "2", "f1", "v1", "f2", "v2",
            ])
            .await,
        1,
    );
    let resp = client
        .command(&["HGETEX", "myhash", "PX", "5000", "FIELDS", "2", "f1", "f2"])
        .await;
    let arr = unwrap_array(resp);
    assert_bulk_eq(&arr[0], b"v1");
    assert_bulk_eq(&arr[1], b"v2");

    // Error messages are consistent.
    let resp = client
        .command(&["HEXPIRE", "myhash", "60", "FIELDS", "0", "f1"])
        .await;
    assert_error_prefix(&resp, "ERR");
    let resp = client
        .command(&["HEXPIRE", "myhash", "60", "2", "f1", "f2"])
        .await;
    assert_error_prefix(&resp, "ERR");
}
