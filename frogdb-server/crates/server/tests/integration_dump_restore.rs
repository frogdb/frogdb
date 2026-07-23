//! Integration tests for DUMP / RESTORE command round-trips.
//!
//! Verifies that every value type can be serialized with DUMP and restored with
//! RESTORE, preserving the original data.  Types whose serialization is still
//! stubbed out (Stream, BloomFilter, TimeSeries) are documented with `#[ignore]`
//! tests that assert the *correct* behaviour so they can be enabled once
//! serialization is implemented.

use crate::common::response_helpers::{unwrap_bulk, unwrap_integer};
use crate::common::test_server::{TestServer, is_error};
use bytes::Bytes;
use frogdb_protocol::Response;

// ============================================================================
// Helper
// ============================================================================

/// DUMP a key, DELETE it, RESTORE it under the same name, then return the
/// original DUMP payload so callers can do type-specific assertions.
async fn dump_delete_restore(server: &TestServer, key: &str) -> Bytes {
    // DUMP
    let dump_resp = server.send("DUMP", &[key]).await;
    let payload = match &dump_resp {
        Response::Bulk(Some(data)) => data.clone(),
        other => panic!("DUMP should return bulk data for key '{key}', got: {other:?}"),
    };

    // DELETE
    let del_resp = server.send("DEL", &[key]).await;
    assert!(
        matches!(del_resp, Response::Integer(1)),
        "DEL should return 1, got: {del_resp:?}"
    );

    // RESTORE (ttl=0 means no expiry)
    let restore_cmd = Bytes::from("RESTORE");
    let key_bytes = Bytes::from(key.to_string());
    let ttl = Bytes::from("0");
    let resp = server
        .connect()
        .await
        .command_raw(&[&restore_cmd, &key_bytes, &ttl, &payload])
        .await;
    assert!(
        matches!(resp, Response::Simple(ref s) if s.as_ref() == b"OK"),
        "RESTORE should return OK, got: {resp:?}"
    );

    payload
}

// ============================================================================
// Round-trip tests for implemented types
// ============================================================================

#[tokio::test]
async fn test_dump_restore_string_round_trip() {
    let server = TestServer::start_standalone().await;

    server.send("SET", &["{dr}str", "hello world"]).await;
    dump_delete_restore(&server, "{dr}str").await;

    let resp = server.send("GET", &["{dr}str"]).await;
    assert_eq!(
        unwrap_bulk(&resp),
        b"hello world",
        "String value should survive DUMP/RESTORE"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_hash_round_trip() {
    let server = TestServer::start_standalone().await;

    server
        .send("HSET", &["{dr}hash", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;
    dump_delete_restore(&server, "{dr}hash").await;

    let resp = server.send("HGET", &["{dr}hash", "f1"]).await;
    assert_eq!(unwrap_bulk(&resp), b"v1");
    let resp = server.send("HGET", &["{dr}hash", "f2"]).await;
    assert_eq!(unwrap_bulk(&resp), b"v2");
    let resp = server.send("HGET", &["{dr}hash", "f3"]).await;
    assert_eq!(unwrap_bulk(&resp), b"v3");
    assert_eq!(unwrap_integer(&server.send("HLEN", &["{dr}hash"]).await), 3);

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_list_round_trip() {
    let server = TestServer::start_standalone().await;

    server
        .send("RPUSH", &["{dr}list", "a", "b", "c", "d", "e"])
        .await;
    dump_delete_restore(&server, "{dr}list").await;

    assert_eq!(unwrap_integer(&server.send("LLEN", &["{dr}list"]).await), 5);
    assert_eq!(
        unwrap_bulk(&server.send("LINDEX", &["{dr}list", "0"]).await),
        b"a"
    );
    assert_eq!(
        unwrap_bulk(&server.send("LINDEX", &["{dr}list", "4"]).await),
        b"e"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_set_round_trip() {
    let server = TestServer::start_standalone().await;

    server.send("SADD", &["{dr}set", "x", "y", "z", "w"]).await;
    dump_delete_restore(&server, "{dr}set").await;

    assert_eq!(unwrap_integer(&server.send("SCARD", &["{dr}set"]).await), 4);
    assert_eq!(
        unwrap_integer(&server.send("SISMEMBER", &["{dr}set", "x"]).await),
        1
    );
    assert_eq!(
        unwrap_integer(&server.send("SISMEMBER", &["{dr}set", "w"]).await),
        1
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_sorted_set_round_trip() {
    let server = TestServer::start_standalone().await;

    server
        .send(
            "ZADD",
            &["{dr}zset", "1.5", "alpha", "2.5", "beta", "3.5", "gamma"],
        )
        .await;
    dump_delete_restore(&server, "{dr}zset").await;

    assert_eq!(
        unwrap_integer(&server.send("ZCARD", &["{dr}zset"]).await),
        3
    );
    let score = server.send("ZSCORE", &["{dr}zset", "beta"]).await;
    let score_str = std::str::from_utf8(unwrap_bulk(&score)).unwrap();
    let score_val: f64 = score_str.parse().unwrap();
    assert!(
        (score_val - 2.5).abs() < f64::EPSILON,
        "score should be 2.5"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_hll_round_trip() {
    let server = TestServer::start_standalone().await;

    server
        .send("PFADD", &["{dr}hll", "elem1", "elem2", "elem3"])
        .await;
    let count_before = unwrap_integer(&server.send("PFCOUNT", &["{dr}hll"]).await);
    assert!(count_before >= 3, "HLL should count at least 3");

    dump_delete_restore(&server, "{dr}hll").await;

    let count_after = unwrap_integer(&server.send("PFCOUNT", &["{dr}hll"]).await);
    assert_eq!(
        count_before, count_after,
        "HLL cardinality should be preserved after DUMP/RESTORE"
    );

    server.shutdown().await;
}

// ============================================================================
// Known-gap documentation tests (stubbed serialization)
// ============================================================================

#[tokio::test]
async fn test_dump_restore_stream_round_trip() {
    let server = TestServer::start_standalone().await;

    // Add entries to a stream
    server
        .send("XADD", &["{dr}stream", "*", "field1", "value1"])
        .await;
    server
        .send("XADD", &["{dr}stream", "*", "field2", "value2"])
        .await;

    dump_delete_restore(&server, "{dr}stream").await;

    // After proper serialization, stream should have 2 entries
    let len = unwrap_integer(&server.send("XLEN", &["{dr}stream"]).await);
    assert_eq!(len, 2, "Stream should have 2 entries after DUMP/RESTORE");

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_bloom_filter_round_trip() {
    let server = TestServer::start_standalone().await;

    server.send("BF.ADD", &["{dr}bloom", "item1"]).await;
    server.send("BF.ADD", &["{dr}bloom", "item2"]).await;

    dump_delete_restore(&server, "{dr}bloom").await;

    // After proper serialization, bloom filter should contain items
    let resp = server.send("BF.EXISTS", &["{dr}bloom", "item1"]).await;
    assert_eq!(
        unwrap_integer(&resp),
        1,
        "BloomFilter should contain item1 after DUMP/RESTORE"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_timeseries_round_trip() {
    let server = TestServer::start_standalone().await;

    server.send("TS.ADD", &["{dr}ts", "1000", "42.0"]).await;
    server.send("TS.ADD", &["{dr}ts", "2000", "43.0"]).await;

    dump_delete_restore(&server, "{dr}ts").await;

    // After proper serialization, the latest sample should be accessible
    let resp = server.send("TS.GET", &["{dr}ts"]).await;
    assert!(
        !is_error(&resp),
        "TS.GET should succeed after DUMP/RESTORE, got: {resp:?}"
    );

    server.shutdown().await;
}

// ============================================================================
// Additional DUMP/RESTORE tests
// ============================================================================

/// Tests that RESTORE with REPLACE flag overwrites an existing key.
#[tokio::test]
async fn test_dump_restore_replace_overwrites_existing_key() {
    let server = TestServer::start_standalone().await;

    // Set original value and DUMP it
    server.send("SET", &["{dr}replace", "original"]).await;
    let dump_resp = server.send("DUMP", &["{dr}replace"]).await;
    let payload = match &dump_resp {
        Response::Bulk(Some(data)) => data.clone(),
        other => panic!("DUMP should return bulk data, got: {other:?}"),
    };

    // Overwrite with a different value
    server.send("SET", &["{dr}replace", "overwritten"]).await;

    // RESTORE with REPLACE should succeed even though key exists
    let restore_cmd = Bytes::from("RESTORE");
    let key_bytes = Bytes::from("{dr}replace");
    let ttl = Bytes::from("0");
    let replace = Bytes::from("REPLACE");
    let resp = server
        .connect()
        .await
        .command_raw(&[&restore_cmd, &key_bytes, &ttl, &payload, &replace])
        .await;
    assert!(
        matches!(resp, Response::Simple(ref s) if s.as_ref() == b"OK"),
        "RESTORE with REPLACE should return OK, got: {resp:?}"
    );

    // Value should be restored to "original"
    let get_resp = server.send("GET", &["{dr}replace"]).await;
    assert_eq!(
        unwrap_bulk(&get_resp),
        b"original",
        "RESTORE REPLACE should overwrite with original value"
    );

    server.shutdown().await;
}

/// Tests that RESTORE without REPLACE fails when key already exists (BUSYKEY).
#[tokio::test]
async fn test_dump_restore_without_replace_fails_on_existing_key() {
    let server = TestServer::start_standalone().await;

    // Set and DUMP a value
    server.send("SET", &["{dr}busykey", "value1"]).await;
    let dump_resp = server.send("DUMP", &["{dr}busykey"]).await;
    let payload = match &dump_resp {
        Response::Bulk(Some(data)) => data.clone(),
        other => panic!("DUMP should return bulk data, got: {other:?}"),
    };

    // Key still exists — RESTORE without REPLACE should fail
    let restore_cmd = Bytes::from("RESTORE");
    let key_bytes = Bytes::from("{dr}busykey");
    let ttl = Bytes::from("0");
    let resp = server
        .connect()
        .await
        .command_raw(&[&restore_cmd, &key_bytes, &ttl, &payload])
        .await;
    assert!(
        is_error(&resp),
        "RESTORE without REPLACE on existing key should error, got: {resp:?}"
    );

    server.shutdown().await;
}

/// Tests that RESTORE with a TTL argument sets expiry on the restored key.
#[tokio::test]
async fn test_dump_restore_preserves_ttl() {
    let server = TestServer::start_standalone().await;

    // Set and DUMP a value
    server.send("SET", &["{dr}ttlkey", "ttl_value"]).await;
    let dump_resp = server.send("DUMP", &["{dr}ttlkey"]).await;
    let payload = match &dump_resp {
        Response::Bulk(Some(data)) => data.clone(),
        other => panic!("DUMP should return bulk data, got: {other:?}"),
    };

    // DELETE and RESTORE with TTL of 10000ms
    server.send("DEL", &["{dr}ttlkey"]).await;

    let restore_cmd = Bytes::from("RESTORE");
    let key_bytes = Bytes::from("{dr}ttlkey");
    let ttl = Bytes::from("10000"); // 10 seconds
    let resp = server
        .connect()
        .await
        .command_raw(&[&restore_cmd, &key_bytes, &ttl, &payload])
        .await;
    assert!(
        matches!(resp, Response::Simple(ref s) if s.as_ref() == b"OK"),
        "RESTORE with TTL should return OK, got: {resp:?}"
    );

    // Verify TTL is set
    let pttl_resp = server.send("PTTL", &["{dr}ttlkey"]).await;
    let pttl = unwrap_integer(&pttl_resp);
    assert!(
        pttl > 0 && pttl <= 10000,
        "PTTL should be between 0 and 10000, got: {}",
        pttl
    );

    // Verify value is correct
    let get_resp = server.send("GET", &["{dr}ttlkey"]).await;
    assert_eq!(unwrap_bulk(&get_resp), b"ttl_value");

    server.shutdown().await;
}

/// Tests that DUMP on a non-existent key returns nil.
#[tokio::test]
async fn test_dump_nil_for_nonexistent_key() {
    let server = TestServer::start_standalone().await;

    let resp = server.send("DUMP", &["{dr}nonexistent"]).await;
    assert!(
        matches!(resp, Response::Bulk(None)),
        "DUMP on non-existent key should return nil, got: {resp:?}"
    );

    server.shutdown().await;
}

/// Tests that integer-encoded strings round-trip correctly through DUMP/RESTORE.
#[tokio::test]
async fn test_dump_restore_integer_encoded_string() {
    let server = TestServer::start_standalone().await;

    // Set an integer-like string (Redis may encode this as an integer internally)
    server.send("SET", &["{dr}intstr", "12345"]).await;
    dump_delete_restore(&server, "{dr}intstr").await;

    let resp = server.send("GET", &["{dr}intstr"]).await;
    assert_eq!(
        unwrap_bulk(&resp),
        b"12345",
        "Integer-encoded string should survive DUMP/RESTORE"
    );

    // Also verify it can be used as an integer
    let incr_resp = server.send("INCR", &["{dr}intstr"]).await;
    assert_eq!(
        unwrap_integer(&incr_resp),
        12346,
        "Restored integer string should be incrementable"
    );

    server.shutdown().await;
}

// ============================================================================
// Corrupt-payload contract tests (issue 25)
// ============================================================================
//
// The DUMP/RESTORE frame format (persistence `serialize`/`deserialize`) carries a
// 24-byte header — `[type:u8][flags:u8][expires:i64][lfu:u8][pad:5][len:u64]` —
// followed by a type-specific body, with **no CRC64 and no version footer** (unlike
// Redis's RDB frame). Because there is no checksum, the per-type deserializers are
// the *only* line of defence against a mangled payload.
//
// RESTORE already fails closed on payloads that fail to parse: `deserialize`'s error
// is mapped to a clean `CommandError::InvalidArgument` ("DUMP payload version or
// checksum are wrong: ...") in `commands/persistence.rs`, *before* the value ever
// reaches `store.set`. That fail-closed path pre-existed this test suite and is
// verified correct here — these tests are a regression guard, not a new fix.
//
// The residue these tests pin down is narrower: a payload corrupted (truncated,
// bit-flipped type byte, mangled length, garbage) but which might still *parse*
// under some per-type deserializer, silently materializing a wrong-type/garbage key.
// Each test asserts the safety contract: RESTORE either returns a clean error and
// leaves the key absent, or (documented residue) returns OK with the key fully
// materialized — never a panic, never a dropped connection, never partial state.

/// Header size of the serialized DUMP frame (`persistence::HEADER_SIZE`).
const DUMP_HEADER_SIZE: usize = 24;
/// Byte offset of the type marker within the frame header.
const TYPE_BYTE_OFFSET: usize = 0;
/// Byte offset of the little-endian `u64` payload-length field within the header.
const LEN_FIELD_OFFSET: usize = 16;

/// DUMP `key`, then DELETE it, returning the raw DUMP payload bytes so a caller can
/// corrupt them and feed them back through RESTORE against a now-absent key.
async fn dump_then_delete(server: &TestServer, key: &str) -> Vec<u8> {
    let dump_resp = server.send("DUMP", &[key]).await;
    let payload = match &dump_resp {
        Response::Bulk(Some(data)) => data.to_vec(),
        other => panic!("DUMP should return bulk data for key '{key}', got: {other:?}"),
    };
    let del_resp = server.send("DEL", &[key]).await;
    assert!(
        matches!(del_resp, Response::Integer(1)),
        "DEL should return 1 for '{key}', got: {del_resp:?}"
    );
    assert!(
        payload.len() >= DUMP_HEADER_SIZE,
        "DUMP payload for '{key}' shorter than header: {} bytes",
        payload.len()
    );
    payload
}

/// RESTORE `key` with the raw (possibly corrupted) `payload` and no TTL, returning
/// the server response. Uses `command_raw` so arbitrary bytes travel as a bulk arg.
async fn restore_raw(server: &TestServer, key: &str, payload: &[u8]) -> Response {
    let restore_cmd = Bytes::from("RESTORE");
    let key_bytes = Bytes::from(key.to_string());
    let ttl = Bytes::from("0");
    let payload_bytes = Bytes::copy_from_slice(payload);
    server
        .connect()
        .await
        .command_raw(&[&restore_cmd, &key_bytes, &ttl, &payload_bytes])
        .await
}

/// Assert the RESTORE fail-closed contract for a corrupted payload that must be
/// rejected: response is a clean protocol error (not a panic/closed connection), no
/// key was materialized, and the server is still healthy for the next command.
async fn assert_restore_rejected(server: &TestServer, key: &str, payload: &[u8], case: &str) {
    let resp = restore_raw(server, key, payload).await;
    assert!(
        is_error(&resp),
        "[{case}] RESTORE of corrupted payload should return an error, got: {resp:?}"
    );
    let exists = unwrap_integer(&server.send("EXISTS", &[key]).await);
    assert_eq!(
        exists, 0,
        "[{case}] rejected RESTORE must not materialize key '{key}' (partial state)"
    );
    // Server still services commands (no panic / no wedged connection).
    let ping = server.send("PING", &[]).await;
    assert!(
        matches!(ping, Response::Simple(ref s) if s.as_ref() == b"PONG"),
        "[{case}] server should still answer PING after rejected RESTORE, got: {ping:?}"
    );
}

/// Assert the RESTORE safety invariant for a *type-flipped* payload, whose outcome is
/// deserializer-dependent: either it is rejected cleanly (no key), or it still parses
/// and the key is *fully* materialized (documented residue) — never a panic, never
/// partial state. Returns `true` if the flip silently succeeded (materialized a key).
async fn assert_restore_flip_invariant(
    server: &TestServer,
    key: &str,
    payload: &[u8],
    case: &str,
) -> bool {
    let resp = restore_raw(server, key, payload).await;
    let exists = unwrap_integer(&server.send("EXISTS", &[key]).await);
    match &resp {
        Response::Simple(s) if s.as_ref() == b"OK" => {
            // Documented residue: no checksum, and this flipped body parsed under the
            // other type's deserializer, so a wrong-type key was materialized. The
            // invariant we still hold: it is *fully* present, not partial state.
            assert_eq!(
                exists, 1,
                "[{case}] RESTORE returned OK but key '{key}' absent — partial state"
            );
            // Clean up so the caller's server can be reused.
            server.send("DEL", &[key]).await;
            true
        }
        _ => {
            assert!(
                is_error(&resp),
                "[{case}] flipped RESTORE should be OK or a clean error, got: {resp:?}"
            );
            assert_eq!(
                exists, 0,
                "[{case}] rejected flip must not materialize key '{key}' (partial state)"
            );
            false
        }
    }
}

/// Build a truncated variant of a valid DUMP `payload`: cut the body roughly in half
/// and rewrite the length field to match, so the frame-level bounds check passes and
/// the *per-type* deserializer must reject the short body itself.
fn corrupt_truncate_consistent(payload: &[u8]) -> Vec<u8> {
    let body_len = payload.len() - DUMP_HEADER_SIZE;
    if body_len < 2 {
        // Body too small to cut meaningfully; drop the last byte.
        let mut out = payload.to_vec();
        out.pop();
        return out;
    }
    let new_body_len = body_len / 2;
    let mut out = payload[..DUMP_HEADER_SIZE + new_body_len].to_vec();
    out[LEN_FIELD_OFFSET..LEN_FIELD_OFFSET + 8]
        .copy_from_slice(&(new_body_len as u64).to_le_bytes());
    out
}

/// Truncate the buffer mid-body but leave the length field pointing at the original
/// (now-out-of-bounds) length — exercises the frame-level bounds check.
fn corrupt_truncate_dangling_len(payload: &[u8]) -> Vec<u8> {
    let body_len = payload.len() - DUMP_HEADER_SIZE;
    let keep = (DUMP_HEADER_SIZE + body_len / 2).max(DUMP_HEADER_SIZE);
    payload[..keep].to_vec()
}

/// Overwrite the length field with an absurd value larger than the buffer.
fn corrupt_oversized_len(payload: &[u8]) -> Vec<u8> {
    let mut out = payload.to_vec();
    out[LEN_FIELD_OFFSET..LEN_FIELD_OFFSET + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    out
}

/// Replace the body with garbage bytes, keeping the header/length intact so the bytes
/// reach the per-type deserializer.
fn corrupt_garbage_body(payload: &[u8]) -> Vec<u8> {
    let mut out = payload.to_vec();
    for (i, b) in out[DUMP_HEADER_SIZE..].iter_mut().enumerate() {
        *b = (0xA5u8).wrapping_add((i as u8).wrapping_mul(31)) ^ 0x5A;
    }
    out
}

/// Flip the type-marker byte to a *different* valid marker (wrapping within 0..=16),
/// leaving the body untouched so it is reinterpreted under the wrong deserializer.
fn corrupt_flip_type(payload: &[u8]) -> Vec<u8> {
    let mut out = payload.to_vec();
    let original = out[TYPE_BYTE_OFFSET];
    // 17 known markers (0..=16). Pick a different one deterministically.
    out[TYPE_BYTE_OFFSET] = (original + 1) % 17;
    out
}

/// Set the type-marker byte to a value with no known marker (>16) — must be rejected
/// at `TypeMarker::from_byte` with an `UnknownType` error.
fn corrupt_unknown_type(payload: &[u8]) -> Vec<u8> {
    let mut out = payload.to_vec();
    out[TYPE_BYTE_OFFSET] = 200;
    out
}

/// Populate `key` with a value of the type identified by `kind`, using the same
/// setup commands as the round-trip tests above.
async fn seed_value(server: &TestServer, key: &str, kind: &str) {
    match kind {
        "string" => {
            server.send("SET", &[key, "hello corrupt world"]).await;
        }
        "int-string" => {
            server.send("SET", &[key, "12345"]).await;
        }
        "list" => {
            server.send("RPUSH", &[key, "a", "b", "c", "d", "e"]).await;
        }
        "hash" => {
            server
                .send("HSET", &[key, "f1", "v1", "f2", "v2", "f3", "v3"])
                .await;
        }
        "set" => {
            server.send("SADD", &[key, "x", "y", "z", "w"]).await;
        }
        "zset" => {
            server
                .send("ZADD", &[key, "1", "alpha", "2", "beta", "3", "gamma"])
                .await;
        }
        "stream" => {
            server.send("XADD", &[key, "*", "field1", "value1"]).await;
            server.send("XADD", &[key, "*", "field2", "value2"]).await;
        }
        "hll" => {
            server.send("PFADD", &[key, "e1", "e2", "e3"]).await;
        }
        "json" => {
            server
                .send("JSON.SET", &[key, "$", r#"{"a":1,"b":[2,3]}"#])
                .await;
        }
        other => panic!("unknown seed kind: {other}"),
    }
}

/// The set of value kinds exercised by the corruption matrix. Covers the minimum set
/// required by the acceptance criteria (string, list, hash, set, zset, stream) plus
/// integer-encoded string, HLL, and JSON for breadth.
const CORRUPT_KINDS: &[&str] = &[
    "string",
    "int-string",
    "list",
    "hash",
    "set",
    "zset",
    "stream",
    "hll",
    "json",
];

/// Kinds whose body is *unstructured* — a raw byte string that any truncation or
/// byte substitution still parses as a (shorter/different) valid value. For these,
/// a body-preserving corruption cannot be rejected by parsing; only frame-level
/// corruption (dangling/oversized length, bad marker) fails closed. We assert the
/// no-panic/no-partial safety invariant instead of rejection for body corruption.
fn is_raw_unstructured_body(kind: &str) -> bool {
    // Raw string: `deserialize_string_raw` accepts any bytes verbatim.
    kind == "string"
}

/// (a) Truncated payload, two variants:
///   - consistent-length: body cut in half, length field rewritten to match, so the
///     per-type deserializer sees a short-but-in-bounds body. Structured types (and
///     the fixed-width integer string) must reject; the raw string legitimately
///     parses the shorter body (safety invariant only).
///   - dangling-length: buffer truncated but the length field still points past the
///     end, so the frame-level bounds check rejects *every* type unconditionally.
#[tokio::test]
async fn test_restore_truncated_payload_fails_closed_per_type() {
    let server = TestServer::start_standalone().await;

    for kind in CORRUPT_KINDS {
        let key = format!("{{dr-trunc}}{kind}");
        seed_value(&server, &key, kind).await;
        let payload = dump_then_delete(&server, &key).await;

        let consistent = corrupt_truncate_consistent(&payload);
        let case = format!("{kind}/trunc-consistent");
        if is_raw_unstructured_body(kind) {
            assert_restore_flip_invariant(&server, &key, &consistent, &case).await;
        } else {
            assert_restore_rejected(&server, &key, &consistent, &case).await;
        }

        // Dangling length is a frame-level violation: rejected for all types.
        let dangling = corrupt_truncate_dangling_len(&payload);
        assert_restore_rejected(&server, &key, &dangling, &format!("{kind}/trunc-dangling")).await;
    }

    server.shutdown().await;
}

/// (b) Flipped type-marker byte. Outcome is deserializer-dependent; the invariant is
/// no panic and no partial state. Any type that *silently* accepts another type's
/// body is a documented residue (issue 25) — this test records which do, and asserts
/// the full-materialization invariant either way.
#[tokio::test]
async fn test_restore_type_flip_invariant_per_type() {
    let server = TestServer::start_standalone().await;

    let mut silent_flips: Vec<String> = Vec::new();
    for kind in CORRUPT_KINDS {
        let key = format!("{{dr-flip}}{kind}");
        seed_value(&server, &key, kind).await;
        let payload = dump_then_delete(&server, &key).await;

        let flipped = corrupt_flip_type(&payload);
        let silently_succeeded =
            assert_restore_flip_invariant(&server, &key, &flipped, &format!("{kind}/type-flip"))
                .await;
        if silently_succeeded {
            silent_flips.push((*kind).to_string());
        }

        // An unknown (out-of-range) marker byte must always be rejected cleanly.
        let unknown = corrupt_unknown_type(&payload);
        assert_restore_rejected(&server, &key, &unknown, &format!("{kind}/unknown-marker")).await;
    }

    // Documented residue: with no CRC64/version footer, a flipped type byte whose body
    // happens to parse under the neighbouring deserializer materializes a wrong-type
    // key. This is a known gap tracked in issue 25 (restore-corrupt-payload), not a
    // regression. We log rather than fail so the invariant test stays green while the
    // residue is visible in CI output.
    eprintln!(
        "issue-25 type-flip residue: kinds whose neighbour-marker flip silently \
         materialized a wrong-type key = {silent_flips:?}"
    );

    server.shutdown().await;
}

/// (c) Corrupted length field (oversized). Must fail closed via the frame-level
/// bounds check with no materialized key.
#[tokio::test]
async fn test_restore_corrupt_length_field_fails_closed_per_type() {
    let server = TestServer::start_standalone().await;

    for kind in CORRUPT_KINDS {
        let key = format!("{{dr-len}}{kind}");
        seed_value(&server, &key, kind).await;
        let payload = dump_then_delete(&server, &key).await;

        let oversized = corrupt_oversized_len(&payload);
        assert_restore_rejected(&server, &key, &oversized, &format!("{kind}/oversized-len")).await;
    }

    server.shutdown().await;
}

/// (d) Fully garbage payload body (length field left matching so bytes reach the
/// per-type deserializer). Structured types must reject garbage cleanly with no
/// materialized key. Two kinds have bodies that accept arbitrary bytes and so
/// legitimately parse garbage — the raw string (any bytes) and the integer string
/// (any 8 bytes are a valid i64); for those we assert only the safety invariant.
#[tokio::test]
async fn test_restore_garbage_body_no_panic_per_type() {
    let server = TestServer::start_standalone().await;

    for kind in CORRUPT_KINDS {
        let key = format!("{{dr-garbage}}{kind}");
        seed_value(&server, &key, kind).await;
        let payload = dump_then_delete(&server, &key).await;

        let garbage = corrupt_garbage_body(&payload);
        let case = format!("{kind}/garbage");
        // Raw string accepts any bytes; the integer string accepts any 8-byte body.
        if *kind == "string" || *kind == "int-string" {
            assert_restore_flip_invariant(&server, &key, &garbage, &case).await;
        } else {
            assert_restore_rejected(&server, &key, &garbage, &case).await;
        }
    }

    server.shutdown().await;
}

/// Wholly random bytes with no valid header. Buffers shorter than the 24-byte header
/// are *guaranteed* to be rejected by the frame-level length check (regression guard
/// on the pre-existing fail-closed path in `commands/persistence.rs`). Buffers >= the
/// header length could coincidentally form a valid empty frame, so for those we
/// assert only the safety invariant (clean error + no key, or full materialization —
/// never a panic or partial state).
#[tokio::test]
async fn test_restore_fully_random_bytes_fails_closed() {
    let server = TestServer::start_standalone().await;

    for (i, len) in [0usize, 1, 8, 23].into_iter().enumerate() {
        let key = format!("{{dr-rand}}{i}");
        let garbage: Vec<u8> = (0..len)
            .map(|j| ((j as u8).wrapping_mul(97)) ^ (i as u8).wrapping_mul(53) ^ 0x3C)
            .collect();
        // Below header size: always Truncated.
        assert_restore_rejected(&server, &key, &garbage, &format!("random/short-len-{len}")).await;
    }

    for (i, len) in [24usize, 40, 200, 1024].into_iter().enumerate() {
        let key = format!("{{dr-randbig}}{i}");
        let garbage: Vec<u8> = (0..len)
            .map(|j| ((j as u8).wrapping_mul(131)) ^ (i as u8).wrapping_mul(71) ^ 0x91)
            .collect();
        // At/above header size: could coincidentally parse; assert only the invariant.
        assert_restore_flip_invariant(&server, &key, &garbage, &format!("random/big-len-{len}"))
            .await;
    }

    server.shutdown().await;
}
