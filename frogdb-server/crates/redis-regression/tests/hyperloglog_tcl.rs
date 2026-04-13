//! Rust port of Redis 8.6.0 `unit/hyperloglog.tcl` test suite.
//!
//! Excludes: `needs:pfdebug` tests (PFSELFTEST, PFDEBUG GETREG, PFDEBUG encoding,
//! PFDEBUG todense, PFDEBUG simd), `large-memory` tests, fuzzing tests (require
//! `randomInt`/`randstring` helpers), corrupted-HLL tests that rely on internal binary
//! layout manipulation.
//!
//! ## Intentional exclusions
//!
//! HyperLogLog internal sparse/dense encoding (FrogDB stores HLL as a single
//! type with no sparse/dense encoding distinction):
//! - `HyperLogLog self test passes` — redis-specific — Redis-internal HLL self-test
//! - `HyperLogLogs are promote from sparse to dense` — intentional-incompatibility:encoding — internal-encoding (HLL)
//! - `Change hll-sparse-max-bytes` — intentional-incompatibility:encoding — internal-encoding (HLL)
//! - `Hyperloglog promote to dense well in different hll-sparse-max-bytes` — intentional-incompatibility:encoding — internal-encoding (HLL)
//! - `HyperLogLog sparse encoding stress test` — tested-elsewhere — internal-encoding (HLL) + stress
//! - `PFMERGE results with simd` — redis-specific — Redis-internal SIMD path
//! - `PFDEBUG GETREG returns the HyperLogLog raw registers` — intentional-incompatibility:debug — needs:debug (PFDEBUG)
//!
//! Corrupted-HLL binary-layout tests (rely on internal HLL byte layout):
//! - `Corrupted sparse HyperLogLogs doesn't cause overflow and out-of-bounds with XZERO opcode` — intentional-incompatibility:encoding — internal-encoding (HLL)
//! - `Corrupted sparse HyperLogLogs doesn't cause overflow and out-of-bounds with ZERO opcode` — intentional-incompatibility:encoding — internal-encoding (HLL)
//! - `Fuzzing dense/sparse encoding: Redis should always detect errors` — tested-elsewhere — internal-encoding (HLL) + fuzzing
//!
//! Large-memory:
//! - `PFADD with 2GB entry should not crash server due to overflow in MurmurHash64A` — tested-elsewhere — large-memory

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// PFADD basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_pfadd_without_arguments_creates_hll_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // PFADD with no elements creates an empty HLL and returns 1 (register altered).
    assert_integer_eq(&client.command(&["PFADD", "hll"]).await, 1);
    assert_integer_eq(&client.command(&["EXISTS", "hll"]).await, 1);
}

#[tokio::test]
async fn tcl_approximated_cardinality_after_creation_is_zero() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["PFADD", "hll"]).await;
    assert_integer_eq(&client.command(&["PFCOUNT", "hll"]).await, 0);
}

#[tokio::test]
async fn tcl_pfadd_returns_1_when_at_least_1_reg_was_modified() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_integer_eq(&client.command(&["PFADD", "hll", "a", "b", "c"]).await, 1);
}

#[tokio::test]
async fn tcl_pfadd_returns_0_when_no_reg_was_modified() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["PFADD", "hll", "a", "b", "c"]).await;
    assert_integer_eq(&client.command(&["PFADD", "hll", "a", "b", "c"]).await, 0);
}

#[tokio::test]
async fn tcl_pfadd_works_with_empty_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Regression test: PFADD with empty string element should not error.
    let resp = client.command(&["PFADD", "hll", ""]).await;
    // Should return 1 (modified) or at minimum not error.
    assert!(
        matches!(resp, Response::Integer(_)),
        "expected integer response, got {resp:?}"
    );
}

// ---------------------------------------------------------------------------
// PFCOUNT basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_pfcount_returns_approximated_cardinality_of_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "hll"]).await;
    client
        .command(&["PFADD", "hll", "1", "2", "3", "4", "5"])
        .await;
    assert_integer_eq(&client.command(&["PFCOUNT", "hll"]).await, 5);

    // Add more elements, including a duplicate (8), to test cache invalidation.
    client
        .command(&["PFADD", "hll", "6", "7", "8", "8", "9", "10"])
        .await;
    assert_integer_eq(&client.command(&["PFCOUNT", "hll"]).await, 10);
}

// ---------------------------------------------------------------------------
// PFADD, PFCOUNT, PFMERGE type checking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_pfadd_pfcount_pfmerge_type_checking_works() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{hll}foo", "bar"]).await;

    assert_error_prefix(
        &client.command(&["PFADD", "{hll}foo", "1"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(&client.command(&["PFCOUNT", "{hll}foo"]).await, "WRONGTYPE");
    assert_error_prefix(
        &client.command(&["PFMERGE", "{hll}bar2", "{hll}foo"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client.command(&["PFMERGE", "{hll}foo", "{hll}bar2"]).await,
        "WRONGTYPE",
    );
}

// ---------------------------------------------------------------------------
// PFMERGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_pfmerge_results_on_cardinality_of_union_of_sets() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["DEL", "{hll}0", "{hll}1", "{hll}2", "{hll}3"])
        .await;
    client.command(&["PFADD", "{hll}1", "a", "b", "c"]).await;
    client.command(&["PFADD", "{hll}2", "b", "c", "d"]).await;
    client.command(&["PFADD", "{hll}3", "c", "d", "e"]).await;
    assert_ok(
        &client
            .command(&["PFMERGE", "{hll}0", "{hll}1", "{hll}2", "{hll}3"])
            .await,
    );
    assert_integer_eq(&client.command(&["PFCOUNT", "{hll}0"]).await, 5);
}

#[tokio::test]
async fn tcl_pfmerge_on_missing_source_keys_creates_empty_destkey() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["DEL", "{hll}src", "{hll}src2", "{hll}dst", "{hll}dst2"])
        .await;

    assert_ok(&client.command(&["PFMERGE", "{hll}dst", "{hll}src"]).await);
    assert_integer_eq(&client.command(&["EXISTS", "{hll}dst"]).await, 1);
    assert_integer_eq(&client.command(&["PFCOUNT", "{hll}dst"]).await, 0);

    assert_ok(
        &client
            .command(&["PFMERGE", "{hll}dst2", "{hll}src", "{hll}src2"])
            .await,
    );
    assert_integer_eq(&client.command(&["EXISTS", "{hll}dst2"]).await, 1);
    assert_integer_eq(&client.command(&["PFCOUNT", "{hll}dst2"]).await, 0);
}

#[tokio::test]
async fn tcl_pfmerge_with_one_empty_input_key_creates_empty_destkey() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "destkey"]).await;
    assert_ok(&client.command(&["PFMERGE", "destkey"]).await);
    assert_integer_eq(&client.command(&["EXISTS", "destkey"]).await, 1);
    assert_integer_eq(&client.command(&["PFCOUNT", "destkey"]).await, 0);
}

#[tokio::test]
async fn tcl_pfmerge_with_one_non_empty_input_key_dest_is_source() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "destkey"]).await;
    assert_integer_eq(
        &client.command(&["PFADD", "destkey", "a", "b", "c"]).await,
        1,
    );
    assert_ok(&client.command(&["PFMERGE", "destkey"]).await);
    assert_integer_eq(&client.command(&["EXISTS", "destkey"]).await, 1);
    assert_integer_eq(&client.command(&["PFCOUNT", "destkey"]).await, 3);
}

// ---------------------------------------------------------------------------
// PFCOUNT with multiple keys (union cardinality)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_pfcount_multiple_keys_merge_returns_cardinality_of_union_1() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "{hll}1", "{hll}2", "{hll}3"]).await;

    // Add distinct prefixed elements to three separate HLLs in batches.
    for x in 1..10000 {
        client
            .command(&["PFADD", "{hll}1", &format!("foo-{x}")])
            .await;
        client
            .command(&["PFADD", "{hll}2", &format!("bar-{x}")])
            .await;
        client
            .command(&["PFADD", "{hll}3", &format!("zap-{x}")])
            .await;

        // Only check periodically to keep runtime reasonable.
        if x % 1000 == 0 {
            let card = unwrap_integer(
                &client
                    .command(&["PFCOUNT", "{hll}1", "{hll}2", "{hll}3"])
                    .await,
            );
            let realcard = (x * 3) as i64;
            let err = (card - realcard).unsigned_abs();
            // Within 5% error.
            assert!(
                err < ((card as f64 / 100.0) * 5.0) as u64,
                "cardinality {card} too far from expected {realcard} (err={err}) at x={x}"
            );
        }
    }
}

#[tokio::test]
async fn tcl_pfcount_multiple_keys_merge_returns_cardinality_of_union_2() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "{hll}1", "{hll}2", "{hll}3"]).await;

    use std::collections::HashSet;
    let mut elements = HashSet::new();

    // Use a simple LCG PRNG to avoid needing the rand crate.
    let mut rng_state: u64 = 12345;
    for _x in 1..10000 {
        for j in 1..=3 {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let rint = ((rng_state >> 33) as u32) % 20000;
            let key = format!("{{hll}}{j}");
            client.command(&["PFADD", &key, &rint.to_string()]).await;
            elements.insert(rint);
        }
    }
    let realcard = elements.len() as i64;
    let card = unwrap_integer(
        &client
            .command(&["PFCOUNT", "{hll}1", "{hll}2", "{hll}3"])
            .await,
    );
    let err = (card - realcard).unsigned_abs();
    // Within 5% error.
    assert!(
        err < ((card as f64 / 100.0) * 5.0) as u64,
        "cardinality {card} too far from expected {realcard} (err={err})"
    );
}

// ---------------------------------------------------------------------------
// PFADD / PFCOUNT cache invalidation
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "relies on GETRANGE of internal HLL binary representation which may differ in FrogDB"]
async fn tcl_pfadd_pfcount_cache_invalidation_works() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "hll"]).await;
    client.command(&["PFADD", "hll", "a", "b", "c"]).await;
    client.command(&["PFCOUNT", "hll"]).await;

    // In Redis, byte 15 of the HLL is the cache invalidation flag.
    // After PFCOUNT caches, the flag at offset 15 should be 0x00.
    assert_bulk_eq(
        &client.command(&["GETRANGE", "hll", "15", "15"]).await,
        b"\x00",
    );
    // Re-adding same elements should not invalidate.
    client.command(&["PFADD", "hll", "a", "b", "c"]).await;
    assert_bulk_eq(
        &client.command(&["GETRANGE", "hll", "15", "15"]).await,
        b"\x00",
    );
    // Adding new elements should invalidate the cache.
    client.command(&["PFADD", "hll", "1", "2", "3"]).await;
    assert_bulk_eq(
        &client.command(&["GETRANGE", "hll", "15", "15"]).await,
        b"\x80",
    );
}

// ---------------------------------------------------------------------------
// Corrupted HLL detection (subset that does not need PFDEBUG)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "relies on APPEND to corrupt internal HLL binary representation"]
async fn tcl_corrupted_sparse_hll_detected_additional_at_tail() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "hll"]).await;
    client.command(&["PFADD", "hll", "a", "b", "c"]).await;
    client.command(&["APPEND", "hll", "hello"]).await;
    assert_error_prefix(&client.command(&["PFCOUNT", "hll"]).await, "INVALIDOBJ");
}

#[tokio::test]
#[ignore = "relies on SETRANGE to corrupt internal HLL binary representation"]
async fn tcl_corrupted_sparse_hll_detected_broken_magic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "hll"]).await;
    client.command(&["PFADD", "hll", "a", "b", "c"]).await;
    client.command(&["SETRANGE", "hll", "0", "0123"]).await;
    assert_error_prefix(&client.command(&["PFCOUNT", "hll"]).await, "WRONGTYPE");
}

#[tokio::test]
#[ignore = "relies on SETRANGE to corrupt internal HLL binary representation"]
async fn tcl_corrupted_sparse_hll_detected_invalid_encoding() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "hll"]).await;
    client.command(&["PFADD", "hll", "a", "b", "c"]).await;
    client.command(&["SETRANGE", "hll", "4", "x"]).await;
    assert_error_prefix(&client.command(&["PFCOUNT", "hll"]).await, "WRONGTYPE");
}

#[tokio::test]
#[ignore = "relies on SETRANGE to corrupt internal HLL binary representation"]
async fn tcl_corrupted_dense_hll_detected_wrong_length() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "hll"]).await;
    client.command(&["PFADD", "hll", "a", "b", "c"]).await;
    client.command(&["SETRANGE", "hll", "4", "\x00"]).await;
    assert_error_prefix(&client.command(&["PFCOUNT", "hll"]).await, "WRONGTYPE");
}

// ---------------------------------------------------------------------------
// Large-element cardinality test (sparse to dense promotion)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hll_large_element_cardinality_within_tolerance() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "hll"]).await;

    // Add 100k unique random elements in batches of 100.
    let mut n: u64 = 0;
    while n < 100_000 {
        let mut args = vec!["PFADD", "hll"];
        let elems: Vec<String> = (0..100).map(|i| format!("elem-{}", n + i)).collect();
        let elem_refs: Vec<&str> = elems.iter().map(|s| s.as_str()).collect();
        args.extend_from_slice(&elem_refs);
        client.command(&args).await;
        n += 100;

        // Check periodically.
        if n.is_multiple_of(10000) {
            let card = unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await);
            let err = (card - n as i64).unsigned_abs();
            // Within 5% error.
            assert!(
                err < ((card as f64 / 100.0) * 5.0) as u64,
                "at n={n}, card={card}, err={err} exceeds 5%"
            );
        }
    }
}
