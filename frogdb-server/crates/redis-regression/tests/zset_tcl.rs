//! Rust port of Redis 8.6.0 `unit/type/zset.tcl` test suite.
//!
//! Excludes: encoding-specific loops (listpack/skiplist), readraw, RESP3,
//! blocking (BZPOPMIN/MAX/BZMPOP), fuzzing/stress, chi-square distribution,
//! `needs:repl`, `needs:debug`, config-dependent encoding tests.
//!
//! ## Intentional exclusions
//!
//! Encoding-loop variants (FrogDB has a single internal encoding):
//! - `Check encoding - $encoding` — internal-encoding
//! - `ZADD with options syntax error with incomplete pair - $encoding` — internal-encoding
//! - `ZINCRBY accepts hexadecimal inputs - $encoding` — internal-encoding
//! - `ZINCRBY against invalid incr value - $encoding` — internal-encoding
//! - `ZRANGEBYLEX with LIMIT - $encoding` — internal-encoding
//! - `ZUNIONSTORE with AGGREGATE MIN - $encoding` — internal-encoding
//! - `ZUNIONSTORE with AGGREGATE MAX - $encoding` — internal-encoding
//! - `ZINTERSTORE with weights - $encoding` — internal-encoding
//! - `ZINTERSTORE with a regular set and weights - $encoding` — internal-encoding
//! - `ZINTERSTORE with AGGREGATE MIN - $encoding` — internal-encoding
//! - `ZINTERSTORE with AGGREGATE MAX - $encoding` — internal-encoding
//! - `$cmd with +inf/-inf scores - $encoding` — internal-encoding
//! - `$cmd with NaN weights - $encoding` — internal-encoding
//! - `ZDIFFSTORE with a regular set - $encoding` — internal-encoding
//! - `ZSCORE - $encoding` — internal-encoding
//! - `ZMSCORE - $encoding` — internal-encoding
//! - `Basic $popmin/$popmax with a single key - $encoding` — internal-encoding
//! - `$popmin/$popmax with count - $encoding` — internal-encoding
//! - `$popmin/$popmax with a single existing sorted set - $encoding` — internal-encoding
//! - `$popmin/$popmax with multiple existing sorted sets - $encoding` — internal-encoding
//! - `$popmin/$popmax second sorted set has members - $encoding` — internal-encoding
//! - `ZRANDMEMBER - $type` — internal-encoding
//! - `ZADD overflows the maximum allowed elements in a listpack - $type` — internal-encoding (listpack)
//! - `ZRANGESTORE with zset-max-listpack-entries 0 #10767 case` — internal-encoding (listpack)
//!
//! RESP3 variants and readraw protocol tests:
//! - `ZINTER RESP3 - $encoding` — RESP3-only
//! - `Basic $popmin/$popmax - $encoding RESP3` — RESP3-only
//! - `$popmin/$popmax with count - $encoding RESP3` — RESP3-only
//! - `$popmin/$popmax - $encoding RESP3` — RESP3-only
//! - `BZPOPMIN/BZPOPMAX readraw in RESP$resp` — RESP3-only
//! - `ZMPOP readraw in RESP$resp` — RESP3-only
//! - `BZMPOP readraw in RESP$resp` — RESP3-only
//! - `ZRANGESTORE RESP3` — RESP3-only
//! - `ZRANDMEMBER with RESP3` — RESP3-only
//!
//! Replication-propagation tests (FrogDB has different replication model):
//! - `ZMPOP propagate as pop with count command to replica` — replication-internal
//! - `BZMPOP propagate as pop with count command to replica` — replication-internal
//!
//! Skiplist / listpack internals (FrogDB has different storage):
//! - `ZSETs skiplist implementation backlink consistency test - $encoding` — internal-encoding
//! - `ZSETs ZRANK augmented skip list stress testing - $encoding` — internal-encoding (stress)
//! - `ZSET skiplist order consistency when elements are moved` — internal-encoding
//!
//! DEBUG-dependent:
//! - `ZSCORE after a DEBUG RELOAD - $encoding` — needs:debug
//!
//! Fuzz / stress tests:
//! - `ZSET sorting stresser - $encoding` — fuzzing/stress
//! - `ZRANGEBYSCORE fuzzy test, 100 ranges in $elements element sorted set - $encoding` — fuzzing/stress
//! - `ZRANGEBYLEX fuzzy test, 100 ranges in $elements element sorted set - $encoding` — fuzzing/stress
//! - `ZREMRANGEBYLEX fuzzy test, 100 ranges in $elements element sorted set - $encoding` — fuzzing/stress
//! - `ZDIFF fuzzing - $encoding` — fuzzing/stress
//!
//! Redis-internal command name / syntax-edge tests:
//! - `ZUNIONSTORE result is sorted` — Redis-internal ordering invariant
//! - `zunionInterDiffGenericCommand acts on SET and ZSET` — Redis-internal command name
//! - `ZRANGESTORE invalid syntax` — Redis-internal syntax-error format
//! - `ZRANGE invalid syntax` — Redis-internal syntax-error format
//! - `$pop with the count 0 returns an empty array` — intentional behavioral diff (count=0 edge)
//!
//! Blocking-edge tests deferred (need test-harness enhancements — multi-client
//! fairness helper, blocked-state verification on DEL/expiry, unblock-then-reblock
//! pattern, transaction-during-blocking pattern):
//! - `$pop, ZADD + DEL should not awake blocked client` — deferred — needs blocked-state verification helper
//! - `$pop, ZADD + DEL + SET should not awake blocked client` — deferred — needs blocked-state verification helper
//! - `BZPOPMIN unblock but the key is expired and then block again - reprocessing command` — deferred — needs reblock-aware test pattern
//! - `BZPOPMIN with same key multiple times should work` — deferred — needs multi-client harness helper
//! - `MULTI/EXEC is isolated from the point of view of $pop` — deferred — needs transaction-during-blocking pattern
//! - `$pop with zero timeout should block indefinitely` — deferred — needs indefinite-block verification
//! - `BZMPOP with multiple blocked clients` — deferred — needs multi-client harness helper
//! - `BZMPOP should not blocks on non key arguments - #10762` — deferred — needs blocked-state verification helper

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// ZADD basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zadd_basic_and_score_update() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "10", "x"]).await;
    c.command(&["ZADD", "ztmp", "20", "y"]).await;
    c.command(&["ZADD", "ztmp", "30", "z"]).await;
    let r = extract_bulk_strings(&c.command(&["ZRANGE", "ztmp", "0", "-1"]).await);
    assert_eq!(r, vec!["x", "y", "z"]);

    c.command(&["ZADD", "ztmp", "1", "y"]).await;
    let r = extract_bulk_strings(&c.command(&["ZRANGE", "ztmp", "0", "-1"]).await);
    assert_eq!(r, vec!["y", "x", "z"]);
}

#[tokio::test]
async fn tcl_zadd_nan_rejected() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;
    assert_error_prefix(&c.command(&["ZADD", "myzset", "nan", "abc"]).await, "ERR");
}

#[tokio::test]
async fn tcl_zincrby_nan_rejected() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;
    assert_error_prefix(
        &c.command(&["ZINCRBY", "myzset", "nan", "abc"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_zadd_xx_option() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    // XX on non-existing key
    assert_integer_eq(&c.command(&["ZADD", "ztmp", "XX", "10", "x"]).await, 0);

    c.command(&["ZADD", "ztmp", "10", "x"]).await;
    // XX does not add new members
    assert_integer_eq(&c.command(&["ZADD", "ztmp", "XX", "20", "y"]).await, 0);
    assert_integer_eq(&c.command(&["ZCARD", "ztmp"]).await, 1);

    // XX updates existing score
    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "10", "x", "20", "y", "30", "z"])
        .await;
    c.command(&[
        "ZADD", "ztmp", "XX", "5", "foo", "11", "x", "21", "y", "40", "zap",
    ])
    .await;
    assert_integer_eq(&c.command(&["ZCARD", "ztmp"]).await, 3);
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "x"]).await, b"11");
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "y"]).await, b"21");
}

#[tokio::test]
async fn tcl_zadd_nx_option() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "NX", "10", "x", "20", "y", "30", "z"])
        .await;
    assert_integer_eq(&c.command(&["ZCARD", "ztmp"]).await, 3);

    // NX only adds new, doesn't update existing
    assert_integer_eq(
        &c.command(&[
            "ZADD", "ztmp", "NX", "11", "x", "21", "y", "100", "a", "200", "b",
        ])
        .await,
        2,
    );
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "x"]).await, b"10");
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "y"]).await, b"20");
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "a"]).await, b"100");
}

#[tokio::test]
async fn tcl_zadd_xx_nx_not_compatible() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;
    assert_error_prefix(
        &c.command(&["ZADD", "ztmp", "XX", "NX", "10", "x"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_zadd_gt_lt_nx_not_compatible() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;
    assert_error_prefix(
        &c.command(&["ZADD", "ztmp", "GT", "NX", "10", "x"]).await,
        "ERR",
    );
    assert_error_prefix(
        &c.command(&["ZADD", "ztmp", "LT", "NX", "10", "x"]).await,
        "ERR",
    );
    assert_error_prefix(
        &c.command(&["ZADD", "ztmp", "LT", "GT", "10", "x"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_zadd_gt_updates_when_new_scores_greater() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "10", "x", "20", "y", "30", "z"])
        .await;
    // GT CH: foo is new (+1), x updated to 11 (+1 changed), y updated to 21 (+1), z stays 30
    assert_integer_eq(
        &c.command(&[
            "ZADD", "ztmp", "GT", "CH", "5", "foo", "11", "x", "21", "y", "29", "z",
        ])
        .await,
        3,
    );
    assert_integer_eq(&c.command(&["ZCARD", "ztmp"]).await, 4);
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "x"]).await, b"11");
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "y"]).await, b"21");
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "z"]).await, b"30");
}

#[tokio::test]
async fn tcl_zadd_lt_updates_when_new_scores_lower() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "10", "x", "20", "y", "30", "z"])
        .await;
    assert_integer_eq(
        &c.command(&[
            "ZADD", "ztmp", "LT", "CH", "5", "foo", "11", "x", "21", "y", "29", "z",
        ])
        .await,
        2,
    );
    assert_integer_eq(&c.command(&["ZCARD", "ztmp"]).await, 4);
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "x"]).await, b"10");
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "y"]).await, b"20");
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "z"]).await, b"29");
}

#[tokio::test]
async fn tcl_zadd_ch_option() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "10", "x", "20", "y", "30", "z"])
        .await;
    assert_integer_eq(
        &c.command(&["ZADD", "ztmp", "11", "x", "21", "y", "30", "z"])
            .await,
        0,
    );
    assert_integer_eq(
        &c.command(&["ZADD", "ztmp", "CH", "12", "x", "22", "y", "30", "z"])
            .await,
        2,
    );
}

#[tokio::test]
async fn tcl_zadd_incr_works_like_zincrby() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "10", "x", "20", "y", "30", "z"])
        .await;
    c.command(&["ZADD", "ztmp", "INCR", "15", "x"]).await;
    assert_bulk_eq(&c.command(&["ZSCORE", "ztmp", "x"]).await, b"25");
}

#[tokio::test]
async fn tcl_zadd_variadic() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "myzset"]).await;
    assert_integer_eq(
        &c.command(&["ZADD", "myzset", "10", "a", "20", "b", "30", "c"])
            .await,
        3,
    );
    let r = extract_bulk_strings(
        &c.command(&["ZRANGE", "myzset", "0", "-1", "WITHSCORES"])
            .await,
    );
    assert_eq!(r, vec!["a", "10", "b", "20", "c", "30"]);
}

#[tokio::test]
async fn tcl_zadd_variadic_return_value() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "myzset"]).await;
    c.command(&["ZADD", "myzset", "10", "a", "20", "b", "30", "c"])
        .await;
    // Only x is new (a,b,c exist)
    assert_integer_eq(
        &c.command(&["ZADD", "myzset", "5", "x", "20", "b", "30", "c"])
            .await,
        1,
    );
}

// ---------------------------------------------------------------------------
// ZCARD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zcard_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "10", "a", "20", "b", "30", "c"])
        .await;
    assert_integer_eq(&c.command(&["ZCARD", "ztmp"]).await, 3);
    assert_integer_eq(&c.command(&["ZCARD", "zdoesntexist"]).await, 0);
}

// ---------------------------------------------------------------------------
// ZREM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zrem_removes_key_after_last_element() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "10", "x", "20", "y"]).await;
    assert_integer_eq(&c.command(&["EXISTS", "ztmp"]).await, 1);
    assert_integer_eq(&c.command(&["ZREM", "ztmp", "z"]).await, 0);
    assert_integer_eq(&c.command(&["ZREM", "ztmp", "y"]).await, 1);
    assert_integer_eq(&c.command(&["ZREM", "ztmp", "x"]).await, 1);
    assert_integer_eq(&c.command(&["EXISTS", "ztmp"]).await, 0);
}

#[tokio::test]
async fn tcl_zrem_variadic() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "10", "a", "20", "b", "30", "c"])
        .await;
    assert_integer_eq(
        &c.command(&["ZREM", "ztmp", "x", "y", "a", "b", "k"]).await,
        2,
    );
    assert_integer_eq(&c.command(&["ZREM", "ztmp", "foo", "bar"]).await, 0);
    assert_integer_eq(&c.command(&["ZREM", "ztmp", "c"]).await, 1);
    assert_integer_eq(&c.command(&["EXISTS", "ztmp"]).await, 0);
}

// ---------------------------------------------------------------------------
// ZRANGE / ZREVRANGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zrange_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;

    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "ztmp", "0", "-1"]).await),
        vec!["a", "b", "c", "d"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "ztmp", "0", "-2"]).await),
        vec!["a", "b", "c"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "ztmp", "1", "-1"]).await),
        vec!["b", "c", "d"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "ztmp", "-2", "-1"]).await),
        vec!["c", "d"]
    );

    // out of range
    assert!(unwrap_array(c.command(&["ZRANGE", "ztmp", "5", "-1"]).await).is_empty());
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "ztmp", "0", "5"]).await),
        vec!["a", "b", "c", "d"]
    );

    // withscores
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGE", "ztmp", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["a", "1", "b", "2", "c", "3", "d", "4"]
    );
}

#[tokio::test]
async fn tcl_zrevrange_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "ztmp"]).await;
    c.command(&["ZADD", "ztmp", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;

    assert_eq!(
        extract_bulk_strings(&c.command(&["ZREVRANGE", "ztmp", "0", "-1"]).await),
        vec!["d", "c", "b", "a"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZREVRANGE", "ztmp", "0", "-2"]).await),
        vec!["d", "c", "b"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZREVRANGE", "ztmp", "1", "-1"]).await),
        vec!["c", "b", "a"]
    );

    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZREVRANGE", "ztmp", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["d", "4", "c", "3", "b", "2", "a", "1"]
    );
}

// ---------------------------------------------------------------------------
// ZRANK / ZREVRANK
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zrank_zrevrank_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zranktmp"]).await;
    c.command(&["ZADD", "zranktmp", "10", "x", "20", "y", "30", "z"])
        .await;
    assert_integer_eq(&c.command(&["ZRANK", "zranktmp", "x"]).await, 0);
    assert_integer_eq(&c.command(&["ZRANK", "zranktmp", "y"]).await, 1);
    assert_integer_eq(&c.command(&["ZRANK", "zranktmp", "z"]).await, 2);
    assert_integer_eq(&c.command(&["ZREVRANK", "zranktmp", "x"]).await, 2);
    assert_integer_eq(&c.command(&["ZREVRANK", "zranktmp", "y"]).await, 1);
    assert_integer_eq(&c.command(&["ZREVRANK", "zranktmp", "z"]).await, 0);
    assert_nil(&c.command(&["ZRANK", "zranktmp", "foo"]).await);
    assert_nil(&c.command(&["ZREVRANK", "zranktmp", "foo"]).await);
}

#[tokio::test]
async fn tcl_zrank_after_deletion() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zranktmp"]).await;
    c.command(&["ZADD", "zranktmp", "10", "x", "20", "y", "30", "z"])
        .await;
    c.command(&["ZREM", "zranktmp", "y"]).await;
    assert_integer_eq(&c.command(&["ZRANK", "zranktmp", "x"]).await, 0);
    assert_integer_eq(&c.command(&["ZRANK", "zranktmp", "z"]).await, 1);
}

// ---------------------------------------------------------------------------
// ZINCRBY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zincrby_create_new_sorted_set() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    c.command(&["ZINCRBY", "zset", "1", "foo"]).await;
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "zset", "0", "-1"]).await),
        vec!["foo"]
    );
    assert_bulk_eq(&c.command(&["ZSCORE", "zset", "foo"]).await, b"1");
}

#[tokio::test]
async fn tcl_zincrby_increment_and_decrement() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    c.command(&["ZINCRBY", "zset", "1", "foo"]).await;
    c.command(&["ZINCRBY", "zset", "2", "foo"]).await;
    c.command(&["ZINCRBY", "zset", "1", "bar"]).await;
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "zset", "0", "-1"]).await),
        vec!["bar", "foo"]
    );

    c.command(&["ZINCRBY", "zset", "10", "bar"]).await;
    c.command(&["ZINCRBY", "zset", "-5", "foo"]).await;
    c.command(&["ZINCRBY", "zset", "-5", "bar"]).await;
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "zset", "0", "-1"]).await),
        vec!["foo", "bar"]
    );
    assert_bulk_eq(&c.command(&["ZSCORE", "zset", "foo"]).await, b"-2");
    assert_bulk_eq(&c.command(&["ZSCORE", "zset", "bar"]).await, b"6");
}

#[tokio::test]
async fn tcl_zincrby_leading_to_nan_is_error() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "myzset"]).await;
    c.command(&["ZINCRBY", "myzset", "+inf", "abc"]).await;
    assert_error_prefix(
        &c.command(&["ZINCRBY", "myzset", "-inf", "abc"]).await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZCOUNT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zrangebyscore_zcount_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    for (score, member) in [
        ("-inf", "a"),
        ("1", "b"),
        ("2", "c"),
        ("3", "d"),
        ("4", "e"),
        ("5", "f"),
        ("+inf", "g"),
    ] {
        c.command(&["ZADD", "zset", score, member]).await;
    }

    // inclusive
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGEBYSCORE", "zset", "-inf", "2"]).await),
        vec!["a", "b", "c"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGEBYSCORE", "zset", "0", "3"]).await),
        vec!["b", "c", "d"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGEBYSCORE", "zset", "3", "6"]).await),
        vec!["d", "e", "f"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGEBYSCORE", "zset", "4", "+inf"]).await),
        vec!["e", "f", "g"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZREVRANGEBYSCORE", "zset", "2", "-inf"]).await),
        vec!["c", "b", "a"]
    );
    assert_integer_eq(&c.command(&["ZCOUNT", "zset", "0", "3"]).await, 3);

    // exclusive
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGEBYSCORE", "zset", "(-inf", "(2"]).await),
        vec!["b"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGEBYSCORE", "zset", "(0", "(3"]).await),
        vec!["b", "c"]
    );
    assert_integer_eq(&c.command(&["ZCOUNT", "zset", "(0", "(3"]).await, 2);
}

#[tokio::test]
async fn tcl_zrangebyscore_with_withscores() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    for (score, member) in [
        ("-inf", "a"),
        ("1", "b"),
        ("2", "c"),
        ("3", "d"),
        ("4", "e"),
        ("5", "f"),
        ("+inf", "g"),
    ] {
        c.command(&["ZADD", "zset", score, member]).await;
    }
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGEBYSCORE", "zset", "0", "3", "WITHSCORES"])
                .await
        ),
        vec!["b", "1", "c", "2", "d", "3"]
    );
}

#[tokio::test]
async fn tcl_zrangebyscore_with_limit() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    for (score, member) in [
        ("-inf", "a"),
        ("1", "b"),
        ("2", "c"),
        ("3", "d"),
        ("4", "e"),
        ("5", "f"),
        ("+inf", "g"),
    ] {
        c.command(&["ZADD", "zset", score, member]).await;
    }
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGEBYSCORE", "zset", "0", "10", "LIMIT", "0", "2"])
                .await
        ),
        vec!["b", "c"]
    );
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGEBYSCORE", "zset", "0", "10", "LIMIT", "2", "3"])
                .await
        ),
        vec!["d", "e", "f"]
    );
    assert!(
        unwrap_array(
            c.command(&["ZRANGEBYSCORE", "zset", "0", "10", "LIMIT", "20", "10"])
                .await
        )
        .is_empty()
    );
}

// ---------------------------------------------------------------------------
// ZRANGEBYLEX / ZREVRANGEBYLEX / ZLEXCOUNT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zrangebylex_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    for m in [
        "alpha", "bar", "cool", "down", "elephant", "foo", "great", "hill", "omega",
    ] {
        c.command(&["ZADD", "zset", "0", m]).await;
    }

    // inclusive
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGEBYLEX", "zset", "-", "[cool"]).await),
        vec!["alpha", "bar", "cool"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGEBYLEX", "zset", "[bar", "[down"]).await),
        vec!["bar", "cool", "down"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGEBYLEX", "zset", "[g", "+"]).await),
        vec!["great", "hill", "omega"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZREVRANGEBYLEX", "zset", "[cool", "-"]).await),
        vec!["cool", "bar", "alpha"]
    );
    assert_integer_eq(&c.command(&["ZLEXCOUNT", "zset", "[ele", "[h"]).await, 3);

    // exclusive
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGEBYLEX", "zset", "-", "(cool"]).await),
        vec!["alpha", "bar"]
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGEBYLEX", "zset", "(bar", "(down"]).await),
        vec!["cool"]
    );
    assert_integer_eq(
        &c.command(&["ZLEXCOUNT", "zset", "(ele", "(great"]).await,
        2,
    );
}

#[tokio::test]
async fn tcl_zlexcount_advanced() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    for m in [
        "alpha", "bar", "cool", "down", "elephant", "foo", "great", "hill", "omega",
    ] {
        c.command(&["ZADD", "zset", "0", m]).await;
    }

    assert_integer_eq(&c.command(&["ZLEXCOUNT", "zset", "-", "+"]).await, 9);
    assert_integer_eq(&c.command(&["ZLEXCOUNT", "zset", "+", "-"]).await, 0);
    assert_integer_eq(&c.command(&["ZLEXCOUNT", "zset", "[bar", "+"]).await, 8);
    assert_integer_eq(&c.command(&["ZLEXCOUNT", "zset", "[bar", "[foo"]).await, 5);
    assert_integer_eq(&c.command(&["ZLEXCOUNT", "zset", "[bar", "(foo"]).await, 4);
    assert_integer_eq(&c.command(&["ZLEXCOUNT", "zset", "(bar", "[foo"]).await, 4);
    assert_integer_eq(&c.command(&["ZLEXCOUNT", "zset", "(bar", "(foo"]).await, 3);
}

// ---------------------------------------------------------------------------
// ZREMRANGEBYSCORE / ZREMRANGEBYRANK / ZREMRANGEBYLEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zremrangebyscore_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    // inner range
    c.command(&["DEL", "zset"]).await;
    c.command(&[
        "ZADD", "zset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ])
    .await;
    assert_integer_eq(&c.command(&["ZREMRANGEBYSCORE", "zset", "2", "4"]).await, 3);
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "zset", "0", "-1"]).await),
        vec!["a", "e"]
    );

    // -inf to +inf
    c.command(&["DEL", "zset"]).await;
    c.command(&[
        "ZADD", "zset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ])
    .await;
    assert_integer_eq(
        &c.command(&["ZREMRANGEBYSCORE", "zset", "-inf", "+inf"])
            .await,
        5,
    );
    assert!(unwrap_array(c.command(&["ZRANGE", "zset", "0", "-1"]).await).is_empty());

    // exclusive
    c.command(&["DEL", "zset"]).await;
    c.command(&[
        "ZADD", "zset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ])
    .await;
    assert_integer_eq(
        &c.command(&["ZREMRANGEBYSCORE", "zset", "(1", "(5"]).await,
        3,
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "zset", "0", "-1"]).await),
        vec!["a", "e"]
    );
}

#[tokio::test]
async fn tcl_zremrangebyrank_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    c.command(&[
        "ZADD", "zset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ])
    .await;
    assert_integer_eq(&c.command(&["ZREMRANGEBYRANK", "zset", "1", "3"]).await, 3);
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "zset", "0", "-1"]).await),
        vec!["a", "e"]
    );

    // destroy when empty
    c.command(&["DEL", "zset"]).await;
    c.command(&[
        "ZADD", "zset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ])
    .await;
    assert_integer_eq(&c.command(&["ZREMRANGEBYRANK", "zset", "0", "4"]).await, 5);
    assert_integer_eq(&c.command(&["EXISTS", "zset"]).await, 0);
}

#[tokio::test]
async fn tcl_zremrangebylex_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    for m in [
        "alpha", "bar", "cool", "down", "elephant", "foo", "great", "hill", "omega",
    ] {
        c.command(&["ZADD", "zset", "0", m]).await;
    }
    assert_integer_eq(
        &c.command(&["ZREMRANGEBYLEX", "zset", "-", "[cool"]).await,
        3,
    );
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "zset", "0", "-1"]).await),
        vec!["down", "elephant", "foo", "great", "hill", "omega"]
    );
}

// ---------------------------------------------------------------------------
// ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE / ZUNION / ZINTER / ZDIFF
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zunionstore_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zseta{t}", "zsetb{t}", "zsetc{t}"])
        .await;
    c.command(&["ZADD", "zseta{t}", "1", "a", "2", "b", "3", "c"])
        .await;
    c.command(&["ZADD", "zsetb{t}", "1", "b", "2", "c", "3", "d"])
        .await;

    assert_integer_eq(
        &c.command(&["ZUNIONSTORE", "zsetc{t}", "2", "zseta{t}", "zsetb{t}"])
            .await,
        4,
    );
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGE", "zsetc{t}", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["a", "1", "b", "3", "d", "3", "c", "5"]
    );
}

#[tokio::test]
async fn tcl_zinterstore_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zseta{t}", "zsetb{t}", "zsetc{t}"])
        .await;
    c.command(&["ZADD", "zseta{t}", "1", "a", "2", "b", "3", "c"])
        .await;
    c.command(&["ZADD", "zsetb{t}", "1", "b", "2", "c", "3", "d"])
        .await;

    assert_integer_eq(
        &c.command(&["ZINTERSTORE", "zsetc{t}", "2", "zseta{t}", "zsetb{t}"])
            .await,
        2,
    );
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGE", "zsetc{t}", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["b", "3", "c", "5"]
    );
}

#[tokio::test]
async fn tcl_zdiffstore_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zseta{t}", "zsetb{t}", "zsetc{t}"])
        .await;
    c.command(&["ZADD", "zseta{t}", "1", "a", "2", "b", "3", "c"])
        .await;
    c.command(&["ZADD", "zsetb{t}", "1", "b", "2", "c", "3", "d"])
        .await;

    assert_integer_eq(
        &c.command(&["ZDIFFSTORE", "zsetc{t}", "2", "zseta{t}", "zsetb{t}"])
            .await,
        1,
    );
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGE", "zsetc{t}", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["a", "1"]
    );
}

#[tokio::test]
async fn tcl_zunion_zinter_zdiff_against_non_existing() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zseta"]).await;
    assert!(unwrap_array(c.command(&["ZUNION", "1", "zseta"]).await).is_empty());
    assert!(unwrap_array(c.command(&["ZINTER", "1", "zseta"]).await).is_empty());
    assert_integer_eq(&c.command(&["ZINTERCARD", "1", "zseta"]).await, 0);
    assert!(unwrap_array(c.command(&["ZDIFF", "1", "zseta"]).await).is_empty());
}

#[tokio::test]
async fn tcl_zunionstore_with_weights() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zseta{t}", "zsetb{t}", "zsetc{t}"])
        .await;
    c.command(&["ZADD", "zseta{t}", "1", "a", "2", "b", "3", "c"])
        .await;
    c.command(&["ZADD", "zsetb{t}", "1", "b", "2", "c", "3", "d"])
        .await;

    assert_integer_eq(
        &c.command(&[
            "ZUNIONSTORE",
            "zsetc{t}",
            "2",
            "zseta{t}",
            "zsetb{t}",
            "WEIGHTS",
            "2",
            "3",
        ])
        .await,
        4,
    );
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGE", "zsetc{t}", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["a", "2", "b", "7", "d", "9", "c", "12"]
    );
}

#[tokio::test]
async fn tcl_zintercard_basics() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zseta{t}", "zsetb{t}"]).await;
    c.command(&["ZADD", "zseta{t}", "1", "a", "2", "b", "3", "c"])
        .await;
    c.command(&["ZADD", "zsetb{t}", "1", "b", "2", "c", "3", "d"])
        .await;

    assert_integer_eq(
        &c.command(&["ZINTERCARD", "2", "zseta{t}", "zsetb{t}"])
            .await,
        2,
    );
    assert_integer_eq(
        &c.command(&["ZINTERCARD", "2", "zseta{t}", "zsetb{t}", "LIMIT", "1"])
            .await,
        1,
    );
    assert_integer_eq(
        &c.command(&["ZINTERCARD", "2", "zseta{t}", "zsetb{t}", "LIMIT", "10"])
            .await,
        2,
    );
}

// ---------------------------------------------------------------------------
// ZPOPMIN / ZPOPMAX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zpopmin_zpopmax_basic() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    c.command(&[
        "ZADD", "zset", "-1", "a", "1", "b", "2", "c", "3", "d", "4", "e",
    ])
    .await;

    let r = extract_bulk_strings(&c.command(&["ZPOPMIN", "zset"]).await);
    assert_eq!(r, vec!["a", "-1"]);
    let r = extract_bulk_strings(&c.command(&["ZPOPMIN", "zset"]).await);
    assert_eq!(r, vec!["b", "1"]);
    let r = extract_bulk_strings(&c.command(&["ZPOPMAX", "zset"]).await);
    assert_eq!(r, vec!["e", "4"]);
    let r = extract_bulk_strings(&c.command(&["ZPOPMAX", "zset"]).await);
    assert_eq!(r, vec!["d", "3"]);
    let r = extract_bulk_strings(&c.command(&["ZPOPMIN", "zset"]).await);
    assert_eq!(r, vec!["c", "2"]);
    assert_integer_eq(&c.command(&["EXISTS", "zset"]).await, 0);
}

#[tokio::test]
async fn tcl_zpopmin_zpopmax_with_count() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "z1"]).await;
    c.command(&["ZADD", "z1", "0", "a", "1", "b", "2", "c", "3", "d"])
        .await;

    let r = extract_bulk_strings(&c.command(&["ZPOPMIN", "z1", "2"]).await);
    assert_eq!(r, vec!["a", "0", "b", "1"]);
    let r = extract_bulk_strings(&c.command(&["ZPOPMAX", "z1", "2"]).await);
    assert_eq!(r, vec!["d", "3", "c", "2"]);
}

#[tokio::test]
async fn tcl_zpopmin_zpopmax_count_zero() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    c.command(&["ZADD", "zset", "1", "a", "2", "b", "3", "c"])
        .await;
    assert!(unwrap_array(c.command(&["ZPOPMIN", "zset", "0"]).await).is_empty());
    assert!(unwrap_array(c.command(&["ZPOPMAX", "zset", "0"]).await).is_empty());
    assert_integer_eq(&c.command(&["ZCARD", "zset"]).await, 3);
}

#[tokio::test]
async fn tcl_zpopmin_zpopmax_negative_count() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zset"]).await;
    c.command(&["ZADD", "zset", "1", "a", "2", "b", "3", "c"])
        .await;
    assert_error_prefix(&c.command(&["ZPOPMIN", "zset", "-1"]).await, "ERR");
    assert_error_prefix(&c.command(&["ZPOPMAX", "zset", "-3"]).await, "ERR");
}

#[tokio::test]
async fn tcl_zpop_zmpop_against_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["SET", "foo{t}", "bar"]).await;
    assert_error_prefix(&c.command(&["ZPOPMIN", "foo{t}"]).await, "WRONGTYPE");
    assert_error_prefix(&c.command(&["ZPOPMAX", "foo{t}"]).await, "WRONGTYPE");
    assert_error_prefix(
        &c.command(&["ZMPOP", "1", "foo{t}", "MIN"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &c.command(&["ZMPOP", "1", "foo{t}", "MAX"]).await,
        "WRONGTYPE",
    );
}

// ---------------------------------------------------------------------------
// ZMPOP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zmpop_illegal_arguments() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    assert_error_prefix(&c.command(&["ZMPOP"]).await, "ERR");
    assert_error_prefix(&c.command(&["ZMPOP", "1"]).await, "ERR");
    assert_error_prefix(&c.command(&["ZMPOP", "1", "myzset{t}"]).await, "ERR");
    assert_error_prefix(&c.command(&["ZMPOP", "0", "myzset{t}", "MIN"]).await, "ERR");
    assert_error_prefix(
        &c.command(&["ZMPOP", "1", "myzset{t}", "bad_where"]).await,
        "ERR",
    );
    assert_error_prefix(
        &c.command(&["ZMPOP", "1", "myzset{t}", "MIN", "COUNT", "0"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &c.command(&["ZMPOP", "1", "myzset{t}", "MIN", "COUNT", "-1"])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// ZMSCORE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zmscore_retrieve() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zmscoretest"]).await;
    c.command(&["ZADD", "zmscoretest", "10", "x", "20", "y"])
        .await;
    let r = extract_bulk_strings(&c.command(&["ZMSCORE", "zmscoretest", "x", "y"]).await);
    assert_eq!(r, vec!["10", "20"]);
}

#[tokio::test]
async fn tcl_zmscore_retrieve_from_empty_set() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zmscoretest"]).await;
    let items = unwrap_array(c.command(&["ZMSCORE", "zmscoretest", "x", "y"]).await);
    assert_nil(&items[0]);
    assert_nil(&items[1]);
}

#[tokio::test]
async fn tcl_zmscore_retrieve_with_missing_member() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zmscoretest"]).await;
    c.command(&["ZADD", "zmscoretest", "10", "x"]).await;
    let items = unwrap_array(c.command(&["ZMSCORE", "zmscoretest", "x", "y"]).await);
    assert_bulk_eq(&items[0], b"10");
    assert_nil(&items[1]);
}

#[tokio::test]
async fn tcl_zmscore_requires_one_or_more_members() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zmscoretest"]).await;
    c.command(&["ZADD", "zmscoretest", "10", "x"]).await;
    assert_error_prefix(&c.command(&["ZMSCORE", "zmscoretest"]).await, "ERR");
}

// ---------------------------------------------------------------------------
// ZRANGESTORE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zrangestore_basic() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["FLUSHALL"]).await;
    c.command(&["ZADD", "z1{t}", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    assert_integer_eq(
        &c.command(&["ZRANGESTORE", "z2{t}", "z1{t}", "0", "-1"])
            .await,
        4,
    );
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGE", "z2{t}", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["a", "1", "b", "2", "c", "3", "d", "4"]
    );
}

#[tokio::test]
async fn tcl_zrangestore_range() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "z1{t}", "z2{t}"]).await;
    c.command(&["ZADD", "z1{t}", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    assert_integer_eq(
        &c.command(&["ZRANGESTORE", "z2{t}", "z1{t}", "1", "2"])
            .await,
        2,
    );
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGE", "z2{t}", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["b", "2", "c", "3"]
    );
}

#[tokio::test]
async fn tcl_zrangestore_bylex() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "z1{t}", "z2{t}"]).await;
    c.command(&["ZADD", "z1{t}", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    assert_integer_eq(
        &c.command(&["ZRANGESTORE", "z2{t}", "z1{t}", "[b", "[c", "BYLEX"])
            .await,
        2,
    );
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGE", "z2{t}", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["b", "2", "c", "3"]
    );
}

#[tokio::test]
async fn tcl_zrangestore_byscore() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "z1{t}", "z2{t}"]).await;
    c.command(&["ZADD", "z1{t}", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    assert_integer_eq(
        &c.command(&["ZRANGESTORE", "z2{t}", "z1{t}", "1", "2", "BYSCORE"])
            .await,
        2,
    );
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGE", "z2{t}", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["a", "1", "b", "2"]
    );
}

#[tokio::test]
async fn tcl_zrangestore_byscore_limit() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "z1{t}", "z2{t}"]).await;
    c.command(&["ZADD", "z1{t}", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    assert_integer_eq(
        &c.command(&[
            "ZRANGESTORE",
            "z2{t}",
            "z1{t}",
            "0",
            "5",
            "BYSCORE",
            "LIMIT",
            "0",
            "2",
        ])
        .await,
        2,
    );
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGE", "z2{t}", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["a", "1", "b", "2"]
    );
}

#[tokio::test]
async fn tcl_zrangestore_src_key_missing() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "z2{t}"]).await;
    assert_integer_eq(
        &c.command(&["ZRANGESTORE", "z2{t}", "missing{t}", "0", "-1"])
            .await,
        0,
    );
    assert_integer_eq(&c.command(&["EXISTS", "z2{t}"]).await, 0);
}

#[tokio::test]
async fn tcl_zrangestore_src_key_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "z2{t}", "foo{t}"]).await;
    c.command(&["ZADD", "z2{t}", "1", "a"]).await;
    c.command(&["SET", "foo{t}", "bar"]).await;
    assert_error_prefix(
        &c.command(&["ZRANGESTORE", "z2{t}", "foo{t}", "0", "-1"])
            .await,
        "WRONGTYPE",
    );
    // z2 should be untouched
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "z2{t}", "0", "-1"]).await),
        vec!["a"]
    );
}

#[tokio::test]
async fn tcl_zrangestore_empty_range() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "z1{t}", "z2{t}"]).await;
    c.command(&["ZADD", "z1{t}", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    assert_integer_eq(
        &c.command(&["ZRANGESTORE", "z2{t}", "z1{t}", "5", "6"])
            .await,
        0,
    );
    assert_integer_eq(&c.command(&["EXISTS", "z2{t}"]).await, 0);
}

// ---------------------------------------------------------------------------
// ZRANGE BYLEX / BYSCORE REV LIMIT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zrange_bylex() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "z1{t}"]).await;
    c.command(&["ZADD", "z1{t}", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "z1{t}", "[b", "[c", "BYLEX"]).await),
        vec!["b", "c"]
    );
}

#[tokio::test]
async fn tcl_zrange_byscore_rev_limit() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "z1{t}"]).await;
    c.command(&["ZADD", "z1{t}", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    assert_eq!(
        extract_bulk_strings(
            &c.command(&[
                "ZRANGE",
                "z1{t}",
                "5",
                "0",
                "BYSCORE",
                "REV",
                "LIMIT",
                "0",
                "2",
                "WITHSCORES"
            ])
            .await
        ),
        vec!["d", "4", "c", "3"]
    );
}

// ---------------------------------------------------------------------------
// ZRANDMEMBER
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zrandmember_count_of_0() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "myzset"]).await;
    c.command(&["ZADD", "myzset", "1", "a", "2", "b"]).await;
    assert!(unwrap_array(c.command(&["ZRANDMEMBER", "myzset", "0"]).await).is_empty());
}

#[tokio::test]
async fn tcl_zrandmember_with_count_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    assert!(unwrap_array(c.command(&["ZRANDMEMBER", "nonexisting_key", "100"]).await).is_empty());
}

#[tokio::test]
async fn tcl_zrandmember_count_overflow() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "myzset"]).await;
    c.command(&["ZADD", "myzset", "0", "a"]).await;
    assert_error_prefix(
        &c.command(&[
            "ZRANDMEMBER",
            "myzset",
            "-9223372036854770000",
            "WITHSCORES",
        ])
        .await,
        "ERR",
    );
    assert_error_prefix(
        &c.command(&["ZRANDMEMBER", "myzset", "-9223372036854775808"])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// ZSET commands with empty string score / at least 1 input key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zset_commands_dont_accept_empty_strings_as_valid_score() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;
    assert_error_prefix(&c.command(&["ZADD", "myzset", "", "abc"]).await, "ERR");
}

#[tokio::test]
async fn tcl_zunion_inter_diff_at_least_1_input_key() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    assert_error_prefix(&c.command(&["ZUNION", "0", "key{t}"]).await, "ERR");
    assert_error_prefix(
        &c.command(&["ZUNIONSTORE", "dst_key{t}", "0", "key{t}"])
            .await,
        "ERR",
    );
    assert_error_prefix(&c.command(&["ZINTER", "0", "key{t}"]).await, "ERR");
    assert_error_prefix(
        &c.command(&["ZINTERSTORE", "dst_key{t}", "0", "key{t}"])
            .await,
        "ERR",
    );
    assert_error_prefix(&c.command(&["ZDIFF", "0", "key{t}"]).await, "ERR");
    assert_error_prefix(
        &c.command(&["ZDIFFSTORE", "dst_key{t}", "0", "key{t}"])
            .await,
        "ERR",
    );
    assert_error_prefix(&c.command(&["ZINTERCARD", "0", "key{t}"]).await, "ERR");
}

// ---------------------------------------------------------------------------
// ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE WITHSCORES error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zstore_error_if_using_withscores() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "zsetd{t}", "zsetf{t}"]).await;
    c.command(&["ZADD", "zsetd{t}", "1", "a"]).await;
    c.command(&["ZADD", "zsetf{t}", "1", "a"]).await;
    assert_error_prefix(
        &c.command(&[
            "ZUNIONSTORE",
            "foo{t}",
            "2",
            "zsetd{t}",
            "zsetf{t}",
            "WITHSCORES",
        ])
        .await,
        "ERR",
    );
    assert_error_prefix(
        &c.command(&[
            "ZINTERSTORE",
            "foo{t}",
            "2",
            "zsetd{t}",
            "zsetf{t}",
            "WITHSCORES",
        ])
        .await,
        "ERR",
    );
    assert_error_prefix(
        &c.command(&[
            "ZDIFFSTORE",
            "foo{t}",
            "2",
            "zsetd{t}",
            "zsetf{t}",
            "WITHSCORES",
        ])
        .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// Regression tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zunionstore_regression_should_not_create_nan() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "z{t}", "out{t}"]).await;
    c.command(&["ZADD", "z{t}", "-inf", "neginf"]).await;
    c.command(&["ZUNIONSTORE", "out{t}", "1", "z{t}", "WEIGHTS", "0"])
        .await;
    assert_eq!(
        extract_bulk_strings(
            &c.command(&["ZRANGE", "out{t}", "0", "-1", "WITHSCORES"])
                .await
        ),
        vec!["neginf", "0"]
    );
}

#[tokio::test]
async fn tcl_zinterstore_516_regression() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    c.command(&["DEL", "one{t}", "two{t}", "three{t}", "to_here{t}"])
        .await;
    c.command(&["SADD", "one{t}", "100", "101", "102", "103"])
        .await;
    c.command(&["SADD", "two{t}", "100", "200", "201", "202"])
        .await;
    c.command(&[
        "ZADD", "three{t}", "1", "500", "1", "501", "1", "502", "1", "503", "1", "100",
    ])
    .await;
    c.command(&[
        "ZINTERSTORE",
        "to_here{t}",
        "3",
        "one{t}",
        "two{t}",
        "three{t}",
        "WEIGHTS",
        "0",
        "0",
        "1",
    ])
    .await;
    assert_eq!(
        extract_bulk_strings(&c.command(&["ZRANGE", "to_here{t}", "0", "-1"]).await),
        vec!["100"]
    );
}
