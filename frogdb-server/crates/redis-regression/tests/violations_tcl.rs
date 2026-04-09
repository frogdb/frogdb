//! Rust port of Redis 8.6.0 `unit/violations.tcl` test suite.
//!
//! Most tests in this file are `large-memory`-tagged stress tests (5GB/4GB
//! bulk strings) that trigger integer-overflow edge cases in Redis' listpack
//! and protocol bulk-read paths. Those tests are out of scope for the unit
//! test port — they allocate multi-gigabyte bulks and depend on Redis' raw
//! socket writing helpers.
//!
//! Only one test is not `large-memory`-tagged: `SORT adds integer field to
//! list`, a coverage regression ensuring SORT..GET S* STORE correctly reads
//! an integer-encoded value.
//!
//! ## Intentional exclusions
//!
//! Large-memory stress tests (allocate 4-5 GB bulks to trigger overflow paths):
//! - `XADD one huge field` — large-memory tag — out of scope for unit tests
//! - `XADD one huge field - 1` — large-memory tag — out of scope for unit tests
//! - `several XADD big fields` — large-memory tag — out of scope for unit tests
//! - `single XADD big fields` — large-memory tag — out of scope for unit tests
//! - `hash with many big fields` — large-memory tag — out of scope for unit tests
//! - `hash with one huge field` — large-memory tag — out of scope for unit tests

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::{TestServer, TestServerConfig};

/// Upstream: `SORT adds integer field to list`
///
/// Coverage test — a SORT..BY..GET..STORE round-trip that reads an
/// integer-encoded source value into a list. Upstream asserts `assert_encoding
/// "int" S2`, which is Redis-internal; we just check the user-visible LLEN
/// result.
///
/// Uses a single-shard server because SORT with external BY/GET keys rejects
/// cross-shard key sets with a CROSSSLOT error (same reason
/// `sort_regression.rs` uses `num_shards: Some(1)`).
#[tokio::test]
async fn tcl_sort_adds_integer_field_to_list() {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "S1", "asdf"]).await);
    // Integer-encoded in Redis (FrogDB has no equivalent internal encoding),
    // but the functional contract is the same: SORT should read "123" back.
    assert_ok(&client.command(&["SET", "S2", "123"]).await);

    assert_integer_eq(&client.command(&["SADD", "myset", "1", "2"]).await, 2);
    assert_ok(&client.command(&["MSET", "D1", "1", "D2", "2"]).await);

    let sort_resp = client
        .command(&["SORT", "myset", "BY", "D*", "GET", "S*", "STORE", "mylist"])
        .await;
    assert_integer_eq(&sort_resp, 2);

    assert_integer_eq(&client.command(&["LLEN", "mylist"]).await, 2);
}
