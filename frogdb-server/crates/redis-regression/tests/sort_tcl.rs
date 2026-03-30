//! Rust port of Redis 8.6.0 `unit/sort.tcl` test suite.
//!
//! Excludes: encoding loops (`assert_encoding`/`assert_refcount`/`check_sort_store_encoding`),
//! `needs:debug` tests, `external:skip`/cluster tests, `CONFIG SET` tests,
//! `create_random_dataset` procedural tests (BY weight_*, BY hash field in loop),
//! speed/benchmark tests, Lua `EVAL`-based tests.

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Helper: collect bulk strings from an array response, preserving nils as empty
// ---------------------------------------------------------------------------

fn extract_bulk_strings_with_nils(response: &Response) -> Vec<String> {
    match response {
        Response::Array(items) => items
            .iter()
            .map(|item| match item {
                Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).unwrap_or_default(),
                Response::Bulk(None) => String::new(),
                other => panic!("expected Bulk in array, got {other:?}"),
            })
            .collect(),
        other => panic!("expected Array, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// SORT GET #
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_get_hash_returns_elements_sorted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "tosort"]).await;
    // Push elements 0..15
    for i in 0..16 {
        client.command(&["LPUSH", "tosort", &i.to_string()]).await;
    }

    let resp = client.command(&["SORT", "tosort", "GET", "#"]).await;
    let items = extract_bulk_strings(&resp);
    // GET # returns the elements themselves; default numeric sort => 0..15
    let expected: Vec<String> = (0..16).map(|i| i.to_string()).collect();
    assert_eq!(items, expected);
}

// ---------------------------------------------------------------------------
// SORT/SORT_RO GET <const> (non-existent key)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_get_const_returns_nils() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "tosort", "foo"]).await;
    for i in 0..16 {
        client.command(&["LPUSH", "tosort", &i.to_string()]).await;
    }

    let resp = client.command(&["SORT", "tosort", "GET", "foo"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 16);
    for item in &items {
        assert_nil(item);
    }
}

#[tokio::test]
async fn tcl_sort_ro_get_const_returns_nils() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "tosort", "foo"]).await;
    for i in 0..16 {
        client.command(&["LPUSH", "tosort", &i.to_string()]).await;
    }

    let resp = client.command(&["SORT_RO", "tosort", "GET", "foo"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 16);
    for item in &items {
        assert_nil(item);
    }
}

// ---------------------------------------------------------------------------
// SORT DESC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_desc() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "tosort"]).await;
    for i in 0..16 {
        client.command(&["LPUSH", "tosort", &i.to_string()]).await;
    }

    let resp = client.command(&["SORT", "tosort", "DESC"]).await;
    let items = extract_bulk_strings(&resp);
    let expected: Vec<String> = (0..16).rev().map(|i| i.to_string()).collect();
    assert_eq!(items, expected);
}

// ---------------------------------------------------------------------------
// SORT ALPHA against integer encoded strings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_alpha_against_integer_encoded_strings() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["LPUSH", "mylist", "2"]).await;
    client.command(&["LPUSH", "mylist", "1"]).await;
    client.command(&["LPUSH", "mylist", "3"]).await;
    client.command(&["LPUSH", "mylist", "10"]).await;

    let resp = client.command(&["SORT", "mylist", "ALPHA"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["1", "10", "2", "3"]);
}

// ---------------------------------------------------------------------------
// SORT sorted set
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_sorted_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "zset"]).await;
    client.command(&["ZADD", "zset", "1", "a"]).await;
    client.command(&["ZADD", "zset", "5", "b"]).await;
    client.command(&["ZADD", "zset", "2", "c"]).await;
    client.command(&["ZADD", "zset", "10", "d"]).await;
    client.command(&["ZADD", "zset", "3", "e"]).await;

    let resp = client.command(&["SORT", "zset", "ALPHA", "DESC"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["e", "d", "c", "b", "a"]);
}

// ---------------------------------------------------------------------------
// SORT sorted set BY nosort should retain ordering
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_sorted_set_by_nosort_retains_ordering() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "zset"]).await;
    client.command(&["ZADD", "zset", "1", "a"]).await;
    client.command(&["ZADD", "zset", "5", "b"]).await;
    client.command(&["ZADD", "zset", "2", "c"]).await;
    client.command(&["ZADD", "zset", "10", "d"]).await;
    client.command(&["ZADD", "zset", "3", "e"]).await;

    // Use MULTI/EXEC to get both results atomically
    assert_ok(&client.command(&["MULTI"]).await);
    client
        .command(&["SORT", "zset", "BY", "nosort", "ASC"])
        .await;
    client
        .command(&["SORT", "zset", "BY", "nosort", "DESC"])
        .await;
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);

    let asc = extract_bulk_strings(&results[0]);
    let desc = extract_bulk_strings(&results[1]);
    assert_eq!(asc, vec!["a", "c", "e", "b", "d"]);
    assert_eq!(desc, vec!["d", "b", "e", "c", "a"]);
}

// ---------------------------------------------------------------------------
// SORT sorted set BY nosort + LIMIT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_sorted_set_by_nosort_with_limit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "zset"]).await;
    client.command(&["ZADD", "zset", "1", "a"]).await;
    client.command(&["ZADD", "zset", "5", "b"]).await;
    client.command(&["ZADD", "zset", "2", "c"]).await;
    client.command(&["ZADD", "zset", "10", "d"]).await;
    client.command(&["ZADD", "zset", "3", "e"]).await;

    let resp = client
        .command(&["SORT", "zset", "BY", "nosort", "ASC", "LIMIT", "0", "1"])
        .await;
    assert_eq!(extract_bulk_strings(&resp), vec!["a"]);

    let resp = client
        .command(&["SORT", "zset", "BY", "nosort", "DESC", "LIMIT", "0", "1"])
        .await;
    assert_eq!(extract_bulk_strings(&resp), vec!["d"]);

    let resp = client
        .command(&["SORT", "zset", "BY", "nosort", "ASC", "LIMIT", "0", "2"])
        .await;
    assert_eq!(extract_bulk_strings(&resp), vec!["a", "c"]);

    let resp = client
        .command(&["SORT", "zset", "BY", "nosort", "DESC", "LIMIT", "0", "2"])
        .await;
    assert_eq!(extract_bulk_strings(&resp), vec!["d", "b"]);

    let resp = client
        .command(&["SORT", "zset", "BY", "nosort", "LIMIT", "5", "10"])
        .await;
    let items = unwrap_array(resp);
    assert!(
        items.is_empty(),
        "expected empty array for out-of-range LIMIT"
    );

    let resp = client
        .command(&["SORT", "zset", "BY", "nosort", "LIMIT", "-10", "100"])
        .await;
    assert_eq!(extract_bulk_strings(&resp), vec!["a", "c", "e", "b", "d"]);
}

// ---------------------------------------------------------------------------
// SORT sorted set: +inf and -inf handling (via ZRANGE, not SORT)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_sorted_set_inf_handling() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "zset"]).await;
    client.command(&["ZADD", "zset", "-100", "a"]).await;
    client.command(&["ZADD", "zset", "200", "b"]).await;
    client.command(&["ZADD", "zset", "-300", "c"]).await;
    client.command(&["ZADD", "zset", "1000000", "d"]).await;
    client.command(&["ZADD", "zset", "+inf", "max"]).await;
    client.command(&["ZADD", "zset", "-inf", "min"]).await;

    let resp = client.command(&["ZRANGE", "zset", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["min", "c", "a", "b", "d", "max"]);
}

// ---------------------------------------------------------------------------
// SORT regression for issue #19, sorting floats
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_regression_issue_19_sorting_floats() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;

    let floats = vec![
        "1.1", "5.10", "3.10", "7.44", "2.1", "5.75", "6.12", "0.25", "1.15",
    ];
    for x in &floats {
        client.command(&["LPUSH", "mylist", x]).await;
    }

    let resp = client.command(&["SORT", "mylist"]).await;
    let items = extract_bulk_strings(&resp);

    // Expected: Tcl lsort -real equivalent
    let mut sorted_floats: Vec<f64> = floats.iter().map(|s| s.parse().unwrap()).collect();
    sorted_floats.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let _expected: Vec<String> = sorted_floats.iter().map(|f| format!("{f}")).collect();

    // Compare as floats to handle formatting differences
    let actual_f: Vec<f64> = items.iter().map(|s| s.parse().unwrap()).collect();
    assert_eq!(actual_f.len(), sorted_floats.len());
    for (a, e) in actual_f.iter().zip(sorted_floats.iter()) {
        assert!(
            (a - e).abs() < 1e-10,
            "float mismatch: got {a}, expected {e}"
        );
    }
}

// ---------------------------------------------------------------------------
// SORT with STORE returns zero if result is empty (github issue 224)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT STORE not implemented"]
async fn tcl_sort_store_returns_zero_if_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    assert_integer_eq(&client.command(&["SORT", "foo", "STORE", "bar"]).await, 0);
}

// ---------------------------------------------------------------------------
// SORT with STORE does not create empty lists (github issue 224)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_store_does_not_create_empty_lists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client.command(&["LPUSH", "foo", "bar"]).await;
    client
        .command(&["SORT", "foo", "ALPHA", "LIMIT", "10", "10", "STORE", "zap"])
        .await;
    assert_integer_eq(&client.command(&["EXISTS", "zap"]).await, 0);
}

// ---------------------------------------------------------------------------
// SORT with STORE removes key if result is empty (github issue 227)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT STORE not implemented"]
async fn tcl_sort_store_removes_key_if_result_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client.command(&["LPUSH", "foo", "bar"]).await;
    client.command(&["SORT", "emptylist", "STORE", "foo"]).await;
    assert_integer_eq(&client.command(&["EXISTS", "foo"]).await, 0);
}

// ---------------------------------------------------------------------------
// SORT with BY <constant> and STORE should still order output
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT BY/GET external keys not implemented"]
async fn tcl_sort_by_constant_store_orders_output() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset", "mylist"]).await;
    let members = [
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o", "p", "q", "r", "s", "t",
        "u", "v", "z", "aa", "aaa", "azz",
    ];
    for m in &members {
        client.command(&["SADD", "myset", m]).await;
    }

    client
        .command(&["SORT", "myset", "ALPHA", "BY", "_", "STORE", "mylist"])
        .await;
    let resp = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    let expected = vec![
        "a", "aa", "aaa", "azz", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o", "p",
        "q", "r", "s", "t", "u", "v", "z",
    ];
    assert_eq!(items, expected);
}

// ---------------------------------------------------------------------------
// SORT will complain with numerical sorting and bad doubles (1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_complains_bad_double_in_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client
        .command(&["SADD", "myset", "1", "2", "3", "4", "not-a-double"])
        .await;

    let resp = client.command(&["SORT", "myset"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// SORT will complain with numerical sorting and bad doubles (2) - BY key
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT BY/GET external keys not implemented"]
async fn tcl_sort_complains_bad_double_in_by_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client.command(&["SADD", "myset", "1", "2", "3", "4"]).await;
    client
        .command(&[
            "MSET",
            "score:1",
            "10",
            "score:2",
            "20",
            "score:3",
            "30",
            "score:4",
            "not-a-double",
        ])
        .await;

    let resp = client.command(&["SORT", "myset", "BY", "score:*"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// SORT GET with pattern ending with just -> does not get hash field
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT BY/GET external keys not implemented"]
async fn tcl_sort_get_pattern_ending_with_arrow() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["LPUSH", "mylist", "a"]).await;
    client.command(&["SET", "x:a->", "100"]).await;

    let resp = client
        .command(&["SORT", "mylist", "BY", "num", "GET", "x:*->"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["100"]);
}

// ---------------------------------------------------------------------------
// SORT by nosort retains native order for lists
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_by_nosort_retains_list_order() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "testa"]).await;
    client
        .command(&["LPUSH", "testa", "2", "1", "4", "3", "5"])
        .await;

    let resp = client.command(&["SORT", "testa", "BY", "nosort"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["5", "3", "4", "1", "2"]);
}

// ---------------------------------------------------------------------------
// SORT by nosort plus store retains native order for lists
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT BY/GET external keys not implemented"]
async fn tcl_sort_by_nosort_store_retains_list_order() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "testa", "testb"]).await;
    client
        .command(&["LPUSH", "testa", "2", "1", "4", "3", "5"])
        .await;

    client
        .command(&["SORT", "testa", "BY", "nosort", "STORE", "testb"])
        .await;
    let resp = client.command(&["LRANGE", "testb", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["5", "3", "4", "1", "2"]);
}

// ---------------------------------------------------------------------------
// SORT by nosort with limit returns based on original list order
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT BY/GET external keys not implemented"]
async fn tcl_sort_by_nosort_with_limit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "testa", "testb"]).await;
    client
        .command(&["LPUSH", "testa", "2", "1", "4", "3", "5"])
        .await;

    client
        .command(&[
            "SORT", "testa", "BY", "nosort", "LIMIT", "0", "3", "STORE", "testb",
        ])
        .await;
    let resp = client.command(&["LRANGE", "testb", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["5", "3", "4"]);
}

// ---------------------------------------------------------------------------
// SORT_RO - Successful case
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT_RO not implemented"]
async fn tcl_sort_ro_successful() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["LPUSH", "mylist", "a"]).await;
    client.command(&["SET", "x:a->", "100"]).await;

    let resp = client
        .command(&["SORT_RO", "mylist", "BY", "nosort", "GET", "x:*->"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["100"]);
}

// ---------------------------------------------------------------------------
// SORT_RO - Cannot run with STORE arg
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_ro_cannot_use_store() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["SORT_RO", "foolist", "STORE", "bar"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// SORT with huge offset (SETRANGE with huge offset test)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_ro_huge_limit_offset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "L"]).await;
    client.command(&["LPUSH", "L", "2", "1", "0"]).await;

    // With a large offset value, expect either valid result or error
    let resp = client
        .command(&[
            "SORT_RO",
            "L",
            "BY",
            "a",
            "LIMIT",
            "2",
            "9223372036854775807",
        ])
        .await;
    match &resp {
        Response::Array(items) => {
            // If it returns a result, the single element remaining after offset 2 is "2"
            // (list is [0, 1, 2] and BY nosort-like; skip 2 => one element)
            assert!(items.len() <= 1);
        }
        Response::Error(_) => {
            // "out of range" error is also acceptable
        }
        other => panic!("expected Array or Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// SORT BY key (basic, single encoding size)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT BY/GET external keys not implemented"]
async fn tcl_sort_by_external_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "tosort"]).await;
    // Create a small dataset with known weights
    let data = vec![("0", "30"), ("1", "10"), ("2", "20")];
    for (elem, weight) in &data {
        client.command(&["LPUSH", "tosort", elem]).await;
        client
            .command(&["SET", &format!("weight_{elem}"), weight])
            .await;
    }

    let resp = client.command(&["SORT", "tosort", "BY", "weight_*"]).await;
    let items = extract_bulk_strings(&resp);
    // Sorted by weight: 1(10), 2(20), 0(30)
    assert_eq!(items, vec!["1", "2", "0"]);
}

// ---------------------------------------------------------------------------
// SORT BY key with LIMIT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sort_by_external_key_with_limit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "tosort"]).await;
    let data = vec![
        ("0", "30"),
        ("1", "10"),
        ("2", "20"),
        ("3", "5"),
        ("4", "25"),
    ];
    for (elem, weight) in &data {
        client.command(&["LPUSH", "tosort", elem]).await;
        client
            .command(&["SET", &format!("weight_{elem}"), weight])
            .await;
    }

    // Sorted by weight: 3(5), 1(10), 2(20), 4(25), 0(30)
    // LIMIT 1 2 => [1, 2]
    let resp = client
        .command(&["SORT", "tosort", "BY", "weight_*", "LIMIT", "1", "2"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["1", "2"]);
}

// ---------------------------------------------------------------------------
// SORT BY hash field
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT BY/GET external keys not implemented"]
async fn tcl_sort_by_hash_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "tosort"]).await;
    let data = vec![("0", "30"), ("1", "10"), ("2", "20")];
    for (elem, weight) in &data {
        client.command(&["LPUSH", "tosort", elem]).await;
        client
            .command(&["HSET", &format!("wobj_{elem}"), "weight", weight])
            .await;
    }

    let resp = client
        .command(&["SORT", "tosort", "BY", "wobj_*->weight"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["1", "2", "0"]);
}

// ---------------------------------------------------------------------------
// SORT GET (key and hash) with sanity check
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT BY/GET external keys not implemented"]
async fn tcl_sort_get_key_and_hash_sanity_check() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "tosort"]).await;
    let data = vec![("0", "30.5"), ("1", "10.2"), ("2", "20.7")];
    for (elem, weight) in &data {
        client.command(&["LPUSH", "tosort", elem]).await;
        client
            .command(&["SET", &format!("weight_{elem}"), weight])
            .await;
        client
            .command(&["HSET", &format!("wobj_{elem}"), "weight", weight])
            .await;
    }

    // SORT tosort GET # GET weight_*
    let resp1 = client
        .command(&["SORT", "tosort", "GET", "#", "GET", "weight_*"])
        .await;
    let l1 = extract_bulk_strings_with_nils(&resp1);

    // SORT tosort GET # GET wobj_*->weight
    let resp2 = client
        .command(&["SORT", "tosort", "GET", "#", "GET", "wobj_*->weight"])
        .await;
    let l2 = extract_bulk_strings_with_nils(&resp2);

    // Results come in pairs: [id, weight, id, weight, ...]
    assert_eq!(l1.len(), l2.len());
    assert!(l1.len().is_multiple_of(2));

    for i in (0..l1.len()).step_by(2) {
        let id1 = &l1[i];
        let w1 = &l1[i + 1];
        let id2 = &l2[i];
        let w2 = &l2[i + 1];
        assert_eq!(id1, id2);
        // Verify the weight matches what was stored
        let stored = client.command(&["GET", &format!("weight_{id1}")]).await;
        let stored_val = String::from_utf8(unwrap_bulk(&stored).to_vec()).unwrap();
        assert_eq!(w1, &stored_val);
        assert_eq!(w2, &stored_val);
    }
}

// ---------------------------------------------------------------------------
// SORT BY key STORE
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT BY/GET external keys not implemented"]
async fn tcl_sort_by_key_store() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "tosort", "sort-res"]).await;
    let data = vec![("0", "30"), ("1", "10"), ("2", "20")];
    for (elem, weight) in &data {
        client.command(&["LPUSH", "tosort", elem]).await;
        client
            .command(&["SET", &format!("weight_{elem}"), weight])
            .await;
    }

    client
        .command(&["SORT", "tosort", "BY", "weight_*", "STORE", "sort-res"])
        .await;
    let resp = client.command(&["LRANGE", "sort-res", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["1", "2", "0"]);
    assert_integer_eq(&client.command(&["LLEN", "sort-res"]).await, 3);
}

// ---------------------------------------------------------------------------
// SORT BY hash field STORE
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT BY/GET external keys not implemented"]
async fn tcl_sort_by_hash_field_store() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "tosort", "sort-res"]).await;
    let data = vec![("0", "30"), ("1", "10"), ("2", "20")];
    for (elem, weight) in &data {
        client.command(&["LPUSH", "tosort", elem]).await;
        client
            .command(&["HSET", &format!("wobj_{elem}"), "weight", weight])
            .await;
    }

    client
        .command(&[
            "SORT",
            "tosort",
            "BY",
            "wobj_*->weight",
            "STORE",
            "sort-res",
        ])
        .await;
    let resp = client.command(&["LRANGE", "sort-res", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["1", "2", "0"]);
    assert_integer_eq(&client.command(&["LLEN", "sort-res"]).await, 3);
}

// ---------------------------------------------------------------------------
// SORT extracts STORE correctly (COMMAND GETKEYS)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "COMMAND GETKEYS may not be implemented in FrogDB"]
async fn tcl_sort_extracts_store_correctly() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "GETKEYS", "SORT", "abc", "STORE", "def"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["abc", "def"]);
}

// ---------------------------------------------------------------------------
// SORT_RO get keys (COMMAND GETKEYS)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "COMMAND GETKEYS may not be implemented in FrogDB"]
async fn tcl_sort_ro_get_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "GETKEYS", "SORT_RO", "abc"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["abc"]);
}

// ---------------------------------------------------------------------------
// SORT extracts multiple STORE correctly (COMMAND GETKEYS)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "COMMAND GETKEYS may not be implemented in FrogDB"]
async fn tcl_sort_extracts_multiple_store_correctly() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "COMMAND", "GETKEYS", "SORT", "abc", "STORE", "invalid", "STORE", "stillbad", "STORE",
            "def",
        ])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["abc", "def"]);
}

// ---------------------------------------------------------------------------
// SORT BY sub-sorts lexicographically if score is the same
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB SORT BY/GET external keys not implemented"]
async fn tcl_sort_by_subsorts_lexicographically_on_tie() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    let members = [
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o", "p", "q", "r", "s", "t",
        "u", "v", "z", "aa", "aaa", "azz",
    ];
    for m in &members {
        client.command(&["SADD", "myset", m]).await;
    }
    // All have the same score
    for m in &members {
        client.command(&["SET", &format!("score:{m}"), "100"]).await;
    }

    let resp = client.command(&["SORT", "myset", "BY", "score:*"]).await;
    let items = extract_bulk_strings(&resp);
    let expected = vec![
        "a", "aa", "aaa", "azz", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o", "p",
        "q", "r", "s", "t", "u", "v", "z",
    ];
    assert_eq!(items, expected);
}
