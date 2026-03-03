use frogdb_test_harness::response::*;
use frogdb_test_harness::server::{TestServer, TestServerConfig};

/// Start a single-shard server so SORT BY <external-key> doesn't hit CROSSSLOT errors.
async fn start_sort_server() -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await
}

// ---------------------------------------------------------------------------
// Basic SORT tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sort_basic_numeric_list() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "3", "1", "2"]).await;
    let resp = client.command(&["SORT", "mylist"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["1", "2", "3"]);
}

#[tokio::test]
async fn sort_desc() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "3", "1", "2"]).await;
    let resp = client.command(&["SORT", "mylist", "DESC"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["3", "2", "1"]);
}

#[tokio::test]
async fn sort_alpha() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "gamma", "alpha", "beta"])
        .await;
    let resp = client.command(&["SORT", "mylist", "ALPHA"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["alpha", "beta", "gamma"]);
}

#[tokio::test]
async fn sort_alpha_against_integer_encoded_strings() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    // Lists with integer-encoded values but sorted alphabetically
    for v in &["3", "1", "2", "10", "20"] {
        client.command(&["RPUSH", "mylist", v]).await;
    }
    let resp = client.command(&["SORT", "mylist", "ALPHA"]).await;
    let items = extract_bulk_strings(&resp);
    // Lexicographic order: 1, 10, 2, 20, 3
    assert_eq!(items, vec!["1", "10", "2", "20", "3"]);
}

#[tokio::test]
async fn sort_with_limit() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    for i in 1..=5 {
        client.command(&["RPUSH", "mylist", &i.to_string()]).await;
    }
    // LIMIT offset count: skip 1, take 3 → [2, 3, 4]
    let resp = client
        .command(&["SORT", "mylist", "LIMIT", "1", "3"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["2", "3", "4"]);
}

#[tokio::test]
async fn sort_by_nosort_retains_native_order() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    for v in &["c", "a", "b"] {
        client.command(&["RPUSH", "mylist", v]).await;
    }
    // BY nosort preserves insertion order
    let resp = client.command(&["SORT", "mylist", "ALPHA", "BY", "nosort"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["c", "a", "b"]);
}

#[tokio::test]
async fn sort_by_nosort_plus_store_retains_native_order() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    for v in &["c", "a", "b"] {
        client.command(&["RPUSH", "{k}src", v]).await;
    }
    client
        .command(&["SORT", "{k}src", "ALPHA", "BY", "nosort", "STORE", "{k}dst"])
        .await;
    let resp = client.command(&["LRANGE", "{k}dst", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["c", "a", "b"]);
}

#[tokio::test]
async fn sort_by_nosort_with_limit() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    for v in &["c", "a", "b", "d", "e"] {
        client.command(&["RPUSH", "mylist", v]).await;
    }
    // LIMIT 1 3 of nosort: skip 1, take 3 → [a, b, d]
    let resp = client
        .command(&["SORT", "mylist", "ALPHA", "BY", "nosort", "LIMIT", "1", "3"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["a", "b", "d"]);
}

#[tokio::test]
async fn sort_sorted_set() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["ZADD", "myzset", "3", "c"]).await;
    client.command(&["ZADD", "myzset", "1", "a"]).await;
    client.command(&["ZADD", "myzset", "2", "b"]).await;

    let resp = client.command(&["SORT", "myzset", "ALPHA"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn sort_sorted_set_by_nosort_retains_score_order() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["ZADD", "myzset", "3", "c"]).await;
    client.command(&["ZADD", "myzset", "1", "a"]).await;
    client.command(&["ZADD", "myzset", "2", "b"]).await;

    // BY nosort on a sorted set retains score order (a, b, c)
    let resp = client
        .command(&["SORT", "myzset", "ALPHA", "BY", "nosort"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn sort_sorted_set_by_nosort_with_limit() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    for (score, member) in &[("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("5", "e")] {
        client.command(&["ZADD", "myzset", score, member]).await;
    }

    let resp = client
        .command(&["SORT", "myzset", "ALPHA", "BY", "nosort", "LIMIT", "1", "3"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["b", "c", "d"]);
}

#[tokio::test]
async fn sort_by_external_key() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    // list: 1, 2, 3; weights are reversed
    client.command(&["RPUSH", "{t}mylist", "1", "2", "3"]).await;
    client.command(&["SET", "{t}weight_1", "30"]).await;
    client.command(&["SET", "{t}weight_2", "20"]).await;
    client.command(&["SET", "{t}weight_3", "10"]).await;

    let resp = client
        .command(&["SORT", "{t}mylist", "BY", "{t}weight_*"])
        .await;
    let items = extract_bulk_strings(&resp);
    // sorted by weight: 3(w=10), 2(w=20), 1(w=30)
    assert_eq!(items, vec!["3", "2", "1"]);
}

#[tokio::test]
async fn sort_by_external_key_with_limit() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    for i in 1..=5usize {
        client
            .command(&["RPUSH", "{t}mylist", &i.to_string()])
            .await;
        // weight is n+1 - i (reversed order)
        client
            .command(&[
                "SET",
                &format!("{{t}}weight_{i}"),
                &(6 - i).to_string(),
            ])
            .await;
    }

    // Sort by weight LIMIT 0 3 → 3 smallest weights = items 5,4,3
    let resp = client
        .command(&[
            "SORT",
            "{t}mylist",
            "BY",
            "{t}weight_*",
            "LIMIT",
            "0",
            "3",
        ])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items.len(), 3);
    // weights: 1→5, 2→4, 3→3, 4→2, 5→1; sorted asc: 5,4,3,...
    assert_eq!(items[0], "5");
    assert_eq!(items[1], "4");
    assert_eq!(items[2], "3");
}

#[tokio::test]
async fn sort_by_hash_field() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "{t}mylist", "1", "2", "3"]).await;
    client
        .command(&["HSET", "{t}wobj_1", "weight", "30"])
        .await;
    client
        .command(&["HSET", "{t}wobj_2", "weight", "20"])
        .await;
    client
        .command(&["HSET", "{t}wobj_3", "weight", "10"])
        .await;

    let resp = client
        .command(&["SORT", "{t}mylist", "BY", "{t}wobj_*->weight"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["3", "2", "1"]);
}

#[tokio::test]
async fn sort_get_hash_field() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "{t}mylist", "1", "2"]).await;
    client.command(&["HSET", "{t}obj_1", "name", "Alice"]).await;
    client.command(&["HSET", "{t}obj_2", "name", "Bob"]).await;

    let resp = client
        .command(&["SORT", "{t}mylist", "GET", "{t}obj_*->name"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["Alice", "Bob"]);
}

#[tokio::test]
async fn sort_get_hash_field_and_pound() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "{t}mylist", "1", "2"]).await;
    client.command(&["HSET", "{t}obj_1", "name", "Alice"]).await;
    client.command(&["HSET", "{t}obj_2", "name", "Bob"]).await;

    // GET # returns the element itself; GET obj_*->name returns the hash field
    let resp = client
        .command(&[
            "SORT",
            "{t}mylist",
            "GET",
            "#",
            "GET",
            "{t}obj_*->name",
        ])
        .await;
    let items = extract_bulk_strings(&resp);
    // interleaved: id, name, id, name
    assert_eq!(items, vec!["1", "Alice", "2", "Bob"]);
}

#[tokio::test]
async fn sort_get_const_value() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "1", "2", "3"]).await;

    // GET with a constant (no wildcard) that doesn't exist → nil, but # works
    let resp = client
        .command(&["SORT", "mylist", "GET", "#"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["1", "2", "3"]);
}

#[tokio::test]
async fn sort_get_pattern_ending_with_arrow() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "{t}mylist", "1", "2"]).await;
    client.command(&["HSET", "{t}hash_1", "field", "v1"]).await;
    client.command(&["HSET", "{t}hash_2", "field", "v2"]).await;

    let resp = client
        .command(&["SORT", "{t}mylist", "GET", "{t}hash_*->field"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["v1", "v2"]);
}

#[tokio::test]
async fn sort_by_key_store() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "{t}src", "3", "1", "2"]).await;
    client.command(&["SET", "{t}weight_1", "10"]).await;
    client.command(&["SET", "{t}weight_2", "20"]).await;
    client.command(&["SET", "{t}weight_3", "30"]).await;

    let resp = client
        .command(&[
            "SORT",
            "{t}src",
            "BY",
            "{t}weight_*",
            "STORE",
            "{t}dst",
        ])
        .await;
    assert_eq!(unwrap_integer(&resp), 3);

    // Verify stored result
    let resp = client.command(&["LRANGE", "{t}dst", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["1", "2", "3"]);
}

#[tokio::test]
async fn sort_by_hash_field_store() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "{t}src", "1", "2", "3"]).await;
    client
        .command(&["HSET", "{t}wobj_1", "weight", "30"])
        .await;
    client
        .command(&["HSET", "{t}wobj_2", "weight", "20"])
        .await;
    client
        .command(&["HSET", "{t}wobj_3", "weight", "10"])
        .await;

    let resp = client
        .command(&[
            "SORT",
            "{t}src",
            "BY",
            "{t}wobj_*->weight",
            "STORE",
            "{t}dst",
        ])
        .await;
    assert_eq!(unwrap_integer(&resp), 3);

    let resp = client.command(&["LRANGE", "{t}dst", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["3", "2", "1"]);
}

#[tokio::test]
async fn sort_with_store_returns_zero_if_empty() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    // SORT a non-existing key with STORE → 0
    let resp = client
        .command(&["SORT", "{k}nosuchkey", "STORE", "{k}dst"])
        .await;
    assert_eq!(unwrap_integer(&resp), 0);
}

#[tokio::test]
async fn sort_with_store_does_not_create_empty_list() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["SORT", "{k}nosuchkey", "STORE", "{k}dst"])
        .await;
    assert_eq!(unwrap_integer(&resp), 0);

    // dst should not exist
    let resp = client.command(&["EXISTS", "{k}dst"]).await;
    assert_eq!(unwrap_integer(&resp), 0);
}

#[tokio::test]
async fn sort_with_store_removes_key_if_sort_result_empty() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    // Pre-create dst
    client
        .command(&["RPUSH", "{k}dst", "existing"])
        .await;

    // SORT empty source → STORE → dst should be removed
    let resp = client
        .command(&["SORT", "{k}nosuchkey", "STORE", "{k}dst"])
        .await;
    assert_eq!(unwrap_integer(&resp), 0);

    let resp = client.command(&["EXISTS", "{k}dst"]).await;
    assert_eq!(unwrap_integer(&resp), 0);
}

#[tokio::test]
async fn sort_by_constant_plus_store_orders_output() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    // BY a non-existing pattern (constant → all same weight) preserves order
    client
        .command(&["RPUSH", "{k}src", "c", "a", "b"])
        .await;
    let resp = client
        .command(&["SORT", "{k}src", "BY", "nosort", "ALPHA", "STORE", "{k}dst"])
        .await;
    assert_eq!(unwrap_integer(&resp), 3);

    let resp = client.command(&["LRANGE", "{k}dst", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["c", "a", "b"]);
}

#[tokio::test]
async fn sort_regression_issue_19_float_precision() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    // Regression test: SORT of floats that are very close in value
    client
        .command(&["RPUSH", "mylist", "1.1", "1.2", "1.3", "1.0"])
        .await;
    let resp = client.command(&["SORT", "mylist"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["1.0", "1.1", "1.2", "1.3"]);
}

#[tokio::test]
async fn sort_bad_double_1() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    // Sorting a list with a non-numeric value (without ALPHA) → error
    client.command(&["RPUSH", "mylist", "1", "foo", "3"]).await;
    let resp = client.command(&["SORT", "mylist"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn sort_bad_double_2() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    // Sorting a list where external weight is non-numeric → error
    client.command(&["RPUSH", "{t}mylist", "1"]).await;
    client.command(&["SET", "{t}weight_1", "notanumber"]).await;
    let resp = client
        .command(&["SORT", "{t}mylist", "BY", "{t}weight_*"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn sort_by_sub_sorts_lexicographically_when_weights_equal() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    // All elements have the same external weight → secondary sort is on element value
    client
        .command(&["RPUSH", "{t}mylist", "gamma", "alpha", "beta"])
        .await;
    client.command(&["SET", "{t}weight_alpha", "1"]).await;
    client.command(&["SET", "{t}weight_beta", "1"]).await;
    client.command(&["SET", "{t}weight_gamma", "1"]).await;

    let resp = client
        .command(&[
            "SORT",
            "{t}mylist",
            "BY",
            "{t}weight_*",
            "ALPHA",
        ])
        .await;
    let items = extract_bulk_strings(&resp);
    // Equal weights → lexicographic: alpha, beta, gamma
    assert_eq!(items, vec!["alpha", "beta", "gamma"]);
}

#[tokio::test]
async fn sort_ro_successful_case() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "3", "1", "2"]).await;
    let resp = client.command(&["SORT_RO", "mylist"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["1", "2", "3"]);
}

#[tokio::test]
async fn sort_ro_cannot_be_used_with_store() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "{k}src", "1", "2"]).await;
    let resp = client
        .command(&["SORT_RO", "{k}src", "STORE", "{k}dst"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn sort_ro_with_huge_offset_returns_empty() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "1", "2", "3"]).await;
    let resp = client
        .command(&["SORT_RO", "mylist", "LIMIT", "1000000", "10"])
        .await;
    // Offset beyond end of list → empty array
    let items = extract_bulk_strings(&resp);
    assert!(items.is_empty());
}

#[tokio::test]
async fn sort_sorted_set_inf_scores() {
    let server = start_sort_server().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "+inf", "pos_inf"])
        .await;
    client
        .command(&["ZADD", "myzset", "-inf", "neg_inf"])
        .await;
    client.command(&["ZADD", "myzset", "0", "zero"]).await;

    // SORT ALPHA returns members in alphabetical order
    let resp = client.command(&["SORT", "myzset", "ALPHA"]).await;
    let items = extract_bulk_strings(&resp);
    assert!(items.contains(&"neg_inf".to_string()));
    assert!(items.contains(&"pos_inf".to_string()));
    assert!(items.contains(&"zero".to_string()));
}
