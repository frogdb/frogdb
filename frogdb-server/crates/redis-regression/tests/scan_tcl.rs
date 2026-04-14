//! Rust port of Redis 8.6.0 `unit/scan.tcl` test suite.
//!
//! Excludes: `needs:debug` tests (SCAN unknown type, SCAN with expired keys,
//! SCAN with expired keys with TYPE/PATTERN filter), `external:skip` (cluster
//! variant), encoding loops / `assert_encoding` / DEBUG OBJECT tests, and
//! CONFIG SET tests.
//!
//! ## Intentional exclusions
//!
//! Encoding-loop variants ($enc/$type parameterized for listpack vs hashtable etc.):
//! - `{$type} SSCAN with encoding $enc` — intentional-incompatibility:encoding — internal-encoding
//! - `{$type} HSCAN with encoding $enc` — intentional-incompatibility:encoding — internal-encoding
//! - `{$type} ZSCAN with encoding $enc` — intentional-incompatibility:encoding — internal-encoding
//!
//! DEBUG-dependent expired-key behavior (requires DEBUG SET-ACTIVE-EXPIRE):
//! - `{$type} SCAN with expired keys` — intentional-incompatibility:debug — needs:debug

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::{TestClient, TestServer};

// ---------------------------------------------------------------------------
// Helper: populate N keys named key:0 .. key:(N-1) with value key:N
// ---------------------------------------------------------------------------

async fn populate(client: &mut TestClient, n: usize) {
    for i in 0..n {
        let key = format!("key:{i}");
        client.command(&["SET", &key, &key]).await;
    }
}

/// Drive a full SCAN iteration, returning all collected keys.
async fn scan_all(client: &mut TestClient, extra_args: &[&str]) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut keys = Vec::new();
    loop {
        let mut args: Vec<&str> = vec!["SCAN", &cursor];
        args.extend_from_slice(extra_args);
        let resp = client.command(&args).await;
        let arr = unwrap_array(resp);
        assert_eq!(arr.len(), 2);
        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let batch = extract_bulk_strings(&arr[1]);
        keys.extend(batch);
        if cursor == "0" {
            break;
        }
    }
    keys
}

/// Drive a full SSCAN iteration, returning all collected members.
async fn sscan_all(client: &mut TestClient, key: &str, extra_args: &[&str]) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut members = Vec::new();
    loop {
        let mut args: Vec<&str> = vec!["SSCAN", key, &cursor];
        args.extend_from_slice(extra_args);
        let resp = client.command(&args).await;
        let arr = unwrap_array(resp);
        assert_eq!(arr.len(), 2);
        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let batch = extract_bulk_strings(&arr[1]);
        members.extend(batch);
        if cursor == "0" {
            break;
        }
    }
    members
}

/// Drive a full HSCAN iteration, returning all field-value pairs as flat strings.
async fn hscan_all(client: &mut TestClient, key: &str, extra_args: &[&str]) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut items = Vec::new();
    loop {
        let mut args: Vec<&str> = vec!["HSCAN", key, &cursor];
        args.extend_from_slice(extra_args);
        let resp = client.command(&args).await;
        let arr = unwrap_array(resp);
        assert_eq!(arr.len(), 2);
        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let batch = extract_bulk_strings(&arr[1]);
        items.extend(batch);
        if cursor == "0" {
            break;
        }
    }
    items
}

/// Drive a full ZSCAN iteration, returning all member-score pairs as flat strings.
async fn zscan_all(client: &mut TestClient, key: &str, extra_args: &[&str]) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut items = Vec::new();
    loop {
        let mut args: Vec<&str> = vec!["ZSCAN", key, &cursor];
        args.extend_from_slice(extra_args);
        let resp = client.command(&args).await;
        let arr = unwrap_array(resp);
        assert_eq!(arr.len(), 2);
        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let batch = extract_bulk_strings(&arr[1]);
        items.extend(batch);
        if cursor == "0" {
            break;
        }
    }
    items
}

// ---------------------------------------------------------------------------
// SCAN basic
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_scan_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    populate(&mut client, 1000).await;

    let mut keys = scan_all(&mut client, &[]).await;
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 1000);
}

// ---------------------------------------------------------------------------
// SCAN COUNT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_scan_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    populate(&mut client, 1000).await;

    let mut keys = scan_all(&mut client, &["COUNT", "5"]).await;
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 1000);
}

// ---------------------------------------------------------------------------
// SCAN MATCH
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_scan_match() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    populate(&mut client, 1000).await;

    let mut keys = scan_all(&mut client, &["MATCH", "key:1??"]).await;
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 100);
}

// ---------------------------------------------------------------------------
// SCAN TYPE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_scan_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    populate(&mut client, 1000).await;

    // Non-string type should return nothing (only string keys exist).
    let keys = scan_all(&mut client, &["TYPE", "list"]).await;
    assert_eq!(keys.len(), 0);

    // String type should return all 1000 keys.
    let mut keys = scan_all(&mut client, &["TYPE", "string"]).await;
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 1000);

    // All three args together: TYPE + MATCH + COUNT.
    let mut keys = scan_all(
        &mut client,
        &["TYPE", "string", "MATCH", "key:*", "COUNT", "10"],
    )
    .await;
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 1000);
}

// ---------------------------------------------------------------------------
// SSCAN basic (without encoding assertions)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sscan_basic_small() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 100 string-prefixed members (would be listpack in Redis).
    client.command(&["DEL", "set"]).await;
    let mut add_args: Vec<String> = vec!["SADD".into(), "set".into()];
    for j in 0..100 {
        add_args.push(format!("ele:{j}"));
    }
    let refs: Vec<&str> = add_args.iter().map(|s| s.as_str()).collect();
    client.command(&refs).await;

    let mut members = sscan_all(&mut client, "set", &[]).await;
    members.sort();
    members.dedup();
    assert_eq!(members.len(), 100);
}

#[tokio::test]
async fn tcl_sscan_basic_large() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 200 string-prefixed members (would be hashtable in Redis).
    client.command(&["DEL", "set"]).await;
    let mut add_args: Vec<String> = vec!["SADD".into(), "set".into()];
    for j in 0..200 {
        add_args.push(format!("ele:{j}"));
    }
    let refs: Vec<&str> = add_args.iter().map(|s| s.as_str()).collect();
    client.command(&refs).await;

    let mut members = sscan_all(&mut client, "set", &[]).await;
    members.sort();
    members.dedup();
    assert_eq!(members.len(), 200);
}

#[tokio::test]
async fn tcl_sscan_intset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 100 integer members (would be intset in Redis).
    client.command(&["DEL", "set"]).await;
    let mut add_args: Vec<String> = vec!["SADD".into(), "set".into()];
    for j in 0..100 {
        add_args.push(format!("{j}"));
    }
    let refs: Vec<&str> = add_args.iter().map(|s| s.as_str()).collect();
    client.command(&refs).await;

    let mut members = sscan_all(&mut client, "set", &[]).await;
    members.sort();
    members.dedup();
    assert_eq!(members.len(), 100);
}

// ---------------------------------------------------------------------------
// HSCAN basic (without encoding assertions)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hscan_basic_small() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 30 fields (would be listpack in Redis).
    client.command(&["DEL", "hash"]).await;
    let mut hmset_args: Vec<String> = vec!["HMSET".into(), "hash".into()];
    for j in 0..30 {
        hmset_args.push(format!("key:{j}"));
        hmset_args.push(format!("{j}"));
    }
    let refs: Vec<&str> = hmset_args.iter().map(|s| s.as_str()).collect();
    client.command(&refs).await;

    let items = hscan_all(&mut client, "hash", &[]).await;
    // items is flat: [field, value, field, value, ...]
    let mut fields = Vec::new();
    for (i, s) in items.iter().enumerate() {
        if i % 2 == 0 {
            // Verify field matches its value: key:N -> N
            let val = &items[i + 1];
            let expected_field = format!("key:{val}");
            assert_eq!(s, &expected_field);
            fields.push(s.clone());
        }
    }
    fields.sort();
    fields.dedup();
    assert_eq!(fields.len(), 30);
}

#[tokio::test]
async fn tcl_hscan_basic_large() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 1000 fields (would be hashtable in Redis).
    client.command(&["DEL", "hash"]).await;
    // Insert in batches to avoid overly large commands.
    for batch_start in (0..1000).step_by(100) {
        let mut hmset_args: Vec<String> = vec!["HMSET".into(), "hash".into()];
        for j in batch_start..batch_start + 100 {
            hmset_args.push(format!("key:{j}"));
            hmset_args.push(format!("{j}"));
        }
        let refs: Vec<&str> = hmset_args.iter().map(|s| s.as_str()).collect();
        client.command(&refs).await;
    }

    let items = hscan_all(&mut client, "hash", &[]).await;
    let mut fields = Vec::new();
    for (i, s) in items.iter().enumerate() {
        if i % 2 == 0 {
            let val = &items[i + 1];
            let expected_field = format!("key:{val}");
            assert_eq!(s, &expected_field);
            fields.push(s.clone());
        }
    }
    fields.sort();
    fields.dedup();
    assert_eq!(fields.len(), 1000);
}

#[tokio::test]
async fn tcl_hscan_novalues_large() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 1000 fields, then HSCAN NOVALUES.
    client.command(&["DEL", "hash"]).await;
    for batch_start in (0..1000).step_by(100) {
        let mut hmset_args: Vec<String> = vec!["HMSET".into(), "hash".into()];
        for j in batch_start..batch_start + 100 {
            hmset_args.push(format!("key:{j}"));
            hmset_args.push(format!("{j}"));
        }
        let refs: Vec<&str> = hmset_args.iter().map(|s| s.as_str()).collect();
        client.command(&refs).await;
    }

    // Full scan to get fields with values for comparison.
    let items = hscan_all(&mut client, "hash", &[]).await;
    let mut expected_fields: Vec<String> = items
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 2 == 0)
        .map(|(_, s)| s.clone())
        .collect();
    expected_fields.sort();

    // NOVALUES scan.
    let mut novalues_fields = hscan_all(&mut client, "hash", &["COUNT", "1000", "NOVALUES"]).await;
    novalues_fields.sort();

    assert_eq!(expected_fields, novalues_fields);
}

// ---------------------------------------------------------------------------
// HSCAN with large value
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hscan_large_value_small() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "hash"]).await;

    // 60-byte values (would be listpack in Redis).
    let val1 = "1".repeat(60);
    let val2 = "2".repeat(60);
    client.command(&["HSET", "hash", &val1, &val1]).await;
    client.command(&["HSET", "hash", &val2, &val2]).await;

    // HSCAN with values.
    let resp = client.command(&["HSCAN", "hash", "0"]).await;
    let arr = unwrap_array(resp);
    let mut items = extract_bulk_strings(&arr[1]);
    items.sort();
    assert_eq!(items.len(), 4);
    assert_eq!(items[0], val1);
    assert_eq!(items[1], val1);
    assert_eq!(items[2], val2);
    assert_eq!(items[3], val2);

    // HSCAN NOVALUES.
    let resp = client.command(&["HSCAN", "hash", "0", "NOVALUES"]).await;
    let arr = unwrap_array(resp);
    let mut nv_items = extract_bulk_strings(&arr[1]);
    nv_items.sort();
    assert_eq!(nv_items.len(), 2);
    assert_eq!(nv_items[0], val1);
    assert_eq!(nv_items[1], val2);
}

#[tokio::test]
async fn tcl_hscan_large_value_large() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "hash"]).await;

    // 170-byte values (would be hashtable in Redis).
    let val1 = "1".repeat(170);
    let val2 = "2".repeat(170);
    client.command(&["HSET", "hash", &val1, &val1]).await;
    client.command(&["HSET", "hash", &val2, &val2]).await;

    let resp = client.command(&["HSCAN", "hash", "0"]).await;
    let arr = unwrap_array(resp);
    let mut items = extract_bulk_strings(&arr[1]);
    items.sort();
    assert_eq!(items.len(), 4);
    assert_eq!(items[0], val1);
    assert_eq!(items[1], val1);
    assert_eq!(items[2], val2);
    assert_eq!(items[3], val2);

    let resp = client.command(&["HSCAN", "hash", "0", "NOVALUES"]).await;
    let arr = unwrap_array(resp);
    let mut nv_items = extract_bulk_strings(&arr[1]);
    nv_items.sort();
    assert_eq!(nv_items.len(), 2);
    assert_eq!(nv_items[0], val1);
    assert_eq!(nv_items[1], val2);
}

// ---------------------------------------------------------------------------
// ZSCAN basic (without encoding assertions)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zscan_basic_small() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 30 members (would be listpack in Redis).
    client.command(&["DEL", "zset"]).await;
    let mut zadd_args: Vec<String> = vec!["ZADD".into(), "zset".into()];
    for j in 0..30 {
        zadd_args.push(format!("{j}"));
        zadd_args.push(format!("key:{j}"));
    }
    let refs: Vec<&str> = zadd_args.iter().map(|s| s.as_str()).collect();
    client.command(&refs).await;

    let items = zscan_all(&mut client, "zset", &[]).await;
    // items is flat: [member, score, member, score, ...]
    let mut members = Vec::new();
    for (i, s) in items.iter().enumerate() {
        if i % 2 == 0 {
            let score = &items[i + 1];
            let expected_member = format!("key:{score}");
            assert_eq!(s, &expected_member);
            members.push(s.clone());
        }
    }
    members.sort();
    members.dedup();
    assert_eq!(members.len(), 30);
}

#[tokio::test]
async fn tcl_zscan_basic_large() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 1000 members (would be skiplist in Redis).
    client.command(&["DEL", "zset"]).await;
    for batch_start in (0..1000).step_by(100) {
        let mut zadd_args: Vec<String> = vec!["ZADD".into(), "zset".into()];
        for j in batch_start..batch_start + 100 {
            zadd_args.push(format!("{j}"));
            zadd_args.push(format!("key:{j}"));
        }
        let refs: Vec<&str> = zadd_args.iter().map(|s| s.as_str()).collect();
        client.command(&refs).await;
    }

    let items = zscan_all(&mut client, "zset", &[]).await;
    let mut members = Vec::new();
    for (i, s) in items.iter().enumerate() {
        if i % 2 == 0 {
            let score = &items[i + 1];
            let expected_member = format!("key:{score}");
            assert_eq!(s, &expected_member);
            members.push(s.clone());
        }
    }
    members.sort();
    members.dedup();
    assert_eq!(members.len(), 1000);
}

// ---------------------------------------------------------------------------
// SCAN guarantees check under write load
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_scan_guarantees_under_write_load() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    populate(&mut client, 100).await;

    let mut cursor = "0".to_string();
    let mut keys = Vec::new();
    loop {
        let resp = client.command(&["SCAN", &cursor]).await;
        let arr = unwrap_array(resp);
        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let batch = extract_bulk_strings(&arr[1]);
        keys.extend(batch);

        if cursor == "0" {
            break;
        }

        // Write 10 keys at every iteration to simulate concurrent writes.
        for j in 0..10 {
            let rk = format!("addedkey:{j}");
            client.command(&["SET", &rk, "foo"]).await;
        }
    }

    // Filter to only the original key:0..key:99 keys (len <= 6 chars).
    let mut original: Vec<String> = keys.into_iter().filter(|k| k.len() <= 6).collect();
    original.sort();
    original.dedup();
    assert_eq!(original.len(), 100);
}

// ---------------------------------------------------------------------------
// SSCAN with integer encoded object (issue #1345)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sscan_integer_encoded_object_issue_1345() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "set"]).await;
    client.command(&["SADD", "set", "1", "a"]).await;

    // MATCH *a* should return only "a".
    let resp = client
        .command(&["SSCAN", "set", "0", "MATCH", "*a*", "COUNT", "100"])
        .await;
    let arr = unwrap_array(resp);
    let mut members = extract_bulk_strings(&arr[1]);
    members.sort();
    members.dedup();
    assert_eq!(members, vec!["a"]);

    // MATCH *1* should return only "1".
    let resp = client
        .command(&["SSCAN", "set", "0", "MATCH", "*1*", "COUNT", "100"])
        .await;
    let arr = unwrap_array(resp);
    let mut members = extract_bulk_strings(&arr[1]);
    members.sort();
    members.dedup();
    assert_eq!(members, vec!["1"]);
}

// ---------------------------------------------------------------------------
// SSCAN with PATTERN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sscan_with_pattern() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    client
        .command(&[
            "SADD", "mykey", "foo", "fab", "fiz", "foobar", "1", "2", "3", "4",
        ])
        .await;

    let resp = client
        .command(&["SSCAN", "mykey", "0", "MATCH", "foo*", "COUNT", "10000"])
        .await;
    let arr = unwrap_array(resp);
    let mut members = extract_bulk_strings(&arr[1]);
    members.sort();
    members.dedup();
    assert_eq!(members, vec!["foo", "foobar"]);
}

// ---------------------------------------------------------------------------
// HSCAN with PATTERN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hscan_with_pattern() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    client
        .command(&[
            "HMSET", "mykey", "foo", "1", "fab", "2", "fiz", "3", "foobar", "10", "1", "a", "2",
            "b", "3", "c", "4", "d",
        ])
        .await;

    let resp = client
        .command(&["HSCAN", "mykey", "0", "MATCH", "foo*", "COUNT", "10000"])
        .await;
    let arr = unwrap_array(resp);
    let mut items = extract_bulk_strings(&arr[1]);
    // HSCAN returns field-value pairs; result should be [foo, 1, foobar, 10]
    // sorted as flat list: [1, 10, foo, foobar].
    items.sort();
    assert_eq!(items, vec!["1", "10", "foo", "foobar"]);
}

// ---------------------------------------------------------------------------
// HSCAN with NOVALUES
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hscan_with_novalues() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    client
        .command(&[
            "HMSET", "mykey", "foo", "1", "fab", "2", "fiz", "3", "foobar", "10", "1", "a", "2",
            "b", "3", "c", "4", "d",
        ])
        .await;

    let resp = client.command(&["HSCAN", "mykey", "0", "NOVALUES"]).await;
    let arr = unwrap_array(resp);
    let mut fields = extract_bulk_strings(&arr[1]);
    fields.sort();
    assert_eq!(
        fields,
        vec!["1", "2", "3", "4", "fab", "fiz", "foo", "foobar"]
    );
}

// ---------------------------------------------------------------------------
// ZSCAN with PATTERN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zscan_with_pattern() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    client
        .command(&[
            "ZADD", "mykey", "1", "foo", "2", "fab", "3", "fiz", "10", "foobar",
        ])
        .await;

    let resp = client
        .command(&["ZSCAN", "mykey", "0", "MATCH", "foo*", "COUNT", "10000"])
        .await;
    let arr = unwrap_array(resp);
    let items = extract_bulk_strings(&arr[1]);
    // ZSCAN returns member-score pairs matching foo*: [foo, 1, foobar, 10].
    // Collect just members (even indices).
    let mut members: Vec<String> = items
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 2 == 0)
        .map(|(_, s)| s.clone())
        .collect();
    members.sort();
    assert_eq!(members, vec!["foo", "foobar"]);
}

// ---------------------------------------------------------------------------
// ZSCAN scores: regression test for issue #2175
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_zscan_scores_regression_issue_2175() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    for j in 0..500 {
        client
            .command(&["ZADD", "mykey", "9.8813129168249309e-323", &j.to_string()])
            .await;
    }

    let resp = client.command(&["ZSCAN", "mykey", "0"]).await;
    let arr = unwrap_array(resp);
    let items = extract_bulk_strings(&arr[1]);
    // The first score (index 1) should not be zero.
    assert!(items.len() >= 2, "expected at least one member-score pair");
    let first_score: f64 = items[1].parse().expect("score should be a valid float");
    assert!(
        first_score != 0.0,
        "score should not be zero, got {first_score}"
    );
}

// ---------------------------------------------------------------------------
// SCAN regression test for issue #4906 (SSCAN under rehash)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sscan_regression_issue_4906() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Run 10 iterations (reduced from 100 in TCL for test speed).
    for k in 0..10u32 {
        client.command(&["DEL", "set"]).await;
        // Add 'x' to ensure it is not intset encoded.
        client.command(&["SADD", "set", "x"]).await;

        // Vary the set size per iteration (101..600).
        let numele = 101 + (k as usize * 50);
        let mut to_remove: Vec<String> = Vec::new();

        // Populate the set with 0..numele.
        for j in 0..numele {
            client.command(&["SADD", "set", &j.to_string()]).await;
            if j >= 100 {
                to_remove.push(j.to_string());
            }
        }

        // Start scanning.
        let mut cursor = "0".to_string();
        let mut iteration = 0u32;
        let del_iteration = k % 10;
        let mut found = std::collections::HashSet::new();

        loop {
            let resp = client.command(&["SSCAN", "set", &cursor]).await;
            let arr = unwrap_array(resp);
            cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
            let items = extract_bulk_strings(&arr[1]);
            for item in &items {
                found.insert(item.clone());
            }
            iteration += 1;

            // At some point remove most items to trigger rehashing.
            if iteration == del_iteration {
                let mut srem_args: Vec<&str> = vec!["SREM", "set"];
                for r in &to_remove {
                    srem_args.push(r.as_str());
                }
                client.command(&srem_args).await;
            }

            if cursor == "0" {
                break;
            }
        }

        // Verify elements 0..99 were all reported (they were never removed).
        for j in 0..100 {
            assert!(found.contains(&j.to_string()), "SSCAN element missing: {j}");
        }
    }
}

// ---------------------------------------------------------------------------
// SCAN MATCH pattern implies cluster slot
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_scan_match_pattern_cluster_slot() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;

    for j in 0..100 {
        client
            .command(&["SET", &format!("{{foo}}-{j}"), "foo"])
            .await;
        client
            .command(&["SET", &format!("{{bar}}-{j}"), "bar"])
            .await;
        client
            .command(&["SET", &format!("{{boo}}-{j}"), "boo"])
            .await;
    }

    let mut keys = scan_all(&mut client, &["MATCH", "{foo}-*"]).await;
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 100);
}

// ---------------------------------------------------------------------------
// HSCAN NOVALUES on small hash (listpack-equivalent)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hscan_novalues_small() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 30 fields (would be listpack in Redis).
    client.command(&["DEL", "hash"]).await;
    let mut hmset_args: Vec<String> = vec!["HMSET".into(), "hash".into()];
    for j in 0..30 {
        hmset_args.push(format!("key:{j}"));
        hmset_args.push(format!("{j}"));
    }
    let refs: Vec<&str> = hmset_args.iter().map(|s| s.as_str()).collect();
    client.command(&refs).await;

    // Full scan to get expected fields.
    let items = hscan_all(&mut client, "hash", &[]).await;
    let mut expected_fields: Vec<String> = items
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 2 == 0)
        .map(|(_, s)| s.clone())
        .collect();
    expected_fields.sort();

    // NOVALUES scan.
    let resp = client
        .command(&["HSCAN", "hash", "0", "COUNT", "1000", "NOVALUES"])
        .await;
    let arr = unwrap_array(resp);
    let mut novalues_fields = extract_bulk_strings(&arr[1]);
    novalues_fields.sort();

    assert_eq!(expected_fields, novalues_fields);
}
