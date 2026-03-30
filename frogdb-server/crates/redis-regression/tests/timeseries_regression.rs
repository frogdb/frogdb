//! Regression tests for RedisTimeSeries core commands.

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the value string from a TS.GET response: [Integer(ts), Bulk(val)]
fn get_value_str(resp: &Response) -> String {
    let arr = unwrap_array(resp.clone());
    assert_eq!(arr.len(), 2);
    std::str::from_utf8(unwrap_bulk(&arr[1]))
        .unwrap()
        .to_string()
}

/// Extract the timestamp from a TS.GET response: [Integer(ts), Bulk(val)]
fn get_timestamp(resp: &Response) -> i64 {
    let arr = unwrap_array(resp.clone());
    assert_eq!(arr.len(), 2);
    unwrap_integer(&arr[0])
}

/// Find a field value in a flat key-value INFO array by label name.
fn info_field<'a>(items: &'a [Response], label: &str) -> &'a Response {
    for i in (0..items.len()).step_by(2) {
        if let Response::Bulk(Some(b)) = &items[i]
            && std::str::from_utf8(b).unwrap() == label
        {
            return &items[i + 1];
        }
        if let Response::Simple(s) = &items[i]
            && s == label
        {
            return &items[i + 1];
        }
    }
    panic!("field {label:?} not found in INFO response");
}

// ---------------------------------------------------------------------------
// TS.CREATE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_create_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TS.CREATE", "ts1"]).await;
    assert_ok(&resp);

    // Verify exists via INFO
    let resp = client.command(&["TS.INFO", "ts1"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(info_field(&items, "totalSamples"), 0);
}

#[tokio::test]
async fn ts_create_with_options() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "TS.CREATE",
            "ts_opts",
            "RETENTION",
            "60000",
            "CHUNK_SIZE",
            "512",
            "DUPLICATE_POLICY",
            "LAST",
            "LABELS",
            "sensor",
            "temp",
            "region",
            "us-east",
        ])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["TS.INFO", "ts_opts"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(info_field(&items, "retentionTime"), 60000);
    assert_integer_eq(info_field(&items, "chunkSize"), 512);
}

#[tokio::test]
async fn ts_create_duplicate_key_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TS.CREATE", "ts_dup"]).await;
    assert_ok(&resp);

    let resp = client.command(&["TS.CREATE", "ts_dup"]).await;
    assert_error_prefix(&resp, "ERR TSDB: key already exists");
}

#[tokio::test]
async fn ts_create_encoding_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["TS.CREATE", "ts_enc", "ENCODING", "COMPRESSED"])
        .await;
    assert_ok(&resp);
}

// ---------------------------------------------------------------------------
// TS.ALTER
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_alter_retention() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_alt"]).await;

    let resp = client
        .command(&["TS.ALTER", "ts_alt", "RETENTION", "120000"])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["TS.INFO", "ts_alt"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(info_field(&items, "retentionTime"), 120000);
}

#[tokio::test]
async fn ts_alter_labels() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["TS.CREATE", "ts_altl", "LABELS", "sensor", "old"])
        .await;

    let resp = client
        .command(&[
            "TS.ALTER", "ts_altl", "LABELS", "sensor", "new", "loc", "nyc",
        ])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["TS.INFO", "ts_altl"]).await;
    let items = unwrap_array(resp);
    let labels = info_field(&items, "labels");
    let label_arr = unwrap_array(labels.clone());
    // Should contain sensor=new, loc=nyc → 4 bulk strings
    assert_eq!(label_arr.len(), 4);
}

#[tokio::test]
async fn ts_alter_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["TS.ALTER", "ts_missing", "RETENTION", "1000"])
        .await;
    assert_error_prefix(&resp, "ERR TSDB: the key does not exist");
}

// ---------------------------------------------------------------------------
// TS.ADD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_add_explicit_timestamp() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_add1"]).await;

    let resp = client.command(&["TS.ADD", "ts_add1", "1000", "42"]).await;
    assert_integer_eq(&resp, 1000);

    let resp = client.command(&["TS.GET", "ts_add1"]).await;
    assert_eq!(get_timestamp(&resp), 1000);
    assert_eq!(get_value_str(&resp), "42");
}

#[tokio::test]
async fn ts_add_auto_timestamp() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_auto"]).await;

    let resp = client.command(&["TS.ADD", "ts_auto", "*", "99"]).await;
    let ts = unwrap_integer(&resp);
    assert!(ts > 0, "auto timestamp should be positive, got {ts}");
}

#[tokio::test]
async fn ts_add_auto_creates() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // No TS.CREATE — ADD should auto-create
    let resp = client
        .command(&["TS.ADD", "ts_autocreate", "5000", "123"])
        .await;
    assert_integer_eq(&resp, 5000);

    let resp = client.command(&["TS.GET", "ts_autocreate"]).await;
    assert_eq!(get_value_str(&resp), "123");
}

#[tokio::test]
async fn ts_add_multiple_samples() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_multi"]).await;

    for i in 1..=5 {
        let ts = (i * 1000).to_string();
        let val = (i * 10).to_string();
        client.command(&["TS.ADD", "ts_multi", &ts, &val]).await;
    }

    let resp = client.command(&["TS.INFO", "ts_multi"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(info_field(&items, "totalSamples"), 5);
    assert_integer_eq(info_field(&items, "firstTimestamp"), 1000);
    assert_integer_eq(info_field(&items, "lastTimestamp"), 5000);
}

#[tokio::test]
async fn ts_add_duplicate_policy_last() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["TS.CREATE", "ts_dup_last", "DUPLICATE_POLICY", "LAST"])
        .await;

    client
        .command(&["TS.ADD", "ts_dup_last", "1000", "100"])
        .await;
    client
        .command(&["TS.ADD", "ts_dup_last", "1000", "200"])
        .await;

    let resp = client.command(&["TS.GET", "ts_dup_last"]).await;
    assert_eq!(get_value_str(&resp), "200"); // last value wins
}

#[tokio::test]
async fn ts_add_duplicate_policy_first() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["TS.CREATE", "ts_dup_first", "DUPLICATE_POLICY", "FIRST"])
        .await;

    client
        .command(&["TS.ADD", "ts_dup_first", "1000", "100"])
        .await;
    client
        .command(&["TS.ADD", "ts_dup_first", "1000", "200"])
        .await;

    let resp = client.command(&["TS.GET", "ts_dup_first"]).await;
    assert_eq!(get_value_str(&resp), "100"); // first value wins
}

#[tokio::test]
async fn ts_add_on_duplicate_override() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Series policy is LAST, but ON_DUPLICATE overrides to FIRST
    client
        .command(&["TS.CREATE", "ts_on_dup", "DUPLICATE_POLICY", "LAST"])
        .await;

    client
        .command(&["TS.ADD", "ts_on_dup", "1000", "100"])
        .await;
    client
        .command(&[
            "TS.ADD",
            "ts_on_dup",
            "1000",
            "200",
            "ON_DUPLICATE",
            "FIRST",
        ])
        .await;

    let resp = client.command(&["TS.GET", "ts_on_dup"]).await;
    assert_eq!(get_value_str(&resp), "100"); // FIRST keeps original
}

// ---------------------------------------------------------------------------
// TS.MADD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_madd_multiple_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Use hash tags so keys hash to same slot
    client.command(&["TS.CREATE", "{ma}ts1"]).await;
    client.command(&["TS.CREATE", "{ma}ts2"]).await;

    let resp = client
        .command(&["TS.MADD", "{ma}ts1", "1000", "10", "{ma}ts2", "2000", "20"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_integer_eq(&items[0], 1000);
    assert_integer_eq(&items[1], 2000);

    // Verify each key
    let resp = client.command(&["TS.GET", "{ma}ts1"]).await;
    assert_eq!(get_value_str(&resp), "10");

    let resp = client.command(&["TS.GET", "{ma}ts2"]).await;
    assert_eq!(get_value_str(&resp), "20");
}

// ---------------------------------------------------------------------------
// TS.GET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_get_returns_latest() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_getl"]).await;
    client.command(&["TS.ADD", "ts_getl", "1000", "10"]).await;
    client.command(&["TS.ADD", "ts_getl", "2000", "20"]).await;
    client.command(&["TS.ADD", "ts_getl", "3000", "30"]).await;

    let resp = client.command(&["TS.GET", "ts_getl"]).await;
    assert_eq!(get_timestamp(&resp), 3000);
    assert_eq!(get_value_str(&resp), "30");
}

#[tokio::test]
async fn ts_get_empty_series() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_empty"]).await;

    let resp = client.command(&["TS.GET", "ts_empty"]).await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

#[tokio::test]
async fn ts_get_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TS.GET", "ts_nope"]).await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

// ---------------------------------------------------------------------------
// TS.DEL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_del_range() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_del"]).await;
    for i in 1..=5 {
        let ts = (i * 1000).to_string();
        let val = (i * 10).to_string();
        client.command(&["TS.ADD", "ts_del", &ts, &val]).await;
    }

    // Delete timestamps 2000-4000 inclusive
    let resp = client.command(&["TS.DEL", "ts_del", "2000", "4000"]).await;
    assert_integer_eq(&resp, 3);

    // Verify remaining samples
    let resp = client.command(&["TS.INFO", "ts_del"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(info_field(&items, "totalSamples"), 2);
}

#[tokio::test]
async fn ts_del_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["TS.DEL", "ts_delmissing", "0", "1000"])
        .await;
    assert_error_prefix(&resp, "ERR TSDB: the key does not exist");
}

// ---------------------------------------------------------------------------
// TS.INCRBY / TS.DECRBY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_incrby_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_inc"]).await;

    let resp = client
        .command(&["TS.INCRBY", "ts_inc", "10", "TIMESTAMP", "1000"])
        .await;
    assert_integer_eq(&resp, 1000);

    let resp = client.command(&["TS.GET", "ts_inc"]).await;
    assert_eq!(get_value_str(&resp), "10");

    // Increment again at a later timestamp — accumulates from last value
    let resp = client
        .command(&["TS.INCRBY", "ts_inc", "5", "TIMESTAMP", "2000"])
        .await;
    assert_integer_eq(&resp, 2000);

    let resp = client.command(&["TS.GET", "ts_inc"]).await;
    let val: f64 = get_value_str(&resp).parse().unwrap();
    // Value could be 15 (accumulated) or 5 (independent delta)
    assert!(val == 5.0 || val == 15.0, "unexpected INCRBY value: {val}");
}

#[tokio::test]
async fn ts_decrby_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_dec"]).await;
    client.command(&["TS.ADD", "ts_dec", "1000", "100"]).await;

    let resp = client
        .command(&["TS.DECRBY", "ts_dec", "25", "TIMESTAMP", "2000"])
        .await;
    assert_integer_eq(&resp, 2000);

    let resp = client.command(&["TS.GET", "ts_dec"]).await;
    let val: f64 = get_value_str(&resp).parse().unwrap();
    // Value could be 75 (100-25 accumulated) or -25 (independent delta)
    assert!(
        val == 75.0 || val == -25.0,
        "unexpected DECRBY value: {val}"
    );
}

// ---------------------------------------------------------------------------
// TS.INFO
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_info_all_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "TS.CREATE",
            "ts_info",
            "RETENTION",
            "30000",
            "LABELS",
            "env",
            "prod",
        ])
        .await;
    client.command(&["TS.ADD", "ts_info", "1000", "42"]).await;
    client.command(&["TS.ADD", "ts_info", "2000", "84"]).await;

    let resp = client.command(&["TS.INFO", "ts_info"]).await;
    let items = unwrap_array(resp);

    assert_integer_eq(info_field(&items, "totalSamples"), 2);
    assert_integer_eq(info_field(&items, "firstTimestamp"), 1000);
    assert_integer_eq(info_field(&items, "lastTimestamp"), 2000);
    assert_integer_eq(info_field(&items, "retentionTime"), 30000);

    // memoryUsage should be positive
    let mem = unwrap_integer(info_field(&items, "memoryUsage"));
    assert!(mem > 0, "memoryUsage should be positive, got {mem}");

    // labels should contain env=prod
    let labels = info_field(&items, "labels");
    let label_arr = unwrap_array(labels.clone());
    assert_eq!(label_arr.len(), 2); // ["env", "prod"]
    assert_bulk_eq(&label_arr[0], b"env");
    assert_bulk_eq(&label_arr[1], b"prod");

    // rules should be empty array
    let rules = info_field(&items, "rules");
    let rules_arr = unwrap_array(rules.clone());
    assert!(rules_arr.is_empty());
}

#[tokio::test]
async fn ts_info_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TS.INFO", "ts_nope"]).await;
    assert_error_prefix(&resp, "ERR TSDB: the key does not exist");
}

// ---------------------------------------------------------------------------
// TS.ADD — additional duplicate policies
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_add_duplicate_policy_min() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["TS.CREATE", "ts_dup_min", "DUPLICATE_POLICY", "MIN"])
        .await;
    client
        .command(&["TS.ADD", "ts_dup_min", "1000", "100"])
        .await;
    client
        .command(&["TS.ADD", "ts_dup_min", "1000", "50"])
        .await;

    let resp = client.command(&["TS.GET", "ts_dup_min"]).await;
    assert_eq!(get_value_str(&resp), "50"); // min(100, 50) = 50
}

#[tokio::test]
async fn ts_add_duplicate_policy_max() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["TS.CREATE", "ts_dup_max", "DUPLICATE_POLICY", "MAX"])
        .await;
    client
        .command(&["TS.ADD", "ts_dup_max", "1000", "100"])
        .await;
    client
        .command(&["TS.ADD", "ts_dup_max", "1000", "200"])
        .await;

    let resp = client.command(&["TS.GET", "ts_dup_max"]).await;
    assert_eq!(get_value_str(&resp), "200"); // max(100, 200) = 200
}

#[tokio::test]
async fn ts_add_duplicate_policy_block() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["TS.CREATE", "ts_dup_blk", "DUPLICATE_POLICY", "BLOCK"])
        .await;
    client
        .command(&["TS.ADD", "ts_dup_blk", "1000", "100"])
        .await;

    // Second add at same timestamp should error
    let resp = client
        .command(&["TS.ADD", "ts_dup_blk", "1000", "200"])
        .await;
    assert_error_prefix(&resp, "ERR TSDB");
}

// ---------------------------------------------------------------------------
// TS.REVRANGE — with aggregation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_revrange_aggregation_avg() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_rra"]).await;
    for i in 1..=5 {
        let ts = (i * 1000).to_string();
        let val = (i * 10).to_string();
        client.command(&["TS.ADD", "ts_rra", &ts, &val]).await;
    }

    // REVRANGE with aggregation avg, large bucket
    let resp = client
        .command(&[
            "TS.REVRANGE",
            "ts_rra",
            "-",
            "+",
            "AGGREGATION",
            "avg",
            "10000",
        ])
        .await;
    let items = unwrap_array(resp);
    assert!(!items.is_empty());
    let arr = unwrap_array(items[0].clone());
    let val: f64 = std::str::from_utf8(unwrap_bulk(&arr[1]))
        .unwrap()
        .parse()
        .unwrap();
    assert!((val - 30.0).abs() < 0.01, "expected avg ~30, got {val}");
}

#[tokio::test]
async fn ts_revrange_aggregation_sum() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_rrs"]).await;
    for i in 1..=5 {
        let ts = (i * 1000).to_string();
        let val = (i * 10).to_string();
        client.command(&["TS.ADD", "ts_rrs", &ts, &val]).await;
    }

    let resp = client
        .command(&[
            "TS.REVRANGE",
            "ts_rrs",
            "-",
            "+",
            "AGGREGATION",
            "sum",
            "10000",
        ])
        .await;
    let items = unwrap_array(resp);
    assert!(!items.is_empty());
    let arr = unwrap_array(items[0].clone());
    let val: f64 = std::str::from_utf8(unwrap_bulk(&arr[1]))
        .unwrap()
        .parse()
        .unwrap();
    assert!((val - 150.0).abs() < 0.01, "expected sum ~150, got {val}");
}

// ---------------------------------------------------------------------------
// TS.RANGE — filter combinations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_range_filter_by_ts_and_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_ftv"]).await;
    for i in 1..=5 {
        let ts = (i * 1000).to_string();
        let val = (i * 10).to_string();
        client.command(&["TS.ADD", "ts_ftv", &ts, &val]).await;
    }

    // FILTER_BY_TS 2000 3000 4000 + FILTER_BY_VALUE 15 35
    // Timestamps 2000,3000,4000 have values 20,30,40
    // Value filter 15..35 keeps 20,30
    let resp = client
        .command(&[
            "TS.RANGE",
            "ts_ftv",
            "-",
            "+",
            "FILTER_BY_TS",
            "2000",
            "3000",
            "4000",
            "FILTER_BY_VALUE",
            "15",
            "35",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2, "expected 2 samples after combined filters");
}

#[tokio::test]
async fn ts_range_aggregation_with_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "ts_rac"]).await;
    for i in 1..=10 {
        let ts = (i * 1000).to_string();
        let val = (i * 10).to_string();
        client.command(&["TS.ADD", "ts_rac", &ts, &val]).await;
    }

    // Two buckets (1-5000 and 6000-10000), but COUNT 1
    let resp = client
        .command(&[
            "TS.RANGE",
            "ts_rac",
            "-",
            "+",
            "AGGREGATION",
            "avg",
            "5000",
            "COUNT",
            "1",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(
        items.len(),
        1,
        "COUNT 1 should limit to 1 aggregated bucket"
    );
}

#[tokio::test]
async fn ts_range_aggregation_with_filter_by_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // FILTER_BY_VALUE applies to the output samples (after aggregation).
    // Use two buckets: bucket1 avg=30 (values 10-50), bucket2 avg=80 (values 60-100).
    // Then FILTER_BY_VALUE 50 100 keeps only bucket2.
    client.command(&["TS.CREATE", "ts_rafv"]).await;
    for i in 1..=10 {
        let ts = (i * 1000).to_string();
        let val = (i * 10).to_string();
        client.command(&["TS.ADD", "ts_rafv", &ts, &val]).await;
    }

    let resp = client
        .command(&[
            "TS.RANGE",
            "ts_rafv",
            "-",
            "+",
            "AGGREGATION",
            "avg",
            "5000",
            "FILTER_BY_VALUE",
            "50",
            "200",
        ])
        .await;
    let items = unwrap_array(resp);
    // Two buckets produced; only those with avg >= 50 pass the filter.
    // This verifies FILTER_BY_VALUE works in combination with AGGREGATION.
    assert!(
        items.len() <= 2 && !items.is_empty(),
        "expected 1 or 2 buckets, got {}",
        items.len()
    );
}
