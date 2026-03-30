//! Regression tests for RedisTimeSeries range queries and downsampling rules.

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a TS.RANGE sample: [Integer(ts), Bulk(value)] → (i64, String)
fn parse_sample(resp: &Response) -> (i64, String) {
    let arr = match resp {
        Response::Array(items) => items,
        other => panic!("expected Array for sample, got {other:?}"),
    };
    assert_eq!(arr.len(), 2, "sample should have 2 elements");
    let ts = unwrap_integer(&arr[0]);
    let val = std::str::from_utf8(unwrap_bulk(&arr[1]))
        .unwrap()
        .to_string();
    (ts, val)
}

/// Find a field value in a flat key-value INFO array by label name.
fn info_field<'a>(items: &'a [Response], label: &str) -> &'a Response {
    for i in (0..items.len()).step_by(2) {
        if let Response::Bulk(Some(b)) = &items[i] {
            if std::str::from_utf8(b).unwrap() == label {
                return &items[i + 1];
            }
        }
        if let Response::Simple(s) = &items[i] {
            if s == label {
                return &items[i + 1];
            }
        }
    }
    panic!("field {label:?} not found in INFO response");
}

/// Populate a test series with 5 samples: ts 1000-5000, values 10-50.
async fn populate_5_samples(client: &mut frogdb_test_harness::server::TestClient, key: &str) {
    client.command(&["TS.CREATE", key]).await;
    for i in 1..=5 {
        let ts = (i * 1000).to_string();
        let val = (i * 10).to_string();
        client.command(&["TS.ADD", key, &ts, &val]).await;
    }
}

// ---------------------------------------------------------------------------
// TS.RANGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_range_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_r1").await;

    let resp = client.command(&["TS.RANGE", "ts_r1", "-", "+"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 5);

    let (ts, val) = parse_sample(&items[0]);
    assert_eq!(ts, 1000);
    assert_eq!(val, "10");

    let (ts, val) = parse_sample(&items[4]);
    assert_eq!(ts, 5000);
    assert_eq!(val, "50");
}

#[tokio::test]
async fn ts_range_subset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_r2").await;

    let resp = client
        .command(&["TS.RANGE", "ts_r2", "2000", "4000"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_eq!(parse_sample(&items[0]).0, 2000);
    assert_eq!(parse_sample(&items[2]).0, 4000);
}

#[tokio::test]
async fn ts_range_with_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_r3").await;

    let resp = client
        .command(&["TS.RANGE", "ts_r3", "-", "+", "COUNT", "2"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_eq!(parse_sample(&items[0]).0, 1000);
    assert_eq!(parse_sample(&items[1]).0, 2000);
}

#[tokio::test]
async fn ts_range_aggregation_avg() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_ravg").await;

    // Large bucket covers all 5 samples → avg(10,20,30,40,50) = 30
    let resp = client
        .command(&[
            "TS.RANGE",
            "ts_ravg",
            "-",
            "+",
            "AGGREGATION",
            "avg",
            "10000",
        ])
        .await;
    let items = unwrap_array(resp);
    assert!(!items.is_empty());

    let (_, val) = parse_sample(&items[0]);
    let v: f64 = val.parse().unwrap();
    assert!((v - 30.0).abs() < 0.01, "expected ~30, got {v}");
}

#[tokio::test]
async fn ts_range_aggregation_sum() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_rsum").await;

    // Large bucket covers all samples: sum = 10+20+30+40+50 = 150
    let resp = client
        .command(&[
            "TS.RANGE",
            "ts_rsum",
            "-",
            "+",
            "AGGREGATION",
            "sum",
            "10000",
        ])
        .await;
    let items = unwrap_array(resp);
    assert!(!items.is_empty());

    let (_, val) = parse_sample(&items[0]);
    let v: f64 = val.parse().unwrap();
    assert!((v - 150.0).abs() < 0.01, "expected ~150, got {v}");
}

#[tokio::test]
async fn ts_range_aggregation_min_max() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_rmm").await;

    // Large bucket covers all
    let resp = client
        .command(&[
            "TS.RANGE",
            "ts_rmm",
            "-",
            "+",
            "AGGREGATION",
            "min",
            "10000",
        ])
        .await;
    let items = unwrap_array(resp);
    let (_, val) = parse_sample(&items[0]);
    assert_eq!(val, "10");

    let resp = client
        .command(&[
            "TS.RANGE",
            "ts_rmm",
            "-",
            "+",
            "AGGREGATION",
            "max",
            "10000",
        ])
        .await;
    let items = unwrap_array(resp);
    let (_, val) = parse_sample(&items[0]);
    assert_eq!(val, "50");
}

#[tokio::test]
async fn ts_range_aggregation_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_rcnt").await;

    let resp = client
        .command(&[
            "TS.RANGE",
            "ts_rcnt",
            "-",
            "+",
            "AGGREGATION",
            "count",
            "10000",
        ])
        .await;
    let items = unwrap_array(resp);
    let (_, val) = parse_sample(&items[0]);
    assert_eq!(val, "5");
}

#[tokio::test]
async fn ts_range_filter_by_ts() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_rfts").await;

    let resp = client
        .command(&[
            "TS.RANGE",
            "ts_rfts",
            "-",
            "+",
            "FILTER_BY_TS",
            "1000",
            "3000",
            "5000",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_eq!(parse_sample(&items[0]).0, 1000);
    assert_eq!(parse_sample(&items[1]).0, 3000);
    assert_eq!(parse_sample(&items[2]).0, 5000);
}

#[tokio::test]
async fn ts_range_filter_by_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_rfv").await;

    let resp = client
        .command(&[
            "TS.RANGE",
            "ts_rfv",
            "-",
            "+",
            "FILTER_BY_VALUE",
            "20",
            "40",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_eq!(parse_sample(&items[0]).1, "20");
    assert_eq!(parse_sample(&items[1]).1, "30");
    assert_eq!(parse_sample(&items[2]).1, "40");
}

#[tokio::test]
async fn ts_range_empty_result() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_rempty").await;

    // Query a range with no data
    let resp = client
        .command(&["TS.RANGE", "ts_rempty", "9000", "10000"])
        .await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

// ---------------------------------------------------------------------------
// TS.REVRANGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_revrange_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_rr1").await;

    let resp = client
        .command(&["TS.REVRANGE", "ts_rr1", "-", "+"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 5);

    // First item should be the latest
    assert_eq!(parse_sample(&items[0]).0, 5000);
    assert_eq!(parse_sample(&items[0]).1, "50");

    // Last item should be the earliest
    assert_eq!(parse_sample(&items[4]).0, 1000);
    assert_eq!(parse_sample(&items[4]).1, "10");
}

#[tokio::test]
async fn ts_revrange_with_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_5_samples(&mut client, "ts_rr2").await;

    let resp = client
        .command(&["TS.REVRANGE", "ts_rr2", "-", "+", "COUNT", "2"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_eq!(parse_sample(&items[0]).0, 5000);
    assert_eq!(parse_sample(&items[1]).0, 4000);
}

// ---------------------------------------------------------------------------
// TS.CREATERULE / TS.DELETERULE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_createrule_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "{r}ts_src"]).await;
    client.command(&["TS.CREATE", "{r}ts_dst"]).await;

    let resp = client
        .command(&[
            "TS.CREATERULE",
            "{r}ts_src",
            "{r}ts_dst",
            "AGGREGATION",
            "avg",
            "5000",
        ])
        .await;
    assert_ok(&resp);

    // Verify rule in INFO
    let resp = client.command(&["TS.INFO", "{r}ts_src"]).await;
    let items = unwrap_array(resp);
    let rules = info_field(&items, "rules");
    let rules_arr = unwrap_array(rules.clone());
    assert!(!rules_arr.is_empty(), "should have at least one rule");
}

#[tokio::test]
async fn ts_createrule_dest_must_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "{r}ts_src2"]).await;

    let resp = client
        .command(&[
            "TS.CREATERULE",
            "{r}ts_src2",
            "{r}ts_nodst",
            "AGGREGATION",
            "avg",
            "5000",
        ])
        .await;
    assert_error_prefix(&resp, "ERR TSDB: the key does not exist");
}

#[tokio::test]
async fn ts_createrule_aggregation_required() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "{r}ts_src3"]).await;
    client.command(&["TS.CREATE", "{r}ts_dst3"]).await;

    let resp = client
        .command(&[
            "TS.CREATERULE",
            "{r}ts_src3",
            "{r}ts_dst3",
            "NOTAFIELD",
            "avg",
            "5000",
        ])
        .await;
    assert_error_prefix(&resp, "ERR TSDB: AGGREGATION keyword expected");
}

#[tokio::test]
async fn ts_deleterule_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "{r}ts_drs"]).await;
    client.command(&["TS.CREATE", "{r}ts_drd"]).await;
    client
        .command(&[
            "TS.CREATERULE",
            "{r}ts_drs",
            "{r}ts_drd",
            "AGGREGATION",
            "sum",
            "3000",
        ])
        .await;

    let resp = client
        .command(&["TS.DELETERULE", "{r}ts_drs", "{r}ts_drd"])
        .await;
    assert_ok(&resp);

    // Verify rule removed
    let resp = client.command(&["TS.INFO", "{r}ts_drs"]).await;
    let items = unwrap_array(resp);
    let rules = info_field(&items, "rules");
    let rules_arr = unwrap_array(rules.clone());
    assert!(rules_arr.is_empty(), "rules should be empty after delete");
}

#[tokio::test]
async fn ts_deleterule_nonexistent_source() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["TS.DELETERULE", "ts_nosrc", "ts_nodst"])
        .await;
    assert_error_prefix(&resp, "ERR TSDB: the key does not exist");
}

// ---------------------------------------------------------------------------
// TS.QUERYINDEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_queryindex_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "TS.CREATE",
            "ts_qi1",
            "LABELS",
            "sensor",
            "temp",
            "region",
            "east",
        ])
        .await;
    client
        .command(&[
            "TS.CREATE",
            "ts_qi2",
            "LABELS",
            "sensor",
            "humidity",
            "region",
            "east",
        ])
        .await;
    client
        .command(&[
            "TS.CREATE",
            "ts_qi3",
            "LABELS",
            "sensor",
            "temp",
            "region",
            "west",
        ])
        .await;

    // Query for sensor=temp
    let resp = client
        .command(&["TS.QUERYINDEX", "sensor=temp"])
        .await;
    let items = unwrap_array(resp);
    // Should match ts_qi1 and ts_qi3
    assert_eq!(items.len(), 2);
    let keys: Vec<String> = items
        .iter()
        .map(|r| std::str::from_utf8(unwrap_bulk(r)).unwrap().to_string())
        .collect();
    assert!(keys.contains(&"ts_qi1".to_string()));
    assert!(keys.contains(&"ts_qi3".to_string()));
}

// ---------------------------------------------------------------------------
// TS.MGET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_mget_with_filter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "TS.CREATE",
            "ts_mg1",
            "LABELS",
            "type",
            "cpu",
        ])
        .await;
    client
        .command(&[
            "TS.CREATE",
            "ts_mg2",
            "LABELS",
            "type",
            "mem",
        ])
        .await;

    client.command(&["TS.ADD", "ts_mg1", "1000", "80"]).await;
    client.command(&["TS.ADD", "ts_mg2", "1000", "60"]).await;

    let resp = client
        .command(&["TS.MGET", "FILTER", "type=cpu"])
        .await;
    let items = unwrap_array(resp);
    // Should return at least 1 series matching type=cpu
    assert!(!items.is_empty(), "MGET should return matching series");
}

// ---------------------------------------------------------------------------
// TS.MRANGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ts_mrange_with_filter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "TS.CREATE",
            "ts_mr1",
            "LABELS",
            "app",
            "web",
        ])
        .await;
    client
        .command(&[
            "TS.CREATE",
            "ts_mr2",
            "LABELS",
            "app",
            "api",
        ])
        .await;

    client.command(&["TS.ADD", "ts_mr1", "1000", "100"]).await;
    client.command(&["TS.ADD", "ts_mr1", "2000", "200"]).await;
    client.command(&["TS.ADD", "ts_mr2", "1000", "50"]).await;

    let resp = client
        .command(&["TS.MRANGE", "-", "+", "FILTER", "app=web"])
        .await;
    let items = unwrap_array(resp);
    assert!(!items.is_empty(), "MRANGE should return matching series");
}
