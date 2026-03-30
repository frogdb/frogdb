//! Regression tests for T-Digest (TDIGEST.*) commands.

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

/// Parse a bulk response as an f64.
fn parse_bulk_f64(resp: &frogdb_protocol::Response) -> f64 {
    let bytes = unwrap_bulk(resp);
    std::str::from_utf8(bytes).unwrap().parse::<f64>().unwrap()
}

/// Parse a bulk response as a string (for nan/inf checks).
fn parse_bulk_str(resp: &frogdb_protocol::Response) -> String {
    let bytes = unwrap_bulk(resp);
    std::str::from_utf8(bytes).unwrap().to_string()
}

/// Add values 1 through 100 to the given key.
async fn populate_1_to_100(client: &mut frogdb_test_harness::server::TestClient, key: &str) {
    let mut args: Vec<String> = vec!["TDIGEST.ADD".to_string(), key.to_string()];
    for i in 1..=100 {
        args.push(i.to_string());
    }
    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let resp = client.command(&refs).await;
    assert_ok(&resp);
}

// ---------------------------------------------------------------------------
// TDIGEST.CREATE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tdigest_create_default() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TDIGEST.CREATE", "td"]).await;
    assert_ok(&resp);

    // Verify it exists via INFO
    let info = client.command(&["TDIGEST.INFO", "td"]).await;
    let arr = unwrap_array(info);
    // Should have the standard fields: at least 16 elements (8 key-value pairs)
    assert!(arr.len() >= 16, "INFO should return at least 16 elements, got {}", arr.len());

    // First pair should be "Compression" with default 100
    let label = std::str::from_utf8(unwrap_bulk(&arr[0])).unwrap();
    assert_eq!(label, "Compression");
    let compression = unwrap_integer(&arr[1]);
    assert_eq!(compression, 100);
}

#[tokio::test]
async fn tdigest_create_custom_compression() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TDIGEST.CREATE", "td", "COMPRESSION", "500"]).await;
    assert_ok(&resp);

    let info = client.command(&["TDIGEST.INFO", "td"]).await;
    let arr = unwrap_array(info);
    let label = std::str::from_utf8(unwrap_bulk(&arr[0])).unwrap();
    assert_eq!(label, "Compression");
    let compression = unwrap_integer(&arr[1]);
    assert_eq!(compression, 500);
}

// ---------------------------------------------------------------------------
// TDIGEST.ADD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tdigest_add_single_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TDIGEST.CREATE", "td"]).await;

    let resp = client.command(&["TDIGEST.ADD", "td", "42.5"]).await;
    assert_ok(&resp);

    // MIN and MAX should both be 42.5
    let min_resp = client.command(&["TDIGEST.MIN", "td"]).await;
    let min_val = parse_bulk_f64(&min_resp);
    assert!((min_val - 42.5).abs() < 0.01, "MIN should be 42.5, got {min_val}");

    let max_resp = client.command(&["TDIGEST.MAX", "td"]).await;
    let max_val = parse_bulk_f64(&max_resp);
    assert!((max_val - 42.5).abs() < 0.01, "MAX should be 42.5, got {max_val}");
}

#[tokio::test]
async fn tdigest_add_multiple_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TDIGEST.CREATE", "td"]).await;

    let resp = client
        .command(&["TDIGEST.ADD", "td", "1", "2", "3", "4", "5"])
        .await;
    assert_ok(&resp);

    let min_resp = client.command(&["TDIGEST.MIN", "td"]).await;
    let min_val = parse_bulk_f64(&min_resp);
    assert!((min_val - 1.0).abs() < 0.01, "MIN should be 1.0, got {min_val}");

    let max_resp = client.command(&["TDIGEST.MAX", "td"]).await;
    let max_val = parse_bulk_f64(&max_resp);
    assert!((max_val - 5.0).abs() < 0.01, "MAX should be 5.0, got {max_val}");
}

#[tokio::test]
async fn tdigest_add_auto_creates() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // ADD on non-existent key should auto-create with default compression
    let resp = client.command(&["TDIGEST.ADD", "td_new", "10", "20", "30"]).await;
    assert_ok(&resp);

    // Verify it was created
    let info = client.command(&["TDIGEST.INFO", "td_new"]).await;
    let arr = unwrap_array(info);
    let label = std::str::from_utf8(unwrap_bulk(&arr[0])).unwrap();
    assert_eq!(label, "Compression");
    // Default compression is 100
    let compression = unwrap_integer(&arr[1]);
    assert_eq!(compression, 100);

    let min_resp = client.command(&["TDIGEST.MIN", "td_new"]).await;
    let min_val = parse_bulk_f64(&min_resp);
    assert!((min_val - 10.0).abs() < 0.01, "MIN should be 10.0, got {min_val}");
}

// ---------------------------------------------------------------------------
// TDIGEST.QUANTILE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tdigest_quantile_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_1_to_100(&mut client, "td").await;

    // p50 should be approximately 50
    let resp = client.command(&["TDIGEST.QUANTILE", "td", "0.5"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);
    let p50 = parse_bulk_f64(&arr[0]);
    assert!(
        (p50 - 50.0).abs() < 5.0,
        "p50 should be near 50, got {p50}"
    );
}

#[tokio::test]
async fn tdigest_quantile_multiple() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_1_to_100(&mut client, "td").await;

    let resp = client
        .command(&["TDIGEST.QUANTILE", "td", "0.0", "0.5", "1.0"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 3);

    let p0 = parse_bulk_f64(&arr[0]);
    let p50 = parse_bulk_f64(&arr[1]);
    let p100 = parse_bulk_f64(&arr[2]);

    // p0 should be near 1
    assert!(
        (p0 - 1.0).abs() < 2.0,
        "p0 should be near 1.0, got {p0}"
    );
    // p50 should be near 50
    assert!(
        (p50 - 50.0).abs() < 5.0,
        "p50 should be near 50.0, got {p50}"
    );
    // p100 should be near 100
    assert!(
        (p100 - 100.0).abs() < 2.0,
        "p100 should be near 100.0, got {p100}"
    );
}

// ---------------------------------------------------------------------------
// TDIGEST.CDF
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tdigest_cdf_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_1_to_100(&mut client, "td").await;

    // CDF of 50 should be approximately 0.5
    let resp = client.command(&["TDIGEST.CDF", "td", "50"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);
    let cdf_50 = parse_bulk_f64(&arr[0]);
    assert!(
        (cdf_50 - 0.5).abs() < 0.1,
        "CDF(50) should be near 0.5, got {cdf_50}"
    );
}

// ---------------------------------------------------------------------------
// TDIGEST.RANK / TDIGEST.REVRANK
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tdigest_rank_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_1_to_100(&mut client, "td").await;

    let resp = client.command(&["TDIGEST.RANK", "td", "50"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);
    let rank = unwrap_integer(&arr[0]);
    // Rank of 50 in 1..=100 should be roughly 49-50 (0-indexed count of values <= 50)
    assert!(
        (rank - 50).unsigned_abs() < 5,
        "RANK of 50 should be near 50, got {rank}"
    );
}

#[tokio::test]
async fn tdigest_revrank_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_1_to_100(&mut client, "td").await;

    let resp = client.command(&["TDIGEST.REVRANK", "td", "50"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);
    let revrank = unwrap_integer(&arr[0]);
    // Reverse rank of 50 in 1..=100: about 50 values are >= 50
    assert!(
        (revrank - 50).unsigned_abs() < 5,
        "REVRANK of 50 should be near 50, got {revrank}"
    );
}

// ---------------------------------------------------------------------------
// TDIGEST.MIN / TDIGEST.MAX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tdigest_min_max() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_1_to_100(&mut client, "td").await;

    let min_resp = client.command(&["TDIGEST.MIN", "td"]).await;
    let min_val = parse_bulk_f64(&min_resp);
    assert!(
        (min_val - 1.0).abs() < 0.01,
        "MIN should be 1.0, got {min_val}"
    );

    let max_resp = client.command(&["TDIGEST.MAX", "td"]).await;
    let max_val = parse_bulk_f64(&max_resp);
    assert!(
        (max_val - 100.0).abs() < 0.01,
        "MAX should be 100.0, got {max_val}"
    );
}

// ---------------------------------------------------------------------------
// TDIGEST.MERGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tdigest_merge_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create two digests with different ranges (hash tags for same slot)
    // td1: 1..=50
    let mut args1: Vec<String> = vec!["TDIGEST.ADD".to_string(), "{td}1".to_string()];
    for i in 1..=50 {
        args1.push(i.to_string());
    }
    let refs1: Vec<&str> = args1.iter().map(|s| s.as_str()).collect();
    assert_ok(&client.command(&refs1).await);

    // td2: 51..=100
    let mut args2: Vec<String> = vec!["TDIGEST.ADD".to_string(), "{td}2".to_string()];
    for i in 51..=100 {
        args2.push(i.to_string());
    }
    let refs2: Vec<&str> = args2.iter().map(|s| s.as_str()).collect();
    assert_ok(&client.command(&refs2).await);

    // Pre-create destination, then merge
    assert_ok(&client.command(&["TDIGEST.CREATE", "{td}merged"]).await);
    let resp = client
        .command(&["TDIGEST.MERGE", "{td}merged", "2", "{td}1", "{td}2"])
        .await;
    assert_ok(&resp);

    // Combined min should be 1, max should be 100
    let min_resp = client.command(&["TDIGEST.MIN", "{td}merged"]).await;
    let min_val = parse_bulk_f64(&min_resp);
    assert!(
        (min_val - 1.0).abs() < 0.01,
        "merged MIN should be 1.0, got {min_val}"
    );

    let max_resp = client.command(&["TDIGEST.MAX", "{td}merged"]).await;
    let max_val = parse_bulk_f64(&max_resp);
    assert!(
        (max_val - 100.0).abs() < 0.01,
        "merged MAX should be 100.0, got {max_val}"
    );

    // p50 of merged should be near 50
    let q_resp = client
        .command(&["TDIGEST.QUANTILE", "{td}merged", "0.5"])
        .await;
    let q_arr = unwrap_array(q_resp);
    let p50 = parse_bulk_f64(&q_arr[0]);
    assert!(
        (p50 - 50.0).abs() < 5.0,
        "merged p50 should be near 50, got {p50}"
    );
}

// ---------------------------------------------------------------------------
// TDIGEST.RESET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tdigest_reset_clears_data() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_1_to_100(&mut client, "td").await;

    let resp = client.command(&["TDIGEST.RESET", "td"]).await;
    assert_ok(&resp);

    // After reset, INFO should show zero weight
    let info = client.command(&["TDIGEST.INFO", "td"]).await;
    let arr = unwrap_array(info);

    // Find "Merged weight" and "Unmerged weight" — both should be "0"
    let mut merged_weight = None;
    let mut unmerged_weight = None;
    for i in (0..arr.len()).step_by(2) {
        if let Ok(label) = std::str::from_utf8(unwrap_bulk(&arr[i])) {
            match label {
                "Merged weight" => {
                    merged_weight = Some(parse_bulk_f64(&arr[i + 1]));
                }
                "Unmerged weight" => {
                    unmerged_weight = Some(parse_bulk_f64(&arr[i + 1]));
                }
                _ => {}
            }
        }
    }

    assert!(
        (merged_weight.unwrap_or(0.0) + unmerged_weight.unwrap_or(0.0)).abs() < 0.01,
        "total weight after reset should be 0"
    );

    // MIN of empty digest should be "nan"
    let min_resp = client.command(&["TDIGEST.MIN", "td"]).await;
    let min_str = parse_bulk_str(&min_resp);
    assert_eq!(min_str, "nan", "MIN of empty digest should be nan");
}

// ---------------------------------------------------------------------------
// TDIGEST.INFO
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tdigest_info_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_1_to_100(&mut client, "td").await;

    let info = client.command(&["TDIGEST.INFO", "td"]).await;
    // Should have 8 key-value pairs = 16 elements
    assert_array_len(&info, 16);

    let arr = unwrap_array(info);

    let expected_labels = [
        "Compression",
        "Capacity",
        "Merged nodes",
        "Unmerged nodes",
        "Merged weight",
        "Unmerged weight",
        "Total compressions",
        "Memory usage",
    ];

    for (i, expected) in expected_labels.iter().enumerate() {
        let idx = i * 2;
        let label = std::str::from_utf8(unwrap_bulk(&arr[idx])).unwrap();
        assert_eq!(label, *expected, "INFO field {i} label mismatch");
    }

    // Compression should be 100 (default from auto-create via ADD)
    let compression = unwrap_integer(&arr[1]);
    assert_eq!(compression, 100);

    // Total weight (merged + unmerged) should be 100
    let merged_w = parse_bulk_f64(&arr[9]);
    let unmerged_w = parse_bulk_f64(&arr[11]);
    let total = merged_w + unmerged_w;
    assert!(
        (total - 100.0).abs() < 0.01,
        "total weight should be 100.0, got {total}"
    );
}

// ---------------------------------------------------------------------------
// TDIGEST.TRIMMED_MEAN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tdigest_trimmedmean_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    populate_1_to_100(&mut client, "td").await;

    // Trimmed mean between 0.1 and 0.9 should exclude the bottom 10% and top 10%
    // For uniform 1..=100 this should be close to 50.5
    let resp = client
        .command(&["TDIGEST.TRIMMED_MEAN", "td", "0.1", "0.9"])
        .await;
    let mean_val = parse_bulk_f64(&resp);
    assert!(
        (mean_val - 50.5).abs() < 5.0,
        "trimmed mean (0.1, 0.9) should be near 50.5, got {mean_val}"
    );
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tdigest_create_zero_compression() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // COMPRESSION 0 should fail
    let resp = client
        .command(&["TDIGEST.CREATE", "td", "COMPRESSION", "0"])
        .await;
    assert_error_prefix(&resp, "ERR");
}
