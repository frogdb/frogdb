//! Integration tests for TimeSeries commands.

mod common;

use common::test_server::{TestServer, TestServerConfig};
use frogdb_protocol::Response;
use frogdb_telemetry::testing::{MetricsDelta, MetricsSnapshot, fetch_metrics};
use std::time::Duration;

async fn start_server() -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await
}

#[tokio::test]
async fn test_ts_create() {
    let server = start_server().await;
    let mut client = server.connect().await;

    // Get baseline metrics
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);

    // Basic create
    let response = client.command(&["TS.CREATE", "temp"]).await;
    assert_eq!(response, Response::ok());

    // Create with options
    let response = client
        .command(&[
            "TS.CREATE",
            "temp2",
            "RETENTION",
            "86400000",
            "DUPLICATE_POLICY",
            "LAST",
            "CHUNK_SIZE",
            "512",
            "LABELS",
            "location",
            "kitchen",
            "sensor",
            "DHT22",
        ])
        .await;
    assert_eq!(response, Response::ok());

    // Create existing key should fail
    let response = client.command(&["TS.CREATE", "temp"]).await;
    assert!(matches!(response, Response::Error(_)));

    // Verify metrics - 3 TS.CREATE commands total
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);
    MetricsDelta::new(before, after).assert_counter_increased(
        "frogdb_commands_total",
        &[("command", "TS.CREATE")],
        3.0,
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_add_and_get() {
    let server = start_server().await;
    let mut client = server.connect().await;

    // Create time series
    client.command(&["TS.CREATE", "temp"]).await;

    // Get baseline metrics (after CREATE)
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);

    // Add with explicit timestamp
    let response = client.command(&["TS.ADD", "temp", "1000", "23.5"]).await;
    assert_eq!(response, Response::Integer(1000));

    // Add another sample
    let response = client.command(&["TS.ADD", "temp", "2000", "24.1"]).await;
    assert_eq!(response, Response::Integer(2000));

    // Get last sample
    let response = client.command(&["TS.GET", "temp"]).await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Response::Integer(2000));
            // Value should be "24.1"
            if let Response::Bulk(Some(b)) = &arr[1] {
                assert_eq!(&**b, b"24.1");
            } else {
                panic!("Expected bulk string for value");
            }
        }
        _ => panic!("Expected array response"),
    }

    // Verify metrics - 2 TS.ADD and 1 TS.GET
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);
    MetricsDelta::new(before, after)
        .assert_counter_increased("frogdb_commands_total", &[("command", "TS.ADD")], 2.0)
        .assert_counter_increased("frogdb_commands_total", &[("command", "TS.GET")], 1.0);

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_add_auto_create() {
    let server = start_server().await;
    let mut client = server.connect().await;

    // Add to non-existent key should auto-create
    let response = client
        .command(&["TS.ADD", "newkey", "1000", "42.0", "LABELS", "type", "test"])
        .await;
    assert_eq!(response, Response::Integer(1000));

    // Verify it was created
    let response = client.command(&["TS.GET", "newkey"]).await;
    assert!(matches!(response, Response::Array(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_add_auto_timestamp() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "temp"]).await;

    // Add with auto-timestamp (*)
    let response = client.command(&["TS.ADD", "temp", "*", "25.0"]).await;
    match response {
        Response::Integer(ts) => {
            // Should be a reasonable timestamp (after year 2020)
            assert!(ts > 1577836800000);
        }
        _ => panic!("Expected integer response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_range() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "temp"]).await;

    // Add samples
    for i in 0..10 {
        let ts = 1000 + i * 100;
        let val = format!("{}.0", i + 1);
        client
            .command(&["TS.ADD", "temp", &ts.to_string(), &val])
            .await;
    }

    // Get baseline metrics (after CREATE and ADDs)
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);

    // Query range
    let response = client.command(&["TS.RANGE", "temp", "1000", "1500"]).await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 6); // 1000, 1100, 1200, 1300, 1400, 1500
        }
        _ => panic!("Expected array response"),
    }

    // Query with - and + for full range
    let response = client.command(&["TS.RANGE", "temp", "-", "+"]).await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 10);
        }
        _ => panic!("Expected array response"),
    }

    // Query with COUNT
    let response = client
        .command(&["TS.RANGE", "temp", "-", "+", "COUNT", "3"])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 3);
        }
        _ => panic!("Expected array response"),
    }

    // Verify metrics - 3 TS.RANGE commands
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);
    MetricsDelta::new(before, after).assert_counter_increased(
        "frogdb_commands_total",
        &[("command", "TS.RANGE")],
        3.0,
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_revrange() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "temp"]).await;

    // Add samples
    client.command(&["TS.ADD", "temp", "1000", "10.0"]).await;
    client.command(&["TS.ADD", "temp", "2000", "20.0"]).await;
    client.command(&["TS.ADD", "temp", "3000", "30.0"]).await;

    // Query reverse range
    let response = client
        .command(&["TS.REVRANGE", "temp", "1000", "3000"])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 3);
            // First should be 3000 (newest)
            if let Response::Array(first) = &arr[0] {
                assert_eq!(first[0], Response::Integer(3000));
            }
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_range_with_aggregation() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "temp"]).await;

    // Add samples: 10 samples with 100ms interval
    for i in 0..10 {
        let ts = 1000 + i * 100;
        let val = format!("{}.0", (i + 1) * 10);
        client
            .command(&["TS.ADD", "temp", &ts.to_string(), &val])
            .await;
    }

    // Aggregate with AVG in 500ms buckets
    let response = client
        .command(&[
            "TS.RANGE",
            "temp",
            "1000",
            "2000",
            "AGGREGATION",
            "AVG",
            "500",
        ])
        .await;
    match response {
        Response::Array(arr) => {
            // Should have 2 buckets: 1000-1400 and 1500-1900
            assert_eq!(arr.len(), 2);
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_del() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "temp"]).await;

    // Add samples
    for i in 0..10 {
        let ts = 1000 + i * 100;
        client
            .command(&["TS.ADD", "temp", &ts.to_string(), "1.0"])
            .await;
    }

    // Delete middle range
    let response = client.command(&["TS.DEL", "temp", "1300", "1600"]).await;
    assert_eq!(response, Response::Integer(4)); // 1300, 1400, 1500, 1600

    // Verify
    let response = client.command(&["TS.RANGE", "temp", "-", "+"]).await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 6); // 10 - 4 = 6
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_madd() {
    let server = start_server().await;
    let mut client = server.connect().await;

    // Create multiple time series
    client.command(&["TS.CREATE", "temp1"]).await;
    client.command(&["TS.CREATE", "temp2"]).await;

    // Get baseline metrics (after CREATEs)
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);

    // Add to multiple series
    let response = client
        .command(&[
            "TS.MADD", "temp1", "1000", "10.0", "temp2", "1000", "20.0", "temp1", "2000", "15.0",
        ])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Response::Integer(1000));
            assert_eq!(arr[1], Response::Integer(1000));
            assert_eq!(arr[2], Response::Integer(2000));
        }
        _ => panic!("Expected array response"),
    }

    // Verify metrics - 1 TS.MADD command
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);
    MetricsDelta::new(before, after).assert_counter_increased(
        "frogdb_commands_total",
        &[("command", "TS.MADD")],
        1.0,
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_incrby_decrby() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "counter"]).await;

    // Increment
    let response = client
        .command(&["TS.INCRBY", "counter", "10", "TIMESTAMP", "1000"])
        .await;
    assert_eq!(response, Response::Integer(1000));

    // Increment again at same timestamp
    let response = client
        .command(&["TS.INCRBY", "counter", "5", "TIMESTAMP", "1000"])
        .await;
    assert_eq!(response, Response::Integer(1000));

    // Decrement
    let response = client
        .command(&["TS.DECRBY", "counter", "3", "TIMESTAMP", "1000"])
        .await;
    assert_eq!(response, Response::Integer(1000));

    // Get should show final value
    let response = client.command(&["TS.GET", "counter"]).await;
    match response {
        Response::Array(arr) => {
            if let Response::Bulk(Some(b)) = &arr[1] {
                let val: f64 = std::str::from_utf8(b).unwrap().parse().unwrap();
                // 10 + 5 - 3 = 12
                assert!((val - 12.0).abs() < 0.001);
            }
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_info() {
    let server = start_server().await;
    let mut client = server.connect().await;

    // Create with labels
    client
        .command(&[
            "TS.CREATE",
            "temp",
            "RETENTION",
            "86400000",
            "LABELS",
            "location",
            "kitchen",
        ])
        .await;

    // Add some samples
    for i in 0..5 {
        client
            .command(&["TS.ADD", "temp", &(1000 + i * 100).to_string(), "20.0"])
            .await;
    }

    // Get info
    let response = client.command(&["TS.INFO", "temp"]).await;
    match response {
        Response::Array(arr) => {
            // Should have key-value pairs
            let mut found_samples = false;
            let mut found_retention = false;
            for chunk in arr.chunks(2) {
                if let Response::Bulk(Some(key)) = &chunk[0] {
                    if &**key == b"totalSamples"
                        && let Response::Integer(val) = &chunk[1]
                    {
                        assert_eq!(*val, 5);
                        found_samples = true;
                    }
                    if &**key == b"retentionTime"
                        && let Response::Integer(val) = &chunk[1]
                    {
                        assert_eq!(*val, 86400000);
                        found_retention = true;
                    }
                }
            }
            assert!(found_samples, "Should find totalSamples in INFO");
            assert!(found_retention, "Should find retention in INFO");
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_alter() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "temp"]).await;

    // Alter retention
    let response = client
        .command(&["TS.ALTER", "temp", "RETENTION", "3600000"])
        .await;
    assert_eq!(response, Response::ok());

    // Alter labels
    let response = client
        .command(&["TS.ALTER", "temp", "LABELS", "newlabel", "newvalue"])
        .await;
    assert_eq!(response, Response::ok());

    // Verify with INFO
    let response = client.command(&["TS.INFO", "temp"]).await;
    match response {
        Response::Array(arr) => {
            let mut found_retention = false;
            for chunk in arr.chunks(2) {
                if let Response::Bulk(Some(key)) = &chunk[0]
                    && &**key == b"retentionTime"
                    && let Response::Integer(val) = &chunk[1]
                {
                    assert_eq!(*val, 3600000);
                    found_retention = true;
                }
            }
            assert!(found_retention, "Should find retention in INFO");
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_duplicate_policies() {
    let server = start_server().await;
    let mut client = server.connect().await;

    // Test BLOCK policy
    client
        .command(&["TS.CREATE", "test_block", "DUPLICATE_POLICY", "BLOCK"])
        .await;
    client
        .command(&["TS.ADD", "test_block", "1000", "10.0"])
        .await;
    let response = client
        .command(&["TS.ADD", "test_block", "1000", "20.0"])
        .await;
    assert!(matches!(response, Response::Error(_)));

    // Test LAST policy (default)
    client
        .command(&["TS.CREATE", "test_last", "DUPLICATE_POLICY", "LAST"])
        .await;
    client
        .command(&["TS.ADD", "test_last", "1000", "10.0"])
        .await;
    client
        .command(&["TS.ADD", "test_last", "1000", "20.0"])
        .await;
    let response = client.command(&["TS.GET", "test_last"]).await;
    match response {
        Response::Array(arr) => {
            if let Response::Bulk(Some(b)) = &arr[1] {
                assert_eq!(&**b, b"20");
            }
        }
        _ => panic!("Expected array"),
    }

    // Test SUM policy
    client
        .command(&["TS.CREATE", "test_sum", "DUPLICATE_POLICY", "SUM"])
        .await;
    client
        .command(&["TS.ADD", "test_sum", "1000", "10.0"])
        .await;
    client
        .command(&["TS.ADD", "test_sum", "1000", "20.0"])
        .await;
    let response = client.command(&["TS.GET", "test_sum"]).await;
    match response {
        Response::Array(arr) => {
            if let Response::Bulk(Some(b)) = &arr[1] {
                let val: f64 = std::str::from_utf8(b).unwrap().parse().unwrap();
                assert!((val - 30.0).abs() < 0.001);
            }
        }
        _ => panic!("Expected array"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_filter_by_value() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "temp"]).await;

    // Add samples with varying values
    for i in 0..10 {
        let ts = 1000 + i * 100;
        let val = (i + 1) * 10; // 10, 20, 30, ..., 100
        client
            .command(&["TS.ADD", "temp", &ts.to_string(), &val.to_string()])
            .await;
    }

    // Filter by value
    let response = client
        .command(&["TS.RANGE", "temp", "-", "+", "FILTER_BY_VALUE", "30", "60"])
        .await;
    match response {
        Response::Array(arr) => {
            // Should only include values 30, 40, 50, 60
            assert_eq!(arr.len(), 4);
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

// =============================================================================
// TS.QUERYINDEX tests
// =============================================================================

#[tokio::test]
async fn test_ts_queryindex() {
    let server = start_server().await;
    let mut client = server.connect().await;

    // Create series with labels
    client
        .command(&[
            "TS.CREATE",
            "temp:kitchen",
            "LABELS",
            "location",
            "kitchen",
            "type",
            "temperature",
        ])
        .await;
    client
        .command(&[
            "TS.CREATE",
            "temp:bedroom",
            "LABELS",
            "location",
            "bedroom",
            "type",
            "temperature",
        ])
        .await;
    client
        .command(&[
            "TS.CREATE",
            "humidity:kitchen",
            "LABELS",
            "location",
            "kitchen",
            "type",
            "humidity",
        ])
        .await;

    // Query by single filter
    let response = client.command(&["TS.QUERYINDEX", "location=kitchen"]).await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 2);
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    // Query by type
    let response = client.command(&["TS.QUERYINDEX", "type=temperature"]).await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 2);
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_queryindex_empty() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client
        .command(&["TS.CREATE", "temp", "LABELS", "location", "kitchen"])
        .await;

    // No matches
    let response = client.command(&["TS.QUERYINDEX", "location=garage"]).await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 0);
        }
        _ => panic!("Expected empty array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_queryindex_multiple_filters() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "TS.CREATE",
            "s1",
            "LABELS",
            "location",
            "kitchen",
            "type",
            "temp",
        ])
        .await;
    client
        .command(&[
            "TS.CREATE",
            "s2",
            "LABELS",
            "location",
            "kitchen",
            "type",
            "humidity",
        ])
        .await;
    client
        .command(&[
            "TS.CREATE",
            "s3",
            "LABELS",
            "location",
            "bedroom",
            "type",
            "temp",
        ])
        .await;

    // AND logic: both filters must match
    let response = client
        .command(&["TS.QUERYINDEX", "location=kitchen", "type=temp"])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            if let Response::Bulk(Some(key)) = &arr[0] {
                assert_eq!(&**key, b"s1");
            }
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

// =============================================================================
// TS.MGET tests
// =============================================================================

#[tokio::test]
async fn test_ts_mget() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client
        .command(&["TS.CREATE", "temp:a", "LABELS", "type", "temperature"])
        .await;
    client
        .command(&["TS.CREATE", "temp:b", "LABELS", "type", "temperature"])
        .await;

    client.command(&["TS.ADD", "temp:a", "1000", "25.5"]).await;
    client.command(&["TS.ADD", "temp:b", "1000", "22.3"]).await;

    let response = client
        .command(&["TS.MGET", "FILTER", "type=temperature"])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 2);
            // Each entry is [key, labels, [timestamp, value]]
            for entry in &arr {
                if let Response::Array(parts) = entry {
                    assert_eq!(parts.len(), 3);
                    // parts[0] is key
                    assert!(matches!(&parts[0], Response::Bulk(Some(_))));
                    // parts[2] is sample (array of [ts, val])
                    assert!(matches!(&parts[2], Response::Array(_)));
                }
            }
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_mget_withlabels() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "TS.CREATE",
            "temp",
            "LABELS",
            "location",
            "kitchen",
            "type",
            "temp",
        ])
        .await;
    client.command(&["TS.ADD", "temp", "1000", "25.0"]).await;

    let response = client
        .command(&["TS.MGET", "WITHLABELS", "FILTER", "location=kitchen"])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            if let Response::Array(parts) = &arr[0] {
                // parts[1] is labels array
                if let Response::Array(labels) = &parts[1] {
                    assert_eq!(labels.len(), 2); // location + type
                }
            }
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_mget_selected_labels() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "TS.CREATE",
            "temp",
            "LABELS",
            "location",
            "kitchen",
            "type",
            "temp",
            "sensor",
            "DHT22",
        ])
        .await;
    client.command(&["TS.ADD", "temp", "1000", "25.0"]).await;

    let response = client
        .command(&[
            "TS.MGET",
            "SELECTED_LABELS",
            "location",
            "sensor",
            "FILTER",
            "location=kitchen",
        ])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            if let Response::Array(parts) = &arr[0]
                && let Response::Array(labels) = &parts[1]
            {
                assert_eq!(labels.len(), 2); // only location + sensor
            }
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

// =============================================================================
// TS.MRANGE / TS.MREVRANGE tests
// =============================================================================

#[tokio::test]
async fn test_ts_mrange() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client
        .command(&["TS.CREATE", "temp:a", "LABELS", "type", "temp"])
        .await;
    client
        .command(&["TS.CREATE", "temp:b", "LABELS", "type", "temp"])
        .await;

    for i in 0..5 {
        let ts = (1000 + i * 100).to_string();
        let val = format!("{}.0", i + 1);
        client.command(&["TS.ADD", "temp:a", &ts, &val]).await;
        client
            .command(&["TS.ADD", "temp:b", &ts, &format!("{}.0", (i + 1) * 10)])
            .await;
    }

    let response = client
        .command(&["TS.MRANGE", "-", "+", "FILTER", "type=temp"])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 2); // two series
            for entry in &arr {
                if let Response::Array(parts) = entry {
                    assert_eq!(parts.len(), 3); // [key, labels, samples]
                    if let Response::Array(samples) = &parts[2] {
                        assert_eq!(samples.len(), 5);
                    }
                }
            }
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_mrange_with_aggregation() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client
        .command(&["TS.CREATE", "temp", "LABELS", "type", "temp"])
        .await;

    // Add 10 samples at 100ms intervals
    for i in 0..10 {
        let ts = (1000 + i * 100).to_string();
        let val = format!("{}.0", (i + 1) * 10);
        client.command(&["TS.ADD", "temp", &ts, &val]).await;
    }

    // Aggregate with AVG in 500ms buckets
    let response = client
        .command(&[
            "TS.MRANGE",
            "1000",
            "1900",
            "AGGREGATION",
            "AVG",
            "500",
            "FILTER",
            "type=temp",
        ])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1); // one series
            if let Response::Array(parts) = &arr[0]
                && let Response::Array(samples) = &parts[2]
            {
                assert_eq!(samples.len(), 2); // 2 buckets
            }
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_mrevrange() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client
        .command(&["TS.CREATE", "temp", "LABELS", "type", "temp"])
        .await;

    client.command(&["TS.ADD", "temp", "1000", "10.0"]).await;
    client.command(&["TS.ADD", "temp", "2000", "20.0"]).await;
    client.command(&["TS.ADD", "temp", "3000", "30.0"]).await;

    let response = client
        .command(&["TS.MREVRANGE", "-", "+", "FILTER", "type=temp"])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            if let Response::Array(parts) = &arr[0]
                && let Response::Array(samples) = &parts[2]
            {
                assert_eq!(samples.len(), 3);
                // First should be 3000 (reversed)
                if let Response::Array(first) = &samples[0] {
                    assert_eq!(first[0], Response::Integer(3000));
                }
            }
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

// =============================================================================
// TS.CREATERULE / TS.DELETERULE tests
// =============================================================================

#[tokio::test]
async fn test_ts_createrule() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "source"]).await;
    client.command(&["TS.CREATE", "dest"]).await;

    let response = client
        .command(&[
            "TS.CREATERULE",
            "source",
            "dest",
            "AGGREGATION",
            "AVG",
            "5000",
        ])
        .await;
    assert_eq!(response, Response::ok());

    // Verify via TS.INFO
    let response = client.command(&["TS.INFO", "source"]).await;
    match response {
        Response::Array(arr) => {
            let mut found_rules = false;
            for chunk in arr.chunks(2) {
                if let Response::Bulk(Some(key)) = &chunk[0]
                    && &**key == b"rules"
                {
                    found_rules = true;
                    if let Response::Array(rules) = &chunk[1] {
                        assert_eq!(rules.len(), 1);
                    }
                }
            }
            assert!(found_rules, "Should find rules in INFO");
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_createrule_duplicate() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "source"]).await;
    client.command(&["TS.CREATE", "dest"]).await;

    client
        .command(&[
            "TS.CREATERULE",
            "source",
            "dest",
            "AGGREGATION",
            "AVG",
            "5000",
        ])
        .await;

    // Duplicate should error
    let response = client
        .command(&[
            "TS.CREATERULE",
            "source",
            "dest",
            "AGGREGATION",
            "SUM",
            "5000",
        ])
        .await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_deleterule() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "source"]).await;
    client.command(&["TS.CREATE", "dest"]).await;

    client
        .command(&[
            "TS.CREATERULE",
            "source",
            "dest",
            "AGGREGATION",
            "AVG",
            "5000",
        ])
        .await;

    let response = client.command(&["TS.DELETERULE", "source", "dest"]).await;
    assert_eq!(response, Response::ok());

    // Verify removed via TS.INFO
    let response = client.command(&["TS.INFO", "source"]).await;
    match response {
        Response::Array(arr) => {
            for chunk in arr.chunks(2) {
                if let Response::Bulk(Some(key)) = &chunk[0]
                    && &**key == b"rules"
                    && let Response::Array(rules) = &chunk[1]
                {
                    assert_eq!(rules.len(), 0);
                }
            }
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_deleterule_nonexistent() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "source"]).await;

    let response = client
        .command(&["TS.DELETERULE", "source", "nonexistent"])
        .await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ts_createrule_inline_downsample() {
    let server = start_server().await;
    let mut client = server.connect().await;

    client.command(&["TS.CREATE", "source"]).await;
    client.command(&["TS.CREATE", "dest"]).await;

    // Create rule: AVG over 1000ms buckets
    client
        .command(&[
            "TS.CREATERULE",
            "source",
            "dest",
            "AGGREGATION",
            "AVG",
            "1000",
        ])
        .await;

    // Add samples in first bucket (0-999)
    client.command(&["TS.ADD", "source", "0", "10.0"]).await;
    client.command(&["TS.ADD", "source", "500", "20.0"]).await;

    // Cross bucket boundary — this should trigger aggregation of bucket 0
    client.command(&["TS.ADD", "source", "1000", "30.0"]).await;

    // Dest should have one aggregated sample for bucket 0: avg(10, 20) = 15
    let response = client.command(&["TS.RANGE", "dest", "-", "+"]).await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            if let Response::Array(sample) = &arr[0] {
                assert_eq!(sample[0], Response::Integer(0)); // bucket start
                if let Response::Bulk(Some(b)) = &sample[1] {
                    let val: f64 = std::str::from_utf8(b).unwrap().parse().unwrap();
                    assert!((val - 15.0).abs() < 0.001, "Expected 15.0, got {}", val);
                }
            }
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}
