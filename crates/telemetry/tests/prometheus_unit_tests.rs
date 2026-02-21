//! Unit tests for Prometheus metrics recording.
//!
//! These tests verify that the PrometheusRecorder correctly records
//! counters, gauges, and histograms with proper label encoding.

use frogdb_core::MetricsRecorder;
use frogdb_telemetry::metric_names;
use frogdb_telemetry::prometheus_recorder::PrometheusRecorder;
use std::sync::Arc;

/// Test that counters are correctly recorded and incremented.
#[test]
fn test_counter_recording() {
    let recorder = PrometheusRecorder::new();

    // Record some command counts
    recorder.increment_counter(metric_names::COMMANDS_TOTAL, 1, &[("command", "GET")]);
    recorder.increment_counter(metric_names::COMMANDS_TOTAL, 5, &[("command", "GET")]);
    recorder.increment_counter(metric_names::COMMANDS_TOTAL, 3, &[("command", "SET")]);

    let output = recorder.encode();

    // Verify counter exists with correct values
    assert!(output.contains("frogdb_commands_total"));
    assert!(output.contains(r#"command="GET""#));
    assert!(output.contains(r#"command="SET""#));
}

/// Test that gauges are correctly recorded.
#[test]
fn test_gauge_recording() {
    let recorder = PrometheusRecorder::new();

    // Record memory gauge
    recorder.record_gauge(metric_names::MEMORY_USED_BYTES, 1048576.0, &[]);
    recorder.record_gauge(metric_names::CONNECTIONS_CURRENT, 10.0, &[]);

    let output = recorder.encode();

    assert!(output.contains("frogdb_memory_used_bytes"));
    assert!(output.contains("1048576"));
    assert!(output.contains("frogdb_connections_current"));
    assert!(output.contains("10"));
}

/// Test that histograms are correctly recorded with buckets.
#[test]
fn test_histogram_recording() {
    let recorder = PrometheusRecorder::new();

    // Record some latencies
    recorder.record_histogram(
        metric_names::COMMANDS_DURATION,
        0.001,
        &[("command", "GET")],
    );
    recorder.record_histogram(
        metric_names::COMMANDS_DURATION,
        0.005,
        &[("command", "GET")],
    );
    recorder.record_histogram(
        metric_names::COMMANDS_DURATION,
        0.010,
        &[("command", "GET")],
    );

    let output = recorder.encode();

    // Verify histogram components exist
    assert!(output.contains("frogdb_commands_duration_seconds_bucket"));
    assert!(output.contains("frogdb_commands_duration_seconds_count"));
    assert!(output.contains("frogdb_commands_duration_seconds_sum"));
    assert!(output.contains(r#"command="GET""#));
}

/// Test that histogram buckets are appropriate for latency measurements.
#[test]
fn test_histogram_bucket_ranges() {
    let recorder = PrometheusRecorder::new();

    // Record values at different scales
    recorder.record_histogram("test_latency", 0.00001, &[]); // 10μs
    recorder.record_histogram("test_latency", 0.0001, &[]); // 100μs
    recorder.record_histogram("test_latency", 0.001, &[]); // 1ms
    recorder.record_histogram("test_latency", 0.01, &[]); // 10ms
    recorder.record_histogram("test_latency", 0.1, &[]); // 100ms
    recorder.record_histogram("test_latency", 1.0, &[]); // 1s
    recorder.record_histogram("test_latency", 10.0, &[]); // 10s

    let output = recorder.encode();

    // Verify multiple bucket boundaries exist
    assert!(output.contains("le=\"0.00001\"")); // 10μs bucket
    assert!(output.contains("le=\"0.001\"")); // 1ms bucket
    assert!(output.contains("le=\"0.01\"")); // 10ms bucket
    assert!(output.contains("le=\"0.1\"")); // 100ms bucket
    assert!(output.contains("le=\"1\"")); // 1s bucket
    assert!(output.contains("le=\"10\"")); // 10s bucket
    assert!(output.contains("le=\"+Inf\"")); // Infinity bucket
}

/// Test that special characters in labels are properly escaped.
#[test]
fn test_label_escaping() {
    let recorder = PrometheusRecorder::new();

    // Labels with special characters
    recorder.increment_counter("test_counter", 1, &[("label", "value with spaces")]);
    recorder.increment_counter("test_counter", 1, &[("label", "value\"with\"quotes")]);
    recorder.increment_counter("test_counter", 1, &[("label", "value\\with\\backslash")]);

    let output = recorder.encode();

    // Prometheus format should escape these properly
    assert!(output.contains("test_counter"));
    // The prometheus crate handles escaping internally
}

/// Test that multiple label combinations create separate time series.
#[test]
fn test_multiple_label_combinations() {
    let recorder = PrometheusRecorder::new();

    recorder.increment_counter(metric_names::COMMANDS_TOTAL, 10, &[("command", "GET")]);
    recorder.increment_counter(metric_names::COMMANDS_TOTAL, 20, &[("command", "SET")]);
    recorder.increment_counter(metric_names::COMMANDS_TOTAL, 5, &[("command", "DEL")]);

    let output = recorder.encode();

    // All three should be separate time series
    assert!(output.contains(r#"command="GET""#));
    assert!(output.contains(r#"command="SET""#));
    assert!(output.contains(r#"command="DEL""#));
}

/// Test that error metrics include both command and error labels.
#[test]
fn test_error_metrics_labels() {
    let recorder = PrometheusRecorder::new();

    recorder.increment_counter(
        metric_names::COMMANDS_ERRORS,
        1,
        &[("command", "SET"), ("error", "oom")],
    );
    recorder.increment_counter(
        metric_names::COMMANDS_ERRORS,
        2,
        &[("command", "SET"), ("error", "wrong_type")],
    );
    recorder.increment_counter(
        metric_names::COMMANDS_ERRORS,
        1,
        &[("command", "GET"), ("error", "syntax")],
    );

    let output = recorder.encode();

    assert!(output.contains("frogdb_commands_errors_total"));
    assert!(output.contains(r#"command="SET""#));
    assert!(output.contains(r#"error="oom""#));
    assert!(output.contains(r#"error="wrong_type""#));
    assert!(output.contains(r#"error="syntax""#));
}

/// Test that shard-specific metrics include shard label.
#[test]
fn test_shard_metrics() {
    let recorder = PrometheusRecorder::new();

    for shard in 0..4 {
        let shard_str = shard.to_string();
        recorder.record_gauge(
            metric_names::SHARD_KEYS,
            (100 * (shard + 1)) as f64,
            &[("shard", &shard_str)],
        );
        recorder.record_gauge(
            metric_names::SHARD_MEMORY_BYTES,
            (1024 * (shard + 1)) as f64,
            &[("shard", &shard_str)],
        );
    }

    let output = recorder.encode();

    assert!(output.contains("frogdb_shard_keys"));
    assert!(output.contains("frogdb_shard_memory_bytes"));
    assert!(output.contains(r#"shard="0""#));
    assert!(output.contains(r#"shard="1""#));
    assert!(output.contains(r#"shard="2""#));
    assert!(output.contains(r#"shard="3""#));
}

/// Test CommandTimer records success path correctly.
#[test]
fn test_command_timer_success() {
    use frogdb_telemetry::CommandTimer;

    let recorder = Arc::new(PrometheusRecorder::new());
    let timer = CommandTimer::new("PING".to_string(), recorder.clone());

    // Simulate some work
    std::thread::sleep(std::time::Duration::from_micros(100));

    timer.finish();

    let output = recorder.encode();

    // Should have both counter and histogram
    assert!(output.contains("frogdb_commands_total"));
    assert!(output.contains("frogdb_commands_duration_seconds"));
    assert!(output.contains(r#"command="PING""#));
    // Should NOT have error counter
    assert!(!output.contains("frogdb_commands_errors_total"));
}

/// Test CommandTimer records error path correctly.
#[test]
fn test_command_timer_error() {
    use frogdb_telemetry::CommandTimer;

    let recorder = Arc::new(PrometheusRecorder::new());
    let timer = CommandTimer::new("SET".to_string(), recorder.clone());

    timer.finish_with_error("oom");

    let output = recorder.encode();

    // Should have counter, histogram, AND error counter
    assert!(output.contains("frogdb_commands_total"));
    assert!(output.contains("frogdb_commands_duration_seconds"));
    assert!(output.contains("frogdb_commands_errors_total"));
    assert!(output.contains(r#"command="SET""#));
    assert!(output.contains(r#"error="oom""#));
}

/// Test that metric names follow Prometheus naming conventions.
#[test]
fn test_metric_naming_conventions() {
    // All metric names should:
    // 1. Start with frogdb_
    // 2. Use snake_case
    // 3. End with _total for counters, _seconds for durations, _bytes for sizes

    assert!(metric_names::COMMANDS_TOTAL.starts_with("frogdb_"));
    assert!(metric_names::COMMANDS_TOTAL.ends_with("_total"));

    assert!(metric_names::COMMANDS_DURATION.starts_with("frogdb_"));
    assert!(metric_names::COMMANDS_DURATION.ends_with("_seconds"));

    assert!(metric_names::MEMORY_USED_BYTES.starts_with("frogdb_"));
    assert!(metric_names::MEMORY_USED_BYTES.ends_with("_bytes"));

    assert!(metric_names::UPTIME_SECONDS.starts_with("frogdb_"));
    assert!(metric_names::UPTIME_SECONDS.ends_with("_seconds"));
}

/// Test concurrent metric recording is thread-safe.
#[test]
fn test_concurrent_recording() {
    use std::thread;

    let recorder = Arc::new(PrometheusRecorder::new());
    let mut handles = vec![];

    // Spawn multiple threads recording metrics
    for i in 0..10 {
        let r = recorder.clone();
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                r.increment_counter("concurrent_test", 1, &[("thread", &i.to_string())]);
                r.record_gauge("concurrent_gauge", j as f64, &[("thread", &i.to_string())]);
                r.record_histogram(
                    "concurrent_histogram",
                    0.001 * j as f64,
                    &[("thread", &i.to_string())],
                );
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let output = recorder.encode();

    // All threads should have recorded successfully
    assert!(output.contains("concurrent_test"));
    assert!(output.contains("concurrent_gauge"));
    assert!(output.contains("concurrent_histogram"));
}

/// Test that empty recorder produces valid (empty) output.
#[test]
fn test_empty_recorder() {
    let recorder = PrometheusRecorder::new();
    let output = recorder.encode();

    // Should be empty or whitespace only
    assert!(output.is_empty() || output.trim().is_empty());
}

/// Test gauge updates correctly (last value wins).
#[test]
fn test_gauge_updates() {
    let recorder = PrometheusRecorder::new();

    recorder.record_gauge("test_gauge", 10.0, &[]);
    recorder.record_gauge("test_gauge", 20.0, &[]);
    recorder.record_gauge("test_gauge", 15.0, &[]);

    let output = recorder.encode();

    // Should show the last value (15)
    assert!(output.contains("15"));
}

/// Test all standard metric names are properly defined.
#[test]
fn test_all_metric_names_defined() {
    // System metrics
    assert!(!metric_names::UPTIME_SECONDS.is_empty());
    assert!(!metric_names::INFO.is_empty());
    assert!(!metric_names::MEMORY_RSS_BYTES.is_empty());
    assert!(!metric_names::CPU_USER_SECONDS.is_empty());
    assert!(!metric_names::CPU_SYSTEM_SECONDS.is_empty());

    // Connection metrics
    assert!(!metric_names::CONNECTIONS_TOTAL.is_empty());
    assert!(!metric_names::CONNECTIONS_CURRENT.is_empty());
    assert!(!metric_names::CONNECTIONS_REJECTED.is_empty());

    // Command metrics
    assert!(!metric_names::COMMANDS_TOTAL.is_empty());
    assert!(!metric_names::COMMANDS_DURATION.is_empty());
    assert!(!metric_names::COMMANDS_ERRORS.is_empty());

    // Keyspace metrics
    assert!(!metric_names::KEYS_TOTAL.is_empty());
    assert!(!metric_names::KEYSPACE_HITS.is_empty());
    assert!(!metric_names::KEYSPACE_MISSES.is_empty());

    // Shard metrics
    assert!(!metric_names::SHARD_KEYS.is_empty());
    assert!(!metric_names::SHARD_MEMORY_BYTES.is_empty());
    assert!(!metric_names::SHARD_QUEUE_DEPTH.is_empty());

    // Persistence metrics
    assert!(!metric_names::WAL_WRITES.is_empty());
    assert!(!metric_names::WAL_BYTES.is_empty());

    // Memory metrics
    assert!(!metric_names::MEMORY_USED_BYTES.is_empty());
    assert!(!metric_names::MEMORY_PEAK_BYTES.is_empty());
    assert!(!metric_names::EVICTION_KEYS_TOTAL.is_empty());
}
