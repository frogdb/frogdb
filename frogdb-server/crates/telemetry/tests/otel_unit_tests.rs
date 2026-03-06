//! Unit tests for OpenTelemetry tracing and metrics.
//!
//! These tests verify that the OtelRecorder and CompositeRecorder
//! work correctly without requiring an actual OTLP collector.

use frogdb_core::{MetricsRecorder, NoopMetricsRecorder};
use frogdb_telemetry::config::MetricsConfig;
use frogdb_telemetry::otlp::{CompositeRecorder, OtlpRecorder};
use frogdb_telemetry::prometheus_recorder::PrometheusRecorder;
use std::sync::Arc;

/// Test that OTLP recorder returns None when disabled.
#[test]
fn test_otlp_disabled_returns_none() {
    let config = MetricsConfig {
        enabled: true,
        otlp_enabled: false,
        ..Default::default()
    };

    let recorder = OtlpRecorder::new(&config);
    assert!(recorder.is_none());
}

/// Test that CompositeRecorder distributes to all backends.
#[test]
fn test_composite_recorder_distributes() {
    let prom = Arc::new(PrometheusRecorder::new());
    let prom_trait = prom.clone() as Arc<dyn MetricsRecorder>;
    let noop = Arc::new(NoopMetricsRecorder::new()) as Arc<dyn MetricsRecorder>;

    let composite = CompositeRecorder::new(vec![prom_trait, noop]);

    // Record through composite
    composite.increment_counter("test_counter", 5, &[("label", "value")]);
    composite.record_gauge("test_gauge", 42.0, &[]);
    composite.record_histogram("test_histogram", 0.123, &[]);

    // Verify prometheus backend received the metrics
    let output = prom.encode();
    assert!(output.contains("test_counter"));
    assert!(output.contains("test_gauge"));
    assert!(output.contains("test_histogram"));
}

/// Test that CompositeRecorder handles empty backend list.
#[test]
fn test_composite_recorder_empty() {
    let composite = CompositeRecorder::new(vec![]);

    // Should not panic with no backends
    composite.increment_counter("test", 1, &[]);
    composite.record_gauge("test", 1.0, &[]);
    composite.record_histogram("test", 0.1, &[]);
}

/// Test that CompositeRecorder preserves labels correctly.
#[test]
fn test_composite_recorder_labels() {
    let prom = Arc::new(PrometheusRecorder::new());
    let prom_trait = prom.clone() as Arc<dyn MetricsRecorder>;

    let composite = CompositeRecorder::new(vec![prom_trait]);

    // Record with multiple labels
    composite.increment_counter(
        "multi_label_counter",
        1,
        &[("command", "GET"), ("shard", "0"), ("status", "ok")],
    );

    let output = prom.encode();
    assert!(output.contains("multi_label_counter"));
    assert!(output.contains(r#"command="GET""#));
    assert!(output.contains(r#"shard="0""#));
    assert!(output.contains(r#"status="ok""#));
}

/// Test that NoopMetricsRecorder has zero overhead.
#[test]
fn test_noop_recorder_no_overhead() {
    let recorder = NoopMetricsRecorder::new();

    // These should be no-ops
    for _ in 0..1000 {
        recorder.increment_counter("test", 1, &[("a", "b"), ("c", "d")]);
        recorder.record_gauge("test", 1.0, &[("x", "y")]);
        recorder.record_histogram("test", 0.001, &[("cmd", "GET")]);
    }

    // If we got here without issues, the noop is working correctly
}

/// Test that multiple CompositeRecorders can be nested.
#[test]
fn test_nested_composite_recorders() {
    let prom1 = Arc::new(PrometheusRecorder::new());
    let prom2 = Arc::new(PrometheusRecorder::new());

    let inner = Arc::new(CompositeRecorder::new(vec![
        prom1.clone() as Arc<dyn MetricsRecorder>
    ])) as Arc<dyn MetricsRecorder>;

    let outer = CompositeRecorder::new(vec![inner, prom2.clone() as Arc<dyn MetricsRecorder>]);

    outer.increment_counter("nested_test", 1, &[]);

    // Both recorders should have the metric
    assert!(prom1.encode().contains("nested_test"));
    assert!(prom2.encode().contains("nested_test"));
}

/// Test that metrics config defaults are sensible.
#[test]
fn test_metrics_config_defaults() {
    let config = MetricsConfig::default();

    // Should be enabled by default
    assert!(config.enabled);

    // OTLP should be disabled by default (requires explicit opt-in)
    assert!(!config.otlp_enabled);

    // Should have reasonable defaults
    assert!(config.port > 0);
    assert!(config.otlp_interval_secs > 0);
}

/// Test semantic attributes for database operations.
/// Note: Prometheus label names cannot contain dots, so we use underscores.
#[test]
fn test_semantic_attributes() {
    let recorder = PrometheusRecorder::new();

    // Record with OpenTelemetry-style semantic convention attributes
    // (using underscores instead of dots for Prometheus compatibility)
    recorder.increment_counter(
        "db_operation",
        1,
        &[("db_system", "frogdb"), ("db_operation", "GET")],
    );

    let output = recorder.encode();
    assert!(output.contains(r#"db_system="frogdb""#));
    assert!(output.contains(r#"db_operation="GET""#));
}

/// Test that histograms record count and sum correctly.
#[test]
fn test_histogram_count_and_sum() {
    let recorder = PrometheusRecorder::new();

    // Record specific values
    recorder.record_histogram("test_hist", 1.0, &[]);
    recorder.record_histogram("test_hist", 2.0, &[]);
    recorder.record_histogram("test_hist", 3.0, &[]);

    let output = recorder.encode();

    // Should have count of 3 and sum of 6
    assert!(output.contains("test_hist_count 3"));
    assert!(output.contains("test_hist_sum 6"));
}

/// Test concurrent access to CompositeRecorder.
#[test]
fn test_composite_recorder_concurrent() {
    use std::thread;

    let prom = Arc::new(PrometheusRecorder::new());
    let composite = Arc::new(CompositeRecorder::new(vec![
        prom.clone() as Arc<dyn MetricsRecorder>
    ]));

    let mut handles = vec![];

    for i in 0..5 {
        let c = composite.clone();
        handles.push(thread::spawn(move || {
            for j in 0..50 {
                c.increment_counter("concurrent", 1, &[("thread", &i.to_string())]);
                c.record_histogram("concurrent_hist", j as f64 * 0.001, &[]);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let output = prom.encode();
    assert!(output.contains("concurrent"));
    assert!(output.contains("concurrent_hist"));
}
