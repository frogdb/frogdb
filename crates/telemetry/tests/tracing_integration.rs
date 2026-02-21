//! Integration tests for distributed tracing.
//!
//! Tests are organized into two categories:
//! 1. Behavior tests - verify spans are created with correct content
//! 2. Config tests - verify config flags control span emission

use frogdb_telemetry::{ScatterGatherSpan, TestTracer, TracingConfig};
use std::collections::HashMap;

// =============================================================================
// BEHAVIOR TESTS - Verify correct spans for each behavior (all spans enabled)
// =============================================================================

#[test]
fn test_request_span_attributes() {
    let test_tracer = TestTracer::new_all_enabled();

    let span = test_tracer.tracer.start_request_span("GET", 42);
    span.set_ok();
    span.end();

    let spans = test_tracer.get_finished_spans();
    assert_eq!(spans.len(), 1, "Should create exactly one request span");

    let span = &spans[0];
    assert!(
        span.name.as_ref().contains("get"),
        "Span name should contain command: {}",
        span.name
    );

    // Verify semantic attributes
    let attrs: HashMap<_, _> = span
        .attributes
        .iter()
        .map(|kv| (kv.key.as_str(), &kv.value))
        .collect();

    assert_eq!(
        attrs.get("db.system").map(|v| v.as_str()),
        Some("frogdb".into()),
        "Should have db.system attribute"
    );
    assert_eq!(
        attrs.get("db.operation").map(|v| v.as_str()),
        Some("GET".into()),
        "Should have db.operation attribute"
    );
    assert_eq!(
        attrs.get("frogdb.connection_id").map(|v| match v {
            opentelemetry::Value::I64(i) => Some(*i),
            _ => None,
        }),
        Some(Some(42)),
        "Should have connection_id attribute"
    );
}

#[test]
fn test_request_span_error_status() {
    let test_tracer = TestTracer::new_all_enabled();

    let span = test_tracer.tracer.start_request_span("SET", 1);
    span.set_error("WRONGTYPE Operation against a key holding the wrong kind of value");
    span.end();

    let spans = test_tracer.get_finished_spans();
    assert_eq!(spans.len(), 1);

    let span = &spans[0];
    // Verify error status is set (status will be Error)
    assert!(
        matches!(span.status, opentelemetry::trace::Status::Error { .. }),
        "Span should have error status"
    );
}

#[test]
fn test_request_span_ok_status() {
    let test_tracer = TestTracer::new_all_enabled();

    let span = test_tracer.tracer.start_request_span("PING", 1);
    span.set_ok();
    span.end();

    let spans = test_tracer.get_finished_spans();
    assert_eq!(spans.len(), 1);

    let span = &spans[0];
    assert!(
        matches!(span.status, opentelemetry::trace::Status::Ok),
        "Span should have OK status"
    );
}

#[test]
fn test_shard_span_is_child_of_request() {
    let test_tracer = TestTracer::new_all_enabled();

    let request_span = test_tracer.tracer.start_request_span("GET", 10);
    let shard_span = request_span.start_shard_span(0);
    shard_span.end();
    request_span.end();

    let spans = test_tracer.get_finished_spans();
    assert_eq!(spans.len(), 2, "Should have request and shard spans");

    // Verify parent-child relationship
    let shard = spans
        .iter()
        .find(|s| s.name.as_ref().contains("shard"))
        .expect("Should have shard span");
    let request = spans
        .iter()
        .find(|s| s.name.as_ref().contains("request"))
        .expect("Should have request span");

    assert_eq!(
        shard.parent_span_id,
        request.span_context.span_id(),
        "Shard span should be child of request span"
    );
}

#[test]
fn test_shard_span_attributes() {
    let test_tracer = TestTracer::new_all_enabled();

    let request_span = test_tracer.tracer.start_request_span("SET", 5);
    let shard_span = request_span.start_shard_span(2);
    shard_span.end();
    request_span.end();

    let spans = test_tracer.get_finished_spans();
    let shard = spans
        .iter()
        .find(|s| s.name.as_ref().contains("shard"))
        .expect("Should have shard span");

    let attrs: HashMap<_, _> = shard
        .attributes
        .iter()
        .map(|kv| (kv.key.as_str(), &kv.value))
        .collect();

    assert_eq!(
        attrs.get("frogdb.shard_id").map(|v| match v {
            opentelemetry::Value::I64(i) => Some(*i),
            _ => None,
        }),
        Some(Some(2)),
        "Should have shard_id attribute set to 2"
    );
}

#[test]
fn test_store_span_is_child_of_shard() {
    let test_tracer = TestTracer::new_all_enabled();

    let request_span = test_tracer.tracer.start_request_span("GET", 1);
    let shard_span = request_span.start_shard_span(0);
    let store_span = shard_span.start_store_span("get");
    store_span.end();
    shard_span.end();
    request_span.end();

    let spans = test_tracer.get_finished_spans();
    assert_eq!(
        spans.len(),
        3,
        "Should have request, shard, and store spans"
    );

    // Verify store is child of shard
    let store = spans
        .iter()
        .find(|s| s.name.as_ref().contains("store"))
        .expect("Should have store span");
    let shard = spans
        .iter()
        .find(|s| s.name.as_ref().contains("shard"))
        .expect("Should have shard span");

    assert_eq!(
        store.parent_span_id,
        shard.span_context.span_id(),
        "Store span should be child of shard span"
    );
}

#[test]
fn test_store_span_attributes() {
    let test_tracer = TestTracer::new_all_enabled();

    let request_span = test_tracer.tracer.start_request_span("GET", 1);
    let shard_span = request_span.start_shard_span(0);
    let store_span = shard_span.start_store_span("get");
    store_span.end();
    shard_span.end();
    request_span.end();

    let spans = test_tracer.get_finished_spans();
    let store = spans
        .iter()
        .find(|s| s.name.as_ref().contains("store"))
        .expect("Should have store span");

    let attrs: HashMap<_, _> = store
        .attributes
        .iter()
        .map(|kv| (kv.key.as_str(), &kv.value))
        .collect();

    assert_eq!(
        attrs.get("db.system").map(|v| v.as_str()),
        Some("frogdb".into()),
        "Store span should have db.system attribute"
    );
    assert_eq!(
        attrs.get("db.operation").map(|v| v.as_str()),
        Some("get".into()),
        "Store span should have db.operation attribute"
    );
}

#[test]
fn test_scatter_gather_span_attributes() {
    let test_tracer = TestTracer::new_all_enabled();

    let sg_span = ScatterGatherSpan::new(&test_tracer.tracer, "MGET", 5);
    sg_span.end();

    let spans = test_tracer.get_finished_spans();
    assert_eq!(spans.len(), 1);

    let attrs: HashMap<_, _> = spans[0]
        .attributes
        .iter()
        .map(|kv| (kv.key.as_str(), &kv.value))
        .collect();

    assert_eq!(
        attrs.get("frogdb.key_count").map(|v| match v {
            opentelemetry::Value::I64(i) => Some(*i),
            _ => None,
        }),
        Some(Some(5)),
        "Should have key_count attribute set to 5"
    );
}

#[test]
fn test_scatter_gather_child_shard_spans() {
    let test_tracer = TestTracer::new_all_enabled();

    let sg_span = ScatterGatherSpan::new(&test_tracer.tracer, "MGET", 10);
    let shard0 = sg_span.start_shard_span(0, 3);
    let shard1 = sg_span.start_shard_span(1, 7);
    shard0.end();
    shard1.end();
    sg_span.end();

    let spans = test_tracer.get_finished_spans();
    assert_eq!(
        spans.len(),
        3,
        "Should have scatter-gather span and 2 shard spans"
    );

    // Find the scatter-gather span
    let sg = spans
        .iter()
        .find(|s| s.name.as_ref().contains("scatter_gather"))
        .expect("Should have scatter-gather span");

    // Verify shard spans are children of scatter-gather span
    let shard_spans: Vec<_> = spans
        .iter()
        .filter(|s| s.name.as_ref().contains("scatter.shard"))
        .collect();

    assert_eq!(shard_spans.len(), 2, "Should have 2 shard child spans");

    for shard in shard_spans {
        assert_eq!(
            shard.parent_span_id,
            sg.span_context.span_id(),
            "Shard span should be child of scatter-gather span"
        );
    }
}

#[test]
fn test_full_span_hierarchy() {
    let test_tracer = TestTracer::new_all_enabled();

    // Create a full hierarchy: request -> shard -> store
    let request = test_tracer.tracer.start_request_span("SET", 100);
    let shard = request.start_shard_span(5);
    let store = shard.start_store_span("set");

    // End in reverse order
    store.end();
    shard.end();
    request.end();

    let spans = test_tracer.get_finished_spans();
    assert_eq!(spans.len(), 3, "Should have 3 spans in hierarchy");

    // Find each span by name
    let request_span = spans
        .iter()
        .find(|s| s.name.as_ref().contains("request"))
        .expect("Should have request span");
    let shard_span = spans
        .iter()
        .find(|s| s.name.as_ref().contains("shard.5"))
        .expect("Should have shard span");
    let store_span = spans
        .iter()
        .find(|s| s.name.as_ref().contains("store"))
        .expect("Should have store span");

    // Verify hierarchy
    assert_eq!(
        shard_span.parent_span_id,
        request_span.span_context.span_id(),
        "Shard should be child of request"
    );
    assert_eq!(
        store_span.parent_span_id,
        shard_span.span_context.span_id(),
        "Store should be child of shard"
    );
}

#[test]
fn test_reset_clears_spans() {
    let test_tracer = TestTracer::new_all_enabled();

    let span = test_tracer.tracer.start_request_span("GET", 1);
    span.end();

    assert_eq!(test_tracer.get_finished_spans().len(), 1);

    test_tracer.reset();
    assert_eq!(
        test_tracer.get_finished_spans().len(),
        0,
        "Reset should clear all spans"
    );
}

// =============================================================================
// CONFIG TESTS - Verify config flags control span emission
// =============================================================================

#[test]
fn test_no_spans_when_tracing_disabled() {
    let config = TracingConfig::default(); // enabled: false by default
    let test_tracer = TestTracer::new(&config);

    let span = test_tracer.tracer.start_request_span("GET", 42);
    span.end();

    let spans = test_tracer.get_finished_spans();
    assert!(
        spans.is_empty(),
        "No spans should be created when tracing is disabled"
    );
}

#[test]
fn test_no_spans_when_sampling_rate_zero() {
    let config = TracingConfig {
        enabled: true,
        sampling_rate: 0.0, // Sample none
        ..Default::default()
    };
    let test_tracer = TestTracer::new(&config);

    let span = test_tracer.tracer.start_request_span("GET", 42);
    span.end();

    let spans = test_tracer.get_finished_spans();
    assert!(
        spans.is_empty(),
        "No spans should be created with sampling_rate=0.0"
    );
}

#[test]
fn test_spans_created_when_enabled_with_full_sampling() {
    let config = TracingConfig {
        enabled: true,
        sampling_rate: 1.0,
        ..Default::default()
    };
    let test_tracer = TestTracer::new(&config);

    let span = test_tracer.tracer.start_request_span("GET", 42);
    span.end();

    let spans = test_tracer.get_finished_spans();
    assert_eq!(spans.len(), 1, "Span should be created when enabled");
}

#[test]
fn test_custom_service_name_in_resource() {
    let config = TracingConfig {
        enabled: true,
        sampling_rate: 1.0,
        service_name: "my-custom-service".to_string(),
        ..Default::default()
    };
    let test_tracer = TestTracer::new(&config);

    let span = test_tracer.tracer.start_request_span("PING", 1);
    span.end();

    let spans = test_tracer.get_finished_spans();
    assert_eq!(
        spans.len(),
        1,
        "Should create span with custom service name"
    );

    // Note: Resource attributes are not on individual spans but on the tracer provider.
    // We verify the tracer was created successfully with the custom config.
}

#[test]
fn test_noop_spans_have_no_context_when_disabled() {
    let config = TracingConfig {
        enabled: false,
        ..Default::default()
    };
    let test_tracer = TestTracer::new(&config);

    let span = test_tracer.tracer.start_request_span("GET", 1);
    assert!(
        span.context().is_none(),
        "Disabled tracer should produce noop spans with no context"
    );
}

#[test]
fn test_enabled_spans_have_context() {
    let config = TracingConfig {
        enabled: true,
        sampling_rate: 1.0,
        ..Default::default()
    };
    let test_tracer = TestTracer::new(&config);

    let span = test_tracer.tracer.start_request_span("GET", 1);
    assert!(
        span.context().is_some(),
        "Enabled tracer should produce spans with context"
    );
    span.end();
}

#[test]
fn test_child_spans_not_created_when_parent_disabled() {
    let config = TracingConfig::default(); // disabled
    let test_tracer = TestTracer::new(&config);

    let request = test_tracer.tracer.start_request_span("GET", 1);
    let shard = request.start_shard_span(0);
    let store = shard.start_store_span("get");

    store.end();
    shard.end();
    request.end();

    let spans = test_tracer.get_finished_spans();
    assert!(
        spans.is_empty(),
        "No child spans should be created when tracing is disabled"
    );
}

#[test]
fn test_multiple_operations_produce_independent_traces() {
    let test_tracer = TestTracer::new_all_enabled();

    // First operation
    let span1 = test_tracer.tracer.start_request_span("GET", 1);
    span1.set_ok();
    span1.end();

    // Second operation
    let span2 = test_tracer.tracer.start_request_span("SET", 2);
    span2.set_ok();
    span2.end();

    let spans = test_tracer.get_finished_spans();
    assert_eq!(spans.len(), 2, "Should have two independent spans");

    // Verify they have different trace IDs (independent traces)
    assert_ne!(
        spans[0].span_context.trace_id(),
        spans[1].span_context.trace_id(),
        "Different requests should have different trace IDs"
    );
}
