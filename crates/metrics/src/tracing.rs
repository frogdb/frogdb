//! OpenTelemetry distributed tracing support for FrogDB.
//!
//! This module provides distributed tracing capabilities using OpenTelemetry,
//! enabling request tracing across the system with proper span hierarchy:
//! - Request span (connection handler)
//! - Shard execution span (shard worker)
//! - Store operation span (individual operations)

use crate::config::TracingConfig;
use opentelemetry::{
    global,
    trace::{SpanKind, Status, TraceContextExt, Tracer, TracerProvider as _},
    Context, KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace::{RandomIdGenerator, Sampler, TracerProvider},
    Resource,
};
use std::sync::Arc;

/// Semantic conventions for database operations.
pub mod semantic {
    /// The database system identifier.
    pub const DB_SYSTEM: &str = "db.system";
    /// The database operation being performed.
    pub const DB_OPERATION: &str = "db.operation";
    /// The database statement (command).
    pub const DB_STATEMENT: &str = "db.statement";

    /// FrogDB-specific attributes.
    pub const FROGDB_SHARD_ID: &str = "frogdb.shard_id";
    pub const FROGDB_CONNECTION_ID: &str = "frogdb.connection_id";
    pub const FROGDB_KEY_COUNT: &str = "frogdb.key_count";
}

/// OpenTelemetry tracer wrapper for FrogDB.
pub struct OtelTracer {
    tracer: opentelemetry_sdk::trace::Tracer,
    enabled: bool,
}

impl OtelTracer {
    /// Create a new OpenTelemetry tracer with the given configuration.
    pub fn new(config: &TracingConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if !config.enabled {
            // Return a disabled tracer
            let provider = TracerProvider::builder().build();
            let tracer = provider.tracer("frogdb");
            return Ok(Self {
                tracer,
                enabled: false,
            });
        }

        // Configure the OTLP exporter
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&config.otlp_endpoint)
            .build()?;

        // Configure the sampler based on sampling rate
        let sampler = if config.sampling_rate >= 1.0 {
            Sampler::AlwaysOn
        } else if config.sampling_rate <= 0.0 {
            Sampler::AlwaysOff
        } else {
            Sampler::TraceIdRatioBased(config.sampling_rate)
        };

        // Build the tracer provider
        let provider = TracerProvider::builder()
            .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_sampler(sampler)
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(Resource::new(vec![
                KeyValue::new("service.name", config.service_name.clone()),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            ]))
            .build();

        // Set the global tracer provider
        let _ = global::set_tracer_provider(provider.clone());

        let tracer = provider.tracer("frogdb");

        Ok(Self {
            tracer,
            enabled: true,
        })
    }

    /// Create a tracer for testing that doesn't connect to OTLP.
    ///
    /// This creates an "enabled" tracer that creates real spans but uses
    /// a simple in-memory provider that discards spans. Use this for unit tests.
    #[cfg(test)]
    pub fn new_for_test(config: &TracingConfig) -> Self {
        // Configure the sampler based on sampling rate
        let sampler = if config.sampling_rate >= 1.0 {
            Sampler::AlwaysOn
        } else if config.sampling_rate <= 0.0 {
            Sampler::AlwaysOff
        } else {
            Sampler::TraceIdRatioBased(config.sampling_rate)
        };

        // Build provider WITHOUT exporter - no network, no blocking
        let provider = TracerProvider::builder()
            .with_sampler(sampler)
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(Resource::new(vec![
                KeyValue::new("service.name", config.service_name.clone()),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            ]))
            .build();

        // Don't set global provider in tests (causes race conditions)
        let tracer = provider.tracer("frogdb-test");

        Self {
            tracer,
            enabled: config.enabled,
        }
    }

    /// Check if tracing is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Start a new request span for an incoming command.
    pub fn start_request_span(&self, command: &str, conn_id: u64) -> RequestSpan {
        if !self.enabled {
            return RequestSpan::noop();
        }

        let span = self
            .tracer
            .span_builder(format!("frogdb.request.{}", command.to_lowercase()))
            .with_kind(SpanKind::Server)
            .with_attributes(vec![
                KeyValue::new(semantic::DB_SYSTEM, "frogdb"),
                KeyValue::new(semantic::DB_OPERATION, command.to_string()),
                KeyValue::new(semantic::FROGDB_CONNECTION_ID, conn_id as i64),
            ])
            .start(&self.tracer);

        let context = Context::current_with_span(span);

        RequestSpan {
            context: Some(context),
            tracer: Some(self.tracer.clone()),
        }
    }

    /// Shutdown the tracer and flush pending spans.
    pub fn shutdown(&self) {
        if self.enabled {
            global::shutdown_tracer_provider();
        }
    }
}

/// A span representing a single request lifecycle.
pub struct RequestSpan {
    context: Option<Context>,
    tracer: Option<opentelemetry_sdk::trace::Tracer>,
}

impl RequestSpan {
    /// Create a no-op span when tracing is disabled.
    fn noop() -> Self {
        Self {
            context: None,
            tracer: None,
        }
    }

    /// Get the span context for propagation to child spans.
    pub fn context(&self) -> Option<&Context> {
        self.context.as_ref()
    }

    /// Add an attribute to the span.
    pub fn set_attribute(&self, key: &str, value: impl Into<opentelemetry::Value>) {
        if let Some(ref ctx) = self.context {
            ctx.span().set_attribute(KeyValue::new(key.to_string(), value.into()));
        }
    }

    /// Record that this span resulted in an error.
    pub fn set_error(&self, message: &str) {
        if let Some(ref ctx) = self.context {
            ctx.span().set_status(Status::error(message.to_string()));
        }
    }

    /// Record success status.
    pub fn set_ok(&self) {
        if let Some(ref ctx) = self.context {
            ctx.span().set_status(Status::Ok);
        }
    }

    /// Start a child span for shard execution.
    pub fn start_shard_span(&self, shard_id: usize) -> ShardSpan {
        if let (Some(ref ctx), Some(ref tracer)) = (&self.context, &self.tracer) {
            let _guard = ctx.clone().attach();
            let span = tracer
                .span_builder(format!("frogdb.shard.{}", shard_id))
                .with_kind(SpanKind::Internal)
                .with_attributes(vec![
                    KeyValue::new(semantic::DB_SYSTEM, "frogdb"),
                    KeyValue::new(semantic::FROGDB_SHARD_ID, shard_id as i64),
                ])
                .start(tracer);

            let child_context = Context::current_with_span(span);

            ShardSpan {
                context: Some(child_context),
                tracer: Some(tracer.clone()),
            }
        } else {
            ShardSpan::noop()
        }
    }

    /// End the span.
    pub fn end(self) {
        if let Some(ctx) = self.context {
            ctx.span().end();
        }
    }
}

/// A span representing shard-level execution.
pub struct ShardSpan {
    context: Option<Context>,
    tracer: Option<opentelemetry_sdk::trace::Tracer>,
}

impl ShardSpan {
    /// Create a no-op span.
    fn noop() -> Self {
        Self {
            context: None,
            tracer: None,
        }
    }

    /// Get the span context.
    pub fn context(&self) -> Option<&Context> {
        self.context.as_ref()
    }

    /// Add an attribute.
    pub fn set_attribute(&self, key: &str, value: impl Into<opentelemetry::Value>) {
        if let Some(ref ctx) = self.context {
            ctx.span().set_attribute(KeyValue::new(key.to_string(), value.into()));
        }
    }

    /// Record an error.
    pub fn set_error(&self, message: &str) {
        if let Some(ref ctx) = self.context {
            ctx.span().set_status(Status::error(message.to_string()));
        }
    }

    /// Start a child span for a store operation.
    pub fn start_store_span(&self, operation: &str) -> StoreSpan {
        if let (Some(ref ctx), Some(ref tracer)) = (&self.context, &self.tracer) {
            let _guard = ctx.clone().attach();
            let span = tracer
                .span_builder(format!("frogdb.store.{}", operation.to_lowercase()))
                .with_kind(SpanKind::Internal)
                .with_attributes(vec![
                    KeyValue::new(semantic::DB_SYSTEM, "frogdb"),
                    KeyValue::new(semantic::DB_OPERATION, operation.to_string()),
                ])
                .start(tracer);

            let child_context = Context::current_with_span(span);

            StoreSpan {
                context: Some(child_context),
            }
        } else {
            StoreSpan::noop()
        }
    }

    /// End the span.
    pub fn end(self) {
        if let Some(ctx) = self.context {
            ctx.span().end();
        }
    }
}

/// A span representing a store-level operation.
pub struct StoreSpan {
    context: Option<Context>,
}

impl StoreSpan {
    /// Create a no-op span.
    fn noop() -> Self {
        Self { context: None }
    }

    /// Add an attribute.
    pub fn set_attribute(&self, key: &str, value: impl Into<opentelemetry::Value>) {
        if let Some(ref ctx) = self.context {
            ctx.span().set_attribute(KeyValue::new(key.to_string(), value.into()));
        }
    }

    /// Record an error.
    pub fn set_error(&self, message: &str) {
        if let Some(ref ctx) = self.context {
            ctx.span().set_status(Status::error(message.to_string()));
        }
    }

    /// End the span.
    pub fn end(self) {
        if let Some(ctx) = self.context {
            ctx.span().end();
        }
    }
}

/// A scatter-gather span that tracks multi-shard operations.
pub struct ScatterGatherSpan {
    context: Option<Context>,
    tracer: Option<opentelemetry_sdk::trace::Tracer>,
}

impl ScatterGatherSpan {
    /// Create a no-op span.
    pub fn noop() -> Self {
        Self {
            context: None,
            tracer: None,
        }
    }

    /// Create a new scatter-gather span from an OtelTracer.
    pub fn new(tracer: &OtelTracer, operation: &str, key_count: usize) -> Self {
        if !tracer.enabled {
            return Self::noop();
        }

        let span = tracer
            .tracer
            .span_builder(format!("frogdb.scatter_gather.{}", operation.to_lowercase()))
            .with_kind(SpanKind::Internal)
            .with_attributes(vec![
                KeyValue::new(semantic::DB_SYSTEM, "frogdb"),
                KeyValue::new(semantic::DB_OPERATION, operation.to_string()),
                KeyValue::new(semantic::FROGDB_KEY_COUNT, key_count as i64),
            ])
            .start(&tracer.tracer);

        let context = Context::current_with_span(span);

        Self {
            context: Some(context),
            tracer: Some(tracer.tracer.clone()),
        }
    }

    /// Start a child span for a specific shard in the scatter-gather.
    pub fn start_shard_span(&self, shard_id: usize, key_count: usize) -> ShardSpan {
        if let (Some(ref ctx), Some(ref tracer)) = (&self.context, &self.tracer) {
            let _guard = ctx.clone().attach();
            let span = tracer
                .span_builder(format!("frogdb.scatter.shard_{}", shard_id))
                .with_kind(SpanKind::Internal)
                .with_attributes(vec![
                    KeyValue::new(semantic::FROGDB_SHARD_ID, shard_id as i64),
                    KeyValue::new(semantic::FROGDB_KEY_COUNT, key_count as i64),
                ])
                .start(tracer);

            let child_context = Context::current_with_span(span);

            ShardSpan {
                context: Some(child_context),
                tracer: Some(tracer.clone()),
            }
        } else {
            ShardSpan::noop()
        }
    }

    /// Add an attribute.
    pub fn set_attribute(&self, key: &str, value: impl Into<opentelemetry::Value>) {
        if let Some(ref ctx) = self.context {
            ctx.span().set_attribute(KeyValue::new(key.to_string(), value.into()));
        }
    }

    /// Record an error.
    pub fn set_error(&self, message: &str) {
        if let Some(ref ctx) = self.context {
            ctx.span().set_status(Status::error(message.to_string()));
        }
    }

    /// End the span.
    pub fn end(self) {
        if let Some(ctx) = self.context {
            ctx.span().end();
        }
    }
}

/// A thread-safe wrapper for OtelTracer.
pub type SharedTracer = Arc<OtelTracer>;

/// Create a shared tracer from configuration.
pub fn create_tracer(config: &TracingConfig) -> Result<SharedTracer, Box<dyn std::error::Error + Send + Sync>> {
    let tracer = OtelTracer::new(config)?;
    Ok(Arc::new(tracer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_tracer() {
        let config = TracingConfig::default(); // disabled by default
        let tracer = OtelTracer::new(&config).unwrap();
        assert!(!tracer.is_enabled());

        // Should create noop spans
        let span = tracer.start_request_span("GET", 1);
        assert!(span.context().is_none());
    }

    #[test]
    fn test_noop_span_operations() {
        let span = RequestSpan::noop();
        // These should not panic
        span.set_attribute("test", "value");
        span.set_error("error");
        span.set_ok();
        let child = span.start_shard_span(0);
        child.set_attribute("test", "value");
        child.end();
        span.end();
    }

    #[test]
    fn test_scatter_gather_noop() {
        let config = TracingConfig::default();
        let tracer = OtelTracer::new(&config).unwrap();
        let span = ScatterGatherSpan::new(&tracer, "MGET", 5);
        span.set_attribute("test", "value");
        let shard_span = span.start_shard_span(0, 2);
        shard_span.end();
        span.end();
    }

    // ===== Enabled Tracer Tests =====
    // These tests use new_for_test() to create tracers without OTLP connections,
    // allowing us to test the enabled code path without network dependencies.

    #[test]
    fn test_enabled_tracer_with_always_off_sampler() {
        // sampling_rate = 0.0 creates AlwaysOff sampler
        // Tracer is "enabled" but spans aren't exported
        let config = TracingConfig {
            enabled: true,
            sampling_rate: 0.0,
            ..Default::default()
        };
        let tracer = OtelTracer::new_for_test(&config);
        assert!(tracer.is_enabled());
    }

    #[test]
    fn test_enabled_tracer_creates_spans_with_context() {
        let config = TracingConfig {
            enabled: true,
            sampling_rate: 0.0,
            ..Default::default()
        };
        let tracer = OtelTracer::new_for_test(&config);
        let span = tracer.start_request_span("GET", 1);
        // With enabled tracer, context should be Some even with AlwaysOff
        // (span exists, just not exported)
        assert!(span.context().is_some());
        span.end();
    }

    #[test]
    fn test_request_span_creates_shard_child() {
        let config = TracingConfig {
            enabled: true,
            sampling_rate: 0.0,
            ..Default::default()
        };
        let tracer = OtelTracer::new_for_test(&config);
        let request_span = tracer.start_request_span("SET", 42);

        let shard_span = request_span.start_shard_span(0);
        assert!(shard_span.context().is_some());
        shard_span.end();
        request_span.end();
    }

    #[test]
    fn test_shard_span_creates_store_child() {
        let config = TracingConfig {
            enabled: true,
            sampling_rate: 0.0,
            ..Default::default()
        };
        let tracer = OtelTracer::new_for_test(&config);
        let request_span = tracer.start_request_span("GET", 1);
        let shard_span = request_span.start_shard_span(0);

        let store_span = shard_span.start_store_span("get");
        // Store span should have context
        store_span.set_attribute("key", "test_key");
        store_span.end();

        shard_span.end();
        request_span.end();
    }

    #[test]
    fn test_span_attribute_operations() {
        let config = TracingConfig {
            enabled: true,
            sampling_rate: 0.0,
            ..Default::default()
        };
        let tracer = OtelTracer::new_for_test(&config);
        let span = tracer.start_request_span("MGET", 1);

        // These should not panic on enabled spans
        span.set_attribute("db.statement", "MGET key1 key2");
        span.set_attribute("frogdb.key_count", 2i64);
        span.set_ok();
        span.end();
    }

    #[test]
    fn test_span_error_status() {
        let config = TracingConfig {
            enabled: true,
            sampling_rate: 0.0,
            ..Default::default()
        };
        let tracer = OtelTracer::new_for_test(&config);
        let span = tracer.start_request_span("SET", 1);

        span.set_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        span.end();
    }

    #[test]
    fn test_scatter_gather_span_enabled() {
        let config = TracingConfig {
            enabled: true,
            sampling_rate: 0.0,
            ..Default::default()
        };
        let tracer = OtelTracer::new_for_test(&config);

        let sg_span = ScatterGatherSpan::new(&tracer, "MGET", 5);
        assert!(sg_span.context.is_some());

        // Create child shard spans
        let shard0 = sg_span.start_shard_span(0, 2);
        let shard1 = sg_span.start_shard_span(1, 3);

        shard0.end();
        shard1.end();
        sg_span.end();
    }

    // ===== Sampling Configuration Tests =====

    #[test]
    fn test_sampler_always_on_config() {
        let config = TracingConfig {
            enabled: true,
            sampling_rate: 1.0,
            ..Default::default()
        };
        // This tests the config path - actual sampler behavior is internal
        let tracer = OtelTracer::new_for_test(&config);
        assert!(tracer.is_enabled());
    }

    #[test]
    fn test_sampler_ratio_based_config() {
        let config = TracingConfig {
            enabled: true,
            sampling_rate: 0.5,
            ..Default::default()
        };
        let tracer = OtelTracer::new_for_test(&config);
        assert!(tracer.is_enabled());
    }

    // ===== Semantic Convention Tests =====

    #[test]
    fn test_semantic_conventions_values() {
        assert_eq!(semantic::DB_SYSTEM, "db.system");
        assert_eq!(semantic::DB_OPERATION, "db.operation");
        assert_eq!(semantic::DB_STATEMENT, "db.statement");
        assert_eq!(semantic::FROGDB_SHARD_ID, "frogdb.shard_id");
        assert_eq!(semantic::FROGDB_CONNECTION_ID, "frogdb.connection_id");
        assert_eq!(semantic::FROGDB_KEY_COUNT, "frogdb.key_count");
    }

    #[test]
    fn test_create_tracer_helper() {
        let config = TracingConfig::default();
        let tracer = create_tracer(&config).unwrap();
        assert!(!tracer.is_enabled()); // Default is disabled
    }

    // ===== Store Span Tests =====

    #[test]
    fn test_store_span_noop_operations() {
        let span = StoreSpan::noop();
        // These should not panic on noop spans
        span.set_attribute("key", "test_key");
        span.set_error("test error");
        span.end();
    }

    #[test]
    fn test_shard_span_noop() {
        let span = ShardSpan::noop();
        assert!(span.context().is_none());
        span.set_attribute("test", "value");
        span.set_error("test error");
        let store = span.start_store_span("get");
        store.end();
        span.end();
    }
}
