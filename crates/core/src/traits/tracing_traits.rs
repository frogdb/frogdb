//! Distributed tracing traits.
//!
//! These traits define the interface for distributed tracing, allowing
//! requests to be tracked across services.

/// Tracer trait for distributed tracing.
///
/// Implementations create spans that track the execution of operations
/// across the system.
pub trait Tracer: Send + Sync {
    /// Start a new span.
    fn start_span(&self, name: &str) -> Box<dyn Span>;
}

/// A tracing span.
///
/// Spans represent individual operations within a trace. They can have
/// attributes and are ended when the operation completes.
pub trait Span: Send {
    /// Add an attribute to the span.
    fn set_attribute(&mut self, key: &str, value: &str);

    /// End the span.
    fn end(self: Box<Self>);
}

/// Noop tracer.
///
/// Use this when tracing is disabled or for testing.
#[derive(Debug, Default)]
pub struct NoopTracer;

impl NoopTracer {
    /// Create a new noop tracer.
    pub fn new() -> Self {
        Self
    }
}

impl Tracer for NoopTracer {
    fn start_span(&self, name: &str) -> Box<dyn Span> {
        tracing::trace!(name, "Noop span start");
        Box::new(NoopSpan)
    }
}

/// Noop span.
///
/// A span implementation that does nothing.
#[derive(Debug)]
pub struct NoopSpan;

impl Span for NoopSpan {
    fn set_attribute(&mut self, _key: &str, _value: &str) {}

    fn end(self: Box<Self>) {
        tracing::trace!("Noop span end");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_tracer() {
        let tracer = NoopTracer::new();
        let mut span = tracer.start_span("test_operation");
        span.set_attribute("key", "value");
        span.end();
    }
}
