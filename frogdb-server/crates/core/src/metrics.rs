//! Metrics and observability abstractions.
//!
//! This module defines traits for observability features so that the server
//! crate can depend on abstract interfaces rather than concrete types from
//! the metrics crate.
//!
//! # Example
//!
//! ```rust,ignore
//! use frogdb_core::metrics::{ObservabilityConfig, MemoryReport};
//!
//! async fn run_diagnostics(config: &dyn ObservabilityConfig) {
//!     if let Some(collector) = config.memory_diagnostics() {
//!         let report = collector.collect().await;
//!         println!("{}", report.format());
//!     }
//! }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A memory diagnostic report.
///
/// This is an abstract representation of memory analysis results.
/// The actual format depends on the implementation.
pub trait MemoryReport: Send + Sync + fmt::Display {
    /// Format the report for display.
    fn format(&self) -> String {
        self.to_string()
    }
}

/// A collector for memory diagnostics.
///
/// Implementations analyze memory usage across shards and generate reports.
pub trait MemoryDiagnosticsCollector: Send + Sync {
    /// Collect memory diagnostics and return a report.
    fn collect(&self) -> Pin<Box<dyn Future<Output = Arc<dyn MemoryReport>> + Send + '_>>;
}

/// A hot shard detector report.
///
/// Contains information about shard traffic patterns.
pub trait HotShardReport: Send + Sync + fmt::Display {
    /// Format the report for display.
    fn format(&self) -> String {
        self.to_string()
    }
}

/// A collector for hot shard detection.
///
/// Implementations analyze shard traffic to identify hot spots.
pub trait HotShardDetector: Send + Sync {
    /// Collect hot shard statistics and return a report.
    fn collect(&self) -> Pin<Box<dyn Future<Output = Arc<dyn HotShardReport>> + Send + '_>>;
}

/// Configuration for observability features.
///
/// This trait provides access to optional observability collectors.
/// Implementations can choose which collectors to provide based on
/// the server's configuration.
///
/// # Example
///
/// ```rust,ignore
/// struct MyObservability {
///     memory_collector: Option<Arc<dyn MemoryDiagnosticsCollector>>,
/// }
///
/// impl ObservabilityConfig for MyObservability {
///     fn memory_diagnostics(&self) -> Option<&dyn MemoryDiagnosticsCollector> {
///         self.memory_collector.as_ref().map(|c| c.as_ref())
///     }
///
///     fn hot_shard_detector(&self) -> Option<&dyn HotShardDetector> {
///         None // Not configured
///     }
/// }
/// ```
pub trait ObservabilityConfig: Send + Sync {
    /// Get the memory diagnostics collector, if configured.
    fn memory_diagnostics(&self) -> Option<&dyn MemoryDiagnosticsCollector>;

    /// Get the hot shard detector, if configured.
    fn hot_shard_detector(&self) -> Option<&dyn HotShardDetector>;
}

/// A no-op observability config that provides no collectors.
///
/// Use this when observability features are disabled.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopObservability;

impl ObservabilityConfig for NoopObservability {
    fn memory_diagnostics(&self) -> Option<&dyn MemoryDiagnosticsCollector> {
        None
    }

    fn hot_shard_detector(&self) -> Option<&dyn HotShardDetector> {
        None
    }
}
