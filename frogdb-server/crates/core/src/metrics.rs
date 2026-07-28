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

use serde::{Deserialize, Serialize};

use crate::conn_command::BoxFuture;

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

/// How a shard's share of the fleet's traffic classifies against the
/// configured hot/warm thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShardLoadClass {
    /// Shard is receiving a disproportionately high share of traffic.
    Hot,
    /// Shard is receiving an elevated but not critical share of traffic.
    Warm,
    /// Shard traffic is within normal bounds.
    Ok,
}

impl ShardLoadClass {
    /// The operator-facing label (`HOT`/`WARM`/`OK`).
    pub fn as_str(&self) -> &'static str {
        match self {
            ShardLoadClass::Hot => "HOT",
            ShardLoadClass::Warm => "WARM",
            ShardLoadClass::Ok => "OK",
        }
    }
}

impl fmt::Display for ShardLoadClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One shard's windowed load, as measured over the report's period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardLoad {
    /// Shard id.
    pub shard_id: usize,
    /// Total operations per second over the window.
    pub ops_per_sec: f64,
    /// Read operations per second over the window.
    pub reads_per_sec: f64,
    /// Write operations per second over the window.
    pub writes_per_sec: f64,
    /// This shard's share of fleet traffic, as a percentage (0-100).
    pub percentage: f64,
    /// Shard message-queue depth at sampling time.
    pub queue_depth: usize,
    /// Classification of `percentage` against the configured thresholds.
    pub class: ShardLoadClass,
}

/// A point-in-time hot-shard report: per-shard windowed load plus the derived
/// fleet-level imbalance summary.
///
/// Plain data (no formatting, no server types) so every surface renders the
/// same numbers: the `FROGDB.HOTSHARDS` command, the `/status` JSON, and the
/// debug web UI panel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotShardSnapshot {
    /// Window, in seconds, the per-shard rates were computed over.
    pub period_secs: u64,
    /// Sum of every shard's `ops_per_sec`.
    pub total_ops_per_sec: f64,
    /// `max_ops_per_sec / mean_ops_per_sec` (1.0 when perfectly balanced or idle).
    pub imbalance_ratio: f64,
    /// Number of shards classified [`ShardLoadClass::Hot`].
    pub hot_count: usize,
    /// Number of shards classified [`ShardLoadClass::Warm`].
    pub warm_count: usize,
    /// Per-shard load, sorted by `ops_per_sec` descending.
    pub shards: Vec<ShardLoad>,
    /// Human-readable operator hints derived from the numbers above.
    pub recommendations: Vec<String>,
}

/// A collector for hot shard detection.
///
/// Implementations analyze shard traffic to identify hot spots.
pub trait HotShardDetector: Send + Sync {
    /// Collect per-shard load over `period_secs` (the implementation's
    /// configured default window when `None`).
    fn collect_snapshot(&self, period_secs: Option<u64>) -> BoxFuture<'_, HotShardSnapshot>;
}

/// A source of "why are writes being rejected right now" for operator surfaces.
///
/// A write gate that rejects writes without saying why forces the operator to
/// guess between the several independent gates (replica quorum loss,
/// `min-replicas-to-write`, cluster health). Implementations report the reason
/// for a fence that is engaged *at this instant*, and `None` whenever writes
/// would be accepted — per the observability convention, the field is absent
/// rather than reporting a fence that is not in force.
pub trait WriteFenceReporter: Send + Sync {
    /// A short, stable reason a write would be rejected now, or `None` if
    /// writes are not fenced by this reporter.
    fn write_fence_reason(&self) -> Option<&'static str>;
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
