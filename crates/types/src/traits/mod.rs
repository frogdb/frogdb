//! Core trait abstractions.
//!
//! This module organizes core traits and their noop implementations into
//! logical groups. Each submodule contains a trait and its default no-op
//! implementation.
//!
//! # Module Structure
//!
//! - `wal` - Write-ahead log (WAL) traits
//! - `replication` - Replication tracking traits
//! - `metrics` - Metrics recording traits
//! - `tracing` - Distributed tracing traits
//!
//! # Example
//!
//! ```rust,ignore
//! use frogdb_core::traits::{WalWriter, NoopWalWriter, MetricsRecorder, NoopMetricsRecorder};
//!
//! // Use noop implementations for testing or when feature is disabled
//! let wal: Box<dyn WalWriter> = Box::new(NoopWalWriter::new());
//! let metrics: Arc<dyn MetricsRecorder> = Arc::new(NoopMetricsRecorder);
//! ```

pub mod metrics;
pub mod replication;
pub mod tracing_traits;
pub mod wal;

// Re-export all traits and noop implementations
pub use metrics::{MetricsRecorder, NoopMetricsRecorder};
pub use replication::{NoopReplicationTracker, ReplicationConfig, ReplicationTracker};
pub use tracing_traits::{NoopSpan, NoopTracer, Span, Tracer};
pub use wal::{NoopWalWriter, WalOperation, WalWriter};
