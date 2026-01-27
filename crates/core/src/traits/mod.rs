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

pub mod wal;
pub mod replication;
pub mod metrics;
pub mod tracing_traits;

// Re-export all traits and noop implementations
pub use wal::{WalWriter, WalOperation, NoopWalWriter};
pub use replication::{ReplicationTracker, ReplicationConfig, NoopReplicationTracker};
pub use metrics::{MetricsRecorder, NoopMetricsRecorder};
pub use tracing_traits::{Tracer, Span, NoopTracer, NoopSpan};
