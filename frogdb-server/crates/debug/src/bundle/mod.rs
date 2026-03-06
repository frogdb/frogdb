//! Diagnostic bundle generation and storage.
//!
//! Bundles collect diagnostic data from all shards, traces, and metrics
//! into a ZIP archive for offline analysis.

pub mod collector;
pub mod generator;
pub mod store;

pub use collector::{ClusterStateJson, DiagnosticCollector, DiagnosticData};
pub use generator::BundleGenerator;
pub use store::{BundleInfo, BundleStore};

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Default directory for storing bundles.
pub const DEFAULT_BUNDLE_DIRECTORY: &str = "frogdb-data/bundles";

/// Default maximum number of bundles to retain.
pub const DEFAULT_MAX_BUNDLES: usize = 10;

/// Default bundle TTL in seconds (1 hour).
pub const DEFAULT_BUNDLE_TTL_SECS: u64 = 3600;

/// Default maximum slowlog entries to include in a bundle.
pub const DEFAULT_MAX_SLOWLOG_ENTRIES: usize = 256;

/// Default maximum trace entries to include in a bundle.
pub const DEFAULT_MAX_TRACE_ENTRIES: usize = 100;

/// Configuration for diagnostic bundles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleConfig {
    /// Directory for storing bundles.
    pub directory: PathBuf,
    /// Maximum number of bundles to retain.
    pub max_bundles: usize,
    /// Bundle TTL in seconds before automatic cleanup.
    pub bundle_ttl_secs: u64,
    /// Maximum slowlog entries to include.
    pub max_slowlog_entries: usize,
    /// Maximum trace entries to include.
    pub max_trace_entries: usize,
}

impl Default for BundleConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from(DEFAULT_BUNDLE_DIRECTORY),
            max_bundles: DEFAULT_MAX_BUNDLES,
            bundle_ttl_secs: DEFAULT_BUNDLE_TTL_SECS,
            max_slowlog_entries: DEFAULT_MAX_SLOWLOG_ENTRIES,
            max_trace_entries: DEFAULT_MAX_TRACE_ENTRIES,
        }
    }
}
