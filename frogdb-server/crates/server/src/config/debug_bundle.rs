//! Debug bundle configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Debug bundle configuration.
///
/// Controls the generation and storage of diagnostic bundles for troubleshooting.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DebugBundleConfig {
    /// Directory for storing bundles.
    #[serde(default = "default_bundle_directory")]
    pub directory: String,

    /// Maximum number of bundles to retain.
    #[serde(default = "default_max_bundles")]
    pub max_bundles: usize,

    /// Bundle TTL in seconds before automatic cleanup (default: 1 hour).
    #[serde(default = "default_bundle_ttl_secs")]
    pub bundle_ttl_secs: u64,

    /// Maximum slowlog entries to include in bundles.
    #[serde(default = "default_max_slowlog_entries")]
    pub max_slowlog_entries: usize,

    /// Maximum trace entries to include in bundles.
    #[serde(default = "default_max_trace_entries")]
    pub max_trace_entries: usize,
}

fn default_bundle_directory() -> String {
    frogdb_debug::bundle::DEFAULT_BUNDLE_DIRECTORY.to_string()
}

fn default_max_bundles() -> usize {
    frogdb_debug::bundle::DEFAULT_MAX_BUNDLES
}

fn default_bundle_ttl_secs() -> u64 {
    frogdb_debug::bundle::DEFAULT_BUNDLE_TTL_SECS
}

fn default_max_slowlog_entries() -> usize {
    frogdb_debug::bundle::DEFAULT_MAX_SLOWLOG_ENTRIES
}

fn default_max_trace_entries() -> usize {
    frogdb_debug::bundle::DEFAULT_MAX_TRACE_ENTRIES
}

impl Default for DebugBundleConfig {
    fn default() -> Self {
        Self {
            directory: default_bundle_directory(),
            max_bundles: default_max_bundles(),
            bundle_ttl_secs: default_bundle_ttl_secs(),
            max_slowlog_entries: default_max_slowlog_entries(),
            max_trace_entries: default_max_trace_entries(),
        }
    }
}

impl DebugBundleConfig {
    /// Convert to BundleConfig for the metrics crate.
    pub fn to_bundle_config(&self) -> frogdb_debug::BundleConfig {
        frogdb_debug::BundleConfig {
            directory: std::path::PathBuf::from(&self.directory),
            max_bundles: self.max_bundles,
            bundle_ttl_secs: self.bundle_ttl_secs,
            max_slowlog_entries: self.max_slowlog_entries,
            max_trace_entries: self.max_trace_entries,
        }
    }
}
