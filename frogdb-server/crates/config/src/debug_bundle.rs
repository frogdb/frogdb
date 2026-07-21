//! Debug bundle configuration.

use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default bundle directory.
pub const DEFAULT_BUNDLE_DIRECTORY: &str = "frogdb-data/bundles";

/// Default maximum number of bundles to retain.
pub const DEFAULT_MAX_BUNDLES: usize = 10;

/// Default bundle TTL in seconds (1 hour).
pub const DEFAULT_BUNDLE_TTL_SECS: u64 = 3600;

/// Default maximum slowlog entries to include in bundles.
pub const DEFAULT_MAX_SLOWLOG_ENTRIES: usize = 256;

/// Default maximum trace entries to include in bundles.
pub const DEFAULT_MAX_TRACE_ENTRIES: usize = 100;

/// Debug bundle configuration.
///
/// Controls the generation and storage of diagnostic bundles for troubleshooting.
//
// No fields are exposed as CONFIG GET/SET parameters; each carries an explicit
// `#[param(skip)]` to satisfy the per-field coverage guarantee.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "debug-bundle")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct DebugBundleConfig {
    /// Directory for storing bundles.
    #[serde(default = "default_bundle_directory")]
    #[param(skip)]
    // skip: borderline: diagnostic-bundle output path; niche troubleshooting subsystem
    pub directory: String,

    /// Maximum number of bundles to retain.
    #[serde(default = "default_max_bundles")]
    #[param(skip)]
    // skip: borderline: diagnostic-bundle retention count; niche, no Redis analogue
    pub max_bundles: usize,

    /// Bundle TTL in seconds before automatic cleanup.
    #[serde(default = "default_bundle_ttl_secs")]
    #[param(skip)]
    // skip: borderline: diagnostic-bundle retention TTL; niche, no Redis analogue
    pub bundle_ttl_secs: u64,

    /// Maximum slowlog entries to include in bundles.
    #[serde(default = "default_max_slowlog_entries")]
    #[param(skip)] // skip: internal per-bundle slowlog content cap; no operator story
    pub max_slowlog_entries: usize,

    /// Maximum trace entries to include in bundles.
    #[serde(default = "default_max_trace_entries")]
    #[param(skip)] // skip: internal per-bundle trace content cap; no operator story
    pub max_trace_entries: usize,
}

fn default_bundle_directory() -> String {
    DEFAULT_BUNDLE_DIRECTORY.to_string()
}

fn default_max_bundles() -> usize {
    DEFAULT_MAX_BUNDLES
}

fn default_bundle_ttl_secs() -> u64 {
    DEFAULT_BUNDLE_TTL_SECS
}

fn default_max_slowlog_entries() -> usize {
    DEFAULT_MAX_SLOWLOG_ENTRIES
}

fn default_max_trace_entries() -> usize {
    DEFAULT_MAX_TRACE_ENTRIES
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
