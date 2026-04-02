//! Debug web UI and diagnostic tools for FrogDB.
//!
//! This crate provides:
//! - Memory diagnostics (MEMORY DOCTOR)
//! - Diagnostic bundle generation and storage
//! - Debug web UI with real-time metrics

pub mod bundle;
pub mod config;
pub mod memory;
pub mod web_ui;

// Config re-exports
pub use config::{HotShardConfig, MemoryDiagConfig};

// Memory diagnostics re-exports
pub use memory::{MemoryDiagCollector, format_memory_report};

// Bundle re-exports
pub use bundle::store::BundleInfo;
pub use bundle::{
    BundleConfig, BundleGenerator, BundleStore, ClusterStateJson, DiagnosticCollector,
};

// Web UI re-exports
pub use web_ui::{
    ClientInfoProvider, ClientSnapshot, ClusterInfoProvider, ClusterNodeSnapshot,
    ClusterOverviewSnapshot, ConfigEntry, DebugState, MigrationSnapshot, ServerInfo,
};
