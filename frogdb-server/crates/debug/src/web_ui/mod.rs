//! Debug web UI for FrogDB.
//!
//! This module provides a read-only debug/admin web interface accessible at
//! `http://localhost:9090/debug`. The interface displays:
//! - Cluster status (role, replicas, shards, keys, memory)
//! - Configuration values
//! - Real-time metrics with charts
//! - Slowlog entries
//! - Latency histograms
//!
//! ## Architecture
//!
//! The debug UI uses:
//! - HTMX for dynamic content updates via HTML partials
//! - Alpine.js for lightweight client-side interactivity
//! - uPlot for time-series charts
//! - Chota CSS for styling
//!
//! All assets are embedded in the binary via rust-embed.

pub mod handlers;
pub mod routes;
pub mod state;

pub use routes::handle_debug_request;
pub use state::{
    ClientInfoProvider, ClientSnapshot, ClusterInfoProvider, ClusterNodeSnapshot,
    ClusterOverviewSnapshot, ConfigEntry, DebugState, MigrationSnapshot, ServerInfo,
};
