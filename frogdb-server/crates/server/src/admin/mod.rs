//! Admin HTTP API for cluster management and monitoring.
//!
//! Provides handlers for:
//! - Health checks
//! - Cluster state inspection
//! - Role management
//! - Node listing
//!
//! These handlers are mounted into the unified HTTP server
//! (see `crate::observability_server`).

pub mod handlers;
