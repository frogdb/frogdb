//! Admin HTTP API for cluster management and monitoring.
//!
//! Provides endpoints for:
//! - Health checks
//! - Cluster state inspection
//! - Role management
//! - Node listing

pub mod handlers;
pub mod server;

pub use server::AdminServer;
