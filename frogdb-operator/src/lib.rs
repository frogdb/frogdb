//! FrogDB Kubernetes Operator library.
//!
//! Re-exports internal modules for use by integration tests.

pub mod config_gen;
pub mod controller;
pub mod crd;
pub mod health;
pub mod resources;
pub mod telemetry;
pub mod testing;
