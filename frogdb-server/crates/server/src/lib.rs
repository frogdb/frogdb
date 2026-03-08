//! FrogDB Server
//!
//! Main server implementation including TCP acceptor, connection handling,
//! configuration, and routing.

pub mod acceptor;
pub mod admin;
pub mod cluster_bus;
pub mod cluster_pubsub;
pub mod commands;
pub mod config;
pub mod connection;
pub mod failure_detector;
pub mod latency_test;
pub mod migrate;
pub mod net;
pub mod observability_server;
pub mod operations;
pub mod replication;
pub mod replication_quorum;
pub mod routing;
pub mod runtime_config;
pub mod scatter;
pub mod server;

pub use admin::AdminServer;
pub use config::Config;
pub use runtime_config::ConfigManager;
pub use server::{Server, ServerListeners, register_commands};
