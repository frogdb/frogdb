//! FrogDB Server
//!
//! Main server implementation including TCP acceptor, connection handling,
//! configuration, and routing.

pub mod acceptor;
pub mod admin;
pub mod cli;
pub mod cluster;
pub mod commands;
pub mod config;
pub(crate) mod config_persister;
pub mod connection;
pub mod cursor_store;
pub mod debug_providers;
pub mod function_store;
pub mod info;
pub mod latency_test;
pub mod malloc_conf;
pub mod migrate;
pub mod monitor;
pub mod net;
pub mod observability_server;
pub mod operations;
pub mod replication;
pub mod role_manager;
pub mod runtime_config;
pub mod scatter;
pub mod server;
pub mod server_observability;
pub mod slot_migration;
#[cfg(not(feature = "turmoil"))]
pub mod tls;
#[cfg(not(feature = "turmoil"))]
pub mod tls_runtime;
#[cfg(not(feature = "turmoil"))]
pub mod tls_watch;
pub(crate) mod vll_adapter;

pub use config::Config;
pub use runtime_config::ConfigManager;
pub use server::{Server, ServerListeners, register_commands};
