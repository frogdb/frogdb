//! FrogDB Server
//!
//! Main server implementation including TCP acceptor, connection handling,
//! configuration, and routing.

/// jemalloc as the global allocator, for this crate's test binary only.
///
/// [`shard_arena_reading`]'s end-to-end tests assert that a shard thread's
/// *Rust* allocations reach that thread's own arena and come back through the
/// broker, which is only true if Rust allocations go through jemalloc at all.
/// `main.rs` declares the same allocator for the production binary and
/// `frogdb-telemetry` does the same for its own tests; without this
/// declaration the lib's tests would allocate from the system allocator and
/// read zero from every arena — passing or failing for reasons unrelated to
/// what they check.
#[cfg(test)]
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static TEST_GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
pub mod shard_arena_reading;
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
