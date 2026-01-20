//! FrogDB Server
//!
//! Main server implementation including TCP acceptor, connection handling,
//! configuration, and routing.

pub mod acceptor;
pub mod config;
pub mod connection;
pub mod routing;
pub mod server;

pub use config::Config;
pub use server::{commands, register_commands, Server};
