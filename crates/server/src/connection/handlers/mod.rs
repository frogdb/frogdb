//! Command handlers for connection-level operations.
//!
//! This module organizes handlers by category:
//!
//! - [`auth`] - Authentication (AUTH, HELLO, ACL)
//! - [`pubsub`] - Pub/Sub response helpers
//! - [`scripting`] - Scripting (EVAL, EVALSHA, SCRIPT, FCALL, FUNCTION)
//! - [`admin`] - Administrative (CLIENT, CONFIG, DEBUG, MEMORY, LATENCY)
//! - [`persistence`] - Persistence (BGSAVE, MIGRATE, DUMP/RESTORE)
//! - [`scatter`] - Scatter-gather (SCAN, KEYS, DBSIZE, RANDOMKEY)
//!
//! Each handler module provides functions that take the connection state
//! and arguments, returning a Response.

pub mod admin;
pub mod auth;
pub mod persistence;
pub mod pubsub;
pub mod scatter;
pub mod scripting;

// Re-export commonly used types
pub use admin::AdminHandler;
pub use auth::{AuthHandler, HelloResult};
pub use persistence::PersistenceHandler;
pub use scatter::ScatterHandler;

use frogdb_protocol::Response;

/// Trait for ACL permission checking.
///
/// This trait is implemented by connection state to allow handlers to
/// check permissions before executing commands.
pub trait AclChecker {
    /// Check if a command is allowed for the current user.
    fn check_command_permission(&self, cmd_name: &str) -> Result<(), Response>;

    /// Check if a key pattern is allowed for the current user.
    fn check_key_permission(&self, key: &[u8], access: KeyAccess) -> Result<(), Response>;

    /// Check if a pub/sub channel is allowed for the current user.
    fn check_channel_permission(&self, channel: &[u8]) -> Result<(), Response>;
}

/// Type of key access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyAccess {
    /// Read access (GET, HGET, etc.).
    Read,
    /// Write access (SET, HSET, etc.).
    Write,
    /// Both read and write (GETSET, etc.).
    ReadWrite,
}
