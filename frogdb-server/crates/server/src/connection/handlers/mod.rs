//! Command handlers for connection-level operations.
//!
//! This module organizes handlers by category:
//!
//! - [`pubsub`] - Pub/Sub response helpers
//! - [`scripting`] - Scripting (EVAL, EVALSHA, SCRIPT, FCALL, FUNCTION)
//! - [`transaction`] - Transaction (MULTI, EXEC, DISCARD, WATCH, UNWATCH)
//! - [`admin`] - Administrative (CLIENT, CONFIG, DEBUG, MEMORY, LATENCY)
//! - [`info`] - INFO (gather sources once, render sections via [`crate::info`])
//! - [`persistence`] - Persistence (BGSAVE, MIGRATE, DUMP/RESTORE)
//! - [`scatter`] - Scatter-gather (SCAN, KEYS, DBSIZE, RANDOMKEY)
//!
//! Each handler module provides functions that take the connection state
//! and arguments, returning a Response.

pub mod blocking;
pub mod debug;
pub mod hotkeys;
pub mod info;
pub mod pubsub;
pub mod scripting;
pub mod search;
pub mod slowlog;
pub mod transaction;
