//! Lua scripting support for FrogDB.
//!
//! This module provides Redis-compatible Lua scripting, including:
//! - EVAL, EVALSHA, and SCRIPT commands
//! - Per-shard Lua VM isolation
//! - Script caching with LRU eviction
//! - redis.call() and redis.pcall() bindings
//! - Strict key validation

mod bindings;
mod cache;
mod config;
mod error;
mod executor;
mod lua_vm;
mod router;

pub use cache::{CachedScript, ScriptCache, ScriptSha, compute_sha, hex_to_sha, sha_to_hex};
pub use config::ScriptingConfig;
pub use error::ScriptError;
pub use executor::ScriptExecutor;
pub use lua_vm::LuaVm;
pub use router::{CrossShardRouter, ScriptRoute, ScriptRouter, SingleShardRouter};
