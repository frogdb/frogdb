//! Scripting command handlers.
//!
//! This module handles scripting commands:
//! - EVAL - Execute a Lua script
//! - EVALSHA - Execute a cached Lua script by SHA
//! - SCRIPT LOAD/EXISTS/FLUSH/KILL/HELP
//! - FCALL/FCALL_RO - Call a registered function
//! - FUNCTION LOAD/DELETE/FLUSH/LIST/STATS/DUMP/RESTORE/KILL/HELP
//!
//! These handlers are implemented as extension methods on `ConnectionHandler`.

mod eval;
mod function;
mod script;
