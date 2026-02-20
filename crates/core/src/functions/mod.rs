//! Redis Functions implementation.
//!
//! This module provides support for Redis Functions (Redis 7.0+), which are
//! persistent, named, library-organized Lua code as an alternative to ephemeral
//! EVAL scripts.
//!
//! # Features
//!
//! - Library-based organization with named functions
//! - Persistence across server restarts via WAL
//! - Function flags (no-writes, allow-oom, allow-stale)
//! - FUNCTION LOAD, LIST, DELETE, FLUSH commands
//! - FCALL and FCALL_RO execution
//! - FUNCTION DUMP/RESTORE for backup and migration

mod error;
mod function;
mod library;
mod loader;
mod parser;
mod persistence;
mod registry;

pub use error::FunctionError;
pub use function::{FunctionFlags, RegisteredFunction};
pub use library::FunctionLibrary;
pub use loader::{load_library, validate_library};
pub use parser::{parse_shebang, CapturedRegistration, ParsedLibrary, ShebangInfo};
pub use persistence::{
    dump_libraries, load_from_file, restore_libraries, save_to_file, RestorePolicy,
};
pub use registry::{
    new_shared_registry, FunctionRegistry, FunctionStats, RunningFunctionInfo,
    SharedFunctionRegistry,
};
