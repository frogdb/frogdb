//! JSON commands.
//!
//! Commands for JSON document manipulation (RedisJSON compatible):
//! - JSON.SET, JSON.GET, JSON.DEL, JSON.MGET - basic operations
//! - JSON.TYPE - type inspection
//! - JSON.NUMINCRBY, JSON.NUMMULTBY - numeric operations
//! - JSON.STRAPPEND, JSON.STRLEN - string operations
//! - JSON.ARRAPPEND, JSON.ARRINDEX, JSON.ARRINSERT, JSON.ARRLEN, JSON.ARRPOP, JSON.ARRTRIM - array operations
//! - JSON.OBJKEYS, JSON.OBJLEN - object operations
//! - JSON.CLEAR, JSON.TOGGLE, JSON.MERGE - utility operations

mod array;
mod basic;
mod mutation;
mod numeric;
mod object;
mod string;

pub use array::*;
pub use basic::*;
pub use mutation::*;
pub use numeric::*;
pub use object::*;
pub use string::*;

use bytes::Bytes;
use frogdb_core::{CommandError, JsonError, JsonLimits};
use serde_json::Value as JsonData;

/// Parse a JSON path argument, defaulting to root if not provided.
fn parse_path(arg: Option<&Bytes>) -> String {
    arg.map(|b| String::from_utf8_lossy(b).to_string())
        .unwrap_or_else(|| "$".to_string())
}

/// Parse a JSON value from bytes.
fn parse_json_value(bytes: &[u8]) -> Result<JsonData, CommandError> {
    serde_json::from_slice(bytes).map_err(|e| CommandError::InvalidArgument {
        message: format!("invalid JSON: {}", e),
    })
}

/// Convert a JsonError to a CommandError.
fn json_error_to_command_error(err: JsonError) -> CommandError {
    CommandError::InvalidArgument {
        message: err.to_string(),
    }
}

/// Default JSON limits (will be replaced with config-based limits).
fn default_limits() -> JsonLimits {
    JsonLimits::default()
}
