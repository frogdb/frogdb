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
use frogdb_protocol::Response;
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

/// Get an immutable clone of the JSON value at `key`, returning null if the key
/// doesn't exist or `WrongType` if the value isn't JSON.
macro_rules! get_json {
    ($ctx:expr, $key:expr) => {
        match $ctx.store.get($key) {
            Some(value) => match value.as_json() {
                Some(j) => j.clone(),
                None => return Err(CommandError::WrongType),
            },
            None => return Ok(Response::null()),
        }
    };
}
pub(crate) use get_json;

/// Get a mutable reference to the JSON value at `key`. Checks existence and type
/// first, returning `$none_resp` (default: `Response::null()`) if the key doesn't
/// exist, or `WrongType` if the value isn't JSON.
macro_rules! get_json_mut {
    ($ctx:expr, $key:expr) => {
        get_json_mut!($ctx, $key, Response::null())
    };
    ($ctx:expr, $key:expr, $none_resp:expr) => {{
        {
            let value = match $ctx.store.get($key) {
                Some(v) => v,
                None => return Ok($none_resp),
            };
            if value.as_json().is_none() {
                return Err(CommandError::WrongType);
            }
        }
        $ctx.store.get_mut($key).unwrap().as_json_mut().unwrap()
    }};
}
pub(crate) use get_json_mut;

/// Convert a results vec into a single response or an array of responses.
/// When the vec has exactly one element, applies `convert` and returns
/// the single response directly; otherwise wraps all converted responses
/// in `Response::Array`.
pub(crate) fn single_or_multi<T, F>(results: Vec<T>, convert: F) -> Response
where
    F: Fn(T) -> Response,
{
    if results.len() == 1 {
        convert(results.into_iter().next().unwrap())
    } else {
        Response::Array(results.into_iter().map(convert).collect())
    }
}
