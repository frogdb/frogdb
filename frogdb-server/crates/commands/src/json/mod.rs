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
use frogdb_core::{CommandError, JsonError, JsonLimits, JsonValue};
use frogdb_protocol::Response;
use serde_json::Value as JsonData;

/// Parse a JSON path argument, defaulting to root if not provided.
fn parse_path(arg: Option<&Bytes>) -> String {
    arg.map(|b| String::from_utf8_lossy(b).to_string())
        .unwrap_or_else(|| "$".to_string())
}

/// Parse client-supplied JSON, enforcing the configured document limits at the
/// ingest boundary.
///
/// Every command that parses JSON coming off the wire must route through here so
/// an over-nested or oversized fragment is rejected before it can be stored.
/// Errors are the same [`JsonError`] family the JSON.SET new-document path
/// surfaces (via [`JsonValue::parse_with_limits`]), so callers report one
/// consistent error message. Re-parses of already-stored, already-validated
/// documents (internal re-serialization) do not belong here and stay unbounded.
fn parse_json_value_limited(bytes: &[u8], limits: &JsonLimits) -> Result<JsonData, CommandError> {
    JsonValue::parse_with_limits(bytes, limits)
        .map(JsonValue::into_data)
        .map_err(json_error_to_command_error)
}

/// Enforce configured limits on a document that a growth mutation just produced,
/// reverting to `snapshot` (leaving the store unmutated) if the result exceeds
/// them.
///
/// Input-side parsing only bounds the *fragment* a command supplies; a mutation
/// (MERGE, ARRAPPEND, ARRINSERT, STRAPPEND, nested SET) can still push the
/// *resulting* stored document past the depth or serialized-size cap. Callers
/// apply the mutation in place through the copy-on-write handle, then call this
/// to validate the whole document and roll back on violation so the reject is a
/// true no-op.
fn enforce_growth_limits(
    json: &mut JsonValue,
    snapshot: JsonValue,
    limits: &JsonLimits,
) -> Result<(), CommandError> {
    if let Err(e) = json.validate_limits(limits) {
        *json = snapshot;
        return Err(json_error_to_command_error(e));
    }
    Ok(())
}

/// Convert a JsonError to a CommandError.
fn json_error_to_command_error(err: JsonError) -> CommandError {
    CommandError::InvalidArgument {
        message: err.to_string(),
    }
}

/// Get an immutable clone of the JSON value at `key`, returning null if the key
/// doesn't exist or `WrongType` if the value isn't JSON.
macro_rules! get_json {
    ($ctx:expr, $key:expr) => {
        // Typed access owns the WrongType invariant and reads via the shared
        // (non-COW) handle. UFCS with absolute paths so call sites need no
        // extra imports.
        match ::frogdb_core::StoreTypedExt::get_typed::<::frogdb_core::JsonValue>($ctx.store, $key)?
        {
            Some(j) => (*j).clone(),
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
        // Typed access owns the WrongType invariant and the check-before-mut
        // (COW-avoiding) ordering. UFCS with absolute paths so call sites need
        // no extra imports.
        match ::frogdb_core::StoreTypedExt::get_typed_mut::<::frogdb_core::JsonValue>(
            $ctx.store, $key,
        )? {
            Some(j) => j,
            None => return Ok($none_resp),
        }
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
