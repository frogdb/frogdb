//! Event Sourcing commands (ES.*).
//!
//! FrogDB-proprietary extensions providing first-class event sourcing primitives
//! on top of Redis Streams. These commands add optimistic concurrency control (OCC),
//! version-based reads, snapshot-accelerated replay, idempotent writes, and a global
//! `$all` stream.
//!
//! Commands:
//! - ES.APPEND  — append an event with OCC version check
//! - ES.READ    — read events by version range
//! - ES.REPLAY  — replay all events (optionally from a snapshot)
//! - ES.INFO    — stream metadata (version, length, etc.)
//! - ES.SNAPSHOT — store a snapshot at a given version
//! - ES.ALL     — read the global event stream across all shards

mod all;
mod append;
mod info;
mod read;
mod replay;
mod snapshot;

pub use all::*;
pub use append::*;
pub use info::*;
pub use read::*;
pub use replay::*;
pub use snapshot::*;

use bytes::Bytes;
use frogdb_core::StreamEntry;
use frogdb_protocol::Response;

/// Format a versioned event entry as a Response.
///
/// Returns: `[version, stream_id, [field, value, ...]]`
pub(crate) fn versioned_entry_to_response(version: u64, entry: &StreamEntry) -> Response {
    let fields: Vec<Response> = entry
        .fields
        .iter()
        .flat_map(|(k, v)| vec![Response::bulk(k.clone()), Response::bulk(v.clone())])
        .collect();
    Response::Array(vec![
        Response::Integer(version as i64),
        Response::bulk(Bytes::from(entry.id.to_string())),
        Response::Array(fields),
    ])
}
