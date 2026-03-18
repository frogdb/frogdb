//! Stream commands.
//!
//! Commands for stream manipulation:
//! - XADD - add entry to stream
//! - XLEN - get stream length
//! - XRANGE, XREVRANGE - range queries
//! - XDEL - delete entries
//! - XTRIM - trim stream
//! - XREAD - read entries (non-blocking for now)
//! - XGROUP - consumer group management
//! - XREADGROUP - read as consumer
//! - XACK - acknowledge entries
//! - XCLAIM, XAUTOCLAIM - claim pending entries
//! - XPENDING - pending entry info
//! - XINFO - stream/group/consumer info
//! - XSETID - set stream last ID
//! - XDELEX - extended delete with reference control
//! - XACKDEL - atomic acknowledge + conditional delete

mod basic;
mod consumer_groups;
mod info;
mod pending;
mod read;

pub use basic::*;
pub use consumer_groups::*;
pub use info::*;
pub use pending::*;
pub use read::*;

use bytes::Bytes;
use frogdb_core::{
    CommandError, DeleteRefStrategy, StreamEntry, StreamId, StreamTrimOptions, StreamTrimStrategy,
};
use frogdb_protocol::Response;

use super::utils::{parse_optional_limit, parse_trim_mode, parse_u64};

/// Format a stream entry as a Response.
pub(crate) fn entry_to_response(entry: &StreamEntry) -> Response {
    let id = Response::bulk(Bytes::from(entry.id.to_string()));
    let fields: Vec<Response> = entry
        .fields
        .iter()
        .flat_map(|(k, v)| vec![Response::bulk(k.clone()), Response::bulk(v.clone())])
        .collect();
    Response::Array(vec![id, Response::Array(fields)])
}

/// Parse trimming options from arguments starting at given index.
/// Returns (trim_options, next_index).
pub(crate) fn parse_trim_options(
    args: &[Bytes],
    mut i: usize,
) -> Result<(Option<StreamTrimOptions>, usize), CommandError> {
    if i >= args.len() {
        return Ok((None, i));
    }

    let arg = args[i].to_ascii_uppercase();
    let strategy = match arg.as_slice() {
        b"MAXLEN" => {
            i += 1;
            if i >= args.len() {
                return Err(CommandError::SyntaxError);
            }

            // Parse mode (= or ~) using shared utility
            let (mode, consumed) = parse_trim_mode(args[i].as_ref());
            if consumed {
                i += 1;
            }

            if i >= args.len() {
                return Err(CommandError::SyntaxError);
            }

            let threshold = parse_u64(&args[i])?;
            i += 1;

            // Parse optional LIMIT using shared utility
            let (limit, next_i) = parse_optional_limit(args, i)?;
            i = next_i;

            Some(StreamTrimOptions {
                strategy: StreamTrimStrategy::MaxLen(threshold),
                mode,
                limit,
            })
        }
        b"MINID" => {
            i += 1;
            if i >= args.len() {
                return Err(CommandError::SyntaxError);
            }

            // Parse mode (= or ~) using shared utility
            let (mode, consumed) = parse_trim_mode(args[i].as_ref());
            if consumed {
                i += 1;
            }

            if i >= args.len() {
                return Err(CommandError::SyntaxError);
            }

            let min_id = StreamId::parse(&args[i])?;
            i += 1;

            // Parse optional LIMIT using shared utility
            let (limit, next_i) = parse_optional_limit(args, i)?;
            i = next_i;

            Some(StreamTrimOptions {
                strategy: StreamTrimStrategy::MinId(min_id),
                mode,
                limit,
            })
        }
        _ => None,
    };

    Ok((strategy, i))
}

/// Parse optional KEEPREF/DELREF/ACKED token from arguments.
///
/// Returns the parsed strategy and advances the index if a token was consumed.
pub(crate) fn parse_delete_ref_strategy(
    args: &[Bytes],
    i: &mut usize,
) -> DeleteRefStrategy {
    if *i >= args.len() {
        return DeleteRefStrategy::default();
    }

    let arg = args[*i].to_ascii_uppercase();
    match arg.as_slice() {
        b"KEEPREF" => {
            *i += 1;
            DeleteRefStrategy::KeepRef
        }
        b"DELREF" => {
            *i += 1;
            DeleteRefStrategy::DelRef
        }
        b"ACKED" => {
            *i += 1;
            DeleteRefStrategy::Acked
        }
        _ => DeleteRefStrategy::default(),
    }
}

/// Parse `IDS numids id [id ...]` block from arguments.
///
/// Validates that the actual ID count matches `numids`.
pub(crate) fn parse_ids_block(
    args: &[Bytes],
    i: &mut usize,
) -> Result<Vec<StreamId>, CommandError> {
    if *i >= args.len() {
        return Err(CommandError::SyntaxError);
    }

    // Expect "IDS"
    if args[*i].to_ascii_uppercase().as_slice() != b"IDS" {
        return Err(CommandError::SyntaxError);
    }
    *i += 1;

    // Expect numids
    if *i >= args.len() {
        return Err(CommandError::SyntaxError);
    }
    let num_ids = parse_u64(&args[*i])? as usize;
    *i += 1;

    if num_ids == 0 {
        return Err(CommandError::InvalidArgument {
            message: "numids must be positive".to_string(),
        });
    }

    // Parse exactly numids IDs
    let remaining = args.len() - *i;
    if remaining < num_ids {
        return Err(CommandError::InvalidArgument {
            message: format!(
                "expected {} IDs but got {}",
                num_ids, remaining
            ),
        });
    }

    let mut ids = Vec::with_capacity(num_ids);
    for _ in 0..num_ids {
        let id = StreamId::parse(&args[*i])?;
        ids.push(id);
        *i += 1;
    }

    Ok(ids)
}
