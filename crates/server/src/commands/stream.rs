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

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ExecutionStrategy, StreamAddError,
    StreamEntry, StreamGroupError, StreamId, StreamIdParseError, StreamTrimMode, StreamTrimOptions,
    StreamTrimStrategy, Value,
};
use frogdb_protocol::{BlockingOp, Response};

use super::utils::{get_or_create_stream, parse_u64, parse_usize};

/// Convert StreamIdParseError to CommandError.
fn stream_id_error(_: StreamIdParseError) -> CommandError {
    CommandError::InvalidArgument {
        message: "Invalid stream ID specified as stream command argument".to_string(),
    }
}

/// Convert StreamAddError to CommandError.
fn stream_add_error(_: StreamAddError) -> CommandError {
    CommandError::InvalidArgument {
        message: "The ID specified in XADD is equal or smaller than the target stream top item"
            .to_string(),
    }
}

/// Convert StreamGroupError to CommandError.
fn stream_group_error(e: StreamGroupError) -> CommandError {
    match e {
        StreamGroupError::GroupExists => CommandError::BusyGroup,
        StreamGroupError::NoGroup => CommandError::NoGroup,
    }
}

/// Format a stream entry as a Response.
fn entry_to_response(entry: &StreamEntry) -> Response {
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
fn parse_trim_options(args: &[Bytes], mut i: usize) -> Result<(Option<StreamTrimOptions>, usize), CommandError> {
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

            // Check for mode (= or ~)
            let mode = if args[i].as_ref() == b"=" {
                i += 1;
                StreamTrimMode::Exact
            } else if args[i].as_ref() == b"~" {
                i += 1;
                StreamTrimMode::Approximate
            } else {
                StreamTrimMode::Exact
            };

            if i >= args.len() {
                return Err(CommandError::SyntaxError);
            }

            let threshold = parse_u64(&args[i])?;
            i += 1;

            // Check for LIMIT
            let limit = if i < args.len() && args[i].to_ascii_uppercase().as_slice() == b"LIMIT" {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let l = parse_usize(&args[i])?;
                i += 1;
                l
            } else {
                0
            };

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

            // Check for mode (= or ~)
            let mode = if args[i].as_ref() == b"=" {
                i += 1;
                StreamTrimMode::Exact
            } else if args[i].as_ref() == b"~" {
                i += 1;
                StreamTrimMode::Approximate
            } else {
                StreamTrimMode::Exact
            };

            if i >= args.len() {
                return Err(CommandError::SyntaxError);
            }

            let min_id = StreamId::parse(&args[i]).map_err(stream_id_error)?;
            i += 1;

            // Check for LIMIT
            let limit = if i < args.len() && args[i].to_ascii_uppercase().as_slice() == b"LIMIT" {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let l = parse_usize(&args[i])?;
                i += 1;
                l
            } else {
                0
            };

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

// ============================================================================
// XADD - Add entry to stream
// ============================================================================

pub struct XaddCommand;

impl Command for XaddCommand {
    fn name(&self) -> &'static str {
        "XADD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4) // XADD key id field value [field value ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let mut i = 1;
        let mut nomkstream = false;
        let mut trim_options = None;

        // Parse options before ID
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"NOMKSTREAM" => {
                    nomkstream = true;
                    i += 1;
                }
                b"MAXLEN" | b"MINID" => {
                    let (opts, next_i) = parse_trim_options(args, i)?;
                    trim_options = opts;
                    i = next_i;
                }
                _ => break,
            }
        }

        // Parse ID
        if i >= args.len() {
            return Err(CommandError::WrongArity { command: "XADD" });
        }
        let id_spec = StreamId::parse_for_add(&args[i]).map_err(stream_id_error)?;
        i += 1;

        // Parse field-value pairs
        if (args.len() - i) % 2 != 0 {
            return Err(CommandError::WrongArity { command: "XADD" });
        }
        if args.len() - i == 0 {
            return Err(CommandError::WrongArity { command: "XADD" });
        }

        let mut fields = Vec::with_capacity((args.len() - i) / 2);
        while i < args.len() {
            let field = args[i].clone();
            let value = args[i + 1].clone();
            fields.push((field, value));
            i += 2;
        }

        // Check NOMKSTREAM
        if nomkstream && ctx.store.get(key).is_none() {
            return Ok(Response::null());
        }

        // Get or create stream
        let stream = get_or_create_stream(ctx, key)?;

        // Add entry
        let id = stream.add(id_spec, fields).map_err(stream_add_error)?;

        // Apply trimming if specified
        if let Some(opts) = trim_options {
            stream.trim(opts);
        }

        Ok(Response::bulk(Bytes::from(id.to_string())))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XLEN - Get stream length
// ============================================================================

pub struct XlenCommand;

impl Command for XlenCommand {
    fn name(&self) -> &'static str {
        "XLEN"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // XLEN key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let stream = value.as_stream().ok_or(CommandError::WrongType)?;
                Ok(Response::Integer(stream.len() as i64))
            }
            None => Ok(Response::Integer(0)),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XRANGE - Query by ID range
// ============================================================================

pub struct XrangeCommand;

impl Command for XrangeCommand {
    fn name(&self) -> &'static str {
        "XRANGE"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 3, max: 5 } // XRANGE key start end [COUNT count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let start = StreamId::parse_range_bound(&args[1]).map_err(stream_id_error)?;
        let end = StreamId::parse_range_bound(&args[2]).map_err(stream_id_error)?;

        let count = if args.len() > 3 {
            if args[3].to_ascii_uppercase().as_slice() != b"COUNT" {
                return Err(CommandError::SyntaxError);
            }
            if args.len() < 5 {
                return Err(CommandError::SyntaxError);
            }
            Some(parse_usize(&args[4])?)
        } else {
            None
        };

        match ctx.store.get(key) {
            Some(value) => {
                let stream = value.as_stream().ok_or(CommandError::WrongType)?;
                let entries = stream.range(start, end, count);
                let responses: Vec<Response> = entries.iter().map(entry_to_response).collect();
                Ok(Response::Array(responses))
            }
            None => Ok(Response::Array(vec![])),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XREVRANGE - Reverse query by ID range
// ============================================================================

pub struct XrevrangeCommand;

impl Command for XrevrangeCommand {
    fn name(&self) -> &'static str {
        "XREVRANGE"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 3, max: 5 } // XREVRANGE key end start [COUNT count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        // Note: XREVRANGE has end first, then start
        let end = StreamId::parse_range_bound(&args[1]).map_err(stream_id_error)?;
        let start = StreamId::parse_range_bound(&args[2]).map_err(stream_id_error)?;

        let count = if args.len() > 3 {
            if args[3].to_ascii_uppercase().as_slice() != b"COUNT" {
                return Err(CommandError::SyntaxError);
            }
            if args.len() < 5 {
                return Err(CommandError::SyntaxError);
            }
            Some(parse_usize(&args[4])?)
        } else {
            None
        };

        match ctx.store.get(key) {
            Some(value) => {
                let stream = value.as_stream().ok_or(CommandError::WrongType)?;
                let entries = stream.range_rev(end, start, count);
                let responses: Vec<Response> = entries.iter().map(entry_to_response).collect();
                Ok(Response::Array(responses))
            }
            None => Ok(Response::Array(vec![])),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XDEL - Delete specific entries
// ============================================================================

pub struct XdelCommand;

impl Command for XdelCommand {
    fn name(&self) -> &'static str {
        "XDEL"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // XDEL key id [id ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse IDs
        let mut ids = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            let id = StreamId::parse(arg).map_err(stream_id_error)?;
            ids.push(id);
        }

        match ctx.store.get_mut(key) {
            Some(value) => {
                let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;
                let deleted = stream.delete(&ids);
                Ok(Response::Integer(deleted as i64))
            }
            None => Ok(Response::Integer(0)),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XTRIM - Trim stream
// ============================================================================

pub struct XtrimCommand;

impl Command for XtrimCommand {
    fn name(&self) -> &'static str {
        "XTRIM"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        let (trim_options, _) = parse_trim_options(args, 1)?;
        let trim_options = trim_options.ok_or(CommandError::SyntaxError)?;

        match ctx.store.get_mut(key) {
            Some(value) => {
                let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;
                let trimmed = stream.trim(trim_options);
                Ok(Response::Integer(trimmed as i64))
            }
            None => Ok(Response::Integer(0)),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XREAD - Read entries from streams (non-blocking)
// ============================================================================

pub struct XreadCommand;

impl Command for XreadCommand {
    fn name(&self) -> &'static str {
        "XREAD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        // XREAD can block when BLOCK option is specified
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let mut i = 0;
        let mut count: Option<usize> = None;
        let mut block_ms: Option<u64> = None;

        // Parse options
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    count = Some(parse_usize(&args[i])?);
                    i += 1;
                }
                b"BLOCK" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    block_ms = Some(parse_u64(&args[i])?);
                    i += 1;
                }
                b"STREAMS" => {
                    i += 1;
                    break;
                }
                _ => {
                    return Err(CommandError::SyntaxError);
                }
            }
        }

        // Parse keys and IDs
        let remaining = args.len() - i;
        if remaining == 0 || remaining % 2 != 0 {
            return Err(CommandError::SyntaxError);
        }

        let num_streams = remaining / 2;
        let keys = &args[i..i + num_streams];
        let ids = &args[i + num_streams..];

        // Read from each stream, tracking resolved IDs for blocking
        let mut results = Vec::new();
        let mut resolved_ids: Vec<(u64, u64)> = Vec::with_capacity(num_streams);

        for (key, id_arg) in keys.iter().zip(ids.iter()) {
            let after_id = if id_arg.as_ref() == b"$" {
                // $ means current last ID - only new entries
                match ctx.store.get(key.as_ref()) {
                    Some(value) => {
                        let stream = value.as_stream().ok_or(CommandError::WrongType)?;
                        stream.last_id()
                    }
                    None => StreamId::default(),
                }
            } else {
                StreamId::parse(id_arg).map_err(stream_id_error)?
            };

            // Track resolved ID for blocking
            resolved_ids.push((after_id.ms, after_id.seq));

            match ctx.store.get(key.as_ref()) {
                Some(value) => {
                    let stream = value.as_stream().ok_or(CommandError::WrongType)?;
                    let entries = stream.read_after(&after_id, count);

                    if !entries.is_empty() {
                        let entry_responses: Vec<Response> =
                            entries.iter().map(entry_to_response).collect();
                        results.push(Response::Array(vec![
                            Response::bulk(key.clone()),
                            Response::Array(entry_responses),
                        ]));
                    }
                }
                None => {
                    // Stream doesn't exist, skip it
                }
            }
        }

        if results.is_empty() {
            // Check if we should block
            if let Some(block_ms) = block_ms {
                let timeout = if block_ms == 0 {
                    0.0
                } else {
                    block_ms as f64 / 1000.0
                };
                return Ok(Response::BlockingNeeded {
                    keys: keys.to_vec(),
                    timeout,
                    op: BlockingOp::XRead {
                        after_ids: resolved_ids,
                        count,
                    },
                });
            }
            // For non-blocking XREAD, return null if no data
            Ok(Response::null())
        } else {
            Ok(Response::Array(results))
        }
    }

    fn requires_same_slot(&self) -> bool {
        // Blocking XREAD requires all keys to be on the same shard
        true
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Find STREAMS keyword and return keys after it
        let mut i = 0;
        while i < args.len() {
            if args[i].to_ascii_uppercase().as_slice() == b"STREAMS" {
                i += 1;
                break;
            }
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"COUNT" | b"BLOCK" => i += 2,
                _ => i += 1,
            }
        }

        let remaining = args.len() - i;
        if remaining == 0 || remaining % 2 != 0 {
            return vec![];
        }

        let num_streams = remaining / 2;
        args[i..i + num_streams]
            .iter()
            .map(|k| k.as_ref())
            .collect()
    }
}

// ============================================================================
// XGROUP - Consumer group management (subcommand router)
// ============================================================================

pub struct XgroupCommand;

impl Command for XgroupCommand {
    fn name(&self) -> &'static str {
        "XGROUP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // XGROUP subcommand [args...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Err(CommandError::WrongArity { command: "XGROUP" });
        }

        let subcommand = args[0].to_ascii_uppercase();
        match subcommand.as_slice() {
            b"CREATE" => xgroup_create(ctx, &args[1..]),
            b"DESTROY" => xgroup_destroy(ctx, &args[1..]),
            b"CREATECONSUMER" => xgroup_createconsumer(ctx, &args[1..]),
            b"DELCONSUMER" => xgroup_delconsumer(ctx, &args[1..]),
            b"SETID" => xgroup_setid(ctx, &args[1..]),
            b"HELP" => {
                let help = vec![
                    Response::bulk(Bytes::from_static(b"XGROUP CREATE key group id|$ [MKSTREAM] [ENTRIESREAD n]")),
                    Response::bulk(Bytes::from_static(b"XGROUP DESTROY key group")),
                    Response::bulk(Bytes::from_static(b"XGROUP CREATECONSUMER key group consumer")),
                    Response::bulk(Bytes::from_static(b"XGROUP DELCONSUMER key group consumer")),
                    Response::bulk(Bytes::from_static(b"XGROUP SETID key group id|$ [ENTRIESREAD n]")),
                ];
                Ok(Response::Array(help))
            }
            _ => Err(CommandError::InvalidArgument {
                message: format!(
                    "Unknown XGROUP subcommand '{}'. Try XGROUP HELP.",
                    String::from_utf8_lossy(&subcommand)
                ),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Key is second argument for most subcommands
        if args.len() >= 2 {
            vec![&args[1]]
        } else {
            vec![]
        }
    }
}

fn xgroup_create(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XGROUP CREATE key group id|$ [MKSTREAM] [ENTRIESREAD n]
    if args.len() < 3 {
        return Err(CommandError::WrongArity { command: "XGROUP|CREATE" });
    }

    let key = &args[0];
    let group_name = args[1].clone();
    let id_arg = &args[2];

    let mut mkstream = false;
    let mut entries_read: Option<u64> = None;
    let mut i = 3;

    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"MKSTREAM" => {
                mkstream = true;
                i += 1;
            }
            b"ENTRIESREAD" => {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                entries_read = Some(parse_u64(&args[i])?);
                i += 1;
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    // Check if stream exists
    let exists = ctx.store.get(key).is_some();
    if !exists {
        if mkstream {
            ctx.store.set(key.clone(), Value::stream());
        } else {
            return Err(CommandError::InvalidArgument {
                message: "The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.".to_string(),
            });
        }
    }

    // Parse the ID
    let last_id = if id_arg.as_ref() == b"$" {
        // $ means current last ID
        let value = ctx.store.get(key).unwrap();
        let stream = value.as_stream().ok_or(CommandError::WrongType)?;
        stream.last_id()
    } else if id_arg.as_ref() == b"0" || id_arg.as_ref() == b"0-0" {
        StreamId::default()
    } else {
        StreamId::parse(id_arg).map_err(stream_id_error)?
    };

    // Create the group
    let stream = ctx.store.get_mut(key).unwrap().as_stream_mut().ok_or(CommandError::WrongType)?;
    stream.create_group(group_name, last_id, entries_read).map_err(stream_group_error)?;

    Ok(Response::ok())
}

fn xgroup_destroy(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XGROUP DESTROY key group
    if args.len() < 2 {
        return Err(CommandError::WrongArity { command: "XGROUP|DESTROY" });
    }

    let key = &args[0];
    let group_name = &args[1];

    match ctx.store.get_mut(key) {
        Some(value) => {
            let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;
            let destroyed = stream.destroy_group(group_name);
            Ok(Response::Integer(if destroyed { 1 } else { 0 }))
        }
        None => Err(CommandError::InvalidArgument {
            message: "The XGROUP subcommand requires the key to exist.".to_string(),
        }),
    }
}

fn xgroup_createconsumer(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XGROUP CREATECONSUMER key group consumer
    if args.len() < 3 {
        return Err(CommandError::WrongArity { command: "XGROUP|CREATECONSUMER" });
    }

    let key = &args[0];
    let group_name = &args[1];
    let consumer_name = args[2].clone();

    match ctx.store.get_mut(key) {
        Some(value) => {
            let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;
            let group = stream.get_group_mut(group_name).ok_or(CommandError::NoGroup)?;
            let created = group.create_consumer(consumer_name);
            Ok(Response::Integer(if created { 1 } else { 0 }))
        }
        None => Err(CommandError::InvalidArgument {
            message: "The XGROUP subcommand requires the key to exist.".to_string(),
        }),
    }
}

fn xgroup_delconsumer(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XGROUP DELCONSUMER key group consumer
    if args.len() < 3 {
        return Err(CommandError::WrongArity { command: "XGROUP|DELCONSUMER" });
    }

    let key = &args[0];
    let group_name = &args[1];
    let consumer_name = &args[2];

    match ctx.store.get_mut(key) {
        Some(value) => {
            let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;
            let group = stream.get_group_mut(group_name).ok_or(CommandError::NoGroup)?;
            let pending_deleted = group.delete_consumer(consumer_name);
            Ok(Response::Integer(pending_deleted as i64))
        }
        None => Err(CommandError::InvalidArgument {
            message: "The XGROUP subcommand requires the key to exist.".to_string(),
        }),
    }
}

fn xgroup_setid(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XGROUP SETID key group id|$ [ENTRIESREAD n]
    if args.len() < 3 {
        return Err(CommandError::WrongArity { command: "XGROUP|SETID" });
    }

    let key = &args[0];
    let group_name = &args[1];
    let id_arg = &args[2];

    let mut entries_read: Option<u64> = None;
    if args.len() > 3 {
        if args[3].to_ascii_uppercase().as_slice() == b"ENTRIESREAD" {
            if args.len() < 5 {
                return Err(CommandError::SyntaxError);
            }
            entries_read = Some(parse_u64(&args[4])?);
        } else {
            return Err(CommandError::SyntaxError);
        }
    }

    // Parse the ID
    match ctx.store.get_mut(key) {
        Some(value) => {
            let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;

            let new_id = if id_arg.as_ref() == b"$" {
                stream.last_id()
            } else if id_arg.as_ref() == b"0" || id_arg.as_ref() == b"0-0" {
                StreamId::default()
            } else {
                StreamId::parse(id_arg).map_err(stream_id_error)?
            };

            stream.set_group_id(group_name, new_id, entries_read).map_err(stream_group_error)?;
            Ok(Response::ok())
        }
        None => Err(CommandError::InvalidArgument {
            message: "The XGROUP subcommand requires the key to exist.".to_string(),
        }),
    }
}

// ============================================================================
// XREADGROUP - Read entries as consumer
// ============================================================================

pub struct XreadgroupCommand;

impl Command for XreadgroupCommand {
    fn name(&self) -> &'static str {
        "XREADGROUP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(6) // XREADGROUP GROUP group consumer STREAMS key id
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE // Modifies PEL
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        // XREADGROUP can block when BLOCK option is specified
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let mut i = 0;
        let mut group_name: Option<Bytes> = None;
        let mut consumer_name: Option<Bytes> = None;
        let mut count: Option<usize> = None;
        let mut block_ms: Option<u64> = None;
        let mut noack = false;

        // Parse options
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"GROUP" => {
                    i += 1;
                    if i + 1 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    group_name = Some(args[i].clone());
                    i += 1;
                    consumer_name = Some(args[i].clone());
                    i += 1;
                }
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    count = Some(parse_usize(&args[i])?);
                    i += 1;
                }
                b"BLOCK" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    block_ms = Some(parse_u64(&args[i])?);
                    i += 1;
                }
                b"NOACK" => {
                    noack = true;
                    i += 1;
                }
                b"STREAMS" => {
                    i += 1;
                    break;
                }
                _ => {
                    return Err(CommandError::SyntaxError);
                }
            }
        }

        let group_name = group_name.ok_or(CommandError::SyntaxError)?;
        let consumer_name = consumer_name.ok_or(CommandError::SyntaxError)?;

        // Parse keys and IDs
        let remaining = args.len() - i;
        if remaining == 0 || remaining % 2 != 0 {
            return Err(CommandError::SyntaxError);
        }

        let num_streams = remaining / 2;
        let keys = &args[i..i + num_streams];
        let ids = &args[i + num_streams..];

        // For now, only support single stream (no cross-shard)
        if num_streams != 1 {
            return Err(CommandError::InvalidArgument {
                message: "XREADGROUP with multiple streams not yet supported".to_string(),
            });
        }

        let key = &keys[0];
        let id_arg = &ids[0];

        // Get or check stream
        let stream = ctx.store.get_mut(key.as_ref())
            .ok_or_else(|| CommandError::InvalidArgument {
                message: format!("No such key '{}'", String::from_utf8_lossy(key)),
            })?
            .as_stream_mut()
            .ok_or(CommandError::WrongType)?;

        // Get the group
        let group = stream.get_group_mut(&group_name).ok_or(CommandError::NoGroup)?;

        // Ensure consumer exists
        group.get_or_create_consumer(consumer_name.clone());

        let entries: Vec<StreamEntry> = if id_arg.as_ref() == b">" {
            // Read new messages (not yet delivered)
            let last_delivered = group.last_delivered_id;
            let new_entries = stream.read_after(&last_delivered, count);

            if new_entries.is_empty() {
                // No new entries - check if we should block
                if let Some(block_ms) = block_ms {
                    let timeout = if block_ms == 0 {
                        0.0
                    } else {
                        block_ms as f64 / 1000.0
                    };
                    return Ok(Response::BlockingNeeded {
                        keys: keys.to_vec(),
                        timeout,
                        op: BlockingOp::XReadGroup {
                            group: group_name,
                            consumer: consumer_name,
                            noack,
                            count,
                        },
                    });
                }
                return Ok(Response::null());
            }

            // Update last_delivered_id and add to PEL
            if let Some(last) = new_entries.last() {
                // Need to re-get the group after using stream
                let group = stream.get_group_mut(&group_name).unwrap();
                group.last_delivered_id = last.id;

                if !noack {
                    for entry in &new_entries {
                        group.add_pending(entry.id, consumer_name.clone());
                    }
                }
            }

            new_entries
        } else {
            // Re-read from PEL (for retry) - never blocks
            let start_id = if id_arg.as_ref() == b"0" || id_arg.as_ref() == b"0-0" {
                StreamId::default()
            } else {
                StreamId::parse(id_arg).map_err(stream_id_error)?
            };

            // Find pending entries for this consumer starting from start_id
            let pending_ids: Vec<StreamId> = group
                .pending
                .range(start_id..)
                .filter(|(_, pe)| pe.consumer == consumer_name)
                .take(count.unwrap_or(usize::MAX))
                .map(|(id, _)| *id)
                .collect();

            // Get the actual entries
            let mut entries = Vec::new();
            for id in pending_ids {
                if let Some(entry) = stream.get(&id) {
                    entries.push(entry);
                }
            }
            entries
        };

        if entries.is_empty() {
            Ok(Response::null())
        } else {
            let entry_responses: Vec<Response> = entries.iter().map(entry_to_response).collect();
            Ok(Response::Array(vec![Response::Array(vec![
                Response::bulk(key.clone()),
                Response::Array(entry_responses),
            ])]))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Find STREAMS keyword and return keys after it
        let mut i = 0;
        while i < args.len() {
            if args[i].to_ascii_uppercase().as_slice() == b"STREAMS" {
                i += 1;
                break;
            }
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"GROUP" => i += 3,
                b"COUNT" | b"BLOCK" => i += 2,
                b"NOACK" => i += 1,
                _ => i += 1,
            }
        }

        let remaining = args.len() - i;
        if remaining == 0 || remaining % 2 != 0 {
            return vec![];
        }

        let num_streams = remaining / 2;
        args[i..i + num_streams]
            .iter()
            .map(|k| k.as_ref())
            .collect()
    }
}

// ============================================================================
// XACK - Acknowledge entries
// ============================================================================

pub struct XackCommand;

impl Command for XackCommand {
    fn name(&self) -> &'static str {
        "XACK"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // XACK key group id [id ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let group_name = &args[1];

        // Parse IDs
        let mut ids = Vec::with_capacity(args.len() - 2);
        for arg in &args[2..] {
            let id = StreamId::parse(arg).map_err(stream_id_error)?;
            ids.push(id);
        }

        match ctx.store.get_mut(key) {
            Some(value) => {
                let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;
                let group = stream.get_group_mut(group_name).ok_or(CommandError::NoGroup)?;
                let acked = group.ack(&ids);
                Ok(Response::Integer(acked as i64))
            }
            None => Ok(Response::Integer(0)),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XPENDING - Show pending entries
// ============================================================================

pub struct XpendingCommand;

impl Command for XpendingCommand {
    fn name(&self) -> &'static str {
        "XPENDING"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let group_name = &args[1];

        match ctx.store.get(key) {
            Some(value) => {
                let stream = value.as_stream().ok_or(CommandError::WrongType)?;
                let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

                if args.len() == 2 {
                    // Summary mode
                    let pending_count = group.pending_count();
                    let range = group.pending_range();

                    // Count pending per consumer
                    let mut consumer_counts: std::collections::HashMap<&Bytes, i64> = std::collections::HashMap::new();
                    for pe in group.pending.values() {
                        *consumer_counts.entry(&pe.consumer).or_insert(0) += 1;
                    }

                    let consumers: Vec<Response> = consumer_counts
                        .iter()
                        .map(|(name, count)| {
                            Response::Array(vec![
                                Response::bulk((*name).clone()),
                                Response::bulk(Bytes::from(count.to_string())),
                            ])
                        })
                        .collect();

                    match range {
                        Some((first, last)) => Ok(Response::Array(vec![
                            Response::Integer(pending_count as i64),
                            Response::bulk(Bytes::from(first.to_string())),
                            Response::bulk(Bytes::from(last.to_string())),
                            Response::Array(consumers),
                        ])),
                        None => Ok(Response::Array(vec![
                            Response::Integer(0),
                            Response::null(),
                            Response::null(),
                            Response::null(),
                        ])),
                    }
                } else {
                    // Detailed mode: XPENDING key group [IDLE min-idle] start end count [consumer]
                    let mut i = 2;
                    let mut min_idle_ms: Option<u64> = None;

                    // Check for IDLE option
                    if i < args.len() && args[i].to_ascii_uppercase().as_slice() == b"IDLE" {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::SyntaxError);
                        }
                        min_idle_ms = Some(parse_u64(&args[i])?);
                        i += 1;
                    }

                    if i + 2 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }

                    let start = StreamId::parse_range_bound(&args[i]).map_err(stream_id_error)?;
                    let end = StreamId::parse_range_bound(&args[i + 1]).map_err(stream_id_error)?;
                    let count = parse_usize(&args[i + 2])?;
                    i += 3;

                    let consumer_filter: Option<&Bytes> = if i < args.len() {
                        Some(&args[i])
                    } else {
                        None
                    };

                    // Collect matching pending entries
                    let results: Vec<Response> = group
                        .pending
                        .iter()
                        .filter(|(id, pe)| {
                            start.satisfies_min(id)
                                && end.satisfies_max(id)
                                && min_idle_ms.is_none_or(|min| pe.idle_ms() >= min)
                                && consumer_filter.is_none_or(|c| pe.consumer == *c)
                        })
                        .take(count)
                        .map(|(id, pe)| {
                            Response::Array(vec![
                                Response::bulk(Bytes::from(id.to_string())),
                                Response::bulk(pe.consumer.clone()),
                                Response::Integer(pe.idle_ms() as i64),
                                Response::Integer(pe.delivery_count as i64),
                            ])
                        })
                        .collect();

                    Ok(Response::Array(results))
                }
            }
            None => {
                // Stream doesn't exist
                Err(CommandError::InvalidArgument {
                    message: format!("No such key '{}'", String::from_utf8_lossy(key)),
                })
            }
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XCLAIM - Claim pending entries
// ============================================================================

pub struct XclaimCommand;

impl Command for XclaimCommand {
    fn name(&self) -> &'static str {
        "XCLAIM"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(5) // XCLAIM key group consumer min-idle-time id [id ...] [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let group_name = &args[1];
        let consumer_name = args[2].clone();
        let min_idle_time = parse_u64(&args[3])?;

        // Parse IDs and options
        let mut ids = Vec::new();
        let mut idle: Option<u64> = None;
        let mut time: Option<u64> = None;
        let mut retrycount: Option<u32> = None;
        let mut force = false;
        let mut justid = false;

        let mut i = 4;
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"IDLE" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    idle = Some(parse_u64(&args[i])?);
                    i += 1;
                }
                b"TIME" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    time = Some(parse_u64(&args[i])?);
                    i += 1;
                }
                b"RETRYCOUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    retrycount = Some(parse_u64(&args[i])? as u32);
                    i += 1;
                }
                b"FORCE" => {
                    force = true;
                    i += 1;
                }
                b"JUSTID" => {
                    justid = true;
                    i += 1;
                }
                b"LASTID" => {
                    // LASTID is deprecated, skip it
                    i += 2;
                }
                _ => {
                    // Try to parse as ID
                    match StreamId::parse(&args[i]) {
                        Ok(id) => {
                            ids.push(id);
                            i += 1;
                        }
                        Err(_) => return Err(CommandError::SyntaxError),
                    }
                }
            }
        }

        if ids.is_empty() {
            return Err(CommandError::WrongArity { command: "XCLAIM" });
        }

        // First pass: determine which IDs should be claimed (read-only)
        let ids_to_claim: Vec<StreamId> = {
            let value = ctx.store.get(key)
                .ok_or_else(|| CommandError::InvalidArgument {
                    message: format!("No such key '{}'", String::from_utf8_lossy(key)),
                })?;
            let stream = value.as_stream().ok_or(CommandError::WrongType)?;
            let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

            ids.iter()
                .filter(|id| {
                    if let Some(pe) = group.pending.get(id) {
                        pe.idle_ms() >= min_idle_time
                    } else {
                        force
                    }
                })
                .copied()
                .collect()
        };

        // Second pass: perform mutations
        {
            let stream = ctx.store.get_mut(key).unwrap().as_stream_mut().unwrap();
            let group = stream.get_group_mut(group_name).ok_or(CommandError::NoGroup)?;

            // Ensure consumer exists
            group.get_or_create_consumer(consumer_name.clone());

            for id in &ids_to_claim {
                // Remove from old consumer's count
                if let Some(pe) = group.pending.get(id) {
                    let old_consumer_name = pe.consumer.clone();
                    if let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
                        old_consumer.pending_count = old_consumer.pending_count.saturating_sub(1);
                    }
                }

                // Update or insert pending entry
                let pe = group.pending.entry(*id).or_insert_with(|| {
                    frogdb_core::PendingEntry::new(consumer_name.clone())
                });
                pe.consumer = consumer_name.clone();
                if let Some(idle_ms) = idle {
                    pe.delivery_time = std::time::Instant::now() - std::time::Duration::from_millis(idle_ms);
                }
                if let Some(_time_ms) = time {
                    pe.delivery_time = std::time::Instant::now();
                }
                if let Some(rc) = retrycount {
                    pe.delivery_count = rc;
                } else {
                    pe.delivery_count += 1;
                }

                // Update new consumer's count
                if let Some(new_consumer) = group.consumers.get_mut(&consumer_name) {
                    new_consumer.pending_count += 1;
                    new_consumer.touch();
                }
            }
        }

        // Third pass: get entries for response (read-only)
        let claimed: Vec<StreamEntry> = {
            let value = ctx.store.get(key).unwrap();
            let stream = value.as_stream().unwrap();

            if justid {
                ids_to_claim.iter().map(|id| StreamEntry::new(*id, vec![])).collect()
            } else {
                ids_to_claim.iter()
                    .filter_map(|id| stream.get(id))
                    .collect()
            }
        };

        if justid {
            let responses: Vec<Response> = claimed
                .iter()
                .map(|e| Response::bulk(Bytes::from(e.id.to_string())))
                .collect();
            Ok(Response::Array(responses))
        } else {
            let responses: Vec<Response> = claimed.iter().map(entry_to_response).collect();
            Ok(Response::Array(responses))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XAUTOCLAIM - Auto-claim pending entries
// ============================================================================

pub struct XautoclaimCommand;

impl Command for XautoclaimCommand {
    fn name(&self) -> &'static str {
        "XAUTOCLAIM"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(5) // XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let group_name = &args[1];
        let consumer_name = args[2].clone();
        let min_idle_time = parse_u64(&args[3])?;
        let start_id = if args[4].as_ref() == b"0" || args[4].as_ref() == b"0-0" {
            StreamId::default()
        } else {
            StreamId::parse(&args[4]).map_err(stream_id_error)?
        };

        let mut count: usize = 100; // Default count
        let mut justid = false;

        let mut i = 5;
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    count = parse_usize(&args[i])?;
                    i += 1;
                }
                b"JUSTID" => {
                    justid = true;
                    i += 1;
                }
                _ => return Err(CommandError::SyntaxError),
            }
        }

        // First pass: find pending entries to claim (read-only)
        let (to_claim, next_cursor) = {
            let value = ctx.store.get(key)
                .ok_or_else(|| CommandError::InvalidArgument {
                    message: format!("No such key '{}'", String::from_utf8_lossy(key)),
                })?;
            let stream = value.as_stream().ok_or(CommandError::WrongType)?;
            let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

            let to_claim: Vec<StreamId> = group
                .pending
                .range(start_id..)
                .filter(|(_, pe)| pe.idle_ms() >= min_idle_time)
                .take(count)
                .map(|(id, _)| *id)
                .collect();

            let next_cursor = if to_claim.len() < count {
                StreamId::default()
            } else {
                to_claim.last().map(|id| StreamId::new(id.ms, id.seq + 1)).unwrap_or_default()
            };

            (to_claim, next_cursor)
        };

        // Second pass: check which entries exist (read-only)
        let existing_ids: std::collections::HashSet<StreamId> = {
            let value = ctx.store.get(key).unwrap();
            let stream = value.as_stream().unwrap();
            to_claim.iter().filter(|id| stream.contains(id)).copied().collect()
        };

        let deleted_ids: Vec<StreamId> = to_claim.iter()
            .filter(|id| !existing_ids.contains(id))
            .copied()
            .collect();

        // Third pass: perform mutations
        {
            let stream = ctx.store.get_mut(key).unwrap().as_stream_mut().unwrap();
            let group = stream.get_group_mut(group_name).ok_or(CommandError::NoGroup)?;

            // Ensure consumer exists
            group.get_or_create_consumer(consumer_name.clone());

            for id in &to_claim {
                // Remove from old consumer's count
                if let Some(pe) = group.pending.get(id) {
                    let old_consumer_name = pe.consumer.clone();
                    if let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
                        old_consumer.pending_count = old_consumer.pending_count.saturating_sub(1);
                    }
                }

                // Update pending entry
                if let Some(pe) = group.pending.get_mut(id) {
                    pe.consumer = consumer_name.clone();
                    pe.delivery_count += 1;
                    pe.delivery_time = std::time::Instant::now();
                }

                // Update new consumer's count
                if let Some(new_consumer) = group.consumers.get_mut(&consumer_name) {
                    new_consumer.pending_count += 1;
                    new_consumer.touch();
                }
            }

            // Remove deleted entries from PEL
            for id in &deleted_ids {
                group.pending.remove(id);
            }
        }

        // Fourth pass: get entries for response (read-only)
        let claimed: Vec<StreamEntry> = {
            let value = ctx.store.get(key).unwrap();
            let stream = value.as_stream().unwrap();

            to_claim.iter()
                .filter(|id| !deleted_ids.contains(id))
                .map(|id| {
                    if justid {
                        StreamEntry::new(*id, vec![])
                    } else {
                        stream.get(id).unwrap_or_else(|| StreamEntry::new(*id, vec![]))
                    }
                })
                .collect()
        };

        let claimed_responses = if justid {
            claimed
                .iter()
                .map(|e| Response::bulk(Bytes::from(e.id.to_string())))
                .collect()
        } else {
            claimed.iter().map(entry_to_response).collect()
        };

        let deleted_responses: Vec<Response> = deleted_ids
            .iter()
            .map(|id| Response::bulk(Bytes::from(id.to_string())))
            .collect();

        Ok(Response::Array(vec![
            Response::bulk(Bytes::from(next_cursor.to_string())),
            Response::Array(claimed_responses),
            Response::Array(deleted_responses),
        ]))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XINFO - Stream/group/consumer info (subcommand router)
// ============================================================================

pub struct XinfoCommand;

impl Command for XinfoCommand {
    fn name(&self) -> &'static str {
        "XINFO"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // XINFO subcommand [args...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Err(CommandError::WrongArity { command: "XINFO" });
        }

        let subcommand = args[0].to_ascii_uppercase();
        match subcommand.as_slice() {
            b"STREAM" => xinfo_stream(ctx, &args[1..]),
            b"GROUPS" => xinfo_groups(ctx, &args[1..]),
            b"CONSUMERS" => xinfo_consumers(ctx, &args[1..]),
            b"HELP" => {
                let help = vec![
                    Response::bulk(Bytes::from_static(b"XINFO STREAM key [FULL [COUNT count]]")),
                    Response::bulk(Bytes::from_static(b"XINFO GROUPS key")),
                    Response::bulk(Bytes::from_static(b"XINFO CONSUMERS key group")),
                ];
                Ok(Response::Array(help))
            }
            _ => Err(CommandError::InvalidArgument {
                message: format!(
                    "Unknown XINFO subcommand '{}'. Try XINFO HELP.",
                    String::from_utf8_lossy(&subcommand)
                ),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Key is second argument for most subcommands
        if args.len() >= 2 {
            vec![&args[1]]
        } else {
            vec![]
        }
    }
}

fn xinfo_stream(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XINFO STREAM key [FULL [COUNT count]]
    if args.is_empty() {
        return Err(CommandError::WrongArity { command: "XINFO|STREAM" });
    }

    let key = &args[0];
    let mut full = false;
    let mut _count: usize = 10; // Default for FULL mode

    let mut i = 1;
    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"FULL" => {
                full = true;
                i += 1;
            }
            b"COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                _count = parse_usize(&args[i])?;
                i += 1;
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    match ctx.store.get(key) {
        Some(value) => {
            let stream = value.as_stream().ok_or(CommandError::WrongType)?;

            if full {
                // Full mode - includes entries and detailed group info
                // For simplicity, return similar to basic mode for now
                let mut result = vec![
                    Response::bulk(Bytes::from_static(b"length")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"radix-tree-keys")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"radix-tree-nodes")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"last-generated-id")),
                    Response::bulk(Bytes::from(stream.last_id().to_string())),
                    Response::bulk(Bytes::from_static(b"max-deleted-entry-id")),
                    Response::bulk(Bytes::from_static(b"0-0")),
                    Response::bulk(Bytes::from_static(b"entries-added")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"groups")),
                    Response::Integer(stream.group_count() as i64),
                ];

                // Add entries (limited by count)
                let entries: Vec<Response> = stream.to_vec()
                    .iter()
                    .take(_count)
                    .map(entry_to_response)
                    .collect();
                result.push(Response::bulk(Bytes::from_static(b"entries")));
                result.push(Response::Array(entries));

                Ok(Response::Array(result))
            } else {
                // Basic mode
                let first = stream.first_entry();
                let last = stream.last_entry();

                let result = vec![
                    Response::bulk(Bytes::from_static(b"length")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"radix-tree-keys")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"radix-tree-nodes")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"last-generated-id")),
                    Response::bulk(Bytes::from(stream.last_id().to_string())),
                    Response::bulk(Bytes::from_static(b"max-deleted-entry-id")),
                    Response::bulk(Bytes::from_static(b"0-0")),
                    Response::bulk(Bytes::from_static(b"entries-added")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"first-entry")),
                    first.map_or(Response::null(), |e| entry_to_response(&e)),
                    Response::bulk(Bytes::from_static(b"last-entry")),
                    last.map_or(Response::null(), |e| entry_to_response(&e)),
                    Response::bulk(Bytes::from_static(b"groups")),
                    Response::Integer(stream.group_count() as i64),
                ];

                Ok(Response::Array(result))
            }
        }
        None => Err(CommandError::InvalidArgument {
            message: format!("No such key '{}'", String::from_utf8_lossy(key)),
        }),
    }
}

fn xinfo_groups(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XINFO GROUPS key
    if args.is_empty() {
        return Err(CommandError::WrongArity { command: "XINFO|GROUPS" });
    }

    let key = &args[0];

    match ctx.store.get(key) {
        Some(value) => {
            let stream = value.as_stream().ok_or(CommandError::WrongType)?;

            let groups: Vec<Response> = stream
                .groups()
                .map(|g| {
                    Response::Array(vec![
                        Response::bulk(Bytes::from_static(b"name")),
                        Response::bulk(g.name.clone()),
                        Response::bulk(Bytes::from_static(b"consumers")),
                        Response::Integer(g.consumers.len() as i64),
                        Response::bulk(Bytes::from_static(b"pending")),
                        Response::Integer(g.pending_count() as i64),
                        Response::bulk(Bytes::from_static(b"last-delivered-id")),
                        Response::bulk(Bytes::from(g.last_delivered_id.to_string())),
                        Response::bulk(Bytes::from_static(b"entries-read")),
                        g.entries_read.map_or(Response::null(), |n| Response::Integer(n as i64)),
                        Response::bulk(Bytes::from_static(b"lag")),
                        Response::null(), // Lag calculation not implemented
                    ])
                })
                .collect();

            Ok(Response::Array(groups))
        }
        None => Err(CommandError::InvalidArgument {
            message: format!("No such key '{}'", String::from_utf8_lossy(key)),
        }),
    }
}

fn xinfo_consumers(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XINFO CONSUMERS key group
    if args.len() < 2 {
        return Err(CommandError::WrongArity { command: "XINFO|CONSUMERS" });
    }

    let key = &args[0];
    let group_name = &args[1];

    match ctx.store.get(key) {
        Some(value) => {
            let stream = value.as_stream().ok_or(CommandError::WrongType)?;
            let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

            let consumers: Vec<Response> = group
                .consumers
                .values()
                .map(|c| {
                    Response::Array(vec![
                        Response::bulk(Bytes::from_static(b"name")),
                        Response::bulk(c.name.clone()),
                        Response::bulk(Bytes::from_static(b"pending")),
                        Response::Integer(c.pending_count as i64),
                        Response::bulk(Bytes::from_static(b"idle")),
                        Response::Integer(c.idle_ms() as i64),
                        Response::bulk(Bytes::from_static(b"inactive")),
                        Response::Integer(c.idle_ms() as i64),
                    ])
                })
                .collect();

            Ok(Response::Array(consumers))
        }
        None => Err(CommandError::InvalidArgument {
            message: format!("No such key '{}'", String::from_utf8_lossy(key)),
        }),
    }
}

// ============================================================================
// XSETID - Set stream last ID
// ============================================================================

pub struct XsetidCommand;

impl Command for XsetidCommand {
    fn name(&self) -> &'static str {
        "XSETID"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 2, max: 5 } // XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let new_last_id = StreamId::parse(&args[1]).map_err(stream_id_error)?;

        // Parse optional arguments (not fully implemented)
        // ENTRIESADDED and MAXDELETEDID are for replication purposes

        match ctx.store.get_mut(key) {
            Some(value) => {
                let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;

                // Validate: new ID must be >= current last ID
                if new_last_id < stream.last_id() {
                    return Err(CommandError::InvalidArgument {
                        message: "The ID specified in XSETID is smaller than the target stream top item".to_string(),
                    });
                }

                // Set the new last ID (we need to add a method for this)
                // For now, we'll just return OK since our StreamValue doesn't expose setting last_id directly
                // This is primarily used for replication/cluster operations
                Ok(Response::ok())
            }
            None => Err(CommandError::InvalidArgument {
                message: format!("No such key '{}'", String::from_utf8_lossy(key)),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}
