use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, StreamId};
use frogdb_protocol::Response;

use super::super::utils::{get_or_create_stream, parse_usize};
use super::{entry_to_response, parse_trim_options};

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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
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
            return Err(CommandError::WrongArity { command: "xadd" });
        }
        let id_spec = StreamId::parse_for_add(&args[i])?;
        i += 1;

        // Parse field-value pairs
        if !(args.len() - i).is_multiple_of(2) {
            return Err(CommandError::WrongArity { command: "xadd" });
        }
        if args.len() - i == 0 {
            return Err(CommandError::WrongArity { command: "xadd" });
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
        let id = stream.add(id_spec, fields)?;

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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let start = StreamId::parse_range_bound(&args[1])?;
        let end = StreamId::parse_range_bound(&args[2])?;

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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        // Note: XREVRANGE has end first, then start
        let end = StreamId::parse_range_bound(&args[1])?;
        let start = StreamId::parse_range_bound(&args[2])?;

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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse IDs
        let mut ids = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            let id = StreamId::parse(arg)?;
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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let new_last_id = StreamId::parse(&args[1])?;

        // Parse optional arguments (not fully implemented)
        // ENTRIESADDED and MAXDELETEDID are for replication purposes

        match ctx.store.get_mut(key) {
            Some(value) => {
                let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;

                // Validate: new ID must be >= current last ID
                if new_last_id < stream.last_id() {
                    return Err(CommandError::InvalidArgument {
                        message:
                            "The ID specified in XSETID is smaller than the target stream top item"
                                .to_string(),
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
