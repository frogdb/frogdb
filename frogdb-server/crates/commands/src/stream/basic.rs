use bytes::Bytes;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    EventSpec, ExecutionStrategy, KeySpec, KeyspaceEventFlags, LookupSpec, StoreTypedFamilyExt,
    StreamId, StreamValue, WaiterKind, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::super::utils::{parse_i64, parse_usize};
use super::{entry_to_response, parse_delete_ref_strategy, parse_ids_block, parse_trim_options};

// ============================================================================
// XADD - Add entry to stream
// ============================================================================

pub struct XaddCommand;

impl Command for XaddCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XADD",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::Kind(WaiterKind::Stream),
            // XADD unconditionally notifies "xadd" on success (xaddCommand,
            // t_stream.c:2562), but a trailing MAXLEN/MINID clause fires a
            // *second*, conditional "xtrim" only when that trim actually
            // removed something (t_stream.c:2567, `if (streamTrim(...))`) —
            // Dynamic so both can be deposited independently.
            event: EventSpec::Dynamic,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let mut nomkstream = false;
        let mut trim_options = None;

        // Parse options before ID, stopping at the first non-option (the ID).
        let mut parser = ArgParser::from_position(args, 1);
        while parser.has_more() {
            if parser.try_flag(b"NOMKSTREAM") {
                nomkstream = true;
            } else if parser.peek().is_some_and(|a| {
                a.eq_ignore_ascii_case(b"MAXLEN") || a.eq_ignore_ascii_case(b"MINID")
            }) {
                // MAXLEN/MINID introduce a trim clause consuming a variable
                // number of following tokens (optional =/~, threshold, optional
                // LIMIT); delegate to the shared parser and resync position.
                let (opts, next_i) = parse_trim_options(args, parser.position())?;
                trim_options = opts;
                parser.reset_to(next_i);
            } else {
                break;
            }
        }
        let mut i = parser.position();

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
        let stream = ctx.get_or_create::<StreamValue>(key)?;

        // Add entry
        let id = stream.add(id_spec, fields)?;

        // Apply trimming if specified
        let trimmed = trim_options.map(|opts| stream.trim(opts)).unwrap_or(0);

        ctx.notify_event(key.clone(), "xadd", KeyspaceEventFlags::STREAM);
        if trimmed > 0 {
            ctx.notify_event(key.clone(), "xtrim", KeyspaceEventFlags::STREAM);
        }

        Ok(Response::bulk(Bytes::from(id.to_string())))
    }
}

// ============================================================================
// XLEN - Get stream length
// ============================================================================

pub struct XlenCommand;

impl Command for XlenCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XLEN",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_stream(key)? {
            Some(stream) => Ok(Response::Integer(stream.len() as i64)),
            None => Ok(Response::Integer(0)),
        }
    }
}

// ============================================================================
// XRANGE - Query by ID range
// ============================================================================

pub struct XrangeCommand;

impl Command for XrangeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XRANGE",
            arity: Arity::Range { min: 3, max: 5 },
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
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

        match ctx.store.get_stream(key)? {
            Some(stream) => {
                let entries = stream.range(start, end, count);
                let responses: Vec<Response> = entries.iter().map(entry_to_response).collect();
                Ok(Response::Array(responses))
            }
            None => Ok(Response::Array(vec![])),
        }
    }
}

// ============================================================================
// XREVRANGE - Reverse query by ID range
// ============================================================================

pub struct XrevrangeCommand;

impl Command for XrevrangeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XREVRANGE",
            arity: Arity::Range { min: 3, max: 5 },
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
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

        match ctx.store.get_stream(key)? {
            Some(stream) => {
                let entries = stream.range_rev(end, start, count);
                let responses: Vec<Response> = entries.iter().map(entry_to_response).collect();
                Ok(Response::Array(responses))
            }
            None => Ok(Response::Array(vec![])),
        }
    }
}

// ============================================================================
// XDEL - Delete specific entries
// ============================================================================

pub struct XdelCommand;

impl Command for XdelCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XDEL",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            // Redis fires "xdel" only when at least one ID was actually
            // deleted (xdelCommand, t_stream.c:4582, `if (deleted) { ...
            // notifyKeyspaceEvent(...,"xdel",...) }`) — a call whose IDs are
            // all absent notifies nothing, so this is Dynamic rather than a
            // blanket Emits.
            event: EventSpec::Dynamic,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse IDs
        let mut ids = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            let id = StreamId::parse(arg)?;
            ids.push(id);
        }

        match ctx.store.get_stream_mut(key)? {
            Some(stream) => {
                let deleted = stream.delete(&ids);
                if deleted > 0 {
                    ctx.notify_event(key.clone(), "xdel", KeyspaceEventFlags::STREAM);
                }
                Ok(Response::Integer(deleted as i64))
            }
            None => Ok(Response::Integer(0)),
        }
    }
}

// ============================================================================
// XTRIM - Trim stream
// ============================================================================

pub struct XtrimCommand;

impl Command for XtrimCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XTRIM",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            // Redis fires "xtrim" only when at least one entry was actually
            // removed (xtrimCommand, t_stream.c:4776, `if (deleted) { ...
            // notifyKeyspaceEvent(...,"xtrim",...) }`) — a trim that removes
            // nothing (e.g. MAXLEN already satisfied) notifies nothing.
            event: EventSpec::Dynamic,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let (trim_options, _) = parse_trim_options(args, 1)?;
        let trim_options = trim_options.ok_or(CommandError::SyntaxError)?;

        match ctx.store.get_stream_mut(key)? {
            Some(stream) => {
                let trimmed = stream.trim(trim_options);
                if trimmed > 0 {
                    ctx.notify_event(key.clone(), "xtrim", KeyspaceEventFlags::STREAM);
                }
                Ok(Response::Integer(trimmed as i64))
            }
            None => Ok(Response::Integer(0)),
        }
    }
}

// ============================================================================
// XSETID - Set stream last ID
// ============================================================================

pub struct XsetidCommand;

impl Command for XsetidCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XSETID",
            arity: Arity::Range { min: 2, max: 5 },
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            // Redis unconditionally notifies "xsetid" on success (xsetidCommand,
            // t_stream.c:3667) — every error path (missing key, ID regression)
            // returns before that point, so a static Emits matches XADD/XDEL/XTRIM.
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::STREAM,
                name: "xsetid",
            },
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let new_last_id = StreamId::parse(&args[1])?;

        // Parse optional keyword arguments: ENTRIESADDED <n> | MAXDELETEDID <id>
        let mut entries_added_val: Option<u64> = None;
        let mut max_deleted_id_val: Option<StreamId> = None;
        let mut parser = ArgParser::from_position(args, 2);
        while parser.has_more() {
            if parser.try_flag(b"ENTRIESADDED") {
                let val = parse_i64(parser.next_arg()?)?;
                if val < 0 {
                    return Err(CommandError::InvalidArgument {
                        message: "Invalid entries-added specified for XSETID".to_string(),
                    });
                }
                entries_added_val = Some(val as u64);
            } else if parser.try_flag(b"MAXDELETEDID") {
                max_deleted_id_val = Some(StreamId::parse(parser.next_arg()?)?);
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        // Validate: max_deleted_id must be <= new_last_id
        if let Some(mdid) = max_deleted_id_val
            && mdid > new_last_id
        {
            return Err(CommandError::InvalidArgument {
                message:
                    "The ID specified in XSETID is smaller than the provided max_deleted_entry_id"
                        .to_string(),
            });
        }

        match ctx.store.get_stream_mut(key)? {
            Some(stream) => {
                // Validate: new ID must be >= current last ID
                if new_last_id < stream.last_id() {
                    return Err(CommandError::InvalidArgument {
                        message:
                            "The ID specified in XSETID is smaller than the target stream top item"
                                .to_string(),
                    });
                }

                stream.set_last_id(new_last_id);
                if let Some(ea) = entries_added_val {
                    stream.set_entries_added(ea);
                }
                if let Some(mdid) = max_deleted_id_val {
                    stream.set_max_deleted_id(mdid);
                }

                Ok(Response::ok())
            }
            None => {
                // XSETID on a nonexistent key returns an error in Redis 7+
                Err(CommandError::InvalidArgument {
                    message: "ERR The XSETID target key does not exist".to_string(),
                })
            }
        }
    }
}

// ============================================================================
// XDELEX - Extended delete with reference control
// ============================================================================

pub struct XdelexCommand;

impl Command for XdelexCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XDELEX",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            // Redis fires "xdel" (the same event XDEL uses) only when an ID was
            // actually removed from the stream (xdelexCommand, t_stream.c:4739,
            // `if (deleted) { ... notifyKeyspaceEvent(...,"xdel",...) }`) — an
            // all-STILL_REFERENCED/all-not-found call notifies nothing, so this
            // is Dynamic rather than a blanket Emits.
            event: EventSpec::Dynamic,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let mut i = 1;

        // Parse optional KEEPREF/DELREF/ACKED
        let strategy = parse_delete_ref_strategy(args, &mut i);

        // Parse IDS numids id [id ...]
        let ids = parse_ids_block(args, &mut i)?;

        match ctx.store.get_stream_mut(key)? {
            Some(stream) => {
                let results = stream.delete_ex(&ids, strategy);
                // `1` marks an ID that was actually deleted (see delete_ex);
                // `-1` not-found and `2` acked-but-not-deleted never delete.
                if results.contains(&1) {
                    ctx.notify_event(key.clone(), "xdel", KeyspaceEventFlags::STREAM);
                }
                Ok(Response::Array(
                    results.into_iter().map(Response::Integer).collect(),
                ))
            }
            None => {
                // Non-existent key: all IDs return -1
                Ok(Response::Array(vec![Response::Integer(-1); ids.len()]))
            }
        }
    }
}
