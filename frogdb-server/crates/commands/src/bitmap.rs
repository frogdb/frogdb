//! Bitmap commands.
//!
//! Commands for bit-level operations on strings:
//! - SETBIT, GETBIT - single bit operations
//! - BITCOUNT - count set bits
//! - BITOP - bitwise operations between strings
//! - BITPOS - find first bit set to 0 or 1
//! - BITFIELD, BITFIELD_RO - arbitrary bit field operations

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, BitOp, BitfieldEncoding, BitfieldOffset, BitfieldSubCommand,
    Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec, ExecutionStrategy,
    KeyAccessFlag, KeySpec, KeyspaceEventFlags, LookupSpec, OverflowMode, StringValue, Value,
    WaiterWake, WalStrategy, bitop,
};
use frogdb_protocol::Response;

use super::utils::{parse_i64, parse_u64};

// ============================================================================
// SETBIT - Set or clear a bit
// ============================================================================

pub struct SetbitCommand;

impl Command for SetbitCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SETBIT",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::UniformRW,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let offset = parse_u64(&args[1])?;
        let value = parse_u64(&args[2])?;

        if value > 1 {
            return Err(CommandError::InvalidArgument {
                message: "bit is not an integer or out of range".to_string(),
            });
        }

        // Maximum offset (512MB * 8 = 4GB bits)
        const MAX_OFFSET: u64 = 512 * 1024 * 1024 * 8;
        if offset >= MAX_OFFSET {
            return Err(CommandError::InvalidArgument {
                message: "bit offset is not an integer or out of range".to_string(),
            });
        }

        let (old_bit, is_dirty) = if let Some(existing) = ctx.store.get_mut(key) {
            if let Some(sv) = existing.as_string_mut() {
                let byte_idx = (offset / 8) as usize;
                let will_extend = byte_idx >= sv.as_bytes().len();
                let old = sv.setbit(offset, value as u8);
                // Dirty if the bit value changed or the string was extended
                (old, old != value as u8 || will_extend)
            } else {
                return Err(CommandError::WrongType);
            }
        } else {
            // Create new empty string - always dirty (key creation)
            let mut sv = StringValue::new(Bytes::new());
            let old_bit = sv.setbit(offset, value as u8);
            ctx.store.set(key.clone(), Value::String(sv));
            (old_bit, true)
        };

        // Signal dirty state to the shard for rdb_changes_since_last_save tracking
        if !is_dirty {
            ctx.effects.dirty_delta = -1; // No actual change, suppress dirty increment
        }

        Ok(Response::Integer(old_bit as i64))
    }
}

// ============================================================================
// GETBIT - Get a bit value
// ============================================================================

pub struct GetbitCommand;

impl Command for GetbitCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GETBIT",
            arity: Arity::Fixed(2),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let offset = parse_u64(&args[1])?;

        let bit = match ctx.store.get(key) {
            Some(value) => {
                if let Some(sv) = value.as_string() {
                    sv.getbit(offset)
                } else {
                    return Err(CommandError::WrongType);
                }
            }
            None => 0,
        };

        Ok(Response::Integer(bit as i64))
    }
}

// ============================================================================
// BITCOUNT - Count set bits
// ============================================================================

pub struct BitcountCommand;

impl Command for BitcountCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BITCOUNT",
            arity: Arity::Range { min: 1, max: 4 },
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Must have 0 or 2 range args (not 1)
        if args.len() == 2 {
            return Err(CommandError::SyntaxError);
        }

        let (start, end, bit_mode) = if args.len() >= 3 {
            let start = Some(parse_i64(&args[1])?);
            let end = Some(parse_i64(&args[2])?);

            let bit_mode = if args.len() >= 4 {
                let mode = args[3].to_ascii_uppercase();
                match mode.as_slice() {
                    b"BYTE" => false,
                    b"BIT" => true,
                    _ => {
                        return Err(CommandError::SyntaxError);
                    }
                }
            } else {
                false
            };

            (start, end, bit_mode)
        } else {
            (None, None, false)
        };

        let count = match ctx.store.get(key) {
            Some(value) => {
                if let Some(sv) = value.as_string() {
                    sv.bitcount(start, end, bit_mode)
                } else {
                    return Err(CommandError::WrongType);
                }
            }
            None => 0,
        };

        Ok(Response::Integer(count as i64))
    }
}

// ============================================================================
// BITOP - Bitwise operations
// ============================================================================

pub struct BitopCommand;

impl Command for BitopCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BITOP",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::Skip(1),
            // Destination (key 0 of the `Skip(1)` extraction, args[1]) is
            // overwritten; the source keys are read-only.
            access: AccessSpec::Positional(&[KeyAccessFlag::OW, KeyAccessFlag::R]),
            // Persist-or-delete the destination: a non-empty result stores, an
            // empty result deletes (bitops.c bitopCommand). The deletion must
            // reach the WAL so it survives a restart — a plain persist-if-exists
            // would leave the stale prior value on disk. `Dynamic` resolves the
            // destination through the declared write-access key (the sole `OW`
            // key above) and emits persist-or-delete over it.
            wal: WalStrategy::Dynamic,
            wakes: WaiterWake::None,
            // Runtime-deposited (bitops.c bitopCommand): `set` under
            // NOTIFY_STRING on the destination when the result is non-empty,
            // but a `del` under NOTIFY_GENERIC when the result is empty and a
            // pre-existing destination is deleted (nothing at all when there
            // was no destination to delete). The read-only sources stay silent.
            // Set-or-del cannot be a static EmitsAt — see execute().
            event: EventSpec::Dynamic,
            requires_same_slot: true,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let op = BitOp::parse(&args[0]).ok_or_else(|| CommandError::InvalidArgument {
            message: "operation is not a valid BITOP operation".to_string(),
        })?;

        let destkey = &args[1];
        let source_keys = &args[2..];

        // NOT requires exactly one source
        if op == BitOp::Not && source_keys.len() != 1 {
            return Err(CommandError::InvalidArgument {
                message: "BITOP NOT requires exactly one source key".to_string(),
            });
        }

        // Collect source strings
        let mut sources: Vec<Bytes> = Vec::with_capacity(source_keys.len());
        for key in source_keys {
            if let Some(value) = ctx.store.get(key) {
                if let Some(sv) = value.as_string() {
                    sources.push(sv.as_bytes());
                } else {
                    return Err(CommandError::WrongType);
                }
            } else {
                sources.push(Bytes::new());
            }
        }

        let source_refs: Vec<&[u8]> = sources.iter().map(|b| b.as_ref()).collect();
        let result = bitop(op, &source_refs);
        let result_len = result.len();

        // Redis parity (bitops.c bitopCommand): a non-empty result is stored
        // and fires `set`; an empty result deletes the destination and fires
        // `del` iff the destination existed (a never-existed destination stays
        // absent and silent).
        if result_len == 0 {
            if ctx.store.delete(destkey) {
                ctx.notify_event(destkey.clone(), "del", KeyspaceEventFlags::GENERIC);
            }
        } else {
            ctx.store.set(
                destkey.clone(),
                Value::String(StringValue::new(Bytes::from(result))),
            );
            ctx.notify_event(destkey.clone(), "set", KeyspaceEventFlags::STRING);
        }

        Ok(Response::Integer(result_len as i64))
    }
}

// ============================================================================
// BITPOS - Find first bit set to 0 or 1
// ============================================================================

pub struct BitposCommand;

impl Command for BitposCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BITPOS",
            arity: Arity::Range { min: 2, max: 5 },
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let bit = parse_u64(&args[1])?;

        if bit > 1 {
            return Err(CommandError::InvalidArgument {
                message: "bit must be 0 or 1".to_string(),
            });
        }

        let (start, end, bit_mode, end_given) = if args.len() >= 3 {
            let start = Some(parse_i64(&args[2])?);
            let end_given = args.len() >= 4;
            let end = if end_given {
                Some(parse_i64(&args[3])?)
            } else {
                None
            };

            let bit_mode = if args.len() >= 5 {
                let mode = args[4].to_ascii_uppercase();
                match mode.as_slice() {
                    b"BYTE" => false,
                    b"BIT" => true,
                    _ => {
                        return Err(CommandError::SyntaxError);
                    }
                }
            } else {
                false
            };

            (start, end, bit_mode, end_given)
        } else {
            (None, None, false, false)
        };

        let pos = match ctx.store.get(key) {
            Some(value) => {
                if let Some(sv) = value.as_string() {
                    sv.bitpos(bit as u8, start, end, bit_mode, end_given)
                } else {
                    return Err(CommandError::WrongType);
                }
            }
            None => {
                // For empty/non-existent key, looking for 0 returns 0, looking for 1 returns -1
                if bit == 0 { Some(0) } else { None }
            }
        };

        match pos {
            Some(p) => Ok(Response::Integer(p)),
            None => Ok(Response::Integer(-1)),
        }
    }
}

// ============================================================================
// BITFIELD - Arbitrary bit field operations
// ============================================================================

pub struct BitfieldCommand;

impl Command for BitfieldCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BITFIELD",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            // VARIABLE_FLAGS in Redis: the required perm depends on the
            // sub-operations. A `GET` sub-op reads (`ACCESS`), `SET`/`INCRBY`
            // write (`UPDATE`). Resolved per-invocation in
            // `dynamic_keys_with_flags`: `RW` when both present, `R` for a
            // GET-only BITFIELD, `OW` for write-only sub-ops.
            access: AccessSpec::Dynamic,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        execute_bitfield(ctx, args, false)
    }

    /// Per-key access flags for `COMMAND GETKEYSANDFLAGS` / ACL, resolved from
    /// the BITFIELD sub-operations (Redis `VARIABLE_FLAGS`): `GET` contributes
    /// read, `SET`/`INCRBY` contribute write. `OVERFLOW` takes one argument
    /// (`WRAP`/`SAT`/`FAIL`) and contributes nothing.
    fn dynamic_keys_with_flags<'a>(
        &self,
        args: &'a [Bytes],
    ) -> Vec<(&'a [u8], Vec<KeyAccessFlag>)> {
        let Some(key) = args.first() else {
            return Vec::new();
        };
        let mut has_read = false;
        let mut has_write = false;
        let mut i = 1;
        while i < args.len() {
            let tok = args[i].as_ref();
            if tok.eq_ignore_ascii_case(b"GET") {
                has_read = true;
                i += 3; // GET type offset
            } else if tok.eq_ignore_ascii_case(b"SET") || tok.eq_ignore_ascii_case(b"INCRBY") {
                has_write = true;
                i += 4; // SET/INCRBY type offset value
            } else if tok.eq_ignore_ascii_case(b"OVERFLOW") {
                i += 2; // OVERFLOW WRAP|SAT|FAIL
            } else {
                i += 1;
            }
        }
        let flag = match (has_read, has_write) {
            (true, true) => KeyAccessFlag::RW,
            (true, false) => KeyAccessFlag::R,
            // Write-only sub-ops, or a malformed/empty op list: default to the
            // blind-overwrite flag a plain WRITE command would carry.
            (false, _) => KeyAccessFlag::OW,
        };
        vec![(key.as_ref(), vec![flag])]
    }
}

// ============================================================================
// BITFIELD_RO - Read-only bitfield operations
// ============================================================================

pub struct BitfieldRoCommand;

impl Command for BitfieldRoCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BITFIELD_RO",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        execute_bitfield(ctx, args, true)
    }
}

/// Execute BITFIELD or BITFIELD_RO.
fn execute_bitfield(
    ctx: &mut CommandContext,
    args: &[Bytes],
    readonly: bool,
) -> Result<Response, CommandError> {
    let key = &args[0];

    // Parse subcommands
    let subcommands = parse_bitfield_subcommands(&args[1..], readonly)?;

    // Track whether the key is new (for dirty tracking)
    let key_is_new = ctx.store.get(key).is_none();

    // Get or create the string value
    let mut data = if let Some(value) = ctx.store.get(key) {
        if let Some(sv) = value.as_string() {
            sv.as_bytes_vec()
        } else {
            return Err(CommandError::WrongType);
        }
    } else {
        Vec::new()
    };

    let mut results = Vec::new();
    let mut overflow_mode = OverflowMode::default();
    let mut modified = false;
    let mut dirty_count: i64 = 0;
    let original_data = data.clone();

    for subcmd in subcommands {
        match subcmd {
            BitfieldSubCommand::Get { encoding, offset } => {
                let bit_offset = offset.resolve(encoding);
                let value = frogdb_core::bitfield_get(&data, encoding, bit_offset);
                results.push(Response::Integer(value));
            }
            BitfieldSubCommand::Set {
                encoding,
                offset,
                value,
            } => {
                let bit_offset = offset.resolve(encoding);
                let data_before = data.clone();
                let old_value = frogdb_core::bitfield_set(&mut data, encoding, bit_offset, value);
                results.push(Response::Integer(old_value));
                modified = true;
                // Dirty if the value changed, the data length changed, or the key is new
                if key_is_new || data_before != data {
                    dirty_count += 1;
                }
            }
            BitfieldSubCommand::IncrBy {
                encoding,
                offset,
                increment,
            } => {
                let bit_offset = offset.resolve(encoding);
                let (new_value, _overflowed) = frogdb_core::bitfield_incrby(
                    &mut data,
                    encoding,
                    bit_offset,
                    increment,
                    overflow_mode,
                );
                match new_value {
                    Some(v) => results.push(Response::Integer(v)),
                    None => results.push(Response::null()),
                }
                modified = true;
                // INCRBY always counts as dirty
                dirty_count += 1;
            }
            BitfieldSubCommand::Overflow(mode) => {
                overflow_mode = mode;
            }
        }
    }

    // If the key was new but no write subcommands were issued, don't count as dirty
    // (GET-only operations don't create the key)
    let _ = original_data;

    // Update the value if modified
    if modified {
        if data.is_empty() {
            ctx.store.delete(key);
        } else {
            // Always create/overwrite with new value
            let mut new_sv = StringValue::new(Bytes::new());
            new_sv.set_bytes(data);
            ctx.store.set(key.clone(), Value::String(new_sv));
        }
    }

    // Signal dirty state to the shard for rdb_changes_since_last_save tracking
    if dirty_count > 0 {
        ctx.effects.dirty_delta = dirty_count;
    } else if modified {
        // Modified but no dirty subcommands -> suppress dirty increment
        ctx.effects.dirty_delta = -1;
    }

    Ok(Response::Array(results))
}

/// Parse BITFIELD subcommands.
fn parse_bitfield_subcommands(
    args: &[Bytes],
    readonly: bool,
) -> Result<Vec<BitfieldSubCommand>, CommandError> {
    let mut subcommands = Vec::new();
    let mut parser = ArgParser::new(args);

    while parser.has_more() {
        if parser.try_flag(b"GET") {
            if parser.remaining_count() < 2 {
                return Err(CommandError::SyntaxError);
            }
            let encoding =
                BitfieldEncoding::parse(parser.next_arg()?).ok_or(CommandError::SyntaxError)?;
            let offset =
                BitfieldOffset::parse(parser.next_arg()?).ok_or(CommandError::SyntaxError)?;
            subcommands.push(BitfieldSubCommand::Get { encoding, offset });
        } else if parser.try_flag(b"SET") {
            if readonly {
                return Err(CommandError::InvalidArgument {
                    message: "BITFIELD_RO only supports the GET subcommand".to_string(),
                });
            }
            if parser.remaining_count() < 3 {
                return Err(CommandError::SyntaxError);
            }
            let encoding =
                BitfieldEncoding::parse(parser.next_arg()?).ok_or(CommandError::SyntaxError)?;
            let offset =
                BitfieldOffset::parse(parser.next_arg()?).ok_or(CommandError::SyntaxError)?;
            let value = parse_i64(parser.next_arg()?)?;
            subcommands.push(BitfieldSubCommand::Set {
                encoding,
                offset,
                value,
            });
        } else if parser.try_flag(b"INCRBY") {
            if readonly {
                return Err(CommandError::InvalidArgument {
                    message: "BITFIELD_RO only supports the GET subcommand".to_string(),
                });
            }
            if parser.remaining_count() < 3 {
                return Err(CommandError::SyntaxError);
            }
            let encoding =
                BitfieldEncoding::parse(parser.next_arg()?).ok_or(CommandError::SyntaxError)?;
            let offset =
                BitfieldOffset::parse(parser.next_arg()?).ok_or(CommandError::SyntaxError)?;
            let increment = parse_i64(parser.next_arg()?)?;
            subcommands.push(BitfieldSubCommand::IncrBy {
                encoding,
                offset,
                increment,
            });
        } else if parser.try_flag(b"OVERFLOW") {
            if readonly {
                return Err(CommandError::InvalidArgument {
                    message: "BITFIELD_RO only supports the GET subcommand".to_string(),
                });
            }
            let mode = OverflowMode::parse(parser.next_arg()?).ok_or(CommandError::SyntaxError)?;
            subcommands.push(BitfieldSubCommand::Overflow(mode));
        } else {
            return Err(CommandError::SyntaxError);
        }
    }

    Ok(subcommands)
}
