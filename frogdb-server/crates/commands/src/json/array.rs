use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::{
    get_json, get_json_mut, json_error_to_command_error, parse_json_value, parse_path,
    single_or_multi,
};
use crate::utils::parse_i64;

// ============================================================================
// JSON.ARRAPPEND - Append values to an array at a path
// ============================================================================

pub struct JsonArrAppendCommand;

impl Command for JsonArrAppendCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.ARRAPPEND",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();

        // Parse all values
        let mut values = Vec::new();
        for arg in args.iter().skip(2) {
            values.push(parse_json_value(arg)?);
        }

        let json = get_json_mut!(ctx, key);
        let results = json
            .arr_append(&path, values)
            .map_err(json_error_to_command_error)?;

        Ok(single_or_multi(results, |len| {
            Response::Integer(len as i64)
        }))
    }
}

// ============================================================================
// JSON.ARRINDEX - Find the index of a value in an array
// ============================================================================

pub struct JsonArrIndexCommand;

impl Command for JsonArrIndexCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.ARRINDEX",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();
        let value = parse_json_value(&args[2])?;
        let start = if args.len() > 3 {
            parse_i64(&args[3])?
        } else {
            0
        };
        let stop = if args.len() > 4 {
            parse_i64(&args[4])?
        } else {
            0
        };

        let json = get_json!(ctx, key);

        let results = json
            .arr_index(&path, &value, start, stop)
            .map_err(json_error_to_command_error)?;

        Ok(single_or_multi(results, Response::Integer))
    }
}

// ============================================================================
// JSON.ARRINSERT - Insert values into an array at a specific index
// ============================================================================

pub struct JsonArrInsertCommand;

impl Command for JsonArrInsertCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.ARRINSERT",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();
        let index = parse_i64(&args[2])?;

        // Parse all values
        let mut values = Vec::new();
        for arg in args.iter().skip(3) {
            values.push(parse_json_value(arg)?);
        }

        let json = get_json_mut!(ctx, key);
        let results = json
            .arr_insert(&path, index, values)
            .map_err(json_error_to_command_error)?;

        Ok(single_or_multi(results, |len| {
            Response::Integer(len as i64)
        }))
    }
}

// ============================================================================
// JSON.ARRLEN - Get the length of an array at a path
// ============================================================================

pub struct JsonArrLenCommand;

impl Command for JsonArrLenCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.ARRLEN",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = parse_path(args.get(1));

        let json = get_json!(ctx, key);

        let results = json.arr_len(&path).map_err(json_error_to_command_error)?;

        if results.is_empty() {
            return Ok(Response::null());
        }

        Ok(single_or_multi(results, |len| match len {
            Some(l) => Response::Integer(l as i64),
            None => Response::null(),
        }))
    }
}

// ============================================================================
// JSON.ARRPOP - Pop a value from an array
// ============================================================================

pub struct JsonArrPopCommand;

impl Command for JsonArrPopCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.ARRPOP",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = parse_path(args.get(1));
        let index = if args.len() > 2 {
            Some(parse_i64(&args[2])?)
        } else {
            None
        };

        let json = get_json_mut!(ctx, key);
        let results = json
            .arr_pop(&path, index)
            .map_err(json_error_to_command_error)?;

        Ok(single_or_multi(results, |value| match value {
            Some(v) => {
                let json_str = serde_json::to_string(&v).unwrap_or_default();
                Response::bulk(Bytes::from(json_str))
            }
            None => Response::null(),
        }))
    }
}

// ============================================================================
// JSON.ARRTRIM - Trim an array to a range
// ============================================================================

pub struct JsonArrTrimCommand;

impl Command for JsonArrTrimCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.ARRTRIM",
            arity: Arity::Fixed(4),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();
        let start = parse_i64(&args[2])?;
        let stop = parse_i64(&args[3])?;

        let json = get_json_mut!(ctx, key);
        let results = json
            .arr_trim(&path, start, stop)
            .map_err(json_error_to_command_error)?;

        Ok(single_or_multi(results, |len| {
            Response::Integer(len as i64)
        }))
    }
}
