use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, WalStrategy, impl_keys_first,
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
    fn name(&self) -> &'static str {
        "JSON.ARRAPPEND"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // key path value [value ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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

    impl_keys_first!();
}

// ============================================================================
// JSON.ARRINDEX - Find the index of a value in an array
// ============================================================================

pub struct JsonArrIndexCommand;

impl Command for JsonArrIndexCommand {
    fn name(&self) -> &'static str {
        "JSON.ARRINDEX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // key path value [start [stop]]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
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

    impl_keys_first!();
}

// ============================================================================
// JSON.ARRINSERT - Insert values into an array at a specific index
// ============================================================================

pub struct JsonArrInsertCommand;

impl Command for JsonArrInsertCommand {
    fn name(&self) -> &'static str {
        "JSON.ARRINSERT"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4) // key path index value [value ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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

    impl_keys_first!();
}

// ============================================================================
// JSON.ARRLEN - Get the length of an array at a path
// ============================================================================

pub struct JsonArrLenCommand;

impl Command for JsonArrLenCommand {
    fn name(&self) -> &'static str {
        "JSON.ARRLEN"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // key [path]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
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

    impl_keys_first!();
}

// ============================================================================
// JSON.ARRPOP - Pop a value from an array
// ============================================================================

pub struct JsonArrPopCommand;

impl Command for JsonArrPopCommand {
    fn name(&self) -> &'static str {
        "JSON.ARRPOP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // key [path [index]]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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

    impl_keys_first!();
}

// ============================================================================
// JSON.ARRTRIM - Trim an array to a range
// ============================================================================

pub struct JsonArrTrimCommand;

impl Command for JsonArrTrimCommand {
    fn name(&self) -> &'static str {
        "JSON.ARRTRIM"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(4) // key path start stop
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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

    impl_keys_first!();
}
