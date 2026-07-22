use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;
use serde_json::Value as JsonData;

use super::{
    get_json, get_json_mut, json_error_to_command_error, parse_json_value, parse_path,
    single_or_multi,
};

// ============================================================================
// JSON.STRAPPEND - Append to a string at a path
// ============================================================================

pub struct JsonStrAppendCommand;

impl Command for JsonStrAppendCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.STRAPPEND",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::FirstKey {
                kind: frogdb_core::IndexKind::Json,
            },
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse path and value - if 3 args: key path value, if 2 args: key value (path = $)
        let (path, value_str) = if args.len() == 3 {
            (String::from_utf8_lossy(&args[1]).to_string(), &args[2])
        } else {
            ("$".to_string(), &args[1])
        };

        // Parse the JSON string value to extract the actual string content
        let append_value: JsonData = parse_json_value(value_str)?;
        let append_str = match &append_value {
            JsonData::String(s) => s.as_str(),
            _ => {
                return Err(CommandError::InvalidArgument {
                    message: "value must be a JSON string".to_string(),
                });
            }
        };

        let json = get_json_mut!(ctx, key);
        let results = json
            .str_append(&path, append_str)
            .map_err(json_error_to_command_error)?;

        Ok(single_or_multi(results, |len| {
            Response::Integer(len as i64)
        }))
    }
}

// ============================================================================
// JSON.STRLEN - Get the length of a string at a path
// ============================================================================

pub struct JsonStrLenCommand;

impl Command for JsonStrLenCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.STRLEN",
            arity: Arity::AtLeast(1),
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
        let path = parse_path(args.get(1));

        let json = get_json!(ctx, key);

        let results = json.str_len(&path).map_err(json_error_to_command_error)?;

        if results.is_empty() {
            return Ok(Response::null());
        }

        Ok(single_or_multi(results, |len| match len {
            Some(l) => Response::Integer(l as i64),
            None => Response::null(),
        }))
    }
}
