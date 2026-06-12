use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::{
    get_json_mut, json_error_to_command_error, parse_json_value, parse_path, single_or_multi,
};

// ============================================================================
// JSON.CLEAR - Clear containers (arrays/objects) or set numbers to 0
// ============================================================================

pub struct JsonClearCommand;

impl Command for JsonClearCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.CLEAR",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = parse_path(args.get(1));

        let json = get_json_mut!(ctx, key, Response::Integer(0));
        let cleared = json.clear(&path).map_err(json_error_to_command_error)?;
        Ok(Response::Integer(cleared as i64))
    }
}

// ============================================================================
// JSON.TOGGLE - Toggle boolean values
// ============================================================================

pub struct JsonToggleCommand;

impl Command for JsonToggleCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.TOGGLE",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = parse_path(args.get(1));

        let json = get_json_mut!(ctx, key);
        let results = json.toggle(&path).map_err(json_error_to_command_error)?;

        Ok(single_or_multi(results, |b| {
            Response::Integer(if b { 1 } else { 0 })
        }))
    }
}

// ============================================================================
// JSON.MERGE - Merge a JSON value using RFC 7396 JSON Merge Patch
// ============================================================================

pub struct JsonMergeCommand;

impl Command for JsonMergeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.MERGE",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();
        let patch = parse_json_value(&args[2])?;

        let json = get_json_mut!(ctx, key);
        json.merge(&path, patch)
            .map_err(json_error_to_command_error)?;

        Ok(Response::ok())
    }
}
