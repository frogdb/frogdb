use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, impl_keys_first};
use frogdb_protocol::Response;

use super::{
    get_json_mut, json_error_to_command_error, parse_json_value, parse_path, single_or_multi,
};

// ============================================================================
// JSON.CLEAR - Clear containers (arrays/objects) or set numbers to 0
// ============================================================================

pub struct JsonClearCommand;

impl Command for JsonClearCommand {
    fn name(&self) -> &'static str {
        "JSON.CLEAR"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // key [path]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = parse_path(args.get(1));

        let json = get_json_mut!(ctx, key, Response::Integer(0));
        let cleared = json.clear(&path).map_err(json_error_to_command_error)?;
        Ok(Response::Integer(cleared as i64))
    }

    impl_keys_first!();
}

// ============================================================================
// JSON.TOGGLE - Toggle boolean values
// ============================================================================

pub struct JsonToggleCommand;

impl Command for JsonToggleCommand {
    fn name(&self) -> &'static str {
        "JSON.TOGGLE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // key [path]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
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

    impl_keys_first!();
}

// ============================================================================
// JSON.MERGE - Merge a JSON value using RFC 7396 JSON Merge Patch
// ============================================================================

pub struct JsonMergeCommand;

impl Command for JsonMergeCommand {
    fn name(&self) -> &'static str {
        "JSON.MERGE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // key path value
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
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

    impl_keys_first!();
}
