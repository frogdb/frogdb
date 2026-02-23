use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

use super::{json_error_to_command_error, parse_json_value, parse_path};

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

        // Check existence and type
        {
            let value = match ctx.store.get(key) {
                Some(v) => v,
                None => return Ok(Response::Integer(0)),
            };
            if value.as_json().is_none() {
                return Err(CommandError::WrongType);
            }
        }

        // Get mutable reference and clear
        let json = ctx.store.get_mut(key).unwrap().as_json_mut().unwrap();
        let cleared = json.clear(&path).map_err(json_error_to_command_error)?;
        Ok(Response::Integer(cleared as i64))
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

        // Check existence and type
        {
            let value = match ctx.store.get(key) {
                Some(v) => v,
                None => return Ok(Response::null()),
            };
            if value.as_json().is_none() {
                return Err(CommandError::WrongType);
            }
        }

        // Get mutable reference and toggle
        let json = ctx.store.get_mut(key).unwrap().as_json_mut().unwrap();
        let results = json.toggle(&path).map_err(json_error_to_command_error)?;

        if results.len() == 1 {
            Ok(Response::Integer(if results[0] { 1 } else { 0 }))
        } else {
            let responses: Vec<Response> = results
                .iter()
                .map(|&b| Response::Integer(if b { 1 } else { 0 }))
                .collect();
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

        // Check existence and type
        {
            let value = match ctx.store.get(key) {
                Some(v) => v,
                None => return Ok(Response::null()),
            };
            if value.as_json().is_none() {
                return Err(CommandError::WrongType);
            }
        }

        // Get mutable reference and merge
        let json = ctx.store.get_mut(key).unwrap().as_json_mut().unwrap();
        json.merge(&path, patch)
            .map_err(json_error_to_command_error)?;

        Ok(Response::ok())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}
