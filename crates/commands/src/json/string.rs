use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, impl_keys_first};
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
    fn name(&self) -> &'static str {
        "JSON.STRAPPEND"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // JSON.STRAPPEND key [path] value
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
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

    impl_keys_first!();
}

// ============================================================================
// JSON.STRLEN - Get the length of a string at a path
// ============================================================================

pub struct JsonStrLenCommand;

impl Command for JsonStrLenCommand {
    fn name(&self) -> &'static str {
        "JSON.STRLEN"
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

        let results = json.str_len(&path).map_err(json_error_to_command_error)?;

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
