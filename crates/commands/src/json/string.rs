use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;
use serde_json::Value as JsonData;

use super::{json_error_to_command_error, parse_json_value, parse_path};

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

        // Get mutable reference and append
        let json = ctx.store.get_mut(key).unwrap().as_json_mut().unwrap();
        let results = json
            .str_append(&path, append_str)
            .map_err(json_error_to_command_error)?;

        if results.len() == 1 {
            Ok(Response::Integer(results[0] as i64))
        } else {
            let responses: Vec<Response> = results
                .iter()
                .map(|&len| Response::Integer(len as i64))
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

        let json = match ctx.store.get(key) {
            Some(value) => match value.as_json() {
                Some(j) => j.clone(),
                None => return Err(CommandError::WrongType),
            },
            None => return Ok(Response::null()),
        };

        let results = json.str_len(&path).map_err(json_error_to_command_error)?;

        if results.is_empty() {
            return Ok(Response::null());
        }

        if results.len() == 1 {
            match results[0] {
                Some(len) => Ok(Response::Integer(len as i64)),
                None => Ok(Response::null()),
            }
        } else {
            let responses: Vec<Response> = results
                .iter()
                .map(|&len| match len {
                    Some(l) => Response::Integer(l as i64),
                    None => Response::null(),
                })
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
