use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

use super::{json_error_to_command_error, parse_json_value, parse_path};
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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();

        // Parse all values
        let mut values = Vec::new();
        for arg in args.iter().skip(2) {
            values.push(parse_json_value(arg)?);
        }

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
            .arr_append(&path, values)
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

        let json = match ctx.store.get(key) {
            Some(value) => match value.as_json() {
                Some(j) => j.clone(),
                None => return Err(CommandError::WrongType),
            },
            None => return Ok(Response::null()),
        };

        let results = json
            .arr_index(&path, &value, start, stop)
            .map_err(json_error_to_command_error)?;

        if results.len() == 1 {
            Ok(Response::Integer(results[0]))
        } else {
            let responses: Vec<Response> =
                results.iter().map(|&idx| Response::Integer(idx)).collect();
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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();
        let index = parse_i64(&args[2])?;

        // Parse all values
        let mut values = Vec::new();
        for arg in args.iter().skip(3) {
            values.push(parse_json_value(arg)?);
        }

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

        // Get mutable reference and insert
        let json = ctx.store.get_mut(key).unwrap().as_json_mut().unwrap();
        let results = json
            .arr_insert(&path, index, values)
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

        let json = match ctx.store.get(key) {
            Some(value) => match value.as_json() {
                Some(j) => j.clone(),
                None => return Err(CommandError::WrongType),
            },
            None => return Ok(Response::null()),
        };

        let results = json.arr_len(&path).map_err(json_error_to_command_error)?;

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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = parse_path(args.get(1));
        let index = if args.len() > 2 {
            Some(parse_i64(&args[2])?)
        } else {
            None
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

        // Get mutable reference and pop
        let json = ctx.store.get_mut(key).unwrap().as_json_mut().unwrap();
        let results = json
            .arr_pop(&path, index)
            .map_err(json_error_to_command_error)?;

        if results.len() == 1 {
            match &results[0] {
                Some(value) => {
                    let json_str = serde_json::to_string(value).unwrap_or_default();
                    Ok(Response::bulk(Bytes::from(json_str)))
                }
                None => Ok(Response::null()),
            }
        } else {
            let responses: Vec<Response> = results
                .iter()
                .map(|value| match value {
                    Some(v) => {
                        let json_str = serde_json::to_string(v).unwrap_or_default();
                        Response::bulk(Bytes::from(json_str))
                    }
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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();
        let start = parse_i64(&args[2])?;
        let stop = parse_i64(&args[3])?;

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

        // Get mutable reference and trim
        let json = ctx.store.get_mut(key).unwrap().as_json_mut().unwrap();
        let results = json
            .arr_trim(&path, start, stop)
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
