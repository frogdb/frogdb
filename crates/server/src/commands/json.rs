//! JSON commands.
//!
//! Commands for JSON document manipulation (RedisJSON compatible):
//! - JSON.SET, JSON.GET, JSON.DEL, JSON.MGET - basic operations
//! - JSON.TYPE - type inspection
//! - JSON.NUMINCRBY, JSON.NUMMULTBY - numeric operations
//! - JSON.STRAPPEND, JSON.STRLEN - string operations
//! - JSON.ARRAPPEND, JSON.ARRINDEX, JSON.ARRINSERT, JSON.ARRLEN, JSON.ARRPOP, JSON.ARRTRIM - array operations
//! - JSON.OBJKEYS, JSON.OBJLEN - object operations
//! - JSON.CLEAR, JSON.TOGGLE, JSON.MERGE - utility operations

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, JsonError, JsonLimits, JsonValue,
    Value,
};
use frogdb_protocol::Response;
use serde_json::Value as JsonData;

use super::utils::{format_float, parse_f64, parse_i64};

/// Parse a JSON path argument, defaulting to root if not provided.
fn parse_path(arg: Option<&Bytes>) -> String {
    arg.map(|b| String::from_utf8_lossy(b).to_string())
        .unwrap_or_else(|| "$".to_string())
}

/// Parse a JSON value from bytes.
fn parse_json_value(bytes: &[u8]) -> Result<JsonData, CommandError> {
    serde_json::from_slice(bytes).map_err(|e| CommandError::InvalidArgument {
        message: format!("invalid JSON: {}", e),
    })
}

/// Convert a JsonError to a CommandError.
fn json_error_to_command_error(err: JsonError) -> CommandError {
    CommandError::InvalidArgument {
        message: err.to_string(),
    }
}

/// Default JSON limits (will be replaced with config-based limits).
fn default_limits() -> JsonLimits {
    JsonLimits::default()
}

// ============================================================================
// JSON.SET - Set a JSON value at a path
// ============================================================================

pub struct JsonSetCommand;

impl Command for JsonSetCommand {
    fn name(&self) -> &'static str {
        "JSON.SET"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // key path value [NX|XX]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();
        let value = parse_json_value(&args[2])?;

        // Parse NX/XX options
        let mut nx = false;
        let mut xx = false;

        for arg in args.iter().skip(3) {
            let arg_upper = String::from_utf8_lossy(arg).to_uppercase();
            match arg_upper.as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                _ => {
                    return Err(CommandError::InvalidArgument {
                        message: format!("unknown option: {}", arg_upper),
                    })
                }
            }
        }

        if nx && xx {
            return Err(CommandError::InvalidArgument {
                message: "NX and XX options are mutually exclusive".to_string(),
            });
        }

        let limits = default_limits();

        // Check if key exists
        let key_exists = ctx.store.get(key).is_some();

        if !key_exists {
            // Creating new document
            if xx {
                // XX requires key to exist
                return Ok(Response::null());
            }

            // For new documents, path must be root
            if path != "$" && path != "." {
                return Err(CommandError::InvalidArgument {
                    message: "new document must be created at root path".to_string(),
                });
            }

            // Parse and validate the value
            let json_bytes =
                serde_json::to_vec(&value).map_err(|e| CommandError::InvalidArgument {
                    message: format!("invalid JSON: {}", e),
                })?;
            let json = JsonValue::parse_with_limits(&json_bytes, &limits)
                .map_err(json_error_to_command_error)?;
            ctx.store.set(key.clone(), Value::Json(json));
            return Ok(Response::ok());
        }

        // Key exists - check type
        {
            let value = ctx.store.get(key).ok_or(CommandError::WrongType)?;
            if value.as_json().is_none() {
                return Err(CommandError::WrongType);
            }
        }

        // Get mutable reference and perform set
        let json = ctx
            .store
            .get_mut(key)
            .and_then(|v| v.as_json_mut())
            .ok_or(CommandError::WrongType)?;

        let result = json
            .set(&path, value, nx, xx)
            .map_err(json_error_to_command_error)?;

        if result {
            Ok(Response::ok())
        } else {
            Ok(Response::null())
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
// JSON.GET - Get JSON value(s) at path(s)
// ============================================================================

pub struct JsonGetCommand;

impl Command for JsonGetCommand {
    fn name(&self) -> &'static str {
        "JSON.GET"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // key [INDENT indent] [NEWLINE newline] [SPACE space] [path ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse options and paths first
        let mut indent: Option<String> = None;
        let mut newline: Option<String> = None;
        let mut space: Option<String> = None;
        let mut paths: Vec<String> = Vec::new();

        let mut i = 1;
        while i < args.len() {
            let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
            match arg.as_str() {
                "INDENT" => {
                    if i + 1 >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "INDENT requires an argument".to_string(),
                        });
                    }
                    indent = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                    i += 2;
                }
                "NEWLINE" => {
                    if i + 1 >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "NEWLINE requires an argument".to_string(),
                        });
                    }
                    newline = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                    i += 2;
                }
                "SPACE" => {
                    if i + 1 >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "SPACE requires an argument".to_string(),
                        });
                    }
                    space = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                    i += 2;
                }
                _ => {
                    // Treat as path
                    paths.push(String::from_utf8_lossy(&args[i]).to_string());
                    i += 1;
                }
            }
        }

        // Default path is root
        if paths.is_empty() {
            paths.push("$".to_string());
        }

        // Check if key exists and get the JSON value (cloned for use after borrow ends)
        let json = match ctx.store.get(key) {
            Some(value) => match value.as_json() {
                Some(j) => j.clone(),
                None => return Err(CommandError::WrongType),
            },
            None => return Ok(Response::null()),
        };

        // Get values for each path
        if paths.len() == 1 {
            // Single path: return array of values
            let values = json.get(&paths[0]).map_err(json_error_to_command_error)?;
            if values.is_empty() {
                return Ok(Response::null());
            }

            let result: Vec<JsonData> = values.into_iter().cloned().collect();
            let result_json = JsonData::Array(result);

            let output = if indent.is_some() || newline.is_some() || space.is_some() {
                let temp_json = JsonValue::new(result_json);
                temp_json.to_formatted_string(
                    indent.as_deref(),
                    newline.as_deref(),
                    space.as_deref(),
                )
            } else {
                serde_json::to_string(&result_json).unwrap_or_default()
            };

            Ok(Response::bulk(Bytes::from(output)))
        } else {
            // Multiple paths: return object with path -> values
            let mut result_obj = serde_json::Map::new();
            for path in &paths {
                let values = json.get(path).map_err(json_error_to_command_error)?;
                let arr: Vec<JsonData> = values.into_iter().cloned().collect();
                result_obj.insert(path.clone(), JsonData::Array(arr));
            }

            let result_json = JsonData::Object(result_obj);
            let output = if indent.is_some() || newline.is_some() || space.is_some() {
                let temp_json = JsonValue::new(result_json);
                temp_json.to_formatted_string(
                    indent.as_deref(),
                    newline.as_deref(),
                    space.as_deref(),
                )
            } else {
                serde_json::to_string(&result_json).unwrap_or_default()
            };

            Ok(Response::bulk(Bytes::from(output)))
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
// JSON.DEL - Delete values at a path
// ============================================================================

pub struct JsonDelCommand;

impl Command for JsonDelCommand {
    fn name(&self) -> &'static str {
        "JSON.DEL"
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

        // Check if deleting root
        if path == "$" || path == "." {
            // Delete the entire key
            let deleted = if ctx.store.delete(key) { 1 } else { 0 };
            return Ok(Response::Integer(deleted));
        }

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

        // Get mutable reference and delete
        let json = ctx.store.get_mut(key).unwrap().as_json_mut().unwrap();
        let deleted = json.delete(&path).map_err(json_error_to_command_error)?;
        Ok(Response::Integer(deleted as i64))
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
// JSON.MGET - Get a path from multiple keys
// ============================================================================

pub struct JsonMgetCommand;

impl Command for JsonMgetCommand {
    fn name(&self) -> &'static str {
        "JSON.MGET"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // JSON.MGET key [key ...] path
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Last argument is the path
        let path = String::from_utf8_lossy(&args[args.len() - 1]).to_string();
        let keys = &args[..args.len() - 1];

        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            let result = match ctx.store.get(key) {
                Some(value) => match value.as_json() {
                    Some(json) => match json.get(&path) {
                        Ok(values) if !values.is_empty() => {
                            let arr: Vec<JsonData> = values.into_iter().cloned().collect();
                            let json_str =
                                serde_json::to_string(&JsonData::Array(arr)).unwrap_or_default();
                            Response::bulk(Bytes::from(json_str))
                        }
                        _ => Response::null(),
                    },
                    None => Response::null(), // Wrong type, return null
                },
                None => Response::null(),
            };
            results.push(result);
        }

        Ok(Response::Array(results))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            vec![]
        } else {
            args[..args.len() - 1].iter().map(|b| b.as_ref()).collect()
        }
    }
}

// ============================================================================
// JSON.TYPE - Get the type of value at a path
// ============================================================================

pub struct JsonTypeCommand;

impl Command for JsonTypeCommand {
    fn name(&self) -> &'static str {
        "JSON.TYPE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // key [path]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
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

        let types = json.type_at(&path).map_err(json_error_to_command_error)?;
        if types.is_empty() {
            return Ok(Response::null());
        }

        if types.len() == 1 {
            Ok(Response::bulk(Bytes::from(types[0].as_str())))
        } else {
            let results: Vec<Response> = types
                .iter()
                .map(|t| Response::bulk(Bytes::from(t.as_str())))
                .collect();
            Ok(Response::Array(results))
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
// JSON.NUMINCRBY - Increment a number at a path
// ============================================================================

pub struct JsonNumIncrByCommand;

impl Command for JsonNumIncrByCommand {
    fn name(&self) -> &'static str {
        "JSON.NUMINCRBY"
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
        let incr = parse_f64(&args[2])?;

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

        // Get mutable reference and increment
        let json = ctx.store.get_mut(key).unwrap().as_json_mut().unwrap();
        let results = json
            .num_incr_by(&path, incr)
            .map_err(json_error_to_command_error)?;

        if results.len() == 1 {
            Ok(Response::bulk(Bytes::from(format_float(results[0]))))
        } else {
            let arr: Vec<JsonData> = results
                .iter()
                .map(|&r| {
                    if r.fract() == 0.0 && r >= i64::MIN as f64 && r <= i64::MAX as f64 {
                        JsonData::Number(serde_json::Number::from(r as i64))
                    } else {
                        serde_json::Number::from_f64(r)
                            .map(JsonData::Number)
                            .unwrap_or(JsonData::Null)
                    }
                })
                .collect();
            let json_str = serde_json::to_string(&JsonData::Array(arr)).unwrap_or_default();
            Ok(Response::bulk(Bytes::from(json_str)))
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
// JSON.NUMMULTBY - Multiply a number at a path
// ============================================================================

pub struct JsonNumMultByCommand;

impl Command for JsonNumMultByCommand {
    fn name(&self) -> &'static str {
        "JSON.NUMMULTBY"
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
        let mult = parse_f64(&args[2])?;

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

        // Get mutable reference and multiply
        let json = ctx.store.get_mut(key).unwrap().as_json_mut().unwrap();
        let results = json
            .num_mult_by(&path, mult)
            .map_err(json_error_to_command_error)?;

        if results.len() == 1 {
            Ok(Response::bulk(Bytes::from(format_float(results[0]))))
        } else {
            let arr: Vec<JsonData> = results
                .iter()
                .map(|&r| {
                    if r.fract() == 0.0 && r >= i64::MIN as f64 && r <= i64::MAX as f64 {
                        JsonData::Number(serde_json::Number::from(r as i64))
                    } else {
                        serde_json::Number::from_f64(r)
                            .map(JsonData::Number)
                            .unwrap_or(JsonData::Null)
                    }
                })
                .collect();
            let json_str = serde_json::to_string(&JsonData::Array(arr)).unwrap_or_default();
            Ok(Response::bulk(Bytes::from(json_str)))
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
                })
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

// ============================================================================
// JSON.OBJKEYS - Get the keys of an object at a path
// ============================================================================

pub struct JsonObjKeysCommand;

impl Command for JsonObjKeysCommand {
    fn name(&self) -> &'static str {
        "JSON.OBJKEYS"
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

        let results = json.obj_keys(&path).map_err(json_error_to_command_error)?;

        if results.is_empty() {
            return Ok(Response::null());
        }

        if results.len() == 1 {
            match &results[0] {
                Some(keys) => {
                    let responses: Vec<Response> = keys
                        .iter()
                        .map(|k| Response::bulk(Bytes::from(k.clone())))
                        .collect();
                    Ok(Response::Array(responses))
                }
                None => Ok(Response::null()),
            }
        } else {
            let responses: Vec<Response> = results
                .iter()
                .map(|keys| match keys {
                    Some(ks) => {
                        let inner: Vec<Response> = ks
                            .iter()
                            .map(|k| Response::bulk(Bytes::from(k.clone())))
                            .collect();
                        Response::Array(inner)
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
// JSON.OBJLEN - Get the number of keys in an object at a path
// ============================================================================

pub struct JsonObjLenCommand;

impl Command for JsonObjLenCommand {
    fn name(&self) -> &'static str {
        "JSON.OBJLEN"
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

        let results = json.obj_len(&path).map_err(json_error_to_command_error)?;

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
