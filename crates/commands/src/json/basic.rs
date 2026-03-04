use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, JsonValue, Value, WalStrategy,
    impl_keys_first,
};
use frogdb_protocol::Response;
use serde_json::Value as JsonData;

use super::{
    default_limits, get_json, get_json_mut, json_error_to_command_error, parse_json_value,
    parse_path, single_or_multi,
};

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

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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
                    });
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

    impl_keys_first!();
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

        let json = get_json!(ctx, key);

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

    impl_keys_first!();
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

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistOrDeleteFirstKey
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

        let json = get_json_mut!(ctx, key, Response::Integer(0));
        let deleted = json.delete(&path).map_err(json_error_to_command_error)?;
        Ok(Response::Integer(deleted as i64))
    }

    impl_keys_first!();
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

        let json = get_json!(ctx, key);

        let types = json.type_at(&path).map_err(json_error_to_command_error)?;
        if types.is_empty() {
            return Ok(Response::null());
        }

        Ok(single_or_multi(types, |t| {
            Response::bulk(Bytes::from(t.as_str()))
        }))
    }

    impl_keys_first!();
}
