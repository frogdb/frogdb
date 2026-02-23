use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;
use serde_json::Value as JsonData;

use super::json_error_to_command_error;
use crate::utils::{format_float, parse_f64};

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
