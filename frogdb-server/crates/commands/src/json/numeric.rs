use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;
use serde_json::Value as JsonData;

use super::{get_json_mut, json_error_to_command_error};
use crate::utils::{format_float, parse_f64};

// ============================================================================
// JSON.NUMINCRBY - Increment a number at a path
// ============================================================================

pub struct JsonNumIncrByCommand;

impl Command for JsonNumIncrByCommand {
    fn spec(&self) -> Option<&'static CommandSpec> {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.NUMINCRBY",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        Some(&SPEC)
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();
        let incr = parse_f64(&args[2])?;

        let json = get_json_mut!(ctx, key);
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
}

// ============================================================================
// JSON.NUMMULTBY - Multiply a number at a path
// ============================================================================

pub struct JsonNumMultByCommand;

impl Command for JsonNumMultByCommand {
    fn spec(&self) -> Option<&'static CommandSpec> {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.NUMMULTBY",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        Some(&SPEC)
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();
        let mult = parse_f64(&args[2])?;

        let json = get_json_mut!(ctx, key);
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
}
