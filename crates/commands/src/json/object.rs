use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, impl_keys_first};
use frogdb_protocol::Response;

use super::{get_json, json_error_to_command_error, parse_path, single_or_multi};

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

        let json = get_json!(ctx, key);

        let results = json.obj_keys(&path).map_err(json_error_to_command_error)?;

        if results.is_empty() {
            return Ok(Response::null());
        }

        Ok(single_or_multi(results, |keys| match keys {
            Some(ks) => {
                let responses: Vec<Response> = ks
                    .iter()
                    .map(|k| Response::bulk(Bytes::from(k.clone())))
                    .collect();
                Response::Array(responses)
            }
            None => Response::null(),
        }))
    }

    impl_keys_first!();
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

        let json = get_json!(ctx, key);

        let results = json.obj_len(&path).map_err(json_error_to_command_error)?;

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
