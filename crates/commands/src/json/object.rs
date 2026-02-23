use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

use super::{json_error_to_command_error, parse_path};

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
