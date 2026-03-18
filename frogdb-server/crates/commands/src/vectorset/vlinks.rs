//! VLINKS command — return HNSW neighbor links for an element.
//!
//! VLINKS key element [layer]

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

pub struct VlinksCommand;

impl Command for VlinksCommand {
    fn name(&self) -> &'static str {
        "VLINKS"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 2, max: 3 }
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let element = &args[1];
        let layer = if args.len() > 2 {
            Some(
                std::str::from_utf8(&args[2])
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid layer number".to_string(),
                    })?
                    .parse::<usize>()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid layer number".to_string(),
                    })?,
            )
        } else {
            None
        };

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::null()),
        };
        let vs = value.as_vectorset().ok_or(CommandError::WrongType)?;

        match vs.links(element, layer) {
            Some(links) => {
                let arr: Vec<Response> =
                    links.into_iter().map(Response::bulk).collect();
                Ok(Response::Array(arr))
            }
            None => Ok(Response::null()),
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
