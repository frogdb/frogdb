//! VGETATTR command — get JSON attributes of an element.

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

pub struct VgetattrCommand;

impl Command for VgetattrCommand {
    fn name(&self) -> &'static str {
        "VGETATTR"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let element = &args[1];

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::null()),
        };
        let vs = value.as_vectorset().ok_or(CommandError::WrongType)?;

        if !vs.contains(element) {
            return Ok(Response::null());
        }

        match vs.get_attr(element) {
            Some(attr) => Ok(Response::bulk(Bytes::from(attr.to_string()))),
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
