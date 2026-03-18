//! VDIM command — return the dimensionality of a vector set.

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

pub struct VdimCommand;

impl Command for VdimCommand {
    fn name(&self) -> &'static str {
        "VDIM"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let vs = value.as_vectorset().ok_or(CommandError::WrongType)?;
                // Return original_dim if REDUCE was used, otherwise dim.
                let d = if vs.original_dim() > 0 {
                    vs.original_dim()
                } else {
                    vs.dim()
                };
                Ok(Response::Integer(d as i64))
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
