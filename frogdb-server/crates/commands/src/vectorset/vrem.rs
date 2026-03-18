//! VREM command — remove an element from a vector set.

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, WalStrategy};
use frogdb_protocol::Response;

pub struct VremCommand;

impl Command for VremCommand {
    fn name(&self) -> &'static str {
        "VREM"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistOrDeleteFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let element = &args[1];

        match ctx.store.get_mut(key) {
            Some(value) => {
                let vs = value.as_vectorset_mut().ok_or(CommandError::WrongType)?;
                let removed = vs.remove(element);

                // If the set is now empty, delete the key.
                if vs.card() == 0 {
                    ctx.store.delete(key);
                }

                Ok(Response::Integer(if removed { 1 } else { 0 }))
            }
            None => Ok(Response::Integer(0)),
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
