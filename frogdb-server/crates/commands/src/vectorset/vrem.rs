//! VREM command — remove an element from a vector set.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VremCommand;

impl Command for VremCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "VREM",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        &SPEC
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
}
