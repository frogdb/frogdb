//! VCARD command — return the number of elements in a vector set.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VcardCommand;

impl Command for VcardCommand {
    fn spec(&self) -> Option<&'static CommandSpec> {
        static SPEC: CommandSpec = CommandSpec {
            name: "VCARD",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        Some(&SPEC)
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let vs = value.as_vectorset().ok_or(CommandError::WrongType)?;
                Ok(Response::Integer(vs.card() as i64))
            }
            None => Ok(Response::Integer(0)),
        }
    }
}
