//! VDIM command — return the dimensionality of a vector set.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VdimCommand;

impl Command for VdimCommand {
    fn spec(&self) -> Option<&'static CommandSpec> {
        static SPEC: CommandSpec = CommandSpec {
            name: "VDIM",
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
}
