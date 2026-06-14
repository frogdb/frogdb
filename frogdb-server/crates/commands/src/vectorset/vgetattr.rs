//! VGETATTR command — get JSON attributes of an element.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, StoreTypedFamilyExt, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VgetattrCommand;

impl Command for VgetattrCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "VGETATTR",
            arity: Arity::Fixed(2),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let element = &args[1];

        let Some(vs) = ctx.store.get_vectorset(key)? else {
            return Ok(Response::null());
        };

        if !vs.contains(element) {
            return Ok(Response::null());
        }

        match vs.get_attr(element) {
            Some(attr) => Ok(Response::bulk(Bytes::from(attr.to_string()))),
            None => Ok(Response::null()),
        }
    }
}
