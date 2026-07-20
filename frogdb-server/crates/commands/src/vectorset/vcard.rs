//! VCARD command — return the number of elements in a vector set.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VcardCommand;

impl Command for VcardCommand {
    fn spec(&self) -> &'static CommandSpec {
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
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_vectorset(key)? {
            Some(vs) => Ok(Response::Integer(vs.card() as i64)),
            None => Ok(Response::Integer(0)),
        }
    }
}
