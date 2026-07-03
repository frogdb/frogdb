//! VLINKS command — return HNSW neighbor links for an element.
//!
//! VLINKS key element [layer]

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, LookupSpec, StoreTypedFamilyExt, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VlinksCommand;

impl Command for VlinksCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "VLINKS",
            arity: Arity::Range { min: 2, max: 3 },
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
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

        let Some(vs) = ctx.store.get_vectorset(key)? else {
            return Ok(Response::null());
        };

        match vs.links(element, layer) {
            Some(links) => {
                let arr: Vec<Response> = links.into_iter().map(Response::bulk).collect();
                Ok(Response::Array(arr))
            }
            None => Ok(Response::null()),
        }
    }
}
