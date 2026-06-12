//! VRANDMEMBER command — return random elements from a vector set.
//!
//! VRANDMEMBER key [count]

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VrandmemberCommand;

impl Command for VrandmemberCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "VRANDMEMBER",
            arity: Arity::Range { min: 1, max: 2 },
            flags: CommandFlags::READONLY,
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

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::null()),
        };
        let vs = value.as_vectorset().ok_or(CommandError::WrongType)?;

        if args.len() == 1 {
            // No count: return single element or nil.
            let members = vs.rand_member(1);
            if members.is_empty() {
                Ok(Response::null())
            } else {
                Ok(Response::bulk(members[0].clone()))
            }
        } else {
            let count: i64 = std::str::from_utf8(&args[1])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid count".to_string(),
                })?
                .parse()
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid count".to_string(),
                })?;

            let members = vs.rand_member(count);
            let arr: Vec<Response> = members.into_iter().map(Response::bulk).collect();
            Ok(Response::Array(arr))
        }
    }
}
