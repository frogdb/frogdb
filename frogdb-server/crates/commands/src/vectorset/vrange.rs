//! VRANGE command — iterate elements in lexicographic order.
//!
//! VRANGE key cursor COUNT count

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VrangeCommand;

impl Command for VrangeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "VRANGE",
            arity: Arity::Range { min: 3, max: 4 },
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let cursor = &args[1];

        // Parse COUNT.
        let count = if args.len() > 2 {
            // args[2] might be "COUNT" keyword or the count value directly.
            if args.len() == 4 && args[2].eq_ignore_ascii_case(b"COUNT") {
                std::str::from_utf8(&args[3])
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid count".to_string(),
                    })?
                    .parse::<usize>()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid count".to_string(),
                    })?
            } else {
                std::str::from_utf8(&args[2])
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid count".to_string(),
                    })?
                    .parse::<usize>()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid count".to_string(),
                    })?
            }
        } else {
            10
        };

        let Some(vs) = ctx.store.get_vectorset(key)? else {
            return Ok(Response::Array(vec![]));
        };

        let cursor_bytes = if cursor.as_ref() == b"0" || cursor.is_empty() {
            b"" as &[u8]
        } else {
            cursor.as_ref()
        };

        let elements = vs.range(cursor_bytes, count);
        let arr: Vec<Response> = elements.into_iter().map(Response::bulk).collect();
        Ok(Response::Array(arr))
    }
}
