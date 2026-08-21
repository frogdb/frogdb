use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use crate::utils::format_float;

// ============================================================================
// ZSCAN - Cursor-based iteration
// ============================================================================

pub struct ZscanCommand;

impl Command for ZscanCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZSCAN",
            docs: frogdb_core::CommandDocs {
                summary: "Iterates over members and scores of a sorted set.",
                since: "2.8.0",
                group: "sorted-set",
                complexity: Some(
                    "O(1) for every call. O(N) for a complete iteration, including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection.",
                ),
            },
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse cursor, [MATCH pattern], [COUNT count]; no ZSCAN-specific flags.
        let request = crate::utils::ScanRequest::parse(&args[1..], |_| Ok(false))?;

        let zset = ctx.store.get_zset(key)?;
        let items = zset.map(|z| z.to_vec().into_iter());

        Ok(crate::utils::scan_reply(
            &request,
            items,
            |entry: &(Bytes, f64)| entry.0.as_ref(),
            |entry: (Bytes, f64), results: &mut Vec<Response>| {
                results.push(Response::bulk(entry.0));
                results.push(Response::bulk(Bytes::from(format_float(entry.1))));
            },
        ))
    }
}
