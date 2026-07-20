use bytes::Bytes;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    EventSpec, ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, WaiterWake,
    WalStrategy,
};
use frogdb_protocol::Response;

use super::versioned_entry_to_response;
use crate::utils::parse_u64;

// ============================================================================
// ES.READ — read events by version range
// ============================================================================

pub struct EsReadCommand;

impl Command for EsReadCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ES.READ",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
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
        let start_version = parse_u64(&args[1])?;

        let mut end_version: Option<u64> = None;
        let mut count: Option<usize> = None;

        let mut parser = ArgParser::from_position(args, 2);
        while parser.has_more() {
            if parser.try_flag(b"COUNT") {
                count = Some(crate::utils::parse_usize(parser.next_arg()?)?);
            } else if end_version.is_none() {
                // First non-COUNT token is the optional end_version.
                end_version = Some(parse_u64(parser.next_arg()?)?);
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        // Look up the stream and collect results within the borrow scope
        let Some(stream) = ctx.store.get_stream(key)? else {
            return Ok(Response::Array(vec![]));
        };
        let entries = stream.range_by_version(start_version, end_version, count);

        let results: Vec<Response> = entries
            .iter()
            .map(|(ver, entry)| versioned_entry_to_response(*ver, entry))
            .collect();

        Ok(Response::Array(results))
    }
}
