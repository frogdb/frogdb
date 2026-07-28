use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, ServerWideOp, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

// ============================================================================
// ES.ALL — read the global $all stream (server-wide scatter-gather)
// ============================================================================

pub struct EsAllCommand;

impl Command for EsAllCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ES.ALL",
            arity: Arity::AtLeast(0),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::EsAll),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Execution is handled by the server-wide dispatch path.
        // This method should not be called directly.
        Err(CommandError::Internal {
            message: "ES.ALL should be dispatched server-wide".to_string(),
        })
    }
}
