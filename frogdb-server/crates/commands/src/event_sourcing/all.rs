use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ExecutionStrategy, ServerWideOp,
};
use frogdb_protocol::Response;

// ============================================================================
// ES.ALL — read the global $all stream (server-wide scatter-gather)
// ============================================================================

pub struct EsAllCommand;

impl Command for EsAllCommand {
    fn name(&self) -> &'static str {
        "ES.ALL"
    }

    fn arity(&self) -> Arity {
        // ES.ALL [COUNT n] [AFTER stream_id]
        Arity::AtLeast(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::EsAll)
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // No specific key — reads across all shards
    }
}
