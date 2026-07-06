use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

// ============================================================================
// ES.INFO — event stream metadata
// ============================================================================

pub struct EsInfoCommand;

impl Command for EsInfoCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ES.INFO",
            arity: Arity::Fixed(1),
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

        let Some(stream) = ctx.store.get_stream(key)? else {
            return Ok(Response::null());
        };

        let version = stream.total_appended();
        let length = stream.len();
        let first_id = stream
            .first_id()
            .map(|id| Response::bulk(Bytes::from(id.to_string())))
            .unwrap_or(Response::null());
        let last_id = if stream.last_id().is_zero() && stream.is_empty() {
            Response::null()
        } else {
            Response::bulk(Bytes::from(stream.last_id().to_string()))
        };
        let idem_count = stream.idempotency().map(|s| s.len()).unwrap_or(0);

        Ok(Response::Array(vec![
            Response::bulk(Bytes::from_static(b"version")),
            Response::Integer(version as i64),
            Response::bulk(Bytes::from_static(b"entries")),
            Response::Integer(length as i64),
            Response::bulk(Bytes::from_static(b"first-id")),
            first_id,
            Response::bulk(Bytes::from_static(b"last-id")),
            last_id,
            Response::bulk(Bytes::from_static(b"idempotency-keys")),
            Response::Integer(idem_count as i64),
        ]))
    }
}
