use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

// ============================================================================
// ES.SNAPSHOT — store a snapshot at a given version
// ============================================================================

pub struct EsSnapshotCommand;

impl Command for EsSnapshotCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ES.SNAPSHOT",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let snapshot_key = &args[0];

        // Parse version
        let version: u64 = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::NotInteger)?
            .parse()
            .map_err(|_| CommandError::NotInteger)?;

        let state = &args[2];

        // Store as "version:state" so ES.REPLAY can parse it back
        let stored = format!("{}:{}", version, std::str::from_utf8(state).unwrap_or(""));
        let val = frogdb_core::Value::string(Bytes::from(stored));
        ctx.store.set(snapshot_key.clone(), val);

        Ok(Response::ok())
    }
}
