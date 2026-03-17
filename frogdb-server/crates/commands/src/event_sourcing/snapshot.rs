use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, WalStrategy};
use frogdb_protocol::Response;

// ============================================================================
// ES.SNAPSHOT — store a snapshot at a given version
// ============================================================================

pub struct EsSnapshotCommand;

impl Command for EsSnapshotCommand {
    fn name(&self) -> &'static str {
        "ES.SNAPSHOT"
    }

    fn arity(&self) -> Arity {
        // ES.SNAPSHOT snapshot_key version state
        Arity::Fixed(3)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}
