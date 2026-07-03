//! Persistence commands: BGSAVE, LASTSAVE, DUMP, RESTORE.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeyMetadata, KeySpec, KeyspaceEventFlags,
    LookupSpec, WaiterWake, WalStrategy, deserialize, serialize,
};
use frogdb_protocol::Response;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use frogdb_core::parse_i64;

/// BGSAVE command - trigger a background save (noop in this implementation).
pub struct BgsaveCommand;

impl Command for BgsaveCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BGSAVE",
            arity: Arity::Range { min: 0, max: 1 },
            flags: CommandFlags::ADMIN,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Persistence)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // In the noop implementation, we just return success immediately
        // The actual snapshot coordination would be done at the server level
        Ok(Response::Simple(Bytes::from("Background saving started")))
    }
}

/// LASTSAVE command - return Unix timestamp of last save.
pub struct LastsaveCommand;

impl Command for LastsaveCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LASTSAVE",
            arity: Arity::Fixed(0),
            flags: CommandFlags::READONLY
                .union(CommandFlags::FAST)
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Persistence)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Return current time as "last save" since we persist on every write
        // In a real implementation, this would query the snapshot coordinator
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Ok(Response::Integer(now.as_secs() as i64))
    }
}

/// DUMP command - serialize a key's value.
pub struct DumpCommand;

impl Command for DumpCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "DUMP",
            arity: Arity::Fixed(1),
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

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                // Get the expiry if any
                let expires_at = ctx.store.get_expiry(key);
                let mut metadata = KeyMetadata::new(value.memory_size());
                metadata.expires_at = expires_at;

                let serialized = serialize(&value, &metadata);
                Ok(Response::bulk(Bytes::from(serialized)))
            }
            None => Ok(Response::null()),
        }
    }
}

/// RESTORE command - deserialize and store a key's value.
pub struct RestoreCommand;

impl Command for RestoreCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "RESTORE",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::GENERIC,
                name: "restore",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = args[0].clone();
        let ttl_ms = parse_i64(&args[1])?;
        let serialized_value = &args[2];

        // Parse options
        let mut replace = false;
        let mut absttl = false;
        let mut i = 3;

        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"REPLACE" => replace = true,
                b"ABSTTL" => absttl = true,
                b"IDLETIME" | b"FREQ" => {
                    // Skip these options and their arguments
                    i += 1;
                }
                _ => {
                    return Err(CommandError::SyntaxError);
                }
            }
            i += 1;
        }

        // Check if key exists and REPLACE not specified
        if !replace && ctx.store.contains(&key) {
            return Err(CommandError::BusyKey);
        }

        // Deserialize the value
        let (value, mut metadata) =
            deserialize(serialized_value).map_err(|e| CommandError::InvalidArgument {
                message: format!("DUMP payload version or checksum are wrong: {}", e),
            })?;

        // Handle TTL
        if ttl_ms > 0 {
            let expires_at = if absttl {
                // TTL is an absolute Unix timestamp in milliseconds
                unix_ms_to_instant(ttl_ms)
            } else {
                // TTL is relative in milliseconds
                Instant::now() + Duration::from_millis(ttl_ms as u64)
            };
            metadata.expires_at = Some(expires_at);
        } else if ttl_ms == 0 {
            // Preserve the expiry from the serialized data (if any)
            // Already set in metadata from deserialization
        } else {
            // Negative TTL - remove expiry
            metadata.expires_at = None;
        }

        // Store the value
        ctx.store.set(key.clone(), value);

        // Set expiry if needed
        if let Some(expires_at) = metadata.expires_at {
            ctx.store.set_expiry(&key, expires_at);
        }

        Ok(Response::ok())
    }
}

/// Convert a Unix timestamp in milliseconds to an Instant.
fn unix_ms_to_instant(unix_ms: i64) -> Instant {
    let now_instant = Instant::now();
    let now_system = SystemTime::now();

    let target = UNIX_EPOCH + Duration::from_millis(unix_ms as u64);

    match target.duration_since(now_system) {
        Ok(duration) => {
            // Target is in the future
            now_instant + duration
        }
        Err(e) => {
            // Target is in the past
            let duration = e.duration();
            now_instant.checked_sub(duration).unwrap_or(now_instant)
        }
    }
}
