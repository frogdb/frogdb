use bytes::Bytes;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    EventSpec, ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, StreamEntry,
    WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::versioned_entry_to_response;

// ============================================================================
// ES.REPLAY — replay all events, optionally starting from a snapshot
// ============================================================================

pub struct EsReplayCommand;

impl Command for EsReplayCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ES.REPLAY",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY.union(CommandFlags::MOVABLEKEYS),
            keys: KeySpec::Dynamic,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: true,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse optional SNAPSHOT
        let mut snapshot_key: Option<&Bytes> = None;
        let mut parser = ArgParser::from_position(args, 1);
        while parser.has_more() {
            if parser.try_flag(b"SNAPSHOT") {
                snapshot_key = Some(parser.next_arg()?);
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        // Read snapshot if provided — extract version and state into owned data
        let (snapshot_version, snapshot_state): (u64, Option<Bytes>) = if let Some(snap_key) =
            snapshot_key
        {
            match ctx.store.get_string(snap_key)? {
                Some(sv) => {
                    let raw = sv.as_bytes();
                    let raw_str =
                        std::str::from_utf8(&raw).map_err(|_| CommandError::InvalidArgument {
                            message: "invalid snapshot format".to_string(),
                        })?;
                    if let Some(colon_pos) = raw_str.find(':') {
                        let ver: u64 = raw_str[..colon_pos].parse().map_err(|_| {
                            CommandError::InvalidArgument {
                                message: "invalid snapshot version".to_string(),
                            }
                        })?;
                        let state = Bytes::copy_from_slice(&raw_str.as_bytes()[colon_pos + 1..]);
                        (ver, Some(state))
                    } else {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid snapshot format".to_string(),
                        });
                    }
                }
                None => (0, None),
            }
        } else {
            (0, None)
        };

        // Read events from the event stream — collect into owned data
        let start_version = snapshot_version + 1;
        let entries: Vec<(u64, StreamEntry)> = match ctx.store.get_stream(key)? {
            Some(stream) => stream.range_by_version(start_version, None, None),
            None => vec![],
        };

        let events: Vec<Response> = entries
            .iter()
            .map(|(ver, entry)| versioned_entry_to_response(*ver, entry))
            .collect();

        // Return [snapshot_state_or_null, [events...]]
        let snap_resp = match snapshot_state {
            Some(state) => Response::bulk(state),
            None => Response::null(),
        };

        Ok(Response::Array(vec![snap_resp, Response::Array(events)]))
    }

    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        let mut keys = vec![];
        if !args.is_empty() {
            keys.push(args[0].as_ref());
        }
        // Also include snapshot key if present
        let mut parser = ArgParser::from_position(args, 1);
        while parser.has_more() {
            if parser.try_flag(b"SNAPSHOT") {
                if let Some(k) = parser.next() {
                    keys.push(k.as_ref());
                }
                break;
            } else {
                parser.skip(1);
            }
        }
        keys
    }
}
