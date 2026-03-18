use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, StreamEntry};
use frogdb_protocol::Response;

use super::versioned_entry_to_response;

// ============================================================================
// ES.REPLAY — replay all events, optionally starting from a snapshot
// ============================================================================

pub struct EsReplayCommand;

impl Command for EsReplayCommand {
    fn name(&self) -> &'static str {
        "ES.REPLAY"
    }

    fn arity(&self) -> Arity {
        // ES.REPLAY key [SNAPSHOT snapshot_key]
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn requires_same_slot(&self) -> bool {
        true
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse optional SNAPSHOT
        let mut snapshot_key: Option<&Bytes> = None;
        let mut i = 1;
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            if arg == b"SNAPSHOT".as_slice() {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                snapshot_key = Some(&args[i]);
                i += 1;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        // Read snapshot if provided — extract version and state into owned data
        let (snapshot_version, snapshot_state): (u64, Option<Bytes>) = if let Some(snap_key) =
            snapshot_key
        {
            match ctx.store.get(snap_key) {
                Some(val) => {
                    let sv = val.as_string().ok_or(CommandError::WrongType)?;
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
        let entries: Vec<(u64, StreamEntry)> = match ctx.store.get(key) {
            Some(val) => {
                let stream = val.as_stream().ok_or(CommandError::WrongType)?;
                stream.range_by_version(start_version, None, None)
            }
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        let mut keys = vec![];
        if !args.is_empty() {
            keys.push(args[0].as_ref());
        }
        // Also include snapshot key if present
        let mut i = 1;
        while i < args.len() {
            if args[i].to_ascii_uppercase() == b"SNAPSHOT".as_slice() {
                if i + 1 < args.len() {
                    keys.push(args[i + 1].as_ref());
                }
                break;
            }
            i += 1;
        }
        keys
    }
}
