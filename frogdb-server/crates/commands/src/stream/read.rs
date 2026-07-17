use bytes::Bytes;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    EventSpec, ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, StreamEntry, StreamId,
    StreamRangeBound, WaiterWake, WalStrategy,
};
use frogdb_protocol::{BlockingOp, Response};

use super::entry_to_response;

// ============================================================================
// XREAD - Read entries from streams (non-blocking)
// ============================================================================

pub struct XreadCommand;

impl Command for XreadCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XREAD",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::READONLY.union(CommandFlags::MOVABLEKEYS),
            keys: KeySpec::Dynamic,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: // Blocking XREAD requires all keys to be on the same shard
        true,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Blocking { default_timeout: None, },
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let mut count: Option<usize> = None;
        let mut block_ms: Option<u64> = None;

        // Parse options up to the STREAMS terminator.
        let mut parser = ArgParser::new(args);
        while parser.has_more() {
            if let Some(c) = parser.try_flag_usize(b"COUNT")? {
                count = Some(c);
            } else if let Some(b) = parser.try_flag_u64(b"BLOCK")? {
                block_ms = Some(b);
            } else if parser.try_flag(b"STREAMS") {
                break;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }
        let i = parser.position();

        // Parse keys and IDs
        let remaining = args.len() - i;
        if remaining == 0 || !remaining.is_multiple_of(2) {
            return Err(CommandError::SyntaxError);
        }

        let num_streams = remaining / 2;
        let keys = &args[i..i + num_streams];
        let ids = &args[i + num_streams..];

        // Read from each stream, tracking resolved IDs for blocking
        let mut results = Vec::new();
        let mut resolved_ids: Vec<(u64, u64)> = Vec::with_capacity(num_streams);

        for (key, id_arg) in keys.iter().zip(ids.iter()) {
            let after_id = if id_arg.as_ref() == b"$" {
                // $ means current last ID - only new entries
                match ctx.store.get_stream(key.as_ref())? {
                    Some(stream) => stream.last_id(),
                    None => StreamId::default(),
                }
            } else {
                StreamId::parse(id_arg)?
            };

            // Track resolved ID for blocking
            resolved_ids.push((after_id.ms, after_id.seq));

            match ctx.store.get_stream(key.as_ref())? {
                Some(stream) => {
                    let entries = stream.read_after(&after_id, count);

                    if !entries.is_empty() {
                        let entry_responses: Vec<Response> =
                            entries.iter().map(entry_to_response).collect();
                        results.push(Response::Array(vec![
                            Response::bulk(key.clone()),
                            Response::Array(entry_responses),
                        ]));
                    }
                }
                None => {
                    // Stream doesn't exist, skip it
                }
            }
        }

        if results.is_empty() {
            // Check if we should block
            if let Some(block_ms) = block_ms {
                let timeout = if block_ms == 0 {
                    0.0
                } else {
                    block_ms as f64 / 1000.0
                };
                return Ok(Response::BlockingNeeded {
                    keys: keys.to_vec(),
                    timeout,
                    op: BlockingOp::XRead {
                        after_ids: resolved_ids,
                        count,
                    },
                });
            }
            // For non-blocking XREAD, return null if no data
            Ok(Response::null())
        } else {
            Ok(Response::Array(results))
        }
    }

    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Find STREAMS keyword and return keys after it
        let mut parser = ArgParser::new(args);
        while parser.has_more() {
            if parser.try_flag(b"STREAMS") {
                break;
            } else if parser.try_flag_any(&[b"COUNT", b"BLOCK"]).is_some() {
                parser.skip(1); // skip the option value
            } else {
                parser.skip(1);
            }
        }
        let i = parser.position();

        let remaining = args.len() - i;
        if remaining == 0 || !remaining.is_multiple_of(2) {
            return vec![];
        }

        let num_streams = remaining / 2;
        args[i..i + num_streams]
            .iter()
            .map(|k| k.as_ref())
            .collect()
    }
}

// ============================================================================
// XREADGROUP - Read entries as consumer
// ============================================================================

pub struct XreadgroupCommand;

impl Command for XreadgroupCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XREADGROUP",
            arity: Arity::AtLeast(6),
            flags: CommandFlags::WRITE.union(CommandFlags::MOVABLEKEYS),
            keys: KeySpec::Dynamic,
            access: AccessSpec::UniformRW,
            wal: WalStrategy::Dynamic,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Blocking {
                default_timeout: None,
            },
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let mut group_name: Option<Bytes> = None;
        let mut consumer_name: Option<Bytes> = None;
        let mut count: Option<usize> = None;
        let mut block_ms: Option<u64> = None;
        let mut noack = false;

        // Parse options up to the STREAMS terminator.
        let mut parser = ArgParser::new(args);
        while parser.has_more() {
            if parser.try_flag(b"GROUP") {
                if parser.remaining_count() < 2 {
                    return Err(CommandError::SyntaxError);
                }
                group_name = Some(parser.next_arg()?.clone());
                consumer_name = Some(parser.next_arg()?.clone());
            } else if let Some(c) = parser.try_flag_usize(b"COUNT")? {
                count = Some(c);
            } else if let Some(b) = parser.try_flag_u64(b"BLOCK")? {
                block_ms = Some(b);
            } else if parser.try_flag(b"NOACK") {
                noack = true;
            } else if parser.try_flag(b"STREAMS") {
                break;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }
        let i = parser.position();

        let group_name = group_name.ok_or(CommandError::SyntaxError)?;
        let consumer_name = consumer_name.ok_or(CommandError::SyntaxError)?;

        // Parse keys and IDs
        let remaining = args.len() - i;
        if remaining == 0 || !remaining.is_multiple_of(2) {
            return Err(CommandError::SyntaxError);
        }

        let num_streams = remaining / 2;
        let keys = &args[i..i + num_streams];
        let ids = &args[i + num_streams..];

        // For now, only support single stream (no cross-shard)
        if num_streams != 1 {
            return Err(CommandError::InvalidArgument {
                message: "XREADGROUP with multiple streams not yet supported".to_string(),
            });
        }

        let key = &keys[0];
        let id_arg = &ids[0];

        // Get or check stream
        let stream = ctx.store.get_stream_mut(key.as_ref())?.ok_or_else(|| {
            CommandError::InvalidArgument {
                message: format!("No such key '{}'", String::from_utf8_lossy(key)),
            }
        })?;

        // Get the group
        let group = stream
            .get_group_mut(&group_name)
            .ok_or(CommandError::NoGroup)?;

        // Ensure consumer exists and touch seen-time
        group.get_or_create_consumer(consumer_name.clone()).touch();

        let entries: Vec<StreamEntry> = if id_arg.as_ref() == b">" {
            // Read new messages (not yet delivered)
            let last_delivered = group.last_delivered_id();
            let new_entries = stream.read_after(&last_delivered, count);

            if new_entries.is_empty() {
                // No new entries - check if we should block
                if let Some(block_ms) = block_ms {
                    let timeout = if block_ms == 0 {
                        0.0
                    } else {
                        block_ms as f64 / 1000.0
                    };
                    return Ok(Response::BlockingNeeded {
                        keys: keys.to_vec(),
                        timeout,
                        op: BlockingOp::XReadGroup {
                            group: group_name,
                            consumer: consumer_name,
                            noack,
                            count,
                        },
                    });
                }
                return Ok(Response::null());
            }

            // Update group state: last_delivered_id, entries_read, PEL, consumer timestamps
            stream.record_group_delivery(&group_name, &consumer_name, &new_entries, noack);

            new_entries
        } else {
            // Re-read from PEL (for retry) - never blocks
            let start_id = if id_arg.as_ref() == b"0" || id_arg.as_ref() == b"0-0" {
                StreamId::default()
            } else {
                StreamId::parse(id_arg)?
            };

            // Find pending entries for this consumer starting from start_id
            let pending_ids: Vec<StreamId> = group
                .pending_entries(
                    StreamRangeBound::Inclusive(start_id),
                    StreamRangeBound::Max,
                    count.unwrap_or(usize::MAX),
                    None,
                    Some(&consumer_name),
                )
                .into_iter()
                .map(|pe| pe.id)
                .collect();

            // Get the actual entries — deleted entries return [id, []] (empty fields)
            let mut pel_responses = Vec::new();
            for id in pending_ids {
                if let Some(entry) = stream.get(&id) {
                    pel_responses.push(entry_to_response(&entry));
                } else {
                    // Entry was deleted from the stream but still in PEL
                    pel_responses.push(Response::Array(vec![
                        Response::bulk(Bytes::from(id.to_string())),
                        Response::Array(vec![]),
                    ]));
                }
            }

            // Return PEL results directly (already formatted as responses)
            // Even when empty, Redis returns [["key", []]] (not null)
            return Ok(Response::Array(vec![Response::Array(vec![
                Response::bulk(key.clone()),
                Response::Array(pel_responses),
            ])]));
        };

        let entry_responses: Vec<Response> = entries.iter().map(entry_to_response).collect();
        Ok(Response::Array(vec![Response::Array(vec![
            Response::bulk(key.clone()),
            Response::Array(entry_responses),
        ])]))
    }

    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Find STREAMS keyword and return keys after it
        let mut parser = ArgParser::new(args);
        while parser.has_more() {
            if parser.try_flag(b"STREAMS") {
                break;
            } else if parser.try_flag(b"GROUP") {
                parser.skip(2); // skip group + consumer
            } else if parser.try_flag_any(&[b"COUNT", b"BLOCK"]).is_some() {
                parser.skip(1); // skip the option value
            } else {
                parser.skip(1); // NOACK or unknown token
            }
        }
        let i = parser.position();

        let remaining = args.len() - i;
        if remaining == 0 || !remaining.is_multiple_of(2) {
            return vec![];
        }

        let num_streams = remaining / 2;
        args[i..i + num_streams]
            .iter()
            .map(|k| k.as_ref())
            .collect()
    }
}
