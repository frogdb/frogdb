use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, ClaimOpts, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    EventSpec, ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, StreamEntry, StreamId,
    WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::super::utils::{parse_u64, parse_usize};
use super::entry_to_response;

// ============================================================================
// XPENDING - Show pending entries
// ============================================================================

pub struct XpendingCommand;

impl Command for XpendingCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XPENDING",
            arity: Arity::AtLeast(2),
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
        let group_name = &args[1];

        match ctx.store.get_stream(key)? {
            Some(stream) => {
                let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

                if args.len() == 2 {
                    // Summary mode — the group owns the PEL summary (count, ID
                    // bounds, per-consumer counts).
                    match group.pending_summary() {
                        Some(summary) => {
                            let consumers: Vec<Response> = summary
                                .per_consumer
                                .iter()
                                .map(|(name, count)| {
                                    Response::Array(vec![
                                        Response::bulk(name.clone()),
                                        Response::bulk(Bytes::from(count.to_string())),
                                    ])
                                })
                                .collect();
                            Ok(Response::Array(vec![
                                Response::Integer(summary.count as i64),
                                Response::bulk(Bytes::from(summary.min_id.to_string())),
                                Response::bulk(Bytes::from(summary.max_id.to_string())),
                                Response::Array(consumers),
                            ]))
                        }
                        None => Ok(Response::Array(vec![
                            Response::Integer(0),
                            Response::null(),
                            Response::null(),
                            Response::null(),
                        ])),
                    }
                } else {
                    // Detailed mode: XPENDING key group [IDLE min-idle] start end count [consumer]
                    let mut i = 2;
                    let mut min_idle_ms: Option<u64> = None;

                    // Check for IDLE option
                    if i < args.len() && args[i].to_ascii_uppercase().as_slice() == b"IDLE" {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::SyntaxError);
                        }
                        min_idle_ms = Some(parse_u64(&args[i])?);
                        i += 1;
                    }

                    if i + 2 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }

                    let start = StreamId::parse_range_bound(&args[i])?;
                    let end = StreamId::parse_range_bound(&args[i + 1])?;
                    let count = parse_usize(&args[i + 2])?;
                    i += 3;

                    let consumer_filter: Option<&Bytes> =
                        if i < args.len() { Some(&args[i]) } else { None };

                    // Collect matching pending entries via the group's PEL query.
                    let results: Vec<Response> = group
                        .pending_entries(start, end, count, min_idle_ms, consumer_filter)
                        .into_iter()
                        .map(|pe| {
                            Response::Array(vec![
                                Response::bulk(Bytes::from(pe.id.to_string())),
                                Response::bulk(pe.consumer),
                                Response::Integer(pe.idle_ms as i64),
                                Response::Integer(pe.delivery_count as i64),
                            ])
                        })
                        .collect();

                    Ok(Response::Array(results))
                }
            }
            None => {
                // Stream doesn't exist
                Err(CommandError::InvalidArgument {
                    message: format!("No such key '{}'", String::from_utf8_lossy(key)),
                })
            }
        }
    }
}

// ============================================================================
// XCLAIM - Claim pending entries
// ============================================================================

pub struct XclaimCommand;

impl Command for XclaimCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XCLAIM",
            arity: Arity::AtLeast(5),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let group_name = &args[1];
        let consumer_name = args[2].clone();
        let min_idle_time = parse_u64(&args[3])?;

        // Parse IDs and options
        let mut ids = Vec::new();
        let mut idle: Option<u64> = None;
        let mut time: Option<u64> = None;
        let mut retrycount: Option<u32> = None;
        let mut force = false;
        let mut justid = false;

        let mut i = 4;
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"IDLE" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    idle = Some(parse_u64(&args[i])?);
                    i += 1;
                }
                b"TIME" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    time = Some(parse_u64(&args[i])?);
                    i += 1;
                }
                b"RETRYCOUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    retrycount = Some(parse_u64(&args[i])? as u32);
                    i += 1;
                }
                b"FORCE" => {
                    force = true;
                    i += 1;
                }
                b"JUSTID" => {
                    justid = true;
                    i += 1;
                }
                b"LASTID" => {
                    // LASTID is deprecated, skip it
                    i += 2;
                }
                _ => {
                    // Try to parse as ID
                    match StreamId::parse(&args[i]) {
                        Ok(id) => {
                            ids.push(id);
                            i += 1;
                        }
                        Err(_) => return Err(CommandError::SyntaxError),
                    }
                }
            }
        }

        if ids.is_empty() {
            return Err(CommandError::WrongArity { command: "xclaim" });
        }

        // First pass: determine which IDs should be claimed (read-only)
        let ids_to_claim: Vec<StreamId> = {
            let stream =
                ctx.store
                    .get_stream(key)?
                    .ok_or_else(|| CommandError::InvalidArgument {
                        message: format!("No such key '{}'", String::from_utf8_lossy(key)),
                    })?;
            let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

            ids.iter()
                .filter(|id| {
                    // Present in the PEL: claim when idle enough. Absent: only
                    // FORCE creates the entry.
                    group
                        .pending_idle(id)
                        .map_or(force, |idle| idle >= min_idle_time)
                })
                .copied()
                .collect()
        };

        // Second pass: perform mutations
        {
            let Some(stream) = ctx.store.get_stream_mut(key)? else {
                return Err(CommandError::InvalidArgument {
                    message: format!("No such key '{}'", String::from_utf8_lossy(key)),
                });
            };
            let group = stream
                .get_group_mut(group_name)
                .ok_or(CommandError::NoGroup)?;

            // Ensure the target consumer exists even when nothing is claimed
            // (Redis creates it regardless).
            group.get_or_create_consumer(consumer_name.clone());

            // The IDLE/TIME/RETRYCOUNT/JUSTID rules now live in ClaimOpts::apply;
            // claim_pending keeps the per-consumer pending counts correct.
            let opts = ClaimOpts {
                idle,
                time,
                retrycount,
                justid,
            };
            for id in &ids_to_claim {
                group.claim_pending(*id, &consumer_name, opts);
            }
        }

        // Third pass: get entries for response (read-only)
        let claimed: Vec<StreamEntry> = {
            let value = ctx.store.get(key).unwrap();
            let stream = value.as_stream().unwrap();

            if justid {
                ids_to_claim
                    .iter()
                    .map(|id| StreamEntry::new(*id, vec![]))
                    .collect()
            } else {
                ids_to_claim
                    .iter()
                    .filter_map(|id| stream.get(id))
                    .collect()
            }
        };

        if justid {
            let responses: Vec<Response> = claimed
                .iter()
                .map(|e| Response::bulk(Bytes::from(e.id.to_string())))
                .collect();
            Ok(Response::Array(responses))
        } else {
            let responses: Vec<Response> = claimed.iter().map(entry_to_response).collect();
            Ok(Response::Array(responses))
        }
    }
}

// ============================================================================
// XAUTOCLAIM - Auto-claim pending entries
// ============================================================================

pub struct XautoclaimCommand;

impl Command for XautoclaimCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XAUTOCLAIM",
            arity: Arity::AtLeast(5),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let group_name = &args[1];
        let consumer_name = args[2].clone();
        let min_idle_time = parse_u64(&args[3])?;
        let start_id =
            if args[4].as_ref() == b"0" || args[4].as_ref() == b"0-0" || args[4].as_ref() == b"-" {
                StreamId::default()
            } else {
                StreamId::parse(&args[4])?
            };

        let mut count: usize = 100; // Default count
        let mut justid = false;

        let mut i = 5;
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    count = parse_usize(&args[i])?;
                    i += 1;
                }
                b"JUSTID" => {
                    justid = true;
                    i += 1;
                }
                _ => return Err(CommandError::SyntaxError),
            }
        }

        // First pass: find pending entries to claim (read-only)
        let (to_claim, next_cursor) = {
            let stream =
                ctx.store
                    .get_stream(key)?
                    .ok_or_else(|| CommandError::InvalidArgument {
                        message: format!("No such key '{}'", String::from_utf8_lossy(key)),
                    })?;
            let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

            // The group owns the PEL scan and cursor arithmetic.
            group.autoclaim_scan(start_id, min_idle_time, count)
        };

        // Second pass: check which entries exist (read-only)
        let existing_ids: std::collections::HashSet<StreamId> = {
            let value = ctx.store.get(key).unwrap();
            let stream = value.as_stream().unwrap();
            to_claim
                .iter()
                .filter(|id| stream.contains(id))
                .copied()
                .collect()
        };

        let deleted_ids: Vec<StreamId> = to_claim
            .iter()
            .filter(|id| !existing_ids.contains(id))
            .copied()
            .collect();

        // Third pass: perform mutations
        {
            let Some(stream) = ctx.store.get_stream_mut(key)? else {
                return Err(CommandError::InvalidArgument {
                    message: format!("No such key '{}'", String::from_utf8_lossy(key)),
                });
            };
            let group = stream
                .get_group_mut(group_name)
                .ok_or(CommandError::NoGroup)?;

            // Ensure the target consumer exists even when nothing is claimed.
            group.get_or_create_consumer(consumer_name.clone());

            // Live entries are reassigned to the new consumer; entries whose
            // underlying stream message was deleted between scan and claim are
            // evicted (NOT claimed). Routing both through count-correct methods
            // keeps sum(pending_count) == pending.len(): the old bare remove of
            // deleted ids dropped the entry without decrementing its owner, so
            // the new consumer over-counted by the number of deleted entries.
            let opts = ClaimOpts::touch_now(justid);
            for id in &to_claim {
                if existing_ids.contains(id) {
                    group.claim_pending(*id, &consumer_name, opts);
                } else {
                    group.drop_missing_pending(id);
                }
            }
        }

        // Fourth pass: get entries for response (read-only)
        let claimed: Vec<StreamEntry> = {
            let value = ctx.store.get(key).unwrap();
            let stream = value.as_stream().unwrap();

            to_claim
                .iter()
                .filter(|id| !deleted_ids.contains(id))
                .map(|id| {
                    if justid {
                        StreamEntry::new(*id, vec![])
                    } else {
                        stream
                            .get(id)
                            .unwrap_or_else(|| StreamEntry::new(*id, vec![]))
                    }
                })
                .collect()
        };

        let claimed_responses = if justid {
            claimed
                .iter()
                .map(|e| Response::bulk(Bytes::from(e.id.to_string())))
                .collect()
        } else {
            claimed.iter().map(entry_to_response).collect()
        };

        let deleted_responses: Vec<Response> = deleted_ids
            .iter()
            .map(|id| Response::bulk(Bytes::from(id.to_string())))
            .collect();

        Ok(Response::Array(vec![
            Response::bulk(Bytes::from(next_cursor.to_string())),
            Response::Array(claimed_responses),
            Response::Array(deleted_responses),
        ]))
    }
}
