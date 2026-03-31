use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, StreamEntry, StreamId, WalStrategy,
};
use frogdb_protocol::Response;

use super::super::utils::{parse_u64, parse_usize};
use super::entry_to_response;

// ============================================================================
// XPENDING - Show pending entries
// ============================================================================

pub struct XpendingCommand;

impl Command for XpendingCommand {
    fn name(&self) -> &'static str {
        "XPENDING"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let group_name = &args[1];

        match ctx.store.get(key) {
            Some(value) => {
                let stream = value.as_stream().ok_or(CommandError::WrongType)?;
                let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

                if args.len() == 2 {
                    // Summary mode
                    let pending_count = group.pending_count();
                    let range = group.pending_range();

                    // Count pending per consumer
                    let mut consumer_counts: std::collections::HashMap<&Bytes, i64> =
                        std::collections::HashMap::new();
                    for pe in group.pending.values() {
                        *consumer_counts.entry(&pe.consumer).or_insert(0) += 1;
                    }

                    let consumers: Vec<Response> = consumer_counts
                        .iter()
                        .map(|(name, count)| {
                            Response::Array(vec![
                                Response::bulk((*name).clone()),
                                Response::bulk(Bytes::from(count.to_string())),
                            ])
                        })
                        .collect();

                    match range {
                        Some((first, last)) => Ok(Response::Array(vec![
                            Response::Integer(pending_count as i64),
                            Response::bulk(Bytes::from(first.to_string())),
                            Response::bulk(Bytes::from(last.to_string())),
                            Response::Array(consumers),
                        ])),
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

                    // Collect matching pending entries
                    let results: Vec<Response> = group
                        .pending
                        .iter()
                        .filter(|(id, pe)| {
                            start.satisfies_min(id)
                                && end.satisfies_max(id)
                                && min_idle_ms.is_none_or(|min| pe.idle_ms() >= min)
                                && consumer_filter.is_none_or(|c| pe.consumer == *c)
                        })
                        .take(count)
                        .map(|(id, pe)| {
                            Response::Array(vec![
                                Response::bulk(Bytes::from(id.to_string())),
                                Response::bulk(pe.consumer.clone()),
                                Response::Integer(pe.idle_ms() as i64),
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XCLAIM - Claim pending entries
// ============================================================================

pub struct XclaimCommand;

impl Command for XclaimCommand {
    fn name(&self) -> &'static str {
        "XCLAIM"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(5) // XCLAIM key group consumer min-idle-time id [id ...] [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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
            let value = ctx
                .store
                .get(key)
                .ok_or_else(|| CommandError::InvalidArgument {
                    message: format!("No such key '{}'", String::from_utf8_lossy(key)),
                })?;
            let stream = value.as_stream().ok_or(CommandError::WrongType)?;
            let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

            ids.iter()
                .filter(|id| {
                    if let Some(pe) = group.pending.get(id) {
                        pe.idle_ms() >= min_idle_time
                    } else {
                        force
                    }
                })
                .copied()
                .collect()
        };

        // Second pass: perform mutations
        {
            let stream = ctx.store.get_mut(key).unwrap().as_stream_mut().unwrap();
            let group = stream
                .get_group_mut(group_name)
                .ok_or(CommandError::NoGroup)?;

            // Ensure consumer exists
            group.get_or_create_consumer(consumer_name.clone());

            for id in &ids_to_claim {
                // Remove from old consumer's count
                if let Some(pe) = group.pending.get(id) {
                    let old_consumer_name = pe.consumer.clone();
                    if let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
                        old_consumer.pending_count = old_consumer.pending_count.saturating_sub(1);
                    }
                }

                // Update or insert pending entry
                let pe = group
                    .pending
                    .entry(*id)
                    .or_insert_with(|| frogdb_core::PendingEntry::new(consumer_name.clone()));
                pe.consumer = consumer_name.clone();
                if let Some(idle_ms) = idle {
                    pe.delivery_time =
                        std::time::Instant::now() - std::time::Duration::from_millis(idle_ms);
                }
                if let Some(_time_ms) = time {
                    pe.delivery_time = std::time::Instant::now();
                }
                if let Some(rc) = retrycount {
                    pe.delivery_count = rc;
                } else if !justid {
                    pe.delivery_count += 1;
                }

                // Update new consumer's count
                if let Some(new_consumer) = group.consumers.get_mut(&consumer_name) {
                    new_consumer.pending_count += 1;
                    new_consumer.touch();
                }
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// XAUTOCLAIM - Auto-claim pending entries
// ============================================================================

pub struct XautoclaimCommand;

impl Command for XautoclaimCommand {
    fn name(&self) -> &'static str {
        "XAUTOCLAIM"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(5) // XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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
            let value = ctx
                .store
                .get(key)
                .ok_or_else(|| CommandError::InvalidArgument {
                    message: format!("No such key '{}'", String::from_utf8_lossy(key)),
                })?;
            let stream = value.as_stream().ok_or(CommandError::WrongType)?;
            let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

            let mut to_claim: Vec<StreamId> = Vec::new();
            let mut scanned_to_end = true;

            for (id, pe) in group.pending.range(start_id..) {
                if pe.idle_ms() >= min_idle_time {
                    to_claim.push(*id);
                    if to_claim.len() >= count {
                        // Check if there are more entries after this one
                        let next_id = StreamId::new(id.ms, id.seq + 1);
                        scanned_to_end = group.pending.range(next_id..).next().is_none();
                        break;
                    }
                }
            }

            let next_cursor = if scanned_to_end {
                StreamId::default()
            } else {
                to_claim
                    .last()
                    .map(|id| StreamId::new(id.ms, id.seq + 1))
                    .unwrap_or_default()
            };

            (to_claim, next_cursor)
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
            let stream = ctx.store.get_mut(key).unwrap().as_stream_mut().unwrap();
            let group = stream
                .get_group_mut(group_name)
                .ok_or(CommandError::NoGroup)?;

            // Ensure consumer exists
            group.get_or_create_consumer(consumer_name.clone());

            for id in &to_claim {
                // Remove from old consumer's count
                if let Some(pe) = group.pending.get(id) {
                    let old_consumer_name = pe.consumer.clone();
                    if let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
                        old_consumer.pending_count = old_consumer.pending_count.saturating_sub(1);
                    }
                }

                // Update pending entry
                if let Some(pe) = group.pending.get_mut(id) {
                    pe.consumer = consumer_name.clone();
                    if !justid {
                        pe.delivery_count += 1;
                    }
                    pe.delivery_time = std::time::Instant::now();
                }

                // Update new consumer's count
                if let Some(new_consumer) = group.consumers.get_mut(&consumer_name) {
                    new_consumer.pending_count += 1;
                    new_consumer.touch();
                }
            }

            // Remove deleted entries from PEL
            for id in &deleted_ids {
                group.pending.remove(id);
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}
