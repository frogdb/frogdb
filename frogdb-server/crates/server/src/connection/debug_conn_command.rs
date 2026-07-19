//! DEBUG connection command.
//!
//! DEBUG is migrated behind the [`ConnectionCommand`] seam (see
//! [`crate::connection::conn_command`] and the CONFIG executor there for the
//! template). It is an admin command with a wide subcommand surface: some
//! subcommands are pure (STRUCTSIZE, HELP, HASHING, RESP3), some sleep the
//! connection task (SLEEP), and several round-trip the shards (VLL,
//! SET-ACTIVE-EXPIRE, KEYSIZES-HIST-ASSERT, ALLOCSIZE-SLOTS-ASSERT) or reach
//! server-only subsystems (TRACING, PUBSUB LIMITS, BUNDLE).
//!
//! The subcommand routing and argument parsing live here in the executor; the
//! per-subcommand *I/O* that needs the handler (the tracer, per-shard messages,
//! this connection's subscription counts, the `frogdb_debug` bundle machinery,
//! the `enable-debug-command` gate) stays behind the [`frogdb_core::DebugProvider`]
//! seam, implemented for `ConnectionHandler` in
//! [`crate::connection::handlers::debug`]. The wire output of every subcommand
//! is byte-for-byte identical to the pre-migration `dispatch_debug`.
//!
//! DEBUG is registered *only* as a `CommandImpl::Connection` executor. `COMMAND
//! GETKEYS` resolves it through the registry union (`get_entry`), so this
//! executor's [`dynamic_keys`] override supplies DEBUG OBJECT's key directly —
//! no shard-local key-extraction stub is required. The spec declares
//! `KeySpec::Dynamic` + `MOVABLEKEYS` so `COMMAND` metadata is correct.
//!
//! [`dynamic_keys`]: ConnectionCommand::dynamic_keys

use std::mem;
use std::time::Duration;

use bytes::Bytes;
use frogdb_core::shard::{extract_hash_tag, shard_for_key, slot_for_key};
use frogdb_core::{
    AccessSpec, Arity, BloomFilterValue, BoxFuture, CommandFlags, CommandSpec, ConnCtx,
    ConnectionCommand, ConnectionLevelOp, DebugProvider, EventSpec, ExecutionStrategy, HashValue,
    HyperLogLogValue, JsonValue, KeyMetadata, KeySpec, KeysizeType, ListValue, LookupSpec,
    SetValue, SortedSetValue, StreamValue, StringValue, TimeSeriesValue, Value, WaiterWake,
    WalStrategy,
};
use frogdb_protocol::Response;

/// The `CommandSpec` for DEBUG — arity `AtLeast(1)`, `ADMIN | NOSCRIPT | LOADING
/// | STALE | MOVABLEKEYS`, `KeySpec::Dynamic`, strategy `ConnectionLevel(Admin)`.
/// This is the sole registered executor for DEBUG, so `COMMAND`/`get_entry`
/// metadata comes straight from here. The registry validates that the strategy
/// agrees with the `Connection` executor variant.
static DEBUG_SPEC: CommandSpec = CommandSpec {
    name: "DEBUG",
    arity: Arity::AtLeast(1),
    flags: CommandFlags::ADMIN
        .union(CommandFlags::NOSCRIPT)
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE)
        .union(CommandFlags::MOVABLEKEYS),
    keys: KeySpec::Dynamic,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` DEBUG executor. Registered via
/// [`frogdb_core::CommandRegistry::register_connection`] in `server/register.rs`.
pub(crate) static DEBUG_CONN_COMMAND: DebugConnCommand = DebugConnCommand;

/// DEBUG — inspection / test-support subcommands. Routes subcommands and parses
/// their arguments here, delegating per-subcommand I/O to
/// [`ConnCtx::debug`] ([`frogdb_core::DebugProvider`]).
pub(crate) struct DebugConnCommand;

impl ConnectionCommand for DebugConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &DEBUG_SPEC
    }

    /// DEBUG OBJECT's key is its second argument; every other subcommand is
    /// keyless.
    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() >= 2 {
            let subcommand = args[0].to_ascii_uppercase();
            if subcommand == b"OBJECT".as_slice() {
                return vec![&args[1]];
            }
        }
        vec![]
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            // Arity `AtLeast(1)` is enforced before dispatch, so `args` is never
            // empty here; guard defensively with the same error the arity check
            // would have produced.
            if args.is_empty() {
                return Response::error("ERR wrong number of arguments for 'debug' command");
            }

            // DEBUG dispatches through the read-only `conn_ctx` builder, which
            // always wires `ConnCtx::debug`.
            let debug = ctx
                .debug
                .expect("DEBUG dispatches through conn_ctx, which sets ConnCtx::debug");
            let num_shards = ctx.num_shards;
            let shard_count = ctx.shard_senders.len();

            let subcommand = args[0].to_ascii_uppercase();
            match subcommand.as_slice() {
                b"SLEEP" => {
                    if !debug.debug_command_enabled() {
                        Response::error(
                            "ERR DEBUG SLEEP is disabled. Set server.enable-debug-command in the config to allow it.",
                        )
                    } else {
                        debug_sleep(args).await
                    }
                }
                b"TRACING" => {
                    if args.len() > 1 && args[1].eq_ignore_ascii_case(b"STATUS") {
                        debug.tracing_status()
                    } else if args.len() > 1 && args[1].eq_ignore_ascii_case(b"RECENT") {
                        // args[0] = "TRACING", args[1] = "RECENT", args[2] = optional count
                        let count = args
                            .get(2)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(10);
                        debug.tracing_recent(count)
                    } else {
                        Response::error(
                            "ERR Unknown DEBUG TRACING subcommand. Use STATUS or RECENT [count].",
                        )
                    }
                }
                b"STRUCTSIZE" => debug_structsize(),
                b"HELP" => debug_help(),
                b"VLL" => {
                    let shard_filter = match parse_vll_shard_filter(args, shard_count) {
                        Ok(filter) => filter,
                        Err(err) => return Response::error(err),
                    };
                    let infos = debug.gather_vll(shard_filter).await;
                    format_vll_response(infos)
                }
                b"LOCKTABLE" => format_locktable_response(debug.gather_lock_table().await),
                b"PUBSUB" => {
                    if args.len() > 1 && args[1].eq_ignore_ascii_case(b"LIMITS") {
                        debug.pubsub_limits().await
                    } else {
                        Response::error("ERR Unknown DEBUG PUBSUB subcommand. Use LIMITS.")
                    }
                }
                b"BUNDLE" => {
                    if args.len() > 1 && args[1].eq_ignore_ascii_case(b"GENERATE") {
                        match parse_bundle_duration(args) {
                            Ok(duration_secs) => debug.bundle_generate(duration_secs).await,
                            Err(err) => Response::error(err),
                        }
                    } else if args.len() > 1 && args[1].eq_ignore_ascii_case(b"LIST") {
                        debug.bundle_list()
                    } else {
                        Response::error(
                            "ERR Unknown DEBUG BUNDLE subcommand. Use GENERATE [DURATION <seconds>] or LIST.",
                        )
                    }
                }
                b"HASHING" => debug_hashing(num_shards, args),
                b"RESP3" => debug_resp3(args),
                b"SET-ACTIVE-EXPIRE" => {
                    // args[0] = "SET-ACTIVE-EXPIRE", args[1] = "0" or "1"
                    if args.len() < 2 {
                        return Response::error(
                            "ERR wrong number of arguments for 'DEBUG SET-ACTIVE-EXPIRE' command",
                        );
                    }
                    let enabled = match args[1].as_ref() {
                        b"0" => false,
                        b"1" => true,
                        _ => {
                            return Response::error("ERR DEBUG SET-ACTIVE-EXPIRE requires 0 or 1");
                        }
                    };
                    debug.set_active_expire(enabled).await;
                    Response::ok()
                }
                b"KEYSIZES-HIST-ASSERT" => keysizes_hist_assert(debug, args).await,
                b"ALLOCSIZE-SLOTS-ASSERT" => allocsize_slots_assert(debug, args).await,
                // Dangerous commands — intentionally not supported
                b"SEGFAULT" | b"RELOAD" | b"CRASH-AND-RECOVER" | b"OOM" | b"PANIC" => {
                    Response::error(format!(
                        "ERR DEBUG {} is not supported (unsafe command)",
                        String::from_utf8_lossy(&subcommand)
                    ))
                }
                _ => Response::error(format!(
                    "ERR Unknown DEBUG subcommand '{}'",
                    String::from_utf8_lossy(&subcommand)
                )),
            }
        })
    }
}

/// DEBUG SLEEP <seconds> — sleep the connection task (never the shard worker).
async fn debug_sleep(args: &[Bytes]) -> Response {
    if args.len() < 2 {
        return Response::error("ERR wrong number of arguments for 'debug|sleep' command");
    }

    // args[0] is "SLEEP", args[1] is the duration
    let duration_str = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return Response::error("ERR invalid duration"),
    };

    let duration: f64 = match duration_str.parse() {
        Ok(d) => d,
        Err(_) => return Response::error("ERR invalid duration"),
    };

    if duration < 0.0 {
        return Response::error("ERR invalid duration");
    }

    let duration_ms = (duration * 1000.0) as u64;
    tokio::time::sleep(Duration::from_millis(duration_ms)).await;

    Response::ok()
}

/// DEBUG HELP — usage lines.
fn debug_help() -> Response {
    let help = vec![
        "DEBUG SLEEP <seconds>",
        "    Sleep for the specified number of seconds.",
        "DEBUG STRUCTSIZE",
        "    Show sizes of internal data structures.",
        "DEBUG TRACING STATUS",
        "    Show tracing configuration and status.",
        "DEBUG TRACING RECENT [count]",
        "    Show recent trace entries.",
        "DEBUG VLL [shard_id]",
        "    Show VLL queue info.",
        "DEBUG LOCKTABLE",
        "    Show the per-shard VLL lock table (intents, grants, continuation locks).",
        "DEBUG PUBSUB LIMITS",
        "    Show pub/sub subscription usage vs limits.",
        "DEBUG BUNDLE GENERATE [DURATION <seconds>]",
        "    Generate a diagnostic bundle.",
        "DEBUG BUNDLE LIST",
        "    List available diagnostic bundles.",
        "DEBUG OBJECT <key>",
        "    Inspect key internals.",
        "DEBUG HASHING <key> [key ...]",
        "    Show hash slot and shard for keys.",
        "DEBUG RESP3 BIGNUMBER <value>",
        "    Return a RESP3 BigNumber response.",
        "DEBUG RESP3 BOOLEAN <0|1>",
        "    Return a RESP3 Boolean response.",
        "DEBUG RESP3 VERBATIM <encoding> <text>",
        "    Return a RESP3 VerbatimString response.",
        "DEBUG HELP",
        "    Show this help.",
    ];
    Response::Array(help.into_iter().map(Response::bulk).collect())
}

/// DEBUG STRUCTSIZE — sizes of internal data structures.
fn debug_structsize() -> Response {
    let pairs = [
        ("bits", usize::BITS as usize),
        ("value", mem::size_of::<Value>()),
        ("string", mem::size_of::<StringValue>()),
        ("list", mem::size_of::<ListValue>()),
        ("set", mem::size_of::<SetValue>()),
        ("hash", mem::size_of::<HashValue>()),
        ("sortedset", mem::size_of::<SortedSetValue>()),
        ("stream", mem::size_of::<StreamValue>()),
        ("json", mem::size_of::<JsonValue>()),
        ("bloom", mem::size_of::<BloomFilterValue>()),
        ("hll", mem::size_of::<HyperLogLogValue>()),
        ("timeseries", mem::size_of::<TimeSeriesValue>()),
        ("skiplistnode", frogdb_core::skiplist::NODE_SIZE),
        ("metadata", mem::size_of::<KeyMetadata>()),
    ];
    let output: Vec<String> = pairs.iter().map(|(k, v)| format!("{}:{}", k, v)).collect();
    Response::Bulk(Some(Bytes::from(output.join(" "))))
}

/// DEBUG HASHING <key> [key ...] — hash slot and shard mapping for the keys.
fn debug_hashing(num_shards: usize, args: &[Bytes]) -> Response {
    // args[0] = "HASHING", args[1..] = keys
    if args.len() < 2 {
        return Response::error("ERR wrong number of arguments for 'DEBUG HASHING' command");
    }
    let keys = &args[1..];

    let format_key = |key: &Bytes| -> String {
        let slot = slot_for_key(key);
        let shard = shard_for_key(key, num_shards);
        let hash_tag = extract_hash_tag(key);
        let tag_str = match hash_tag {
            Some(tag) => String::from_utf8_lossy(tag).to_string(),
            None => "(none)".to_string(),
        };
        let hash_key = hash_tag.unwrap_or(key.as_ref());
        let crc = crc16::State::<crc16::XMODEM>::calculate(hash_key);
        format!(
            "key:{} hash_tag:{} hash:0x{:04x} slot:{} shard:{} num_shards:{}",
            String::from_utf8_lossy(key),
            tag_str,
            crc,
            slot,
            shard,
            num_shards,
        )
    };

    if keys.len() == 1 {
        Response::Simple(Bytes::from(format_key(&keys[0])))
    } else {
        Response::Array(
            keys.iter()
                .map(|key| Response::Bulk(Some(Bytes::from(format_key(key)))))
                .collect(),
        )
    }
}

/// DEBUG RESP3 BIGNUMBER|BOOLEAN|VERBATIM — RESP3-type test responses.
fn debug_resp3(args: &[Bytes]) -> Response {
    // args[0] = "RESP3", args[1] = subcommand, args[2..] = arguments
    if args.len() < 2 {
        return Response::error(
            "ERR wrong number of arguments for 'DEBUG RESP3' command. Use BIGNUMBER, BOOLEAN, or VERBATIM.",
        );
    }

    let sub = args[1].to_ascii_uppercase();
    match sub.as_slice() {
        b"BIGNUMBER" => {
            if args.len() < 3 {
                return Response::error(
                    "ERR wrong number of arguments for 'DEBUG RESP3 BIGNUMBER' command",
                );
            }
            Response::BigNumber(args[2].clone())
        }
        b"BOOLEAN" => {
            if args.len() < 3 {
                return Response::error(
                    "ERR wrong number of arguments for 'DEBUG RESP3 BOOLEAN' command",
                );
            }
            let val = match args[2].as_ref() {
                b"1" | b"true" | b"TRUE" => true,
                b"0" | b"false" | b"FALSE" => false,
                _ => {
                    return Response::error("ERR value must be 0, 1, true, or false");
                }
            };
            Response::Boolean(val)
        }
        b"VERBATIM" => {
            if args.len() < 4 {
                return Response::error(
                    "ERR wrong number of arguments for 'DEBUG RESP3 VERBATIM' command. Usage: DEBUG RESP3 VERBATIM <encoding> <text>",
                );
            }
            let encoding = &args[2];
            if encoding.len() != 3 {
                return Response::error(
                    "ERR encoding must be exactly 3 characters (e.g., txt, mkd)",
                );
            }
            let mut format = [0u8; 3];
            format.copy_from_slice(&encoding[..3]);
            Response::VerbatimString {
                format,
                data: args[3].clone(),
            }
        }
        _ => Response::error(format!(
            "ERR Unknown DEBUG RESP3 subcommand '{}'. Use BIGNUMBER, BOOLEAN, or VERBATIM.",
            String::from_utf8_lossy(&sub)
        )),
    }
}

/// Parse the optional `shard_id` argument of DEBUG VLL, validating it against the
/// live shard count. Returns `Ok(None)` when no shard is named; the `Err` string
/// is the raw `ERR …` message the caller wraps in [`Response::error`].
fn parse_vll_shard_filter(args: &[Bytes], shard_count: usize) -> Result<Option<usize>, String> {
    // args[0] = "VLL", args[1] = optional shard_id
    if args.len() > 1 {
        match std::str::from_utf8(&args[1]) {
            Ok(s) => match s.parse::<usize>() {
                Ok(id) => {
                    if id >= shard_count {
                        Err(format!(
                            "ERR invalid shard_id: {} (num_shards: {})",
                            id, shard_count
                        ))
                    } else {
                        Ok(Some(id))
                    }
                }
                Err(_) => Err("ERR invalid shard_id: must be a number".to_string()),
            },
            Err(_) => Err("ERR invalid shard_id: must be valid UTF-8".to_string()),
        }
    } else {
        Ok(None)
    }
}

/// Format the VLL queue info gathered from the shards into the DEBUG VLL reply.
fn format_vll_response(infos: Vec<frogdb_core::shard::VllQueueInfo>) -> Response {
    // Check if all queues are empty
    let all_empty = infos
        .iter()
        .all(|i| i.queue_depth == 0 && i.continuation_lock.is_none() && i.intent_table.is_empty());

    if all_empty {
        return Response::Bulk(Some(Bytes::from("# VLL queues are empty")));
    }

    let mut lines = Vec::new();

    for info in infos {
        // Shard header
        let mut header = format!("shard:{} queue_depth:{}", info.shard_id, info.queue_depth);
        if let Some(txid) = info.executing_txid {
            header.push_str(&format!(" executing_txid:{}", txid));
        }
        lines.push(header);

        // Continuation lock
        if let Some(ref lock) = info.continuation_lock {
            lines.push(format!(
                "continuation_lock: txid:{} conn_id:{} age_ms:{}",
                lock.txid, lock.conn_id, lock.age_ms
            ));
        }

        // Pending operations
        if !info.pending_ops.is_empty() {
            lines.push("pending:".to_string());
            for op in &info.pending_ops {
                lines.push(format!(
                    "  txid:{} operation:{} keys:{} state:{} age_ms:{}",
                    op.txid, op.operation, op.key_count, op.state, op.age_ms
                ));
            }
        }

        // Intent table
        if !info.intent_table.is_empty() {
            lines.push("intents:".to_string());
            for intent in &info.intent_table {
                let txids_str: Vec<String> = intent.txids.iter().map(|t| t.to_string()).collect();
                lines.push(format!(
                    "  key:{} txids:[{}] lock:{}",
                    intent.key,
                    txids_str.join(","),
                    intent.lock_state
                ));
            }
        }

        // Empty line between shards
        lines.push(String::new());
    }

    // Remove trailing empty line
    if lines.last().map(|s| s.is_empty()).unwrap_or(false) {
        lines.pop();
    }

    Response::Bulk(Some(Bytes::from(lines.join("\n"))))
}

/// Format `DEBUG LOCKTABLE` — a RESP map of `shard:<id>` → per-shard detail.
/// Empty across all shards returns a recognizable sentinel bulk string.
fn format_locktable_response(infos: Vec<frogdb_core::shard::LockTableInfo>) -> Response {
    let all_empty = infos
        .iter()
        .all(|i| i.intents.is_empty() && i.continuation_lock.is_none());
    if all_empty {
        return Response::Bulk(Some(Bytes::from("# lock table is empty")));
    }

    let mut shards = Vec::new();
    for info in infos {
        let intents = Response::Array(
            info.intents
                .iter()
                .map(|intent| {
                    Response::Map(vec![
                        (Response::bulk("key"), Response::bulk(intent.key.clone())),
                        (
                            Response::bulk("txids"),
                            Response::Array(
                                intent
                                    .txids
                                    .iter()
                                    .map(|t| Response::Integer(*t as i64))
                                    .collect(),
                            ),
                        ),
                        (
                            Response::bulk("lock_state"),
                            Response::bulk(intent.lock_state.clone()),
                        ),
                    ])
                })
                .collect(),
        );
        let continuation_lock = match &info.continuation_lock {
            Some(l) => Response::bulk(format!(
                "txid:{} conn_id:{} age_ms:{}",
                l.txid, l.conn_id, l.age_ms
            )),
            None => Response::Bulk(None),
        };
        shards.push((
            Response::bulk(format!("shard:{}", info.shard_id)),
            Response::Map(vec![
                (Response::bulk("continuation_lock"), continuation_lock),
                (Response::bulk("intents"), intents),
            ]),
        ));
    }
    Response::Map(shards)
}

/// Parse the optional `DURATION <seconds>` of DEBUG BUNDLE GENERATE. The `Err`
/// string is the raw `ERR …` message the caller wraps in [`Response::error`].
fn parse_bundle_duration(args: &[Bytes]) -> Result<u64, String> {
    // args[0] = "BUNDLE", args[1] = "GENERATE", args[2..] = optional DURATION <seconds>
    let mut duration_secs: u64 = 0;
    let mut i = 2;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"DURATION") {
            if i + 1 >= args.len() {
                return Err("ERR DURATION requires a value in seconds".to_string());
            }
            match std::str::from_utf8(&args[i + 1])
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
            {
                Some(d) => duration_secs = d,
                None => {
                    return Err("ERR DURATION must be a positive integer".to_string());
                }
            }
            i += 2;
        } else {
            return Err(format!(
                "ERR Unknown argument '{}' for DEBUG BUNDLE GENERATE",
                String::from_utf8_lossy(&args[i])
            ));
        }
    }
    Ok(duration_secs)
}

/// DEBUG KEYSIZES-HIST-ASSERT <type> <bin> <expected> — assert a keysize
/// histogram bin's count against the fleet-merged histograms.
async fn keysizes_hist_assert(debug: &dyn DebugProvider, args: &[Bytes]) -> Response {
    // args[0] = "KEYSIZES-HIST-ASSERT", args[1] = type, args[2] = bin, args[3] = expected
    if args.len() < 4 {
        return Response::error(
            "ERR wrong number of arguments for 'DEBUG KEYSIZES-HIST-ASSERT' command. Usage: DEBUG KEYSIZES-HIST-ASSERT <type> <bin> <expected>",
        );
    }

    let type_name = match std::str::from_utf8(&args[1]) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return Response::error("ERR invalid type name"),
    };

    let bin: usize = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(b) => b,
        None => return Response::error("ERR invalid bin index"),
    };

    let expected: u64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(e) => e,
        None => return Response::error("ERR invalid expected count"),
    };

    let merged = debug.keysizes_snapshot().await;

    let actual = if type_name == "keymem" {
        merged.key_memory.get_bin(bin)
    } else if let Some(ty) = KeysizeType::from_debug_name(&type_name) {
        merged.get(ty).get_bin(bin)
    } else {
        return Response::error(format!(
            "ERR unknown type '{}'. Use: strings, lists, sets, hashes, zsets, streams, hlls, keymem",
            type_name
        ));
    };

    if actual == expected {
        Response::ok()
    } else {
        Response::error(format!(
            "ERR KEYSIZES-HIST-ASSERT type={} bin={}: expected {} but got {}",
            type_name, bin, expected, actual
        ))
    }
}

/// DEBUG ALLOCSIZE-SLOTS-ASSERT <slot> <expected> — assert the total allocated
/// memory for keys in a slot against the fleet sum.
async fn allocsize_slots_assert(debug: &dyn DebugProvider, args: &[Bytes]) -> Response {
    // args[0] = "ALLOCSIZE-SLOTS-ASSERT", args[1] = slot, args[2] = expected
    if args.len() < 3 {
        return Response::error(
            "ERR wrong number of arguments for 'DEBUG ALLOCSIZE-SLOTS-ASSERT' command. Usage: DEBUG ALLOCSIZE-SLOTS-ASSERT <slot> <expected>",
        );
    }

    let slot: u16 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(s) => s,
        None => return Response::error("ERR invalid slot"),
    };

    let expected: usize = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(e) => e,
        None => return Response::error("ERR invalid expected size"),
    };

    let total = debug.allocsize_in_slot(slot).await;

    if total == expected {
        Response::ok()
    } else {
        Response::error(format!(
            "ERR ALLOCSIZE-SLOTS-ASSERT slot={}: expected {} but got {}",
            slot, expected, total
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::KeysizeHistograms;

    #[test]
    fn spec_is_connection_level_and_valid() {
        assert!(DEBUG_CONN_COMMAND.spec().validate().is_ok());
        assert!(matches!(
            DEBUG_CONN_COMMAND.spec().strategy,
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
        ));
    }

    #[test]
    fn dynamic_keys_extracts_object_key_only() {
        let object = [Bytes::from_static(b"OBJECT"), Bytes::from_static(b"mykey")];
        assert_eq!(
            DebugConnCommand.dynamic_keys(&object),
            vec![b"mykey".as_slice()]
        );

        // Other subcommands (and OBJECT without a key) are keyless.
        let hashing = [Bytes::from_static(b"HASHING"), Bytes::from_static(b"k")];
        assert!(DebugConnCommand.dynamic_keys(&hashing).is_empty());
        let bare = [Bytes::from_static(b"OBJECT")];
        assert!(DebugConnCommand.dynamic_keys(&bare).is_empty());
    }

    fn arg(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn structsize_is_space_separated_pairs() {
        match debug_structsize() {
            Response::Bulk(Some(b)) => {
                let s = String::from_utf8(b.to_vec()).unwrap();
                assert!(s.starts_with("bits:"), "unexpected structsize: {s}");
                assert!(s.contains(" value:"));
                assert!(s.contains(" metadata:"));
            }
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    #[test]
    fn help_is_nonempty_array() {
        match debug_help() {
            Response::Array(items) => assert!(!items.is_empty()),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn resp3_boolean_parses_and_rejects() {
        assert_eq!(
            debug_resp3(&[arg("RESP3"), arg("BOOLEAN"), arg("1")]),
            Response::Boolean(true)
        );
        assert_eq!(
            debug_resp3(&[arg("RESP3"), arg("BOOLEAN"), arg("false")]),
            Response::Boolean(false)
        );
        assert!(matches!(
            debug_resp3(&[arg("RESP3"), arg("BOOLEAN"), arg("maybe")]),
            Response::Error(_)
        ));
    }

    #[test]
    fn resp3_verbatim_requires_three_char_encoding() {
        assert!(matches!(
            debug_resp3(&[arg("RESP3"), arg("VERBATIM"), arg("toolong"), arg("hi")]),
            Response::Error(_)
        ));
        match debug_resp3(&[arg("RESP3"), arg("VERBATIM"), arg("txt"), arg("hi")]) {
            Response::VerbatimString { format, data } => {
                assert_eq!(&format, b"txt");
                assert_eq!(data, Bytes::from_static(b"hi"));
            }
            other => panic!("expected verbatim, got {other:?}"),
        }
    }

    #[test]
    fn resp3_unknown_subcommand_errors() {
        assert!(matches!(
            debug_resp3(&[arg("RESP3"), arg("NOPE")]),
            Response::Error(_)
        ));
    }

    #[test]
    fn hashing_single_key_is_simple_string() {
        match debug_hashing(4, &[arg("HASHING"), arg("foo")]) {
            Response::Simple(s) => {
                let s = String::from_utf8(s.to_vec()).unwrap();
                assert!(s.starts_with("key:foo "), "unexpected: {s}");
                assert!(s.contains("num_shards:4"));
            }
            other => panic!("expected simple, got {other:?}"),
        }
    }

    #[test]
    fn hashing_multi_key_is_array() {
        match debug_hashing(4, &[arg("HASHING"), arg("a"), arg("b")]) {
            Response::Array(items) => assert_eq!(items.len(), 2),
            other => panic!("expected array, got {other:?}"),
        }
        assert!(matches!(
            debug_hashing(4, &[arg("HASHING")]),
            Response::Error(_)
        ));
    }

    #[test]
    fn vll_shard_filter_validates_bounds() {
        assert_eq!(parse_vll_shard_filter(&[arg("VLL")], 4).unwrap(), None);
        assert_eq!(
            parse_vll_shard_filter(&[arg("VLL"), arg("2")], 4).unwrap(),
            Some(2)
        );
        assert!(parse_vll_shard_filter(&[arg("VLL"), arg("9")], 4).is_err());
        assert!(parse_vll_shard_filter(&[arg("VLL"), arg("nope")], 4).is_err());
    }

    #[test]
    fn empty_vll_response_reports_empty() {
        assert_eq!(
            format_vll_response(vec![]),
            Response::Bulk(Some(Bytes::from("# VLL queues are empty")))
        );
    }

    #[test]
    fn bundle_duration_parses_and_rejects() {
        assert_eq!(
            parse_bundle_duration(&[arg("BUNDLE"), arg("GENERATE")]).unwrap(),
            0
        );
        assert_eq!(
            parse_bundle_duration(&[arg("BUNDLE"), arg("GENERATE"), arg("DURATION"), arg("5")])
                .unwrap(),
            5
        );
        assert!(parse_bundle_duration(&[arg("BUNDLE"), arg("GENERATE"), arg("DURATION")]).is_err());
        assert!(parse_bundle_duration(&[arg("BUNDLE"), arg("GENERATE"), arg("BOGUS")]).is_err());
    }

    /// A stub [`DebugProvider`] so the executor's routing can be exercised
    /// without a live `ConnectionHandler`.
    struct StubDebug {
        enabled: bool,
    }

    impl DebugProvider for StubDebug {
        fn debug_command_enabled(&self) -> bool {
            self.enabled
        }
        fn tracing_status(&self) -> Response {
            Response::ok()
        }
        fn tracing_recent(&self, _count: usize) -> Response {
            Response::Array(vec![])
        }
        fn gather_vll<'a>(
            &'a self,
            _shard_filter: Option<usize>,
        ) -> BoxFuture<'a, Vec<frogdb_core::shard::VllQueueInfo>> {
            Box::pin(async { Vec::new() })
        }
        fn gather_lock_table<'a>(
            &'a self,
        ) -> BoxFuture<'a, Vec<frogdb_core::shard::LockTableInfo>> {
            Box::pin(async { Vec::new() })
        }
        fn pubsub_limits<'a>(&'a self) -> BoxFuture<'a, Response> {
            Box::pin(async { Response::ok() })
        }
        fn bundle_generate<'a>(&'a self, _duration_secs: u64) -> BoxFuture<'a, Response> {
            Box::pin(async { Response::ok() })
        }
        fn bundle_list(&self) -> Response {
            Response::Array(vec![])
        }
        fn set_active_expire<'a>(&'a self, _enabled: bool) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
        fn keysizes_snapshot<'a>(&'a self) -> BoxFuture<'a, KeysizeHistograms> {
            Box::pin(async { KeysizeHistograms::new() })
        }
        fn allocsize_in_slot<'a>(&'a self, _slot: u16) -> BoxFuture<'a, usize> {
            Box::pin(async { 0 })
        }
    }

    #[tokio::test]
    async fn sleep_disabled_reports_error() {
        let stub = StubDebug { enabled: false };
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("SLEEP"), arg("0")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn unknown_subcommand_errors() {
        let stub = StubDebug { enabled: true };
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("NOPE")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn dangerous_subcommand_is_unsupported() {
        let stub = StubDebug { enabled: true };
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("RELOAD")])
            .await;
        match resp {
            Response::Error(e) => {
                assert!(String::from_utf8_lossy(&e).contains("not supported"))
            }
            other => panic!("expected error, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod tests_fixture {
    //! A minimal `ConnCtx` fixture parameterized on the `debug` provider, so the
    //! DEBUG executor can be exercised in isolation.
    use frogdb_core::{
        ClientRegistry, CommandLatencyHistograms, ConnCtx, DebugProvider, KeyspaceStats,
        NoopMetricsRecorder, SharedHotkeySession, new_shared_hotkey_session,
    };

    use crate::connection::ClusterDeps;
    use crate::connection::observability_conn_command::MemoryDiag;
    use crate::cursor_store::AggregateCursorStore;
    use crate::runtime_config::ConfigManager;

    pub(super) struct Deps {
        config_manager: ConfigManager,
        client_registry: ClientRegistry,
        latency_histograms: CommandLatencyHistograms,
        keyspace_stats: KeyspaceStats,
        snapshot_coordinator: frogdb_core::persistence::NoopSnapshotCoordinator,
        hotkey_session: SharedHotkeySession,
        cluster: ClusterDeps,
        cursor_store: AggregateCursorStore,
        metrics_recorder: NoopMetricsRecorder,
        memory_diag: MemoryDiag,
        acl_manager: std::sync::Arc<frogdb_core::AclManager>,
        command_registry: frogdb_core::CommandRegistry,
    }

    impl Deps {
        pub(super) fn new() -> Self {
            Self {
                config_manager: ConfigManager::new(&crate::config::Config::default()),
                client_registry: ClientRegistry::new(),
                latency_histograms: CommandLatencyHistograms::new(true),
                keyspace_stats: KeyspaceStats::new(),
                snapshot_coordinator: frogdb_core::persistence::NoopSnapshotCoordinator::new(),
                hotkey_session: new_shared_hotkey_session(),
                cluster: ClusterDeps::standalone(),
                cursor_store: AggregateCursorStore::new(),
                metrics_recorder: NoopMetricsRecorder::new(),
                memory_diag: MemoryDiag(frogdb_debug::MemoryDiagConfig::default()),
                acl_manager: frogdb_core::AclManager::new(Default::default()),
                command_registry: frogdb_core::CommandRegistry::new(),
            }
        }

        pub(super) fn ctx<'a>(&'a self, debug: Option<&'a dyn DebugProvider>) -> ConnCtx<'a> {
            let mut ctx = ConnCtx::new(
                &self.config_manager,
                &self.client_registry,
                &self.latency_histograms,
                &self.keyspace_stats,
                &[],
                &self.snapshot_coordinator,
                &self.hotkey_session,
                &self.cluster,
                &self.cursor_store,
                &self.metrics_recorder,
                &self.memory_diag,
                self.acl_manager.as_ref(),
                &self.command_registry,
                0,
                10000,
                false,
            )
            .with_username("default");
            ctx.debug = debug;
            ctx
        }
    }
}
