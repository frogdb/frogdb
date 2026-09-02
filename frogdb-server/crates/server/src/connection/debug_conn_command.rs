//! DEBUG connection command.
//!
//! DEBUG is migrated behind the [`ConnectionCommand`] seam (see
//! [`crate::connection::conn_command`] and the CONFIG executor there for the
//! template). It is an admin command with a wide subcommand surface: some
//! subcommands are pure (STRUCTSIZE, HELP, HASHING, RESP3), some sleep the
//! connection task (SLEEP), and several round-trip the shards (VLL,
//! SET-ACTIVE-EXPIRE, KEYSIZES-HIST-ASSERT, ALLOCSIZE-SLOTS-ASSERT) or reach
//! server-only subsystems (TRACING, PUBSUB LIMITS, BUNDLE, CLUSTER CHECK).
//!
//! The subcommand routing and argument parsing live here in the executor; the
//! per-subcommand *I/O* that needs the handler (the tracer, per-shard messages,
//! this connection's subscription counts, the `frogdb_debug` bundle machinery,
//! the live `ClusterState`, the `enable-debug-command` gate) stays behind the
//! [`frogdb_core::DebugProvider`]
//! seam, implemented for `ConnectionHandler` in
//! [`crate::connection::debug_handler`]. The wire output of every subcommand
//! is byte-for-byte identical to the pre-migration `dispatch_debug`.
//!
//! DEBUG is registered *only* as a `CommandImpl::Connection` executor. `COMMAND
//! GETKEYS` resolves it through the registry union (`get_entry`), so this
//! executor's [`dynamic_keys`] override supplies the key of each
//! [keyed subcommand](KEYED_SUBCOMMANDS) — OBJECT, EXPIRE-BACKDATE and
//! RE-ENCODE — directly;
//! no shard-local key-extraction stub is required. The spec declares
//! `KeySpec::Dynamic` + `MOVABLEKEYS` so `COMMAND` metadata is correct. The two
//! halves are held in agreement by `every_keyed_subcommand_is_dispatched`: a
//! subcommand `dynamic_keys` declares but dispatch rejects is metadata that lies
//! to a cluster-aware client, and that test fails on it.
//!
//! [`dynamic_keys`]: ConnectionCommand::dynamic_keys

use std::mem;
use std::time::Duration;

use bytes::Bytes;
use frogdb_core::shard::{extract_hash_tag, shard_for_key, slot_for_key};
use frogdb_core::{
    AccessSpec, Arity, BloomFilterValue, BoxFuture, CLUSTER_SLOTS, CommandFlags, CommandSpec,
    ConnCtx, ConnectionCommand, ConnectionLevelOp, DebugProvider, EventSpec, ExecutionStrategy,
    HashValue, HyperLogLogValue, JsonValue, KeyMetadata, KeySpec, KeysizeType, ListValue,
    LookupSpec, PauseMode, SetValue, SortedSetValue, StreamValue, StringValue, TimeSeriesValue,
    Value, WaiterWake, WalStrategy,
};
use frogdb_protocol::{Response, SafeStatus};

/// The `CommandSpec` for DEBUG — arity `AtLeast(1)`, `ADMIN | NOSCRIPT | LOADING
/// | STALE | MOVABLEKEYS`, `KeySpec::Dynamic`, strategy `ConnectionLevel(Admin)`.
/// This is the sole registered executor for DEBUG, so `COMMAND`/`get_entry`
/// metadata comes straight from here. The registry validates that the strategy
/// agrees with the `Connection` executor variant.
static DEBUG_SPEC: CommandSpec = CommandSpec {
    name: "DEBUG",
    docs: frogdb_core::CommandDocs {
        summary: "A container for debugging commands.",
        since: "1.0.0",
        group: "server",
        complexity: Some("Depends on subcommand."),
    },
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
    reindex: frogdb_core::ReindexSpec::None,
    lookup: LookupSpec::None,
    mutation: frogdb_core::ConnMutation::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The DEBUG subcommands that take a key as their second argument.
///
/// The single source of truth for [`DebugConnCommand::dynamic_keys`]. Every name
/// here must also have a dispatch arm in [`DebugConnCommand::execute`] — a
/// subcommand that declares a key `COMMAND GETKEYS` would report but that
/// dispatch rejects is untruthful metadata, so
/// `every_keyed_subcommand_is_dispatched` cross-checks this list against the
/// dispatch.
const KEYED_SUBCOMMANDS: [&[u8]; 3] = [b"OBJECT", b"EXPIRE-BACKDATE", b"RE-ENCODE"];

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

    /// A [keyed subcommand](KEYED_SUBCOMMANDS)'s key is its second argument;
    /// every other subcommand is keyless.
    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() >= 2 {
            let subcommand = args[0].to_ascii_uppercase();
            if KEYED_SUBCOMMANDS.contains(&subcommand.as_slice()) {
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
            let client_registry = ctx.client_registry;

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
                b"WAITQUEUE" => format_waitqueue_response(debug.gather_wait_queue().await),
                b"WAITQUEUE-LOG" => {
                    format_waitqueue_log_response(debug.gather_wait_queue_log().await)
                }
                b"MEMORY-CHECK" => format_memory_check_response(debug.memory_check().await),
                b"EXPIRY-INDEX-CHECK" => {
                    format_expiry_index_check_response(debug.expiry_index_check().await)
                }
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
                b"EXPIRE-BACKDATE" => {
                    // args[0] = "EXPIRE-BACKDATE", args[1] = key, args[2] = ms
                    if args.len() != 3 {
                        return Response::error(
                            "ERR wrong number of arguments for 'DEBUG EXPIRE-BACKDATE' command",
                        );
                    }
                    let ms = match std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        Some(ms) => ms,
                        None => {
                            return Response::error(
                                "ERR DEBUG EXPIRE-BACKDATE ms must be a non-negative integer",
                            );
                        }
                    };
                    let key = args[1].clone();
                    let shard_id = shard_for_key(&key, num_shards);
                    debug.expire_backdate(shard_id, key, ms).await
                }
                b"OBJECT" => {
                    // args[0] = "OBJECT", args[1] = key
                    if args.len() != 2 {
                        return Response::error(
                            "ERR wrong number of arguments for 'DEBUG OBJECT' command",
                        );
                    }
                    let key = args[1].clone();
                    let shard_id = shard_for_key(&key, num_shards);
                    match debug.object_info(shard_id, key).await {
                        Ok(Some(info)) => format_object_info(&info),
                        // Redis's reply for a key DEBUG OBJECT cannot find.
                        Ok(None) => Response::error("ERR no such key"),
                        Err(err) => err,
                    }
                }
                b"RE-ENCODE" => {
                    // args[0] = "RE-ENCODE", args[1] = key
                    if args.len() != 2 {
                        return Response::error(
                            "ERR wrong number of arguments for 'DEBUG RE-ENCODE' command",
                        );
                    }
                    let key = args[1].clone();
                    let shard_id = shard_for_key(&key, num_shards);
                    match debug.re_encode(shard_id, key).await {
                        Ok(Some(result)) => format_re_encode(&result),
                        // The same reply DEBUG OBJECT gives for a key it cannot
                        // find, for the same reason.
                        Ok(None) => Response::error("ERR no such key"),
                        Err(err) => err,
                    }
                }
                b"CLUSTER" => {
                    if args.len() > 1 && args[1].eq_ignore_ascii_case(b"CHECK") {
                        format_check_response(
                            debug.cluster_check(),
                            "ERR This instance has cluster support disabled",
                        )
                    } else {
                        Response::error("ERR Unknown DEBUG CLUSTER subcommand. Use CHECK.")
                    }
                }
                b"REPLICATION" => {
                    if args.len() > 1 && args[1].eq_ignore_ascii_case(b"CHECK") {
                        format_check_response(
                            debug.replication_check(),
                            "ERR This instance has replication support disabled",
                        )
                    } else {
                        Response::error("ERR Unknown DEBUG REPLICATION subcommand. Use CHECK.")
                    }
                }
                b"ARENA-DECAY" => arena_decay(debug, args),
                b"PAUSE-SLOT" => debug_pause_slot(client_registry, args),
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

/// DEBUG ARENA-DECAY \[&lt;dirty_ms&gt; &lt;muzzy_ms&gt;\] — report or retune how long
/// each shard arena holds on to freed pages before returning them to the OS.
///
/// The server sets both at startup through `MALLOC_CONF` (see
/// [`crate::malloc_conf`]); this is the runtime half of that setting, so a
/// fragmentation problem can be diagnosed and its decay retuned on a live node
/// instead of waiting for a restart. Milliseconds, `0` = decay immediately,
/// `-1` = never.
///
/// A write applies to every shard arena, which is what an operator retuning a
/// node wants; per-arena divergence would make the per-shard metrics
/// incomparable for no gain. It is applied one arena at a time because
/// jemalloc's all-arenas index is not accepted by the `decay_ms` mallctls (it
/// answers `EFAULT`), and a node with no bound arenas is told so rather than
/// given an `+OK` for a setting that reached nothing.
fn arena_decay(debug: &dyn DebugProvider, args: &[Bytes]) -> Response {
    use frogdb_telemetry::jemalloc;

    let arenas = debug.shard_arenas();
    match args.len() {
        1 => {
            let Some(configured) = jemalloc::configured_decay() else {
                return Response::error(
                    "ERR DEBUG ARENA-DECAY requires a jemalloc build of the server",
                );
            };
            let mut lines = vec![
                "# Arena decay in milliseconds (0 = purge on free, -1 = never)".to_string(),
                format!(
                    "startup:dirty={},muzzy={}",
                    configured.dirty_ms, configured.muzzy_ms
                ),
            ];
            for (shard_id, arena) in arenas {
                // An arena whose decay cannot be read is skipped rather than
                // reported with the startup values: this reply exists to expose
                // divergence, so guessing here would defeat it.
                if let Some(decay) = jemalloc::read_arena_decay(arena) {
                    lines.push(format!(
                        "shard{shard_id}:arena={arena},dirty={},muzzy={}",
                        decay.dirty_ms, decay.muzzy_ms
                    ));
                }
            }
            Response::Bulk(Some(Bytes::from(lines.join("\n"))))
        }
        3 => {
            let (Some(dirty_ms), Some(muzzy_ms)) =
                (parse_decay_ms(&args[1]), parse_decay_ms(&args[2]))
            else {
                return Response::error(
                    "ERR DEBUG ARENA-DECAY milliseconds must be an integer >= -1",
                );
            };
            if arenas.is_empty() {
                return Response::error(
                    "ERR DEBUG ARENA-DECAY has no shard arenas to retune on this node",
                );
            }
            let decay = jemalloc::ArenaDecay { dirty_ms, muzzy_ms };
            let total = arenas.len();
            for (applied, (_, arena)) in arenas.into_iter().enumerate() {
                // A mid-loop failure leaves the arenas already visited retuned
                // — jemalloc has no transactional multi-arena write — so the
                // error says how far it got. The no-argument form shows the
                // resulting per-arena state.
                if let Err(err) = jemalloc::set_arena_decay(arena, decay) {
                    return Response::error(format!(
                        "ERR DEBUG ARENA-DECAY failed on arena {arena} \
                         after retuning {applied} of {total} arenas: {err}"
                    ));
                }
            }
            Response::ok()
        }
        _ => Response::error("ERR wrong number of arguments for 'DEBUG ARENA-DECAY' command"),
    }
}

/// A decay setting in milliseconds: any non-negative count, or `-1` for "never
/// decay". Anything below `-1` is not a jemalloc decay value.
fn parse_decay_ms(arg: &Bytes) -> Option<i64> {
    let ms = std::str::from_utf8(arg).ok()?.parse::<i64>().ok()?;
    (ms >= -1).then_some(ms)
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
        "DEBUG WAITQUEUE",
        "    Show blocked waiters by key/connection, in registration order.",
        "DEBUG WAITQUEUE-LOG",
        "    Show every recorded blocking registration, in registration order (test support).",
        "DEBUG MEMORY-CHECK",
        "    Recompute live memory and report the diff vs the tracked counter.",
        "DEBUG EXPIRY-INDEX-CHECK",
        "    Cross-check the expiry index against entry deadlines.",
        "DEBUG EXPIRE-BACKDATE <key> <ms>",
        "    Backdate a key's TTL <ms> into the past so it is already expired (test support).",
        "DEBUG ARENA-DECAY [<dirty_ms> <muzzy_ms>]",
        "    Report each shard arena's page-decay settings, or retune every arena (ms; -1 = never).",
        "DEBUG CLUSTER CHECK",
        "    Run the cluster invariant catalog against live state; empty array = clean.",
        "DEBUG REPLICATION CHECK",
        "    Run the replication invariant catalog against live state (every role, including",
        "    standalone); empty array = clean.",
        "DEBUG PAUSE-SLOT <slot> <timeout-ms> [WRITE|ALL]",
        "    Arm a slot-scoped pause (0 ms disarms it) as the migration barrier does (test support).",
        "DEBUG PUBSUB LIMITS",
        "    Show pub/sub subscription usage vs limits.",
        "DEBUG BUNDLE GENERATE [DURATION <seconds>]",
        "    Generate a diagnostic bundle.",
        "DEBUG BUNDLE LIST",
        "    List available diagnostic bundles.",
        "DEBUG RE-ENCODE <key>",
        "    Rebuild one key's value through its encoding, compacting the slack churn left behind.",
        "DEBUG OBJECT <key>",
        "    Inspect key internals: refcount, encoding, serializedlength, lru stamps.",
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

/// Render `DEBUG OBJECT <key>`'s reply: a status line of `token:value` pairs,
/// as Redis's is.
///
/// **Only truthful tokens are emitted.** Redis's line also carries `at:<ptr>`
/// and, for lists, `ql_nodes`/`ql_avg_node`/…; both are absent here, and
/// deliberately:
///
/// - `at:` is the value's heap address. Printing it to any client that can run
///   DEBUG leaks the process's ASLR layout for no diagnostic gain, so FrogDB
///   omits it — a documented deviation-as-improvement.
/// - `ql_*` describe quicklist internals (node count, per-node fill, LZF
///   compression). FrogDB's list is not a quicklist, so every one of those
///   numbers would be invented. An absent token is better than a fabricated one.
///
/// Everything that *is* printed comes from the same logic as its `OBJECT`
/// subcommand twin — see [`frogdb_core::shard::ObjectInfo`].
fn format_object_info(info: &frogdb_core::shard::ObjectInfo) -> Response {
    Response::Simple(SafeStatus::sanitized(format!(
        "refcount:{} encoding:{} serializedlength:{} lru:{} lru_seconds_idle:{}",
        info.refcount, info.encoding, info.serialized_length, info.lru, info.lru_seconds_idle,
    )))
}

/// DEBUG RE-ENCODE's reply: the resulting encoding and the accounted memory on
/// either side of the rewrite, in the same `field:value` shape
/// [`format_object_info`] uses.
///
/// `after` equalling `before` is a real answer, not a failure: the value was
/// already compact, or its type has only one representation. `re_encoded:0`
/// distinguishes the latter — a type with no encoding choice to remake — from
/// a rewrite that reclaimed nothing.
fn format_re_encode(result: &frogdb_core::store::ReEncodeResult) -> Response {
    Response::Simple(SafeStatus::sanitized(format!(
        "re_encoded:{} encoding:{} memory_before:{} memory_after:{}",
        u8::from(result.re_encoded),
        result.encoding,
        result.before_bytes,
        result.after_bytes,
    )))
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
        Response::Simple(SafeStatus::sanitized(format_key(&keys[0])))
    } else {
        Response::Array(
            keys.iter()
                .map(|key| Response::Bulk(Some(Bytes::from(format_key(key)))))
                .collect(),
        )
    }
}

/// `DEBUG PAUSE-SLOT <slot> <timeout-ms> [WRITE|ALL]` — arm or disarm a
/// *slot-scoped* pause from the wire.
///
/// A slot-scoped pause is the slot-migration finalization barrier's mechanism:
/// it parks only the commands whose keys hash to `<slot>` (plus the commands
/// that cannot be pinned to any slot), leaving the rest of the keyspace serving.
/// Production arms it from the migration path, not from a client; this
/// subcommand exists so tests can drive the barrier end-to-end over a real
/// connection without inventing a slot argument for `CLIENT PAUSE`, which Redis
/// does not have and which would be a visible parity break.
///
/// `timeout-ms` of `0` disarms the slot, mirroring `CLIENT UNPAUSE` for the
/// node-global pause. The mode defaults to `WRITE` — what the barrier itself
/// uses, since reads of a slot that is still owned locally stay correct.
/// Node-global pauses are untouched either way: the two dimensions arm, expire,
/// and release independently.
fn debug_pause_slot(client_registry: &frogdb_core::ClientRegistry, args: &[Bytes]) -> Response {
    // args[0] = "PAUSE-SLOT", args[1] = slot, args[2] = timeout-ms, args[3] = mode
    if args.len() < 3 || args.len() > 4 {
        return Response::error("ERR wrong number of arguments for 'DEBUG PAUSE-SLOT' command");
    }
    let slot = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .filter(|slot| *slot < CLUSTER_SLOTS)
    {
        Some(slot) => slot,
        None => {
            return Response::error(format!(
                "ERR DEBUG PAUSE-SLOT slot must be an integer in 0..{CLUSTER_SLOTS}"
            ));
        }
    };
    let timeout_ms = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(timeout_ms) => timeout_ms,
        None => {
            return Response::error("ERR DEBUG PAUSE-SLOT timeout must be a non-negative integer");
        }
    };
    let mode = match args.get(3) {
        None => PauseMode::Write,
        Some(mode) if mode.eq_ignore_ascii_case(b"WRITE") => PauseMode::Write,
        Some(mode) if mode.eq_ignore_ascii_case(b"ALL") => PauseMode::All,
        Some(_) => return Response::error("ERR DEBUG PAUSE-SLOT mode must be WRITE or ALL"),
    };

    if timeout_ms == 0 {
        client_registry.unpause_slot(slot);
    } else {
        client_registry.pause_slot(slot, mode, timeout_ms);
    }
    Response::ok()
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

/// Format `DEBUG WAITQUEUE` — a RESP map of `shard:<id>` → detail, preserving
/// per-waiter registration order. Empty across all shards -> sentinel bulk.
fn format_waitqueue_response(infos: Vec<frogdb_core::shard::WaitQueueInfo>) -> Response {
    if infos.iter().all(|i| i.total_waiters == 0) {
        return Response::Bulk(Some(Bytes::from("# wait queue is empty")));
    }
    let mut shards = Vec::new();
    for info in infos {
        let keys = Response::Array(
            info.keys
                .iter()
                .map(|k| {
                    let waiters = Response::Array(
                        k.waiters
                            .iter()
                            .map(|w| {
                                Response::Map(vec![
                                    (
                                        Response::bulk("conn_id"),
                                        Response::Integer(w.conn_id as i64),
                                    ),
                                    (Response::bulk("op"), Response::bulk(w.op.clone())),
                                    (
                                        Response::bulk("registration_seq"),
                                        Response::Integer(w.registration_seq as i64),
                                    ),
                                    (
                                        Response::bulk("has_deadline"),
                                        Response::Integer(i64::from(w.has_deadline)),
                                    ),
                                ])
                            })
                            .collect(),
                    );
                    Response::Map(vec![
                        (Response::bulk("key"), Response::bulk(k.key.clone())),
                        (Response::bulk("waiters"), waiters),
                    ])
                })
                .collect(),
        );
        shards.push((
            Response::bulk(format!("shard:{}", info.shard_id)),
            Response::Map(vec![
                (
                    Response::bulk("total_waiters"),
                    Response::Integer(info.total_waiters as i64),
                ),
                (Response::bulk("keys"), keys),
            ]),
        ));
    }
    Response::Map(shards)
}

/// Format `DEBUG WAITQUEUE-LOG` — RESP map of `shard:<id>` → {truncated,
/// registrations}, where each registration is
/// {registration_seq, conn_id, key, op} in registration order.
///
/// Test-support surface: the journal behind it is only recorded when
/// `frogdb-core`'s `wait-queue-log` feature is on (the server's `turmoil`
/// feature turns it on for simulation tests), so a production build always
/// reports empty journals.
fn format_waitqueue_log_response(infos: Vec<frogdb_core::shard::WaitQueueLogInfo>) -> Response {
    let mut shards = Vec::new();
    for info in infos {
        let entries = Response::Array(
            info.entries
                .iter()
                .map(|e| {
                    Response::Map(vec![
                        (
                            Response::bulk("registration_seq"),
                            Response::Integer(e.registration_seq as i64),
                        ),
                        (
                            Response::bulk("conn_id"),
                            Response::Integer(e.conn_id as i64),
                        ),
                        (Response::bulk("key"), Response::bulk(e.key.clone())),
                        (Response::bulk("op"), Response::bulk(e.op.clone())),
                    ])
                })
                .collect(),
        );
        shards.push((
            Response::bulk(format!("shard:{}", info.shard_id)),
            Response::Map(vec![
                (
                    Response::bulk("truncated"),
                    Response::Integer(i64::from(info.truncated)),
                ),
                (Response::bulk("registrations"), entries),
            ]),
        ));
    }
    Response::Map(shards)
}

/// Format `DEBUG MEMORY-CHECK` — RESP map of `shard:<id>` → {tracked, recomputed,
/// diff, consistent}. `diff` is recomputed − tracked (may be negative).
fn format_memory_check_response(infos: Vec<frogdb_core::shard::MemoryCheckInfo>) -> Response {
    let mut shards = Vec::new();
    for info in infos {
        let diff = info.recomputed_bytes as i64 - info.tracked_bytes as i64;
        shards.push((
            Response::bulk(format!("shard:{}", info.shard_id)),
            Response::Map(vec![
                (
                    Response::bulk("tracked_bytes"),
                    Response::Integer(info.tracked_bytes as i64),
                ),
                (
                    Response::bulk("recomputed_bytes"),
                    Response::Integer(info.recomputed_bytes as i64),
                ),
                (Response::bulk("diff"), Response::Integer(diff)),
                (
                    Response::bulk("consistent"),
                    Response::Integer(i64::from(diff == 0)),
                ),
            ]),
        ));
    }
    Response::Map(shards)
}

/// Format `DEBUG EXPIRY-INDEX-CHECK` — sentinel when every shard is clean, else
/// a RESP map of `shard:<id>` → {total_entries, anomalies:[{key, kind}]}.
fn format_expiry_index_check_response(
    infos: Vec<frogdb_core::shard::ExpiryIndexCheckInfo>,
) -> Response {
    if infos.iter().all(|i| i.anomalies.is_empty()) {
        return Response::Bulk(Some(Bytes::from("# expiry index is consistent")));
    }
    let mut shards = Vec::new();
    for info in infos {
        let anomalies = Response::Array(
            info.anomalies
                .iter()
                .map(|a| {
                    Response::Map(vec![
                        (Response::bulk("key"), Response::bulk(a.key.clone())),
                        (
                            Response::bulk("kind"),
                            Response::bulk(format!("{:?}", a.kind)),
                        ),
                    ])
                })
                .collect(),
        );
        shards.push((
            Response::bulk(format!("shard:{}", info.shard_id)),
            Response::Map(vec![
                (
                    Response::bulk("total_entries"),
                    Response::Integer(info.total_entries as i64),
                ),
                (Response::bulk("anomalies"), anomalies),
            ]),
        ));
    }
    Response::Map(shards)
}

/// Format an invariant-catalog check (`DEBUG CLUSTER CHECK`,
/// `DEBUG REPLICATION CHECK`) — a RESP array of `{id, detail}` maps, one per
/// catalog violation of every tier, empty when the state is clean.
///
/// `None` means the catalog is not applicable to this node, and becomes
/// `not_applicable` — never a silently-empty array, which would read as
/// "clean". The two catalogs reach that case very differently, which is why
/// the wording is the caller's: the cluster catalog has nothing to check in
/// standalone mode, while the replication catalog answers in *every* mode and
/// only goes absent on a build with no replication seams wired at all.
fn format_check_response(
    violations: Option<Vec<frogdb_core::Violation>>,
    not_applicable: &'static str,
) -> Response {
    match violations {
        None => Response::error(not_applicable),
        Some(violations) => Response::Array(
            violations
                .into_iter()
                .map(|v| {
                    Response::Map(vec![
                        (Response::bulk("id"), Response::bulk(v.id)),
                        (Response::bulk("detail"), Response::bulk(v.detail)),
                    ])
                })
                .collect(),
        ),
    }
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

        // EXPIRE-BACKDATE's key is also its second argument.
        let backdate = [
            Bytes::from_static(b"EXPIRE-BACKDATE"),
            Bytes::from_static(b"mykey"),
            Bytes::from_static(b"50"),
        ];
        assert_eq!(
            DebugConnCommand.dynamic_keys(&backdate),
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
        cluster_check: Option<Vec<frogdb_core::Violation>>,
        replication_check: Option<Vec<frogdb_core::Violation>>,
        object_info: Option<frogdb_core::shard::ObjectInfo>,
        shard_arenas: Vec<(usize, u32)>,
        re_encode: Option<frogdb_core::store::ReEncodeResult>,
    }

    impl StubDebug {
        fn new(enabled: bool) -> Self {
            Self {
                enabled,
                cluster_check: Some(Vec::new()),
                replication_check: Some(Vec::new()),
                object_info: None,
                shard_arenas: Vec::new(),
                re_encode: None,
            }
        }

        fn with_re_encode(mut self, re_encode: frogdb_core::store::ReEncodeResult) -> Self {
            self.re_encode = Some(re_encode);
            self
        }

        fn with_shard_arenas(mut self, shard_arenas: Vec<(usize, u32)>) -> Self {
            self.shard_arenas = shard_arenas;
            self
        }

        fn with_object_info(mut self, object_info: Option<frogdb_core::shard::ObjectInfo>) -> Self {
            self.object_info = object_info;
            self
        }

        fn with_cluster_check(
            mut self,
            cluster_check: Option<Vec<frogdb_core::Violation>>,
        ) -> Self {
            self.cluster_check = cluster_check;
            self
        }

        fn with_replication_check(
            mut self,
            replication_check: Option<Vec<frogdb_core::Violation>>,
        ) -> Self {
            self.replication_check = replication_check;
            self
        }
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
        fn gather_wait_queue<'a>(
            &'a self,
        ) -> BoxFuture<'a, Vec<frogdb_core::shard::WaitQueueInfo>> {
            Box::pin(async { Vec::new() })
        }
        fn gather_wait_queue_log<'a>(
            &'a self,
        ) -> BoxFuture<'a, Vec<frogdb_core::shard::WaitQueueLogInfo>> {
            Box::pin(async { Vec::new() })
        }
        fn memory_check<'a>(&'a self) -> BoxFuture<'a, Vec<frogdb_core::shard::MemoryCheckInfo>> {
            Box::pin(async { Vec::new() })
        }
        fn expiry_index_check<'a>(
            &'a self,
        ) -> BoxFuture<'a, Vec<frogdb_core::shard::ExpiryIndexCheckInfo>> {
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
        fn expire_backdate<'a>(
            &'a self,
            _shard_id: usize,
            _key: Bytes,
            _ms: u64,
        ) -> BoxFuture<'a, Response> {
            Box::pin(async { Response::ok() })
        }
        fn object_info<'a>(
            &'a self,
            _shard_id: usize,
            _key: Bytes,
        ) -> BoxFuture<'a, Result<Option<frogdb_core::shard::ObjectInfo>, Response>> {
            Box::pin(async move { Ok(self.object_info.clone()) })
        }
        fn keysizes_snapshot<'a>(&'a self) -> BoxFuture<'a, KeysizeHistograms> {
            Box::pin(async { KeysizeHistograms::new() })
        }
        fn allocsize_in_slot<'a>(&'a self, _slot: u16) -> BoxFuture<'a, usize> {
            Box::pin(async { 0 })
        }
        fn cluster_check(&self) -> Option<Vec<frogdb_core::Violation>> {
            self.cluster_check.clone()
        }
        fn replication_check(&self) -> Option<Vec<frogdb_core::Violation>> {
            self.replication_check.clone()
        }
        fn shard_arenas(&self) -> Vec<(usize, u32)> {
            self.shard_arenas.clone()
        }
        fn re_encode<'a>(
            &'a self,
            _shard_id: usize,
            _key: Bytes,
        ) -> BoxFuture<'a, Result<Option<frogdb_core::store::ReEncodeResult>, Response>> {
            let result = self.re_encode;
            Box::pin(async move { Ok(result) })
        }
    }

    #[test]
    fn decay_ms_accepts_never_and_rejects_below_it() {
        assert_eq!(parse_decay_ms(&arg("0")), Some(0));
        assert_eq!(parse_decay_ms(&arg("10000")), Some(10_000));
        // -1 is jemalloc's "never decay"; -2 is not a decay value at all.
        assert_eq!(parse_decay_ms(&arg("-1")), Some(-1));
        assert_eq!(parse_decay_ms(&arg("-2")), None);
        assert_eq!(parse_decay_ms(&arg("soon")), None);
    }

    #[tokio::test]
    async fn arena_decay_rejects_a_wrong_argument_count() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("ARENA-DECAY"), arg("10")])
            .await;
        assert!(matches!(resp, Response::Error(_)), "{resp:?}");
    }

    #[tokio::test]
    async fn arena_decay_rejects_a_millisecond_value_below_never() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx(Some(&stub)),
                &[arg("ARENA-DECAY"), arg("-2"), arg("0")],
            )
            .await;
        assert!(matches!(resp, Response::Error(_)), "{resp:?}");
    }

    /// The report names every shard arena, alongside the startup setting the
    /// runtime values can now diverge from.
    #[cfg(not(target_env = "msvc"))]
    #[tokio::test]
    async fn arena_decay_reports_the_startup_setting_and_every_shard_arena() {
        let arena = frogdb_telemetry::jemalloc::create_arena().expect("arenas.create");
        let stub = StubDebug::new(true).with_shard_arenas(vec![(0, arena)]);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("ARENA-DECAY")])
            .await;
        let Response::Bulk(Some(body)) = resp else {
            panic!("expected a bulk report, got {resp:?}");
        };
        let body = String::from_utf8(body.to_vec()).expect("utf8");
        assert!(body.contains("startup:dirty="), "{body}");
        assert!(
            body.contains(&format!("shard0:arena={arena},dirty=")),
            "{body}"
        );
    }

    /// The retune reaches the allocator: a set is visible in the next report.
    #[cfg(not(target_env = "msvc"))]
    #[tokio::test]
    async fn arena_decay_retunes_every_arena_at_runtime() {
        let arena = frogdb_telemetry::jemalloc::create_arena().expect("arenas.create");
        let restore = frogdb_telemetry::jemalloc::read_arena_decay(arena).expect("decay");
        let stub = StubDebug::new(true).with_shard_arenas(vec![(0, arena)]);
        let fx = super::tests_fixture::Deps::new();

        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx(Some(&stub)),
                &[arg("ARENA-DECAY"), arg("1234"), arg("-1")],
            )
            .await;
        assert_eq!(resp, Response::ok(), "{resp:?}");
        assert_eq!(
            frogdb_telemetry::jemalloc::read_arena_decay(arena),
            Some(frogdb_telemetry::jemalloc::ArenaDecay {
                dirty_ms: 1234,
                muzzy_ms: -1,
            })
        );

        frogdb_telemetry::jemalloc::set_arena_decay(arena, restore)
            .expect("restore this arena's decay");
    }

    /// A node with no bound arenas is told the setting reached nothing, rather
    /// than given an `+OK` for a retune that did not happen.
    #[tokio::test]
    async fn arena_decay_refuses_to_retune_when_no_shard_owns_an_arena() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx(Some(&stub)),
                &[arg("ARENA-DECAY"), arg("0"), arg("0")],
            )
            .await;
        assert!(matches!(resp, Response::Error(_)), "{resp:?}");
    }

    #[tokio::test]
    async fn sleep_disabled_reports_error() {
        let stub = StubDebug::new(false);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("SLEEP"), arg("0")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn unknown_subcommand_errors() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("NOPE")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn expire_backdate_dispatches_to_provider_on_valid_args() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx_with_num_shards(Some(&stub), 1),
                &[arg("EXPIRE-BACKDATE"), arg("k"), arg("50")],
            )
            .await;
        // The stub provider replies +OK for a well-formed request.
        assert!(matches!(resp, Response::Simple(_)), "got {resp:?}");
    }

    #[tokio::test]
    async fn expire_backdate_rejects_bad_arity_and_ms() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();

        // Missing the ms argument.
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx(Some(&stub)),
                &[arg("EXPIRE-BACKDATE"), arg("k")],
            )
            .await;
        assert!(matches!(resp, Response::Error(_)), "got {resp:?}");

        // Non-integer ms.
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx(Some(&stub)),
                &[arg("EXPIRE-BACKDATE"), arg("k"), arg("soon")],
            )
            .await;
        assert!(matches!(resp, Response::Error(_)), "got {resp:?}");

        // Negative ms is not a u64.
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx(Some(&stub)),
                &[arg("EXPIRE-BACKDATE"), arg("k"), arg("-5")],
            )
            .await;
        assert!(matches!(resp, Response::Error(_)), "got {resp:?}");
    }

    #[tokio::test]
    async fn re_encode_reports_the_encoding_and_the_memory_on_either_side() {
        let stub = StubDebug::new(true).with_re_encode(frogdb_core::store::ReEncodeResult {
            re_encoded: true,
            encoding: "listpack",
            before_bytes: 4_096,
            after_bytes: 96,
        });
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx_with_num_shards(Some(&stub), 1),
                &[arg("RE-ENCODE"), arg("h")],
            )
            .await;
        match resp {
            Response::Simple(s) => assert_eq!(
                String::from_utf8_lossy(s.as_bytes()),
                "re_encoded:1 encoding:listpack memory_before:4096 memory_after:96"
            ),
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn re_encode_reports_no_such_key_when_the_shard_has_none() {
        // The stub's default is a miss: the provider found no such key.
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx_with_num_shards(Some(&stub), 1),
                &[arg("RE-ENCODE"), arg("absent")],
            )
            .await;
        match resp {
            Response::Error(e) => assert_eq!(String::from_utf8_lossy(&e), "ERR no such key"),
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn re_encode_rejects_a_wrong_argument_count() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();
        for args in [
            vec![arg("RE-ENCODE")],
            vec![arg("RE-ENCODE"), arg("a"), arg("b")],
        ] {
            let resp = DebugConnCommand
                .execute(&mut fx.ctx_with_num_shards(Some(&stub), 1), &args)
                .await;
            assert!(matches!(resp, Response::Error(_)), "got {resp:?}");
        }
    }

    #[tokio::test]
    async fn dangerous_subcommand_is_unsupported() {
        let stub = StubDebug::new(true);
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

    #[tokio::test]
    async fn cluster_check_reports_an_empty_array_when_clean() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("CLUSTER"), arg("CHECK")])
            .await;
        assert_eq!(resp, Response::Array(vec![]), "got {resp:?}");
    }

    #[tokio::test]
    async fn cluster_check_reports_violations_as_id_detail_maps() {
        let stub = StubDebug::new(true).with_cluster_check(Some(vec![frogdb_core::Violation {
            id: "INV-REF-1",
            detail: "slot 200 owned by unknown node 404".to_string(),
        }]));
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("CLUSTER"), arg("CHECK")])
            .await;
        assert_eq!(
            resp,
            Response::Array(vec![Response::Map(vec![
                (Response::bulk("id"), Response::bulk("INV-REF-1")),
                (
                    Response::bulk("detail"),
                    Response::bulk("slot 200 owned by unknown node 404")
                ),
            ])]),
            "got {resp:?}"
        );
    }

    #[tokio::test]
    async fn cluster_check_reports_cluster_disabled_error_in_standalone_mode() {
        let stub = StubDebug::new(true).with_cluster_check(None);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("CLUSTER"), arg("CHECK")])
            .await;
        match resp {
            Response::Error(e) => assert!(
                String::from_utf8_lossy(&e).contains("cluster support disabled"),
                "got {e:?}"
            ),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn replication_check_reports_an_empty_array_when_clean() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx(Some(&stub)),
                &[arg("REPLICATION"), arg("CHECK")],
            )
            .await;
        assert_eq!(resp, Response::Array(vec![]), "got {resp:?}");
    }

    #[tokio::test]
    async fn replication_check_reports_violations_as_id_detail_maps() {
        let stub =
            StubDebug::new(true).with_replication_check(Some(vec![frogdb_core::Violation {
                id: "INV-OFFSET-1",
                detail: "landed 9 runs ahead of applied 7".to_string(),
            }]));
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx(Some(&stub)),
                &[arg("REPLICATION"), arg("CHECK")],
            )
            .await;
        assert_eq!(
            resp,
            Response::Array(vec![Response::Map(vec![
                (Response::bulk("id"), Response::bulk("INV-OFFSET-1")),
                (
                    Response::bulk("detail"),
                    Response::bulk("landed 9 runs ahead of applied 7")
                ),
            ])]),
            "got {resp:?}"
        );
    }

    /// The absent case is "no replication seams wired at all", never
    /// "standalone" — but when it does happen it must not answer with an empty
    /// array, which reads as clean.
    #[tokio::test]
    async fn replication_check_reports_replication_disabled_error_when_unwired() {
        let stub = StubDebug::new(true).with_replication_check(None);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx(Some(&stub)),
                &[arg("REPLICATION"), arg("CHECK")],
            )
            .await;
        match resp {
            Response::Error(e) => assert!(
                String::from_utf8_lossy(&e).contains("replication support disabled"),
                "got {e:?}"
            ),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn replication_unknown_subcommand_errors() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("REPLICATION"), arg("NOPE")])
            .await;
        assert!(matches!(resp, Response::Error(_)), "got {resp:?}");
    }

    #[tokio::test]
    async fn cluster_unknown_subcommand_errors() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(&mut fx.ctx(Some(&stub)), &[arg("CLUSTER"), arg("NOPE")])
            .await;
        assert!(matches!(resp, Response::Error(_)), "got {resp:?}");
    }

    fn sample_object_info() -> frogdb_core::shard::ObjectInfo {
        frogdb_core::shard::ObjectInfo {
            refcount: 1,
            encoding: "listpack",
            serialized_length: 42,
            lru: 1_700_000_000,
            lru_seconds_idle: 7,
        }
    }

    /// The reply is a status line of `token:value` pairs, and it carries exactly
    /// the truthful tokens — no `at:` heap pointer, no fabricated `ql_*`
    /// quicklist counters.
    #[tokio::test]
    async fn object_reports_truthful_tokens_only() {
        let stub = StubDebug::new(true).with_object_info(Some(sample_object_info()));
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx_with_num_shards(Some(&stub), 1),
                &[arg("OBJECT"), arg("k")],
            )
            .await;

        let line = match resp {
            Response::Simple(s) => String::from_utf8(s.to_vec()).unwrap(),
            other => panic!("expected a status line, got {other:?}"),
        };
        let tokens: Vec<&str> = line.split(' ').collect();
        assert_eq!(
            tokens,
            vec![
                "refcount:1",
                "encoding:listpack",
                "serializedlength:42",
                "lru:1700000000",
                "lru_seconds_idle:7",
            ],
            "got {line}"
        );
    }

    /// A key the owning shard cannot find answers with Redis's own error, not an
    /// empty or zeroed line.
    #[tokio::test]
    async fn object_missing_key_reports_no_such_key() {
        let stub = StubDebug::new(true).with_object_info(None);
        let fx = super::tests_fixture::Deps::new();
        let resp = DebugConnCommand
            .execute(
                &mut fx.ctx_with_num_shards(Some(&stub), 1),
                &[arg("OBJECT"), arg("absent")],
            )
            .await;
        match resp {
            Response::Error(e) => assert_eq!(&e[..], b"ERR no such key"),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn object_rejects_bad_arity() {
        let stub = StubDebug::new(true).with_object_info(Some(sample_object_info()));
        let fx = super::tests_fixture::Deps::new();
        for args in [
            vec![arg("OBJECT")],
            vec![arg("OBJECT"), arg("k"), arg("extra")],
        ] {
            let resp = DebugConnCommand
                .execute(&mut fx.ctx_with_num_shards(Some(&stub), 1), &args)
                .await;
            assert!(matches!(resp, Response::Error(_)), "got {resp:?}");
        }
    }

    /// The exact reply the dispatch's catch-all produces for `subcommand`.
    fn unknown_subcommand_error(subcommand: &str) -> Response {
        Response::error(format!("ERR Unknown DEBUG subcommand '{subcommand}'"))
    }

    /// Every subcommand whose key `dynamic_keys` declares must have a dispatch
    /// arm.
    ///
    /// This is the container-wide gate for the class of bug this test was born
    /// from: `DEBUG OBJECT` was declared keyed — so `COMMAND GETKEYS DEBUG
    /// OBJECT k` reported `k`, and a cluster-aware client routed to `k`'s slot
    /// owner — while dispatch answered `ERR Unknown DEBUG subcommand 'OBJECT'`.
    /// Adding a name to [`KEYED_SUBCOMMANDS`] without an arm in `execute` fails
    /// here.
    #[tokio::test]
    async fn every_keyed_subcommand_is_dispatched() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();

        for subcommand in KEYED_SUBCOMMANDS {
            let name = String::from_utf8(subcommand.to_vec()).unwrap();

            // The declaration half: the key is the second argument.
            let args = [Bytes::from(name.clone()), Bytes::from_static(b"k")];
            assert_eq!(
                DebugConnCommand.dynamic_keys(&args),
                vec![b"k".as_slice()],
                "DEBUG {name} is listed as keyed but dynamic_keys does not extract its key",
            );

            // The dispatch half: whatever it answers, it must not be the
            // unknown-subcommand catch-all.
            let resp = DebugConnCommand
                .execute(&mut fx.ctx_with_num_shards(Some(&stub), 1), &args)
                .await;
            assert_ne!(
                resp,
                unknown_subcommand_error(&name),
                "DEBUG {name} declares a key via dynamic_keys but dispatch rejects it",
            );
        }
    }

    /// Every subcommand `DEBUG HELP` advertises must have a dispatch arm.
    ///
    /// The wider half of the same gate: `HELP` is a documented promise to
    /// operators, so a name that appears there and nowhere in the dispatch is
    /// the same lie in a different place. The subcommand may still reject the
    /// probe (wrong arity, missing nested subcommand, unsafe-and-unsupported) —
    /// only the catch-all is disqualifying.
    #[tokio::test]
    async fn every_help_advertised_subcommand_is_dispatched() {
        let stub = StubDebug::new(true);
        let fx = super::tests_fixture::Deps::new();

        let Response::Array(lines) = debug_help() else {
            panic!("DEBUG HELP must reply with an array");
        };
        let mut checked = 0;
        for line in lines {
            let Response::Bulk(Some(text)) = line else {
                panic!("DEBUG HELP lines are bulk strings");
            };
            let text = String::from_utf8(text.to_vec()).unwrap();
            // Usage lines start with "DEBUG <SUBCOMMAND>"; the indented lines
            // between them are prose.
            let Some(rest) = text.strip_prefix("DEBUG ") else {
                continue;
            };
            let name = rest.split(' ').next().unwrap().to_string();
            checked += 1;

            let resp = DebugConnCommand
                .execute(
                    &mut fx.ctx_with_num_shards(Some(&stub), 1),
                    &[Bytes::from(name.clone())],
                )
                .await;
            assert_ne!(
                resp,
                unknown_subcommand_error(&name),
                "DEBUG HELP advertises {name} but dispatch does not accept it",
            );
        }
        assert!(checked > 10, "expected HELP to advertise the whole surface");
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
            self.ctx_with_num_shards(debug, 0)
        }

        /// Like [`Deps::ctx`] but with an explicit `num_shards`, for keyed
        /// subcommands (e.g. EXPIRE-BACKDATE) whose executor maps the key to a
        /// shard via `shard_for_key` and would divide by a zero shard count.
        pub(super) fn ctx_with_num_shards<'a>(
            &'a self,
            debug: Option<&'a dyn DebugProvider>,
            num_shards: usize,
        ) -> ConnCtx<'a> {
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
                num_shards,
                10000,
                false,
            )
            .with_username("default");
            ctx.debug = debug;
            ctx
        }
    }
}
