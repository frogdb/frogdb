//! Observability read commands (SLOWLOG, MEMORY, LATENCY, STATUS).
//!
//! These are migrated behind the [`ConnectionCommand`] seam (see
//! [`crate::connection::conn_command`] and the CONFIG executor there for the
//! template). Each reads only the observability subsystems it needs through
//! [`ConnCtx`] — `shard_senders` for scatter-gather, `metrics_recorder` for SLO
//! latency bands, `memory_diag` for MEMORY DOCTOR, `status` for STATUS JSON —
//! instead of taking `&ConnectionHandler`, so each is unit-testable in isolation
//! (see `tests`).
//!
//! These are read/aggregate commands with no connection-state mutation; MEMORY
//! broadcasts to shards, and STATUS JSON renders from the shared status collector
//! (see [`crate::connection::status_handler`]) — the same source the HTTP
//! `/status` endpoint uses. Each is its own [`ConnectionCommand`] with its own
//! [`CommandSpec`], registered separately via
//! [`frogdb_core::CommandRegistry::register_connection`].

use std::collections::HashMap;

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, CommandFlags, CommandSpec, ConnCtx, ConnectionCommand,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LatencyEvent, LatencySample,
    LookupSpec, MemoryDiagProvider, ObservabilityMsg, ShardMemoryStats, ShardSender, WaiterWake,
    WalStrategy, generate_latency_graph, shard_for_key,
};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::scatter::{DEFAULT_SCATTER_GATHER_TIMEOUT, ScatterGather};

/// Adapts the server's [`frogdb_debug::MemoryDiagConfig`] to the core
/// [`MemoryDiagProvider`] seam so the MEMORY executor can produce a DOCTOR report
/// behind a `ConnCtx` without the seam naming any `frogdb_debug` type. The
/// `ConnectionHandler` stores one of these and exposes it via
/// [`ConnCtx::memory_diag`].
pub(crate) struct MemoryDiag(pub(crate) frogdb_debug::MemoryDiagConfig);

impl MemoryDiagProvider for MemoryDiag {
    fn doctor_report<'a>(&'a self, shard_senders: &'a [ShardSender]) -> BoxFuture<'a, String> {
        Box::pin(async move {
            let collector = frogdb_debug::MemoryDiagCollector::new(
                std::sync::Arc::new(shard_senders.to_vec()),
                self.0.clone(),
            );
            let report = collector.collect().await;
            frogdb_debug::format_memory_report(&report)
        })
    }
}

// =============================================================================
// Shared shard-aggregation helpers (were pub(crate) methods on ConnectionHandler)
// =============================================================================

/// Gather memory stats from all shards. Used by MEMORY STATS.
async fn gather_memory_stats(shard_senders: &[ShardSender]) -> Vec<ShardMemoryStats> {
    ScatterGather::new(shard_senders, DEFAULT_SCATTER_GATHER_TIMEOUT, 0)
        .gather_all(|_shard, response_tx| ObservabilityMsg::MemoryStats { response_tx })
        .await
}

/// Gather latest latency samples from all shards.
async fn gather_latency_latest(
    shard_senders: &[ShardSender],
) -> Vec<(LatencyEvent, LatencySample)> {
    let mut latest_by_event: HashMap<LatencyEvent, LatencySample> = HashMap::new();

    let per_shard = ScatterGather::new(shard_senders, DEFAULT_SCATTER_GATHER_TIMEOUT, 0)
        .gather_all(|_shard, response_tx| ObservabilityMsg::LatencyLatest { response_tx })
        .await;
    for samples in per_shard {
        for (event, sample) in samples {
            // Keep the most recent sample for each event
            latest_by_event
                .entry(event)
                .and_modify(|existing| {
                    if sample.timestamp > existing.timestamp {
                        *existing = sample;
                    }
                })
                .or_insert(sample);
        }
    }

    latest_by_event.into_iter().collect()
}

/// Gather latency history for a specific event from all shards.
async fn gather_latency_history(
    shard_senders: &[ShardSender],
    event: LatencyEvent,
) -> Vec<LatencySample> {
    let mut all_samples: Vec<LatencySample> =
        ScatterGather::new(shard_senders, DEFAULT_SCATTER_GATHER_TIMEOUT, 0)
            .gather_all(|_shard, response_tx| ObservabilityMsg::LatencyHistory {
                event,
                response_tx,
            })
            .await
            .into_iter()
            .flatten()
            .collect();

    // Sort by timestamp (newest first)
    all_samples.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    all_samples
}

// =============================================================================
// SLOWLOG
// =============================================================================

/// The `CommandSpec` for SLOWLOG. Declared here alongside the executor so the
/// connection command is a single self-contained unit. Strategy is
/// `ConnectionLevel(Admin)`; the registry validates that this agrees with the
/// `Connection` executor variant.
static SLOWLOG_SPEC: CommandSpec = CommandSpec {
    name: "SLOWLOG",
    arity: Arity::AtLeast(1),
    flags: CommandFlags::READONLY
        .union(CommandFlags::ADMIN)
        .union(CommandFlags::FAST)
        .union(CommandFlags::SKIP_SLOWLOG)
        .union(CommandFlags::LOADING),
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    mutation: frogdb_core::ConnMutation::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` SLOWLOG executor.
pub(crate) static SLOWLOG_CONN_COMMAND: SlowlogConnCommand = SlowlogConnCommand;

/// SLOWLOG — slow query log management (GET/LEN/RESET/HELP).
pub(crate) struct SlowlogConnCommand;

impl ConnectionCommand for SlowlogConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &SLOWLOG_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            if args.is_empty() {
                return Response::error("ERR wrong number of arguments for 'slowlog' command");
            }

            let subcommand = args[0].to_ascii_uppercase();
            let subcommand_str = String::from_utf8_lossy(&subcommand);

            match subcommand_str.as_ref() {
                "GET" => slowlog_get(ctx, &args[1..]).await,
                "LEN" => slowlog_len(ctx).await,
                "RESET" => slowlog_reset(ctx).await,
                "HELP" => slowlog_help(),
                _ => Response::error(format!(
                    "ERR unknown subcommand '{}'. Try SLOWLOG HELP.",
                    subcommand_str
                )),
            }
        })
    }
}

/// SLOWLOG GET [count] — get recent slow queries.
async fn slowlog_get(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    // Default count is 10, like Redis
    let count: usize = if args.is_empty() {
        10
    } else {
        match String::from_utf8_lossy(&args[0]).parse::<i64>() {
            Ok(n) if n >= -1 => {
                if n == -1 {
                    usize::MAX // -1 means all entries
                } else {
                    n as usize
                }
            }
            Ok(_) => return Response::error("ERR count should be greater than or equal to -1"),
            Err(_) => return Response::error("ERR value is not an integer or out of range"),
        }
    };

    // Scatter-gather: collect from all shards
    let mut all_entries: Vec<_> =
        ScatterGather::new(ctx.shard_senders, DEFAULT_SCATTER_GATHER_TIMEOUT, 0)
            .gather_all(|_shard, response_tx| ObservabilityMsg::SlowlogGet { count, response_tx })
            .await
            .into_iter()
            .flatten()
            .collect();

    // Sort by ID descending (newest first) and limit to count
    all_entries.sort_by(|a, b| b.id.cmp(&a.id));
    all_entries.truncate(count);

    // Convert to Redis response format
    let entries: Vec<Response> = all_entries
        .into_iter()
        .map(|entry| {
            let args: Vec<Response> = entry.command.into_iter().map(Response::bulk).collect();

            Response::Array(vec![
                Response::Integer(entry.id as i64),
                Response::Integer(entry.timestamp),
                Response::Integer(entry.duration_us as i64),
                Response::Array(args),
                Response::bulk(Bytes::from(entry.client_addr)),
                Response::bulk(Bytes::from(entry.client_name)),
            ])
        })
        .collect();

    Response::Array(entries)
}

/// SLOWLOG LEN — get total number of entries across all shards.
async fn slowlog_len(ctx: &ConnCtx<'_>) -> Response {
    let total_len: usize = ScatterGather::new(ctx.shard_senders, DEFAULT_SCATTER_GATHER_TIMEOUT, 0)
        .gather_all(|_shard, response_tx| ObservabilityMsg::SlowlogLen { response_tx })
        .await
        .into_iter()
        .sum();

    Response::Integer(total_len as i64)
}

/// SLOWLOG RESET — clear all slow query logs.
async fn slowlog_reset(ctx: &ConnCtx<'_>) -> Response {
    // Await-and-discard: the replies are only a barrier confirming every shard
    // cleared its log. Bounded by the shared deadline (was unbounded).
    let _ = ScatterGather::new(ctx.shard_senders, DEFAULT_SCATTER_GATHER_TIMEOUT, 0)
        .gather_all(|_shard, response_tx| ObservabilityMsg::SlowlogReset { response_tx })
        .await;

    Response::ok()
}

/// SLOWLOG HELP — show help text.
fn slowlog_help() -> Response {
    let help = vec![
        Response::bulk(Bytes::from_static(
            b"SLOWLOG <subcommand> [<arg> ...]. Subcommands are:",
        )),
        Response::bulk(Bytes::from_static(b"GET [<count>]")),
        Response::bulk(Bytes::from_static(
            b"    Return top <count> entries from the slowlog (default 10).",
        )),
        Response::bulk(Bytes::from_static(b"    Entries are made of:")),
        Response::bulk(Bytes::from_static(
            b"    id, timestamp, time in microseconds, arguments array, client address, client name",
        )),
        Response::bulk(Bytes::from_static(b"LEN")),
        Response::bulk(Bytes::from_static(
            b"    Return the number of entries in the slowlog.",
        )),
        Response::bulk(Bytes::from_static(b"RESET")),
        Response::bulk(Bytes::from_static(b"    Reset the slowlog.")),
        Response::bulk(Bytes::from_static(b"HELP")),
        Response::bulk(Bytes::from_static(b"    Print this help.")),
    ];
    Response::Array(help)
}

// =============================================================================
// MEMORY
// =============================================================================

/// The `CommandSpec` for MEMORY.
static MEMORY_SPEC: CommandSpec = CommandSpec {
    name: "MEMORY",
    arity: Arity::AtLeast(1),
    flags: CommandFlags::READONLY.union(CommandFlags::RANDOM),
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    mutation: frogdb_core::ConnMutation::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` MEMORY executor.
pub(crate) static MEMORY_CONN_COMMAND: MemoryConnCommand = MemoryConnCommand;

/// MEMORY — memory introspection and diagnostics
/// (DOCTOR/HELP/MALLOC-SIZE/PURGE/STATS/USAGE).
pub(crate) struct MemoryConnCommand;

impl ConnectionCommand for MemoryConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &MEMORY_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            if args.is_empty() {
                return Response::error("ERR wrong number of arguments for 'memory' command");
            }

            let subcommand = args[0].to_ascii_uppercase();
            let subcommand_str = String::from_utf8_lossy(&subcommand);

            match subcommand_str.as_ref() {
                "DOCTOR" => memory_doctor(ctx).await,
                "HELP" => memory_help(),
                "MALLOC-SIZE" => memory_malloc_size(&args[1..]),
                "PURGE" => memory_purge(),
                "STATS" => memory_stats(ctx).await,
                "USAGE" => memory_usage(ctx, &args[1..]).await,
                _ => Response::error(format!(
                    "ERR unknown subcommand '{}'. Try MEMORY HELP.",
                    subcommand_str
                )),
            }
        })
    }
}

/// MEMORY DOCTOR — diagnose memory issues.
async fn memory_doctor(ctx: &ConnCtx<'_>) -> Response {
    let formatted = ctx.memory_diag.doctor_report(ctx.shard_senders).await;
    Response::bulk(Bytes::from(formatted))
}

/// MEMORY HELP — show help text.
fn memory_help() -> Response {
    let help = vec![
        Response::bulk(Bytes::from_static(
            b"MEMORY <subcommand> [<arg> ...]. Subcommands are:",
        )),
        Response::bulk(Bytes::from_static(b"DOCTOR")),
        Response::bulk(Bytes::from_static(b"    Return memory problems reports.")),
        Response::bulk(Bytes::from_static(b"HELP")),
        Response::bulk(Bytes::from_static(b"    Print this help.")),
        Response::bulk(Bytes::from_static(b"MALLOC-SIZE <size>")),
        Response::bulk(Bytes::from_static(
            b"    Return the allocator usable size for the given input size.",
        )),
        Response::bulk(Bytes::from_static(b"PURGE")),
        Response::bulk(Bytes::from_static(
            b"    Attempt to release memory back to the OS.",
        )),
        Response::bulk(Bytes::from_static(b"STATS")),
        Response::bulk(Bytes::from_static(
            b"    Return information about memory usage.",
        )),
        Response::bulk(Bytes::from_static(b"USAGE <key> [SAMPLES <count>]")),
        Response::bulk(Bytes::from_static(
            b"    Return memory used by a key and its value.",
        )),
    ];
    Response::Array(help)
}

/// MEMORY MALLOC-SIZE <size> — get allocator usable size (stub).
fn memory_malloc_size(args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'memory|malloc-size' command");
    }

    // Parse the size argument
    match String::from_utf8_lossy(&args[0]).parse::<i64>() {
        Ok(size) => {
            // Without jemalloc, just return the input size
            // In a real implementation this would query the allocator
            Response::Integer(size)
        }
        Err(_) => Response::error("ERR value is not an integer or out of range"),
    }
}

/// MEMORY PURGE — force memory release (stub).
fn memory_purge() -> Response {
    // Without jemalloc, this is a no-op
    // In a real implementation this would call jemalloc_purge_arena or similar
    Response::ok()
}

/// MEMORY STATS — get detailed memory statistics.
async fn memory_stats(ctx: &ConnCtx<'_>) -> Response {
    let stats = gather_memory_stats(ctx.shard_senders).await;

    let total_data_memory: usize = stats.iter().map(|s| s.data_memory).sum();
    let total_keys: usize = stats.iter().map(|s| s.keys).sum();
    let total_overhead: usize = stats.iter().map(|s| s.overhead_estimate).sum();
    let peak_memory: u64 = stats.iter().map(|s| s.peak_memory).max().unwrap_or(0);
    let total_limit: u64 = stats.iter().map(|s| s.memory_limit).sum();

    // Build a flat array of key-value pairs (Redis MEMORY STATS format)
    let mut result = vec![
        Response::bulk(Bytes::from_static(b"peak.allocated")),
        Response::Integer(peak_memory as i64),
        Response::bulk(Bytes::from_static(b"total.allocated")),
        Response::Integer(total_data_memory as i64),
        Response::bulk(Bytes::from_static(b"startup.allocated")),
        Response::Integer(0), // We don't track startup memory separately
        Response::bulk(Bytes::from_static(b"replication.backlog")),
        Response::Integer(0), // No replication backlog yet
        Response::bulk(Bytes::from_static(b"clients.slaves")),
        Response::Integer(0), // No replica clients yet
        Response::bulk(Bytes::from_static(b"clients.normal")),
        Response::Integer(0), // Would need client tracking
        Response::bulk(Bytes::from_static(b"aof.buffer")),
        Response::Integer(0), // No AOF buffer
        Response::bulk(Bytes::from_static(b"overhead.total")),
        Response::Integer(total_overhead as i64),
        Response::bulk(Bytes::from_static(b"keys.count")),
        Response::Integer(total_keys as i64),
        Response::bulk(Bytes::from_static(b"keys.bytes-per-key")),
        Response::Integer(if total_keys > 0 {
            (total_data_memory / total_keys) as i64
        } else {
            0
        }),
        Response::bulk(Bytes::from_static(b"dataset.bytes")),
        Response::Integer(total_data_memory as i64),
        Response::bulk(Bytes::from_static(b"dataset.percentage")),
        Response::bulk(Bytes::from(if total_data_memory > 0 && total_limit > 0 {
            format!(
                "{:.2}",
                (total_data_memory as f64 / total_limit as f64) * 100.0
            )
        } else {
            "0.00".to_string()
        })),
        Response::bulk(Bytes::from_static(b"peak.percentage")),
        Response::bulk(Bytes::from(if peak_memory > 0 && total_limit > 0 {
            format!("{:.2}", (peak_memory as f64 / total_limit as f64) * 100.0)
        } else {
            "0.00".to_string()
        })),
    ];

    // Add per-shard breakdown
    result.push(Response::bulk(Bytes::from_static(b"db.0")));
    let db_stats = vec![
        Response::bulk(Bytes::from_static(b"overhead.hashtable.main")),
        Response::Integer(total_overhead as i64),
        Response::bulk(Bytes::from_static(b"overhead.hashtable.expires")),
        Response::Integer(0),
    ];
    result.push(Response::Array(db_stats));

    Response::Array(result)
}

/// MEMORY USAGE <key> [SAMPLES count] — get memory for a specific key.
async fn memory_usage(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'memory|usage' command");
    }

    let key = &args[0];
    let samples = if args.len() >= 3 && args[1].eq_ignore_ascii_case(b"SAMPLES") {
        match String::from_utf8_lossy(&args[2]).parse::<usize>() {
            Ok(n) => Some(n),
            Err(_) => return Response::error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };

    // Route to the shard that owns this key
    let shard_id = shard_for_key(key, ctx.shard_senders.len());
    let sender = &ctx.shard_senders[shard_id];

    let (response_tx, response_rx) = oneshot::channel();
    if sender
        .send(ObservabilityMsg::MemoryUsage {
            key: key.clone(),
            samples,
            response_tx,
        })
        .await
        .is_err()
    {
        return Response::error("ERR shard communication error");
    }

    match response_rx.await {
        Ok(Some(usage)) => Response::Integer(usage as i64),
        Ok(None) => Response::Null,
        Err(_) => Response::error("ERR shard response error"),
    }
}

// =============================================================================
// LATENCY
// =============================================================================

/// The `CommandSpec` for LATENCY.
static LATENCY_SPEC: CommandSpec = CommandSpec {
    name: "LATENCY",
    arity: Arity::AtLeast(1),
    flags: CommandFlags::ADMIN
        .union(CommandFlags::NOSCRIPT)
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    mutation: frogdb_core::ConnMutation::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` LATENCY executor.
pub(crate) static LATENCY_CONN_COMMAND: LatencyConnCommand = LatencyConnCommand;

/// LATENCY — latency monitoring and diagnostics
/// (BANDS/DOCTOR/GRAPH/HELP/HISTOGRAM/HISTORY/LATEST/RESET).
pub(crate) struct LatencyConnCommand;

impl ConnectionCommand for LatencyConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &LATENCY_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            if args.is_empty() {
                return Response::error("ERR wrong number of arguments for 'latency' command");
            }

            let subcommand = args[0].to_ascii_uppercase();
            let subcommand_str = String::from_utf8_lossy(&subcommand);

            match subcommand_str.as_ref() {
                "BANDS" => latency_bands(ctx, &args[1..]),
                "DOCTOR" => latency_doctor(ctx).await,
                "GRAPH" => latency_graph(ctx, &args[1..]).await,
                "HELP" => latency_help(),
                "HISTOGRAM" => latency_histogram(&args[1..]).await,
                "HISTORY" => latency_history(ctx, &args[1..]).await,
                "LATEST" => latency_latest(ctx).await,
                "RESET" => latency_reset(ctx, &args[1..]).await,
                _ => Response::error(format!(
                    "ERR unknown subcommand '{}'. Try LATENCY HELP.",
                    subcommand_str
                )),
            }
        })
    }
}

/// LATENCY BANDS [RESET] — show or reset latency band statistics.
fn latency_bands(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    let recorder = ctx.metrics_recorder;
    if !recorder.latency_bands_enabled() {
        return Response::error(
            "ERR latency bands not enabled. Set latency_bands.enabled = true in config.",
        );
    }

    // Handle RESET subcommand
    if !args.is_empty() {
        let subcommand = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        if subcommand == "RESET" {
            recorder.reset_latency_bands();
            return Response::ok();
        } else {
            return Response::error(format!(
                "ERR unknown option '{}'. Try LATENCY BANDS or LATENCY BANDS RESET.",
                subcommand
            ));
        }
    }

    // Build report showing band percentages
    let total = recorder.latency_band_total();
    let percentages = recorder.latency_band_percentages();

    let mut lines = vec![
        format!("Total requests: {}", total),
        String::new(),
        "Band            Count      Percentage".to_string(),
        "----            -----      ----------".to_string(),
    ];

    for (band, count, pct) in &percentages {
        lines.push(format!("{:<15} {:>10} {:>10.2}%", band, count, pct));
    }

    Response::bulk(Bytes::from(lines.join("\r\n")))
}

/// LATENCY DOCTOR — diagnose latency issues.
async fn latency_doctor(ctx: &ConnCtx<'_>) -> Response {
    let latest = gather_latency_latest(ctx.shard_senders).await;

    let mut report = Vec::new();
    report.push("I have a few latency reports to share:".to_string());

    if latest.is_empty() {
        report.push("* No latency events recorded yet.".to_string());
    } else {
        for (event, sample) in &latest {
            if sample.latency_ms > 100 {
                report.push(format!(
                    "* {} event at {} had HIGH latency of {}ms",
                    event.as_str(),
                    sample.timestamp,
                    sample.latency_ms
                ));
            } else if sample.latency_ms > 10 {
                report.push(format!(
                    "* {} event at {} had moderate latency of {}ms",
                    event.as_str(),
                    sample.timestamp,
                    sample.latency_ms
                ));
            }
        }

        if report.len() == 1 {
            report.push("* All recorded events have acceptable latency.".to_string());
        }
    }

    Response::bulk(Bytes::from(report.join("\n")))
}

/// LATENCY GRAPH <event> — show ASCII latency graph.
async fn latency_graph(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'latency|graph' command");
    }

    let event_str = String::from_utf8_lossy(&args[0]);
    let event = match LatencyEvent::from_str(&event_str) {
        Some(e) => e,
        None => {
            return Response::error(format!(
                "ERR Unknown event type: {}. Valid events: command, fork, aof-fsync, expire-cycle, eviction-cycle, snapshot-io",
                event_str
            ));
        }
    };

    let history = gather_latency_history(ctx.shard_senders, event).await;
    let graph = generate_latency_graph(event, &history);

    Response::bulk(Bytes::from(graph))
}

/// LATENCY HELP — show help text.
fn latency_help() -> Response {
    let help = vec![
        Response::bulk(Bytes::from_static(
            b"LATENCY <subcommand> [<arg> ...]. Subcommands are:",
        )),
        Response::bulk(Bytes::from_static(b"BANDS [RESET]")),
        Response::bulk(Bytes::from_static(
            b"    Show SLO latency band statistics, or reset counters.",
        )),
        Response::bulk(Bytes::from_static(b"DOCTOR")),
        Response::bulk(Bytes::from_static(b"    Return latency diagnostic report.")),
        Response::bulk(Bytes::from_static(b"GRAPH <event>")),
        Response::bulk(Bytes::from_static(
            b"    Return an ASCII art graph of latency for the event.",
        )),
        Response::bulk(Bytes::from_static(b"HELP")),
        Response::bulk(Bytes::from_static(b"    Print this help.")),
        Response::bulk(Bytes::from_static(b"HISTOGRAM [<command> ...]")),
        Response::bulk(Bytes::from_static(
            b"    Return a cumulative distribution of command latencies.",
        )),
        Response::bulk(Bytes::from_static(b"HISTORY <event>")),
        Response::bulk(Bytes::from_static(
            b"    Return timestamp-latency pairs for the event.",
        )),
        Response::bulk(Bytes::from_static(b"LATEST")),
        Response::bulk(Bytes::from_static(
            b"    Return the latest latency samples for all events.",
        )),
        Response::bulk(Bytes::from_static(b"RESET [<event> ...]")),
        Response::bulk(Bytes::from_static(
            b"    Reset latency data for specified events, or all if none given.",
        )),
    ];
    Response::Array(help)
}

/// LATENCY HISTOGRAM [command...] — show command latency histogram.
async fn latency_histogram(_args: &[Bytes]) -> Response {
    // This would require command-level latency tracking which is not yet implemented
    // Return an empty response for now
    Response::Array(vec![])
}

/// LATENCY HISTORY <event> — get historical latency data.
async fn latency_history(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'latency|history' command");
    }

    let event_str = String::from_utf8_lossy(&args[0]);
    let event = match LatencyEvent::from_str(&event_str) {
        Some(e) => e,
        None => {
            return Response::error(format!(
                "ERR Unknown event type: {}. Valid events: command, fork, aof-fsync, expire-cycle, eviction-cycle, snapshot-io",
                event_str
            ));
        }
    };

    let history = gather_latency_history(ctx.shard_senders, event).await;

    // Return as array of [timestamp, latency] pairs
    let entries: Vec<Response> = history
        .into_iter()
        .map(|sample| {
            Response::Array(vec![
                Response::Integer(sample.timestamp),
                Response::Integer(sample.latency_ms as i64),
            ])
        })
        .collect();

    Response::Array(entries)
}

/// LATENCY LATEST — get latest latency samples.
async fn latency_latest(ctx: &ConnCtx<'_>) -> Response {
    let latest = gather_latency_latest(ctx.shard_senders).await;

    let entries: Vec<Response> = latest
        .into_iter()
        .map(|(event, sample)| {
            Response::Array(vec![
                Response::bulk(Bytes::from(event.as_str())),
                Response::Integer(sample.timestamp),
                Response::Integer(sample.latency_ms as i64),
                Response::Integer(sample.latency_ms as i64), // max_latency (same as latest in our impl)
            ])
        })
        .collect();

    Response::Array(entries)
}

/// LATENCY RESET [event...] — clear latency data.
async fn latency_reset(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    // Parse event names
    let events: Vec<LatencyEvent> = args
        .iter()
        .filter_map(|arg| {
            let s = String::from_utf8_lossy(arg);
            LatencyEvent::from_str(&s)
        })
        .collect();

    // Broadcast reset to all shards. Await-and-discard: the replies are only a
    // barrier confirming every shard reset. Bounded by the shared deadline.
    let _ = ScatterGather::new(ctx.shard_senders, DEFAULT_SCATTER_GATHER_TIMEOUT, 0)
        .gather_all(|_shard, response_tx| ObservabilityMsg::LatencyReset {
            events: events.clone(),
            response_tx,
        })
        .await;

    Response::ok()
}

// =============================================================================
// STATUS
// =============================================================================

/// The `CommandSpec` for STATUS.
static STATUS_SPEC: CommandSpec = CommandSpec {
    name: "STATUS",
    arity: Arity::Range { min: 0, max: 1 },
    flags: CommandFlags::READONLY
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE)
        .union(CommandFlags::FAST),
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    mutation: frogdb_core::ConnMutation::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` STATUS executor.
pub(crate) static STATUS_CONN_COMMAND: StatusConnCommand = StatusConnCommand;

/// STATUS — server status and health information (JSON/HELP).
pub(crate) struct StatusConnCommand;

impl ConnectionCommand for StatusConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &STATUS_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            if args.is_empty() {
                // STATUS without subcommand shows help (FrogDB-specific behavior)
                return status_help();
            }

            let subcommand = args[0].to_ascii_uppercase();
            let subcommand_str = String::from_utf8_lossy(&subcommand);

            match subcommand_str.as_ref() {
                "JSON" => status_json(ctx).await,
                "HELP" => status_help(),
                _ => Response::error(format!(
                    "ERR unknown subcommand '{}'. Try STATUS HELP.",
                    subcommand_str
                )),
            }
        })
    }
}

/// STATUS JSON — return machine-readable server status.
///
/// Renders from the shared [`frogdb_core::StatusProvider`] — the same
/// [`frogdb_telemetry::StatusCollector`] the HTTP `/status` endpoint uses (see
/// [`crate::connection::status_handler`]) — so the two surfaces always agree.
/// There is no per-command scatter or field assembly here.
async fn status_json(ctx: &ConnCtx<'_>) -> Response {
    ctx.status.status_json().await
}

/// STATUS HELP — show subcommand help.
fn status_help() -> Response {
    let help = vec![
        Response::bulk(Bytes::from_static(
            b"STATUS <subcommand> [<arg> ...]. Subcommands are:",
        )),
        Response::bulk(Bytes::from_static(b"JSON")),
        Response::bulk(Bytes::from_static(
            b"    Return machine-readable server status as JSON.",
        )),
        Response::bulk(Bytes::from_static(b"HELP")),
        Response::bulk(Bytes::from_static(b"    Show this help.")),
    ];
    Response::Array(help)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::connection::ClusterDeps;
    use crate::connection::status_handler::StatusCollectorProvider;
    use crate::cursor_store::AggregateCursorStore;
    use crate::runtime_config::ConfigManager;
    use frogdb_core::persistence::NoopSnapshotCoordinator;
    use frogdb_core::{
        ClientRegistry, CommandLatencyHistograms, KeyspaceStats, NoopMetricsRecorder,
        SharedHotkeySession, new_shared_hotkey_session,
    };
    use frogdb_telemetry::{HealthChecker, LiveMode, StatusCollector, StatusCollectorConfig};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64};

    /// Build a `ConnCtx` over fixture dependencies — no socket, no
    /// `ConnectionHandler`. These commands run with no shards (`shard_senders`
    /// empty), so scatter-gather subcommands return empty aggregates; the
    /// subcommand parsing, help text, and metrics-band paths are exercised
    /// directly. STATUS JSON renders from `status_provider`, a real collector so
    /// the command exercises the shared render path (see `status_handler`).
    struct Fixture {
        config_manager: ConfigManager,
        client_registry: ClientRegistry,
        latency_histograms: CommandLatencyHistograms,
        keyspace_stats: KeyspaceStats,
        snapshot_coordinator: NoopSnapshotCoordinator,
        hotkey_session: SharedHotkeySession,
        cluster: ClusterDeps,
        cursor_store: AggregateCursorStore,
        metrics_recorder: NoopMetricsRecorder,
        memory_diag: MemoryDiag,
        acl_manager: std::sync::Arc<frogdb_core::AclManager>,
        command_registry: frogdb_core::CommandRegistry,
        status_provider: StatusCollectorProvider,
    }

    /// An empty-shard, standalone, persistence-off status collector for fixtures
    /// that don't exercise STATUS JSON's field values.
    fn default_status_collector() -> Arc<StatusCollector> {
        Arc::new(StatusCollector::new(
            StatusCollectorConfig::default(),
            HealthChecker::new(),
            Arc::new(vec![]),
            Arc::new(ClientRegistry::new()),
            Arc::new(frogdb_telemetry::PrometheusRecorder::new()),
            std::time::Instant::now(),
            Arc::new(AtomicU64::new(0)),
            0,
            false,
            "async".to_string(),
            LiveMode::new(false, Arc::new(AtomicBool::new(false)), "standalone"),
        ))
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_status_collector(default_status_collector())
        }

        fn with_status_collector(collector: Arc<StatusCollector>) -> Self {
            Self {
                config_manager: ConfigManager::new(&Config::default()),
                client_registry: ClientRegistry::new(),
                latency_histograms: CommandLatencyHistograms::new(true),
                keyspace_stats: KeyspaceStats::new(),
                snapshot_coordinator: NoopSnapshotCoordinator::new(),
                hotkey_session: new_shared_hotkey_session(),
                cluster: ClusterDeps::standalone(),
                cursor_store: AggregateCursorStore::new(),
                metrics_recorder: NoopMetricsRecorder::new(),
                memory_diag: MemoryDiag(frogdb_debug::MemoryDiagConfig::default()),
                acl_manager: frogdb_core::AclManager::new(Default::default()),
                command_registry: frogdb_core::CommandRegistry::new(),
                status_provider: StatusCollectorProvider(collector),
            }
        }

        fn ctx(&self) -> ConnCtx<'_> {
            ConnCtx::new(
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
            .with_username("default")
            .with_status(&self.status_provider)
        }
    }

    fn arg(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    // ---- SLOWLOG ----

    #[tokio::test]
    async fn slowlog_empty_args_errors() {
        let fx = Fixture::new();
        let resp = SlowlogConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn slowlog_len_with_no_shards_is_zero() {
        let fx = Fixture::new();
        let resp = SlowlogConnCommand
            .execute(&mut fx.ctx(), &[arg("LEN")])
            .await;
        assert_eq!(resp, Response::Integer(0));
    }

    #[tokio::test]
    async fn slowlog_get_with_no_shards_is_empty() {
        let fx = Fixture::new();
        let resp = SlowlogConnCommand
            .execute(&mut fx.ctx(), &[arg("GET")])
            .await;
        assert_eq!(resp, Response::Array(vec![]));
    }

    #[tokio::test]
    async fn slowlog_get_bad_count_errors() {
        let fx = Fixture::new();
        let resp = SlowlogConnCommand
            .execute(&mut fx.ctx(), &[arg("GET"), arg("-2")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn slowlog_reset_with_no_shards_is_ok() {
        let fx = Fixture::new();
        let resp = SlowlogConnCommand
            .execute(&mut fx.ctx(), &[arg("RESET")])
            .await;
        assert_eq!(resp, Response::ok());
    }

    #[tokio::test]
    async fn slowlog_help_lists_subcommands() {
        let fx = Fixture::new();
        let resp = SlowlogConnCommand
            .execute(&mut fx.ctx(), &[arg("HELP")])
            .await;
        assert!(matches!(resp, Response::Array(items) if !items.is_empty()));
    }

    #[tokio::test]
    async fn slowlog_unknown_subcommand_errors() {
        let fx = Fixture::new();
        let resp = SlowlogConnCommand
            .execute(&mut fx.ctx(), &[arg("NOPE")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    // ---- MEMORY ----

    #[tokio::test]
    async fn memory_empty_args_errors() {
        let fx = Fixture::new();
        let resp = MemoryConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn memory_help_lists_subcommands() {
        let fx = Fixture::new();
        let resp = MemoryConnCommand
            .execute(&mut fx.ctx(), &[arg("HELP")])
            .await;
        assert!(matches!(resp, Response::Array(items) if !items.is_empty()));
    }

    #[tokio::test]
    async fn memory_malloc_size_echoes_input() {
        let fx = Fixture::new();
        let resp = MemoryConnCommand
            .execute(&mut fx.ctx(), &[arg("MALLOC-SIZE"), arg("64")])
            .await;
        assert_eq!(resp, Response::Integer(64));
    }

    #[tokio::test]
    async fn memory_malloc_size_bad_value_errors() {
        let fx = Fixture::new();
        let resp = MemoryConnCommand
            .execute(&mut fx.ctx(), &[arg("MALLOC-SIZE"), arg("abc")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn memory_purge_is_ok() {
        let fx = Fixture::new();
        let resp = MemoryConnCommand
            .execute(&mut fx.ctx(), &[arg("PURGE")])
            .await;
        assert_eq!(resp, Response::ok());
    }

    #[tokio::test]
    async fn memory_stats_with_no_shards_returns_array() {
        let fx = Fixture::new();
        let resp = MemoryConnCommand
            .execute(&mut fx.ctx(), &[arg("STATS")])
            .await;
        assert!(matches!(resp, Response::Array(items) if !items.is_empty()));
    }

    #[tokio::test]
    async fn memory_usage_missing_key_errors() {
        let fx = Fixture::new();
        let resp = MemoryConnCommand
            .execute(&mut fx.ctx(), &[arg("USAGE")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn memory_doctor_returns_bulk() {
        let fx = Fixture::new();
        let resp = MemoryConnCommand
            .execute(&mut fx.ctx(), &[arg("DOCTOR")])
            .await;
        assert!(matches!(resp, Response::Bulk(_)));
    }

    #[tokio::test]
    async fn memory_unknown_subcommand_errors() {
        let fx = Fixture::new();
        let resp = MemoryConnCommand
            .execute(&mut fx.ctx(), &[arg("NOPE")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    // ---- LATENCY ----

    #[tokio::test]
    async fn latency_empty_args_errors() {
        let fx = Fixture::new();
        let resp = LatencyConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn latency_help_lists_subcommands() {
        let fx = Fixture::new();
        let resp = LatencyConnCommand
            .execute(&mut fx.ctx(), &[arg("HELP")])
            .await;
        assert!(matches!(resp, Response::Array(items) if !items.is_empty()));
    }

    #[tokio::test]
    async fn latency_bands_disabled_by_default_errors() {
        // NoopMetricsRecorder reports latency bands as disabled.
        let fx = Fixture::new();
        let resp = LatencyConnCommand
            .execute(&mut fx.ctx(), &[arg("BANDS")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn latency_latest_with_no_shards_is_empty() {
        let fx = Fixture::new();
        let resp = LatencyConnCommand
            .execute(&mut fx.ctx(), &[arg("LATEST")])
            .await;
        assert_eq!(resp, Response::Array(vec![]));
    }

    #[tokio::test]
    async fn latency_histogram_is_empty() {
        let fx = Fixture::new();
        let resp = LatencyConnCommand
            .execute(&mut fx.ctx(), &[arg("HISTOGRAM")])
            .await;
        assert_eq!(resp, Response::Array(vec![]));
    }

    #[tokio::test]
    async fn latency_reset_with_no_shards_is_ok() {
        let fx = Fixture::new();
        let resp = LatencyConnCommand
            .execute(&mut fx.ctx(), &[arg("RESET")])
            .await;
        assert_eq!(resp, Response::ok());
    }

    #[tokio::test]
    async fn latency_graph_missing_event_errors() {
        let fx = Fixture::new();
        let resp = LatencyConnCommand
            .execute(&mut fx.ctx(), &[arg("GRAPH")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn latency_history_unknown_event_errors() {
        let fx = Fixture::new();
        let resp = LatencyConnCommand
            .execute(&mut fx.ctx(), &[arg("HISTORY"), arg("bogus-event")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn latency_unknown_subcommand_errors() {
        let fx = Fixture::new();
        let resp = LatencyConnCommand
            .execute(&mut fx.ctx(), &[arg("NOPE")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    // ---- STATUS ----

    #[tokio::test]
    async fn status_no_args_shows_help() {
        let fx = Fixture::new();
        let resp = StatusConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert!(matches!(resp, Response::Array(items) if !items.is_empty()));
    }

    #[tokio::test]
    async fn status_help_lists_subcommands() {
        let fx = Fixture::new();
        let resp = StatusConnCommand
            .execute(&mut fx.ctx(), &[arg("HELP")])
            .await;
        assert!(matches!(resp, Response::Array(items) if !items.is_empty()));
    }

    #[tokio::test]
    async fn status_json_returns_bulk_json() {
        let fx = Fixture::new();
        let resp = StatusConnCommand
            .execute(&mut fx.ctx(), &[arg("JSON")])
            .await;
        match resp {
            Response::Bulk(Some(bytes)) => {
                let s = String::from_utf8_lossy(&bytes);
                assert!(s.contains("\"frogdb\""), "expected status JSON, got {s}");
                assert!(serde_json::from_slice::<serde_json::Value>(&bytes).is_ok());
            }
            other => panic!("expected bulk JSON, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn status_unknown_subcommand_errors() {
        let fx = Fixture::new();
        let resp = StatusConnCommand
            .execute(&mut fx.ctx(), &[arg("NOPE")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    /// STATUS JSON renders from the shared collector with live values — proving
    /// the formerly-hardcoded fields (`cluster.mode`, `persistence.enabled`,
    /// `commands.total_processed`) now reflect their real source, and that the
    /// command output agrees with the HTTP `/status` render from the same
    /// collector.
    #[tokio::test]
    async fn status_json_renders_from_shared_collector_and_agrees_with_http() {
        use frogdb_telemetry::definitions::CommandsTotal;

        let recorder = Arc::new(frogdb_telemetry::PrometheusRecorder::new());
        CommandsTotal::inc_by(&*recorder, 5, "GET");
        CommandsTotal::inc_by(&*recorder, 2, "SET");

        let collector = Arc::new(StatusCollector::new(
            StatusCollectorConfig::default(),
            HealthChecker::new(),
            Arc::new(vec![]),
            Arc::new(ClientRegistry::new()),
            recorder,
            std::time::Instant::now(),
            Arc::new(AtomicU64::new(0)),
            0,
            true, // persistence enabled (was hardcoded false)
            "async".to_string(),
            // cluster mode (was hardcoded "standalone"); cluster is config-static.
            LiveMode::new(true, Arc::new(AtomicBool::new(false)), "primary"),
        ));

        let fx = Fixture::with_status_collector(collector.clone());
        let resp = StatusConnCommand
            .execute(&mut fx.ctx(), &[arg("JSON")])
            .await;
        let bytes = match resp {
            Response::Bulk(Some(b)) => b,
            other => panic!("expected bulk JSON, got {other:?}"),
        };
        let v: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");

        // Live values, not the old hardcoded constants.
        assert_eq!(v["cluster"]["mode"], "cluster");
        assert_eq!(v["persistence"]["enabled"], true);
        assert_eq!(v["commands"]["total_processed"], 7);

        // Agrees field-for-field with the HTTP `/status` render from the same
        // collector (time-varying `frogdb` block excepted).
        let http = collector.collect().await;
        let http_v: serde_json::Value =
            serde_json::from_str(&collector.to_json(&http)).expect("valid JSON");
        for section in [
            "cluster",
            "health",
            "clients",
            "memory",
            "persistence",
            "keyspace",
            "commands",
        ] {
            assert_eq!(v[section], http_v[section], "section {section} disagrees");
        }
    }

    // ---- specs ----

    #[test]
    fn specs_are_connection_level_and_valid() {
        for spec in [
            SLOWLOG_CONN_COMMAND.spec(),
            MEMORY_CONN_COMMAND.spec(),
            LATENCY_CONN_COMMAND.spec(),
            STATUS_CONN_COMMAND.spec(),
        ] {
            assert!(spec.validate().is_ok(), "{}: invalid spec", spec.name);
            assert!(
                matches!(
                    spec.strategy,
                    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
                ),
                "{}: expected ConnectionLevel(Admin)",
                spec.name
            );
        }
    }
}
