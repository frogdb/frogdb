//! TimeSeries commands.
//!
//! Commands for time series data with Gorilla compression.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Aggregation, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags,
    CommandSpec, DownsampleRule, DuplicatePolicy, EventSpec, ExecutionStrategy, KeySpec,
    LookupSpec, StoreTypedFamilyExt, TimeSeriesValue, Value, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;
use std::time::{SystemTime, UNIX_EPOCH};

/// Get current timestamp in milliseconds.
fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Render an unrecognized option token for a "Unknown option" error.
///
/// Mirrors the original per-token `from_utf8(...).to_uppercase()` at the top of
/// each TS option loop: a non-UTF-8 option keyword yields the "Invalid option"
/// error, otherwise its ASCII-uppercased form is returned for the message.
/// `arg` is the still-unconsumed token the parser is positioned at (the caller
/// only reaches here inside `while parser.has_more()`, so it is `Some`).
fn ts_unknown_option(arg: Option<&Bytes>) -> Result<String, CommandError> {
    let arg = arg.expect("caller guarantees a remaining argument");
    Ok(std::str::from_utf8(arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "Invalid option".to_string(),
        })?
        .to_uppercase())
}

/// Parse a timestamp argument ("*" for auto, or integer).
fn parse_timestamp(arg: &[u8]) -> Result<i64, CommandError> {
    if arg == b"*" {
        return Ok(current_timestamp_ms());
    }
    std::str::from_utf8(arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "Invalid timestamp".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "Invalid timestamp".to_string(),
        })
}

/// Parse a float value.
fn parse_float(arg: &[u8]) -> Result<f64, CommandError> {
    std::str::from_utf8(arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "Invalid value".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "Invalid value".to_string(),
        })
}

/// Parse an integer argument.
fn parse_int<T: std::str::FromStr>(arg: &[u8], name: &str) -> Result<T, CommandError> {
    std::str::from_utf8(arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: format!("Invalid {}", name),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: format!("Invalid {}", name),
        })
}

/// Parse labels from LABELS ... arguments.
fn parse_labels(args: &[Bytes], start: usize) -> Result<Vec<(String, String)>, CommandError> {
    let mut labels = Vec::new();
    let mut i = start;

    while i + 1 < args.len() {
        let name = std::str::from_utf8(&args[i])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid label name".to_string(),
            })?
            .to_string();
        let value = std::str::from_utf8(&args[i + 1])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid label value".to_string(),
            })?
            .to_string();
        labels.push((name, value));
        i += 2;
    }

    Ok(labels)
}

// =============================================================================
// TS.CREATE
// =============================================================================

/// TS.CREATE key [RETENTION retentionPeriod] [ENCODING encoding] [CHUNK_SIZE size]
///   [DUPLICATE_POLICY policy] [LABELS label value ...]
pub struct TsCreateCommand;

impl Command for TsCreateCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.CREATE",
            arity: Arity::AtLeast(1),
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

        // Check if key already exists
        if ctx.store.get(key).is_some() {
            return Err(CommandError::InvalidArgument {
                message: "TSDB: key already exists".to_string(),
            });
        }

        // Parse options
        let mut retention_ms = 0u64;
        let mut duplicate_policy = DuplicatePolicy::Last;
        let mut chunk_size = 256usize;
        let mut labels = Vec::new();

        let mut parser = ArgParser::from_position(args, 1);
        while parser.has_more() {
            if parser.try_flag(b"RETENTION") {
                let val = parser
                    .next_arg()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "RETENTION requires a value".to_string(),
                    })?;
                retention_ms = parse_int(val, "retention")?;
            } else if parser.try_flag(b"CHUNK_SIZE") {
                let val = parser
                    .next_arg()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "CHUNK_SIZE requires a value".to_string(),
                    })?;
                chunk_size = parse_int(val, "chunk_size")?;
            } else if parser.try_flag(b"DUPLICATE_POLICY") {
                let val = parser
                    .next_arg()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "DUPLICATE_POLICY requires a value".to_string(),
                    })?;
                let policy_str =
                    std::str::from_utf8(val).map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid duplicate policy".to_string(),
                    })?;
                duplicate_policy = DuplicatePolicy::parse(policy_str).ok_or_else(|| {
                    CommandError::InvalidArgument {
                        message: format!("Unknown duplicate policy: {}", policy_str),
                    }
                })?;
            } else if parser.try_flag(b"LABELS") {
                labels = parse_labels(args, parser.position())?;
                break; // Labels consume the rest
            } else if parser.try_flag(b"ENCODING") {
                // Accept but ignore (we only support compressed)
                parser.skip(1);
            } else {
                let opt = ts_unknown_option(parser.peek())?;
                return Err(CommandError::InvalidArgument {
                    message: format!("Unknown option: {}", opt),
                });
            }
        }

        let ts = TimeSeriesValue::with_options(retention_ms, duplicate_policy, chunk_size, labels);
        ctx.store.set(key.clone(), Value::TimeSeries(ts));

        Ok(Response::ok())
    }
}

// =============================================================================
// TS.ALTER
// =============================================================================

/// TS.ALTER key [RETENTION retentionPeriod] [CHUNK_SIZE size]
///   [DUPLICATE_POLICY policy] [LABELS label value ...]
pub struct TsAlterCommand;

impl Command for TsAlterCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.ALTER",
            arity: Arity::AtLeast(1),
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

        // Parse labels separately to update the label index after releasing the borrow.
        let mut labels_to_update: Option<Vec<(String, String)>> = None;

        {
            let ts = ctx
                .store
                .get_timeseries_mut(key)?
                .ok_or(CommandError::InvalidArgument {
                    message: "TSDB: the key does not exist".to_string(),
                })?;

            let mut parser = ArgParser::from_position(args, 1);
            while parser.has_more() {
                if parser.try_flag(b"RETENTION") {
                    let val = parser
                        .next_arg()
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "RETENTION requires a value".to_string(),
                        })?;
                    let retention: u64 = parse_int(val, "retention")?;
                    ts.set_retention_ms(retention);
                } else if parser.try_flag(b"CHUNK_SIZE") {
                    let val = parser
                        .next_arg()
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "CHUNK_SIZE requires a value".to_string(),
                        })?;
                    let chunk_size: usize = parse_int(val, "chunk_size")?;
                    ts.set_chunk_size(chunk_size);
                } else if parser.try_flag(b"DUPLICATE_POLICY") {
                    let val = parser
                        .next_arg()
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "DUPLICATE_POLICY requires a value".to_string(),
                        })?;
                    let policy_str =
                        std::str::from_utf8(val).map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid duplicate policy".to_string(),
                        })?;
                    let policy = DuplicatePolicy::parse(policy_str).ok_or_else(|| {
                        CommandError::InvalidArgument {
                            message: format!("Unknown duplicate policy: {}", policy_str),
                        }
                    })?;
                    ts.set_duplicate_policy(policy);
                } else if parser.try_flag(b"LABELS") {
                    let labels = parse_labels(args, parser.position())?;
                    ts.set_labels(labels.clone());
                    labels_to_update = Some(labels);
                    break;
                } else {
                    let opt = ts_unknown_option(parser.peek())?;
                    return Err(CommandError::InvalidArgument {
                        message: format!("Unknown option: {}", opt),
                    });
                }
            }
        } // borrow on ctx.store released

        // Update label index if labels were changed
        if let Some(labels) = labels_to_update
            && let Some(ts_labels) = ctx.store.ts_labels_mut()
        {
            ts_labels.index_mut().update(key.clone(), &labels);
        }

        Ok(Response::ok())
    }
}

// =============================================================================
// TS.ADD
// =============================================================================

/// TS.ADD key timestamp value [RETENTION retentionPeriod] [ENCODING encoding]
///   [CHUNK_SIZE size] [ON_DUPLICATE policy] [LABELS label value ...]
pub struct TsAddCommand;

impl Command for TsAddCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.ADD",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
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
        let timestamp = parse_timestamp(&args[1])?;
        let value = parse_float(&args[2])?;

        // Parse options for auto-create
        let mut retention_ms = 0u64;
        let duplicate_policy = DuplicatePolicy::Last;
        let mut chunk_size = 256usize;
        let mut labels = Vec::new();
        let mut on_duplicate: Option<DuplicatePolicy> = None;

        let mut parser = ArgParser::from_position(args, 3);
        while parser.has_more() {
            if parser.try_flag(b"RETENTION") {
                let val = parser
                    .next_arg()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "RETENTION requires a value".to_string(),
                    })?;
                retention_ms = parse_int(val, "retention")?;
            } else if parser.try_flag(b"CHUNK_SIZE") {
                let val = parser
                    .next_arg()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "CHUNK_SIZE requires a value".to_string(),
                    })?;
                chunk_size = parse_int(val, "chunk_size")?;
            } else if parser.try_flag(b"ON_DUPLICATE") {
                let val = parser
                    .next_arg()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "ON_DUPLICATE requires a value".to_string(),
                    })?;
                let policy_str =
                    std::str::from_utf8(val).map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid on_duplicate policy".to_string(),
                    })?;
                on_duplicate = Some(DuplicatePolicy::parse(policy_str).ok_or_else(|| {
                    CommandError::InvalidArgument {
                        message: format!("Unknown on_duplicate policy: {}", policy_str),
                    }
                })?);
            } else if parser.try_flag(b"LABELS") {
                labels = parse_labels(args, parser.position())?;
                break;
            } else if parser.try_flag(b"ENCODING") {
                parser.skip(1); // Skip value
            } else {
                let opt = ts_unknown_option(parser.peek())?;
                return Err(CommandError::InvalidArgument {
                    message: format!("Unknown option: {}", opt),
                });
            }
        }

        // Get or create the time series
        match ctx.store.get_timeseries_mut(key)? {
            Some(ts) => {
                // Apply ON_DUPLICATE override if specified
                let old_policy = ts.duplicate_policy();
                if let Some(policy) = on_duplicate {
                    ts.set_duplicate_policy(policy);
                }

                let result = ts.add(timestamp, value);

                // Restore policy if overridden
                if on_duplicate.is_some() {
                    ts.set_duplicate_policy(old_policy);
                }

                match result {
                    Ok(ts_val) => {
                        // Process inline downsampling
                        process_downsample_rules(ctx, key, timestamp);
                        Ok(Response::Integer(ts_val))
                    }
                    Err(e) => Err(CommandError::InvalidArgument {
                        message: format!("TSDB: {:?}", e),
                    }),
                }
            }
            None => {
                // Auto-create
                let mut ts = TimeSeriesValue::with_options(
                    retention_ms,
                    duplicate_policy,
                    chunk_size,
                    labels,
                );
                ts.add(timestamp, value)
                    .map_err(|e| CommandError::InvalidArgument {
                        message: format!("TSDB: {:?}", e),
                    })?;
                ctx.store.set(key.clone(), Value::TimeSeries(ts));
                Ok(Response::Integer(timestamp))
            }
        }
    }
}

// =============================================================================
// TS.MADD
// =============================================================================

/// TS.MADD key timestamp value [key timestamp value ...]
pub struct TsMaddCommand;

impl Command for TsMaddCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.MADD",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::Stride { step: 3 },
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
        if !args.len().is_multiple_of(3) {
            return Err(CommandError::InvalidArgument {
                message: "TSDB: wrong number of arguments".to_string(),
            });
        }

        let mut results = Vec::with_capacity(args.len() / 3);

        for chunk in args.chunks(3) {
            let key = &chunk[0];
            let timestamp = match parse_timestamp(&chunk[1]) {
                Ok(ts) => ts,
                Err(e) => {
                    results.push(Response::Error(Bytes::from(format!("{}", e))));
                    continue;
                }
            };
            let value = match parse_float(&chunk[2]) {
                Ok(v) => v,
                Err(e) => {
                    results.push(Response::Error(Bytes::from(format!("{}", e))));
                    continue;
                }
            };

            match ctx.store.get_timeseries_mut(key) {
                Ok(Some(ts)) => match ts.add(timestamp, value) {
                    Ok(ts_val) => {
                        process_downsample_rules(ctx, key, timestamp);
                        results.push(Response::Integer(ts_val));
                    }
                    Err(e) => results.push(Response::Error(Bytes::from(format!("TSDB: {:?}", e)))),
                },
                Err(_) => {
                    results.push(Response::Error(Bytes::from("WRONGTYPE")));
                }
                Ok(None) => {
                    // Auto-create
                    let mut ts = TimeSeriesValue::new();
                    match ts.add(timestamp, value) {
                        Ok(ts_val) => {
                            ctx.store.set(key.clone(), Value::TimeSeries(ts));
                            results.push(Response::Integer(ts_val));
                        }
                        Err(e) => {
                            results.push(Response::Error(Bytes::from(format!("TSDB: {:?}", e))))
                        }
                    }
                }
            }
        }

        Ok(Response::Array(results))
    }
}

// =============================================================================
// TS.INCRBY / TS.DECRBY
// =============================================================================

/// TS.INCRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod]
///   [CHUNK_SIZE size] [LABELS label value ...]
pub struct TsIncrbyCommand;

impl Command for TsIncrbyCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.INCRBY",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
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
        execute_incrby(ctx, args, 1.0)
    }
}

/// TS.DECRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod]
///   [CHUNK_SIZE size] [LABELS label value ...]
pub struct TsDecrbyCommand;

impl Command for TsDecrbyCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.DECRBY",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
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
        execute_incrby(ctx, args, -1.0)
    }
}

fn execute_incrby(
    ctx: &mut CommandContext,
    args: &[Bytes],
    sign: f64,
) -> Result<Response, CommandError> {
    let key = &args[0];
    let delta = parse_float(&args[1])? * sign;

    // Parse options
    let mut timestamp = current_timestamp_ms();
    let mut retention_ms = 0u64;
    let mut chunk_size = 256usize;
    let mut labels = Vec::new();

    let mut parser = ArgParser::from_position(args, 2);
    while parser.has_more() {
        if parser.try_flag(b"TIMESTAMP") {
            let val = parser
                .next_arg()
                .map_err(|_| CommandError::InvalidArgument {
                    message: "TIMESTAMP requires a value".to_string(),
                })?;
            timestamp = parse_timestamp(val)?;
        } else if parser.try_flag(b"RETENTION") {
            let val = parser
                .next_arg()
                .map_err(|_| CommandError::InvalidArgument {
                    message: "RETENTION requires a value".to_string(),
                })?;
            retention_ms = parse_int(val, "retention")?;
        } else if parser.try_flag(b"CHUNK_SIZE") {
            let val = parser
                .next_arg()
                .map_err(|_| CommandError::InvalidArgument {
                    message: "CHUNK_SIZE requires a value".to_string(),
                })?;
            chunk_size = parse_int(val, "chunk_size")?;
        } else if parser.try_flag(b"LABELS") {
            labels = parse_labels(args, parser.position())?;
            break;
        } else {
            let opt = ts_unknown_option(parser.peek())?;
            return Err(CommandError::InvalidArgument {
                message: format!("Unknown option: {}", opt),
            });
        }
    }

    match ctx.store.get_timeseries_mut(key)? {
        Some(ts) => {
            let _new_val =
                ts.incrby(timestamp, delta)
                    .map_err(|e| CommandError::InvalidArgument {
                        message: format!("TSDB: {:?}", e),
                    })?;
            process_downsample_rules(ctx, key, timestamp);
            Ok(Response::Integer(timestamp))
        }
        None => {
            // Auto-create
            let mut ts = TimeSeriesValue::with_options(
                retention_ms,
                DuplicatePolicy::Last,
                chunk_size,
                labels,
            );
            ts.add(timestamp, delta)
                .map_err(|e| CommandError::InvalidArgument {
                    message: format!("TSDB: {:?}", e),
                })?;
            ctx.store.set(key.clone(), Value::TimeSeries(ts));
            Ok(Response::Integer(timestamp))
        }
    }
}

// =============================================================================
// TS.GET
// =============================================================================

/// TS.GET key
pub struct TsGetCommand;

impl Command for TsGetCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.GET",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
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

        match ctx.store.get_timeseries(key)? {
            Some(ts) => match ts.get_last() {
                Some((timestamp, val)) => Ok(Response::Array(vec![
                    Response::Integer(timestamp),
                    Response::bulk(Bytes::from(format_float(val))),
                ])),
                None => Ok(Response::Array(vec![])),
            },
            None => Ok(Response::Array(vec![])),
        }
    }
}

// =============================================================================
// TS.DEL
// =============================================================================

/// TS.DEL key fromTimestamp toTimestamp
pub struct TsDelCommand;

impl Command for TsDelCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.DEL",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
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
        let from: i64 = parse_range_bound(&args[1])?;
        let to: i64 = parse_range_bound(&args[2])?;

        match ctx.store.get_timeseries_mut(key)? {
            Some(ts) => {
                let deleted = ts.delete_range(from, to);
                Ok(Response::Integer(deleted as i64))
            }
            None => Err(CommandError::InvalidArgument {
                message: "TSDB: the key does not exist".to_string(),
            }),
        }
    }
}

// =============================================================================
// TS.RANGE / TS.REVRANGE
// =============================================================================

/// TS.RANGE key fromTimestamp toTimestamp [FILTER_BY_TS ts1 ts2 ...] [FILTER_BY_VALUE min max]
///   [COUNT count] [AGGREGATION aggregationType bucketDuration]
pub struct TsRangeCommand;

impl Command for TsRangeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.RANGE",
            arity: Arity::AtLeast(3),
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
        execute_range(ctx, args, false)
    }
}

/// TS.REVRANGE key fromTimestamp toTimestamp [options...]
pub struct TsRevrangeCommand;

impl Command for TsRevrangeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.REVRANGE",
            arity: Arity::AtLeast(3),
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
        execute_range(ctx, args, true)
    }
}

fn parse_range_bound(arg: &[u8]) -> Result<i64, CommandError> {
    let s = std::str::from_utf8(arg).map_err(|_| CommandError::InvalidArgument {
        message: "Invalid range bound".to_string(),
    })?;

    if s == "-" {
        return Ok(i64::MIN);
    }
    if s == "+" {
        return Ok(i64::MAX);
    }

    s.parse().map_err(|_| CommandError::InvalidArgument {
        message: "Invalid range bound".to_string(),
    })
}

fn execute_range(
    ctx: &mut CommandContext,
    args: &[Bytes],
    reverse: bool,
) -> Result<Response, CommandError> {
    let key = &args[0];
    let from = parse_range_bound(&args[1])?;
    let to = parse_range_bound(&args[2])?;

    // Parse options
    let mut filter_ts: Option<Vec<i64>> = None;
    let mut filter_value: Option<(f64, f64)> = None;
    let mut count: Option<usize> = None;
    let mut aggregation: Option<(Aggregation, i64)> = None;

    let mut parser = ArgParser::from_position(args, 3);
    while parser.has_more() {
        if parser.try_flag(b"FILTER_BY_TS") {
            // Greedily consume as many timestamp integers as parse cleanly.
            let mut timestamps = Vec::new();
            while let Some(arg) = parser.peek() {
                if let Ok(ts) = parse_int::<i64>(arg, "timestamp") {
                    timestamps.push(ts);
                    parser.skip(1);
                } else {
                    break;
                }
            }
            filter_ts = Some(timestamps);
        } else if parser.try_flag(b"FILTER_BY_VALUE") {
            if parser.remaining_count() < 2 {
                return Err(CommandError::InvalidArgument {
                    message: "FILTER_BY_VALUE requires min and max".to_string(),
                });
            }
            let min = parse_float(parser.next_arg()?)?;
            let max = parse_float(parser.next_arg()?)?;
            filter_value = Some((min, max));
        } else if parser.try_flag(b"COUNT") {
            let val = parser
                .next_arg()
                .map_err(|_| CommandError::InvalidArgument {
                    message: "COUNT requires a value".to_string(),
                })?;
            count = Some(parse_int(val, "count")?);
        } else if parser.try_flag(b"AGGREGATION") {
            if parser.remaining_count() < 2 {
                return Err(CommandError::InvalidArgument {
                    message: "AGGREGATION requires type and bucket duration".to_string(),
                });
            }
            let agg_str = std::str::from_utf8(parser.next_arg()?).map_err(|_| {
                CommandError::InvalidArgument {
                    message: "Invalid aggregation type".to_string(),
                }
            })?;
            let agg = Aggregation::parse(agg_str).ok_or_else(|| CommandError::InvalidArgument {
                message: format!("Unknown aggregation type: {}", agg_str),
            })?;
            let bucket: i64 = parse_int(parser.next_arg()?, "bucket duration")?;
            aggregation = Some((agg, bucket));
        } else {
            let opt = ts_unknown_option(parser.peek())?;
            return Err(CommandError::InvalidArgument {
                message: format!("Unknown option: {}", opt),
            });
        }
    }

    match ctx.store.get_timeseries(key)? {
        Some(ts) => {
            // Get samples
            let mut samples = if let Some((agg, bucket)) = aggregation {
                ts.range_aggregated(from, to, bucket, agg)
            } else if reverse {
                ts.revrange(from, to)
            } else {
                ts.range(from, to)
            };

            // Apply FILTER_BY_TS
            if let Some(ref allowed_ts) = filter_ts {
                samples.retain(|(ts_val, _)| allowed_ts.contains(ts_val));
            }

            // Apply FILTER_BY_VALUE
            if let Some((min, max)) = filter_value {
                samples.retain(|(_, val)| *val >= min && *val <= max);
            }

            // Apply COUNT
            if let Some(limit) = count {
                samples.truncate(limit);
            }

            // Format response
            let results: Vec<Response> = samples
                .iter()
                .map(|(ts_val, val)| {
                    Response::Array(vec![
                        Response::Integer(*ts_val),
                        Response::bulk(Bytes::from(format_float(*val))),
                    ])
                })
                .collect();

            Ok(Response::Array(results))
        }
        None => Err(CommandError::InvalidArgument {
            message: "TSDB: the key does not exist".to_string(),
        }),
    }
}

// =============================================================================
// TS.INFO
// =============================================================================

/// TS.INFO key
pub struct TsInfoCommand;

impl Command for TsInfoCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.INFO",
            arity: Arity::Range { min: 1, max: 2 },
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

        match ctx.store.get_timeseries(key)? {
            Some(ts) => {
                let mut result = vec![
                    Response::bulk(Bytes::from("totalSamples")),
                    Response::Integer(ts.total_samples() as i64),
                    Response::bulk(Bytes::from("memoryUsage")),
                    Response::Integer(ts.memory_size() as i64),
                    Response::bulk(Bytes::from("firstTimestamp")),
                    Response::Integer(ts.first_timestamp().unwrap_or(0)),
                    Response::bulk(Bytes::from("lastTimestamp")),
                    Response::Integer(ts.last_timestamp().unwrap_or(0)),
                    Response::bulk(Bytes::from("retentionTime")),
                    Response::Integer(ts.retention_ms() as i64),
                    Response::bulk(Bytes::from("chunkCount")),
                    Response::Integer(ts.chunk_count() as i64),
                    Response::bulk(Bytes::from("chunkSize")),
                    Response::Integer(ts.chunk_size() as i64),
                    Response::bulk(Bytes::from("duplicatePolicy")),
                    Response::bulk(Bytes::from(ts.duplicate_policy().as_str())),
                ];

                // Add labels
                let labels: Vec<Response> = ts
                    .labels()
                    .iter()
                    .flat_map(|(k, v)| {
                        vec![
                            Response::bulk(Bytes::from(k.clone())),
                            Response::bulk(Bytes::from(v.clone())),
                        ]
                    })
                    .collect();

                result.push(Response::bulk(Bytes::from("labels")));
                result.push(Response::Array(labels));

                // Add rules
                let rules: Vec<Response> = ts
                    .rules()
                    .iter()
                    .map(|rule| {
                        Response::Array(vec![
                            Response::bulk(rule.dest_key.clone()),
                            Response::Integer(rule.bucket_duration_ms),
                            Response::bulk(Bytes::from(rule.aggregation.as_str())),
                        ])
                    })
                    .collect();

                result.push(Response::bulk(Bytes::from("rules")));
                result.push(Response::Array(rules));

                Ok(Response::Array(result))
            }
            None => Err(CommandError::InvalidArgument {
                message: "TSDB: the key does not exist".to_string(),
            }),
        }
    }
}

// =============================================================================
// TS.QUERYINDEX
// =============================================================================

/// TS.QUERYINDEX filter1 [filter2 ...]
pub struct TsQueryIndexCommand;

impl Command for TsQueryIndexCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.QUERYINDEX",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ServerWide,
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::Array(vec![]))
    }
}

// =============================================================================
// TS.MGET
// =============================================================================

/// TS.MGET [WITHLABELS | SELECTED_LABELS l1 ...] FILTER filter1 [filter2 ...]
pub struct TsMgetCommand;

impl Command for TsMgetCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.MGET",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ServerWide,
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        Ok(Response::Array(vec![]))
    }
}

// =============================================================================
// TS.MRANGE
// =============================================================================

/// TS.MRANGE fromTimestamp toTimestamp [FILTER_BY_TS ts ...] [FILTER_BY_VALUE min max]
///   [WITHLABELS | SELECTED_LABELS l1 ...] [COUNT count]
///   [AGGREGATION type bucketDuration] FILTER filter1 [filter2 ...]
pub struct TsMrangeCommand;

impl Command for TsMrangeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.MRANGE",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ServerWide,
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        Ok(Response::Array(vec![]))
    }
}

// =============================================================================
// TS.MREVRANGE
// =============================================================================

/// TS.MREVRANGE fromTimestamp toTimestamp [options...] FILTER filter1 [filter2 ...]
pub struct TsMrevrangeCommand;

impl Command for TsMrevrangeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.MREVRANGE",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ServerWide,
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        Ok(Response::Array(vec![]))
    }
}

// =============================================================================
// TS.CREATERULE
// =============================================================================

/// TS.CREATERULE sourceKey destKey AGGREGATION aggregationType bucketDuration
pub struct TsCreateRuleCommand;

impl Command for TsCreateRuleCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.CREATERULE",
            arity: Arity::Fixed(5),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
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
        let source_key = &args[0];
        let dest_key = &args[1];

        // args[2] should be "AGGREGATION"
        let agg_keyword = std::str::from_utf8(&args[2])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid argument".to_string(),
            })?
            .to_uppercase();
        if agg_keyword != "AGGREGATION" {
            return Err(CommandError::InvalidArgument {
                message: "TSDB: AGGREGATION keyword expected".to_string(),
            });
        }

        let agg_str = std::str::from_utf8(&args[3]).map_err(|_| CommandError::InvalidArgument {
            message: "Invalid aggregation type".to_string(),
        })?;
        let aggregation =
            Aggregation::parse(agg_str).ok_or_else(|| CommandError::InvalidArgument {
                message: format!("TSDB: Unknown aggregation type: {}", agg_str),
            })?;

        let bucket_duration_ms: i64 = parse_int(&args[4], "bucket duration")?;
        if bucket_duration_ms <= 0 {
            return Err(CommandError::InvalidArgument {
                message: "TSDB: bucket duration must be positive".to_string(),
            });
        }

        // Verify destination exists and is a TimeSeries
        if ctx.store.get(dest_key).is_none() {
            return Err(CommandError::InvalidArgument {
                message: "TSDB: the key does not exist".to_string(),
            });
        }

        // Add the rule to the source
        let rule = DownsampleRule::new(dest_key.clone(), bucket_duration_ms, aggregation);

        let ts =
            ctx.store
                .get_timeseries_mut(source_key)?
                .ok_or(CommandError::InvalidArgument {
                    message: "TSDB: the key does not exist".to_string(),
                })?;

        ts.add_rule(rule)
            .map_err(|e| CommandError::InvalidArgument {
                message: format!("TSDB: {}", e),
            })?;

        Ok(Response::ok())
    }
}

// =============================================================================
// TS.DELETERULE
// =============================================================================

/// TS.DELETERULE sourceKey destKey
pub struct TsDeleteRuleCommand;

impl Command for TsDeleteRuleCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TS.DELETERULE",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
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
        let source_key = &args[0];
        let dest_key = &args[1];

        let ts =
            ctx.store
                .get_timeseries_mut(source_key)?
                .ok_or(CommandError::InvalidArgument {
                    message: "TSDB: the key does not exist".to_string(),
                })?;

        ts.remove_rule(dest_key)
            .map_err(|e| CommandError::InvalidArgument {
                message: format!("TSDB: {}", e),
            })?;

        Ok(Response::ok())
    }
}

/// Process inline downsampling rules after a sample is added to a source key.
///
/// Uses the sequential-release pattern: collect pending writes from the source
/// borrow, release it, then write aggregated values to destination keys.
fn process_downsample_rules(ctx: &mut CommandContext, source_key: &[u8], timestamp: i64) {
    use frogdb_core::timeseries::aggregation::aggregate;

    // Collect pending writes
    let pending_writes: Vec<(Bytes, i64, f64)> = {
        let Ok(Some(ts)) = ctx.store.get_timeseries_mut(source_key) else {
            return;
        };

        // Phase 1: read rules + aggregate (immutable borrow)
        let mut writes = Vec::new();
        let mut bucket_updates: Vec<(usize, i64)> = Vec::new();
        for (idx, rule) in ts.rules().iter().enumerate() {
            let new_bucket = rule.bucket_for(timestamp);
            if let Some(prev_bucket) = rule.current_bucket_start
                && new_bucket != prev_bucket
            {
                let bucket_end = prev_bucket + rule.bucket_duration_ms - 1;
                let samples = ts.range(prev_bucket, bucket_end);
                if let Some(agg_val) = aggregate(&samples, rule.aggregation) {
                    writes.push((rule.dest_key.clone(), prev_bucket, agg_val));
                }
            }
            bucket_updates.push((idx, new_bucket));
        }
        // Phase 2: update bucket tracking (mutable borrow)
        for (idx, new_bucket) in bucket_updates {
            ts.rules_mut()[idx].current_bucket_start = Some(new_bucket);
        }
        writes
    }; // source borrow released

    // Write aggregated values to destination keys
    for (dest_key, bucket_ts, agg_val) in pending_writes {
        if let Ok(Some(dest_ts)) = ctx.store.get_timeseries_mut(&dest_key) {
            let _ = dest_ts.add(bucket_ts, agg_val); // best-effort
        }
    }
}

/// Format a float for output.
/// Uses minimal precision while maintaining round-trip accuracy.
fn format_float(f: f64) -> String {
    if f.is_nan() {
        return "nan".to_string();
    }
    if f.is_infinite() {
        return if f.is_sign_positive() { "inf" } else { "-inf" }.to_string();
    }
    if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{:.0}", f)
    } else {
        // Use Display formatting which provides a reasonable representation
        // that works well for most float values
        format!("{}", f)
    }
}
