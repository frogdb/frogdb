//! TimeSeries commands.
//!
//! Commands for time series data with Gorilla compression.

use bytes::Bytes;
use frogdb_core::{
    Aggregation, Arity, Command, CommandContext, CommandError, CommandFlags, DuplicatePolicy,
    TimeSeriesValue, Value, WalStrategy,
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
    fn name(&self) -> &'static str {
        "TS.CREATE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // TS.CREATE key [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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

        let mut i = 1;
        while i < args.len() {
            let opt = std::str::from_utf8(&args[i])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid option".to_string(),
                })?
                .to_uppercase();

            match opt.as_str() {
                "RETENTION" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "RETENTION requires a value".to_string(),
                        });
                    }
                    retention_ms = parse_int(&args[i], "retention")?;
                }
                "CHUNK_SIZE" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "CHUNK_SIZE requires a value".to_string(),
                        });
                    }
                    chunk_size = parse_int(&args[i], "chunk_size")?;
                }
                "DUPLICATE_POLICY" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "DUPLICATE_POLICY requires a value".to_string(),
                        });
                    }
                    let policy_str = std::str::from_utf8(&args[i]).map_err(|_| {
                        CommandError::InvalidArgument {
                            message: "Invalid duplicate policy".to_string(),
                        }
                    })?;
                    duplicate_policy = DuplicatePolicy::parse(policy_str).ok_or_else(|| {
                        CommandError::InvalidArgument {
                            message: format!("Unknown duplicate policy: {}", policy_str),
                        }
                    })?;
                }
                "LABELS" => {
                    i += 1;
                    labels = parse_labels(args, i)?;
                    break; // Labels consume the rest
                }
                "ENCODING" => {
                    // Accept but ignore (we only support compressed)
                    i += 1;
                }
                _ => {
                    return Err(CommandError::InvalidArgument {
                        message: format!("Unknown option: {}", opt),
                    });
                }
            }
            i += 1;
        }

        let ts = TimeSeriesValue::with_options(retention_ms, duplicate_policy, chunk_size, labels);
        ctx.store.set(key.clone(), Value::TimeSeries(ts));

        Ok(Response::ok())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// =============================================================================
// TS.ALTER
// =============================================================================

/// TS.ALTER key [RETENTION retentionPeriod] [CHUNK_SIZE size]
///   [DUPLICATE_POLICY policy] [LABELS label value ...]
pub struct TsAlterCommand;

impl Command for TsAlterCommand {
    fn name(&self) -> &'static str {
        "TS.ALTER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // TS.ALTER key [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let ts = ctx
            .store
            .get_mut(key)
            .ok_or(CommandError::InvalidArgument {
                message: "TSDB: the key does not exist".to_string(),
            })?
            .as_timeseries_mut()
            .ok_or(CommandError::WrongType)?;

        let mut i = 1;
        while i < args.len() {
            let opt = std::str::from_utf8(&args[i])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid option".to_string(),
                })?
                .to_uppercase();

            match opt.as_str() {
                "RETENTION" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "RETENTION requires a value".to_string(),
                        });
                    }
                    let retention: u64 = parse_int(&args[i], "retention")?;
                    ts.set_retention_ms(retention);
                }
                "CHUNK_SIZE" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "CHUNK_SIZE requires a value".to_string(),
                        });
                    }
                    let chunk_size: usize = parse_int(&args[i], "chunk_size")?;
                    ts.set_chunk_size(chunk_size);
                }
                "DUPLICATE_POLICY" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "DUPLICATE_POLICY requires a value".to_string(),
                        });
                    }
                    let policy_str = std::str::from_utf8(&args[i]).map_err(|_| {
                        CommandError::InvalidArgument {
                            message: "Invalid duplicate policy".to_string(),
                        }
                    })?;
                    let policy = DuplicatePolicy::parse(policy_str).ok_or_else(|| {
                        CommandError::InvalidArgument {
                            message: format!("Unknown duplicate policy: {}", policy_str),
                        }
                    })?;
                    ts.set_duplicate_policy(policy);
                }
                "LABELS" => {
                    i += 1;
                    let labels = parse_labels(args, i)?;
                    ts.set_labels(labels);
                    break;
                }
                _ => {
                    return Err(CommandError::InvalidArgument {
                        message: format!("Unknown option: {}", opt),
                    });
                }
            }
            i += 1;
        }

        Ok(Response::ok())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// =============================================================================
// TS.ADD
// =============================================================================

/// TS.ADD key timestamp value [RETENTION retentionPeriod] [ENCODING encoding]
///   [CHUNK_SIZE size] [ON_DUPLICATE policy] [LABELS label value ...]
pub struct TsAddCommand;

impl Command for TsAddCommand {
    fn name(&self) -> &'static str {
        "TS.ADD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // TS.ADD key timestamp value [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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

        let mut i = 3;
        while i < args.len() {
            let opt = std::str::from_utf8(&args[i])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid option".to_string(),
                })?
                .to_uppercase();

            match opt.as_str() {
                "RETENTION" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "RETENTION requires a value".to_string(),
                        });
                    }
                    retention_ms = parse_int(&args[i], "retention")?;
                }
                "CHUNK_SIZE" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "CHUNK_SIZE requires a value".to_string(),
                        });
                    }
                    chunk_size = parse_int(&args[i], "chunk_size")?;
                }
                "ON_DUPLICATE" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "ON_DUPLICATE requires a value".to_string(),
                        });
                    }
                    let policy_str = std::str::from_utf8(&args[i]).map_err(|_| {
                        CommandError::InvalidArgument {
                            message: "Invalid on_duplicate policy".to_string(),
                        }
                    })?;
                    on_duplicate = Some(DuplicatePolicy::parse(policy_str).ok_or_else(|| {
                        CommandError::InvalidArgument {
                            message: format!("Unknown on_duplicate policy: {}", policy_str),
                        }
                    })?);
                }
                "LABELS" => {
                    i += 1;
                    labels = parse_labels(args, i)?;
                    break;
                }
                "ENCODING" => {
                    i += 1; // Skip value
                }
                _ => {
                    return Err(CommandError::InvalidArgument {
                        message: format!("Unknown option: {}", opt),
                    });
                }
            }
            i += 1;
        }

        // Get or create the time series
        match ctx.store.get_mut(key) {
            Some(value_ref) => {
                let ts = value_ref
                    .as_timeseries_mut()
                    .ok_or(CommandError::WrongType)?;

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
                    Ok(ts_val) => Ok(Response::Integer(ts_val)),
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// =============================================================================
// TS.MADD
// =============================================================================

/// TS.MADD key timestamp value [key timestamp value ...]
pub struct TsMaddCommand;

impl Command for TsMaddCommand {
    fn name(&self) -> &'static str {
        "TS.MADD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // TS.MADD key timestamp value [key timestamp value ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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

            match ctx.store.get_mut(key) {
                Some(value_ref) => match value_ref.as_timeseries_mut() {
                    Some(ts) => match ts.add(timestamp, value) {
                        Ok(ts_val) => results.push(Response::Integer(ts_val)),
                        Err(e) => {
                            results.push(Response::Error(Bytes::from(format!("TSDB: {:?}", e))))
                        }
                    },
                    None => {
                        results.push(Response::Error(Bytes::from("WRONGTYPE")));
                    }
                },
                None => {
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.chunks(3).map(|c| c[0].as_ref()).collect()
    }
}

// =============================================================================
// TS.INCRBY / TS.DECRBY
// =============================================================================

/// TS.INCRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod]
///   [CHUNK_SIZE size] [LABELS label value ...]
pub struct TsIncrbyCommand;

impl Command for TsIncrbyCommand {
    fn name(&self) -> &'static str {
        "TS.INCRBY"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // TS.INCRBY key value [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        execute_incrby(ctx, args, 1.0)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

/// TS.DECRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod]
///   [CHUNK_SIZE size] [LABELS label value ...]
pub struct TsDecrbyCommand;

impl Command for TsDecrbyCommand {
    fn name(&self) -> &'static str {
        "TS.DECRBY"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // TS.DECRBY key value [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        execute_incrby(ctx, args, -1.0)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
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

    let mut i = 2;
    while i < args.len() {
        let opt = std::str::from_utf8(&args[i])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid option".to_string(),
            })?
            .to_uppercase();

        match opt.as_str() {
            "TIMESTAMP" => {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "TIMESTAMP requires a value".to_string(),
                    });
                }
                timestamp = parse_timestamp(&args[i])?;
            }
            "RETENTION" => {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "RETENTION requires a value".to_string(),
                    });
                }
                retention_ms = parse_int(&args[i], "retention")?;
            }
            "CHUNK_SIZE" => {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "CHUNK_SIZE requires a value".to_string(),
                    });
                }
                chunk_size = parse_int(&args[i], "chunk_size")?;
            }
            "LABELS" => {
                i += 1;
                labels = parse_labels(args, i)?;
                break;
            }
            _ => {
                return Err(CommandError::InvalidArgument {
                    message: format!("Unknown option: {}", opt),
                });
            }
        }
        i += 1;
    }

    match ctx.store.get_mut(key) {
        Some(value_ref) => {
            let ts = value_ref
                .as_timeseries_mut()
                .ok_or(CommandError::WrongType)?;
            let _new_val =
                ts.incrby(timestamp, delta)
                    .map_err(|e| CommandError::InvalidArgument {
                        message: format!("TSDB: {:?}", e),
                    })?;
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
    fn name(&self) -> &'static str {
        "TS.GET"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // TS.GET key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let ts = value.as_timeseries().ok_or(CommandError::WrongType)?;
                match ts.get_last() {
                    Some((timestamp, val)) => Ok(Response::Array(vec![
                        Response::Integer(timestamp),
                        Response::bulk(Bytes::from(format_float(val))),
                    ])),
                    None => Ok(Response::Array(vec![])),
                }
            }
            None => Ok(Response::Array(vec![])),
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

// =============================================================================
// TS.DEL
// =============================================================================

/// TS.DEL key fromTimestamp toTimestamp
pub struct TsDelCommand;

impl Command for TsDelCommand {
    fn name(&self) -> &'static str {
        "TS.DEL"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // TS.DEL key fromTimestamp toTimestamp
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistOrDeleteFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let from: i64 = parse_range_bound(&args[1])?;
        let to: i64 = parse_range_bound(&args[2])?;

        match ctx.store.get_mut(key) {
            Some(value) => {
                let ts = value.as_timeseries_mut().ok_or(CommandError::WrongType)?;
                let deleted = ts.delete_range(from, to);
                Ok(Response::Integer(deleted as i64))
            }
            None => Err(CommandError::InvalidArgument {
                message: "TSDB: the key does not exist".to_string(),
            }),
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

// =============================================================================
// TS.RANGE / TS.REVRANGE
// =============================================================================

/// TS.RANGE key fromTimestamp toTimestamp [FILTER_BY_TS ts1 ts2 ...] [FILTER_BY_VALUE min max]
///   [COUNT count] [AGGREGATION aggregationType bucketDuration]
pub struct TsRangeCommand;

impl Command for TsRangeCommand {
    fn name(&self) -> &'static str {
        "TS.RANGE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // TS.RANGE key fromTimestamp toTimestamp [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        execute_range(ctx, args, false)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

/// TS.REVRANGE key fromTimestamp toTimestamp [options...]
pub struct TsRevrangeCommand;

impl Command for TsRevrangeCommand {
    fn name(&self) -> &'static str {
        "TS.REVRANGE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // TS.REVRANGE key fromTimestamp toTimestamp [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        execute_range(ctx, args, true)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
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

    let mut i = 3;
    while i < args.len() {
        let opt = std::str::from_utf8(&args[i])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid option".to_string(),
            })?
            .to_uppercase();

        match opt.as_str() {
            "FILTER_BY_TS" => {
                i += 1;
                let mut timestamps = Vec::new();
                while i < args.len() {
                    if let Ok(ts) = parse_int::<i64>(&args[i], "timestamp") {
                        timestamps.push(ts);
                        i += 1;
                    } else {
                        break;
                    }
                }
                filter_ts = Some(timestamps);
                continue; // Don't increment i again
            }
            "FILTER_BY_VALUE" => {
                i += 1;
                if i + 1 >= args.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "FILTER_BY_VALUE requires min and max".to_string(),
                    });
                }
                let min = parse_float(&args[i])?;
                i += 1;
                let max = parse_float(&args[i])?;
                filter_value = Some((min, max));
            }
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "COUNT requires a value".to_string(),
                    });
                }
                count = Some(parse_int(&args[i], "count")?);
            }
            "AGGREGATION" => {
                i += 1;
                if i + 1 >= args.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "AGGREGATION requires type and bucket duration".to_string(),
                    });
                }
                let agg_str =
                    std::str::from_utf8(&args[i]).map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid aggregation type".to_string(),
                    })?;
                let agg =
                    Aggregation::parse(agg_str).ok_or_else(|| CommandError::InvalidArgument {
                        message: format!("Unknown aggregation type: {}", agg_str),
                    })?;
                i += 1;
                let bucket: i64 = parse_int(&args[i], "bucket duration")?;
                aggregation = Some((agg, bucket));
            }
            _ => {
                return Err(CommandError::InvalidArgument {
                    message: format!("Unknown option: {}", opt),
                });
            }
        }
        i += 1;
    }

    match ctx.store.get(key) {
        Some(value) => {
            let ts = value.as_timeseries().ok_or(CommandError::WrongType)?;

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
    fn name(&self) -> &'static str {
        "TS.INFO"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 1, max: 2 } // TS.INFO key [DEBUG]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let ts = value.as_timeseries().ok_or(CommandError::WrongType)?;

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

                Ok(Response::Array(result))
            }
            None => Err(CommandError::InvalidArgument {
                message: "TSDB: the key does not exist".to_string(),
            }),
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
