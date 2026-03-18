//! T-Digest commands.
//!
//! Commands for approximate quantile estimation on streaming data using
//! the t-digest probabilistic data structure.

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, TDigestValue, Value, WalStrategy};
use frogdb_protocol::Response;

/// Parse a float from a byte slice.
fn parse_f64_arg(arg: &[u8]) -> Result<f64, CommandError> {
    std::str::from_utf8(arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "Invalid float value".to_string(),
        })?
        .parse::<f64>()
        .map_err(|_| CommandError::InvalidArgument {
            message: "Invalid float value".to_string(),
        })
}

/// Format a float for response, using "nan", "inf", "-inf" for special values.
fn f64_response(v: f64) -> Response {
    if v.is_nan() {
        Response::bulk(Bytes::from("nan"))
    } else if v.is_infinite() {
        if v.is_sign_positive() {
            Response::bulk(Bytes::from("inf"))
        } else {
            Response::bulk(Bytes::from("-inf"))
        }
    } else {
        Response::bulk(Bytes::from(format!("{}", v)))
    }
}

/// TDIGEST.CREATE - Create a new t-digest sketch.
///
/// TDIGEST.CREATE key [COMPRESSION compression]
pub struct TdCreate;

impl Command for TdCreate {
    fn name(&self) -> &'static str {
        "TDIGEST.CREATE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let mut compression = 100.0;
        let mut i = 1;

        while i < args.len() {
            let opt = std::str::from_utf8(&args[i])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid option".to_string(),
                })?
                .to_uppercase();
            match opt.as_str() {
                "COMPRESSION" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "COMPRESSION requires a value".to_string(),
                        });
                    }
                    compression = parse_f64_arg(&args[i])?;
                    if compression <= 0.0 {
                        return Err(CommandError::InvalidArgument {
                            message: "Compression must be greater than 0".to_string(),
                        });
                    }
                }
                _ => {
                    return Err(CommandError::InvalidArgument {
                        message: format!("Unknown option: {}", opt),
                    });
                }
            }
            i += 1;
        }

        if ctx.store.get(key).is_some() {
            return Err(CommandError::InvalidArgument {
                message: "Key already exists".to_string(),
            });
        }

        let td = TDigestValue::new(compression);
        ctx.store.set(key.clone(), Value::TDigest(td));

        Ok(Response::ok())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}

/// TDIGEST.ADD - Add values to the t-digest sketch.
///
/// TDIGEST.ADD key value [value ...]
pub struct TdAdd;

impl Command for TdAdd {
    fn name(&self) -> &'static str {
        "TDIGEST.ADD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let values: Vec<f64> = args[1..]
            .iter()
            .map(|a| parse_f64_arg(a))
            .collect::<Result<Vec<_>, _>>()?;

        match ctx.store.get_mut(key) {
            Some(value) => {
                let td = value.as_tdigest_mut().ok_or(CommandError::WrongType)?;
                for v in values {
                    td.add(v);
                }
            }
            None => {
                // Auto-create with default compression
                let mut td = TDigestValue::new(100.0);
                for v in values {
                    td.add(v);
                }
                ctx.store.set(key.clone(), Value::TDigest(td));
            }
        }

        Ok(Response::ok())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}

/// TDIGEST.MERGE - Merge multiple t-digest sketches into a destination.
///
/// TDIGEST.MERGE destkey numkeys src [src ...] [COMPRESSION compression] [OVERRIDE]
pub struct TdMerge;

impl Command for TdMerge {
    fn name(&self) -> &'static str {
        "TDIGEST.MERGE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let dest_key = &args[0];
        let numkeys: usize = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid numkeys".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid numkeys".to_string(),
            })?;

        if numkeys == 0 || 2 + numkeys > args.len() {
            return Err(CommandError::InvalidArgument {
                message: "Invalid number of source keys".to_string(),
            });
        }

        let source_keys = &args[2..2 + numkeys];

        let mut compression: Option<f64> = None;
        let mut override_flag = false;
        let mut i = 2 + numkeys;
        while i < args.len() {
            let opt = std::str::from_utf8(&args[i])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid option".to_string(),
                })?
                .to_uppercase();
            match opt.as_str() {
                "COMPRESSION" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "COMPRESSION requires a value".to_string(),
                        });
                    }
                    let c = parse_f64_arg(&args[i])?;
                    if c <= 0.0 {
                        return Err(CommandError::InvalidArgument {
                            message: "Compression must be greater than 0".to_string(),
                        });
                    }
                    compression = Some(c);
                }
                "OVERRIDE" => {
                    override_flag = true;
                }
                _ => {
                    return Err(CommandError::InvalidArgument {
                        message: format!("Unknown option: {}", opt),
                    });
                }
            }
            i += 1;
        }

        // Clone source t-digest values (to avoid borrow conflicts)
        let mut source_values: Vec<TDigestValue> = Vec::new();
        for sk in source_keys {
            if let Some(val) = ctx.store.get(sk) {
                let td = val.as_tdigest().ok_or(CommandError::WrongType)?;
                source_values.push(td.clone());
            }
            // Skip missing source keys
        }

        // Determine compression for the destination
        let dest_compression = if let Some(c) = compression {
            c
        } else if !override_flag {
            if let Some(existing) = ctx.store.get(dest_key) {
                let td = existing.as_tdigest().ok_or(CommandError::WrongType)?;
                td.compression()
            } else {
                // Use max compression from sources, or default 100
                source_values
                    .iter()
                    .map(|td| td.compression())
                    .fold(0.0f64, f64::max)
                    .max(100.0)
            }
        } else {
            // OVERRIDE without COMPRESSION: use max of sources or default
            source_values
                .iter()
                .map(|td| td.compression())
                .fold(0.0f64, f64::max)
                .max(100.0)
        };

        // Create or reset destination
        let mut dest = if override_flag || ctx.store.get(dest_key).is_none() {
            TDigestValue::new(dest_compression)
        } else {
            let existing = ctx.store.get(dest_key).unwrap();
            let td = existing.as_tdigest().ok_or(CommandError::WrongType)?;
            let mut cloned = td.clone();
            if compression.is_some() {
                cloned.set_compression(dest_compression);
            }
            cloned
        };

        for src in &source_values {
            dest.merge_from(src);
        }

        ctx.store.set(dest_key.clone(), Value::TDigest(dest));

        Ok(Response::ok())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 3 {
            return if args.is_empty() { vec![] } else { vec![&args[0]] };
        }
        let numkeys: usize = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let mut keys = vec![&args[0][..]];
        for sk in args.iter().skip(2).take(numkeys) {
            keys.push(sk);
        }
        keys
    }
}

/// TDIGEST.RESET - Reset a t-digest sketch.
///
/// TDIGEST.RESET key
pub struct TdReset;

impl Command for TdReset {
    fn name(&self) -> &'static str {
        "TDIGEST.RESET"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_mut(key) {
            Some(value) => {
                let td = value.as_tdigest_mut().ok_or(CommandError::WrongType)?;
                td.reset();
                Ok(Response::ok())
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}

/// TDIGEST.QUANTILE - Estimate values at given quantiles.
///
/// TDIGEST.QUANTILE key quantile [quantile ...]
pub struct TdQuantile;

impl Command for TdQuantile {
    fn name(&self) -> &'static str {
        "TDIGEST.QUANTILE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let quantiles: Vec<f64> = args[1..]
            .iter()
            .map(|a| parse_f64_arg(a))
            .collect::<Result<Vec<_>, _>>()?;

        match ctx.store.get_mut(key) {
            Some(value) => {
                let td = value.as_tdigest_mut().ok_or(CommandError::WrongType)?;
                let results: Vec<Response> = quantiles
                    .iter()
                    .map(|&q| f64_response(td.quantile(q)))
                    .collect();
                Ok(Response::Array(results))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}

/// TDIGEST.CDF - Estimate the CDF at given values.
///
/// TDIGEST.CDF key value [value ...]
pub struct TdCdf;

impl Command for TdCdf {
    fn name(&self) -> &'static str {
        "TDIGEST.CDF"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let values: Vec<f64> = args[1..]
            .iter()
            .map(|a| parse_f64_arg(a))
            .collect::<Result<Vec<_>, _>>()?;

        match ctx.store.get_mut(key) {
            Some(value) => {
                let td = value.as_tdigest_mut().ok_or(CommandError::WrongType)?;
                let results: Vec<Response> = values
                    .iter()
                    .map(|&v| f64_response(td.cdf(v)))
                    .collect();
                Ok(Response::Array(results))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}

/// TDIGEST.RANK - Estimate the rank of given values.
///
/// TDIGEST.RANK key value [value ...]
pub struct TdRank;

impl Command for TdRank {
    fn name(&self) -> &'static str {
        "TDIGEST.RANK"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let values: Vec<f64> = args[1..]
            .iter()
            .map(|a| parse_f64_arg(a))
            .collect::<Result<Vec<_>, _>>()?;

        match ctx.store.get_mut(key) {
            Some(value) => {
                let td = value.as_tdigest_mut().ok_or(CommandError::WrongType)?;
                let results: Vec<Response> = values
                    .iter()
                    .map(|&v| Response::Integer(td.rank(v)))
                    .collect();
                Ok(Response::Array(results))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}

/// TDIGEST.REVRANK - Estimate the reverse rank of given values.
///
/// TDIGEST.REVRANK key value [value ...]
pub struct TdRevrank;

impl Command for TdRevrank {
    fn name(&self) -> &'static str {
        "TDIGEST.REVRANK"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let values: Vec<f64> = args[1..]
            .iter()
            .map(|a| parse_f64_arg(a))
            .collect::<Result<Vec<_>, _>>()?;

        match ctx.store.get_mut(key) {
            Some(value) => {
                let td = value.as_tdigest_mut().ok_or(CommandError::WrongType)?;
                let results: Vec<Response> = values
                    .iter()
                    .map(|&v| Response::Integer(td.revrank(v)))
                    .collect();
                Ok(Response::Array(results))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}

/// TDIGEST.MIN - Get the minimum value observed.
///
/// TDIGEST.MIN key
pub struct TdMin;

impl Command for TdMin {
    fn name(&self) -> &'static str {
        "TDIGEST.MIN"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let td = value.as_tdigest().ok_or(CommandError::WrongType)?;
                Ok(f64_response(td.min()))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}

/// TDIGEST.MAX - Get the maximum value observed.
///
/// TDIGEST.MAX key
pub struct TdMax;

impl Command for TdMax {
    fn name(&self) -> &'static str {
        "TDIGEST.MAX"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let td = value.as_tdigest().ok_or(CommandError::WrongType)?;
                Ok(f64_response(td.max()))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}

/// TDIGEST.INFO - Return information about the t-digest sketch.
///
/// TDIGEST.INFO key
pub struct TdInfo;

impl Command for TdInfo {
    fn name(&self) -> &'static str {
        "TDIGEST.INFO"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let td = value.as_tdigest().ok_or(CommandError::WrongType)?;

                Ok(Response::Array(vec![
                    Response::bulk(Bytes::from("Compression")),
                    Response::Integer(td.compression() as i64),
                    Response::bulk(Bytes::from("Capacity")),
                    Response::Integer(td.num_centroids() as i64),
                    Response::bulk(Bytes::from("Merged nodes")),
                    Response::Integer(td.centroids().len() as i64),
                    Response::bulk(Bytes::from("Unmerged nodes")),
                    Response::Integer(td.unmerged().len() as i64),
                    Response::bulk(Bytes::from("Merged weight")),
                    Response::bulk(Bytes::from(format!("{}", td.merged_weight()))),
                    Response::bulk(Bytes::from("Unmerged weight")),
                    Response::bulk(Bytes::from(format!("{}", td.unmerged_weight()))),
                    Response::bulk(Bytes::from("Total compressions")),
                    Response::Integer(0), // Not tracked, matches Redis placeholder
                    Response::bulk(Bytes::from("Memory usage")),
                    Response::Integer(td.memory_size() as i64),
                ]))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}

/// TDIGEST.TRIMMED_MEAN - Estimate the trimmed mean between two quantiles.
///
/// TDIGEST.TRIMMED_MEAN key low_quantile high_quantile
pub struct TdTrimmedMean;

impl Command for TdTrimmedMean {
    fn name(&self) -> &'static str {
        "TDIGEST.TRIMMED_MEAN"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let low_q = parse_f64_arg(&args[1])?;
        let high_q = parse_f64_arg(&args[2])?;

        match ctx.store.get_mut(key) {
            Some(value) => {
                let td = value.as_tdigest_mut().ok_or(CommandError::WrongType)?;
                Ok(f64_response(td.trimmed_mean(low_q, high_q)))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}
