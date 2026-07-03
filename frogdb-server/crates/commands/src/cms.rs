//! Count-Min Sketch commands.
//!
//! Commands for approximate frequency estimation using a Count-Min Sketch.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    CountMinSketchValue, EventSpec, KeySpec, LookupSpec, StoreTypedFamilyExt, Value, WaiterWake,
    WalStrategy,
};
use frogdb_protocol::Response;

/// CMS.INITBYDIM - Create a Count-Min Sketch with explicit dimensions.
///
/// CMS.INITBYDIM key width depth
pub struct CmsInitByDim;

impl Command for CmsInitByDim {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CMS.INITBYDIM",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        if ctx.store.get(key).is_some() {
            return Err(CommandError::InvalidArgument {
                message: "Key already exists".to_string(),
            });
        }

        let width: u32 = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid width".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid width".to_string(),
            })?;

        let depth: u32 = std::str::from_utf8(&args[2])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid depth".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid depth".to_string(),
            })?;

        if width == 0 || depth == 0 {
            return Err(CommandError::InvalidArgument {
                message: "width and depth must be greater than 0".to_string(),
            });
        }

        let cms = CountMinSketchValue::new(width, depth);
        ctx.store.set(key.clone(), Value::CountMinSketch(cms));

        Ok(Response::ok())
    }
}

/// CMS.INITBYPROB - Create a Count-Min Sketch with error rate and probability.
///
/// CMS.INITBYPROB key error probability
pub struct CmsInitByProb;

impl Command for CmsInitByProb {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CMS.INITBYPROB",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        if ctx.store.get(key).is_some() {
            return Err(CommandError::InvalidArgument {
                message: "Key already exists".to_string(),
            });
        }

        let error: f64 = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid error rate".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid error rate".to_string(),
            })?;

        let probability: f64 = std::str::from_utf8(&args[2])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid probability".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid probability".to_string(),
            })?;

        if error <= 0.0 || error >= 1.0 {
            return Err(CommandError::InvalidArgument {
                message: "error must be between 0 and 1 exclusive".to_string(),
            });
        }

        if probability <= 0.0 || probability >= 1.0 {
            return Err(CommandError::InvalidArgument {
                message: "probability must be between 0 and 1 exclusive".to_string(),
            });
        }

        let cms = CountMinSketchValue::from_error_and_prob(error, probability);
        ctx.store.set(key.clone(), Value::CountMinSketch(cms));

        Ok(Response::ok())
    }
}

/// CMS.INCRBY - Increment item counts in a Count-Min Sketch.
///
/// CMS.INCRBY key item increment [item increment ...]
pub struct CmsIncrBy;

impl Command for CmsIncrBy {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CMS.INCRBY",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let pairs = &args[1..];

        if !pairs.len().is_multiple_of(2) {
            return Err(CommandError::InvalidArgument {
                message: "CMS.INCRBY requires item-increment pairs".to_string(),
            });
        }

        let Some(cms) = ctx.store.get_cms_mut(key)? else {
            return Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            });
        };
        let mut results = Vec::with_capacity(pairs.len() / 2);

        for pair in pairs.chunks_exact(2) {
            let item = &pair[0];
            let increment: u64 = std::str::from_utf8(&pair[1])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid increment".to_string(),
                })?
                .parse()
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid increment".to_string(),
                })?;

            cms.increment(item, increment);
            results.push(Response::Integer(cms.query(item) as i64));
        }

        Ok(Response::Array(results))
    }
}

/// CMS.QUERY - Query item counts in a Count-Min Sketch.
///
/// CMS.QUERY key item [item ...]
pub struct CmsQuery;

impl Command for CmsQuery {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CMS.QUERY",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let items = &args[1..];

        let Some(cms) = ctx.store.get_cms(key)? else {
            let results: Vec<Response> = items.iter().map(|_| Response::Integer(0)).collect();
            return Ok(Response::Array(results));
        };
        let results: Vec<Response> = items
            .iter()
            .map(|item| Response::Integer(cms.query(item) as i64))
            .collect();
        Ok(Response::Array(results))
    }
}

/// CMS.MERGE - Merge multiple Count-Min Sketches into a destination.
///
/// CMS.MERGE dest numKeys src1 [src2 ...] [WEIGHTS w1 [w2 ...]]
pub struct CmsMerge;

impl Command for CmsMerge {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CMS.MERGE",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::DestThenNumkeys {
                numkeys: 1,
                first: 2,
            },
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let dest_key = &args[0];

        let num_keys: usize = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid numKeys".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid numKeys".to_string(),
            })?;

        if num_keys == 0 {
            return Err(CommandError::InvalidArgument {
                message: "numKeys must be greater than 0".to_string(),
            });
        }

        if args.len() < 2 + num_keys {
            return Err(CommandError::InvalidArgument {
                message: "Not enough source keys".to_string(),
            });
        }

        let source_keys: Vec<&Bytes> = args[2..2 + num_keys].iter().collect();

        // Parse optional WEIGHTS
        let mut weights = vec![1u64; num_keys];
        let remaining = &args[2 + num_keys..];
        if !remaining.is_empty() {
            let keyword = std::str::from_utf8(&remaining[0])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid option".to_string(),
                })?
                .to_uppercase();
            if keyword != "WEIGHTS" {
                return Err(CommandError::InvalidArgument {
                    message: format!("Unknown option: {}", keyword),
                });
            }
            let weight_args = &remaining[1..];
            if weight_args.len() != num_keys {
                return Err(CommandError::InvalidArgument {
                    message: "Number of weights must match number of keys".to_string(),
                });
            }
            for (i, w) in weight_args.iter().enumerate() {
                weights[i] = std::str::from_utf8(w)
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid weight".to_string(),
                    })?
                    .parse()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid weight".to_string(),
                    })?;
            }
        }

        // Read all source sketches first (before mutating dest)
        let mut width = 0u32;
        let mut depth = 0u32;
        let mut source_data: Vec<Vec<Vec<u64>>> = Vec::with_capacity(num_keys);

        for (i, src_key) in source_keys.iter().enumerate() {
            let Some(cms) = ctx.store.get_cms(src_key)? else {
                return Err(CommandError::InvalidArgument {
                    message: format!("Key '{}' does not exist", String::from_utf8_lossy(src_key)),
                });
            };
            if i == 0 {
                width = cms.width();
                depth = cms.depth();
            } else if cms.width() != width || cms.depth() != depth {
                return Err(CommandError::InvalidArgument {
                    message: "CMS dimensions must match".to_string(),
                });
            }
            source_data.push(cms.counters_raw().to_vec());
        }

        // Compute weighted sum
        let mut merged_counters = vec![vec![0u64; width as usize]; depth as usize];
        let mut merged_count = 0u64;

        for (src_idx, src_counters) in source_data.iter().enumerate() {
            let weight = weights[src_idx];
            for d in 0..depth as usize {
                for w in 0..width as usize {
                    let weighted = src_counters[d][w].saturating_mul(weight);
                    merged_counters[d][w] = merged_counters[d][w].saturating_add(weighted);
                }
            }
        }

        // Compute total count from merged counters (sum of first row as approximation)
        // Actually, we need to sum weighted source counts properly
        for (src_idx, src_key) in source_keys.iter().enumerate() {
            if let Some(value) = ctx.store.get(src_key)
                && let Some(cms) = value.as_cms()
            {
                merged_count =
                    merged_count.saturating_add(cms.count().saturating_mul(weights[src_idx]));
            }
        }

        let merged = CountMinSketchValue::from_raw(width, depth, merged_count, merged_counters);
        ctx.store
            .set(dest_key.clone(), Value::CountMinSketch(merged));

        Ok(Response::ok())
    }
}

/// CMS.INFO - Return information about a Count-Min Sketch.
///
/// CMS.INFO key
pub struct CmsInfo;

impl Command for CmsInfo {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CMS.INFO",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let Some(cms) = ctx.store.get_cms(key)? else {
            return Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            });
        };
        Ok(Response::Array(vec![
            Response::bulk(Bytes::from("width")),
            Response::Integer(cms.width() as i64),
            Response::bulk(Bytes::from("depth")),
            Response::Integer(cms.depth() as i64),
            Response::bulk(Bytes::from("count")),
            Response::Integer(cms.count() as i64),
        ]))
    }
}
