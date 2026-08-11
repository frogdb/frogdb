//! Cuckoo filter commands.
//!
//! Commands for probabilistic set membership testing with deletion and counting
//! support using scalable cuckoo filters.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    CuckooFilterValue, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt,
    Value, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::utils::{flag_value_named, safe_capacity};

/// CF.RESERVE - Create a new cuckoo filter.
///
/// CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion]
pub struct CfReserve;

impl Command for CfReserve {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.RESERVE",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let capacity: u64 = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid capacity".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid capacity".to_string(),
            })?;

        if capacity == 0 {
            return Err(CommandError::InvalidArgument {
                message: "Capacity must be greater than 0".to_string(),
            });
        }

        let mut bucket_size = 2u8;
        let mut max_iterations = 20u16;
        let mut expansion = 1u32;

        let mut parser = ArgParser::from_position(args, 2);
        while parser.has_more() {
            if parser.try_flag(b"BUCKETSIZE") {
                bucket_size = flag_value_named(&mut parser, "BUCKETSIZE", "Invalid bucket size")?;
            } else if parser.try_flag(b"MAXITERATIONS") {
                max_iterations =
                    flag_value_named(&mut parser, "MAXITERATIONS", "Invalid max iterations")?;
            } else if parser.try_flag(b"EXPANSION") {
                expansion = flag_value_named(&mut parser, "EXPANSION", "Invalid expansion")?;
            } else {
                let arg = parser.next().expect("has_more() guarantees an argument");
                let opt = std::str::from_utf8(arg)
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid option".to_string(),
                    })?
                    .to_uppercase();
                return Err(CommandError::InvalidArgument {
                    message: format!("Unknown option: {}", opt),
                });
            }
        }

        if ctx.store.get(key).is_some() {
            return Err(CommandError::InvalidArgument {
                message: "Key already exists".to_string(),
            });
        }

        let cf = CuckooFilterValue::with_options(capacity, bucket_size, max_iterations, expansion);
        ctx.store.set(key.clone(), Value::CuckooFilter(cf));

        Ok(Response::ok())
    }
}

/// CF.ADD - Add an item to the cuckoo filter.
///
/// CF.ADD key item
pub struct CfAdd;

impl Command for CfAdd {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.ADD",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let item = &args[1];

        match ctx.store.get_cuckoo_mut(key)? {
            Some(cf) => {
                cf.add(item).map_err(|_| CommandError::InvalidArgument {
                    message: "Filter is full".to_string(),
                })?;
            }
            None => {
                let mut cf = CuckooFilterValue::new(1024);
                cf.add(item).map_err(|_| CommandError::InvalidArgument {
                    message: "Filter is full".to_string(),
                })?;
                ctx.store.set(key.clone(), Value::CuckooFilter(cf));
            }
        }

        Ok(Response::ok())
    }
}

/// CF.ADDNX - Add an item to the cuckoo filter only if it doesn't exist.
///
/// CF.ADDNX key item
pub struct CfAddnx;

impl Command for CfAddnx {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.ADDNX",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let item = &args[1];

        let added = match ctx.store.get_cuckoo_mut(key)? {
            Some(cf) => cf.add_nx(item).map_err(|_| CommandError::InvalidArgument {
                message: "Filter is full".to_string(),
            })?,
            None => {
                let mut cf = CuckooFilterValue::new(1024);
                let added = cf.add_nx(item).map_err(|_| CommandError::InvalidArgument {
                    message: "Filter is full".to_string(),
                })?;
                ctx.store.set(key.clone(), Value::CuckooFilter(cf));
                added
            }
        };

        Ok(Response::Integer(if added { 1 } else { 0 }))
    }
}

/// CF.INSERT - Insert items into a cuckoo filter, with options.
///
/// CF.INSERT key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
pub struct CfInsert;

impl Command for CfInsert {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.INSERT",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        cf_insert_impl(ctx, args, false)
    }
}

/// CF.INSERTNX - Insert items into a cuckoo filter only if they don't exist.
///
/// CF.INSERTNX key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
pub struct CfInsertnx;

impl Command for CfInsertnx {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.INSERTNX",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        cf_insert_impl(ctx, args, true)
    }
}

/// Shared implementation for CF.INSERT and CF.INSERTNX.
fn cf_insert_impl(
    ctx: &mut CommandContext,
    args: &[Bytes],
    nx: bool,
) -> Result<Response, CommandError> {
    let key = &args[0];

    let mut capacity = 1024u64;
    let mut nocreate = false;
    let mut items = None;

    let mut parser = ArgParser::from_position(args, 1);
    while parser.has_more() {
        if parser.try_flag(b"CAPACITY") {
            capacity = flag_value_named(&mut parser, "CAPACITY", "Invalid capacity")?;
        } else if parser.try_flag(b"NOCREATE") {
            nocreate = true;
        } else if parser.try_flag(b"ITEMS") {
            items = Some(parser.remaining());
            break;
        } else {
            let arg = parser.next().expect("has_more() guarantees an argument");
            let opt = std::str::from_utf8(arg)
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid option".to_string(),
                })?
                .to_uppercase();
            return Err(CommandError::InvalidArgument {
                message: format!("Unknown option: {}", opt),
            });
        }
    }

    let items = items.ok_or_else(|| CommandError::InvalidArgument {
        message: "ITEMS is required".to_string(),
    })?;

    if items.is_empty() {
        return Err(CommandError::InvalidArgument {
            message: "At least one item is required".to_string(),
        });
    }

    let results: Vec<Response> = match ctx.store.get_cuckoo_mut(key)? {
        Some(cf) => items
            .iter()
            .map(|item| {
                if nx {
                    match cf.add_nx(item) {
                        Ok(added) => Response::Integer(if added { 1 } else { 0 }),
                        Err(_) => Response::Integer(-1),
                    }
                } else {
                    match cf.add(item) {
                        Ok(()) => Response::Integer(1),
                        Err(_) => Response::Integer(-1),
                    }
                }
            })
            .collect(),
        None => {
            if nocreate {
                return Err(CommandError::InvalidArgument {
                    message: "Key does not exist".to_string(),
                });
            }
            let mut cf = CuckooFilterValue::new(capacity);
            let results: Vec<Response> = items
                .iter()
                .map(|item| {
                    if nx {
                        match cf.add_nx(item) {
                            Ok(added) => Response::Integer(if added { 1 } else { 0 }),
                            Err(_) => Response::Integer(-1),
                        }
                    } else {
                        match cf.add(item) {
                            Ok(()) => Response::Integer(1),
                            Err(_) => Response::Integer(-1),
                        }
                    }
                })
                .collect();
            ctx.store.set(key.clone(), Value::CuckooFilter(cf));
            results
        }
    };

    Ok(Response::Array(results))
}

/// CF.EXISTS - Check if an item exists in the cuckoo filter.
///
/// CF.EXISTS key item
pub struct CfExists;

impl Command for CfExists {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.EXISTS",
            arity: Arity::Fixed(2),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let item = &args[1];

        match ctx.store.get_cuckoo(key)? {
            Some(cf) => Ok(Response::Integer(if cf.exists(item) { 1 } else { 0 })),
            None => Ok(Response::Integer(0)),
        }
    }
}

/// CF.MEXISTS - Check if multiple items exist in the cuckoo filter.
///
/// CF.MEXISTS key item [item ...]
pub struct CfMexists;

impl Command for CfMexists {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.MEXISTS",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let items = &args[1..];

        let Some(cf) = ctx.store.get_cuckoo(key)? else {
            let results: Vec<Response> = items.iter().map(|_| Response::Integer(0)).collect();
            return Ok(Response::Array(results));
        };
        let results: Vec<Response> = items
            .iter()
            .map(|item| Response::Integer(if cf.exists(item) { 1 } else { 0 }))
            .collect();
        Ok(Response::Array(results))
    }
}

/// CF.DEL - Delete an item from the cuckoo filter.
///
/// CF.DEL key item
pub struct CfDel;

impl Command for CfDel {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.DEL",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let item = &args[1];

        match ctx.store.get_cuckoo_mut(key)? {
            Some(cf) => {
                let deleted = cf.delete(item);
                Ok(Response::Integer(if deleted { 1 } else { 0 }))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }
}

/// CF.COUNT - Count occurrences of an item in the cuckoo filter.
///
/// CF.COUNT key item
pub struct CfCount;

impl Command for CfCount {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.COUNT",
            arity: Arity::Fixed(2),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let item = &args[1];

        match ctx.store.get_cuckoo(key)? {
            Some(cf) => Ok(Response::Integer(cf.count(item) as i64)),
            None => Ok(Response::Integer(0)),
        }
    }
}

/// CF.INFO - Return information about the cuckoo filter.
///
/// CF.INFO key
pub struct CfInfo;

impl Command for CfInfo {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.INFO",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_cuckoo(key)? {
            Some(cf) => Ok(Response::Array(vec![
                Response::bulk(Bytes::from("Size")),
                Response::Integer(cf.memory_size() as i64),
                Response::bulk(Bytes::from("Number of buckets")),
                Response::Integer(cf.total_buckets() as i64),
                Response::bulk(Bytes::from("Number of filters")),
                Response::Integer(cf.num_layers() as i64),
                Response::bulk(Bytes::from("Number of items inserted")),
                Response::Integer(cf.total_count() as i64),
                Response::bulk(Bytes::from("Number of items deleted")),
                Response::Integer(cf.num_items_deleted() as i64),
                Response::bulk(Bytes::from("Bucket size")),
                Response::Integer(cf.bucket_size() as i64),
                Response::bulk(Bytes::from("Expansion rate")),
                Response::Integer(cf.expansion() as i64),
                Response::bulk(Bytes::from("Max iterations")),
                Response::Integer(cf.max_iterations() as i64),
            ])),
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }
}

/// CF.SCANDUMP - Begin an incremental save of the cuckoo filter.
///
/// CF.SCANDUMP key iterator
pub struct CfScandump;

impl Command for CfScandump {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.SCANDUMP",
            arity: Arity::Fixed(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let iterator: u64 = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid iterator".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid iterator".to_string(),
            })?;

        match ctx.store.get_cuckoo(key)? {
            Some(cf) => {
                if iterator == 0 {
                    let mut data = Vec::new();

                    // Header
                    data.push(cf.bucket_size());
                    data.extend_from_slice(&cf.max_iterations().to_le_bytes());
                    data.extend_from_slice(&cf.expansion().to_le_bytes());
                    data.extend_from_slice(&cf.delete_count().to_le_bytes());
                    data.extend_from_slice(&(cf.num_layers() as u32).to_le_bytes());

                    // Each layer
                    for layer in cf.layers() {
                        data.extend_from_slice(&(layer.num_buckets() as u64).to_le_bytes());
                        data.push(layer.bucket_size());
                        data.extend_from_slice(&layer.total_count().to_le_bytes());
                        data.extend_from_slice(&layer.capacity().to_le_bytes());
                        for bucket in layer.buckets() {
                            for &fp in bucket {
                                data.extend_from_slice(&fp.to_le_bytes());
                            }
                        }
                    }

                    Ok(Response::Array(vec![
                        Response::Integer(0),
                        Response::bulk(Bytes::from(data)),
                    ]))
                } else {
                    Ok(Response::Array(vec![Response::Integer(0), Response::Null]))
                }
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }
}

/// CF.LOADCHUNK - Restore a cuckoo filter from a dump.
///
/// CF.LOADCHUNK key iterator data
pub struct CfLoadchunk;

impl Command for CfLoadchunk {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CF.LOADCHUNK",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let iterator: u64 = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid iterator".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid iterator".to_string(),
            })?;
        let data = &args[2];

        if iterator != 0 {
            return Err(CommandError::InvalidArgument {
                message: "Invalid iterator for loadchunk".to_string(),
            });
        }

        // Parse header: bucket_size(1) + max_iterations(2) + expansion(4) + delete_count(8) + num_layers(4) = 19
        if data.len() < 19 {
            return Err(CommandError::InvalidArgument {
                message: "Data too short".to_string(),
            });
        }

        let mut offset = 0;
        let bucket_size = data[offset];
        offset += 1;
        let max_iterations = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let expansion = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let delete_count = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let num_layers = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // `num_layers` is client-controlled. Every layer carries at least 25 bytes of
        // header, so a count larger than the payload could possibly hold is a lie —
        // reject it before it reaches an allocation.
        if num_layers > (data.len() - offset) / 25 {
            return Err(CommandError::InvalidArgument {
                message: "Data truncated".to_string(),
            });
        }

        let mut layers = Vec::with_capacity(safe_capacity(num_layers, 25, data.len() - offset));

        for _ in 0..num_layers {
            // num_buckets(8) + bucket_size(1) + count(8) + capacity(8) = 25
            if data.len() - offset < 25 {
                return Err(CommandError::InvalidArgument {
                    message: "Data truncated".to_string(),
                });
            }

            let num_buckets =
                u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;
            let layer_bucket_size = data[offset];
            offset += 1;
            let count = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let capacity = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            // A layer that claims buckets but zero slots per bucket consumes no
            // fingerprint bytes, so its bucket count would be unbounded by the
            // payload length. Reject it rather than let it drive an allocation.
            if layer_bucket_size == 0 && num_buckets > 0 {
                return Err(CommandError::InvalidArgument {
                    message: "Invalid bucket size in chunk".to_string(),
                });
            }

            // Both factors are client-controlled: a raw multiply wraps in release
            // builds and hands the truncation guard below a small product for a
            // huge bucket count.
            let fp_bytes = num_buckets
                .checked_mul(layer_bucket_size as usize)
                .and_then(|v| v.checked_mul(2))
                .ok_or_else(|| CommandError::InvalidArgument {
                    message: "Invalid fingerprint data size".to_string(),
                })?;
            if fp_bytes > data.len() - offset {
                return Err(CommandError::InvalidArgument {
                    message: "Data truncated at fingerprints".to_string(),
                });
            }

            let mut buckets =
                Vec::with_capacity(safe_capacity(num_buckets, 2, data.len() - offset));
            for _ in 0..num_buckets {
                let mut bucket = Vec::with_capacity(safe_capacity(
                    layer_bucket_size as usize,
                    2,
                    data.len() - offset,
                ));
                for _ in 0..layer_bucket_size {
                    let fp = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
                    offset += 2;
                    bucket.push(fp);
                }
                buckets.push(bucket);
            }

            layers.push(frogdb_core::CuckooLayer::from_raw(
                buckets,
                num_buckets,
                layer_bucket_size,
                count,
                capacity,
            ));
        }

        let cf = CuckooFilterValue::from_raw(
            layers,
            bucket_size,
            max_iterations,
            expansion,
            delete_count,
        );
        ctx.store.set(key.clone(), Value::CuckooFilter(cf));

        Ok(Response::ok())
    }
}

#[cfg(test)]
mod flag_value_pin_tests {
    //! Wire-compat pins for CF.RESERVE / CF.INSERT named-flag value parsing.
    use super::*;
    use frogdb_core::HashMapStore;
    use frogdb_protocol::ProtocolVersion;
    use std::sync::Arc;

    fn ctx() -> CommandContext<'static> {
        let store = Box::leak(Box::new(HashMapStore::new()));
        let shard_senders = Box::leak(Box::new(Arc::new(Vec::new())));
        CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2)
    }

    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|s| Bytes::from(s.to_string())).collect()
    }

    fn err_of<C: Command>(cmd: C, parts: &[&str]) -> String {
        let mut c = ctx();
        match cmd.execute(&mut c, &args(parts)) {
            Err(CommandError::InvalidArgument { message }) => message,
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn reserve_bucketsize_requires_value() {
        assert_eq!(
            err_of(CfReserve, &["k", "100", "BUCKETSIZE"]),
            "BUCKETSIZE requires a value"
        );
    }

    #[test]
    fn reserve_bucketsize_invalid() {
        assert_eq!(
            err_of(CfReserve, &["k", "100", "BUCKETSIZE", "abc"]),
            "Invalid bucket size"
        );
    }

    #[test]
    fn reserve_maxiterations_requires_value() {
        assert_eq!(
            err_of(CfReserve, &["k", "100", "MAXITERATIONS"]),
            "MAXITERATIONS requires a value"
        );
    }

    #[test]
    fn reserve_maxiterations_invalid() {
        assert_eq!(
            err_of(CfReserve, &["k", "100", "MAXITERATIONS", "abc"]),
            "Invalid max iterations"
        );
    }

    #[test]
    fn reserve_expansion_requires_value() {
        assert_eq!(
            err_of(CfReserve, &["k", "100", "EXPANSION"]),
            "EXPANSION requires a value"
        );
    }

    #[test]
    fn reserve_expansion_invalid() {
        assert_eq!(
            err_of(CfReserve, &["k", "100", "EXPANSION", "abc"]),
            "Invalid expansion"
        );
    }

    #[test]
    fn insert_capacity_requires_value() {
        assert_eq!(
            err_of(CfInsert, &["k", "CAPACITY"]),
            "CAPACITY requires a value"
        );
    }

    #[test]
    fn insert_capacity_invalid() {
        assert_eq!(
            err_of(CfInsert, &["k", "CAPACITY", "abc"]),
            "Invalid capacity"
        );
    }
}

#[cfg(test)]
mod loadchunk_hardening_tests {
    //! CF.LOADCHUNK decodes a payload the client fully controls. Layer counts,
    //! bucket counts and the fingerprint-region size must all be validated
    //! against the bytes actually present before they reach an allocation, and
    //! the size computation must not be allowed to wrap past its own guard.
    use super::*;
    use frogdb_core::HashMapStore;
    use frogdb_protocol::ProtocolVersion;
    use std::sync::Arc;

    fn ctx() -> CommandContext<'static> {
        let store = Box::leak(Box::new(HashMapStore::new()));
        let shard_senders = Box::leak(Box::new(Arc::new(Vec::new())));
        CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2)
    }

    /// A 19-byte CF.LOADCHUNK header with a caller-chosen layer count.
    fn header(num_layers: u32) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(2); // bucket_size
        data.extend_from_slice(&20u16.to_le_bytes()); // max_iterations
        data.extend_from_slice(&1u32.to_le_bytes()); // expansion
        data.extend_from_slice(&0u64.to_le_bytes()); // delete_count
        data.extend_from_slice(&num_layers.to_le_bytes());
        data
    }

    /// A 25-byte layer header with caller-chosen bucket count and bucket size.
    fn layer_header(num_buckets: u64, bucket_size: u8) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&num_buckets.to_le_bytes());
        data.push(bucket_size);
        data.extend_from_slice(&0u64.to_le_bytes()); // count
        data.extend_from_slice(&0u64.to_le_bytes()); // capacity
        data
    }

    fn loadchunk_err(data: Vec<u8>) -> String {
        let mut c = ctx();
        let args = vec![
            Bytes::from_static(b"k"),
            Bytes::from_static(b"0"),
            Bytes::from(data),
        ];
        match CfLoadchunk.execute(&mut c, &args) {
            Err(CommandError::InvalidArgument { message }) => message,
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    /// A layer count of `u32::MAX` against an empty layer region must be rejected
    /// on the count itself, before `Vec::with_capacity` honours it.
    #[test]
    fn absurd_layer_count_is_rejected_before_allocating() {
        assert_eq!(loadchunk_err(header(u32::MAX)), "Data truncated");
    }

    /// `num_buckets * bucket_size * 2` wraps `usize` for a bucket count near
    /// `usize::MAX / 4`: in a release build the product came out as 0, sailed
    /// past the "is there room for the fingerprints?" guard, and then drove a
    /// `Vec::with_capacity` of 2^62 buckets. The checked chain rejects it.
    #[test]
    fn wrapping_fingerprint_size_is_rejected() {
        // 2^62 * 2 * 2 == 2^64, i.e. 0 once the multiply wraps.
        let num_buckets = (usize::MAX / 4 + 1) as u64;
        let mut data = header(1);
        data.extend_from_slice(&layer_header(num_buckets, 2));
        assert_eq!(loadchunk_err(data), "Invalid fingerprint data size");
    }

    /// A zero-slot bucket makes the fingerprint region zero bytes long whatever
    /// the bucket count, so the length guard cannot bound the count — the layer
    /// has to be rejected outright.
    #[test]
    fn zero_bucket_size_with_buckets_is_rejected() {
        let mut data = header(1);
        data.extend_from_slice(&layer_header(u64::MAX, 0));
        assert_eq!(loadchunk_err(data), "Invalid bucket size in chunk");
    }

    /// A bucket count whose fingerprint region does not overflow but does not
    /// fit either is caught by the length guard.
    #[test]
    fn oversized_bucket_count_is_rejected() {
        let mut data = header(1);
        data.extend_from_slice(&layer_header(1_000_000, 2));
        assert_eq!(loadchunk_err(data), "Data truncated at fingerprints");
    }

    /// The hardening must not cost a real dump its round trip: SCANDUMP output
    /// fed back through LOADCHUNK reproduces a filter with the same membership.
    #[test]
    fn scandump_loadchunk_round_trip_preserves_membership() {
        let mut c = ctx();

        CfReserve
            .execute(
                &mut c,
                &[Bytes::from_static(b"src"), Bytes::from_static(b"100")],
            )
            .unwrap();
        for item in [&b"alpha"[..], b"beta", b"gamma"] {
            CfAdd
                .execute(
                    &mut c,
                    &[Bytes::from_static(b"src"), Bytes::copy_from_slice(item)],
                )
                .unwrap();
        }

        let dump = CfScandump
            .execute(
                &mut c,
                &[Bytes::from_static(b"src"), Bytes::from_static(b"0")],
            )
            .unwrap();
        let chunk = match dump {
            Response::Array(items) => match &items[1] {
                Response::Bulk(Some(b)) => b.clone(),
                other => panic!("expected bulk chunk, got {other:?}"),
            },
            other => panic!("expected array reply, got {other:?}"),
        };

        CfLoadchunk
            .execute(
                &mut c,
                &[Bytes::from_static(b"dst"), Bytes::from_static(b"0"), chunk],
            )
            .unwrap();

        for item in [&b"alpha"[..], b"beta", b"gamma"] {
            assert_eq!(
                CfExists
                    .execute(
                        &mut c,
                        &[Bytes::from_static(b"dst"), Bytes::copy_from_slice(item)],
                    )
                    .unwrap(),
                Response::Integer(1),
                "restored filter lost a member"
            );
        }
    }
}
