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

use super::utils::flag_value_named;

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
            lookup: LookupSpec::None,
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
            lookup: LookupSpec::None,
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
            lookup: LookupSpec::None,
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
            lookup: LookupSpec::None,
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
            lookup: LookupSpec::None,
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
            lookup: LookupSpec::None,
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
            lookup: LookupSpec::None,
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
            lookup: LookupSpec::None,
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
            lookup: LookupSpec::None,
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
            lookup: LookupSpec::None,
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
            lookup: LookupSpec::None,
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
            lookup: LookupSpec::None,
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

        let mut layers = Vec::with_capacity(num_layers);

        for _ in 0..num_layers {
            // num_buckets(8) + bucket_size(1) + count(8) + capacity(8) = 25
            if offset + 25 > data.len() {
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

            let fp_bytes = num_buckets * layer_bucket_size as usize * 2;
            if offset + fp_bytes > data.len() {
                return Err(CommandError::InvalidArgument {
                    message: "Data truncated at fingerprints".to_string(),
                });
            }

            let mut buckets = Vec::with_capacity(num_buckets);
            for _ in 0..num_buckets {
                let mut bucket = Vec::with_capacity(layer_bucket_size as usize);
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
