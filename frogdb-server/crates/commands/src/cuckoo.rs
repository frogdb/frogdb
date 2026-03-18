//! Cuckoo filter commands.
//!
//! Commands for probabilistic set membership testing with deletion and counting
//! support using scalable cuckoo filters.

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, CuckooFilterValue, Value,
    WalStrategy,
};
use frogdb_protocol::Response;

/// CF.RESERVE - Create a new cuckoo filter.
///
/// CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion]
pub struct CfReserve;

impl Command for CfReserve {
    fn name(&self) -> &'static str {
        "CF.RESERVE"
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
        let mut i = 2;

        while i < args.len() {
            let opt = std::str::from_utf8(&args[i])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid option".to_string(),
                })?
                .to_uppercase();
            match opt.as_str() {
                "BUCKETSIZE" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "BUCKETSIZE requires a value".to_string(),
                        });
                    }
                    bucket_size = std::str::from_utf8(&args[i])
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid bucket size".to_string(),
                        })?
                        .parse()
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid bucket size".to_string(),
                        })?;
                }
                "MAXITERATIONS" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "MAXITERATIONS requires a value".to_string(),
                        });
                    }
                    max_iterations = std::str::from_utf8(&args[i])
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid max iterations".to_string(),
                        })?
                        .parse()
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid max iterations".to_string(),
                        })?;
                }
                "EXPANSION" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "EXPANSION requires a value".to_string(),
                        });
                    }
                    expansion = std::str::from_utf8(&args[i])
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid expansion".to_string(),
                        })?
                        .parse()
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid expansion".to_string(),
                        })?;
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

        let cf = CuckooFilterValue::with_options(capacity, bucket_size, max_iterations, expansion);
        ctx.store.set(key.clone(), Value::CuckooFilter(cf));

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

/// CF.ADD - Add an item to the cuckoo filter.
///
/// CF.ADD key item
pub struct CfAdd;

impl Command for CfAdd {
    fn name(&self) -> &'static str {
        "CF.ADD"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let item = &args[1];

        match ctx.store.get_mut(key) {
            Some(value) => {
                let cf = value
                    .as_cuckoo_filter_mut()
                    .ok_or(CommandError::WrongType)?;
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

/// CF.ADDNX - Add an item to the cuckoo filter only if it doesn't exist.
///
/// CF.ADDNX key item
pub struct CfAddnx;

impl Command for CfAddnx {
    fn name(&self) -> &'static str {
        "CF.ADDNX"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let item = &args[1];

        let added = match ctx.store.get_mut(key) {
            Some(value) => {
                let cf = value
                    .as_cuckoo_filter_mut()
                    .ok_or(CommandError::WrongType)?;
                cf.add_nx(item).map_err(|_| CommandError::InvalidArgument {
                    message: "Filter is full".to_string(),
                })?
            }
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

/// CF.INSERT - Insert items into a cuckoo filter, with options.
///
/// CF.INSERT key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
pub struct CfInsert;

impl Command for CfInsert {
    fn name(&self) -> &'static str {
        "CF.INSERT"
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
        cf_insert_impl(ctx, args, false)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

/// CF.INSERTNX - Insert items into a cuckoo filter only if they don't exist.
///
/// CF.INSERTNX key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
pub struct CfInsertnx;

impl Command for CfInsertnx {
    fn name(&self) -> &'static str {
        "CF.INSERTNX"
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
        cf_insert_impl(ctx, args, true)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
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
    let mut items_start = None;

    let mut i = 1;
    while i < args.len() {
        let opt = std::str::from_utf8(&args[i])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid option".to_string(),
            })?
            .to_uppercase();
        match opt.as_str() {
            "CAPACITY" => {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "CAPACITY requires a value".to_string(),
                    });
                }
                capacity = std::str::from_utf8(&args[i])
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid capacity".to_string(),
                    })?
                    .parse()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid capacity".to_string(),
                    })?;
            }
            "NOCREATE" => {
                nocreate = true;
            }
            "ITEMS" => {
                items_start = Some(i + 1);
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

    let items_start = items_start.ok_or_else(|| CommandError::InvalidArgument {
        message: "ITEMS is required".to_string(),
    })?;
    let items = &args[items_start..];

    if items.is_empty() {
        return Err(CommandError::InvalidArgument {
            message: "At least one item is required".to_string(),
        });
    }

    let results: Vec<Response> = match ctx.store.get_mut(key) {
        Some(value) => {
            let cf = value
                .as_cuckoo_filter_mut()
                .ok_or(CommandError::WrongType)?;
            items
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
                .collect()
        }
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
    fn name(&self) -> &'static str {
        "CF.EXISTS"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let item = &args[1];

        match ctx.store.get(key) {
            Some(value) => {
                let cf = value.as_cuckoo_filter().ok_or(CommandError::WrongType)?;
                Ok(Response::Integer(if cf.exists(item) { 1 } else { 0 }))
            }
            None => Ok(Response::Integer(0)),
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

/// CF.MEXISTS - Check if multiple items exist in the cuckoo filter.
///
/// CF.MEXISTS key item [item ...]
pub struct CfMexists;

impl Command for CfMexists {
    fn name(&self) -> &'static str {
        "CF.MEXISTS"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let items = &args[1..];

        match ctx.store.get(key) {
            Some(value) => {
                let cf = value.as_cuckoo_filter().ok_or(CommandError::WrongType)?;
                let results: Vec<Response> = items
                    .iter()
                    .map(|item| Response::Integer(if cf.exists(item) { 1 } else { 0 }))
                    .collect();
                Ok(Response::Array(results))
            }
            None => {
                let results: Vec<Response> = items.iter().map(|_| Response::Integer(0)).collect();
                Ok(Response::Array(results))
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

/// CF.DEL - Delete an item from the cuckoo filter.
///
/// CF.DEL key item
pub struct CfDel;

impl Command for CfDel {
    fn name(&self) -> &'static str {
        "CF.DEL"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let item = &args[1];

        match ctx.store.get_mut(key) {
            Some(value) => {
                let cf = value
                    .as_cuckoo_filter_mut()
                    .ok_or(CommandError::WrongType)?;
                let deleted = cf.delete(item);
                Ok(Response::Integer(if deleted { 1 } else { 0 }))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
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

/// CF.COUNT - Count occurrences of an item in the cuckoo filter.
///
/// CF.COUNT key item
pub struct CfCount;

impl Command for CfCount {
    fn name(&self) -> &'static str {
        "CF.COUNT"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let item = &args[1];

        match ctx.store.get(key) {
            Some(value) => {
                let cf = value.as_cuckoo_filter().ok_or(CommandError::WrongType)?;
                Ok(Response::Integer(cf.count(item) as i64))
            }
            None => Ok(Response::Integer(0)),
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

/// CF.INFO - Return information about the cuckoo filter.
///
/// CF.INFO key
pub struct CfInfo;

impl Command for CfInfo {
    fn name(&self) -> &'static str {
        "CF.INFO"
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
                let cf = value.as_cuckoo_filter().ok_or(CommandError::WrongType)?;

                Ok(Response::Array(vec![
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
                ]))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
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

/// CF.SCANDUMP - Begin an incremental save of the cuckoo filter.
///
/// CF.SCANDUMP key iterator
pub struct CfScandump;

impl Command for CfScandump {
    fn name(&self) -> &'static str {
        "CF.SCANDUMP"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
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

        match ctx.store.get(key) {
            Some(value) => {
                let cf = value.as_cuckoo_filter().ok_or(CommandError::WrongType)?;

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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

/// CF.LOADCHUNK - Restore a cuckoo filter from a dump.
///
/// CF.LOADCHUNK key iterator data
pub struct CfLoadchunk;

impl Command for CfLoadchunk {
    fn name(&self) -> &'static str {
        "CF.LOADCHUNK"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}
