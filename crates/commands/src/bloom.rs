//! Bloom filter commands.
//!
//! Commands for probabilistic set membership testing using scalable bloom filters.

use bytes::Bytes;
use frogdb_core::{
    Arity, BloomFilterValue, BloomLayer, Command, CommandContext, CommandError, CommandFlags,
    Value, WalStrategy,
};
use frogdb_protocol::Response;

/// BF.RESERVE - Create a new bloom filter.
///
/// BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
pub struct BfReserve;

impl Command for BfReserve {
    fn name(&self) -> &'static str {
        "BF.RESERVE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let error_rate: f64 = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid error rate".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid error rate".to_string(),
            })?;

        if error_rate <= 0.0 || error_rate >= 1.0 {
            return Err(CommandError::InvalidArgument {
                message: "Error rate must be between 0 and 1 exclusive".to_string(),
            });
        }

        let capacity: u64 = std::str::from_utf8(&args[2])
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

        // Parse options
        let mut expansion = 2u32;
        let mut non_scaling = false;
        let mut i = 3;
        while i < args.len() {
            let opt = std::str::from_utf8(&args[i])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid option".to_string(),
                })?
                .to_uppercase();
            match opt.as_str() {
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
                "NONSCALING" => {
                    non_scaling = true;
                }
                _ => {
                    return Err(CommandError::InvalidArgument {
                        message: format!("Unknown option: {}", opt),
                    });
                }
            }
            i += 1;
        }

        // Check if key exists
        if ctx.store.get(key).is_some() {
            return Err(CommandError::InvalidArgument {
                message: "Key already exists".to_string(),
            });
        }

        // Create the bloom filter
        let bf = BloomFilterValue::with_options(capacity, error_rate, expansion, non_scaling);
        ctx.store.set(key.clone(), Value::BloomFilter(bf));

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

/// BF.ADD - Add an item to the bloom filter.
///
/// BF.ADD key item
pub struct BfAdd;

impl Command for BfAdd {
    fn name(&self) -> &'static str {
        "BF.ADD"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3)
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

        // Get or create the bloom filter
        let added = match ctx.store.get_mut(key) {
            Some(value) => {
                let bf = value.as_bloom_filter_mut().ok_or(CommandError::WrongType)?;
                bf.add(item)
            }
            None => {
                // Auto-create with default settings
                let mut bf = BloomFilterValue::new(100, 0.01);
                let added = bf.add(item);
                ctx.store.set(key.clone(), Value::BloomFilter(bf));
                added
            }
        };

        // Returns 1 if newly added, 0 if already existed
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

/// BF.MADD - Add multiple items to the bloom filter.
///
/// BF.MADD key item [item ...]
pub struct BfMadd;

impl Command for BfMadd {
    fn name(&self) -> &'static str {
        "BF.MADD"
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
        let key = &args[0];
        let items = &args[1..];

        // Get or create the bloom filter
        let results: Vec<Response> = match ctx.store.get_mut(key) {
            Some(value) => {
                let bf = value.as_bloom_filter_mut().ok_or(CommandError::WrongType)?;
                items
                    .iter()
                    .map(|item| Response::Integer(if bf.add(item) { 1 } else { 0 }))
                    .collect()
            }
            None => {
                // Auto-create with default settings
                let mut bf = BloomFilterValue::new(100, 0.01);
                let results: Vec<Response> = items
                    .iter()
                    .map(|item| Response::Integer(if bf.add(item) { 1 } else { 0 }))
                    .collect();
                ctx.store.set(key.clone(), Value::BloomFilter(bf));
                results
            }
        };

        Ok(Response::Array(results))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

/// BF.EXISTS - Check if an item exists in the bloom filter.
///
/// BF.EXISTS key item
pub struct BfExists;

impl Command for BfExists {
    fn name(&self) -> &'static str {
        "BF.EXISTS"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let item = &args[1];

        match ctx.store.get(key) {
            Some(value) => {
                let bf = value.as_bloom_filter().ok_or(CommandError::WrongType)?;
                Ok(Response::Integer(if bf.contains(item) { 1 } else { 0 }))
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

/// BF.MEXISTS - Check if multiple items exist in the bloom filter.
///
/// BF.MEXISTS key item [item ...]
pub struct BfMexists;

impl Command for BfMexists {
    fn name(&self) -> &'static str {
        "BF.MEXISTS"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let items = &args[1..];

        match ctx.store.get(key) {
            Some(value) => {
                let bf = value.as_bloom_filter().ok_or(CommandError::WrongType)?;
                let results: Vec<Response> = items
                    .iter()
                    .map(|item| Response::Integer(if bf.contains(item) { 1 } else { 0 }))
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

/// BF.INSERT - Insert items into a bloom filter, with options.
///
/// BF.INSERT key [CAPACITY capacity] [ERROR error_rate] [EXPANSION expansion] [NOCREATE] [NONSCALING] ITEMS item [item ...]
pub struct BfInsert;

impl Command for BfInsert {
    fn name(&self) -> &'static str {
        "BF.INSERT"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(5)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse options
        let mut capacity = 100u64;
        let mut error_rate = 0.01f64;
        let mut expansion = 2u32;
        let mut nocreate = false;
        let mut non_scaling = false;
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
                "ERROR" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "ERROR requires a value".to_string(),
                        });
                    }
                    error_rate = std::str::from_utf8(&args[i])
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid error rate".to_string(),
                        })?
                        .parse()
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid error rate".to_string(),
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
                "NOCREATE" => {
                    nocreate = true;
                }
                "NONSCALING" => {
                    non_scaling = true;
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

        // Get or create the bloom filter
        let results: Vec<Response> = match ctx.store.get_mut(key) {
            Some(value) => {
                let bf = value.as_bloom_filter_mut().ok_or(CommandError::WrongType)?;
                items
                    .iter()
                    .map(|item| Response::Integer(if bf.add(item) { 1 } else { 0 }))
                    .collect()
            }
            None => {
                if nocreate {
                    return Err(CommandError::InvalidArgument {
                        message: "Key does not exist".to_string(),
                    });
                }
                let mut bf =
                    BloomFilterValue::with_options(capacity, error_rate, expansion, non_scaling);
                let results: Vec<Response> = items
                    .iter()
                    .map(|item| Response::Integer(if bf.add(item) { 1 } else { 0 }))
                    .collect();
                ctx.store.set(key.clone(), Value::BloomFilter(bf));
                results
            }
        };

        Ok(Response::Array(results))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

/// BF.INFO - Return information about the bloom filter.
///
/// BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]
pub struct BfInfo;

impl Command for BfInfo {
    fn name(&self) -> &'static str {
        "BF.INFO"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 2, max: 3 }
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let bf = value.as_bloom_filter().ok_or(CommandError::WrongType)?;

                if args.len() == 1 {
                    // Return all info
                    Ok(Response::Array(vec![
                        Response::bulk(Bytes::from("Capacity")),
                        Response::Integer(bf.capacity() as i64),
                        Response::bulk(Bytes::from("Size")),
                        Response::Integer(bf.memory_size() as i64),
                        Response::bulk(Bytes::from("Number of filters")),
                        Response::Integer(bf.num_layers() as i64),
                        Response::bulk(Bytes::from("Number of items inserted")),
                        Response::Integer(bf.count() as i64),
                        Response::bulk(Bytes::from("Expansion rate")),
                        Response::Integer(bf.expansion() as i64),
                    ]))
                } else {
                    // Return specific info
                    let field = std::str::from_utf8(&args[1])
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid field".to_string(),
                        })?
                        .to_uppercase();
                    match field.as_str() {
                        "CAPACITY" => Ok(Response::Integer(bf.capacity() as i64)),
                        "SIZE" => Ok(Response::Integer(bf.memory_size() as i64)),
                        "FILTERS" => Ok(Response::Integer(bf.num_layers() as i64)),
                        "ITEMS" => Ok(Response::Integer(bf.count() as i64)),
                        "EXPANSION" => Ok(Response::Integer(bf.expansion() as i64)),
                        _ => Err(CommandError::InvalidArgument {
                            message: format!("Unknown field: {}", field),
                        }),
                    }
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

/// BF.CARD - Return the cardinality (number of items) in the bloom filter.
///
/// BF.CARD key
pub struct BfCard;

impl Command for BfCard {
    fn name(&self) -> &'static str {
        "BF.CARD"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let bf = value.as_bloom_filter().ok_or(CommandError::WrongType)?;
                Ok(Response::Integer(bf.count() as i64))
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

/// BF.SCANDUMP - Begin an incremental save of the bloom filter.
///
/// BF.SCANDUMP key iterator
///
/// Returns an array with the next iterator and a chunk of data.
/// When iterator is 0, the dump is complete.
pub struct BfScandump;

impl Command for BfScandump {
    fn name(&self) -> &'static str {
        "BF.SCANDUMP"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3)
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
                let bf = value.as_bloom_filter().ok_or(CommandError::WrongType)?;

                if iterator == 0 {
                    // Start a new dump - serialize the entire filter
                    let mut data = Vec::new();

                    // Header: error_rate, expansion, non_scaling, num_layers
                    data.extend_from_slice(&bf.error_rate().to_le_bytes());
                    data.extend_from_slice(&bf.expansion().to_le_bytes());
                    data.push(if bf.is_non_scaling() { 1 } else { 0 });
                    data.extend_from_slice(&(bf.num_layers() as u32).to_le_bytes());

                    // Each layer
                    for layer in bf.layers() {
                        data.extend_from_slice(&layer.k().to_le_bytes());
                        data.extend_from_slice(&layer.count().to_le_bytes());
                        data.extend_from_slice(&layer.capacity().to_le_bytes());
                        let bits_bytes = layer.bits_as_bytes();
                        data.extend_from_slice(&(layer.size_bits() as u64).to_le_bytes());
                        data.extend_from_slice(bits_bytes);
                    }

                    // Return iterator=0 (done) and the data
                    Ok(Response::Array(vec![
                        Response::Integer(0),
                        Response::bulk(Bytes::from(data)),
                    ]))
                } else {
                    // Invalid iterator - return empty
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

/// BF.LOADCHUNK - Restore a bloom filter from a dump.
///
/// BF.LOADCHUNK key iterator data
pub struct BfLoadchunk;

impl Command for BfLoadchunk {
    fn name(&self) -> &'static str {
        "BF.LOADCHUNK"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(4)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        use bitvec::prelude::*;

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

        if data.len() < 17 {
            return Err(CommandError::InvalidArgument {
                message: "Data too short".to_string(),
            });
        }

        // Parse header
        let error_rate = f64::from_le_bytes(data[0..8].try_into().unwrap());
        let expansion = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let non_scaling = data[12] != 0;
        let num_layers = u32::from_le_bytes(data[13..17].try_into().unwrap()) as usize;

        let mut offset = 17;
        let mut layers = Vec::with_capacity(num_layers);

        for _ in 0..num_layers {
            if offset + 28 > data.len() {
                return Err(CommandError::InvalidArgument {
                    message: "Data truncated".to_string(),
                });
            }

            let k = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;

            let count = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            let capacity = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            let bits_len =
                u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;

            let bytes_needed = bits_len.div_ceil(8);
            if offset + bytes_needed > data.len() {
                return Err(CommandError::InvalidArgument {
                    message: "Data truncated at bits".to_string(),
                });
            }

            let bits_bytes = &data[offset..offset + bytes_needed];
            offset += bytes_needed;

            let mut bits: BitVec<u8, Lsb0> = BitVec::from_slice(bits_bytes);
            bits.truncate(bits_len);

            layers.push(BloomLayer::from_raw(bits, k, count, capacity));
        }

        let bf = BloomFilterValue::from_raw(layers, error_rate, expansion, non_scaling);
        ctx.store.set(key.clone(), Value::BloomFilter(bf));

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
