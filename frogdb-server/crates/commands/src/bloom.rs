//! Bloom filter commands.
//!
//! Commands for probabilistic set membership testing using scalable bloom filters.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, BloomFilterValue, BloomLayer, Command, CommandContext,
    CommandError, CommandFlags, CommandSpec, EventSpec, ExecutionStrategy, KeySpec, LookupSpec,
    StoreTypedFamilyExt, Value, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::utils::{flag_value_named, safe_capacity};

/// BF.RESERVE - Create a new bloom filter.
///
/// BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
pub struct BfReserve;

impl Command for BfReserve {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BF.RESERVE",
            docs: frogdb_core::CommandDocs {
                summary: "Creates an empty Bloom filter with a given error rate and capacity.",
                since: "1.0.0",
                group: "bloom-filter",
                complexity: None,
            },
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE.union(CommandFlags::DENYOOM),
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
        let mut parser = ArgParser::from_position(args, 3);
        while parser.has_more() {
            if parser.try_flag(b"EXPANSION") {
                expansion = flag_value_named(&mut parser, "EXPANSION", "Invalid expansion")?;
            } else if parser.try_flag(b"NONSCALING") {
                non_scaling = true;
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
}

/// BF.ADD - Add an item to the bloom filter.
///
/// BF.ADD key item
pub struct BfAdd;

impl Command for BfAdd {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BF.ADD",
            docs: frogdb_core::CommandDocs {
                summary: "Adds an item to a Bloom filter, creating the filter if it does not exist.",
                since: "1.0.0",
                group: "bloom-filter",
                complexity: None,
            },
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE
                .union(CommandFlags::FAST)
                .union(CommandFlags::DENYOOM),
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

        // Get or create the bloom filter
        let added = match ctx.store.get_bloom_mut(key)? {
            Some(bf) => bf.add(item),
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
}

/// BF.MADD - Add multiple items to the bloom filter.
///
/// BF.MADD key item [item ...]
pub struct BfMadd;

impl Command for BfMadd {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BF.MADD",
            docs: frogdb_core::CommandDocs {
                summary: "Adds one or more items to a Bloom filter, creating the filter if it does not exist.",
                since: "1.0.0",
                group: "bloom-filter",
                complexity: None,
            },
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::DENYOOM),
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
        let items = &args[1..];

        // Get or create the bloom filter
        let results: Vec<Response> = match ctx.store.get_bloom_mut(key)? {
            Some(bf) => items
                .iter()
                .map(|item| Response::Integer(if bf.add(item) { 1 } else { 0 }))
                .collect(),
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
}

/// BF.EXISTS - Check if an item exists in the bloom filter.
///
/// BF.EXISTS key item
pub struct BfExists;

impl Command for BfExists {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BF.EXISTS",
            docs: frogdb_core::CommandDocs {
                summary: "Checks whether an item may have been added to a Bloom filter.",
                since: "1.0.0",
                group: "bloom-filter",
                complexity: None,
            },
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

        match ctx.store.get_bloom(key)? {
            Some(bf) => Ok(Response::Integer(if bf.contains(item) { 1 } else { 0 })),
            None => Ok(Response::Integer(0)),
        }
    }
}

/// BF.MEXISTS - Check if multiple items exist in the bloom filter.
///
/// BF.MEXISTS key item [item ...]
pub struct BfMexists;

impl Command for BfMexists {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BF.MEXISTS",
            docs: frogdb_core::CommandDocs {
                summary: "Checks whether each of several items may have been added to a Bloom filter.",
                since: "1.0.0",
                group: "bloom-filter",
                complexity: None,
            },
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

        let Some(bf) = ctx.store.get_bloom(key)? else {
            let results: Vec<Response> = items.iter().map(|_| Response::Integer(0)).collect();
            return Ok(Response::Array(results));
        };
        let results: Vec<Response> = items
            .iter()
            .map(|item| Response::Integer(if bf.contains(item) { 1 } else { 0 }))
            .collect();
        Ok(Response::Array(results))
    }
}

/// BF.INSERT - Insert items into a bloom filter, with options.
///
/// BF.INSERT key [CAPACITY capacity] [ERROR error_rate] [EXPANSION expansion] [NOCREATE] [NONSCALING] ITEMS item [item ...]
pub struct BfInsert;

impl Command for BfInsert {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BF.INSERT",
            docs: frogdb_core::CommandDocs {
                summary: "Adds one or more items to a Bloom filter, with optional creation parameters.",
                since: "1.0.0",
                group: "bloom-filter",
                complexity: None,
            },
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE.union(CommandFlags::DENYOOM),
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

        // Parse options
        let mut capacity = 100u64;
        let mut error_rate = 0.01f64;
        let mut expansion = 2u32;
        let mut nocreate = false;
        let mut non_scaling = false;
        let mut items = None;

        let mut parser = ArgParser::from_position(args, 1);
        while parser.has_more() {
            if parser.try_flag(b"CAPACITY") {
                capacity = flag_value_named(&mut parser, "CAPACITY", "Invalid capacity")?;
            } else if parser.try_flag(b"ERROR") {
                error_rate = flag_value_named(&mut parser, "ERROR", "Invalid error rate")?;
            } else if parser.try_flag(b"EXPANSION") {
                expansion = flag_value_named(&mut parser, "EXPANSION", "Invalid expansion")?;
            } else if parser.try_flag(b"NOCREATE") {
                nocreate = true;
            } else if parser.try_flag(b"NONSCALING") {
                non_scaling = true;
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

        // Get or create the bloom filter
        let results: Vec<Response> = match ctx.store.get_bloom_mut(key)? {
            Some(bf) => items
                .iter()
                .map(|item| Response::Integer(if bf.add(item) { 1 } else { 0 }))
                .collect(),
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
}

/// BF.INFO - Return information about the bloom filter.
///
/// BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]
pub struct BfInfo;

impl Command for BfInfo {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BF.INFO",
            docs: frogdb_core::CommandDocs {
                summary: "Returns capacity, size, and sub-filter information about a Bloom filter.",
                since: "1.0.0",
                group: "bloom-filter",
                complexity: None,
            },
            arity: Arity::Range { min: 1, max: 2 },
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

        match ctx.store.get_bloom(key)? {
            Some(bf) => {
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
}

/// BF.CARD - Return the cardinality (number of items) in the bloom filter.
///
/// BF.CARD key
pub struct BfCard;

impl Command for BfCard {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BF.CARD",
            docs: frogdb_core::CommandDocs {
                summary: "Returns the number of items added to a Bloom filter.",
                since: "1.0.0",
                group: "bloom-filter",
                complexity: None,
            },
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

        match ctx.store.get_bloom(key)? {
            Some(bf) => Ok(Response::Integer(bf.count() as i64)),
            None => Ok(Response::Integer(0)),
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
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BF.SCANDUMP",
            docs: frogdb_core::CommandDocs {
                summary: "Returns successive chunks of a Bloom filter for incremental serialization.",
                since: "1.0.0",
                group: "bloom-filter",
                complexity: None,
            },
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

        match ctx.store.get_bloom(key)? {
            Some(bf) => {
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
}

/// BF.LOADCHUNK - Restore a bloom filter from a dump.
///
/// BF.LOADCHUNK key iterator data
pub struct BfLoadchunk;

impl Command for BfLoadchunk {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BF.LOADCHUNK",
            docs: frogdb_core::CommandDocs {
                summary: "Restores a Bloom filter chunk previously produced by BF.SCANDUMP.",
                since: "1.0.0",
                group: "bloom-filter",
                complexity: None,
            },
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE.union(CommandFlags::DENYOOM),
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

        // `num_layers` is client-controlled. Every layer carries at least 28 bytes of
        // header (k + count + capacity + bits_len), so a count larger than the payload
        // could possibly hold is a lie — reject it before it reaches an allocation.
        if num_layers > (data.len() - offset) / 28 {
            return Err(CommandError::InvalidArgument {
                message: "Data truncated".to_string(),
            });
        }

        let mut layers = Vec::with_capacity(safe_capacity(num_layers, 28, data.len() - offset));

        for _ in 0..num_layers {
            if data.len() - offset < 28 {
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

            // `bits_len` is client-controlled: compare against the bytes that remain
            // rather than adding to `offset`, which would overflow for a declared
            // length near `usize::MAX` and wrap the guard open.
            let bytes_needed = bits_len.div_ceil(8);
            if bytes_needed > data.len() - offset {
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
}

#[cfg(test)]
mod flag_value_pin_tests {
    //! Wire-compat pins for BF.RESERVE / BF.INSERT named-flag value parsing:
    //! the derived `"<FLAG> requires a value"` message and the bespoke
    //! `"Invalid <thing>"` parse-failure message per flag.
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
    fn reserve_expansion_requires_value() {
        assert_eq!(
            err_of(BfReserve, &["k", "0.01", "100", "EXPANSION"]),
            "EXPANSION requires a value"
        );
    }

    #[test]
    fn reserve_expansion_invalid() {
        assert_eq!(
            err_of(BfReserve, &["k", "0.01", "100", "EXPANSION", "abc"]),
            "Invalid expansion"
        );
    }

    #[test]
    fn insert_capacity_requires_value() {
        assert_eq!(
            err_of(BfInsert, &["k", "CAPACITY"]),
            "CAPACITY requires a value"
        );
    }

    #[test]
    fn insert_capacity_invalid() {
        assert_eq!(
            err_of(BfInsert, &["k", "CAPACITY", "abc"]),
            "Invalid capacity"
        );
    }

    #[test]
    fn insert_error_requires_value() {
        assert_eq!(err_of(BfInsert, &["k", "ERROR"]), "ERROR requires a value");
    }

    #[test]
    fn insert_error_invalid() {
        assert_eq!(
            err_of(BfInsert, &["k", "ERROR", "notafloat"]),
            "Invalid error rate"
        );
    }

    #[test]
    fn insert_expansion_requires_value() {
        assert_eq!(
            err_of(BfInsert, &["k", "EXPANSION"]),
            "EXPANSION requires a value"
        );
    }

    #[test]
    fn insert_expansion_invalid() {
        assert_eq!(
            err_of(BfInsert, &["k", "EXPANSION", "abc"]),
            "Invalid expansion"
        );
    }
}

#[cfg(test)]
mod loadchunk_hardening_tests {
    //! BF.LOADCHUNK decodes a payload the client fully controls. Every declared
    //! length must be validated against the bytes actually present *before* it
    //! reaches an allocation, or one small command becomes a remote OOM.
    use super::*;
    use frogdb_core::HashMapStore;
    use frogdb_protocol::ProtocolVersion;
    use std::sync::Arc;

    fn ctx() -> CommandContext<'static> {
        let store = Box::leak(Box::new(HashMapStore::new()));
        let shard_senders = Box::leak(Box::new(Arc::new(Vec::new())));
        CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2)
    }

    /// A 17-byte BF.LOADCHUNK header with a caller-chosen layer count.
    fn header(num_layers: u32) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&0.01f64.to_le_bytes());
        data.extend_from_slice(&2u32.to_le_bytes());
        data.push(0);
        data.extend_from_slice(&num_layers.to_le_bytes());
        data
    }

    fn loadchunk_err(data: Vec<u8>) -> String {
        let mut c = ctx();
        let args = vec![
            Bytes::from_static(b"k"),
            Bytes::from_static(b"0"),
            Bytes::from(data),
        ];
        match BfLoadchunk.execute(&mut c, &args) {
            Err(CommandError::InvalidArgument { message }) => message,
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    /// A layer count of `u32::MAX` against an empty layer region must be rejected
    /// on the count itself. Before the bound existed this reached
    /// `Vec::with_capacity(u32::MAX)` — hundreds of gigabytes of `BloomLayer`
    /// from a single command.
    #[test]
    fn absurd_layer_count_is_rejected_before_allocating() {
        assert_eq!(loadchunk_err(header(u32::MAX)), "Data truncated");
    }

    /// The bound is against the layer region, not merely "nonzero payload": a
    /// count of 1,000,000 with one layer's worth of bytes is still a lie.
    #[test]
    fn layer_count_is_bounded_by_remaining_bytes() {
        let mut data = header(1_000_000);
        data.extend_from_slice(&[0u8; 28]);
        assert_eq!(loadchunk_err(data), "Data truncated");
    }

    /// A count that the layer region *can* hold is admitted by the bound and
    /// then fails on the per-layer read — the bound must not be the only check.
    #[test]
    fn believable_layer_count_still_validated_per_layer() {
        let mut data = header(1);
        data.extend_from_slice(&7u32.to_le_bytes()); // k
        data.extend_from_slice(&0u64.to_le_bytes()); // count
        data.extend_from_slice(&100u64.to_le_bytes()); // capacity
        data.extend_from_slice(&64u64.to_le_bytes()); // bits_len, but no bits follow
        assert_eq!(loadchunk_err(data), "Data truncated at bits");
    }

    /// `bits_len` near `u64::MAX` makes `offset + bits_len.div_ceil(8)` wrap on a
    /// 32-bit target and is a huge allocation everywhere; the guard compares
    /// against the remaining bytes instead of adding to the offset.
    #[test]
    fn absurd_bits_len_is_rejected() {
        let mut data = header(1);
        data.extend_from_slice(&7u32.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&100u64.to_le_bytes());
        data.extend_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(loadchunk_err(data), "Data truncated at bits");
    }

    /// The hardening must not cost a real dump its round trip: SCANDUMP output
    /// fed back through LOADCHUNK reproduces a filter with the same membership.
    #[test]
    fn scandump_loadchunk_round_trip_preserves_membership() {
        let mut c = ctx();

        BfReserve
            .execute(
                &mut c,
                &[
                    Bytes::from_static(b"src"),
                    Bytes::from_static(b"0.01"),
                    Bytes::from_static(b"100"),
                ],
            )
            .unwrap();
        for item in [&b"alpha"[..], b"beta", b"gamma"] {
            BfAdd
                .execute(
                    &mut c,
                    &[Bytes::from_static(b"src"), Bytes::copy_from_slice(item)],
                )
                .unwrap();
        }

        let dump = BfScandump
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

        BfLoadchunk
            .execute(
                &mut c,
                &[Bytes::from_static(b"dst"), Bytes::from_static(b"0"), chunk],
            )
            .unwrap();

        for item in [&b"alpha"[..], b"beta", b"gamma"] {
            assert_eq!(
                BfExists
                    .execute(
                        &mut c,
                        &[Bytes::from_static(b"dst"), Bytes::copy_from_slice(item)],
                    )
                    .unwrap(),
                Response::Integer(1),
                "restored filter lost a member"
            );
        }
        assert_eq!(
            BfExists
                .execute(
                    &mut c,
                    &[Bytes::from_static(b"dst"), Bytes::from_static(b"absent")],
                )
                .unwrap(),
            Response::Integer(0),
            "restored filter gained a member"
        );
    }
}
