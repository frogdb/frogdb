//! Generic key commands.
//!
//! Commands that work with any key type:
//! - TYPE - get key type
//! - RENAME, RENAMENX - rename keys
//! - TOUCH - update last access time
//! - UNLINK - async delete (same as DEL for now)
//! - OBJECT ENCODING/FREQ/IDLETIME - key introspection

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy, MergeStrategy, ServerWideOp, Value, WalStrategy, extract_hash_tag,
    shard_for_key, slot_for_key,
};
use frogdb_protocol::Response;

// ============================================================================
// TYPE - Get key type
// ============================================================================

pub struct TypeCommand;

impl Command for TypeCommand {
    fn name(&self) -> &'static str {
        "TYPE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let key_type = ctx.store.key_type(key);
        Ok(Response::Simple(Bytes::from(key_type.as_str())))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// RENAME - Rename key
// ============================================================================

pub struct RenameCommand;

impl Command for RenameCommand {
    fn name(&self) -> &'static str {
        "RENAME"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::RenameKeys
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let old_key = &args[0];
        let new_key = &args[1];

        // Check same shard requirement
        let old_shard = shard_for_key(old_key, ctx.num_shards);
        let new_shard = shard_for_key(new_key, ctx.num_shards);

        if old_shard != new_shard {
            return Err(CommandError::CrossSlot);
        }

        // Get the value from old key
        let value = ctx
            .store
            .get(old_key)
            .ok_or(CommandError::InvalidArgument {
                message: "no such key".to_string(),
            })?;

        // Get expiry if any
        let expiry = ctx.store.get_expiry(old_key);

        // Delete old key
        ctx.store.delete(old_key);

        // Set new key with same value (unwrap Arc since we're moving it)
        ctx.store.set(new_key.clone(), Arc::unwrap_or_clone(value));

        // Restore expiry if any
        if let Some(expires_at) = expiry {
            ctx.store.set_expiry(new_key, expires_at);
        }

        Ok(Response::ok())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            vec![]
        } else {
            vec![&args[0], &args[1]]
        }
    }
}

// ============================================================================
// RENAMENX - Rename key if new doesn't exist
// ============================================================================

pub struct RenamenxCommand;

impl Command for RenamenxCommand {
    fn name(&self) -> &'static str {
        "RENAMENX"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::RenameKeys
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let old_key = &args[0];
        let new_key = &args[1];

        // Check same shard requirement
        let old_shard = shard_for_key(old_key, ctx.num_shards);
        let new_shard = shard_for_key(new_key, ctx.num_shards);

        if old_shard != new_shard {
            return Err(CommandError::CrossSlot);
        }

        // Check if new key exists
        if ctx.store.contains(new_key) {
            return Ok(Response::Integer(0));
        }

        // Get the value from old key
        let value = ctx
            .store
            .get(old_key)
            .ok_or(CommandError::InvalidArgument {
                message: "no such key".to_string(),
            })?;

        // Get expiry if any
        let expiry = ctx.store.get_expiry(old_key);

        // Delete old key
        ctx.store.delete(old_key);

        // Set new key with same value (unwrap Arc since we're moving it)
        ctx.store.set(new_key.clone(), Arc::unwrap_or_clone(value));

        // Restore expiry if any
        if let Some(expires_at) = expiry {
            ctx.store.set_expiry(new_key, expires_at);
        }

        Ok(Response::Integer(1))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            vec![]
        } else {
            vec![&args[0], &args[1]]
        }
    }
}

// ============================================================================
// TOUCH - Update last access time
// ============================================================================

pub struct TouchCommand;

impl Command for TouchCommand {
    fn name(&self) -> &'static str {
        "TOUCH"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ScatterGather {
            merge: MergeStrategy::SumIntegers,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Multi-key TOUCH: count how many keys were touched
        let mut touched = 0i64;
        for key in args {
            if ctx.store.touch(key) {
                touched += 1;
            }
        }
        Ok(Response::Integer(touched))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// UNLINK - Async delete (same as DEL for now)
// ============================================================================

pub struct UnlinkCommand;

impl Command for UnlinkCommand {
    fn name(&self) -> &'static str {
        "UNLINK"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::DeleteKeys
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ScatterGather {
            merge: MergeStrategy::SumIntegers,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Multi-key UNLINK: delete all keys and return count
        // Currently synchronous, async deletion can be added later
        let mut deleted = 0i64;
        for key in args {
            if ctx.store.delete(key) {
                deleted += 1;
            }
        }
        Ok(Response::Integer(deleted))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// OBJECT - Key introspection
// ============================================================================

pub struct ObjectCommand;

impl Command for ObjectCommand {
    fn name(&self) -> &'static str {
        "OBJECT"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // OBJECT subcommand [key] — HELP has no key arg
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let subcommand = args[0].to_ascii_uppercase();

        match subcommand.as_slice() {
            b"ENCODING" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "object" });
                }
                let key = &args[1];

                match ctx.store.get(key) {
                    Some(value) => {
                        let encoding = match &*value {
                            Value::String(sv) => {
                                if sv.as_integer().is_some() {
                                    "int"
                                } else {
                                    "embstr"
                                }
                            }
                            Value::SortedSet(zset) => {
                                // Redis uses listpack for small sets, skiplist for larger ones
                                if zset.len() <= 128 {
                                    "listpack"
                                } else {
                                    "skiplist"
                                }
                            }
                            Value::Hash(hash) => {
                                if hash.is_listpack() {
                                    "listpack"
                                } else {
                                    "hashtable"
                                }
                            }
                            Value::List(list) => {
                                // Redis uses quicklist (linked list of listpacks)
                                if list.len() <= 64 {
                                    "listpack"
                                } else {
                                    "quicklist"
                                }
                            }
                            Value::Set(set) => {
                                if set.is_listpack() {
                                    "listpack"
                                } else {
                                    "hashtable"
                                }
                            }
                            Value::Stream(_) => {
                                // Redis uses radix tree for streams
                                "radix-tree"
                            }
                            Value::BloomFilter(_) => {
                                // Bloom filters use a custom scalable structure
                                "bloom"
                            }
                            Value::HyperLogLog(hll) => {
                                // HyperLogLog can be sparse or dense
                                if hll.is_sparse() { "sparse" } else { "dense" }
                            }
                            Value::TimeSeries(_) => {
                                // TimeSeries uses Gorilla compression
                                "gorilla"
                            }
                            Value::Json(_) => {
                                // JSON documents use a tree structure
                                "raw"
                            }
                            Value::CuckooFilter(_) => {
                                "cuckoo"
                            }
                            Value::TDigest(_) => {
                                "tdigest"
                            }
                            Value::TopK(_) => {
                                "topk"
                            }
                            Value::CountMinSketch(_) => {
                                "cms"
                            }
                        };
                        Ok(Response::bulk(Bytes::from(encoding)))
                    }
                    None => Ok(Response::null()),
                }
            }
            b"FREQ" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "object" });
                }
                let key = &args[1];

                match ctx.store.get_metadata(key) {
                    Some(meta) => Ok(Response::Integer(meta.lfu_counter as i64)),
                    None => Ok(Response::null()),
                }
            }
            b"IDLETIME" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "object" });
                }
                let key = &args[1];

                match ctx.store.get_metadata(key) {
                    Some(meta) => {
                        let idle_secs = meta.last_access.elapsed().as_secs();
                        Ok(Response::Integer(idle_secs as i64))
                    }
                    None => Ok(Response::null()),
                }
            }
            b"REFCOUNT" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "object" });
                }
                let key = &args[1];

                if ctx.store.contains(key) {
                    Ok(Response::Integer(1)) // Always 1 (no sharing)
                } else {
                    Ok(Response::null())
                }
            }
            b"HELP" => {
                let help = vec![
                    Response::bulk(Bytes::from_static(
                        b"OBJECT <subcommand> [<arg> [value] ...]. Subcommands are:",
                    )),
                    Response::bulk(Bytes::from_static(b"ENCODING <key>")),
                    Response::bulk(Bytes::from_static(
                        b"    Return the encoding of the object stored at <key>.",
                    )),
                    Response::bulk(Bytes::from_static(b"FREQ <key>")),
                    Response::bulk(Bytes::from_static(
                        b"    Return the access frequency index of the key <key>.",
                    )),
                    Response::bulk(Bytes::from_static(b"HELP")),
                    Response::bulk(Bytes::from_static(b"    Return subcommand help summary.")),
                    Response::bulk(Bytes::from_static(b"IDLETIME <key>")),
                    Response::bulk(Bytes::from_static(
                        b"    Return the idle time of the key <key>.",
                    )),
                    Response::bulk(Bytes::from_static(b"REFCOUNT <key>")),
                    Response::bulk(Bytes::from_static(
                        b"    Return the reference count of the object stored at <key>.",
                    )),
                ];
                Ok(Response::Array(help))
            }
            _ => Err(CommandError::InvalidArgument {
                message: format!(
                    "Unknown subcommand or wrong number of arguments for '{}'",
                    String::from_utf8_lossy(&subcommand)
                ),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Key is the second argument (after subcommand)
        if args.len() >= 2 {
            let subcommand = args[0].to_ascii_uppercase();
            if subcommand != b"HELP".as_slice() {
                return vec![&args[1]];
            }
        }
        vec![]
    }
}

// ============================================================================
// DEBUG OBJECT - Debug info (simplified)
// ============================================================================

pub struct DebugCommand;

impl Command for DebugCommand {
    fn name(&self) -> &'static str {
        "DEBUG"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let subcommand = args[0].to_ascii_uppercase();

        match subcommand.as_slice() {
            b"OBJECT" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "debug" });
                }
                let key = &args[1];

                match ctx.store.get(key) {
                    Some(value) => {
                        let encoding = match &*value {
                            Value::String(sv) => {
                                if sv.as_integer().is_some() {
                                    "int"
                                } else {
                                    "embstr"
                                }
                            }
                            Value::SortedSet(zset) => {
                                if zset.len() <= 128 {
                                    "listpack"
                                } else {
                                    "skiplist"
                                }
                            }
                            Value::Hash(hash) => {
                                if hash.len() <= 64 {
                                    "listpack"
                                } else {
                                    "hashtable"
                                }
                            }
                            Value::List(list) => {
                                if list.len() <= 64 {
                                    "listpack"
                                } else {
                                    "quicklist"
                                }
                            }
                            Value::Set(set) => {
                                if set.len() <= 64 {
                                    "listpack"
                                } else {
                                    "hashtable"
                                }
                            }
                            Value::Stream(_) => "radix-tree",
                            Value::BloomFilter(_) => "bloom",
                            Value::HyperLogLog(hll) => {
                                if hll.is_sparse() {
                                    "sparse"
                                } else {
                                    "dense"
                                }
                            }
                            Value::TimeSeries(_) => "gorilla",
                            Value::Json(_) => "raw",
                            Value::CuckooFilter(_) => "cuckoo",
                            Value::TDigest(_) => "tdigest",
                            Value::TopK(_) => "topk",
                            Value::CountMinSketch(_) => "cms",
                        };
                        let info = format!(
                            "Value at:0x0 refcount:1 encoding:{} serializedlength:{} lru:0 lru_seconds_idle:0",
                            encoding,
                            value.memory_size()
                        );
                        Ok(Response::Simple(Bytes::from(info)))
                    }
                    None => Err(CommandError::InvalidArgument {
                        message: "no such key".to_string(),
                    }),
                }
            }
            b"HASHING" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "debug" });
                }

                let num_shards = ctx.num_shards;
                let results: Vec<String> = args[1..]
                    .iter()
                    .map(|key| {
                        let hash_tag = extract_hash_tag(key);
                        let hash_input = hash_tag.unwrap_or(key.as_ref());
                        let raw_hash = crc16::State::<crc16::XMODEM>::calculate(hash_input);
                        let slot = slot_for_key(key);
                        let shard = shard_for_key(key, num_shards);

                        let hash_tag_str = match hash_tag {
                            Some(tag) => String::from_utf8_lossy(tag).to_string(),
                            None => "(none)".to_string(),
                        };

                        format!(
                            "key:{} hash_tag:{} hash:0x{:04x} slot:{} shard:{} num_shards:{}",
                            String::from_utf8_lossy(key),
                            hash_tag_str,
                            raw_hash,
                            slot,
                            shard,
                            num_shards
                        )
                    })
                    .collect();

                if results.len() == 1 {
                    Ok(Response::Simple(Bytes::from(
                        results.into_iter().next().unwrap(),
                    )))
                } else {
                    Ok(Response::Array(
                        results
                            .into_iter()
                            .map(|s| Response::bulk(Bytes::from(s)))
                            .collect(),
                    ))
                }
            }
            _ => Err(CommandError::InvalidArgument {
                message: format!(
                    "Unknown subcommand or wrong number of arguments for '{}'",
                    String::from_utf8_lossy(&subcommand)
                ),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() >= 2 {
            let subcommand = args[0].to_ascii_uppercase();
            if subcommand == b"OBJECT".as_slice() {
                return vec![&args[1]];
            }
            // HASHING doesn't need routing - it's pure computation
        }
        vec![]
    }
}

// ============================================================================
// COPY - Copy key value to another key
// ============================================================================

pub struct CopyCommand;

impl Command for CopyCommand {
    fn name(&self) -> &'static str {
        "COPY"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistDestination(1)
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let source = &args[0];
        let dest = &args[1];

        // Parse optional arguments
        let mut replace = false;
        let mut i = 2;
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"REPLACE" => {
                    replace = true;
                    i += 1;
                }
                b"DB" | b"DESTINATION-DB" => {
                    return Err(CommandError::DatabaseNotSupported { command: "COPY" });
                }
                _ => {
                    return Err(CommandError::InvalidArgument {
                        message: format!("Unknown option: {}", String::from_utf8_lossy(&arg)),
                    });
                }
            }
        }

        // Check if keys are on the same shard
        let source_shard = shard_for_key(source, ctx.num_shards);
        let dest_shard = shard_for_key(dest, ctx.num_shards);

        if source_shard != dest_shard {
            // Cross-shard copy will be handled by the connection layer
            return Err(CommandError::CrossSlot);
        }

        // Same-shard copy: handle directly

        // Check if destination exists (when not using REPLACE)
        if !replace && ctx.store.contains(dest) {
            return Ok(Response::Integer(0));
        }

        // Get source value
        let value = match ctx.store.get(source) {
            Some(v) => v,
            None => return Ok(Response::Integer(0)), // Source doesn't exist
        };

        // Get source expiry
        let expiry = ctx.store.get_expiry(source);

        // If REPLACE, delete the destination first
        if replace {
            ctx.store.delete(dest);
        }

        // Set the value (unwrap Arc since we're copying it)
        ctx.store.set(dest.clone(), Arc::unwrap_or_clone(value));

        // Copy expiry if source had one
        if let Some(expires_at) = expiry {
            ctx.store.set_expiry(dest, expires_at);
        }

        Ok(Response::Integer(1))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            vec![]
        } else {
            vec![&args[0], &args[1]]
        }
    }
}

// ============================================================================
// RANDOMKEY - Return a random key from the database
// ============================================================================

pub struct RandomkeyCommand;

impl Command for RandomkeyCommand {
    fn name(&self) -> &'static str {
        "RANDOMKEY"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::RANDOM
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::RandomKey)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This command is handled specially in connection.rs via scatter-gather
        // It should never reach here in a multi-shard setup
        Err(CommandError::InvalidArgument {
            message: "RANDOMKEY should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}
