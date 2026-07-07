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
    AccessSpec, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    EventSpec, ExecutionStrategy, KeyAccessFlag, KeySpec, KeyspaceEventFlags, LookupSpec,
    MergeStrategy, Value, WaiterWake, WalStrategy, shard_for_key,
};
use frogdb_protocol::Response;

// ============================================================================
// TYPE - Get key type
// ============================================================================

pub struct TypeCommand;

impl Command for TypeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TYPE",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::FirstKey,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let key_type = ctx.store.key_type(key);
        Ok(Response::Simple(Bytes::from(key_type.as_str())))
    }
}

// ============================================================================
// RENAME - Rename key
// ============================================================================

pub struct RenameCommand;

impl Command for RenameCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "RENAME",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Positional(&[KeyAccessFlag::RW, KeyAccessFlag::OW]),
            wal: WalStrategy::RenameKeys,
            wakes: WaiterWake::All,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::GENERIC,
                name: "rename_from",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
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
}

// ============================================================================
// RENAMENX - Rename key if new doesn't exist
// ============================================================================

pub struct RenamenxCommand;

impl Command for RenamenxCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "RENAMENX",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Positional(&[KeyAccessFlag::RW, KeyAccessFlag::OW]),
            wal: WalStrategy::RenameKeys,
            wakes: WaiterWake::All,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::GENERIC,
                name: "rename_from",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
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
}

// ============================================================================
// TOUCH - Update last access time
// ============================================================================

pub struct TouchCommand;

impl Command for TouchCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TOUCH",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::All,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::EveryKey,
            strategy: ExecutionStrategy::ScatterGather {
                merge: MergeStrategy::SumIntegers,
            },
        };
        &SPEC
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
}

// ============================================================================
// UNLINK - Async delete (same as DEL for now)
// ============================================================================

pub struct UnlinkCommand;

impl Command for UnlinkCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "UNLINK",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::All,
            access: AccessSpec::Uniform,
            wal: WalStrategy::DeleteKeys,
            wakes: WaiterWake::All,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::GENERIC,
                name: "del",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ScatterGather {
                merge: MergeStrategy::SumIntegers,
            },
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Multi-key UNLINK: delete all keys and return count
        // Currently synchronous, async deletion can be added later
        let mut deleted = 0i64;
        for key in args {
            // Trigger lazy expiry: expired keys are cleaned up here so the
            // subsequent delete() returns false, matching Redis behavior where
            // UNLINK on an expired key returns 0 and does not dirty WATCH.
            let _ = ctx.store.get_with_expiry_check(key);

            if ctx.store.delete(key) {
                deleted += 1;
            }
        }
        // Track lazyfreed objects for INFO memory reporting
        ctx.lazyfreed_delta = deleted as u64;
        // Signal the post-execution pipeline that no data was modified so
        // it can skip incrementing the shard version (preserving WATCH state).
        if deleted == 0 {
            ctx.dirty_delta = -1;
        }
        Ok(Response::Integer(deleted))
    }
}

// ============================================================================
// OBJECT - Key introspection
// ============================================================================

pub struct ObjectCommand;

impl Command for ObjectCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "OBJECT",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY.union(CommandFlags::MOVABLEKEYS),
            keys: KeySpec::Dynamic,
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
                            Value::CuckooFilter(_) => "cuckoo",
                            Value::TDigest(_) => "tdigest",
                            Value::TopK(_) => "topk",
                            Value::CountMinSketch(_) => "cms",
                            Value::VectorSet(_) => "vectorset",
                        };
                        Ok(Response::bulk(Bytes::from(encoding)))
                    }
                    None => Err(CommandError::InvalidArgument {
                        message: "ERR no such key".to_string(),
                    }),
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

    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
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
// COPY - Copy key value to another key
// ============================================================================

pub struct CopyCommand;

impl Command for CopyCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "COPY",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Positional(&[KeyAccessFlag::R, KeyAccessFlag::OW]),
            wal: WalStrategy::PersistDestination(1),
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::GENERIC,
                name: "copy_to",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let source = &args[0];
        let dest = &args[1];

        // Parse optional arguments
        let mut replace = false;
        let mut parser = ArgParser::from_position(args, 2);
        while parser.has_more() {
            if parser.try_flag(b"REPLACE") {
                replace = true;
            } else if parser.try_flag_any(&[b"DB", b"DESTINATION-DB"]).is_some() {
                return Err(CommandError::DatabaseNotSupported { command: "COPY" });
            } else {
                let arg = parser
                    .peek()
                    .map(|a| a.to_ascii_uppercase())
                    .unwrap_or_default();
                return Err(CommandError::InvalidArgument {
                    message: format!("Unknown option: {}", String::from_utf8_lossy(&arg)),
                });
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
}

// ============================================================================
// RANDOMKEY - Return a random key from the database
// ============================================================================

pub struct RandomkeyCommand;

impl Command for RandomkeyCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "RANDOMKEY",
            arity: Arity::Fixed(0),
            flags: CommandFlags::READONLY.union(CommandFlags::RANDOM),
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
        // This command is handled specially in connection.rs via scatter-gather
        // It should never reach here in a multi-shard setup
        Err(CommandError::InvalidArgument {
            message: "RANDOMKEY should be handled by connection handler".to_string(),
        })
    }
}
