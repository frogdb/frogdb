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
use frogdb_core::clock;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    EventSpec, ExecutionStrategy, KeyAccessFlag, KeySpec, KeyType, KeyspaceEventFlags, LookupSpec,
    ScatterGatherOp, ServerWideOp, Value, WaiterWake, WalStrategy, shard_for_key,
};
use frogdb_protocol::{Response, SafeStatus};

// ============================================================================
// TYPE - Get key type
// ============================================================================

pub struct TypeCommand;

impl Command for TypeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TYPE",
            docs: frogdb_core::CommandDocs {
                summary: "Determines the type of value stored at a key.",
                since: "1.0.0",
                group: "generic",
                complexity: Some("O(1)"),
            },
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::FirstKey,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        // Active expiry is sampled, so a key past its deadline can still be
        // physically present: gate the type read on the logical-expiry probe.
        // `exists_unexpired` (NOT `get_with_expiry_check`) keeps TYPE a pure
        // metadata probe — it must not physically purge or report a removal.
        let key_type = if ctx.store.exists_unexpired(key) {
            ctx.store.key_type(key)
        } else {
            KeyType::None
        };
        Ok(Response::Simple(SafeStatus::sanitized(key_type.as_str())))
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
            docs: frogdb_core::CommandDocs {
                summary: "Renames a key and overwrites the destination.",
                since: "1.0.0",
                group: "generic",
                complexity: Some("O(1)"),
            },
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Positional(&[KeyAccessFlag::RW, KeyAccessFlag::OW]),
            wal: WalStrategy::RenameKeys,
            wakes: WaiterWake::All,
            // Runtime-deposited (proposal 44): Redis-verified per-key names —
            // `rename_from` on the source, `rename_to` on the destination
            // (db.c renameGenericCommand:62-63). A no-op RENAMENX emits nothing.
            event: EventSpec::Dynamic,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::Rename,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
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

        // Get the value from old key. The expiry-checking read (matching
        // UNLINK below) is what makes a source past its deadline — but not yet
        // swept — behave as "no such key" instead of being resurrected under
        // the new name.
        let value =
            ctx.store
                .get_with_expiry_check(old_key)
                .ok_or(CommandError::InvalidArgument {
                    message: "no such key".to_string(),
                })?;

        // src == dst short-circuit. Redis renameGenericCommand checks samekey
        // *after* confirming the source exists (missing source is still an
        // error), then returns plain OK with no modification and no events. A
        // no-op write skips the whole effect pipeline (WAL, replication,
        // notifications, WATCH bump) — matching Redis, which returns before
        // signalModifiedKey / server.dirty++.
        if old_key == new_key {
            ctx.effects.write_was_noop = true;
            return Ok(Response::ok());
        }

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

        // Both keys were written: `rename_from` on the deleted source,
        // `rename_to` on the set destination (db.c renameGenericCommand:62-63).
        ctx.notify_event(old_key.clone(), "rename_from", KeyspaceEventFlags::GENERIC);
        ctx.notify_event(new_key.clone(), "rename_to", KeyspaceEventFlags::GENERIC);

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
            docs: frogdb_core::CommandDocs {
                summary: "Renames a key only when the target key name doesn't exist.",
                since: "1.0.0",
                group: "generic",
                complexity: Some("O(1)"),
            },
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Positional(&[KeyAccessFlag::RW, KeyAccessFlag::OW]),
            wal: WalStrategy::RenameKeys,
            wakes: WaiterWake::All,
            // Runtime-deposited (proposal 44): Redis-verified per-key names —
            // `rename_from` on the source, `rename_to` on the destination
            // (db.c renameGenericCommand:62-63). A no-op RENAMENX emits nothing.
            event: EventSpec::Dynamic,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::Rename,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
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

        // Check if new key exists. Covers RENAMENX k k on an existing k, too:
        // Redis renameGenericCommand replies czero on samekey for NX without
        // modifying anything or emitting events. Declaring the no-op skips the
        // effect pipeline (WAL, replication, notifications, WATCH bump) —
        // Redis returns before signalModifiedKey / server.dirty++.
        // Trigger lazy expiry first (matching UNLINK below): a destination past
        // its deadline does not exist, so RENAMENX must proceed rather than
        // report a spurious 0.
        let _ = ctx.store.get_with_expiry_check(new_key);
        if ctx.store.contains(new_key) {
            ctx.effects.write_was_noop = true;
            return Ok(Response::Integer(0));
        }

        // Get the value from old key (expiry-checking read: a source past its
        // deadline is "no such key", not a value to resurrect).
        let value =
            ctx.store
                .get_with_expiry_check(old_key)
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

        // Both keys were written; the no-op reply (0) above deposits nothing.
        // `rename_from` on the source, `rename_to` on the destination.
        ctx.notify_event(old_key.clone(), "rename_from", KeyspaceEventFlags::GENERIC);
        ctx.notify_event(new_key.clone(), "rename_to", KeyspaceEventFlags::GENERIC);

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
            docs: frogdb_core::CommandDocs {
                summary: "Returns the number of existing keys out of those specified after updating the time they were last accessed.",
                since: "3.2.1",
                group: "generic",
                complexity: Some("O(N) where N is the number of keys that will be touched."),
            },
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::All,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::EveryKey,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ScatterGather(ScatterGatherOp::Touch),
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
            docs: frogdb_core::CommandDocs {
                summary: "Asynchronously deletes one or more keys.",
                since: "4.0.0",
                group: "generic",
                complexity: Some(
                    "O(1) for each key removed regardless of its size. Then the command does O(N) work in a different thread in order to reclaim memory, where N is the number of allocations the deleted objects where composed of.",
                ),
            },
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
            reindex: frogdb_core::ReindexSpec::DeleteKeys,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ScatterGather(ScatterGatherOp::Unlink),
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
        ctx.effects.lazyfreed_delta = deleted as u64;
        // Signal the post-execution pipeline that no data was modified so
        // it can skip incrementing the shard version (preserving WATCH state).
        if deleted == 0 {
            ctx.effects.dirty_delta = -1;
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
            docs: frogdb_core::CommandDocs {
                summary: "A container for object introspection commands.",
                since: "2.2.3",
                group: "generic",
                complexity: Some("Depends on subcommand."),
            },
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY.union(CommandFlags::MOVABLEKEYS),
            keys: KeySpec::Dynamic,
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
        let subcommand = args[0].to_ascii_uppercase();

        match subcommand.as_slice() {
            b"ENCODING" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "object" });
                }
                let key = &args[1];

                match ctx.store.get(key) {
                    // The name comes from `Value::encoding_name` so this arm and
                    // `DEBUG OBJECT`'s `encoding:` token can never disagree.
                    Some(value) => Ok(Response::bulk(Bytes::from(value.encoding_name()))),
                    // A missing key is not an error for OBJECT ENCODING — Redis
                    // 8.6's `kvobjCommandLookupOrReply` replies `shared.null`
                    // for ENCODING/REFCOUNT/IDLETIME/FREQ alike (verified
                    // against a locally built Redis 8.6.1), so this matches
                    // the (already-correct) REFCOUNT/IDLETIME/FREQ arms below.
                    None => Ok(Response::null()),
                }
            }
            b"FREQ" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "object" });
                }
                let key = &args[1];

                match ctx.store.get_metadata(key) {
                    Some(meta) => {
                        // Redis only tracks (and reports) an LFU access
                        // counter when an LFU `maxmemory-policy` is selected
                        // (`object.c`'s FREQ arm); otherwise it errors with
                        // this exact text (verified against a locally built
                        // Redis 8.6.1) rather than answering from an untracked
                        // counter.
                        if !ctx.eviction_policy.uses_lfu() {
                            return Err(CommandError::InvalidArgument {
                                message: "An LFU maxmemory policy is not selected, access \
                                          frequency not tracked. Please note that when \
                                          switching between policies at runtime LRU and LFU \
                                          data will take some time to adjust."
                                    .to_string(),
                            });
                        }
                        Ok(Response::Integer(meta.lfu_counter as i64))
                    }
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
                        let idle_secs = clock::elapsed(meta.last_access).as_secs();
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
                    // Always 1 (no sharing) — shared with `DEBUG OBJECT`'s
                    // `refcount:` token so the two can never disagree.
                    Ok(Response::Integer(Value::REPORTED_REFCOUNT))
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
            docs: frogdb_core::CommandDocs {
                summary: "Copies the value of a key to a new key.",
                since: "6.2.0",
                group: "generic",
                complexity: Some(
                    "O(N) worst case for collections, where N is the number of nested items. O(1) for string values.",
                ),
            },
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::DENYOOM),
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Positional(&[KeyAccessFlag::R, KeyAccessFlag::OW]),
            wal: WalStrategy::PersistDestination,
            wakes: WaiterWake::None,
            event: EventSpec::EmitsAt {
                class: KeyspaceEventFlags::GENERIC,
                name: "copy_to",
                key_index: 1,
            },
            requires_same_slot: false,
            // Same-shard COPY writes the source value into the destination
            // (args[1], with REPLACE clobbering it). Refresh the destination:
            // index it when the copied value is a hash matching a prefix, else
            // drop any stale doc. Cross-shard COPY reconstructs as RESTORE on the
            // destination shard (execution.rs), which carries its own refresh.
            reindex: frogdb_core::ReindexSpec::RefreshSecondKey,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
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

        // Check if destination exists (when not using REPLACE). Nothing is
        // copied, so declare the write a no-op: skip reindex (COPY is
        // `RefreshSecondKey` — otherwise it would re-index the whole unchanged
        // destination), WAL, replication, notification, and WATCH dirty.
        if !replace && ctx.store.contains(dest) {
            ctx.effects.write_was_noop = true;
            return Ok(Response::Integer(0));
        }

        // Get source value
        let value = match ctx.store.get(source) {
            Some(v) => v,
            None => {
                // Source doesn't exist: nothing copied — same no-op contract.
                ctx.effects.write_was_noop = true;
                return Ok(Response::Integer(0));
            }
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
            docs: frogdb_core::CommandDocs {
                summary: "Returns a random key name from the database.",
                since: "1.0.0",
                group: "generic",
                complexity: Some("O(1)"),
            },
            arity: Arity::Fixed(0),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::RandomKey),
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

#[cfg(test)]
mod copy_noop_tests {
    //! COPY's no-op paths (Finding 3): a COPY that copies nothing must declare
    //! the write a no-op so the effect pipeline is skipped. COPY is
    //! `RefreshSecondKey`, so otherwise a no-op COPY would re-index the entire
    //! (unchanged) destination — plus phantom WAL / replication / notification /
    //! WATCH dirty.
    use super::*;
    use frogdb_core::HashMapStore;
    use frogdb_protocol::ProtocolVersion;

    fn ctx() -> CommandContext<'static> {
        // Single shard so COPY's source/dest always land on the same shard.
        let store = Box::leak(Box::new(HashMapStore::new()));
        let shard_senders = Box::leak(Box::new(Arc::new(Vec::new())));
        CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2)
    }

    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|s| Bytes::from(s.to_string())).collect()
    }

    /// A COPY that actually copies is a real write — not a no-op (positive
    /// control).
    #[test]
    fn copy_success_is_not_noop() {
        let mut c = ctx();
        c.store.set(
            Bytes::from_static(b"src"),
            Value::string(Bytes::from_static(b"v")),
        );
        let r = CopyCommand.execute(&mut c, &args(&["src", "dst"])).unwrap();
        assert_eq!(r, Response::Integer(1));
        assert!(!c.effects.write_was_noop, "a real COPY is not a no-op");
    }

    /// COPY with the destination already present and no REPLACE copies nothing
    /// (replies 0) — it must be a no-op write.
    #[test]
    fn copy_dest_exists_without_replace_is_noop() {
        let mut c = ctx();
        c.store.set(
            Bytes::from_static(b"src"),
            Value::string(Bytes::from_static(b"v")),
        );
        c.store.set(
            Bytes::from_static(b"dst"),
            Value::string(Bytes::from_static(b"old")),
        );
        let r = CopyCommand.execute(&mut c, &args(&["src", "dst"])).unwrap();
        assert_eq!(r, Response::Integer(0));
        assert!(
            c.effects.write_was_noop,
            "COPY that did not copy (dest exists, no REPLACE) must be a no-op write"
        );
    }

    /// COPY from a missing source copies nothing (replies 0) — also a no-op.
    #[test]
    fn copy_missing_source_is_noop() {
        let mut c = ctx();
        let r = CopyCommand
            .execute(&mut c, &args(&["absent", "dst"]))
            .unwrap();
        assert_eq!(r, Response::Integer(0));
        assert!(
            c.effects.write_was_noop,
            "COPY from a missing source must be a no-op write"
        );
    }
}

#[cfg(test)]
mod object_tests {
    //! `OBJECT ENCODING`/`FREQ` fidelity: a missing key is nil (not an
    //! error) on every subcommand, and `FREQ` is gated on an LFU
    //! `maxmemory-policy` — both verified against a locally built Redis
    //! 8.6.1.
    use super::*;
    use frogdb_core::{EvictionPolicy, HashMapStore};
    use frogdb_protocol::ProtocolVersion;

    fn ctx() -> CommandContext<'static> {
        let store = Box::leak(Box::new(HashMapStore::new()));
        let shard_senders = Box::leak(Box::new(Arc::new(Vec::new())));
        CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2)
    }

    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|s| Bytes::from(s.to_string())).collect()
    }

    /// `OBJECT ENCODING` on a missing key is a null bulk reply, not an
    /// error — the doubled-`ERR` bug this test guards against returned
    /// `CommandError::InvalidArgument { message: "ERR no such key" }`,
    /// which `Display`-rendered as `ERR ERR no such key`.
    #[test]
    fn encoding_missing_key_is_nil_not_error() {
        let mut c = ctx();
        let r = ObjectCommand
            .execute(&mut c, &args(&["ENCODING", "nosuchkey"]))
            .unwrap();
        assert_eq!(r, Response::null());
    }

    /// `OBJECT ENCODING` on an existing key is unaffected by the fix.
    #[test]
    fn encoding_existing_key_still_reports_encoding() {
        let mut c = ctx();
        c.store.set(
            Bytes::from_static(b"k"),
            Value::string(Bytes::from_static(b"v")),
        );
        let r = ObjectCommand
            .execute(&mut c, &args(&["ENCODING", "k"]))
            .unwrap();
        assert_eq!(r, Response::bulk(Bytes::from_static(b"embstr")));
    }

    /// `OBJECT FREQ` on a missing key is nil regardless of policy — Redis
    /// checks key existence before the LFU-policy gate.
    #[test]
    fn freq_missing_key_is_nil() {
        let mut c = ctx();
        c.eviction_policy = EvictionPolicy::NoEviction;
        let r = ObjectCommand
            .execute(&mut c, &args(&["FREQ", "nosuchkey"]))
            .unwrap();
        assert_eq!(r, Response::null());
    }

    /// `OBJECT FREQ` on an existing key errors, byte-for-byte matching
    /// Redis, when the configured policy is not one of the LFU variants.
    #[test]
    fn freq_existing_key_without_lfu_policy_errors() {
        let mut c = ctx();
        c.eviction_policy = EvictionPolicy::NoEviction;
        c.store.set(
            Bytes::from_static(b"k"),
            Value::string(Bytes::from_static(b"v")),
        );
        let err = ObjectCommand
            .execute(&mut c, &args(&["FREQ", "k"]))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "ERR An LFU maxmemory policy is not selected, access frequency not tracked. \
             Please note that when switching between policies at runtime LRU and LFU data \
             will take some time to adjust."
        );
    }

    /// `OBJECT FREQ` on an existing key answers with a real counter once an
    /// LFU policy is selected.
    #[test]
    fn freq_existing_key_with_lfu_policy_returns_counter() {
        let mut c = ctx();
        c.eviction_policy = EvictionPolicy::AllkeysLfu;
        c.store.set(
            Bytes::from_static(b"k"),
            Value::string(Bytes::from_static(b"v")),
        );
        let r = ObjectCommand
            .execute(&mut c, &args(&["FREQ", "k"]))
            .unwrap();
        assert!(matches!(r, Response::Integer(_)));
    }
}
