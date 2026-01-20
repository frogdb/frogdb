//! Generic key commands.
//!
//! Commands that work with any key type:
//! - TYPE - get key type
//! - RENAME, RENAMENX - rename keys
//! - TOUCH - update last access time
//! - UNLINK - async delete (same as DEL for now)
//! - OBJECT ENCODING/FREQ/IDLETIME - key introspection

use bytes::Bytes;
use frogdb_core::{shard_for_key, Arity, Command, CommandContext, CommandError, CommandFlags, Value};
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let old_key = &args[0];
        let new_key = &args[1];

        // Check same shard requirement
        let old_shard = shard_for_key(old_key, ctx.num_shards);
        let new_shard = shard_for_key(new_key, ctx.num_shards);

        if old_shard != new_shard {
            return Err(CommandError::CrossSlot);
        }

        // Get the value from old key
        let value = ctx.store.get(old_key).ok_or(CommandError::InvalidArgument {
            message: "no such key".to_string(),
        })?;

        // Get expiry if any
        let expiry = ctx.store.get_expiry(old_key);

        // Delete old key
        ctx.store.delete(old_key);

        // Set new key with same value
        ctx.store.set(new_key.clone(), value);

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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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
        let value = ctx.store.get(old_key).ok_or(CommandError::InvalidArgument {
            message: "no such key".to_string(),
        })?;

        // Get expiry if any
        let expiry = ctx.store.get_expiry(old_key);

        // Delete old key
        ctx.store.delete(old_key);

        // Set new key with same value
        ctx.store.set(new_key.clone(), value);

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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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
        Arity::AtLeast(2) // OBJECT subcommand key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let subcommand = args[0].to_ascii_uppercase();

        match subcommand.as_slice() {
            b"ENCODING" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "OBJECT" });
                }
                let key = &args[1];

                match ctx.store.get(key) {
                    Some(value) => {
                        let encoding = match &value {
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
                        };
                        Ok(Response::bulk(Bytes::from(encoding)))
                    }
                    None => Ok(Response::null()),
                }
            }
            b"FREQ" => {
                // LFU frequency counter (placeholder - always returns 0)
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "OBJECT" });
                }
                let key = &args[1];

                if ctx.store.contains(key) {
                    Ok(Response::Integer(0))
                } else {
                    Ok(Response::null())
                }
            }
            b"IDLETIME" => {
                // Return 0 for now (we don't track idle time accurately yet)
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "OBJECT" });
                }
                let key = &args[1];

                if ctx.store.contains(key) {
                    Ok(Response::Integer(0))
                } else {
                    Ok(Response::null())
                }
            }
            b"REFCOUNT" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "OBJECT" });
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
                    Response::bulk(Bytes::from_static(b"OBJECT ENCODING <key>")),
                    Response::bulk(Bytes::from_static(b"OBJECT FREQ <key>")),
                    Response::bulk(Bytes::from_static(b"OBJECT IDLETIME <key>")),
                    Response::bulk(Bytes::from_static(b"OBJECT REFCOUNT <key>")),
                    Response::bulk(Bytes::from_static(b"OBJECT HELP")),
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let subcommand = args[0].to_ascii_uppercase();

        match subcommand.as_slice() {
            b"OBJECT" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "DEBUG" });
                }
                let key = &args[1];

                match ctx.store.get(key) {
                    Some(value) => {
                        let encoding = match &value {
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
        }
        vec![]
    }
}
