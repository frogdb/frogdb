use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ExecutionStrategy, Expiry,
    MergeStrategy, SetCondition, SetOptions, SetResult, Value, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::utils::parse_i64;

/// PING command.
pub struct PingCommand;

impl Command for PingCommand {
    fn name(&self) -> &'static str {
        "PING"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 0, max: 1 }
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST | CommandFlags::STALE | CommandFlags::LOADING
    }

    fn execute(&self, _ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.is_empty() {
            Ok(Response::pong())
        } else {
            Ok(Response::bulk(args[0].clone()))
        }
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}

/// ECHO command.
pub struct EchoCommand;

impl Command for EchoCommand {
    fn name(&self) -> &'static str {
        "ECHO"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, _ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        Ok(Response::bulk(args[0].clone()))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}

/// QUIT command.
pub struct QuitCommand;

impl Command for QuitCommand {
    fn name(&self) -> &'static str {
        "QUIT"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}

/// COMMAND command - server command introspection.
pub struct CommandCommand;

impl Command for CommandCommand {
    fn name(&self) -> &'static str {
        "COMMAND"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.is_empty() {
            // COMMAND - return info about all commands
            return Ok(Response::Array(vec![])); // Simplified for now
        }

        let subcommand = args[0].to_ascii_uppercase();
        match subcommand.as_slice() {
            b"COUNT" => {
                // COMMAND COUNT - return number of commands
                if let Some(registry) = ctx.command_registry {
                    Ok(Response::Integer(registry.len() as i64))
                } else {
                    Ok(Response::Integer(0))
                }
            }
            b"DOCS" => {
                // COMMAND DOCS [command-name ...] - return docs for commands
                if args.len() == 1 {
                    // Return docs for all commands (empty for now)
                    Ok(Response::Array(vec![]))
                } else {
                    // Return docs for specified commands
                    let mut results = Vec::new();
                    for cmd_name in &args[1..] {
                        let cmd_str = String::from_utf8_lossy(cmd_name).to_uppercase();
                        // Build basic doc entry
                        let doc = Response::Array(vec![
                            Response::bulk(cmd_name.clone()),
                            Response::Array(vec![
                                Response::bulk(Bytes::from_static(b"summary")),
                                Response::bulk(Bytes::from(format!("{} command", cmd_str))),
                                Response::bulk(Bytes::from_static(b"since")),
                                Response::bulk(Bytes::from_static(b"1.0.0")),
                                Response::bulk(Bytes::from_static(b"group")),
                                Response::bulk(Bytes::from_static(b"generic")),
                            ]),
                        ]);
                        results.push(doc);
                    }
                    Ok(Response::Array(results))
                }
            }
            b"INFO" => {
                // COMMAND INFO [command-name ...] - return info for commands
                if args.len() == 1 {
                    Ok(Response::Array(vec![]))
                } else {
                    let mut results = Vec::new();
                    for cmd_name in &args[1..] {
                        let name_upper = String::from_utf8_lossy(cmd_name).to_ascii_uppercase();
                        // Only match exact command names (no pipe-subcommand expansion).
                        // "GET" matches, "GET|KEY" or "CONFIG|GET|KEY" do not.
                        let found = if name_upper.contains('|') {
                            false
                        } else {
                            ctx.command_registry
                                .is_some_and(|r| r.get(&name_upper).is_some())
                        };
                        if found {
                            // Build basic command info
                            // Format: [name, arity, [flags], first_key, last_key, step]
                            let info = Response::Array(vec![
                                Response::bulk(Bytes::from(name_upper.to_lowercase())),
                                Response::Integer(-1),   // Variable arity
                                Response::Array(vec![]), // Flags
                                Response::Integer(0),    // First key
                                Response::Integer(0),    // Last key
                                Response::Integer(0),    // Step
                            ]);
                            results.push(info);
                        } else {
                            results.push(Response::Bulk(None));
                        }
                    }
                    Ok(Response::Array(results))
                }
            }
            b"GETKEYS" => {
                // COMMAND GETKEYS command [args...] - return keys for a command
                if args.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "command|getkeys",
                    });
                }

                let cmd_name = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
                let cmd_args = &args[2..];

                if let Some(registry) = ctx.command_registry {
                    if let Some(handler) = registry.get(&cmd_name) {
                        let keys = handler.keys(cmd_args);
                        let response: Vec<Response> = keys
                            .into_iter()
                            .map(|k| Response::bulk(Bytes::copy_from_slice(k)))
                            .collect();
                        Ok(Response::Array(response))
                    } else {
                        Err(CommandError::InvalidArgument {
                            message: format!(
                                "Invalid command specified, or key spec not found for '{}'",
                                cmd_name
                            ),
                        })
                    }
                } else {
                    Ok(Response::Array(vec![]))
                }
            }
            b"LIST" => {
                // COMMAND LIST [FILTERBY MODULE|ACLCAT|PATTERN value]
                if let Some(registry) = ctx.command_registry {
                    let names: Vec<String> = if args.len() >= 3
                        && args[1].to_ascii_uppercase().as_slice() == b"FILTERBY"
                    {
                        let filter_type = args[2].to_ascii_uppercase();
                        if args.len() < 4 {
                            return Err(CommandError::InvalidArgument {
                                message: format!(
                                    "Missing value for FILTERBY {}",
                                    String::from_utf8_lossy(&filter_type)
                                ),
                            });
                        }
                        let filter_value = &args[3];
                        match filter_type.as_slice() {
                            b"MODULE" => vec![], // FrogDB has no modules
                            b"ACLCAT" => {
                                let category = String::from_utf8_lossy(filter_value).to_lowercase();
                                registry
                                    .iter()
                                    .filter(|(_, entry)| {
                                        flags_match_acl_category(entry.flags(), &category)
                                    })
                                    .map(|(name, _)| name.to_lowercase())
                                    .collect()
                            }
                            b"PATTERN" => {
                                let pattern = String::from_utf8_lossy(filter_value).to_lowercase();
                                registry
                                    .names()
                                    .filter(|name| {
                                        frogdb_core::glob_match(
                                            pattern.as_bytes(),
                                            name.to_lowercase().as_bytes(),
                                        )
                                    })
                                    .map(|name| name.to_lowercase())
                                    .collect()
                            }
                            _ => {
                                return Err(CommandError::InvalidArgument {
                                    message: format!(
                                        "Unknown FILTERBY type '{}'",
                                        String::from_utf8_lossy(&filter_type)
                                    ),
                                });
                            }
                        }
                    } else if args.len() > 1 {
                        // Unknown argument (not FILTERBY)
                        return Err(CommandError::SyntaxError);
                    } else {
                        registry.names().map(|n| n.to_lowercase()).collect()
                    };
                    let mut sorted = names;
                    sorted.sort();
                    Ok(Response::Array(
                        sorted
                            .into_iter()
                            .map(|n| Response::bulk(Bytes::from(n)))
                            .collect(),
                    ))
                } else {
                    Ok(Response::Array(vec![]))
                }
            }
            b"HELP" => {
                // COMMAND HELP
                let help = vec![
                    Response::bulk(Bytes::from_static(
                        b"COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    )),
                    Response::bulk(Bytes::from_static(b"(no subcommand)")),
                    Response::bulk(Bytes::from_static(
                        b"    Return details about all Redis commands.",
                    )),
                    Response::bulk(Bytes::from_static(b"COUNT")),
                    Response::bulk(Bytes::from_static(
                        b"    Return number of total commands in this Redis server.",
                    )),
                    Response::bulk(Bytes::from_static(
                        b"DOCS [<command-name> [<command-name> ...]]",
                    )),
                    Response::bulk(Bytes::from_static(
                        b"    Return documentary information about commands.",
                    )),
                    Response::bulk(Bytes::from_static(b"GETKEYS <full-command>")),
                    Response::bulk(Bytes::from_static(
                        b"    Extract keys given a full Redis command.",
                    )),
                    Response::bulk(Bytes::from_static(
                        b"INFO [<command-name> [<command-name> ...]]",
                    )),
                    Response::bulk(Bytes::from_static(
                        b"    Return details about multiple Redis commands.",
                    )),
                    Response::bulk(Bytes::from_static(b"LIST [FILTERBY <filter> <value>]")),
                    Response::bulk(Bytes::from_static(b"    Return a list of command names.")),
                    Response::bulk(Bytes::from_static(b"HELP")),
                    Response::bulk(Bytes::from_static(b"    Return subcommand help summary.")),
                ];
                Ok(Response::Array(help))
            }
            _ => Err(CommandError::InvalidArgument {
                message: format!(
                    "unknown subcommand '{}'. Try COMMAND HELP.",
                    String::from_utf8_lossy(&subcommand)
                ),
            }),
        }
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}

/// GET command.
pub struct GetCommand;

impl Command for GetCommand {
    fn name(&self) -> &'static str {
        "GET"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST | CommandFlags::TRACKS_KEYSPACE
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(sv) = value.as_string() {
                    Ok(Response::bulk(sv.as_bytes()))
                } else {
                    Err(CommandError::WrongType)
                }
            }
            None => Ok(Response::null()),
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

/// SET command with full option support.
pub struct SetCommand;

impl Command for SetCommand {
    fn name(&self) -> &'static str {
        "SET"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // SET key value [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn wakes_waiters(&self) -> WaiterWake {
        // SET can overwrite any key type with a string value. Stream waiters
        // (XREADGROUP) need WRONGTYPE when their stream is replaced; other
        // waiter kinds gracefully find no data and stay blocked.
        WaiterWake::All
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = args[0].clone();
        let value = args[1].clone();

        // Parse options
        let mut opts = SetOptions::default();
        let mut has_condition = false; // NX/XX/IFxx mutual exclusion
        let mut if_condition: Option<(Bytes, Bytes)> = None; // (flag_name_upper, cmp_value)
        let mut i = 2;

        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"NX" => {
                    if has_condition {
                        return Err(CommandError::SyntaxError);
                    }
                    has_condition = true;
                    opts.condition = SetCondition::NX;
                }
                b"XX" => {
                    if has_condition {
                        return Err(CommandError::SyntaxError);
                    }
                    has_condition = true;
                    opts.condition = SetCondition::XX;
                }
                b"IFEQ" | b"IFNE" | b"IFDEQ" | b"IFDNE" => {
                    if has_condition {
                        return Err(CommandError::SyntaxError);
                    }
                    has_condition = true;
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let cmp_val = args[i].clone();
                    // Validate IFDEQ/IFDNE digest format: exactly 16 hex chars
                    if (opt.as_slice() == b"IFDEQ" || opt.as_slice() == b"IFDNE")
                        && (cmp_val.len() != 16 || !cmp_val.iter().all(|b| b.is_ascii_hexdigit()))
                    {
                        return Err(CommandError::InvalidArgument {
                            message: "ERR IFDEQ/IFDNE requires a 16 character hexadecimal digest"
                                .to_string(),
                        });
                    }
                    if_condition = Some((Bytes::from(opt), cmp_val));
                }
                b"GET" => {
                    opts.return_old = true;
                }
                b"KEEPTTL" => {
                    opts.keep_ttl = true;
                }
                b"EX" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let secs = parse_i64(&args[i]).map_err(|_| CommandError::NotInteger)?;
                    if secs <= 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'set' command".to_string(),
                        });
                    }
                    if secs > i64::MAX / 1000 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'set' command".to_string(),
                        });
                    }
                    opts.expiry = Some(Expiry::Ex(secs as u64));
                }
                b"PX" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let ms = parse_i64(&args[i]).map_err(|_| CommandError::NotInteger)?;
                    if ms <= 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'set' command".to_string(),
                        });
                    }
                    opts.expiry = Some(Expiry::Px(ms as u64));
                }
                b"EXAT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let ts = parse_i64(&args[i]).map_err(|_| CommandError::NotInteger)?;
                    if ts <= 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'set' command".to_string(),
                        });
                    }
                    opts.expiry = Some(Expiry::ExAt(ts as u64));
                }
                b"PXAT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let ts = parse_i64(&args[i]).map_err(|_| CommandError::NotInteger)?;
                    if ts <= 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'set' command".to_string(),
                        });
                    }
                    opts.expiry = Some(Expiry::PxAt(ts as u64));
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        // Check for conflicting options
        if opts.keep_ttl && opts.expiry.is_some() {
            return Err(CommandError::SyntaxError);
        }

        // Handle IFEQ/IFNE/IFDEQ/IFDNE conditions
        if let Some((flag, cmp_val)) = if_condition {
            return self.execute_with_if_condition(ctx, key, value, opts, &flag, &cmp_val);
        }

        // Redis returns WRONGTYPE when SET GET is used on a non-string key.
        // This check must happen before set_with_options replaces the value.
        // Also capture the old string value for the GET flag when NX/XX prevents the SET.
        let mut old_string_value: Option<Bytes> = None;
        if opts.return_old
            && let Some(existing) = ctx.store.get(&key)
        {
            if let Some(sv) = existing.as_string() {
                old_string_value = Some(sv.as_bytes());
            } else {
                return Err(CommandError::WrongType);
            }
        }

        match ctx.store.set_with_options(key, Value::string(value), opts) {
            SetResult::Ok => Ok(Response::ok()),
            SetResult::OkWithOldValue(old) => match old {
                Some(v) => {
                    if let Some(sv) = v.as_string() {
                        Ok(Response::bulk(sv.as_bytes()))
                    } else {
                        Ok(Response::null())
                    }
                }
                None => Ok(Response::null()),
            },
            SetResult::NotSet => {
                // When GET flag is set, return the old value even when NX/XX prevents the SET
                match old_string_value {
                    Some(v) => Ok(Response::bulk(v)),
                    None => Ok(Response::null()),
                }
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

impl SetCommand {
    /// Handle SET with IFEQ/IFNE/IFDEQ/IFDNE conditions.
    fn execute_with_if_condition(
        &self,
        ctx: &mut CommandContext,
        key: Bytes,
        value: Bytes,
        opts: SetOptions,
        flag: &[u8],
        cmp_val: &Bytes,
    ) -> Result<Response, CommandError> {
        // Read existing value
        let existing = ctx.store.get_with_expiry_check(&key);

        // Capture old string value for GET flag before we check conditions
        let old_string_value: Option<Bytes> = if let Some(ref v) = existing {
            if let Some(sv) = v.as_string() {
                Some(sv.as_bytes())
            } else {
                // Key exists but isn't a string — WRONGTYPE
                return Err(CommandError::WrongType);
            }
        } else {
            None
        };

        let condition_met = match flag {
            b"IFEQ" => {
                // Key must exist and value must match
                old_string_value
                    .as_ref()
                    .is_some_and(|stored| stored.as_ref() == cmp_val.as_ref())
            }
            b"IFNE" => {
                // Key doesn't exist → succeeds; key exists and value differs → succeeds
                match &old_string_value {
                    None => true,
                    Some(stored) => stored.as_ref() != cmp_val.as_ref(),
                }
            }
            b"IFDEQ" => {
                // Key must exist and digest must match
                old_string_value.as_ref().is_some_and(|stored| {
                    let hash = xxhash_rust::xxh3::xxh3_64(stored.as_ref());
                    let hex = format!("{hash:016x}");
                    hex.as_bytes().eq_ignore_ascii_case(cmp_val.as_ref())
                })
            }
            b"IFDNE" => {
                // Key doesn't exist → succeeds; key exists and digest differs → succeeds
                match &old_string_value {
                    None => true,
                    Some(stored) => {
                        let hash = xxhash_rust::xxh3::xxh3_64(stored.as_ref());
                        let hex = format!("{hash:016x}");
                        !hex.as_bytes().eq_ignore_ascii_case(cmp_val.as_ref())
                    }
                }
            }
            _ => unreachable!(),
        };

        // Drop the Arc reference before mutating the store
        drop(existing);

        if condition_met {
            match ctx.store.set_with_options(key, Value::string(value), opts) {
                SetResult::Ok => Ok(Response::ok()),
                SetResult::OkWithOldValue(_) => {
                    // GET flag: return old value (we already captured it)
                    match old_string_value {
                        Some(v) => Ok(Response::bulk(v)),
                        None => Ok(Response::null()),
                    }
                }
                SetResult::NotSet => {
                    // Shouldn't happen since we don't set NX/XX with IFxx
                    match old_string_value {
                        Some(v) => Ok(Response::bulk(v)),
                        None => Ok(Response::null()),
                    }
                }
            }
        } else {
            // Condition not met — return nil or old value with GET
            if opts.return_old {
                match old_string_value {
                    Some(v) => Ok(Response::bulk(v)),
                    None => Ok(Response::null()),
                }
            } else {
                Ok(Response::null())
            }
        }
    }
}

/// DEL command.
pub struct DelCommand;

impl Command for DelCommand {
    fn name(&self) -> &'static str {
        "DEL"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ScatterGather {
            merge: MergeStrategy::SumIntegers,
        }
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::DeleteKeys
    }

    fn wakes_waiters(&self) -> WaiterWake {
        // DEL can remove any key type. Stream waiters (XREADGROUP) need
        // NOGROUP when their stream disappears; list/zset waiters gracefully
        // find no data and stay blocked.
        WaiterWake::All
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Multi-key DEL: delete all keys and return count
        // Cross-shard routing is handled by connection handler
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

/// Map `CommandFlags` to a Redis ACL category name.
///
/// Redis COMMAND LIST FILTERBY ACLCAT returns commands whose flags match
/// a given ACL category. This helper returns `true` when the flag set
/// belongs to the requested category.
fn flags_match_acl_category(flags: CommandFlags, category: &str) -> bool {
    match category {
        "read" => flags.contains(CommandFlags::READONLY),
        "write" => flags.contains(CommandFlags::WRITE),
        "admin" => flags.contains(CommandFlags::ADMIN),
        "fast" => flags.contains(CommandFlags::FAST),
        "slow" => !flags.contains(CommandFlags::FAST),
        "blocking" => flags.contains(CommandFlags::BLOCKING),
        "pubsub" => flags.contains(CommandFlags::PUBSUB),
        "scripting" => flags.contains(CommandFlags::SCRIPT),
        _ => false,
    }
}

/// EXISTS command.
pub struct ExistsCommand;

impl Command for ExistsCommand {
    fn name(&self) -> &'static str {
        "EXISTS"
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
        // Multi-key EXISTS: count how many keys exist
        // Note: Redis counts duplicates (EXISTS key key returns 2 if key exists)
        let mut count = 0i64;
        for key in args {
            if ctx.store.contains(key) {
                count += 1;
            }
        }
        Ok(Response::Integer(count))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|a| a.as_ref()).collect()
    }
}
