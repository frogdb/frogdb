use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, Expiry, KeyAccessFlag, KeySpec, KeyspaceEventFlags, LookupSpec,
    ScatterGatherOp, SetCondition, SetOptions, SetResult, StoreTypedFamilyExt, Value, WaiterWake,
    WalStrategy,
};
use frogdb_protocol::Response;

use frogdb_core::ArgParser;

use super::utils::{ExpiryErr, checked_expire_value, parse_i64};

/// PING command.
pub struct PingCommand;

impl Command for PingCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "PING",
            arity: Arity::Range { min: 0, max: 1 },
            flags: CommandFlags::READONLY
                .union(CommandFlags::FAST)
                .union(CommandFlags::STALE)
                .union(CommandFlags::LOADING),
            keys: KeySpec::None,
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

    fn execute(&self, _ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.is_empty() {
            Ok(Response::pong())
        } else {
            Ok(Response::bulk(args[0].clone()))
        }
    }
}

/// ECHO command.
pub struct EchoCommand;

impl Command for EchoCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ECHO",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::None,
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

    fn execute(&self, _ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        Ok(Response::bulk(args[0].clone()))
    }
}

/// QUIT command.
pub struct QuitCommand;

impl Command for QuitCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "QUIT",
            arity: Arity::Fixed(0),
            flags: CommandFlags::READONLY
                .union(CommandFlags::FAST)
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
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

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        Ok(Response::ok())
    }
}

/// COMMAND command - server command introspection.
pub struct CommandCommand;

impl Command for CommandCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "COMMAND",
            arity: Arity::AtLeast(0),
            flags: CommandFlags::READONLY
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
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
                        let entry = if name_upper.contains('|') {
                            None
                        } else {
                            ctx.command_registry.and_then(|r| r.get_entry(&name_upper))
                        };
                        if let Some(entry) = entry {
                            // Build command flags array
                            let cmd_flags = entry.flags();
                            let mut flag_strs: Vec<Response> = Vec::new();
                            if cmd_flags.contains(CommandFlags::WRITE) {
                                flag_strs.push(Response::Simple(Bytes::from_static(b"write")));
                            }
                            if cmd_flags.contains(CommandFlags::READONLY) {
                                flag_strs.push(Response::Simple(Bytes::from_static(b"readonly")));
                            }
                            if cmd_flags.contains(CommandFlags::FAST) {
                                flag_strs.push(Response::Simple(Bytes::from_static(b"fast")));
                            }
                            if cmd_flags.contains(CommandFlags::STALE) {
                                flag_strs.push(Response::Simple(Bytes::from_static(b"stale")));
                            }
                            if cmd_flags.contains(CommandFlags::LOADING) {
                                flag_strs.push(Response::Simple(Bytes::from_static(b"loading")));
                            }
                            if cmd_flags.contains(CommandFlags::ADMIN) {
                                flag_strs.push(Response::Simple(Bytes::from_static(b"admin")));
                            }
                            if cmd_flags.contains(CommandFlags::NOSCRIPT) {
                                flag_strs.push(Response::Simple(Bytes::from_static(b"noscript")));
                            }
                            if cmd_flags.contains(CommandFlags::BLOCKING) {
                                flag_strs.push(Response::Simple(Bytes::from_static(b"blocking")));
                            }
                            if cmd_flags.contains(CommandFlags::SKIP_SLOWLOG) {
                                flag_strs
                                    .push(Response::Simple(Bytes::from_static(b"skip_slowlog")));
                            }
                            if cmd_flags.contains(CommandFlags::NO_PROPAGATE) {
                                flag_strs
                                    .push(Response::Simple(Bytes::from_static(b"no-propagate")));
                            }
                            if cmd_flags.contains(CommandFlags::RANDOM) {
                                flag_strs.push(Response::Simple(Bytes::from_static(b"random")));
                            }
                            if cmd_flags.contains(CommandFlags::MOVABLEKEYS) {
                                flag_strs
                                    .push(Response::Simple(Bytes::from_static(b"movablekeys")));
                            }
                            // Build basic command info
                            // Format: [name, arity, [flags], first_key, last_key, step]
                            let info = Response::Array(vec![
                                Response::bulk(Bytes::from(name_upper.to_lowercase())),
                                Response::Integer(-1), // Variable arity
                                Response::Array(flag_strs),
                                Response::Integer(0), // First key
                                Response::Integer(0), // Last key
                                Response::Integer(0), // Step
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
                    // Resolve via the registry *union* (`get_entry`) rather than
                    // the shard-only `commands` map (`get`), so keyed connection
                    // commands (EVAL/EVALSHA/FCALL/WATCH/DEBUG OBJECT) extract
                    // their keys through the entry's `keys` (which delegates to
                    // the connection command's `dynamic_keys`).
                    if let Some(entry) = registry.get_entry(&cmd_name) {
                        let keys = entry.keys(cmd_args);
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
            b"GETKEYSANDFLAGS" => {
                // COMMAND GETKEYSANDFLAGS command [args...] - return keys with access flags
                if args.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "command|getkeysandflags",
                    });
                }

                let cmd_name = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
                let cmd_args = &args[2..];

                if let Some(registry) = ctx.command_registry {
                    if let Some(entry) = registry.get_entry(&cmd_name) {
                        let keys_with_flags = entry.keys_with_flags(cmd_args);
                        let response: Vec<Response> = keys_with_flags
                            .into_iter()
                            .map(|(key, flags)| {
                                let flag_responses: Vec<Response> = flags
                                    .iter()
                                    .map(|f| Response::bulk(Bytes::from(f.as_str())))
                                    .collect();
                                Response::Array(vec![
                                    Response::bulk(Bytes::copy_from_slice(key)),
                                    Response::Array(flag_responses),
                                ])
                            })
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
}

/// GET command.
pub struct GetCommand;

impl Command for GetCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GET",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            // Keyspace hit/miss counted at the seam from `args[0]` existence.
            lookup: LookupSpec::FirstKey,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_string(key)? {
            Some(sv) => Ok(Response::bulk(sv.as_bytes())),
            None => Ok(Response::null()),
        }
    }
}

/// SET command with full option support.
pub struct SetCommand;

impl Command for SetCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SET",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            // VARIABLE_FLAGS in Redis: the `GET` option makes SET read the old
            // value (key becomes `RW,ACCESS`), while a plain `SET k v` is a blind
            // overwrite (`OW`). Resolved per-invocation in
            // `dynamic_keys_with_flags` so a `%W~`-only principal can still
            // `SET k v` but `SET k v GET` correctly requires read too.
            access: AccessSpec::Dynamic,
            wal: WalStrategy::PersistFirstKey,
            wakes: // SET can overwrite any key type with a string value. Stream waiters
        // (XREADGROUP) need WRONGTYPE when their stream is replaced; other
        // waiter kinds gracefully find no data and stay blocked.
        WaiterWake::All,
            event: EventSpec::Emits { class: KeyspaceEventFlags::STRING, name: "set" },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = args[0].clone();
        let value = args[1].clone();

        // Parse options
        let mut opts = SetOptions::default();
        let mut has_condition = false; // NX/XX/IFxx mutual exclusion
        let mut if_condition: Option<(Bytes, Bytes)> = None; // (flag_name_upper, cmp_value)

        const IF_FLAGS: &[&[u8]] = &[b"IFEQ", b"IFNE", b"IFDEQ", b"IFDNE"];
        let mut parser = ArgParser::from_position(args, 2);
        while parser.has_more() {
            if parser.try_flag(b"NX") {
                if has_condition {
                    return Err(CommandError::SyntaxError);
                }
                has_condition = true;
                opts.condition = SetCondition::NX;
            } else if parser.try_flag(b"XX") {
                if has_condition {
                    return Err(CommandError::SyntaxError);
                }
                has_condition = true;
                opts.condition = SetCondition::XX;
            } else if let Some(idx) = parser.try_flag_any(IF_FLAGS) {
                if has_condition {
                    return Err(CommandError::SyntaxError);
                }
                has_condition = true;
                let flag: &[u8] = IF_FLAGS[idx];
                let cmp_val = parser.next_arg()?.clone();
                // Validate IFDEQ/IFDNE digest format: exactly 16 hex chars
                if (flag == b"IFDEQ" || flag == b"IFDNE")
                    && (cmp_val.len() != 16 || !cmp_val.iter().all(|b| b.is_ascii_hexdigit()))
                {
                    return Err(CommandError::InvalidArgument {
                        message: "ERR IFDEQ/IFDNE requires a 16 character hexadecimal digest"
                            .to_string(),
                    });
                }
                if_condition = Some((Bytes::copy_from_slice(flag), cmp_val));
            } else if parser.try_flag(b"GET") {
                opts.return_old = true;
            } else if parser.try_flag(b"KEEPTTL") {
                opts.keep_ttl = true;
            } else if parser.try_flag(b"EX") {
                // EX is a seconds unit: guard the secs*1000 conversion
                // (Redis getExpireMillisecondsOrReply, UNIT_SECONDS).
                let secs = checked_expire_value(
                    parse_i64(parser.next_arg()?)?,
                    true,
                    ExpiryErr::Named("set"),
                )?;
                opts.expiry = Some(Expiry::Ex(secs));
            } else if parser.try_flag(b"PX") {
                let ms = checked_expire_value(
                    parse_i64(parser.next_arg()?)?,
                    false,
                    ExpiryErr::Named("set"),
                )?;
                opts.expiry = Some(Expiry::Px(ms));
            } else if parser.try_flag(b"EXAT") {
                // EXAT is also a seconds unit upstream (unit stays
                // UNIT_SECONDS), so it carries the same overflow guard.
                let ts = checked_expire_value(
                    parse_i64(parser.next_arg()?)?,
                    true,
                    ExpiryErr::Named("set"),
                )?;
                opts.expiry = Some(Expiry::ExAt(ts));
            } else if parser.try_flag(b"PXAT") {
                let ts = checked_expire_value(
                    parse_i64(parser.next_arg()?)?,
                    false,
                    ExpiryErr::Named("set"),
                )?;
                opts.expiry = Some(Expiry::PxAt(ts));
            } else {
                return Err(CommandError::SyntaxError);
            }
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

    /// Per-key access flags for `COMMAND GETKEYSANDFLAGS` / ACL: `RW` when the
    /// `GET` option is present (SET reads the old value), else `OW` (blind
    /// overwrite). Redis models this with `VARIABLE_FLAGS` on the key-spec.
    fn dynamic_keys_with_flags<'a>(
        &self,
        args: &'a [Bytes],
    ) -> Vec<(&'a [u8], Vec<KeyAccessFlag>)> {
        let Some(key) = args.first() else {
            return Vec::new();
        };
        let flag = if set_has_get_option(args) {
            KeyAccessFlag::RW
        } else {
            KeyAccessFlag::OW
        };
        vec![(key.as_ref(), vec![flag])]
    }
}

/// Whether a SET invocation carries the `GET` option (it then reads the old
/// value). Options begin after `key value` (index 2); `EX`/`PX`/`EXAT`/`PXAT`
/// and the `IFxx` family each consume the following argument, which is skipped
/// so a comparison/expiry value equal to `GET` is never mistaken for the flag.
fn set_has_get_option(args: &[Bytes]) -> bool {
    let mut i = 2;
    while i < args.len() {
        let tok = args[i].as_ref();
        if tok.eq_ignore_ascii_case(b"GET") {
            return true;
        }
        let consumes_value = tok.eq_ignore_ascii_case(b"EX")
            || tok.eq_ignore_ascii_case(b"PX")
            || tok.eq_ignore_ascii_case(b"EXAT")
            || tok.eq_ignore_ascii_case(b"PXAT")
            || tok.eq_ignore_ascii_case(b"IFEQ")
            || tok.eq_ignore_ascii_case(b"IFNE")
            || tok.eq_ignore_ascii_case(b"IFDEQ")
            || tok.eq_ignore_ascii_case(b"IFDNE");
        i += if consumes_value { 2 } else { 1 };
    }
    false
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
        // Capture old string value for GET flag before we check conditions. The
        // typed seam yields the shared value only if it is a string (WRONGTYPE
        // otherwise); `as_bytes` clones cheaply so no shared handle lingers into
        // the mutation below.
        let old_string_value: Option<Bytes> = ctx.store.get_string(&key)?.map(|sv| sv.as_bytes());

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
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "DEL",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE,
            keys: KeySpec::All,
            access: AccessSpec::Uniform,
            wal: WalStrategy::DeleteKeys,
            wakes: // DEL can remove any key type. Stream waiters (XREADGROUP) need
        // NOGROUP when their stream disappears; list/zset waiters gracefully
        // find no data and stay blocked.
        WaiterWake::All,
            event: EventSpec::Emits { class: KeyspaceEventFlags::GENERIC, name: "del" },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ScatterGather(ScatterGatherOp::Del),
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Multi-key DEL: delete all keys and return count
        // Cross-shard routing is handled by connection handler
        let mut deleted = 0i64;
        for key in args {
            // Trigger lazy expiry first: if the key is stale (expired metadata),
            // it gets cleaned up here and the subsequent delete() returns false.
            // This matches Redis behavior where DEL on an expired key returns 0
            // and does not dirty WATCH state.
            let _ = ctx.store.get_with_expiry_check(key);

            if ctx.store.delete(key) {
                deleted += 1;
            }
        }
        // Signal the post-execution pipeline that no data was modified so
        // it can skip incrementing the shard version (preserving WATCH state).
        if deleted == 0 {
            ctx.effects.dirty_delta = -1;
        }
        Ok(Response::Integer(deleted))
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
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "EXISTS",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::All,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::EveryKey,
            strategy: ExecutionStrategy::ScatterGather(ScatterGatherOp::Exists),
        };
        &SPEC
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
}

#[cfg(test)]
mod expiry_grammar_pin_tests {
    //! Wire-compat pins for the SET command's EX/PX/EXAT/PXAT grammar. These
    //! assert the exact `invalid expire time in 'set' command` message and the
    //! secs*1000 overflow rejection so the shared-helper migration stays
    //! byte-identical.
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

    fn expect_invalid(parts: &[&str]) -> String {
        let mut c = ctx();
        match SetCommand.execute(&mut c, &args(parts)) {
            Err(CommandError::InvalidArgument { message }) => message,
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn set_ex_zero_message() {
        assert_eq!(
            expect_invalid(&["k", "v", "EX", "0"]),
            "invalid expire time in 'set' command"
        );
    }

    #[test]
    fn set_ex_negative_message() {
        assert_eq!(
            expect_invalid(&["k", "v", "EX", "-1"]),
            "invalid expire time in 'set' command"
        );
    }

    #[test]
    fn set_ex_secs_overflow_rejected() {
        // 18446744073709551 > i64::MAX / 1000, so the seconds->millis conversion
        // would overflow: SET rejects it up front.
        assert_eq!(
            expect_invalid(&["k", "v", "EX", "18446744073709551"]),
            "invalid expire time in 'set' command"
        );
    }

    #[test]
    fn set_px_zero_message() {
        assert_eq!(
            expect_invalid(&["k", "v", "PX", "0"]),
            "invalid expire time in 'set' command"
        );
    }

    #[test]
    fn set_exat_zero_message() {
        assert_eq!(
            expect_invalid(&["k", "v", "EXAT", "0"]),
            "invalid expire time in 'set' command"
        );
    }

    #[test]
    fn set_exat_secs_overflow_rejected() {
        // EXAT is also a seconds unit upstream (unit stays UNIT_SECONDS in
        // parseExtendedStringArgumentsOrReply), so it carries the same guard.
        assert_eq!(
            expect_invalid(&["k", "v", "EXAT", "18446744073709551"]),
            "invalid expire time in 'set' command"
        );
    }

    #[test]
    fn set_pxat_zero_message() {
        assert_eq!(
            expect_invalid(&["k", "v", "PXAT", "0"]),
            "invalid expire time in 'set' command"
        );
    }

    #[test]
    fn set_px_large_value_accepted() {
        // PX does not carry the seconds overflow guard: a large millisecond
        // value that fits i64 is accepted (returns OK, not an error).
        let mut c = ctx();
        let r = SetCommand
            .execute(&mut c, &args(&["k", "v", "PX", "18446744073709551"]))
            .unwrap();
        assert_eq!(r, Response::ok());
    }
}
