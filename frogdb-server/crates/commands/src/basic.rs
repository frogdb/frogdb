use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, Expiry, KeyAccessFlag, KeySpec, KeyspaceEventFlags, LookupSpec,
    ScatterGatherOp, SetCondition, SetOptions, SetResult, StoreTypedFamilyExt, Value, WaiterWake,
    WalStrategy,
};
use frogdb_protocol::Response;

use crate::command_meta::{build_command_docs, build_command_info};

use frogdb_core::ArgParser;

use super::utils::{ExpiryErr, checked_expire_value, parse_i64};

/// PING command.
pub struct PingCommand;

impl Command for PingCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "PING",
            docs: frogdb_core::CommandDocs {
                summary: "Returns the server's liveliness response.",
                since: "1.0.0",
                group: "connection",
                complexity: Some("O(1)"),
            },
            arity: Arity::Range { min: 0, max: 1 },
            flags: CommandFlags::FAST
                .union(CommandFlags::STALE)
                .union(CommandFlags::LOADING),
            keys: KeySpec::None,
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
            docs: frogdb_core::CommandDocs {
                summary: "Returns the given string.",
                since: "1.0.0",
                group: "connection",
                complexity: Some("O(1)"),
            },
            arity: Arity::Fixed(1),
            flags: CommandFlags::FAST,
            keys: KeySpec::None,
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
            docs: frogdb_core::CommandDocs {
                summary: "Closes the connection.",
                since: "1.0.0",
                group: "connection",
                complexity: Some("O(1)"),
            },
            // Upstream's arity is -1: `quitCommand` ignores whatever follows,
            // so a client that sends `QUIT` with trailing junk still gets +OK
            // and a closed connection rather than a wrong-arity error.
            arity: Arity::AtLeast(0),
            flags: CommandFlags::FAST
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
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
            docs: frogdb_core::CommandDocs {
                summary: "Returns detailed information about all commands.",
                since: "2.8.13",
                group: "server",
                complexity: Some("O(N) where N is the total number of Redis commands"),
            },
            arity: Arity::AtLeast(0),
            flags: CommandFlags::LOADING.union(CommandFlags::STALE),
            keys: KeySpec::None,
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
        if args.is_empty() {
            // COMMAND (no subcommand) - full command list, same structured
            // info as a subcommand-less `COMMAND INFO`.
            return Ok(Response::Array(all_commands_info(ctx)));
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
                // COMMAND DOCS [command-name ...] - documentation for commands.
                // Reply is a map name -> docs-map; Redis *skips* names it cannot
                // resolve rather than replying nil for them, so the reply may be
                // shorter than the request.
                if args.len() == 1 {
                    Ok(Response::Map(all_commands_docs(ctx)))
                } else {
                    let mut results = Vec::new();
                    for cmd_name in &args[1..] {
                        let name_upper = String::from_utf8_lossy(cmd_name).to_ascii_uppercase();
                        // Same exact-name rule as `COMMAND INFO`: subcommands are
                        // not separate registry entries, so `config|get` resolves
                        // to nothing (and is therefore skipped).
                        let entry = if name_upper.contains('|') {
                            None
                        } else {
                            ctx.command_registry.and_then(|r| r.get_entry(&name_upper))
                        };
                        if let Some(entry) = entry {
                            let spec = entry.spec();
                            results.push((
                                Response::bulk(Bytes::from(spec.name.to_lowercase())),
                                build_command_docs(spec),
                            ));
                        }
                    }
                    Ok(Response::Map(results))
                }
            }
            b"INFO" => {
                // COMMAND INFO [command-name ...] - return info for commands.
                // With no names, info for every registered command (matches
                // bare `COMMAND`).
                if args.len() == 1 {
                    Ok(Response::Array(all_commands_info(ctx)))
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
                        results.push(match entry {
                            Some(entry) => build_command_info(entry.spec()),
                            None => Response::Bulk(None),
                        });
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
            docs: frogdb_core::CommandDocs {
                summary: "Returns the string value of a key.",
                since: "1.0.0",
                group: "string",
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
            // Keyspace hit/miss counted at the seam from `args[0]` existence.
            lookup: LookupSpec::FirstKey,
            mutation: frogdb_core::ConnMutation::None,
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
            docs: frogdb_core::CommandDocs {
                summary: "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist.",
                since: "1.0.0",
                group: "string",
                complexity: Some("O(1)"),
            },
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::DENYOOM),
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
            // A blind `SET` overwrites any existing key with a string, so it can
            // clobber an indexed hash. Refresh the key: index it if it is still a
            // hash (a failed NX/XX leaves it unchanged), else drop the now-stale
            // hash doc.
            reindex: frogdb_core::ReindexSpec::RefreshFirstKey,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
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
                        message: "IFDEQ/IFDNE requires a 16 character hexadecimal digest"
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
                // NX/XX prevented the SET: nothing was written. Declaring the
                // write a no-op skips the whole effect pipeline (reindex — SET is
                // `RefreshFirstKey`, so an NX miss on an indexed key would
                // otherwise re-index the entire unchanged key — plus WAL,
                // replication, keyspace notification, WATCH dirty). Redis returns
                // before signalModifiedKey on a condition miss.
                ctx.effects.write_was_noop = true;
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
                    ctx.effects.write_was_noop = true;
                    match old_string_value {
                        Some(v) => Ok(Response::bulk(v)),
                        None => Ok(Response::null()),
                    }
                }
            }
        } else {
            // Condition not met — nothing was written, so this is a no-op write
            // (same contract as an NX/XX miss: skip reindex / WAL / replication /
            // notification / WATCH dirty).
            ctx.effects.write_was_noop = true;
            // Return nil or old value with GET
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
            docs: frogdb_core::CommandDocs {
                summary: "Deletes one or more keys.",
                since: "1.0.0",
                group: "generic",
                complexity: Some("O(N) where N is the number of keys that will be removed. When a key to remove holds a value other than a string, the individual complexity for this key is O(M) where M is the number of elements in the list, set, sorted set or hash. Removing a single key that holds a string value is O(1)."),
            },
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
            reindex: frogdb_core::ReindexSpec::DeleteKeys,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
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

/// The `COMMAND INFO`/bare-`COMMAND` reply for every registered command —
/// shared by both call sites so they can never drift.
fn all_commands_info(ctx: &CommandContext) -> Vec<Response> {
    ctx.command_registry
        .map(|registry| {
            registry
                .iter()
                .map(|(_, entry)| build_command_info(entry.spec()))
                .collect()
        })
        .unwrap_or_default()
}

/// The `COMMAND DOCS` reply for every registered command, as map pairs.
fn all_commands_docs(ctx: &CommandContext) -> Vec<(Response, Response)> {
    ctx.command_registry
        .map(|registry| {
            registry
                .iter()
                .map(|(_, entry)| {
                    let spec = entry.spec();
                    (
                        Response::bulk(Bytes::from(spec.name.to_lowercase())),
                        build_command_docs(spec),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
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
            docs: frogdb_core::CommandDocs {
                summary: "Determines whether one or more keys exist.",
                since: "1.0.0",
                group: "generic",
                complexity: Some("O(N) where N is the number of keys to check."),
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
            strategy: ExecutionStrategy::ScatterGather(ScatterGatherOp::Exists),
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Multi-key EXISTS: count how many keys exist
        // Note: Redis counts duplicates (EXISTS key key returns 2 if key exists)
        let mut count = 0i64;
        for key in args {
            // Logical existence, not physical: a key past its deadline that the
            // sampled sweeper has not reached yet does not exist.
            // `exists_unexpired` (NOT `get_with_expiry_check`) keeps EXISTS a
            // non-destructive probe — it must not purge or report a removal.
            if ctx.store.exists_unexpired(key) {
                count += 1;
            }
        }
        Ok(Response::Integer(count))
    }
}

#[cfg(test)]
mod command_info_tests {
    //! `COMMAND INFO`/bare-`COMMAND` reply-shape regression tests (issue
    //! redis-feel/02): full 10-element wire replies for one representative
    //! command per reachable-from-this-crate `KeySpec` variant, exercised
    //! through the real `CommandCommand::execute` dispatch path (not just the
    //! `command_info_triplet` mapping unit-tested in `frogdb_core::
    //! command_spec`). GET/SET/MSET are the issue's named static-key cases;
    //! SINTERCARD (`KeySpec::NumkeysAt`) is the movablekeys case — it lives in
    //! this crate, unlike the issue's suggested EVAL example (a connection
    //! command registered only in `frogdb-server`), so it is the in-crate
    //! stand-in for "a `NumkeysAt`/`Dynamic` command reports `movablekeys` and
    //! `(0,0,0)`".
    use super::*;
    use crate::set::SintercardCommand;
    use crate::string::MsetCommand;
    use frogdb_core::{CommandRegistry, HashMapStore};
    use frogdb_protocol::ProtocolVersion;
    use std::sync::Arc;

    fn ctx_with_registry() -> CommandContext<'static> {
        let mut registry = CommandRegistry::new();
        registry.register(GetCommand);
        registry.register(SetCommand);
        registry.register(MsetCommand);
        registry.register(SintercardCommand);
        let registry: &'static Arc<CommandRegistry> = Box::leak(Box::new(Arc::new(registry)));

        let store = Box::leak(Box::new(HashMapStore::new()));
        let shard_senders = Box::leak(Box::new(Arc::new(Vec::new())));
        let mut c = CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2);
        c.command_registry = Some(registry);
        c
    }

    /// Run `COMMAND INFO <name>` through the real dispatch and return the
    /// single reply entry (not the outer one-element array).
    fn info(name: &str) -> Response {
        let mut c = ctx_with_registry();
        let reply = CommandCommand
            .execute(
                &mut c,
                &[Bytes::from_static(b"INFO"), Bytes::from(name.to_string())],
            )
            .unwrap();
        match reply {
            Response::Array(mut entries) if entries.len() == 1 => entries.remove(0),
            other => panic!("expected a single-entry Array, got {other:?}"),
        }
    }

    fn flag(name: &'static str) -> Response {
        Response::Simple(Bytes::from_static(name.as_bytes()))
    }

    fn category(name: &str) -> Response {
        Response::Simple(Bytes::from(format!("@{name}")))
    }

    /// The structured key-specs array for a command with exactly one
    /// statically-known key range starting at wire position `index`, with
    /// `flags` the already-cased wire spelling (`RO`, `access`, ...).
    fn key_specs(
        flags: &[&'static str],
        index: i64,
        relative_last_key: i64,
        keystep: i64,
    ) -> Response {
        Response::Array(vec![Response::Map(vec![
            (
                Response::bulk(Bytes::from_static(b"flags")),
                Response::Array(flags.iter().copied().map(flag).collect()),
            ),
            (
                Response::bulk(Bytes::from_static(b"begin_search")),
                Response::Map(vec![
                    (
                        Response::bulk(Bytes::from_static(b"type")),
                        Response::bulk(Bytes::from_static(b"index")),
                    ),
                    (
                        Response::bulk(Bytes::from_static(b"spec")),
                        Response::Map(vec![(
                            Response::bulk(Bytes::from_static(b"index")),
                            Response::Integer(index),
                        )]),
                    ),
                ]),
            ),
            (
                Response::bulk(Bytes::from_static(b"find_keys")),
                Response::Map(vec![
                    (
                        Response::bulk(Bytes::from_static(b"type")),
                        Response::bulk(Bytes::from_static(b"range")),
                    ),
                    (
                        Response::bulk(Bytes::from_static(b"spec")),
                        Response::Map(vec![
                            (
                                Response::bulk(Bytes::from_static(b"lastkey")),
                                Response::Integer(relative_last_key),
                            ),
                            (
                                Response::bulk(Bytes::from_static(b"keystep")),
                                Response::Integer(keystep),
                            ),
                            (
                                Response::bulk(Bytes::from_static(b"limit")),
                                Response::Integer(0),
                            ),
                        ]),
                    ),
                ]),
            ),
        ])])
    }

    /// Categories are listed in Redis's own `ACLCommandCategories` order, not
    /// in whatever order our ACL table happens to store them (`@read` before
    /// `@string` before `@fast`).
    #[test]
    fn command_info_get_full_reply() {
        assert_eq!(
            info("get"),
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"get")),
                Response::Integer(2),
                Response::Array(vec![flag("readonly"), flag("fast")]),
                Response::Integer(1),
                Response::Integer(1),
                Response::Integer(1),
                Response::Array(vec![category("read"), category("string"), category("fast"),]),
                Response::Array(vec![]),
                key_specs(&["RO", "access"], 1, 0, 1),
                Response::Array(vec![]),
            ])
        );
    }

    /// SET's vendored key spec carries `notes` and `variable_flags` (the
    /// optional `GET` argument turns the write into a read-modify-write), so
    /// the entry has four fields rather than three.
    #[test]
    fn command_info_set_full_reply() {
        let mut spec = match key_specs(&["RW", "access", "update", "variable_flags"], 1, 0, 1) {
            Response::Array(mut entries) => match entries.remove(0) {
                Response::Map(fields) => fields,
                other => panic!("expected a key-spec Map, got {other:?}"),
            },
            other => panic!("expected an Array, got {other:?}"),
        };
        spec.insert(
            0,
            (
                Response::bulk(Bytes::from_static(b"notes")),
                Response::bulk(Bytes::from_static(
                    b"RW and ACCESS due to the optional `GET` argument",
                )),
            ),
        );
        assert_eq!(
            info("set"),
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"set")),
                Response::Integer(-3),
                Response::Array(vec![flag("write"), flag("denyoom")]),
                Response::Integer(1),
                Response::Integer(1),
                Response::Integer(1),
                Response::Array(vec![
                    category("write"),
                    category("string"),
                    category("slow"),
                ]),
                Response::Array(vec![]),
                Response::Array(vec![Response::Map(spec)]),
                Response::Array(vec![]),
            ])
        );
    }

    /// MSET is the tipped case: its `request_policy`/`response_policy` pair
    /// survived the wave-D2 tip audit, so `COMMAND INFO` repeats it.
    #[test]
    fn command_info_mset_full_reply() {
        assert_eq!(
            info("mset"),
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"mset")),
                Response::Integer(-3),
                Response::Array(vec![flag("write"), flag("denyoom")]),
                Response::Integer(1),
                Response::Integer(-1),
                Response::Integer(2),
                Response::Array(vec![
                    category("write"),
                    category("string"),
                    category("slow"),
                ]),
                Response::Array(vec![
                    Response::bulk(Bytes::from_static(b"request_policy:multi_shard")),
                    Response::bulk(Bytes::from_static(b"response_policy:all_succeeded")),
                ]),
                key_specs(&["OW", "update"], 1, -1, 2),
                Response::Array(vec![]),
            ])
        );
    }

    /// SINTERCARD (`KeySpec::NumkeysAt`) — the movablekeys case: no static
    /// key position exists, so first/last/step are all `0` and `movablekeys`
    /// is asserted even though `SintercardCommand`'s own `CommandFlags` carry
    /// only `READONLY` (see `command_meta::effective_flags`). The structured
    /// key-specs array is still populated, from the vendored `keynum` spec —
    /// the one form the flat triplet cannot express.
    #[test]
    fn command_info_sintercard_reports_movablekeys() {
        assert_eq!(
            info("sintercard"),
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"sintercard")),
                Response::Integer(-3),
                Response::Array(vec![flag("readonly"), flag("movablekeys")]),
                Response::Integer(0),
                Response::Integer(0),
                Response::Integer(0),
                Response::Array(vec![category("read"), category("set"), category("slow")]),
                Response::Array(vec![]),
                Response::Array(vec![Response::Map(vec![
                    (
                        Response::bulk(Bytes::from_static(b"flags")),
                        Response::Array(vec![flag("RO"), flag("access")]),
                    ),
                    (
                        Response::bulk(Bytes::from_static(b"begin_search")),
                        Response::Map(vec![
                            (
                                Response::bulk(Bytes::from_static(b"type")),
                                Response::bulk(Bytes::from_static(b"index")),
                            ),
                            (
                                Response::bulk(Bytes::from_static(b"spec")),
                                Response::Map(vec![(
                                    Response::bulk(Bytes::from_static(b"index")),
                                    Response::Integer(1),
                                )]),
                            ),
                        ]),
                    ),
                    (
                        Response::bulk(Bytes::from_static(b"find_keys")),
                        Response::Map(vec![
                            (
                                Response::bulk(Bytes::from_static(b"type")),
                                Response::bulk(Bytes::from_static(b"keynum")),
                            ),
                            (
                                Response::bulk(Bytes::from_static(b"spec")),
                                Response::Map(vec![
                                    (
                                        Response::bulk(Bytes::from_static(b"keynumidx")),
                                        Response::Integer(0),
                                    ),
                                    (
                                        Response::bulk(Bytes::from_static(b"firstkey")),
                                        Response::Integer(1),
                                    ),
                                    (
                                        Response::bulk(Bytes::from_static(b"keystep")),
                                        Response::Integer(1),
                                    ),
                                ]),
                            ),
                        ]),
                    ),
                ])]),
                Response::Array(vec![]),
            ])
        );
    }

    #[test]
    fn command_info_unknown_command_is_nil() {
        assert_eq!(info("nosuchcommand"), Response::Bulk(None));
    }

    #[test]
    fn command_count_matches_registry_len() {
        let mut c = ctx_with_registry();
        let reply = CommandCommand
            .execute(&mut c, &[Bytes::from_static(b"COUNT")])
            .unwrap();
        assert_eq!(reply, Response::Integer(4));
    }

    /// Bare `COMMAND` (no subcommand) returns the same structured info as
    /// `COMMAND INFO` with no names — not the old empty-array placeholder.
    #[test]
    fn bare_command_returns_full_registry_info() {
        let mut c = ctx_with_registry();
        let reply = CommandCommand.execute(&mut c, &[]).unwrap();
        match reply {
            Response::Array(entries) => assert_eq!(entries.len(), 4),
            other => panic!("expected Array, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod command_docs_tests {
    //! `COMMAND DOCS` reply-shape tests (issue redis-feel/03). The docs come
    //! from the required `CommandSpec::docs` field, so "every command has
    //! documentation" is a compile-time property, not something a test can
    //! regress; what these pin is the *wire shape*: a map of name -> docs-map,
    //! the summary/since/group/complexity field order, the omission of
    //! `complexity` when the spec has none, and the skip-don't-nil behaviour
    //! Redis's `commandDocsCommand` has for unresolvable names.
    //!
    //! The "FrogDB extension" case (hand-written summary, no complexity) is
    //! pinned at the server level instead — every extension family in this
    //! crate is behind a cargo feature the core profile does not build, so
    //! `FROGDB.VERSION` in `redis-regression`'s `introspection_tcl` covers it
    //! against the full registry.
    use super::*;
    use crate::string::AppendCommand;
    use frogdb_core::{CommandRegistry, HashMapStore};
    use frogdb_protocol::ProtocolVersion;
    use std::sync::Arc;

    fn ctx_with_registry() -> CommandContext<'static> {
        let mut registry = CommandRegistry::new();
        registry.register(GetCommand);
        registry.register(AppendCommand);
        let registry: &'static Arc<CommandRegistry> = Box::leak(Box::new(Arc::new(registry)));

        let store = Box::leak(Box::new(HashMapStore::new()));
        let shard_senders = Box::leak(Box::new(Arc::new(Vec::new())));
        let mut c = CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2);
        c.command_registry = Some(registry);
        c
    }

    fn docs(names: &[&str]) -> Response {
        let mut c = ctx_with_registry();
        let mut args = vec![Bytes::from_static(b"DOCS")];
        args.extend(names.iter().map(|n| Bytes::from(n.to_string())));
        CommandCommand.execute(&mut c, &args).unwrap()
    }

    fn field(key: &'static str, value: &str) -> (Response, Response) {
        (
            Response::bulk(Bytes::from_static(key.as_bytes())),
            Response::bulk(Bytes::from(value.to_string())),
        )
    }

    /// A key argument, the simplest `arguments` node: name, type,
    /// `display_text` defaulted from the name, and the index of the key spec
    /// it fills.
    fn key_argument(name: &'static str, key_spec_index: i64) -> Response {
        Response::Map(vec![
            field("name", name),
            field("type", "key"),
            field("display_text", name),
            (
                Response::bulk(Bytes::from_static(b"key_spec_index")),
                Response::Integer(key_spec_index),
            ),
        ])
    }

    /// A vendored Redis command: summary/since/group/complexity from our own
    /// spec, then the vendored argument tree.
    #[test]
    fn command_docs_get_full_reply() {
        assert_eq!(
            docs(&["get"]),
            Response::Map(vec![(
                Response::bulk(Bytes::from_static(b"get")),
                Response::Map(vec![
                    field("summary", "Returns the string value of a key."),
                    field("since", "1.0.0"),
                    field("group", "string"),
                    field("complexity", "O(1)"),
                    (
                        Response::bulk(Bytes::from_static(b"arguments")),
                        Response::Array(vec![key_argument("key", 0)]),
                    ),
                ])
            )])
        );
    }

    /// `complexity` is emitted verbatim from the vendored data, however long
    /// — never truncated or reworded.
    #[test]
    fn command_docs_carries_verbatim_complexity() {
        assert_eq!(
            docs(&["append"]),
            Response::Map(vec![(
                Response::bulk(Bytes::from_static(b"append")),
                Response::Map(vec![
                    field(
                        "summary",
                        "Appends a string to the value of a key. Creates the key if it doesn't exist."
                    ),
                    field("since", "2.0.0"),
                    field("group", "string"),
                    field(
                        "complexity",
                        "O(1). The amortized time complexity is O(1) assuming the appended value is small and the already present value is of any size, since the dynamic string library used by Redis will double the free space available on every reallocation."
                    ),
                    (
                        Response::bulk(Bytes::from_static(b"arguments")),
                        Response::Array(vec![
                            key_argument("key", 0),
                            Response::Map(vec![
                                field("name", "value"),
                                field("type", "string"),
                                field("display_text", "value"),
                            ]),
                        ]),
                    ),
                ])
            )])
        );
    }

    /// Redis skips names it cannot resolve rather than replying nil for them,
    /// so the reply is shorter than the request instead of holding a hole.
    #[test]
    fn command_docs_skips_unknown_names() {
        assert_eq!(
            docs(&["nosuchcommand"]),
            Response::Map(vec![]),
            "unknown names are skipped entirely"
        );
        match docs(&["get", "nosuchcommand"]) {
            Response::Map(pairs) => {
                assert_eq!(pairs.len(), 1);
                assert_eq!(pairs[0].0, Response::bulk(Bytes::from_static(b"get")));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    /// Subcommands are not separate registry entries (same limitation
    /// `build_command_info` reports through an empty subcommands array), so a
    /// dotted/piped name resolves to nothing and is skipped.
    #[test]
    fn command_docs_piped_subcommand_name_is_skipped() {
        assert_eq!(docs(&["command|docs"]), Response::Map(vec![]));
    }

    /// No-argument form covers the whole registry.
    #[test]
    fn command_docs_no_args_covers_registry() {
        let mut c = ctx_with_registry();
        let reply = CommandCommand
            .execute(&mut c, &[Bytes::from_static(b"DOCS")])
            .unwrap();
        match reply {
            Response::Map(pairs) => {
                assert_eq!(pairs.len(), 2, "one entry per registered command");
                for (_, docs) in pairs {
                    match docs {
                        // summary/since/group are always present; everything
                        // after them is emitted only where a source exists.
                        Response::Map(fields) => {
                            assert!(fields.len() >= 3, "unexpected docs field count: {fields:?}")
                        }
                        other => panic!("expected a docs Map, got {other:?}"),
                    }
                }
            }
            other => panic!("expected Map, got {other:?}"),
        }
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

    /// A successful SET is a real write — it must NOT declare itself a no-op
    /// (positive control for the NX/XX miss assertions below).
    #[test]
    fn set_write_is_not_noop() {
        let mut c = ctx();
        SetCommand.execute(&mut c, &args(&["k", "v"])).unwrap();
        assert!(!c.effects.write_was_noop, "a real SET is not a no-op");
    }

    /// Finding 3: `SET k v NX` on an *existing* key sets nothing (`NotSet`). It
    /// must declare the write a no-op so the effect pipeline is skipped — no
    /// reindex (SET is `RefreshFirstKey`, so otherwise it would re-index the
    /// whole existing key), no WAL, no replication, no keyspace notification,
    /// no WATCH dirty. Redis returns before signalModifiedKey on an NX miss.
    #[test]
    fn set_nx_miss_is_noop() {
        let mut c = ctx();
        SetCommand.execute(&mut c, &args(&["k", "v"])).unwrap();
        // Fresh effects for the second command.
        c.effects = Default::default();
        let r = SetCommand
            .execute(&mut c, &args(&["k", "v2", "NX"]))
            .unwrap();
        assert_eq!(r, Response::null(), "NX miss returns nil");
        assert!(
            c.effects.write_was_noop,
            "SET NX that did not set must be a no-op write"
        );
    }

    /// `SET k v XX` on a *missing* key sets nothing (`NotSet`) — the same no-op
    /// contract as the NX miss.
    #[test]
    fn set_xx_miss_is_noop() {
        let mut c = ctx();
        let r = SetCommand
            .execute(&mut c, &args(&["absent", "v", "XX"]))
            .unwrap();
        assert_eq!(r, Response::null(), "XX miss returns nil");
        assert!(
            c.effects.write_was_noop,
            "SET XX that did not set must be a no-op write"
        );
    }

    /// The GET variant of an NX miss (`SET k v NX GET`) returns the old value but
    /// still writes nothing — it is a no-op write.
    #[test]
    fn set_nx_get_miss_is_noop() {
        let mut c = ctx();
        SetCommand.execute(&mut c, &args(&["k", "old"])).unwrap();
        c.effects = Default::default();
        let r = SetCommand
            .execute(&mut c, &args(&["k", "new", "NX", "GET"]))
            .unwrap();
        assert_eq!(r, Response::bulk(Bytes::from_static(b"old")));
        assert!(
            c.effects.write_was_noop,
            "SET NX GET that did not set must be a no-op write"
        );
    }
}
