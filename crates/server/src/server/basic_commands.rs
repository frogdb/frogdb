use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ExecutionStrategy, Expiry,
    MergeStrategy, SetCondition, SetOptions, SetResult, Value,
};
use frogdb_protocol::Response;

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

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.is_empty() {
            // COMMAND - return info about all commands
            return Ok(Response::Array(vec![])); // Simplified for now
        }

        let subcommand = args[0].to_ascii_uppercase();
        match subcommand.as_slice() {
            b"COUNT" => {
                // COMMAND COUNT - return number of commands
                // Approximate count of supported commands
                Ok(Response::Integer(100)) // Placeholder count
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
                        // Build basic command info
                        // Format: [name, arity, [flags], first_key, last_key, step]
                        let info = Response::Array(vec![
                            Response::bulk(cmd_name.clone()),
                            Response::Integer(-1), // Variable arity
                            Response::Array(vec![]), // Flags
                            Response::Integer(0), // First key
                            Response::Integer(0), // Last key
                            Response::Integer(0), // Step
                        ]);
                        results.push(info);
                    }
                    Ok(Response::Array(results))
                }
            }
            b"GETKEYS" => {
                // COMMAND GETKEYS command [args...] - return keys for a command
                if args.len() < 2 {
                    return Err(CommandError::WrongArity { command: "COMMAND|GETKEYS" });
                }
                // For simplicity, return empty array
                Ok(Response::Array(vec![]))
            }
            b"HELP" => {
                // COMMAND HELP
                let help = vec![
                    Response::bulk(Bytes::from_static(b"COMMAND [subcommand [arg [arg ...]]]")),
                    Response::bulk(Bytes::from_static(b"Return info about Redis commands.")),
                    Response::bulk(Bytes::from_static(b"Subcommands:")),
                    Response::bulk(Bytes::from_static(b"  (no subcommand) -- Return info about all commands")),
                    Response::bulk(Bytes::from_static(b"  COUNT -- Return count of commands")),
                    Response::bulk(Bytes::from_static(b"  DOCS [cmd ...] -- Return documentation for commands")),
                    Response::bulk(Bytes::from_static(b"  INFO [cmd ...] -- Return info for commands")),
                    Response::bulk(Bytes::from_static(b"  GETKEYS cmd [args...] -- Extract keys from command")),
                    Response::bulk(Bytes::from_static(b"  HELP -- Print this help")),
                ];
                Ok(Response::Array(help))
            }
            _ => {
                Err(CommandError::InvalidArgument {
                    message: format!(
                        "unknown subcommand '{}'. Try COMMAND HELP.",
                        String::from_utf8_lossy(&subcommand)
                    ),
                })
            }
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
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = args[0].clone();
        let value = args[1].clone();

        // Parse options
        let mut opts = SetOptions::default();
        let mut i = 2;

        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"NX" => {
                    if opts.condition != SetCondition::Always {
                        return Err(CommandError::SyntaxError);
                    }
                    opts.condition = SetCondition::NX;
                }
                b"XX" => {
                    if opts.condition != SetCondition::Always {
                        return Err(CommandError::SyntaxError);
                    }
                    opts.condition = SetCondition::XX;
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
                    let secs = parse_u64(&args[i])?;
                    if secs == 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'set' command".to_string(),
                        });
                    }
                    opts.expiry = Some(Expiry::Ex(secs));
                }
                b"PX" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let ms = parse_u64(&args[i])?;
                    if ms == 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'set' command".to_string(),
                        });
                    }
                    opts.expiry = Some(Expiry::Px(ms));
                }
                b"EXAT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let ts = parse_u64(&args[i])?;
                    opts.expiry = Some(Expiry::ExAt(ts));
                }
                b"PXAT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let ts = parse_u64(&args[i])?;
                    opts.expiry = Some(Expiry::PxAt(ts));
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        // Check for conflicting options
        if opts.keep_ttl && opts.expiry.is_some() {
            return Err(CommandError::SyntaxError);
        }

        match ctx.store.set_with_options(key, Value::string(value), opts) {
            SetResult::Ok => Ok(Response::ok()),
            SetResult::OkWithOldValue(old) => {
                match old {
                    Some(v) => {
                        if let Some(sv) = v.as_string() {
                            Ok(Response::bulk(sv.as_bytes()))
                        } else {
                            // Old value was wrong type but we replaced it anyway
                            Ok(Response::null())
                        }
                    }
                    None => Ok(Response::null()),
                }
            }
            SetResult::NotSet => Ok(Response::null()),
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

/// Parse a string as u64.
fn parse_u64(arg: &[u8]) -> Result<u64, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotInteger)
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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
