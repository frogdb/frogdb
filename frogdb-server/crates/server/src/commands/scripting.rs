//! Scripting commands: EVAL, EVALSHA, SCRIPT.

use bytes::Bytes;
use frogdb_core::scripting::{compute_sha, sha_to_hex};
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy,
};
use frogdb_protocol::Response;

/// EVAL command - execute a Lua script.
///
/// EVAL script numkeys [key ...] [arg ...]
pub struct EvalCommand;

impl Command for EvalCommand {
    fn name(&self) -> &'static str {
        "EVAL"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // script numkeys [key ...] [arg ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::SCRIPT | CommandFlags::NONDETERMINISTIC | CommandFlags::MOVABLEKEYS
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This is a placeholder - actual execution is handled at the connection level
        // because we need to route to the correct shard based on keys
        //
        // The connection handler will:
        // 1. Parse the script, numkeys, keys, and argv
        // 2. Route to the appropriate shard
        // 3. Send a ShardMessage::EvalScript message
        //
        // For now, we return an error indicating the command should be routed
        Err(CommandError::Internal {
            message: "EVAL should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // EVAL script numkeys key [key ...] arg [arg ...]
        if args.len() < 2 {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        if numkeys == 0 || args.len() < 2 + numkeys {
            return vec![];
        }

        args[2..2 + numkeys].iter().map(|k| k.as_ref()).collect()
    }
}

/// EVALSHA command - execute a cached Lua script by SHA.
///
/// EVALSHA sha1 numkeys [key ...] [arg ...]
pub struct EvalshaCommand;

impl Command for EvalshaCommand {
    fn name(&self) -> &'static str {
        "EVALSHA"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // sha1 numkeys [key ...] [arg ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::SCRIPT | CommandFlags::NONDETERMINISTIC | CommandFlags::MOVABLEKEYS
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled by connection handler
        Err(CommandError::Internal {
            message: "EVALSHA should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // EVALSHA sha1 numkeys key [key ...] arg [arg ...]
        if args.len() < 2 {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        if numkeys == 0 || args.len() < 2 + numkeys {
            return vec![];
        }

        args[2..2 + numkeys].iter().map(|k| k.as_ref()).collect()
    }
}

/// EVAL_RO command - execute a Lua script in read-only mode.
///
/// EVAL_RO script numkeys [key ...] [arg ...]
///
/// Identical to EVAL but declares read-only intent. This allows:
/// - Execution on replicas in cluster mode
/// - Not being blocked by CLIENT PAUSE WRITE
pub struct EvalRoCommand;

impl Command for EvalRoCommand {
    fn name(&self) -> &'static str {
        "EVAL_RO"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::SCRIPT
            | CommandFlags::READONLY
            | CommandFlags::NONDETERMINISTIC
            | CommandFlags::MOVABLEKEYS
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        Err(CommandError::Internal {
            message: "EVAL_RO should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        EvalCommand.keys(args)
    }
}

/// EVALSHA_RO command - execute a cached Lua script by SHA in read-only mode.
///
/// EVALSHA_RO sha1 numkeys [key ...] [arg ...]
pub struct EvalshaRoCommand;

impl Command for EvalshaRoCommand {
    fn name(&self) -> &'static str {
        "EVALSHA_RO"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::SCRIPT
            | CommandFlags::READONLY
            | CommandFlags::NONDETERMINISTIC
            | CommandFlags::MOVABLEKEYS
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        Err(CommandError::Internal {
            message: "EVALSHA_RO should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        EvalshaCommand.keys(args)
    }
}

/// SCRIPT command - manage Lua scripts.
///
/// Subcommands:
/// - SCRIPT LOAD script
/// - SCRIPT EXISTS sha1 [sha1 ...]
/// - SCRIPT FLUSH [ASYNC|SYNC]
/// - SCRIPT KILL
pub struct ScriptCommand;

impl Command for ScriptCommand {
    fn name(&self) -> &'static str {
        "SCRIPT"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // subcommand [args...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled by connection handler
        Err(CommandError::Internal {
            message: "SCRIPT should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // SCRIPT commands are keyless
        vec![]
    }
}

/// Parse EVAL/EVALSHA arguments.
///
/// Returns (numkeys, keys, argv) or an error.
pub fn parse_eval_args(
    args: &[Bytes],
    offset: usize,
) -> Result<(Vec<Bytes>, Vec<Bytes>), CommandError> {
    if args.len() < offset + 2 {
        return Err(CommandError::WrongArity { command: "eval" });
    }

    // Parse numkeys
    let numkeys_str =
        std::str::from_utf8(&args[offset + 1]).map_err(|_| CommandError::NotInteger)?;
    let numkeys: usize = numkeys_str.parse().map_err(|_| CommandError::NotInteger)?;

    // Check we have enough arguments for keys
    if args.len() < offset + 2 + numkeys {
        return Err(CommandError::InvalidArgument {
            message: "Number of keys can't be greater than number of args".to_string(),
        });
    }

    // Extract keys and argv
    let keys: Vec<Bytes> = args[offset + 2..offset + 2 + numkeys].to_vec();
    let argv: Vec<Bytes> = args[offset + 2 + numkeys..].to_vec();

    Ok((keys, argv))
}

/// Compute SHA1 of a script and return as hex string.
pub fn script_sha(source: &[u8]) -> String {
    let sha = compute_sha(source);
    sha_to_hex(&sha)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_eval_args_simple() {
        let args = vec![Bytes::from_static(b"return 1"), Bytes::from_static(b"0")];
        let (keys, argv) = parse_eval_args(&args, 0).unwrap();
        assert!(keys.is_empty());
        assert!(argv.is_empty());
    }

    #[test]
    fn test_parse_eval_args_with_keys() {
        let args = vec![
            Bytes::from_static(b"return redis.call('GET', KEYS[1])"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"mykey"),
        ];
        let (keys, argv) = parse_eval_args(&args, 0).unwrap();
        assert_eq!(keys, vec![Bytes::from_static(b"mykey")]);
        assert!(argv.is_empty());
    }

    #[test]
    fn test_parse_eval_args_with_keys_and_argv() {
        let args = vec![
            Bytes::from_static(b"return redis.call('SET', KEYS[1], ARGV[1])"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"mykey"),
            Bytes::from_static(b"myvalue"),
        ];
        let (keys, argv) = parse_eval_args(&args, 0).unwrap();
        assert_eq!(keys, vec![Bytes::from_static(b"mykey")]);
        assert_eq!(argv, vec![Bytes::from_static(b"myvalue")]);
    }

    #[test]
    fn test_parse_eval_args_invalid_numkeys() {
        let args = vec![
            Bytes::from_static(b"return 1"),
            Bytes::from_static(b"invalid"),
        ];
        assert!(parse_eval_args(&args, 0).is_err());
    }

    #[test]
    fn test_parse_eval_args_not_enough_keys() {
        let args = vec![
            Bytes::from_static(b"return 1"),
            Bytes::from_static(b"3"), // Says 3 keys but only 1 provided
            Bytes::from_static(b"key1"),
        ];
        assert!(parse_eval_args(&args, 0).is_err());
    }

    #[test]
    fn test_script_sha_deterministic() {
        let source = b"return 'hello'";
        let sha1 = script_sha(source);
        let sha2 = script_sha(source);
        assert_eq!(sha1, sha2);
        assert_eq!(sha1.len(), 40);
    }

    #[test]
    fn test_eval_command_keys() {
        let cmd = EvalCommand;

        // No keys
        let args = vec![Bytes::from_static(b"return 1"), Bytes::from_static(b"0")];
        assert!(cmd.keys(&args).is_empty());

        // With keys
        let args = vec![
            Bytes::from_static(b"return 1"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"key1"),
            Bytes::from_static(b"key2"),
            Bytes::from_static(b"arg1"),
        ];
        let keys = cmd.keys(&args);
        assert_eq!(keys.len(), 2);
    }
}
