//! FUNCTION and FCALL command implementations.

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy,
};
use frogdb_protocol::Response;

/// FUNCTION command - manages function libraries.
///
/// Subcommands:
/// - FUNCTION LOAD [REPLACE] code
/// - FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]
/// - FUNCTION DELETE library-name
/// - FUNCTION FLUSH [ASYNC|SYNC]
/// - FUNCTION STATS
/// - FUNCTION KILL
/// - FUNCTION DUMP
/// - FUNCTION RESTORE payload [APPEND|REPLACE|FLUSH]
pub struct FunctionCommand;

impl Command for FunctionCommand {
    fn name(&self) -> &'static str {
        "FUNCTION"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::NOSCRIPT
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This command is handled at the connection level
        Err(CommandError::Internal {
            message: "FUNCTION should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

/// FCALL command - execute a function.
///
/// Syntax: FCALL function numkeys [key ...] [arg ...]
pub struct FcallCommand;

impl Command for FcallCommand {
    fn name(&self) -> &'static str {
        "FCALL"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // function numkeys
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::SCRIPT | CommandFlags::NOSCRIPT | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This command is handled at the connection level (like EVAL)
        Err(CommandError::Internal {
            message: "FCALL should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // FCALL function numkeys key [key ...] arg [arg ...]
        if args.len() < 2 {
            return vec![];
        }

        // Parse numkeys from args[1]
        let numkeys = match std::str::from_utf8(&args[1]) {
            Ok(s) => s.parse::<usize>().unwrap_or(0),
            Err(_) => 0,
        };

        if numkeys == 0 || args.len() < 2 + numkeys {
            return vec![];
        }

        args[2..2 + numkeys].iter().map(|k| k.as_ref()).collect()
    }
}

/// FCALL_RO command - execute a read-only function.
///
/// Syntax: FCALL_RO function numkeys [key ...] [arg ...]
pub struct FcallRoCommand;

impl Command for FcallRoCommand {
    fn name(&self) -> &'static str {
        "FCALL_RO"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // function numkeys
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::SCRIPT | CommandFlags::NOSCRIPT | CommandFlags::READONLY | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This command is handled at the connection level (like EVALSHA)
        Err(CommandError::Internal {
            message: "FCALL_RO should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Same as FCALL
        if args.len() < 2 {
            return vec![];
        }

        let numkeys = match std::str::from_utf8(&args[1]) {
            Ok(s) => s.parse::<usize>().unwrap_or(0),
            Err(_) => 0,
        };

        if numkeys == 0 || args.len() < 2 + numkeys {
            return vec![];
        }

        args[2..2 + numkeys].iter().map(|k| k.as_ref()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fcall_keys_extraction() {
        let cmd = FcallCommand;

        // FCALL myfunc 2 key1 key2 arg1
        let args = vec![
            Bytes::from("myfunc"),
            Bytes::from("2"),
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("arg1"),
        ];

        let keys = cmd.keys(&args);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], b"key1");
        assert_eq!(keys[1], b"key2");
    }

    #[test]
    fn test_fcall_no_keys() {
        let cmd = FcallCommand;

        // FCALL myfunc 0 arg1 arg2
        let args = vec![
            Bytes::from("myfunc"),
            Bytes::from("0"),
            Bytes::from("arg1"),
            Bytes::from("arg2"),
        ];

        let keys = cmd.keys(&args);
        assert!(keys.is_empty());
    }

    #[test]
    fn test_fcall_insufficient_args() {
        let cmd = FcallCommand;

        // FCALL myfunc (missing numkeys)
        let args = vec![Bytes::from("myfunc")];
        let keys = cmd.keys(&args);
        assert!(keys.is_empty());
    }
}
