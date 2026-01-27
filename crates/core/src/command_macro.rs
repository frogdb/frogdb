//! Command definition macros.
//!
//! Provides declarative macros to reduce boilerplate when defining Redis commands.
//!
//! # Example
//!
//! ```rust,ignore
//! use frogdb_core::{define_command, Arity, CommandFlags};
//!
//! define_command! {
//!     /// Get the value of a key.
//!     pub struct GetCommand;
//!     name: "GET",
//!     arity: Fixed(1),
//!     flags: (READONLY | FAST),
//!     keys: first,
//!     execute(ctx, args) {
//!         let key = &args[0];
//!         Ok(ctx.store.get(key)
//!             .map(|v| Response::bulk(v.as_bytes()))
//!             .unwrap_or(Response::Null))
//!     }
//! }
//! ```

/// Defines a Redis command with reduced boilerplate.
///
/// This macro generates the command struct and implements the `Command` trait,
/// handling common patterns for key extraction and metadata.
///
/// # Syntax
///
/// ```rust,ignore
/// define_command! {
///     /// Optional doc comment
///     pub struct CommandName;
///     name: "COMMAND_NAME",          // Command name string
///     arity: Fixed(1),               // Arity::Fixed, AtLeast, or Range
///     flags: (READONLY | FAST),      // CommandFlags in parentheses
///     keys: first,                   // Key extraction pattern (see below)
///     execute(ctx, args) {           // Execute implementation
///         // ... command logic ...
///     }
/// }
/// ```
///
/// # Key Patterns
///
/// - `none` - No keys (PING, INFO, etc.)
/// - `first` - First argument is the key (GET, SET, etc.)
/// - `two` - First two arguments are keys (RENAME, etc.)
/// - `all` - All arguments are keys (DEL, MGET with scatter-gather)
/// - `skip(N)` - Skip first N args, rest are keys
/// - `range(start, end)` - Keys from args[start] to args[end]
///
/// # Optional Fields
///
/// - `strategy: ExecutionStrategy` - Override default Standard execution
/// - `requires_same_slot: true` - Require all keys in same slot (MSETNX)
///
/// # Example with Strategy
///
/// ```rust,ignore
/// define_command! {
///     pub struct MgetCommand;
///     name: "MGET",
///     arity: AtLeast(1),
///     flags: (READONLY | MULTI_KEY),
///     strategy: ScatterGather { merge: MergeStrategy::OrderedArray },
///     keys: all,
///     execute(ctx, args) {
///         // ...
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_command {
    // Full form with strategy and requires_same_slot
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        flags: ( $($flags:tt)+ ),
        strategy: $strategy:expr,
        requires_same_slot: $same_slot:expr,
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        execute($ctx:ident, $args:ident) $body:block
    ) => {
        $(#[$meta])*
        pub struct $struct;

        impl $crate::Command for $struct {
            fn name(&self) -> &'static str {
                $name
            }

            fn arity(&self) -> $crate::Arity {
                $crate::define_command!(@arity $arity_kind ( $($arity_args)* ))
            }

            fn flags(&self) -> $crate::CommandFlags {
                $crate::define_command!(@flags $($flags)+)
            }

            fn execution_strategy(&self) -> $crate::ExecutionStrategy {
                $strategy
            }

            fn requires_same_slot(&self) -> bool {
                $same_slot
            }

            fn execute(
                &self,
                $ctx: &mut $crate::CommandContext,
                $args: &[bytes::Bytes],
            ) -> Result<frogdb_protocol::Response, $crate::CommandError> {
                $body
            }

            fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
                $crate::define_command!(@keys args, $keys $( ( $($keys_args)* ) )? )
            }
        }
    };

    // With strategy, no requires_same_slot
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        flags: ( $($flags:tt)+ ),
        strategy: $strategy:expr,
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        execute($ctx:ident, $args:ident) $body:block
    ) => {
        $(#[$meta])*
        pub struct $struct;

        impl $crate::Command for $struct {
            fn name(&self) -> &'static str {
                $name
            }

            fn arity(&self) -> $crate::Arity {
                $crate::define_command!(@arity $arity_kind ( $($arity_args)* ))
            }

            fn flags(&self) -> $crate::CommandFlags {
                $crate::define_command!(@flags $($flags)+)
            }

            fn execution_strategy(&self) -> $crate::ExecutionStrategy {
                $strategy
            }

            fn execute(
                &self,
                $ctx: &mut $crate::CommandContext,
                $args: &[bytes::Bytes],
            ) -> Result<frogdb_protocol::Response, $crate::CommandError> {
                $body
            }

            fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
                $crate::define_command!(@keys args, $keys $( ( $($keys_args)* ) )? )
            }
        }
    };

    // Without strategy (uses default Standard)
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        flags: ( $($flags:tt)+ ),
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        execute($ctx:ident, $args:ident) $body:block
    ) => {
        $(#[$meta])*
        pub struct $struct;

        impl $crate::Command for $struct {
            fn name(&self) -> &'static str {
                $name
            }

            fn arity(&self) -> $crate::Arity {
                $crate::define_command!(@arity $arity_kind ( $($arity_args)* ))
            }

            fn flags(&self) -> $crate::CommandFlags {
                $crate::define_command!(@flags $($flags)+)
            }

            fn execute(
                &self,
                $ctx: &mut $crate::CommandContext,
                $args: &[bytes::Bytes],
            ) -> Result<frogdb_protocol::Response, $crate::CommandError> {
                $body
            }

            fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
                $crate::define_command!(@keys args, $keys $( ( $($keys_args)* ) )? )
            }
        }
    };

    // =========================================================================
    // Arity parsing helpers
    // =========================================================================

    (@arity Fixed ($n:expr)) => {
        $crate::Arity::Fixed($n)
    };
    (@arity AtLeast ($n:expr)) => {
        $crate::Arity::AtLeast($n)
    };
    (@arity Range ($min:expr, $max:expr)) => {
        $crate::Arity::Range { min: $min, max: $max }
    };

    // =========================================================================
    // Flags parsing helpers
    // =========================================================================

    // Single flag
    (@flags WRITE) => { $crate::CommandFlags::WRITE };
    (@flags READONLY) => { $crate::CommandFlags::READONLY };
    (@flags FAST) => { $crate::CommandFlags::FAST };
    (@flags BLOCKING) => { $crate::CommandFlags::BLOCKING };
    (@flags MULTI_KEY) => { $crate::CommandFlags::MULTI_KEY };
    (@flags PUBSUB) => { $crate::CommandFlags::PUBSUB };
    (@flags SCRIPT) => { $crate::CommandFlags::SCRIPT };
    (@flags NOSCRIPT) => { $crate::CommandFlags::NOSCRIPT };
    (@flags LOADING) => { $crate::CommandFlags::LOADING };
    (@flags STALE) => { $crate::CommandFlags::STALE };
    (@flags SKIP_SLOWLOG) => { $crate::CommandFlags::SKIP_SLOWLOG };
    (@flags RANDOM) => { $crate::CommandFlags::RANDOM };
    (@flags ADMIN) => { $crate::CommandFlags::ADMIN };
    (@flags NONDETERMINISTIC) => { $crate::CommandFlags::NONDETERMINISTIC };
    (@flags NO_PROPAGATE) => { $crate::CommandFlags::NO_PROPAGATE };

    // Combined flags with |
    (@flags $first:ident | $($rest:tt)+) => {
        $crate::define_command!(@flags $first) | $crate::define_command!(@flags $($rest)+)
    };

    // =========================================================================
    // Key extraction helpers
    // =========================================================================

    // No keys (use #[allow] since $args is passed but not used)
    (@keys $args:ident, none) => {{
        let _ = $args;
        vec![]
    }};

    // First argument is the key
    (@keys $args:ident, first) => {
        if $args.is_empty() {
            vec![]
        } else {
            vec![&$args[0]]
        }
    };

    // First two arguments are keys
    (@keys $args:ident, two) => {
        if $args.len() < 2 {
            vec![]
        } else {
            vec![&$args[0], &$args[1]]
        }
    };

    // All arguments are keys
    (@keys $args:ident, all) => {
        $args.iter().map(|a| a.as_ref()).collect()
    };

    // Skip first N args, rest are keys
    (@keys $args:ident, skip ($n:expr)) => {
        if $args.len() <= $n {
            vec![]
        } else {
            $args[$n..].iter().map(|a| a.as_ref()).collect()
        }
    };

    // Keys from range [start, end)
    (@keys $args:ident, range ($start:expr, $end:expr)) => {
        if $args.len() < $end {
            vec![]
        } else {
            $args[$start..$end].iter().map(|a| a.as_ref()).collect()
        }
    };
}

/// Defines a metadata-only command (for commands handled at connection level).
///
/// These commands are intercepted by the connection handler before normal
/// command execution. They only need metadata for introspection (COMMAND INFO).
///
/// # Example
///
/// ```rust,ignore
/// define_metadata_command! {
///     pub struct SubscribeMetadata;
///     name: "SUBSCRIBE",
///     arity: AtLeast(1),
///     flags: (PUBSUB),
///     strategy: ConnectionLevel(ConnectionLevelOp::PubSub),
///     keys: all,
/// }
/// ```
#[macro_export]
macro_rules! define_metadata_command {
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        flags: ( $($flags:tt)+ ),
        strategy: $strategy:expr,
        keys: $keys:ident $( ( $($keys_args:tt)* ) )? $(,)?
    ) => {
        $(#[$meta])*
        pub struct $struct;

        impl $crate::CommandMetadata for $struct {
            fn name(&self) -> &'static str {
                $name
            }

            fn arity(&self) -> $crate::Arity {
                $crate::define_command!(@arity $arity_kind ( $($arity_args)* ))
            }

            fn flags(&self) -> $crate::CommandFlags {
                $crate::define_command!(@flags $($flags)+)
            }

            fn execution_strategy(&self) -> $crate::ExecutionStrategy {
                $strategy
            }

            fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
                $crate::define_command!(@keys args, $keys $( ( $($keys_args)* ) )? )
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use frogdb_protocol::Response;

    use crate::{
        Arity, Command, CommandContext, CommandError, CommandFlags, CommandMetadata,
        ConnectionLevelOp, ExecutionStrategy, MergeStrategy,
    };

    // Test basic command definition
    define_command! {
        /// A simple test command.
        pub struct TestGetCommand;
        name: "TEST_GET",
        arity: Fixed(1),
        flags: (READONLY | FAST),
        keys: first,
        execute(_ctx, args) {
            Ok(Response::bulk(args[0].clone()))
        }
    }

    #[test]
    fn test_basic_command() {
        assert_eq!(TestGetCommand.name(), "TEST_GET");
        assert!(matches!(TestGetCommand.arity(), Arity::Fixed(1)));
        assert_eq!(
            TestGetCommand.flags(),
            CommandFlags::READONLY | CommandFlags::FAST
        );
        assert!(matches!(
            TestGetCommand.execution_strategy(),
            ExecutionStrategy::Standard
        ));

        let args = vec![Bytes::from_static(b"key1")];
        let keys = TestGetCommand.keys(&args);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], b"key1");
    }

    // Test command with strategy
    define_command! {
        pub struct TestMgetCommand;
        name: "TEST_MGET",
        arity: AtLeast(1),
        flags: (READONLY | MULTI_KEY),
        strategy: ExecutionStrategy::ScatterGather {
            merge: MergeStrategy::OrderedArray,
        },
        keys: all,
        execute(_ctx, args) {
            let responses: Vec<Response> = args.iter().map(|_| Response::Null).collect();
            Ok(Response::Array(responses))
        }
    }

    #[test]
    fn test_command_with_strategy() {
        assert_eq!(TestMgetCommand.name(), "TEST_MGET");
        assert!(matches!(TestMgetCommand.arity(), Arity::AtLeast(1)));
        assert!(matches!(
            TestMgetCommand.execution_strategy(),
            ExecutionStrategy::ScatterGather {
                merge: MergeStrategy::OrderedArray
            }
        ));

        let args = vec![
            Bytes::from_static(b"key1"),
            Bytes::from_static(b"key2"),
            Bytes::from_static(b"key3"),
        ];
        let keys = TestMgetCommand.keys(&args);
        assert_eq!(keys.len(), 3);
    }

    // Test command with requires_same_slot
    define_command! {
        pub struct TestMsetnxCommand;
        name: "TEST_MSETNX",
        arity: AtLeast(2),
        flags: (WRITE | MULTI_KEY),
        strategy: ExecutionStrategy::ScatterGather {
            merge: MergeStrategy::AllOk,
        },
        requires_same_slot: true,
        keys: skip(0),
        execute(_ctx, _args) {
            Ok(Response::Integer(1))
        }
    }

    #[test]
    fn test_command_requires_same_slot() {
        assert!(TestMsetnxCommand.requires_same_slot());
    }

    // Test no keys pattern
    define_command! {
        pub struct TestPingCommand;
        name: "TEST_PING",
        arity: Range(0, 1),
        flags: (READONLY | FAST | STALE),
        keys: none,
        execute(_ctx, _args) {
            Ok(Response::Simple(Bytes::from_static(b"PONG")))
        }
    }

    #[test]
    fn test_no_keys() {
        let args: Vec<Bytes> = vec![];
        let keys = TestPingCommand.keys(&args);
        assert!(keys.is_empty());

        assert!(matches!(
            TestPingCommand.arity(),
            Arity::Range { min: 0, max: 1 }
        ));
    }

    // Test two keys pattern
    define_command! {
        pub struct TestRenameCommand;
        name: "TEST_RENAME",
        arity: Fixed(2),
        flags: (WRITE),
        keys: two,
        execute(_ctx, _args) {
            Ok(Response::ok())
        }
    }

    #[test]
    fn test_two_keys() {
        let args = vec![Bytes::from_static(b"old"), Bytes::from_static(b"new")];
        let keys = TestRenameCommand.keys(&args);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], b"old");
        assert_eq!(keys[1], b"new");
    }

    // Test skip pattern
    define_command! {
        pub struct TestCopyCommand;
        name: "TEST_COPY",
        arity: AtLeast(2),
        flags: (WRITE),
        keys: skip(1),
        execute(_ctx, _args) {
            Ok(Response::Integer(1))
        }
    }

    #[test]
    fn test_skip_keys() {
        let args = vec![
            Bytes::from_static(b"source"),
            Bytes::from_static(b"dest1"),
            Bytes::from_static(b"dest2"),
        ];
        let keys = TestCopyCommand.keys(&args);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], b"dest1");
        assert_eq!(keys[1], b"dest2");
    }

    // Test metadata command
    define_metadata_command! {
        pub struct TestSubscribeMetadata;
        name: "TEST_SUBSCRIBE",
        arity: AtLeast(1),
        flags: (PUBSUB),
        strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub),
        keys: all,
    }

    #[test]
    fn test_metadata_command() {
        assert_eq!(TestSubscribeMetadata.name(), "TEST_SUBSCRIBE");
        assert!(matches!(TestSubscribeMetadata.arity(), Arity::AtLeast(1)));
        assert_eq!(TestSubscribeMetadata.flags(), CommandFlags::PUBSUB);
        assert!(matches!(
            TestSubscribeMetadata.execution_strategy(),
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
        ));

        let args = vec![
            Bytes::from_static(b"channel1"),
            Bytes::from_static(b"channel2"),
        ];
        let keys = TestSubscribeMetadata.keys(&args);
        assert_eq!(keys.len(), 2);
    }

    // Test empty args edge cases
    #[test]
    fn test_empty_args() {
        let empty: Vec<Bytes> = vec![];

        // first pattern with empty args
        let keys = TestGetCommand.keys(&empty);
        assert!(keys.is_empty());

        // two pattern with empty args
        let keys = TestRenameCommand.keys(&empty);
        assert!(keys.is_empty());

        // two pattern with one arg
        let one = vec![Bytes::from_static(b"one")];
        let keys = TestRenameCommand.keys(&one);
        assert!(keys.is_empty());

        // skip pattern with insufficient args
        let keys = TestCopyCommand.keys(&one);
        assert!(keys.is_empty());
    }
}
