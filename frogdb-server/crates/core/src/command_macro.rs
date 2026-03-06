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
//!
//! # Convenience Macros
//!
//! For common patterns, use the specialized macros:
//!
//! - [`define_readonly_command!`] - For read-only commands (READONLY | FAST flags)
//! - [`define_write_command!`] - For write commands (WRITE | FAST flags)
//!
//! # Validation Block
//!
//! Commands can include a `validate(args)` block for pre-execution validation:
//!
//! ```rust,ignore
//! define_command! {
//!     pub struct SetexCommand;
//!     name: "SETEX",
//!     arity: Fixed(3),
//!     flags: (WRITE | FAST),
//!     keys: first,
//!     validate(args) {
//!         let seconds = parse_u64(&args[1])?;
//!         if seconds == 0 {
//!             return Err(CommandError::InvalidArgument {
//!                 message: "invalid expire time in 'setex' command".to_string(),
//!             });
//!         }
//!     }
//!     execute(ctx, args) {
//!         // ... command logic ...
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

    // =========================================================================
    // Variant with validate block
    // =========================================================================

    // With validate block (no strategy)
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        flags: ( $($flags:tt)+ ),
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        validate($v_args:ident) $validate:block
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
                // Run validation first
                {
                    let $v_args = $args;
                    $validate
                }
                // Then execute
                $body
            }

            fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
                $crate::define_command!(@keys args, $keys $( ( $($keys_args)* ) )? )
            }
        }
    };

    // With validate block and strategy
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        flags: ( $($flags:tt)+ ),
        strategy: $strategy:expr,
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        validate($v_args:ident) $validate:block
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
                // Run validation first
                {
                    let $v_args = $args;
                    $validate
                }
                // Then execute
                $body
            }

            fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
                $crate::define_command!(@keys args, $keys $( ( $($keys_args)* ) )? )
            }
        }
    };
}

/// Defines a read-only command with READONLY | FAST flags.
///
/// This is a convenience wrapper around [`define_command!`] for commands that
/// only read data and are fast (O(1) or O(log N)).
///
/// # Example
///
/// ```rust,ignore
/// define_readonly_command! {
///     /// Get the value of a key.
///     pub struct GetCommand;
///     name: "GET",
///     arity: Fixed(1),
///     keys: first,
///     execute(ctx, args) {
///         Ok(ctx.store.get(&args[0])
///             .map(|v| Response::bulk(v.as_bytes()))
///             .unwrap_or(Response::Null))
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_readonly_command {
    // Basic form
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        execute($ctx:ident, $args:ident) $body:block
    ) => {
        $crate::define_command! {
            $(#[$meta])*
            pub struct $struct;
            name: $name,
            arity: $arity_kind ( $($arity_args)* ),
            flags: (READONLY | FAST),
            keys: $keys $( ( $($keys_args)* ) )?,
            execute($ctx, $args) $body
        }
    };

    // With validate block
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        validate($v_args:ident) $validate:block
        execute($ctx:ident, $args:ident) $body:block
    ) => {
        $crate::define_command! {
            $(#[$meta])*
            pub struct $struct;
            name: $name,
            arity: $arity_kind ( $($arity_args)* ),
            flags: (READONLY | FAST),
            keys: $keys $( ( $($keys_args)* ) )?,
            validate($v_args) $validate
            execute($ctx, $args) $body
        }
    };

    // With strategy
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        strategy: $strategy:expr,
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        execute($ctx:ident, $args:ident) $body:block
    ) => {
        $crate::define_command! {
            $(#[$meta])*
            pub struct $struct;
            name: $name,
            arity: $arity_kind ( $($arity_args)* ),
            flags: (READONLY | FAST),
            strategy: $strategy,
            keys: $keys $( ( $($keys_args)* ) )?,
            execute($ctx, $args) $body
        }
    };
}

/// Defines a write command with WRITE | FAST flags.
///
/// This is a convenience wrapper around [`define_command!`] for commands that
/// modify data and are fast (O(1) or O(log N)).
///
/// # Example
///
/// ```rust,ignore
/// define_write_command! {
///     /// Set the value of a key.
///     pub struct SetnxCommand;
///     name: "SETNX",
///     arity: Fixed(2),
///     keys: first,
///     execute(ctx, args) {
///         let key = args[0].clone();
///         let value = args[1].clone();
///         // ... set logic ...
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_write_command {
    // Basic form
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        execute($ctx:ident, $args:ident) $body:block
    ) => {
        $crate::define_command! {
            $(#[$meta])*
            pub struct $struct;
            name: $name,
            arity: $arity_kind ( $($arity_args)* ),
            flags: (WRITE | FAST),
            keys: $keys $( ( $($keys_args)* ) )?,
            execute($ctx, $args) $body
        }
    };

    // With validate block
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        validate($v_args:ident) $validate:block
        execute($ctx:ident, $args:ident) $body:block
    ) => {
        $crate::define_command! {
            $(#[$meta])*
            pub struct $struct;
            name: $name,
            arity: $arity_kind ( $($arity_args)* ),
            flags: (WRITE | FAST),
            keys: $keys $( ( $($keys_args)* ) )?,
            validate($v_args) $validate
            execute($ctx, $args) $body
        }
    };

    // With strategy
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        strategy: $strategy:expr,
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        execute($ctx:ident, $args:ident) $body:block
    ) => {
        $crate::define_command! {
            $(#[$meta])*
            pub struct $struct;
            name: $name,
            arity: $arity_kind ( $($arity_args)* ),
            flags: (WRITE | FAST),
            strategy: $strategy,
            keys: $keys $( ( $($keys_args)* ) )?,
            execute($ctx, $args) $body
        }
    };

    // With strategy and requires_same_slot
    (
        $(#[$meta:meta])*
        pub struct $struct:ident;
        name: $name:literal,
        arity: $arity_kind:ident ( $($arity_args:tt)* ),
        strategy: $strategy:expr,
        requires_same_slot: $same_slot:expr,
        keys: $keys:ident $( ( $($keys_args:tt)* ) )?,
        execute($ctx:ident, $args:ident) $body:block
    ) => {
        $crate::define_command! {
            $(#[$meta])*
            pub struct $struct;
            name: $name,
            arity: $arity_kind ( $($arity_args)* ),
            flags: (WRITE | FAST),
            strategy: $strategy,
            requires_same_slot: $same_slot,
            keys: $keys $( ( $($keys_args)* ) )?,
            execute($ctx, $args) $body
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

// =============================================================================
// Standalone Key Extraction Macros
// =============================================================================
//
// These macros can be used in manual `impl Command` blocks to reduce boilerplate
// for the `keys()` method. They generate the full method implementation.
//
// # Example
//
// ```rust,ignore
// impl Command for MyCommand {
//     // ... other methods ...
//
//     impl_keys_first!();  // Generates keys() that returns &args[0]
// }
// ```

/// Implements `keys()` method that returns the first argument as the key.
///
/// This is the most common pattern for single-key commands like GET, SET, ZADD, etc.
///
/// # Example
///
/// ```rust,ignore
/// impl Command for ZaddCommand {
///     fn name(&self) -> &'static str { "ZADD" }
///     fn arity(&self) -> Arity { Arity::AtLeast(3) }
///     fn flags(&self) -> CommandFlags { CommandFlags::WRITE | CommandFlags::FAST }
///     fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
///         // ... command logic ...
///     }
///     impl_keys_first!();
/// }
/// ```
#[macro_export]
macro_rules! impl_keys_first {
    () => {
        fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
            if args.is_empty() {
                vec![]
            } else {
                vec![&args[0]]
            }
        }
    };
}

/// Implements `keys()` method that returns no keys.
///
/// Used for keyless commands like PING, INFO, DEBUG, etc.
#[macro_export]
macro_rules! impl_keys_none {
    () => {
        fn keys<'a>(&self, _args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
            vec![]
        }
    };
}

/// Implements `keys()` method that returns the first two arguments as keys.
///
/// Used for commands like RENAME, RENAMENX, COPY, etc.
#[macro_export]
macro_rules! impl_keys_two {
    () => {
        fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
            if args.len() < 2 {
                vec![]
            } else {
                vec![&args[0], &args[1]]
            }
        }
    };
}

/// Implements `keys()` method that returns all arguments as keys.
///
/// Used for commands like DEL, EXISTS, TOUCH, UNLINK, etc.
#[macro_export]
macro_rules! impl_keys_all {
    () => {
        fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
            args.iter().map(|a| a.as_ref()).collect()
        }
    };
}

/// Implements `keys()` method that skips the first N arguments.
///
/// Used for commands where initial arguments are options/counts before keys.
///
/// # Example
///
/// ```rust,ignore
/// impl Command for EvalCommand {
///     // EVAL script numkeys key [key ...] arg [arg ...]
///     impl_keys_skip!(2);  // Skip script and numkeys
/// }
/// ```
#[macro_export]
macro_rules! impl_keys_skip {
    ($n:expr) => {
        fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
            if args.len() <= $n {
                vec![]
            } else {
                args[$n..].iter().map(|a| a.as_ref()).collect()
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use frogdb_protocol::Response;

    #[allow(unused_imports)]
    use crate::{
        Arity, Command, CommandContext, CommandError, CommandFlags, CommandMetadata,
        ConnectionLevelOp, ExecutionStrategy, MergeStrategy, parse_u64,
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

    // Test command with validate block
    define_command! {
        /// Command with validation.
        pub struct TestValidateCommand;
        name: "TEST_VALIDATE",
        arity: Fixed(2),
        flags: (WRITE | FAST),
        keys: first,
        validate(args) {
            let value = parse_u64(&args[1])?;
            if value == 0 {
                return Err(CommandError::InvalidArgument {
                    message: "value cannot be zero".to_string(),
                });
            }
        }
        execute(_ctx, args) {
            Ok(Response::Integer(parse_u64(&args[1]).unwrap() as i64))
        }
    }

    #[test]
    fn test_validate_block() {
        assert_eq!(TestValidateCommand.name(), "TEST_VALIDATE");
        // Note: can't easily test execute without a real context,
        // but we verify the struct was generated correctly
        assert!(matches!(TestValidateCommand.arity(), Arity::Fixed(2)));
    }

    // Test define_readonly_command macro
    define_readonly_command! {
        /// A read-only test command.
        pub struct TestReadonlyCommand;
        name: "TEST_READONLY",
        arity: Fixed(1),
        keys: first,
        execute(_ctx, args) {
            Ok(Response::bulk(args[0].clone()))
        }
    }

    #[test]
    fn test_readonly_command() {
        assert_eq!(TestReadonlyCommand.name(), "TEST_READONLY");
        assert_eq!(
            TestReadonlyCommand.flags(),
            CommandFlags::READONLY | CommandFlags::FAST
        );
    }

    // Test define_write_command macro
    define_write_command! {
        /// A write test command.
        pub struct TestWriteCommand;
        name: "TEST_WRITE",
        arity: Fixed(2),
        keys: first,
        execute(_ctx, _args) {
            Ok(Response::ok())
        }
    }

    #[test]
    fn test_write_command() {
        assert_eq!(TestWriteCommand.name(), "TEST_WRITE");
        assert_eq!(
            TestWriteCommand.flags(),
            CommandFlags::WRITE | CommandFlags::FAST
        );
    }

    // Test define_readonly_command with validate block
    define_readonly_command! {
        pub struct TestReadonlyValidateCommand;
        name: "TEST_READONLY_VALIDATE",
        arity: Fixed(2),
        keys: first,
        validate(args) {
            if args[1].is_empty() {
                return Err(CommandError::SyntaxError);
            }
        }
        execute(_ctx, args) {
            Ok(Response::bulk(args[1].clone()))
        }
    }

    #[test]
    fn test_readonly_with_validate() {
        assert_eq!(TestReadonlyValidateCommand.name(), "TEST_READONLY_VALIDATE");
        assert_eq!(
            TestReadonlyValidateCommand.flags(),
            CommandFlags::READONLY | CommandFlags::FAST
        );
    }

    // Test define_write_command with validate block
    define_write_command! {
        pub struct TestWriteValidateCommand;
        name: "TEST_WRITE_VALIDATE",
        arity: Fixed(2),
        keys: first,
        validate(args) {
            let count = parse_u64(&args[1])?;
            if count > 1000 {
                return Err(CommandError::InvalidArgument {
                    message: "count too large".to_string(),
                });
            }
        }
        execute(_ctx, _args) {
            Ok(Response::ok())
        }
    }

    #[test]
    fn test_write_with_validate() {
        assert_eq!(TestWriteValidateCommand.name(), "TEST_WRITE_VALIDATE");
        assert_eq!(
            TestWriteValidateCommand.flags(),
            CommandFlags::WRITE | CommandFlags::FAST
        );
    }
}
