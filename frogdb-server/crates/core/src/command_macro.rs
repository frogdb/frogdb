//! Command definition macros.
//!
//! Provides the [`define_metadata_command!`] macro for declaring metadata-only
//! commands (handled at the connection level, e.g. pub/sub and transaction
//! commands) that implement [`CommandMetadata`](crate::CommandMetadata) for
//! `COMMAND INFO` introspection without a full `execute()`.
//!
//! Full commands declare a single [`CommandSpec`](crate::CommandSpec) and
//! implement `execute()` directly — there is no code-generation macro for them;
//! the declarative spec is the single source of truth.

/// Internal helper arms shared by [`define_metadata_command!`].
///
/// Only the `@arity`, `@flags`, and `@keys` fragment matchers are defined here;
/// there is intentionally no command-generating form. New full commands declare
/// a [`CommandSpec`](crate::CommandSpec) and implement `execute()` by hand.
#[macro_export]
macro_rules! define_command {
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
    // Key extraction helpers (for metadata-only commands)
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

    #[allow(unused_imports)]
    use crate::{Arity, CommandFlags, CommandMetadata, ConnectionLevelOp, ExecutionStrategy};

    // Test metadata command generation via the declarative macro.
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

    // Metadata-only commands derive a uniform key-access flag from CommandFlags.
    #[test]
    fn test_metadata_keys_with_flags() {
        let args = vec![Bytes::from_static(b"channel1")];
        let kwf = TestSubscribeMetadata.keys_with_flags(&args);
        assert_eq!(kwf.len(), 1);
        assert_eq!(kwf[0].0, b"channel1");
    }
}
