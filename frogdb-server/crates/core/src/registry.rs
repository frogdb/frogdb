//! Command registry.

use std::collections::HashMap;
use std::sync::Arc;

use crate::command::{Arity, Command, CommandFlags, ExecutionStrategy};
use crate::conn_command::ConnectionCommand;

/// The single executor a registered command carries — a tagged union so that a
/// command carries *exactly one* execution path and there is no never-called
/// stub. This is the registry's answer to the old "connection-level command
/// implements a shard `execute()` that just returns a routing-bug error".
///
/// # Variants and [`ExecutionStrategy`] agreement
///
/// [`CommandImpl::validate_strategy`] enforces that the executor variant agrees
/// with the command's declared strategy, so a migrated command's never-called
/// path is *unrepresentable*:
///
/// - [`CommandImpl::Connection`] ⇒ strategy is [`ExecutionStrategy::
///   ConnectionLevel`]. A connection command has no shard `execute`, so the stub
///   simply does not exist.
/// - [`CommandImpl::Shard`] carries a shard executor. (A shard command with a
///   `ConnectionLevel` strategy is a *not-yet-migrated* connection command that
///   still routes through the legacy `router.rs`/`dispatch.rs` path and keeps
///   its stub until it is migrated behind the [`ConnectionCommand`] seam; once
///   every connection group has migrated, the reverse invariant — a
///   `ConnectionLevel` strategy must never be `Shard` — can be enforced too.)
pub enum CommandImpl {
    /// Shard-local executor: `execute(&mut CommandContext)`. Standard,
    /// ScatterGather, Blocking, and ServerWide (per-shard) commands.
    Shard(Arc<dyn Command>),
    /// Connection-level executor: `execute(&ConnCtx)`. Migrated connection
    /// commands (CONFIG; more groups follow in Phase 2).
    Connection(&'static dyn ConnectionCommand),
}

/// Retained name for the registry entry. The entry *is* its [`CommandImpl`]
/// (each variant is spec-backed, so no separate `RegistryEntry { spec, imp }`
/// wrapper is needed): [`Command::spec`] / [`ConnectionCommand::spec`] carry the
/// metadata.
pub type CommandEntry = CommandImpl;

impl CommandImpl {
    /// Get command name.
    pub fn name(&self) -> &'static str {
        match self {
            CommandImpl::Shard(cmd) => cmd.name(),
            CommandImpl::Connection(cmd) => cmd.spec().name,
        }
    }

    /// Get command arity.
    pub fn arity(&self) -> Arity {
        match self {
            CommandImpl::Shard(cmd) => cmd.arity(),
            CommandImpl::Connection(cmd) => cmd.spec().arity,
        }
    }

    /// Get command flags.
    pub fn flags(&self) -> CommandFlags {
        match self {
            CommandImpl::Shard(cmd) => cmd.flags(),
            CommandImpl::Connection(cmd) => cmd.spec().flags,
        }
    }

    /// Get execution strategy.
    pub fn execution_strategy(&self) -> ExecutionStrategy {
        match self {
            CommandImpl::Shard(cmd) => cmd.execution_strategy(),
            CommandImpl::Connection(cmd) => cmd.spec().strategy.clone(),
        }
    }

    /// Get keys from arguments.
    pub fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
        match self {
            CommandImpl::Shard(cmd) => cmd.keys(args),
            // Connection commands extract keys through their `dynamic_keys` hook
            // (default = `spec().keys.extract`): keyless/static-key commands use
            // the spec; the keyed scripting commands (EVAL/EVALSHA/FCALL, whose
            // keys are located by a runtime `numkeys`) override it.
            CommandImpl::Connection(cmd) => cmd.dynamic_keys(args),
        }
    }

    /// Get keys with per-key access flags from arguments.
    pub fn keys_with_flags<'a>(
        &self,
        args: &'a [bytes::Bytes],
    ) -> Vec<(&'a [u8], Vec<crate::command::KeyAccessFlag>)> {
        match self {
            CommandImpl::Shard(cmd) => cmd.keys_with_flags(args),
            CommandImpl::Connection(cmd) => {
                let spec = cmd.spec();
                spec.access.resolve(self.keys(args), spec.is_write())
            }
        }
    }

    /// Check if this is a shard command (has a shard-local `execute()`).
    pub fn is_full(&self) -> bool {
        matches!(self, CommandImpl::Shard(_))
    }

    /// Get as shard command if available.
    pub fn as_command(&self) -> Option<&Arc<dyn Command>> {
        match self {
            CommandImpl::Shard(cmd) => Some(cmd),
            CommandImpl::Connection(_) => None,
        }
    }

    /// Get as connection-level command if this entry carries one.
    pub fn as_connection(&self) -> Option<&'static dyn ConnectionCommand> {
        match self {
            CommandImpl::Connection(cmd) => Some(*cmd),
            CommandImpl::Shard(_) => None,
        }
    }

    /// Enforce that the executor variant agrees with the declared strategy,
    /// making a migrated command's never-called path unrepresentable.
    ///
    /// A [`CommandImpl::Connection`] executor *must* declare an
    /// [`ExecutionStrategy::ConnectionLevel`] strategy: a connection command
    /// that claimed, say, `Standard` would never be reached by connection-level
    /// dispatch. The `Shard` variant is unconstrained here during the transition
    /// (see the type docs).
    pub fn validate_strategy(&self) -> Result<(), String> {
        match self {
            CommandImpl::Connection(cmd) => {
                let spec = cmd.spec();
                if matches!(spec.strategy, ExecutionStrategy::ConnectionLevel(_)) {
                    Ok(())
                } else {
                    Err(format!(
                        "{}: Connection executor requires a ConnectionLevel strategy, found {:?}",
                        spec.name, spec.strategy
                    ))
                }
            }
            CommandImpl::Shard(_) => Ok(()),
        }
    }
}

/// Registry of all available commands.
#[derive(Default)]
pub struct CommandRegistry {
    commands: HashMap<String, Arc<dyn Command>>,
    /// Combined registry supporting both shard commands and connection-level
    /// (introspection-visible, no shard executor) entries.
    entries: HashMap<String, CommandEntry>,
}

impl CommandRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            commands: HashMap::new(),
            entries: HashMap::new(),
        }
    }

    /// Register a command.
    pub fn register(&mut self, command: impl Command + 'static) {
        // In debug builds, assert the command's spec is internally consistent
        // at registration time (the spec is the single source of truth, so an
        // invalid one is a programming error caught here rather than at runtime).
        debug_assert!(
            command.spec().validate().is_ok(),
            "{}: invalid CommandSpec: {:?}",
            command.spec().name,
            command.spec().validate()
        );
        let name = command.name().to_ascii_uppercase();
        let arc_cmd = Arc::new(command);
        self.commands.insert(name.clone(), arc_cmd.clone());
        self.entries
            .insert(name, CommandImpl::Shard(arc_cmd as Arc<dyn Command>));
    }

    /// Register a connection-level command executed against a [`ConnCtx`].
    ///
    /// The command carries its own [`CommandSpec`], validated (in debug builds)
    /// for internal consistency *and* for `strategy` ↔ variant agreement — a
    /// connection executor must declare an [`ExecutionStrategy::ConnectionLevel`]
    /// strategy — so the never-called shard stub is unrepresentable.
    ///
    /// [`ConnCtx`]: crate::conn_command::ConnCtx
    pub fn register_connection(&mut self, command: &'static dyn ConnectionCommand) {
        let entry = CommandImpl::Connection(command);
        debug_assert!(
            command.spec().validate().is_ok(),
            "{}: invalid CommandSpec: {:?}",
            command.spec().name,
            command.spec().validate()
        );
        debug_assert!(
            entry.validate_strategy().is_ok(),
            "{}",
            entry.validate_strategy().unwrap_err()
        );
        let name = command.spec().name.to_ascii_uppercase();
        self.entries.insert(name, entry);
    }

    /// Get a command by name (case-insensitive).
    pub fn get(&self, name: &str) -> Option<Arc<dyn Command>> {
        self.commands.get(&name.to_ascii_uppercase()).cloned()
    }

    /// Get a command entry by name (case-insensitive).
    /// This returns both shard commands and connection-level entries.
    pub fn get_entry(&self, name: &str) -> Option<&CommandEntry> {
        self.entries.get(&name.to_ascii_uppercase())
    }

    /// Get all registered command names (includes connection-level commands,
    /// which have no shard executor).
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(|s| s.as_str())
    }

    /// Get all registered shard command names (excludes connection-level
    /// commands, which are absent from `commands`).
    pub fn command_names(&self) -> impl Iterator<Item = &str> {
        self.commands.keys().map(|s| s.as_str())
    }

    /// Number of registered commands (includes connection-level commands).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if registry is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterator over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &CommandEntry)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }
}

impl std::fmt::Debug for CommandRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandRegistry")
            .field("commands", &self.commands.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{CommandContext, ConnectionLevelOp};
    use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};
    use crate::conn_command::{BoxFuture, ConnCtx, ConnectionCommand};
    use crate::error::CommandError;
    use bytes::Bytes;
    use frogdb_protocol::Response;

    struct TestCommand;

    impl Command for TestCommand {
        fn spec(&self) -> &'static crate::command_spec::CommandSpec {
            use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};
            static SPEC: CommandSpec = CommandSpec {
                name: "TEST",
                arity: Arity::Fixed(0),
                flags: CommandFlags::READONLY.union(CommandFlags::FAST),
                keys: KeySpec::None,
                access: AccessSpec::Uniform,
                wal: crate::command::WalStrategy::NoOp,
                wakes: crate::command::WaiterWake::None,
                event: EventSpec::NotApplicable,
                requires_same_slot: false,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
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

    #[test]
    fn test_register_and_get() {
        let mut registry = CommandRegistry::new();
        registry.register(TestCommand);

        assert!(registry.get("TEST").is_some());
        assert!(registry.get("test").is_some()); // case-insensitive
        assert!(registry.get("MISSING").is_none());
    }

    #[test]
    fn test_names() {
        let mut registry = CommandRegistry::new();
        registry.register(TestCommand);

        let names: Vec<_> = registry.names().collect();
        assert!(names.contains(&"TEST"));
    }

    #[test]
    fn test_get_entry() {
        let mut registry = CommandRegistry::new();
        registry.register(TestCommand);

        let entry = registry.get_entry("TEST").unwrap();
        assert!(entry.is_full());
        assert_eq!(entry.name(), "TEST");
        assert_eq!(entry.execution_strategy(), ExecutionStrategy::Standard);
    }

    /// A connection command whose declared strategy is a parameter, so we can
    /// build both a well-formed entry (`ConnectionLevel`) and a mismatched one.
    struct TestConnCommand {
        spec: &'static CommandSpec,
    }

    impl ConnectionCommand for TestConnCommand {
        fn spec(&self) -> &'static CommandSpec {
            self.spec
        }
        fn execute<'a>(
            &'a self,
            _ctx: &'a mut ConnCtx<'a>,
            _args: &'a [Bytes],
        ) -> BoxFuture<'a, Response> {
            Box::pin(async { Response::ok() })
        }
    }

    const fn conn_spec(strategy: ExecutionStrategy) -> CommandSpec {
        CommandSpec {
            name: "TESTCONN",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::ADMIN,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: crate::command::WalStrategy::NoOp,
            wakes: crate::command::WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy,
            mutation: crate::command::ConnMutation::None,
        }
    }

    #[test]
    fn connection_registration_dispatches_through_union() {
        static SPEC: CommandSpec =
            conn_spec(ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin));
        static CMD: TestConnCommand = TestConnCommand { spec: &SPEC };

        let mut registry = CommandRegistry::new();
        registry.register_connection(&CMD);

        // Metadata (including flags) is readable through the entry, but there
        // is no shard executor. This is the in-`get_entry()`/not-in-`get()`
        // introspection contract the removed `MetadataOnly` variant carried.
        let entry = registry.get_entry("TESTCONN").unwrap();
        assert!(!entry.is_full());
        assert!(entry.as_command().is_none());
        assert!(entry.as_connection().is_some());
        assert_eq!(entry.name(), "TESTCONN");
        assert_eq!(
            entry.execution_strategy(),
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
        );
        assert!(entry.flags().contains(CommandFlags::ADMIN));
        // Not reachable via get() (no shard execute()).
        assert!(registry.get("TESTCONN").is_none());
        // But it is visible to names() and absent from command_names().
        let names: Vec<_> = registry.names().collect();
        assert!(names.contains(&"TESTCONN"));
        let cmd_names: Vec<_> = registry.command_names().collect();
        assert!(!cmd_names.contains(&"TESTCONN"));
    }

    #[test]
    fn validate_strategy_accepts_connection_level() {
        static SPEC: CommandSpec =
            conn_spec(ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin));
        static CMD: TestConnCommand = TestConnCommand { spec: &SPEC };
        let entry = CommandImpl::Connection(&CMD);
        assert!(entry.validate_strategy().is_ok());
    }

    #[test]
    fn validate_strategy_rejects_mismatched_connection_entry() {
        // A Connection executor that (wrongly) declares a shard strategy is a
        // mismatch: connection-level dispatch would never reach it.
        static SPEC: CommandSpec = conn_spec(ExecutionStrategy::Standard);
        static CMD: TestConnCommand = TestConnCommand { spec: &SPEC };
        let entry = CommandImpl::Connection(&CMD);
        let err = entry.validate_strategy().unwrap_err();
        assert!(err.contains("ConnectionLevel"), "unexpected error: {err}");
    }

    #[test]
    fn test_mixed_registration() {
        static SPEC: CommandSpec =
            conn_spec(ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin));
        static CMD: TestConnCommand = TestConnCommand { spec: &SPEC };

        let mut registry = CommandRegistry::new();
        registry.register(TestCommand);
        registry.register_connection(&CMD);

        // Both should be in names()
        let names: Vec<_> = registry.names().collect();
        assert!(names.contains(&"TEST"));
        assert!(names.contains(&"TESTCONN"));

        // Only the shard command in command_names(); the connection-level entry
        // is in names()/get_entry() but not get()/command_names().
        let cmd_names: Vec<_> = registry.command_names().collect();
        assert!(cmd_names.contains(&"TEST"));
        assert!(!cmd_names.contains(&"TESTCONN"));
        assert!(registry.get("TESTCONN").is_none());

        assert_eq!(registry.len(), 2);
    }
}
