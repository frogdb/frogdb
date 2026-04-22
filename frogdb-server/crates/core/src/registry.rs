//! Command registry.

use std::collections::HashMap;
use std::sync::Arc;

use crate::command::{Arity, Command, CommandFlags, CommandMetadata, ExecutionStrategy};

/// Unified entry for both full commands and metadata-only commands.
///
/// This allows the registry to store commands that implement full execution
/// alongside commands that only provide metadata (like pub/sub commands that
/// are handled at the connection level).
pub enum CommandEntry {
    /// Full command with execute() implementation.
    Full(Arc<dyn Command>),
    /// Metadata-only command (execution handled elsewhere).
    MetadataOnly(Arc<dyn CommandMetadata>),
}

impl CommandEntry {
    /// Get command name.
    pub fn name(&self) -> &'static str {
        match self {
            CommandEntry::Full(cmd) => cmd.name(),
            CommandEntry::MetadataOnly(meta) => meta.name(),
        }
    }

    /// Get command arity.
    pub fn arity(&self) -> Arity {
        match self {
            CommandEntry::Full(cmd) => cmd.arity(),
            CommandEntry::MetadataOnly(meta) => meta.arity(),
        }
    }

    /// Get command flags.
    pub fn flags(&self) -> CommandFlags {
        match self {
            CommandEntry::Full(cmd) => cmd.flags(),
            CommandEntry::MetadataOnly(meta) => meta.flags(),
        }
    }

    /// Get execution strategy.
    pub fn execution_strategy(&self) -> ExecutionStrategy {
        match self {
            CommandEntry::Full(cmd) => cmd.execution_strategy(),
            CommandEntry::MetadataOnly(meta) => meta.execution_strategy(),
        }
    }

    /// Get keys from arguments.
    pub fn keys<'a>(&self, args: &'a [bytes::Bytes]) -> Vec<&'a [u8]> {
        match self {
            CommandEntry::Full(cmd) => cmd.keys(args),
            CommandEntry::MetadataOnly(meta) => meta.keys(args),
        }
    }

    /// Get keys with per-key access flags from arguments.
    pub fn keys_with_flags<'a>(
        &self,
        args: &'a [bytes::Bytes],
    ) -> Vec<(&'a [u8], Vec<crate::command::KeyAccessFlag>)> {
        match self {
            CommandEntry::Full(cmd) => cmd.keys_with_flags(args),
            CommandEntry::MetadataOnly(meta) => meta.keys_with_flags(args),
        }
    }

    /// Check if this is a full command (has execute()).
    pub fn is_full(&self) -> bool {
        matches!(self, CommandEntry::Full(_))
    }

    /// Get as full command if available.
    pub fn as_command(&self) -> Option<&Arc<dyn Command>> {
        match self {
            CommandEntry::Full(cmd) => Some(cmd),
            CommandEntry::MetadataOnly(_) => None,
        }
    }
}

/// Registry of all available commands.
#[derive(Default)]
pub struct CommandRegistry {
    commands: HashMap<String, Arc<dyn Command>>,
    /// Combined registry supporting both full commands and metadata-only entries.
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
        let name = command.name().to_ascii_uppercase();
        let arc_cmd = Arc::new(command);
        self.commands.insert(name.clone(), arc_cmd.clone());
        self.entries
            .insert(name, CommandEntry::Full(arc_cmd as Arc<dyn Command>));
    }

    /// Register a metadata-only command (for commands handled at connection level).
    pub fn register_metadata(&mut self, metadata: impl CommandMetadata + 'static) {
        let name = metadata.name().to_ascii_uppercase();
        self.entries
            .insert(name, CommandEntry::MetadataOnly(Arc::new(metadata)));
    }

    /// Get a command by name (case-insensitive).
    pub fn get(&self, name: &str) -> Option<Arc<dyn Command>> {
        self.commands.get(&name.to_ascii_uppercase()).cloned()
    }

    /// Get a command entry by name (case-insensitive).
    /// This returns both full commands and metadata-only entries.
    pub fn get_entry(&self, name: &str) -> Option<&CommandEntry> {
        self.entries.get(&name.to_ascii_uppercase())
    }

    /// Get all registered command names (includes metadata-only commands).
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(|s| s.as_str())
    }

    /// Get all registered full command names (excludes metadata-only).
    pub fn command_names(&self) -> impl Iterator<Item = &str> {
        self.commands.keys().map(|s| s.as_str())
    }

    /// Number of registered commands (includes metadata-only).
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
    use crate::error::CommandError;
    use bytes::Bytes;
    use frogdb_protocol::Response;

    struct TestCommand;

    impl Command for TestCommand {
        fn name(&self) -> &'static str {
            "TEST"
        }

        fn arity(&self) -> Arity {
            Arity::Fixed(0)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::FAST
        }

        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, CommandError> {
            Ok(Response::ok())
        }

        fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
            vec![]
        }
    }

    struct TestMetadataOnly;

    impl CommandMetadata for TestMetadataOnly {
        fn name(&self) -> &'static str {
            "TESTMETA"
        }

        fn arity(&self) -> Arity {
            Arity::AtLeast(1)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::PUBSUB
        }

        fn execution_strategy(&self) -> ExecutionStrategy {
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
        }

        fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
            vec![]
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

    #[test]
    fn test_metadata_only_registration() {
        let mut registry = CommandRegistry::new();
        registry.register_metadata(TestMetadataOnly);

        // Should not be accessible via get() (no execute method)
        assert!(registry.get("TESTMETA").is_none());

        // Should be accessible via get_entry()
        let entry = registry.get_entry("TESTMETA").unwrap();
        assert!(!entry.is_full());
        assert_eq!(entry.name(), "TESTMETA");
        assert_eq!(
            entry.execution_strategy(),
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
        );
        assert!(entry.flags().contains(CommandFlags::PUBSUB));
    }

    #[test]
    fn test_mixed_registration() {
        let mut registry = CommandRegistry::new();
        registry.register(TestCommand);
        registry.register_metadata(TestMetadataOnly);

        // Both should be in names()
        let names: Vec<_> = registry.names().collect();
        assert!(names.contains(&"TEST"));
        assert!(names.contains(&"TESTMETA"));

        // Only full command in command_names()
        let cmd_names: Vec<_> = registry.command_names().collect();
        assert!(cmd_names.contains(&"TEST"));
        assert!(!cmd_names.contains(&"TESTMETA"));

        assert_eq!(registry.len(), 2);
    }
}
