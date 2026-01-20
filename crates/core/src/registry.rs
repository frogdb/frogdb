//! Command registry.

use std::collections::HashMap;
use std::sync::Arc;

use crate::command::Command;

/// Registry of all available commands.
#[derive(Default)]
pub struct CommandRegistry {
    commands: HashMap<String, Arc<dyn Command>>,
}

impl CommandRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            commands: HashMap::new(),
        }
    }

    /// Register a command.
    pub fn register(&mut self, command: impl Command + 'static) {
        let name = command.name().to_ascii_uppercase();
        self.commands.insert(name, Arc::new(command));
    }

    /// Get a command by name (case-insensitive).
    pub fn get(&self, name: &str) -> Option<Arc<dyn Command>> {
        self.commands.get(&name.to_ascii_uppercase()).cloned()
    }

    /// Get all registered command names.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.commands.keys().map(|s| s.as_str())
    }

    /// Number of registered commands.
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Check if registry is empty.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
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
    use crate::command::{Arity, CommandContext, CommandFlags};
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
}
