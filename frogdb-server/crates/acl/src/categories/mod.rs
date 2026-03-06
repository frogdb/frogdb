//! Command categories for ACL.
//!
//! Redis command categories as defined in the ACL system.

mod data;

use std::str::FromStr;

use data::{CATEGORY_COMMANDS, COMMAND_ALL_CATEGORIES, COMMAND_CATEGORIES};

/// Command categories for ACL permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandCategory {
    /// Administrative commands (SHUTDOWN, CONFIG, etc.)
    Admin,
    /// Bitmap commands (SETBIT, GETBIT, etc.)
    Bitmap,
    /// Blocking commands (BLPOP, BRPOP, etc.)
    Blocking,
    /// Connection commands (AUTH, PING, etc.)
    Connection,
    /// Dangerous commands (FLUSHALL, DEBUG, etc.)
    Dangerous,
    /// Fast O(1) commands
    Fast,
    /// Geo commands (GEOADD, GEODIST, etc.)
    Geo,
    /// Hash commands (HSET, HGET, etc.)
    Hash,
    /// HyperLogLog commands (PFADD, PFCOUNT, etc.)
    Hyperloglog,
    /// Keyspace commands (DEL, RENAME, etc.)
    Keyspace,
    /// List commands (LPUSH, RPOP, etc.)
    List,
    /// Pub/Sub commands
    Pubsub,
    /// Read commands
    Read,
    /// Scripting commands (EVAL, SCRIPT, etc.)
    Scripting,
    /// Set commands (SADD, SMEMBERS, etc.)
    Set,
    /// Slow commands
    Slow,
    /// Sorted set commands (ZADD, ZRANGE, etc.)
    Sortedset,
    /// Stream commands (XADD, XREAD, etc.)
    Stream,
    /// String commands (GET, SET, etc.)
    String,
    /// Transaction commands (MULTI, EXEC, etc.)
    Transaction,
    /// Write commands
    Write,
}

impl CommandCategory {
    /// Get all categories.
    pub fn all() -> &'static [CommandCategory] {
        &[
            CommandCategory::Admin,
            CommandCategory::Bitmap,
            CommandCategory::Blocking,
            CommandCategory::Connection,
            CommandCategory::Dangerous,
            CommandCategory::Fast,
            CommandCategory::Geo,
            CommandCategory::Hash,
            CommandCategory::Hyperloglog,
            CommandCategory::Keyspace,
            CommandCategory::List,
            CommandCategory::Pubsub,
            CommandCategory::Read,
            CommandCategory::Scripting,
            CommandCategory::Set,
            CommandCategory::Slow,
            CommandCategory::Sortedset,
            CommandCategory::Stream,
            CommandCategory::String,
            CommandCategory::Transaction,
            CommandCategory::Write,
        ]
    }

    /// Get the string name of this category.
    pub fn name(&self) -> &'static str {
        match self {
            CommandCategory::Admin => "admin",
            CommandCategory::Bitmap => "bitmap",
            CommandCategory::Blocking => "blocking",
            CommandCategory::Connection => "connection",
            CommandCategory::Dangerous => "dangerous",
            CommandCategory::Fast => "fast",
            CommandCategory::Geo => "geo",
            CommandCategory::Hash => "hash",
            CommandCategory::Hyperloglog => "hyperloglog",
            CommandCategory::Keyspace => "keyspace",
            CommandCategory::List => "list",
            CommandCategory::Pubsub => "pubsub",
            CommandCategory::Read => "read",
            CommandCategory::Scripting => "scripting",
            CommandCategory::Set => "set",
            CommandCategory::Slow => "slow",
            CommandCategory::Sortedset => "sortedset",
            CommandCategory::Stream => "stream",
            CommandCategory::String => "string",
            CommandCategory::Transaction => "transaction",
            CommandCategory::Write => "write",
        }
    }

    /// Parse a category from string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "admin" => Some(CommandCategory::Admin),
            "bitmap" => Some(CommandCategory::Bitmap),
            "blocking" => Some(CommandCategory::Blocking),
            "connection" => Some(CommandCategory::Connection),
            "dangerous" => Some(CommandCategory::Dangerous),
            "fast" => Some(CommandCategory::Fast),
            "geo" => Some(CommandCategory::Geo),
            "hash" => Some(CommandCategory::Hash),
            "hyperloglog" => Some(CommandCategory::Hyperloglog),
            "keyspace" => Some(CommandCategory::Keyspace),
            "list" => Some(CommandCategory::List),
            "pubsub" => Some(CommandCategory::Pubsub),
            "read" => Some(CommandCategory::Read),
            "scripting" => Some(CommandCategory::Scripting),
            "set" => Some(CommandCategory::Set),
            "slow" => Some(CommandCategory::Slow),
            "sortedset" => Some(CommandCategory::Sortedset),
            "stream" => Some(CommandCategory::Stream),
            "string" => Some(CommandCategory::String),
            "transaction" => Some(CommandCategory::Transaction),
            "write" => Some(CommandCategory::Write),
            _ => None,
        }
    }

    /// Get the primary category for a command.
    pub fn for_command(cmd: &str) -> Option<Self> {
        COMMAND_CATEGORIES
            .get(&cmd.to_lowercase().as_str())
            .copied()
    }

    /// Get all categories for a command.
    pub fn all_for_command(cmd: &str) -> Vec<Self> {
        COMMAND_ALL_CATEGORIES
            .get(&cmd.to_lowercase().as_str())
            .cloned()
            .unwrap_or_default()
    }

    /// Get all commands in this category.
    pub fn commands(&self) -> Vec<&'static str> {
        CATEGORY_COMMANDS.get(self).cloned().unwrap_or_default()
    }
}

impl FromStr for CommandCategory {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CommandCategory::parse(s).ok_or(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_category_names() {
        assert_eq!(CommandCategory::Admin.name(), "admin");
        assert_eq!(CommandCategory::String.name(), "string");
        assert_eq!(CommandCategory::Write.name(), "write");
    }

    #[test]
    fn test_category_from_str() {
        assert_eq!(
            CommandCategory::parse("admin"),
            Some(CommandCategory::Admin)
        );
        assert_eq!(
            CommandCategory::parse("WRITE"),
            Some(CommandCategory::Write)
        );
        assert_eq!(CommandCategory::parse("invalid"), None);
        // Test FromStr trait
        assert_eq!(
            "admin".parse::<CommandCategory>(),
            Ok(CommandCategory::Admin)
        );
        assert!("invalid".parse::<CommandCategory>().is_err());
    }

    #[test]
    fn test_command_category() {
        assert_eq!(
            CommandCategory::for_command("GET"),
            Some(CommandCategory::String)
        );
        assert_eq!(
            CommandCategory::for_command("lpush"),
            Some(CommandCategory::List)
        );
        assert_eq!(
            CommandCategory::for_command("ZADD"),
            Some(CommandCategory::Sortedset)
        );
        assert_eq!(
            CommandCategory::for_command("AUTH"),
            Some(CommandCategory::Connection)
        );
    }

    #[test]
    fn test_all_categories_for_command() {
        let cats = CommandCategory::all_for_command("GET");
        assert!(cats.contains(&CommandCategory::String));
        assert!(cats.contains(&CommandCategory::Read));
        assert!(cats.contains(&CommandCategory::Fast));

        let cats = CommandCategory::all_for_command("SET");
        assert!(cats.contains(&CommandCategory::String));
        assert!(cats.contains(&CommandCategory::Write));
    }

    #[test]
    fn test_category_commands() {
        let cmds = CommandCategory::String.commands();
        assert!(cmds.contains(&"get"));
        assert!(cmds.contains(&"set"));

        let cmds = CommandCategory::List.commands();
        assert!(cmds.contains(&"lpush"));
        assert!(cmds.contains(&"rpop"));
    }

    #[test]
    fn test_all_categories() {
        let all = CommandCategory::all();
        assert!(all.contains(&CommandCategory::Admin));
        assert!(all.contains(&CommandCategory::String));
        assert!(all.contains(&CommandCategory::Write));
        assert!(all.contains(&CommandCategory::Read));
        assert_eq!(all.len(), 21);
    }
}
