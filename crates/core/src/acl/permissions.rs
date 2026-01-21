//! ACL permission structures.

use std::collections::HashSet;

use super::categories::CommandCategory;

/// Type of access for a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyAccessType {
    /// Read-only access.
    Read,
    /// Write-only access.
    Write,
    /// Both read and write access.
    ReadWrite,
}

impl KeyAccessType {
    /// Check if this access type allows reading.
    pub fn allows_read(&self) -> bool {
        matches!(self, KeyAccessType::Read | KeyAccessType::ReadWrite)
    }

    /// Check if this access type allows writing.
    pub fn allows_write(&self) -> bool {
        matches!(self, KeyAccessType::Write | KeyAccessType::ReadWrite)
    }

    /// Check if this access type satisfies the required access.
    pub fn satisfies(&self, required: KeyAccessType) -> bool {
        match required {
            KeyAccessType::Read => self.allows_read(),
            KeyAccessType::Write => self.allows_write(),
            KeyAccessType::ReadWrite => self.allows_read() && self.allows_write(),
        }
    }
}

/// A key pattern with access permissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPattern {
    /// The pattern string (glob syntax).
    pub pattern: String,
    /// Type of access allowed.
    pub access_type: KeyAccessType,
}

impl KeyPattern {
    /// Create a new key pattern with full access.
    pub fn new(pattern: String) -> Self {
        Self {
            pattern,
            access_type: KeyAccessType::ReadWrite,
        }
    }

    /// Create a new key pattern with specified access type.
    pub fn with_access(pattern: String, access_type: KeyAccessType) -> Self {
        Self {
            pattern,
            access_type,
        }
    }

    /// Check if a key matches this pattern with the required access type.
    pub fn matches(&self, key: &[u8], required_access: KeyAccessType) -> bool {
        // First check if access type is compatible
        if !self.access_type.satisfies(required_access) {
            return false;
        }

        // Then check pattern match using byte-based glob
        crate::glob::glob_match(self.pattern.as_bytes(), key)
    }

    /// Convert to ACL rule string representation.
    pub fn to_rule_string(&self) -> String {
        match self.access_type {
            KeyAccessType::Read => format!("%R~{}", self.pattern),
            KeyAccessType::Write => format!("%W~{}", self.pattern),
            KeyAccessType::ReadWrite => format!("~{}", self.pattern),
        }
    }
}

/// A channel pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelPattern {
    /// The pattern string (glob syntax).
    pub pattern: String,
}

impl ChannelPattern {
    /// Create a new channel pattern.
    pub fn new(pattern: String) -> Self {
        Self { pattern }
    }

    /// Check if a channel matches this pattern.
    pub fn matches(&self, channel: &[u8]) -> bool {
        crate::glob::glob_match(self.pattern.as_bytes(), channel)
    }

    /// Convert to ACL rule string representation.
    pub fn to_rule_string(&self) -> String {
        format!("&{}", self.pattern)
    }
}

/// A subcommand-specific ACL rule (Redis 7.0+).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubcommandRule {
    /// The parent command (e.g., "CLIENT").
    pub command: String,
    /// The subcommand (e.g., "KILL").
    pub subcommand: String,
    /// Whether this rule allows or denies.
    pub allowed: bool,
}

/// Command permissions for a user.
#[derive(Debug, Clone, Default)]
pub struct CommandPermissions {
    /// Allow all commands.
    pub allow_all: bool,
    /// Explicitly allowed commands (lowercase).
    pub allowed_commands: HashSet<String>,
    /// Explicitly denied commands (lowercase).
    pub denied_commands: HashSet<String>,
    /// Allowed command categories.
    pub allowed_categories: HashSet<CommandCategory>,
    /// Denied command categories.
    pub denied_categories: HashSet<CommandCategory>,
    /// Subcommand-specific rules (Redis 7.0+, kept for forward compatibility).
    pub subcommand_rules: Vec<SubcommandRule>,
}

impl CommandPermissions {
    /// Create permissions that allow all commands.
    pub fn allow_all() -> Self {
        Self {
            allow_all: true,
            ..Default::default()
        }
    }

    /// Create permissions that deny all commands.
    pub fn deny_all() -> Self {
        Self::default()
    }

    /// Check if a command is allowed.
    pub fn is_command_allowed(&self, command: &str, _subcommand: Option<&str>) -> bool {
        let cmd_lower = command.to_lowercase();

        // Explicit deny takes precedence
        if self.denied_commands.contains(&cmd_lower) {
            return false;
        }

        // Check denied categories
        if let Some(category) = CommandCategory::for_command(&cmd_lower) {
            if self.denied_categories.contains(&category) {
                // But check if explicitly allowed
                if !self.allowed_commands.contains(&cmd_lower) {
                    return false;
                }
            }
        }

        // Allow all commands
        if self.allow_all {
            return true;
        }

        // Explicit allow
        if self.allowed_commands.contains(&cmd_lower) {
            return true;
        }

        // Check allowed categories
        if let Some(category) = CommandCategory::for_command(&cmd_lower) {
            if self.allowed_categories.contains(&category) {
                return true;
            }
        }

        false
    }

    /// Reset all command permissions.
    pub fn reset(&mut self) {
        self.allow_all = false;
        self.allowed_commands.clear();
        self.denied_commands.clear();
        self.allowed_categories.clear();
        self.denied_categories.clear();
        self.subcommand_rules.clear();
    }

    /// Add an allowed command.
    pub fn allow_command(&mut self, command: &str) {
        self.allowed_commands.insert(command.to_lowercase());
        self.denied_commands.remove(&command.to_lowercase());
    }

    /// Deny a command.
    pub fn deny_command(&mut self, command: &str) {
        self.denied_commands.insert(command.to_lowercase());
        self.allowed_commands.remove(&command.to_lowercase());
    }

    /// Allow a category.
    pub fn allow_category(&mut self, category: CommandCategory) {
        self.allowed_categories.insert(category);
        self.denied_categories.remove(&category);
    }

    /// Deny a category.
    pub fn deny_category(&mut self, category: CommandCategory) {
        self.denied_categories.insert(category);
        self.allowed_categories.remove(&category);
    }
}

/// A complete set of permissions.
#[derive(Debug, Clone, Default)]
pub struct PermissionSet {
    /// Command permissions.
    pub commands: CommandPermissions,
    /// Key patterns.
    pub key_patterns: Vec<KeyPattern>,
    /// Channel patterns.
    pub channel_patterns: Vec<ChannelPattern>,
    /// Allow all keys.
    pub all_keys: bool,
    /// Allow all channels.
    pub all_channels: bool,
}

impl PermissionSet {
    /// Create a permission set that allows everything.
    pub fn allow_all() -> Self {
        Self {
            commands: CommandPermissions::allow_all(),
            key_patterns: vec![],
            channel_patterns: vec![],
            all_keys: true,
            all_channels: true,
        }
    }

    /// Check if a key is accessible with the required access type.
    pub fn check_key_access(&self, key: &[u8], access: KeyAccessType) -> bool {
        if self.all_keys {
            return true;
        }

        for pattern in &self.key_patterns {
            if pattern.matches(key, access) {
                return true;
            }
        }

        false
    }

    /// Check if a channel is accessible.
    pub fn check_channel_access(&self, channel: &[u8]) -> bool {
        if self.all_channels {
            return true;
        }

        for pattern in &self.channel_patterns {
            if pattern.matches(channel) {
                return true;
            }
        }

        false
    }

    /// Check if a command is allowed.
    pub fn check_command(&self, command: &str, subcommand: Option<&str>) -> bool {
        self.commands.is_command_allowed(command, subcommand)
    }

    /// Reset key permissions.
    pub fn reset_keys(&mut self) {
        self.key_patterns.clear();
        self.all_keys = false;
    }

    /// Reset channel permissions.
    pub fn reset_channels(&mut self) {
        self.channel_patterns.clear();
        self.all_channels = false;
    }

    /// Add a key pattern.
    pub fn add_key_pattern(&mut self, pattern: KeyPattern) {
        self.key_patterns.push(pattern);
    }

    /// Add a channel pattern.
    pub fn add_channel_pattern(&mut self, pattern: ChannelPattern) {
        self.channel_patterns.push(pattern);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_access_type() {
        assert!(KeyAccessType::Read.allows_read());
        assert!(!KeyAccessType::Read.allows_write());
        assert!(!KeyAccessType::Write.allows_read());
        assert!(KeyAccessType::Write.allows_write());
        assert!(KeyAccessType::ReadWrite.allows_read());
        assert!(KeyAccessType::ReadWrite.allows_write());

        assert!(KeyAccessType::Read.satisfies(KeyAccessType::Read));
        assert!(!KeyAccessType::Read.satisfies(KeyAccessType::Write));
        assert!(!KeyAccessType::Read.satisfies(KeyAccessType::ReadWrite));
        assert!(KeyAccessType::ReadWrite.satisfies(KeyAccessType::Read));
        assert!(KeyAccessType::ReadWrite.satisfies(KeyAccessType::Write));
        assert!(KeyAccessType::ReadWrite.satisfies(KeyAccessType::ReadWrite));
    }

    #[test]
    fn test_key_pattern() {
        let pattern = KeyPattern::new("user:*".to_string());
        assert!(pattern.matches(b"user:123", KeyAccessType::Read));
        assert!(pattern.matches(b"user:123", KeyAccessType::Write));
        assert!(!pattern.matches(b"session:123", KeyAccessType::Read));

        let read_pattern = KeyPattern::with_access("data:*".to_string(), KeyAccessType::Read);
        assert!(read_pattern.matches(b"data:123", KeyAccessType::Read));
        assert!(!read_pattern.matches(b"data:123", KeyAccessType::Write));
    }

    #[test]
    fn test_channel_pattern() {
        let pattern = ChannelPattern::new("news:*".to_string());
        assert!(pattern.matches(b"news:sports"));
        assert!(!pattern.matches(b"chat:general"));
    }

    #[test]
    fn test_command_permissions() {
        let mut perms = CommandPermissions::default();

        // By default, nothing is allowed
        assert!(!perms.is_command_allowed("GET", None));

        // Allow GET
        perms.allow_command("GET");
        assert!(perms.is_command_allowed("GET", None));
        assert!(perms.is_command_allowed("get", None)); // Case insensitive

        // Deny SET
        perms.deny_command("SET");
        assert!(!perms.is_command_allowed("SET", None));

        // Allow all
        let all_perms = CommandPermissions::allow_all();
        assert!(all_perms.is_command_allowed("GET", None));
        assert!(all_perms.is_command_allowed("SET", None));
        assert!(all_perms.is_command_allowed("FLUSHALL", None));
    }

    #[test]
    fn test_permission_set() {
        let mut perms = PermissionSet::default();

        // No access by default
        assert!(!perms.check_key_access(b"foo", KeyAccessType::Read));
        assert!(!perms.check_channel_access(b"news"));

        // Add patterns
        perms.add_key_pattern(KeyPattern::new("user:*".to_string()));
        perms.add_channel_pattern(ChannelPattern::new("chat:*".to_string()));

        assert!(perms.check_key_access(b"user:123", KeyAccessType::Read));
        assert!(!perms.check_key_access(b"data:123", KeyAccessType::Read));
        assert!(perms.check_channel_access(b"chat:general"));
        assert!(!perms.check_channel_access(b"news:sports"));

        // All access
        let all_perms = PermissionSet::allow_all();
        assert!(all_perms.check_key_access(b"anything", KeyAccessType::ReadWrite));
        assert!(all_perms.check_channel_access(b"any:channel"));
    }
}
