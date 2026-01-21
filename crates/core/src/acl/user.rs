//! ACL user structures.

use std::collections::HashSet;
use std::sync::Arc;

use super::permissions::PermissionSet;

/// A user in the ACL system.
#[derive(Debug, Clone)]
pub struct User {
    /// Username.
    pub name: String,
    /// Whether the user is enabled.
    pub enabled: bool,
    /// SHA256 hashes of passwords (32 bytes each).
    pub password_hashes: HashSet<[u8; 32]>,
    /// Whether the user requires no password (can authenticate with any password).
    pub nopass: bool,
    /// Root permissions for the user.
    pub root_permissions: PermissionSet,
    /// Selectors for additional permission sets (Redis 7.0+).
    /// Empty for now, kept for forward compatibility.
    pub selectors: Vec<PermissionSet>,
}

impl User {
    /// Create a new user with default settings.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            enabled: false,
            password_hashes: HashSet::new(),
            nopass: false,
            root_permissions: PermissionSet::default(),
            selectors: Vec::new(),
        }
    }

    /// Create the default user (enabled, nopass, all permissions).
    pub fn default_user() -> Self {
        Self {
            name: "default".to_string(),
            enabled: true,
            password_hashes: HashSet::new(),
            nopass: true,
            root_permissions: PermissionSet::allow_all(),
            selectors: Vec::new(),
        }
    }

    /// Create a default user with a password (requirepass mode).
    pub fn default_with_password(password_hash: [u8; 32]) -> Self {
        let mut passwords = HashSet::new();
        passwords.insert(password_hash);

        Self {
            name: "default".to_string(),
            enabled: true,
            password_hashes: passwords,
            nopass: false,
            root_permissions: PermissionSet::allow_all(),
            selectors: Vec::new(),
        }
    }

    /// Check if the user can authenticate with the given password hash.
    pub fn verify_password(&self, password_hash: &[u8; 32]) -> bool {
        if self.nopass {
            return true;
        }
        self.password_hashes.contains(password_hash)
    }

    /// Add a password hash.
    pub fn add_password(&mut self, hash: [u8; 32]) {
        self.nopass = false;
        self.password_hashes.insert(hash);
    }

    /// Remove a password hash.
    pub fn remove_password(&mut self, hash: &[u8; 32]) {
        self.password_hashes.remove(hash);
    }

    /// Reset all passwords.
    pub fn reset_passwords(&mut self) {
        self.password_hashes.clear();
        self.nopass = false;
    }

    /// Set nopass mode.
    pub fn set_nopass(&mut self) {
        self.nopass = true;
        self.password_hashes.clear();
    }

    /// Reset the user to default state (off, no passwords, no permissions).
    pub fn reset(&mut self) {
        self.enabled = false;
        self.password_hashes.clear();
        self.nopass = false;
        self.root_permissions = PermissionSet::default();
        self.selectors.clear();
    }

    /// Check if a command is allowed.
    pub fn check_command(&self, command: &str, subcommand: Option<&str>) -> bool {
        self.root_permissions.check_command(command, subcommand)
    }

    /// Check if key access is allowed.
    pub fn check_key_access(&self, key: &[u8], access: super::permissions::KeyAccessType) -> bool {
        self.root_permissions.check_key_access(key, access)
    }

    /// Check if channel access is allowed.
    pub fn check_channel_access(&self, channel: &[u8]) -> bool {
        self.root_permissions.check_channel_access(channel)
    }

    /// Convert to ACL LIST format string.
    pub fn to_acl_string(&self) -> String {
        let mut parts = vec![format!("user {}", self.name)];

        // On/off
        if self.enabled {
            parts.push("on".to_string());
        } else {
            parts.push("off".to_string());
        }

        // Passwords
        if self.nopass {
            parts.push("nopass".to_string());
        } else {
            for hash in &self.password_hashes {
                parts.push(format!("#{}", hex::encode(hash)));
            }
        }

        // Key patterns
        if self.root_permissions.all_keys {
            parts.push("~*".to_string());
        } else {
            for pattern in &self.root_permissions.key_patterns {
                parts.push(pattern.to_rule_string());
            }
        }

        // Channel patterns
        if self.root_permissions.all_channels {
            parts.push("&*".to_string());
        } else {
            for pattern in &self.root_permissions.channel_patterns {
                parts.push(pattern.to_rule_string());
            }
        }

        // Commands
        if self.root_permissions.commands.allow_all {
            parts.push("+@all".to_string());
        } else {
            for category in &self.root_permissions.commands.allowed_categories {
                parts.push(format!("+@{}", category.name()));
            }
            for cmd in &self.root_permissions.commands.allowed_commands {
                parts.push(format!("+{}", cmd));
            }
        }

        for category in &self.root_permissions.commands.denied_categories {
            parts.push(format!("-@{}", category.name()));
        }
        for cmd in &self.root_permissions.commands.denied_commands {
            parts.push(format!("-{}", cmd));
        }

        parts.join(" ")
    }

    /// Convert to ACL GETUSER response format.
    pub fn to_getuser_info(&self) -> Vec<(&'static str, UserInfoValue)> {
        let mut info = Vec::new();

        // flags
        let mut flags = Vec::new();
        if self.enabled {
            flags.push("on".to_string());
        } else {
            flags.push("off".to_string());
        }
        if self.nopass {
            flags.push("nopass".to_string());
        }
        if self.root_permissions.all_keys {
            flags.push("allkeys".to_string());
        }
        if self.root_permissions.all_channels {
            flags.push("allchannels".to_string());
        }
        if self.root_permissions.commands.allow_all {
            flags.push("allcommands".to_string());
        }
        info.push(("flags", UserInfoValue::StringArray(flags)));

        // passwords (hashes)
        let passwords: Vec<String> = self.password_hashes
            .iter()
            .map(hex::encode)
            .collect();
        info.push(("passwords", UserInfoValue::StringArray(passwords)));

        // commands
        let mut commands = String::new();
        if self.root_permissions.commands.allow_all {
            commands.push_str("+@all");
        }
        for cat in &self.root_permissions.commands.allowed_categories {
            if !commands.is_empty() {
                commands.push(' ');
            }
            commands.push_str(&format!("+@{}", cat.name()));
        }
        for cmd in &self.root_permissions.commands.allowed_commands {
            if !commands.is_empty() {
                commands.push(' ');
            }
            commands.push_str(&format!("+{}", cmd));
        }
        for cat in &self.root_permissions.commands.denied_categories {
            if !commands.is_empty() {
                commands.push(' ');
            }
            commands.push_str(&format!("-@{}", cat.name()));
        }
        for cmd in &self.root_permissions.commands.denied_commands {
            if !commands.is_empty() {
                commands.push(' ');
            }
            commands.push_str(&format!("-{}", cmd));
        }
        info.push(("commands", UserInfoValue::String(commands)));

        // keys
        let mut keys = Vec::new();
        if self.root_permissions.all_keys {
            keys.push("~*".to_string());
        } else {
            for pattern in &self.root_permissions.key_patterns {
                keys.push(pattern.to_rule_string());
            }
        }
        info.push(("keys", UserInfoValue::StringArray(keys)));

        // channels
        let mut channels = Vec::new();
        if self.root_permissions.all_channels {
            channels.push("&*".to_string());
        } else {
            for pattern in &self.root_permissions.channel_patterns {
                channels.push(pattern.to_rule_string());
            }
        }
        info.push(("channels", UserInfoValue::StringArray(channels)));

        // selectors (empty for now)
        info.push(("selectors", UserInfoValue::StringArray(vec![])));

        info
    }
}

/// Value type for user info response.
#[derive(Debug, Clone)]
pub enum UserInfoValue {
    String(String),
    StringArray(Vec<String>),
}

/// Permissions snapshot for an authenticated user.
/// This is an immutable snapshot taken at authentication time.
#[derive(Debug, Clone)]
pub struct UserPermissions {
    /// Whether all commands are allowed.
    pub allow_all_commands: bool,
    /// Allowed commands (lowercase).
    pub allowed_commands: HashSet<String>,
    /// Denied commands (lowercase).
    pub denied_commands: HashSet<String>,
    /// Allowed categories.
    pub allowed_categories: HashSet<super::categories::CommandCategory>,
    /// Denied categories.
    pub denied_categories: HashSet<super::categories::CommandCategory>,
    /// Key patterns.
    pub key_patterns: Vec<super::permissions::KeyPattern>,
    /// Whether all keys are allowed.
    pub all_keys: bool,
    /// Channel patterns.
    pub channel_patterns: Vec<super::permissions::ChannelPattern>,
    /// Whether all channels are allowed.
    pub all_channels: bool,
}

impl UserPermissions {
    /// Create from a User.
    pub fn from_user(user: &User) -> Self {
        Self {
            allow_all_commands: user.root_permissions.commands.allow_all,
            allowed_commands: user.root_permissions.commands.allowed_commands.clone(),
            denied_commands: user.root_permissions.commands.denied_commands.clone(),
            allowed_categories: user.root_permissions.commands.allowed_categories.clone(),
            denied_categories: user.root_permissions.commands.denied_categories.clone(),
            key_patterns: user.root_permissions.key_patterns.clone(),
            all_keys: user.root_permissions.all_keys,
            channel_patterns: user.root_permissions.channel_patterns.clone(),
            all_channels: user.root_permissions.all_channels,
        }
    }

    /// Create permissions that allow everything.
    pub fn allow_all() -> Self {
        Self {
            allow_all_commands: true,
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
            allowed_categories: HashSet::new(),
            denied_categories: HashSet::new(),
            key_patterns: vec![],
            all_keys: true,
            channel_patterns: vec![],
            all_channels: true,
        }
    }

    /// Check if a command is allowed.
    pub fn check_command(&self, command: &str, subcommand: Option<&str>) -> bool {
        let cmd_lower = command.to_lowercase();

        // Explicit deny takes precedence
        if self.denied_commands.contains(&cmd_lower) {
            return false;
        }

        // Check denied categories
        if let Some(category) = super::categories::CommandCategory::for_command(&cmd_lower) {
            if self.denied_categories.contains(&category) && !self.allowed_commands.contains(&cmd_lower) {
                return false;
            }
        }

        // Allow all
        if self.allow_all_commands {
            return true;
        }

        // Explicit allow
        if self.allowed_commands.contains(&cmd_lower) {
            return true;
        }

        // Check allowed categories
        if let Some(category) = super::categories::CommandCategory::for_command(&cmd_lower) {
            if self.allowed_categories.contains(&category) {
                return true;
            }
        }

        // Check subcommand (Redis 7.0+)
        if let Some(_sub) = subcommand {
            // For now, subcommand rules are not implemented
        }

        false
    }

    /// Check if key access is allowed.
    pub fn check_key_access(&self, key: &[u8], access: super::permissions::KeyAccessType) -> bool {
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

    /// Check if channel access is allowed.
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
}

/// An authenticated user with a snapshot of their permissions.
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    /// Username.
    pub username: Arc<str>,
    /// Snapshot of permissions at authentication time.
    pub permissions: Arc<UserPermissions>,
}

impl AuthenticatedUser {
    /// Create a new authenticated user.
    pub fn new(username: impl Into<Arc<str>>, permissions: UserPermissions) -> Self {
        Self {
            username: username.into(),
            permissions: Arc::new(permissions),
        }
    }

    /// Create a default authenticated user with full permissions.
    pub fn default_user() -> Self {
        Self {
            username: Arc::from("default"),
            permissions: Arc::new(UserPermissions::allow_all()),
        }
    }

    /// Check if a command is allowed.
    pub fn check_command(&self, command: &str, subcommand: Option<&str>) -> bool {
        self.permissions.check_command(command, subcommand)
    }

    /// Check if key access is allowed.
    pub fn check_key_access(&self, key: &[u8], access: super::permissions::KeyAccessType) -> bool {
        self.permissions.check_key_access(key, access)
    }

    /// Check if channel access is allowed.
    pub fn check_channel_access(&self, channel: &[u8]) -> bool {
        self.permissions.check_channel_access(channel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn hash_password(password: &str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        hasher.finalize().into()
    }

    #[test]
    fn test_new_user() {
        let user = User::new("alice");
        assert_eq!(user.name, "alice");
        assert!(!user.enabled);
        assert!(user.password_hashes.is_empty());
        assert!(!user.nopass);
    }

    #[test]
    fn test_default_user() {
        let user = User::default_user();
        assert_eq!(user.name, "default");
        assert!(user.enabled);
        assert!(user.nopass);
        assert!(user.root_permissions.commands.allow_all);
        assert!(user.root_permissions.all_keys);
        assert!(user.root_permissions.all_channels);
    }

    #[test]
    fn test_password_verification() {
        let mut user = User::new("alice");
        let hash = hash_password("secret");
        user.add_password(hash);
        user.enabled = true;

        assert!(user.verify_password(&hash));
        assert!(!user.verify_password(&hash_password("wrong")));

        // Nopass mode
        user.set_nopass();
        assert!(user.verify_password(&hash_password("anything")));
    }

    #[test]
    fn test_password_management() {
        let mut user = User::new("alice");
        let hash1 = hash_password("pass1");
        let hash2 = hash_password("pass2");

        user.add_password(hash1);
        user.add_password(hash2);
        assert_eq!(user.password_hashes.len(), 2);

        user.remove_password(&hash1);
        assert_eq!(user.password_hashes.len(), 1);
        assert!(!user.verify_password(&hash1));
        assert!(user.verify_password(&hash2));

        user.reset_passwords();
        assert!(user.password_hashes.is_empty());
        assert!(!user.nopass);
    }

    #[test]
    fn test_user_reset() {
        let mut user = User::default_user();
        user.reset();

        assert!(!user.enabled);
        assert!(user.password_hashes.is_empty());
        assert!(!user.nopass);
        assert!(!user.root_permissions.commands.allow_all);
    }

    #[test]
    fn test_acl_string() {
        let user = User::default_user();
        let acl_str = user.to_acl_string();
        assert!(acl_str.contains("user default"));
        assert!(acl_str.contains("on"));
        assert!(acl_str.contains("nopass"));
        assert!(acl_str.contains("~*"));
        assert!(acl_str.contains("+@all"));
    }

    #[test]
    fn test_authenticated_user() {
        let user = AuthenticatedUser::default_user();
        assert_eq!(&*user.username, "default");
        assert!(user.check_command("GET", None));
        assert!(user.check_key_access(b"any:key", super::super::permissions::KeyAccessType::Read));
        assert!(user.check_channel_access(b"any:channel"));
    }

    #[test]
    fn test_user_permissions() {
        let mut user = User::new("test");
        user.root_permissions.commands.allow_command("get");
        user.root_permissions.add_key_pattern(
            super::super::permissions::KeyPattern::new("user:*".to_string())
        );

        let perms = UserPermissions::from_user(&user);
        assert!(perms.check_command("GET", None));
        assert!(!perms.check_command("SET", None));
        assert!(perms.check_key_access(b"user:123", super::super::permissions::KeyAccessType::Read));
        assert!(!perms.check_key_access(b"data:123", super::super::permissions::KeyAccessType::Read));
    }
}
