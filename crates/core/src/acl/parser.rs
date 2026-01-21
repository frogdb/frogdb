//! ACL rule parsing.
//!
//! Parses ACL rule strings into user modifications.

use sha2::{Digest, Sha256};

use super::categories::CommandCategory;
use super::error::AclError;
use super::permissions::{ChannelPattern, KeyAccessType, KeyPattern};
use super::user::User;

/// Hash a password string using SHA256.
pub fn hash_password(password: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hasher.finalize().into()
}

/// Parse a hex-encoded SHA256 hash.
fn parse_hex_hash(hex: &str) -> Result<[u8; 32], AclError> {
    if hex.len() != 64 {
        return Err(AclError::ParseError {
            modifier: format!("#{}", hex),
            reason: "Hash must be 64 hex characters (SHA256)".to_string(),
        });
    }

    let bytes = hex::decode(hex).map_err(|_| AclError::ParseError {
        modifier: format!("#{}", hex),
        reason: "Invalid hex characters".to_string(),
    })?;

    bytes.try_into().map_err(|_| AclError::ParseError {
        modifier: format!("#{}", hex),
        reason: "Hash must be 32 bytes".to_string(),
    })
}

/// ACL rule types that can be applied to a user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AclRule {
    /// Enable the user.
    On,
    /// Disable the user.
    Off,
    /// Reset user to default state.
    Reset,
    /// Add a password (plaintext, will be hashed).
    AddPassword(String),
    /// Remove a password (plaintext, will be hashed).
    RemovePassword(String),
    /// Add a password hash directly.
    AddPasswordHash([u8; 32]),
    /// Remove a password hash.
    RemovePasswordHash([u8; 32]),
    /// Enable nopass mode.
    NoPass,
    /// Reset passwords (clear all, disable nopass).
    ResetPass,
    /// Add a key pattern.
    AddKeyPattern(KeyPattern),
    /// Allow all keys.
    AllKeys,
    /// Reset key patterns.
    ResetKeys,
    /// Add a channel pattern.
    AddChannelPattern(ChannelPattern),
    /// Allow all channels.
    AllChannels,
    /// Reset channel patterns.
    ResetChannels,
    /// Allow a command.
    AllowCommand(String),
    /// Deny a command.
    DenyCommand(String),
    /// Allow a category.
    AllowCategory(CommandCategory),
    /// Deny a category.
    DenyCategory(CommandCategory),
    /// Allow all commands.
    AllCommands,
    /// Deny all commands (reset to no permissions).
    NoCommands,
}

impl AclRule {
    /// Parse a single ACL rule string.
    pub fn parse(rule: &str) -> Result<Self, AclError> {
        let rule = rule.trim();

        // Simple keywords
        match rule.to_lowercase().as_str() {
            "on" => return Ok(AclRule::On),
            "off" => return Ok(AclRule::Off),
            "reset" => return Ok(AclRule::Reset),
            "nopass" => return Ok(AclRule::NoPass),
            "resetpass" => return Ok(AclRule::ResetPass),
            "allkeys" => return Ok(AclRule::AllKeys),
            "resetkeys" => return Ok(AclRule::ResetKeys),
            "allchannels" => return Ok(AclRule::AllChannels),
            "resetchannels" => return Ok(AclRule::ResetChannels),
            "allcommands" | "+@all" => return Ok(AclRule::AllCommands),
            "nocommands" | "-@all" => return Ok(AclRule::NoCommands),
            _ => {}
        }

        // Password rules
        if let Some(password) = rule.strip_prefix('>') {
            if password.is_empty() {
                return Err(AclError::ParseError {
                    modifier: rule.to_string(),
                    reason: "Password cannot be empty".to_string(),
                });
            }
            return Ok(AclRule::AddPassword(password.to_string()));
        }

        if let Some(password) = rule.strip_prefix('<') {
            if password.is_empty() {
                return Err(AclError::ParseError {
                    modifier: rule.to_string(),
                    reason: "Password cannot be empty".to_string(),
                });
            }
            return Ok(AclRule::RemovePassword(password.to_string()));
        }

        if let Some(hex) = rule.strip_prefix('#') {
            let hash = parse_hex_hash(hex)?;
            return Ok(AclRule::AddPasswordHash(hash));
        }

        if let Some(hex) = rule.strip_prefix('!') {
            let hash = parse_hex_hash(hex)?;
            return Ok(AclRule::RemovePasswordHash(hash));
        }

        // Key pattern rules
        if let Some(pattern) = rule.strip_prefix("~") {
            return Ok(AclRule::AddKeyPattern(KeyPattern::new(pattern.to_string())));
        }

        if let Some(pattern) = rule.strip_prefix("%R~") {
            return Ok(AclRule::AddKeyPattern(KeyPattern::with_access(
                pattern.to_string(),
                KeyAccessType::Read,
            )));
        }

        if let Some(pattern) = rule.strip_prefix("%W~") {
            return Ok(AclRule::AddKeyPattern(KeyPattern::with_access(
                pattern.to_string(),
                KeyAccessType::Write,
            )));
        }

        if let Some(pattern) = rule.strip_prefix("%RW~") {
            return Ok(AclRule::AddKeyPattern(KeyPattern::with_access(
                pattern.to_string(),
                KeyAccessType::ReadWrite,
            )));
        }

        // Channel pattern rules
        if let Some(pattern) = rule.strip_prefix('&') {
            return Ok(AclRule::AddChannelPattern(ChannelPattern::new(pattern.to_string())));
        }

        // Command rules
        if let Some(cmd) = rule.strip_prefix("+@") {
            if cmd.to_lowercase() == "all" {
                return Ok(AclRule::AllCommands);
            }
            let category = CommandCategory::from_str(cmd).ok_or_else(|| AclError::ParseError {
                modifier: rule.to_string(),
                reason: format!("Unknown category '{}'", cmd),
            })?;
            return Ok(AclRule::AllowCategory(category));
        }

        if let Some(cmd) = rule.strip_prefix("-@") {
            if cmd.to_lowercase() == "all" {
                return Ok(AclRule::NoCommands);
            }
            let category = CommandCategory::from_str(cmd).ok_or_else(|| AclError::ParseError {
                modifier: rule.to_string(),
                reason: format!("Unknown category '{}'", cmd),
            })?;
            return Ok(AclRule::DenyCategory(category));
        }

        if let Some(cmd) = rule.strip_prefix('+') {
            if cmd.is_empty() {
                return Err(AclError::ParseError {
                    modifier: rule.to_string(),
                    reason: "Command name cannot be empty".to_string(),
                });
            }
            return Ok(AclRule::AllowCommand(cmd.to_lowercase()));
        }

        if let Some(cmd) = rule.strip_prefix('-') {
            if cmd.is_empty() {
                return Err(AclError::ParseError {
                    modifier: rule.to_string(),
                    reason: "Command name cannot be empty".to_string(),
                });
            }
            return Ok(AclRule::DenyCommand(cmd.to_lowercase()));
        }

        Err(AclError::UnknownRule {
            rule: rule.to_string(),
        })
    }

    /// Apply this rule to a user.
    pub fn apply(&self, user: &mut User) {
        match self {
            AclRule::On => {
                user.enabled = true;
            }
            AclRule::Off => {
                user.enabled = false;
            }
            AclRule::Reset => {
                user.reset();
            }
            AclRule::AddPassword(password) => {
                let hash = hash_password(password);
                user.add_password(hash);
            }
            AclRule::RemovePassword(password) => {
                let hash = hash_password(password);
                user.remove_password(&hash);
            }
            AclRule::AddPasswordHash(hash) => {
                user.add_password(*hash);
            }
            AclRule::RemovePasswordHash(hash) => {
                user.remove_password(hash);
            }
            AclRule::NoPass => {
                user.set_nopass();
            }
            AclRule::ResetPass => {
                user.reset_passwords();
            }
            AclRule::AddKeyPattern(pattern) => {
                user.root_permissions.add_key_pattern(pattern.clone());
            }
            AclRule::AllKeys => {
                user.root_permissions.all_keys = true;
                user.root_permissions.key_patterns.clear();
            }
            AclRule::ResetKeys => {
                user.root_permissions.reset_keys();
            }
            AclRule::AddChannelPattern(pattern) => {
                user.root_permissions.add_channel_pattern(pattern.clone());
            }
            AclRule::AllChannels => {
                user.root_permissions.all_channels = true;
                user.root_permissions.channel_patterns.clear();
            }
            AclRule::ResetChannels => {
                user.root_permissions.reset_channels();
            }
            AclRule::AllowCommand(cmd) => {
                user.root_permissions.commands.allow_command(cmd);
            }
            AclRule::DenyCommand(cmd) => {
                user.root_permissions.commands.deny_command(cmd);
            }
            AclRule::AllowCategory(category) => {
                user.root_permissions.commands.allow_category(*category);
            }
            AclRule::DenyCategory(category) => {
                user.root_permissions.commands.deny_category(*category);
            }
            AclRule::AllCommands => {
                user.root_permissions.commands.allow_all = true;
                user.root_permissions.commands.allowed_commands.clear();
                user.root_permissions.commands.denied_commands.clear();
                user.root_permissions.commands.allowed_categories.clear();
                user.root_permissions.commands.denied_categories.clear();
            }
            AclRule::NoCommands => {
                user.root_permissions.commands.reset();
            }
        }
    }
}

/// Parse multiple ACL rules and apply them to a user.
pub fn parse_and_apply_rules(user: &mut User, rules: &[&str]) -> Result<(), AclError> {
    for rule in rules {
        let parsed = AclRule::parse(rule)?;
        parsed.apply(user);
    }
    Ok(())
}

/// Parse an ACL file line (format: "user <username> <rules...>").
pub fn parse_acl_line(line: &str) -> Result<(String, Vec<AclRule>), AclError> {
    let line = line.trim();

    // Skip empty lines and comments
    if line.is_empty() || line.starts_with('#') {
        return Err(AclError::ParseError {
            modifier: line.to_string(),
            reason: "Empty or comment line".to_string(),
        });
    }

    // Must start with "user"
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 || parts[0].to_lowercase() != "user" {
        return Err(AclError::ParseError {
            modifier: line.to_string(),
            reason: "ACL line must start with 'user <username>'".to_string(),
        });
    }

    let username = parts[1].to_string();
    let mut rules = Vec::new();

    for rule in &parts[2..] {
        rules.push(AclRule::parse(rule)?);
    }

    Ok((username, rules))
}

/// Generate a random password of the specified number of bits.
pub fn generate_password(bits: u32) -> String {
    use rand::RngCore;

    let bytes = (bits / 8) as usize;
    let mut buffer = vec![0u8; bytes.max(32)]; // Minimum 256 bits
    rand::thread_rng().fill_bytes(&mut buffer);
    hex::encode(&buffer[..bytes.max(32)])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_password() {
        let hash = hash_password("secret");
        assert_eq!(hash.len(), 32);

        // Same password produces same hash
        assert_eq!(hash, hash_password("secret"));

        // Different passwords produce different hashes
        assert_ne!(hash, hash_password("other"));
    }

    #[test]
    fn test_parse_simple_rules() {
        assert_eq!(AclRule::parse("on"), Ok(AclRule::On));
        assert_eq!(AclRule::parse("OFF"), Ok(AclRule::Off));
        assert_eq!(AclRule::parse("reset"), Ok(AclRule::Reset));
        assert_eq!(AclRule::parse("nopass"), Ok(AclRule::NoPass));
        assert_eq!(AclRule::parse("resetpass"), Ok(AclRule::ResetPass));
        assert_eq!(AclRule::parse("allkeys"), Ok(AclRule::AllKeys));
        assert_eq!(AclRule::parse("resetkeys"), Ok(AclRule::ResetKeys));
        assert_eq!(AclRule::parse("allchannels"), Ok(AclRule::AllChannels));
        assert_eq!(AclRule::parse("resetchannels"), Ok(AclRule::ResetChannels));
        assert_eq!(AclRule::parse("allcommands"), Ok(AclRule::AllCommands));
        assert_eq!(AclRule::parse("+@all"), Ok(AclRule::AllCommands));
        assert_eq!(AclRule::parse("nocommands"), Ok(AclRule::NoCommands));
        assert_eq!(AclRule::parse("-@all"), Ok(AclRule::NoCommands));
    }

    #[test]
    fn test_parse_password_rules() {
        assert_eq!(
            AclRule::parse(">secret"),
            Ok(AclRule::AddPassword("secret".to_string()))
        );
        assert_eq!(
            AclRule::parse("<secret"),
            Ok(AclRule::RemovePassword("secret".to_string()))
        );

        // Empty password
        assert!(AclRule::parse(">").is_err());
        assert!(AclRule::parse("<").is_err());
    }

    #[test]
    fn test_parse_hash_rules() {
        let hash_hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let result = AclRule::parse(&format!("#{}", hash_hex));
        assert!(result.is_ok());

        // Invalid hex
        assert!(AclRule::parse("#invalid").is_err());
        // Wrong length
        assert!(AclRule::parse("#abc").is_err());
    }

    #[test]
    fn test_parse_key_patterns() {
        let result = AclRule::parse("~user:*");
        assert!(matches!(result, Ok(AclRule::AddKeyPattern(p)) if p.pattern == "user:*"));

        let result = AclRule::parse("%R~data:*");
        assert!(matches!(result, Ok(AclRule::AddKeyPattern(p)) if p.access_type == KeyAccessType::Read));

        let result = AclRule::parse("%W~logs:*");
        assert!(matches!(result, Ok(AclRule::AddKeyPattern(p)) if p.access_type == KeyAccessType::Write));
    }

    #[test]
    fn test_parse_channel_patterns() {
        let result = AclRule::parse("&news:*");
        assert!(matches!(result, Ok(AclRule::AddChannelPattern(p)) if p.pattern == "news:*"));
    }

    #[test]
    fn test_parse_command_rules() {
        assert_eq!(
            AclRule::parse("+get"),
            Ok(AclRule::AllowCommand("get".to_string()))
        );
        assert_eq!(
            AclRule::parse("-set"),
            Ok(AclRule::DenyCommand("set".to_string()))
        );
        assert_eq!(
            AclRule::parse("+@read"),
            Ok(AclRule::AllowCategory(CommandCategory::Read))
        );
        assert_eq!(
            AclRule::parse("-@write"),
            Ok(AclRule::DenyCategory(CommandCategory::Write))
        );

        // Empty command
        assert!(AclRule::parse("+").is_err());
        assert!(AclRule::parse("-").is_err());

        // Unknown category
        assert!(AclRule::parse("+@unknown").is_err());
    }

    #[test]
    fn test_apply_rules() {
        let mut user = User::new("test");

        AclRule::On.apply(&mut user);
        assert!(user.enabled);

        AclRule::Off.apply(&mut user);
        assert!(!user.enabled);

        AclRule::AddPassword("secret".to_string()).apply(&mut user);
        assert!(user.verify_password(&hash_password("secret")));

        AclRule::NoPass.apply(&mut user);
        assert!(user.nopass);

        AclRule::AllKeys.apply(&mut user);
        assert!(user.root_permissions.all_keys);

        AclRule::AllCommands.apply(&mut user);
        assert!(user.root_permissions.commands.allow_all);
    }

    #[test]
    fn test_parse_and_apply_rules() {
        let mut user = User::new("alice");
        let rules = vec!["on", ">password", "~user:*", "+@read", "-@write"];
        parse_and_apply_rules(&mut user, &rules).unwrap();

        assert!(user.enabled);
        assert!(user.verify_password(&hash_password("password")));
        assert!(!user.root_permissions.all_keys);
        assert_eq!(user.root_permissions.key_patterns.len(), 1);
        assert!(user.root_permissions.commands.allowed_categories.contains(&CommandCategory::Read));
        assert!(user.root_permissions.commands.denied_categories.contains(&CommandCategory::Write));
    }

    #[test]
    fn test_parse_acl_line() {
        let (username, rules) = parse_acl_line("user alice on nopass ~* +@all").unwrap();
        assert_eq!(username, "alice");
        assert_eq!(rules.len(), 4);
        assert_eq!(rules[0], AclRule::On);
        assert_eq!(rules[1], AclRule::NoPass);
        assert_eq!(rules[2], AclRule::AddKeyPattern(KeyPattern::new("*".to_string())));
        assert_eq!(rules[3], AclRule::AllCommands);

        // Empty line
        assert!(parse_acl_line("").is_err());
        // Comment
        assert!(parse_acl_line("# comment").is_err());
        // Missing user prefix
        assert!(parse_acl_line("alice on nopass").is_err());
    }

    #[test]
    fn test_generate_password() {
        let pass = generate_password(256);
        assert_eq!(pass.len(), 64); // 256 bits = 32 bytes = 64 hex chars

        // Passwords should be different each time
        let pass2 = generate_password(256);
        assert_ne!(pass, pass2);
    }

    #[test]
    fn test_unknown_rule() {
        let result = AclRule::parse("unknown_rule");
        assert!(matches!(result, Err(AclError::UnknownRule { .. })));
    }
}
