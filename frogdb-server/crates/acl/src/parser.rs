//! ACL rule parsing.
//!
//! Parses ACL rule strings into user modifications.

use sha2::{Digest, Sha256};

use super::categories::CommandCategory;
use super::error::AclError;
use super::permissions::{
    ChannelPattern, KeyAccessType, KeyPattern, PermissionSet, SubcommandRule,
};
use super::ratelimit::RateLimitConfig;
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
    /// Allow a specific subcommand (Redis 7.0+).
    AllowSubcommand { command: String, subcommand: String },
    /// Deny a specific subcommand (Redis 7.0+).
    DenySubcommand { command: String, subcommand: String },
    /// Add a selector with additional permissions (Redis 7.0+).
    AddSelector(Box<PermissionSet>),
    /// Clear all selectors (Redis 7.0+).
    ClearSelectors,
    /// Set commands-per-second rate limit.
    RateLimitCommands(u64),
    /// Set bytes-per-second rate limit.
    RateLimitBytes(u64),
    /// Reset rate limit configuration.
    ResetRateLimit,
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
            "clearselectors" => return Ok(AclRule::ClearSelectors),
            "resetratelimit" => return Ok(AclRule::ResetRateLimit),
            _ => {}
        }

        // Rate limit rules (ratelimit:cps=N, ratelimit:bps=N)
        {
            let lower = rule.to_lowercase();
            if let Some(rest) = lower.strip_prefix("ratelimit:") {
                return Self::parse_ratelimit(rest, rule);
            }
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
            return Ok(AclRule::AddChannelPattern(ChannelPattern::new(
                pattern.to_string(),
            )));
        }

        // Selector syntax: (rules)
        if rule.starts_with('(') && rule.ends_with(')') {
            let inner = &rule[1..rule.len() - 1];
            if inner.is_empty() {
                return Err(AclError::ParseError {
                    modifier: rule.to_string(),
                    reason: "Empty selector".to_string(),
                });
            }
            // Check for nested parens (not allowed)
            if inner.contains('(') || inner.contains(')') {
                return Err(AclError::ParseError {
                    modifier: rule.to_string(),
                    reason: "Nested parentheses not allowed in selector".to_string(),
                });
            }
            let perm_set = parse_selector_rules(inner)?;
            return Ok(AclRule::AddSelector(Box::new(perm_set)));
        }

        // Command rules
        if let Some(cmd) = rule.strip_prefix("+@") {
            if cmd.to_lowercase() == "all" {
                return Ok(AclRule::AllCommands);
            }
            let category = CommandCategory::parse(cmd).ok_or_else(|| AclError::ParseError {
                modifier: rule.to_string(),
                reason: format!("Unknown category '{}'", cmd),
            })?;
            return Ok(AclRule::AllowCategory(category));
        }

        if let Some(cmd) = rule.strip_prefix("-@") {
            if cmd.to_lowercase() == "all" {
                return Ok(AclRule::NoCommands);
            }
            let category = CommandCategory::parse(cmd).ok_or_else(|| AclError::ParseError {
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
            // Check for subcommand syntax: +command|subcommand
            if let Some(pipe_pos) = cmd.find('|') {
                let command = &cmd[..pipe_pos];
                let subcommand = &cmd[pipe_pos + 1..];
                if command.is_empty() {
                    return Err(AclError::ParseError {
                        modifier: rule.to_string(),
                        reason: "Command name cannot be empty".to_string(),
                    });
                }
                if subcommand.is_empty() {
                    return Err(AclError::ParseError {
                        modifier: rule.to_string(),
                        reason: "Subcommand name cannot be empty".to_string(),
                    });
                }
                return Ok(AclRule::AllowSubcommand {
                    command: command.to_lowercase(),
                    subcommand: subcommand.to_lowercase(),
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
            // Check for subcommand syntax: -command|subcommand
            if let Some(pipe_pos) = cmd.find('|') {
                let command = &cmd[..pipe_pos];
                let subcommand = &cmd[pipe_pos + 1..];
                if command.is_empty() {
                    return Err(AclError::ParseError {
                        modifier: rule.to_string(),
                        reason: "Command name cannot be empty".to_string(),
                    });
                }
                if subcommand.is_empty() {
                    return Err(AclError::ParseError {
                        modifier: rule.to_string(),
                        reason: "Subcommand name cannot be empty".to_string(),
                    });
                }
                return Ok(AclRule::DenySubcommand {
                    command: command.to_lowercase(),
                    subcommand: subcommand.to_lowercase(),
                });
            }
            return Ok(AclRule::DenyCommand(cmd.to_lowercase()));
        }

        Err(AclError::UnknownRule {
            rule: rule.to_string(),
        })
    }

    /// Parse a ratelimit sub-token (e.g. "cps=1000" or "bps=1048576").
    fn parse_ratelimit(rest: &str, original: &str) -> Result<Self, AclError> {
        if let Some(val_str) = rest.strip_prefix("cps=") {
            let val = val_str.parse::<u64>().map_err(|_| AclError::ParseError {
                modifier: original.to_string(),
                reason: "cps value must be a positive integer".to_string(),
            })?;
            return Ok(AclRule::RateLimitCommands(val));
        }
        if let Some(val_str) = rest.strip_prefix("bps=") {
            let val = val_str.parse::<u64>().map_err(|_| AclError::ParseError {
                modifier: original.to_string(),
                reason: "bps value must be a positive integer".to_string(),
            })?;
            return Ok(AclRule::RateLimitBytes(val));
        }
        Err(AclError::ParseError {
            modifier: original.to_string(),
            reason: "Unknown ratelimit parameter. Use ratelimit:cps=N or ratelimit:bps=N"
                .to_string(),
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
            AclRule::AllowSubcommand {
                command,
                subcommand,
            } => {
                user.root_permissions
                    .commands
                    .subcommand_rules
                    .push(SubcommandRule {
                        command: command.clone(),
                        subcommand: subcommand.clone(),
                        allowed: true,
                    });
            }
            AclRule::DenySubcommand {
                command,
                subcommand,
            } => {
                user.root_permissions
                    .commands
                    .subcommand_rules
                    .push(SubcommandRule {
                        command: command.clone(),
                        subcommand: subcommand.clone(),
                        allowed: false,
                    });
            }
            AclRule::AddSelector(perm_set) => {
                user.selectors.push(*perm_set.clone());
            }
            AclRule::ClearSelectors => {
                user.selectors.clear();
            }
            AclRule::RateLimitCommands(cps) => {
                user.rate_limit.commands_per_second = *cps;
            }
            AclRule::RateLimitBytes(bps) => {
                user.rate_limit.bytes_per_second = *bps;
            }
            AclRule::ResetRateLimit => {
                user.rate_limit = RateLimitConfig::default();
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

/// Parse selector rules (rules within parentheses) and return a PermissionSet.
fn parse_selector_rules(rules_str: &str) -> Result<PermissionSet, AclError> {
    let mut perm_set = PermissionSet::default();

    for rule in rules_str.split_whitespace() {
        // Key patterns
        if let Some(pattern) = rule.strip_prefix("~") {
            perm_set.add_key_pattern(KeyPattern::new(pattern.to_string()));
            continue;
        }

        if let Some(pattern) = rule.strip_prefix("%R~") {
            perm_set.add_key_pattern(KeyPattern::with_access(
                pattern.to_string(),
                KeyAccessType::Read,
            ));
            continue;
        }

        if let Some(pattern) = rule.strip_prefix("%W~") {
            perm_set.add_key_pattern(KeyPattern::with_access(
                pattern.to_string(),
                KeyAccessType::Write,
            ));
            continue;
        }

        if let Some(pattern) = rule.strip_prefix("%RW~") {
            perm_set.add_key_pattern(KeyPattern::with_access(
                pattern.to_string(),
                KeyAccessType::ReadWrite,
            ));
            continue;
        }

        // Channel patterns
        if let Some(pattern) = rule.strip_prefix('&') {
            perm_set.add_channel_pattern(ChannelPattern::new(pattern.to_string()));
            continue;
        }

        // Command categories
        if let Some(cat) = rule.strip_prefix("+@") {
            if cat.to_lowercase() == "all" {
                perm_set.commands.allow_all = true;
            } else {
                let category = CommandCategory::parse(cat).ok_or_else(|| AclError::ParseError {
                    modifier: rule.to_string(),
                    reason: format!("Unknown category '{}'", cat),
                })?;
                perm_set.commands.allow_category(category);
            }
            continue;
        }

        if let Some(cat) = rule.strip_prefix("-@") {
            if cat.to_lowercase() == "all" {
                perm_set.commands.reset();
            } else {
                let category = CommandCategory::parse(cat).ok_or_else(|| AclError::ParseError {
                    modifier: rule.to_string(),
                    reason: format!("Unknown category '{}'", cat),
                })?;
                perm_set.commands.deny_category(category);
            }
            continue;
        }

        // Commands (with potential subcommand)
        if let Some(cmd) = rule.strip_prefix('+') {
            if cmd.is_empty() {
                return Err(AclError::ParseError {
                    modifier: rule.to_string(),
                    reason: "Command name cannot be empty".to_string(),
                });
            }
            if let Some(pipe_pos) = cmd.find('|') {
                let command = &cmd[..pipe_pos];
                let subcommand = &cmd[pipe_pos + 1..];
                if command.is_empty() || subcommand.is_empty() {
                    return Err(AclError::ParseError {
                        modifier: rule.to_string(),
                        reason: "Command and subcommand names cannot be empty".to_string(),
                    });
                }
                perm_set.commands.subcommand_rules.push(SubcommandRule {
                    command: command.to_lowercase(),
                    subcommand: subcommand.to_lowercase(),
                    allowed: true,
                });
            } else {
                perm_set.commands.allow_command(cmd);
            }
            continue;
        }

        if let Some(cmd) = rule.strip_prefix('-') {
            if cmd.is_empty() {
                return Err(AclError::ParseError {
                    modifier: rule.to_string(),
                    reason: "Command name cannot be empty".to_string(),
                });
            }
            if let Some(pipe_pos) = cmd.find('|') {
                let command = &cmd[..pipe_pos];
                let subcommand = &cmd[pipe_pos + 1..];
                if command.is_empty() || subcommand.is_empty() {
                    return Err(AclError::ParseError {
                        modifier: rule.to_string(),
                        reason: "Command and subcommand names cannot be empty".to_string(),
                    });
                }
                perm_set.commands.subcommand_rules.push(SubcommandRule {
                    command: command.to_lowercase(),
                    subcommand: subcommand.to_lowercase(),
                    allowed: false,
                });
            } else {
                perm_set.commands.deny_command(cmd);
            }
            continue;
        }

        // Allkeys/allchannels special keywords
        if rule.eq_ignore_ascii_case("allkeys") {
            perm_set.all_keys = true;
            continue;
        }

        if rule.eq_ignore_ascii_case("allchannels") {
            perm_set.all_channels = true;
            continue;
        }

        return Err(AclError::ParseError {
            modifier: rule.to_string(),
            reason: "Unknown rule in selector".to_string(),
        });
    }

    Ok(perm_set)
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
    use rand::Rng;

    let bytes = (bits / 8) as usize;
    let mut buffer = vec![0u8; bytes.max(32)]; // Minimum 256 bits
    rand::rng().fill_bytes(&mut buffer);
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
        assert!(
            matches!(result, Ok(AclRule::AddKeyPattern(p)) if p.access_type == KeyAccessType::Read)
        );

        let result = AclRule::parse("%W~logs:*");
        assert!(
            matches!(result, Ok(AclRule::AddKeyPattern(p)) if p.access_type == KeyAccessType::Write)
        );
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
        assert!(
            user.root_permissions
                .commands
                .allowed_categories
                .contains(&CommandCategory::Read)
        );
        assert!(
            user.root_permissions
                .commands
                .denied_categories
                .contains(&CommandCategory::Write)
        );
    }

    #[test]
    fn test_parse_acl_line() {
        let (username, rules) = parse_acl_line("user alice on nopass ~* +@all").unwrap();
        assert_eq!(username, "alice");
        assert_eq!(rules.len(), 4);
        assert_eq!(rules[0], AclRule::On);
        assert_eq!(rules[1], AclRule::NoPass);
        assert_eq!(
            rules[2],
            AclRule::AddKeyPattern(KeyPattern::new("*".to_string()))
        );
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

    #[test]
    fn test_parse_subcommand_allow() {
        let result = AclRule::parse("+config|get");
        assert!(matches!(
            result,
            Ok(AclRule::AllowSubcommand { command, subcommand })
            if command == "config" && subcommand == "get"
        ));

        // Case insensitivity
        let result = AclRule::parse("+CONFIG|GET");
        assert!(matches!(
            result,
            Ok(AclRule::AllowSubcommand { command, subcommand })
            if command == "config" && subcommand == "get"
        ));
    }

    #[test]
    fn test_parse_subcommand_deny() {
        let result = AclRule::parse("-config|set");
        assert!(matches!(
            result,
            Ok(AclRule::DenySubcommand { command, subcommand })
            if command == "config" && subcommand == "set"
        ));
    }

    #[test]
    fn test_parse_invalid_subcommand() {
        // Empty subcommand
        assert!(AclRule::parse("+config|").is_err());
        assert!(AclRule::parse("-config|").is_err());

        // Empty command before pipe
        assert!(AclRule::parse("+|get").is_err());
        assert!(AclRule::parse("-|get").is_err());
    }

    #[test]
    fn test_parse_selector_simple() {
        let result = AclRule::parse("(~temp:* +@read)");
        assert!(matches!(result, Ok(AclRule::AddSelector(ref perm_set)) if {
            perm_set.key_patterns.len() == 1 &&
            perm_set.key_patterns[0].pattern == "temp:*" &&
            perm_set.commands.allowed_categories.contains(&CommandCategory::Read)
        }));
    }

    #[test]
    fn test_parse_selector_complex() {
        let result = AclRule::parse("(~cache:* %R~data:* +@read -@write)");
        assert!(matches!(result, Ok(AclRule::AddSelector(ref perm_set)) if {
            perm_set.key_patterns.len() == 2 &&
            perm_set.commands.allowed_categories.contains(&CommandCategory::Read) &&
            perm_set.commands.denied_categories.contains(&CommandCategory::Write)
        }));
    }

    #[test]
    fn test_parse_selector_empty() {
        assert!(AclRule::parse("()").is_err());
    }

    #[test]
    fn test_parse_selector_nested_parens() {
        assert!(AclRule::parse("((~foo:*))").is_err());
        assert!(AclRule::parse("(~foo:* (+@read))").is_err());
    }

    #[test]
    fn test_parse_clearselectors() {
        assert_eq!(
            AclRule::parse("clearselectors"),
            Ok(AclRule::ClearSelectors)
        );
        assert_eq!(
            AclRule::parse("CLEARSELECTORS"),
            Ok(AclRule::ClearSelectors)
        );
    }

    #[test]
    fn test_apply_subcommand_rules() {
        let mut user = User::new("test");

        AclRule::AllowSubcommand {
            command: "config".to_string(),
            subcommand: "get".to_string(),
        }
        .apply(&mut user);

        assert_eq!(user.root_permissions.commands.subcommand_rules.len(), 1);
        assert_eq!(
            user.root_permissions.commands.subcommand_rules[0].command,
            "config"
        );
        assert_eq!(
            user.root_permissions.commands.subcommand_rules[0].subcommand,
            "get"
        );
        assert!(user.root_permissions.commands.subcommand_rules[0].allowed);

        AclRule::DenySubcommand {
            command: "config".to_string(),
            subcommand: "set".to_string(),
        }
        .apply(&mut user);

        assert_eq!(user.root_permissions.commands.subcommand_rules.len(), 2);
        assert!(!user.root_permissions.commands.subcommand_rules[1].allowed);
    }

    #[test]
    fn test_apply_selector() {
        let mut user = User::new("test");

        let mut perm_set = PermissionSet::default();
        perm_set.add_key_pattern(KeyPattern::new("temp:*".to_string()));
        perm_set.commands.allow_category(CommandCategory::Read);

        AclRule::AddSelector(Box::new(perm_set)).apply(&mut user);

        assert_eq!(user.selectors.len(), 1);
        assert_eq!(user.selectors[0].key_patterns[0].pattern, "temp:*");
    }

    #[test]
    fn test_apply_clear_selectors() {
        let mut user = User::new("test");

        // Add some selectors
        let mut perm_set = PermissionSet::default();
        perm_set.add_key_pattern(KeyPattern::new("temp:*".to_string()));
        user.selectors.push(perm_set);
        user.selectors.push(PermissionSet::default());

        assert_eq!(user.selectors.len(), 2);

        AclRule::ClearSelectors.apply(&mut user);

        assert!(user.selectors.is_empty());
    }

    #[test]
    fn test_selector_with_subcommand_rules() {
        let result = AclRule::parse("(~temp:* +config|get -config|set)");
        assert!(matches!(result, Ok(AclRule::AddSelector(ref perm_set)) if {
            perm_set.commands.subcommand_rules.len() == 2 &&
            perm_set.commands.subcommand_rules[0].allowed &&
            !perm_set.commands.subcommand_rules[1].allowed
        }));
    }
}
