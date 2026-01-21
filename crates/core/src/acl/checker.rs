//! ACL permission checking traits and implementations.

use std::sync::Arc;

use super::error::AclError;
use super::manager::AclManager;
use super::permissions::KeyAccessType;
use super::user::AuthenticatedUser;

/// Result of a permission check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionResult {
    /// Permission granted.
    Allowed,
    /// Permission denied with a specific error.
    Denied(AclError),
}

impl PermissionResult {
    /// Check if permission was granted.
    pub fn is_allowed(&self) -> bool {
        matches!(self, PermissionResult::Allowed)
    }

    /// Get the error if permission was denied.
    pub fn error(&self) -> Option<&AclError> {
        match self {
            PermissionResult::Allowed => None,
            PermissionResult::Denied(e) => Some(e),
        }
    }

    /// Convert to Result.
    pub fn into_result(self) -> Result<(), AclError> {
        match self {
            PermissionResult::Allowed => Ok(()),
            PermissionResult::Denied(e) => Err(e),
        }
    }
}

/// Trait for ACL permission checking.
pub trait AclChecker: Send + Sync {
    /// Check if a command is allowed for the user.
    fn check_command(
        &self,
        user: &AuthenticatedUser,
        cmd: &str,
        subcmd: Option<&str>,
    ) -> PermissionResult;

    /// Check if key access is allowed for the user.
    fn check_key_access(
        &self,
        user: &AuthenticatedUser,
        key: &[u8],
        access: KeyAccessType,
    ) -> PermissionResult;

    /// Check if channel access is allowed for the user.
    fn check_channel_access(
        &self,
        user: &AuthenticatedUser,
        channel: &[u8],
    ) -> PermissionResult;

    /// Check if authentication is required.
    fn requires_auth(&self) -> bool;

    /// Check if a command is allowed without authentication.
    /// These commands can run before AUTH (e.g., AUTH, QUIT, HELLO).
    fn is_auth_exempt(&self, cmd: &str) -> bool {
        let cmd_lower = cmd.to_lowercase();
        matches!(cmd_lower.as_str(), "auth" | "quit" | "hello")
    }
}

/// ACL checker that always allows all operations.
/// Used when no authentication is configured.
#[derive(Debug, Default, Clone)]
pub struct AllowAllChecker;

impl AllowAllChecker {
    /// Create a new AllowAllChecker.
    pub fn new() -> Self {
        Self
    }
}

impl AclChecker for AllowAllChecker {
    fn check_command(
        &self,
        _user: &AuthenticatedUser,
        _cmd: &str,
        _subcmd: Option<&str>,
    ) -> PermissionResult {
        PermissionResult::Allowed
    }

    fn check_key_access(
        &self,
        _user: &AuthenticatedUser,
        _key: &[u8],
        _access: KeyAccessType,
    ) -> PermissionResult {
        PermissionResult::Allowed
    }

    fn check_channel_access(
        &self,
        _user: &AuthenticatedUser,
        _channel: &[u8],
    ) -> PermissionResult {
        PermissionResult::Allowed
    }

    fn requires_auth(&self) -> bool {
        false
    }
}

/// Full ACL checker with permission enforcement.
#[derive(Debug, Clone)]
pub struct FullAclChecker {
    /// Whether authentication is required.
    requires_auth: bool,
}

impl FullAclChecker {
    /// Create a new FullAclChecker.
    pub fn new(requires_auth: bool) -> Self {
        Self { requires_auth }
    }

    /// Create from an AclManager.
    pub fn from_manager(manager: &Arc<AclManager>) -> Self {
        Self {
            requires_auth: manager.requires_auth(),
        }
    }
}

impl AclChecker for FullAclChecker {
    fn check_command(
        &self,
        user: &AuthenticatedUser,
        cmd: &str,
        subcmd: Option<&str>,
    ) -> PermissionResult {
        if user.check_command(cmd, subcmd) {
            PermissionResult::Allowed
        } else {
            if let Some(sub) = subcmd {
                PermissionResult::Denied(AclError::NoPermissionSubcommand {
                    command: cmd.to_uppercase(),
                    subcommand: sub.to_uppercase(),
                })
            } else {
                PermissionResult::Denied(AclError::NoPermissionCommand {
                    command: cmd.to_uppercase(),
                })
            }
        }
    }

    fn check_key_access(
        &self,
        user: &AuthenticatedUser,
        key: &[u8],
        access: KeyAccessType,
    ) -> PermissionResult {
        if user.check_key_access(key, access) {
            PermissionResult::Allowed
        } else {
            PermissionResult::Denied(AclError::NoPermissionKey)
        }
    }

    fn check_channel_access(
        &self,
        user: &AuthenticatedUser,
        channel: &[u8],
    ) -> PermissionResult {
        if user.check_channel_access(channel) {
            PermissionResult::Allowed
        } else {
            PermissionResult::Denied(AclError::NoPermissionChannel)
        }
    }

    fn requires_auth(&self) -> bool {
        self.requires_auth
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::permissions::{ChannelPattern, KeyPattern};
    use crate::acl::user::UserPermissions;
    use std::collections::HashSet;

    fn create_test_user(all_commands: bool, all_keys: bool, all_channels: bool) -> AuthenticatedUser {
        let perms = UserPermissions {
            allow_all_commands: all_commands,
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
            allowed_categories: HashSet::new(),
            denied_categories: HashSet::new(),
            key_patterns: vec![],
            all_keys,
            channel_patterns: vec![],
            all_channels,
        };
        AuthenticatedUser::new("test", perms)
    }

    fn create_restricted_user() -> AuthenticatedUser {
        let mut allowed_commands = HashSet::new();
        allowed_commands.insert("get".to_string());
        allowed_commands.insert("set".to_string());

        let perms = UserPermissions {
            allow_all_commands: false,
            allowed_commands,
            denied_commands: HashSet::new(),
            allowed_categories: HashSet::new(),
            denied_categories: HashSet::new(),
            key_patterns: vec![KeyPattern::new("user:*".to_string())],
            all_keys: false,
            channel_patterns: vec![ChannelPattern::new("chat:*".to_string())],
            all_channels: false,
        };
        AuthenticatedUser::new("restricted", perms)
    }

    #[test]
    fn test_allow_all_checker() {
        let checker = AllowAllChecker::new();
        let user = create_test_user(false, false, false);

        assert!(checker.check_command(&user, "GET", None).is_allowed());
        assert!(checker.check_key_access(&user, b"any:key", KeyAccessType::Read).is_allowed());
        assert!(checker.check_channel_access(&user, b"any:channel").is_allowed());
        assert!(!checker.requires_auth());
    }

    #[test]
    fn test_full_checker_all_allowed() {
        let checker = FullAclChecker::new(true);
        let user = create_test_user(true, true, true);

        assert!(checker.check_command(&user, "GET", None).is_allowed());
        assert!(checker.check_command(&user, "FLUSHALL", None).is_allowed());
        assert!(checker.check_key_access(&user, b"any:key", KeyAccessType::ReadWrite).is_allowed());
        assert!(checker.check_channel_access(&user, b"any:channel").is_allowed());
        assert!(checker.requires_auth());
    }

    #[test]
    fn test_full_checker_restricted() {
        let checker = FullAclChecker::new(true);
        let user = create_restricted_user();

        // Allowed commands
        assert!(checker.check_command(&user, "GET", None).is_allowed());
        assert!(checker.check_command(&user, "SET", None).is_allowed());

        // Denied commands
        let result = checker.check_command(&user, "FLUSHALL", None);
        assert!(!result.is_allowed());
        assert!(matches!(result, PermissionResult::Denied(AclError::NoPermissionCommand { .. })));

        // Allowed keys
        assert!(checker.check_key_access(&user, b"user:123", KeyAccessType::Read).is_allowed());

        // Denied keys
        let result = checker.check_key_access(&user, b"admin:123", KeyAccessType::Read);
        assert!(!result.is_allowed());
        assert!(matches!(result, PermissionResult::Denied(AclError::NoPermissionKey)));

        // Allowed channels
        assert!(checker.check_channel_access(&user, b"chat:general").is_allowed());

        // Denied channels
        let result = checker.check_channel_access(&user, b"private:123");
        assert!(!result.is_allowed());
        assert!(matches!(result, PermissionResult::Denied(AclError::NoPermissionChannel)));
    }

    #[test]
    fn test_auth_exempt_commands() {
        let checker = FullAclChecker::new(true);
        assert!(checker.is_auth_exempt("AUTH"));
        assert!(checker.is_auth_exempt("auth"));
        assert!(checker.is_auth_exempt("QUIT"));
        assert!(checker.is_auth_exempt("HELLO"));
        assert!(!checker.is_auth_exempt("GET"));
        assert!(!checker.is_auth_exempt("SET"));
    }

    #[test]
    fn test_permission_result() {
        let allowed = PermissionResult::Allowed;
        assert!(allowed.is_allowed());
        assert!(allowed.error().is_none());
        assert!(allowed.into_result().is_ok());

        let denied = PermissionResult::Denied(AclError::NoPermissionCommand {
            command: "SET".to_string(),
        });
        assert!(!denied.is_allowed());
        assert!(denied.error().is_some());

        let denied2 = PermissionResult::Denied(AclError::NoAuth);
        assert!(denied2.into_result().is_err());
    }
}
