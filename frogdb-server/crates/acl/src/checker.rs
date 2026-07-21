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

/// Full ACL checker with permission enforcement.
///
/// Turns a `bool` permission decision (delegated to [`AuthenticatedUser`]'s
/// snapshot of [`PermissionSet`](crate::permissions::PermissionSet)) into the
/// canonical [`AclError`] reply — the module's load-bearing responsibility.
/// It is the single production ACL checker; there is no polymorphism seam
/// around it.
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

    /// Check if a command is allowed for the user.
    pub fn check_command(
        &self,
        user: &AuthenticatedUser,
        cmd: &str,
        subcmd: Option<&str>,
    ) -> PermissionResult {
        if user.check_command(cmd, subcmd) {
            PermissionResult::Allowed
        } else if let Some(sub) = subcmd {
            // Redis renders the command's lowercase fullname (e.g. `config|set`) in both the
            // NOPERM reply and the ACL LOG object; keep the two in lockstep at the source.
            PermissionResult::Denied(AclError::NoPermissionSubcommand {
                command: cmd.to_lowercase(),
                subcommand: sub.to_lowercase(),
            })
        } else {
            PermissionResult::Denied(AclError::NoPermissionCommand {
                command: cmd.to_lowercase(),
            })
        }
    }

    /// Check if key access is allowed for the user.
    pub fn check_key_access(
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

    /// Check if channel access is allowed for the user.
    pub fn check_channel_access(
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

    /// Check if authentication is required.
    pub fn requires_auth(&self) -> bool {
        self.requires_auth
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::{
        ChannelPattern, CommandPermissions, KeyPattern, PermissionSet, SubcommandRule,
    };
    use std::sync::Arc;

    fn create_test_user(
        all_commands: bool,
        all_keys: bool,
        all_channels: bool,
    ) -> AuthenticatedUser {
        let commands = if all_commands {
            CommandPermissions::allow_all()
        } else {
            CommandPermissions::deny_all()
        };
        let perms = PermissionSet {
            commands,
            key_patterns: vec![],
            channel_patterns: vec![],
            all_keys,
            all_channels,
        };
        AuthenticatedUser::new("test", Arc::new(perms), None)
    }

    fn create_restricted_user() -> AuthenticatedUser {
        let mut commands = CommandPermissions::deny_all();
        commands.allowed_commands.insert("get".to_string());
        commands.allowed_commands.insert("set".to_string());

        let perms = PermissionSet {
            commands,
            key_patterns: vec![KeyPattern::new("user:*".to_string())],
            channel_patterns: vec![ChannelPattern::new("chat:*".to_string())],
            all_keys: false,
            all_channels: false,
        };
        AuthenticatedUser::new("restricted", Arc::new(perms), None)
    }

    #[test]
    fn test_full_checker_all_allowed() {
        let checker = FullAclChecker::new(true);
        let user = create_test_user(true, true, true);

        assert!(checker.check_command(&user, "GET", None).is_allowed());
        assert!(checker.check_command(&user, "FLUSHALL", None).is_allowed());
        assert!(
            checker
                .check_key_access(&user, b"any:key", KeyAccessType::ReadWrite)
                .is_allowed()
        );
        assert!(
            checker
                .check_channel_access(&user, b"any:channel")
                .is_allowed()
        );
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
        assert!(matches!(
            result,
            PermissionResult::Denied(AclError::NoPermissionCommand { .. })
        ));

        // Allowed keys
        assert!(
            checker
                .check_key_access(&user, b"user:123", KeyAccessType::Read)
                .is_allowed()
        );

        // Denied keys
        let result = checker.check_key_access(&user, b"admin:123", KeyAccessType::Read);
        assert!(!result.is_allowed());
        assert!(matches!(
            result,
            PermissionResult::Denied(AclError::NoPermissionKey)
        ));

        // Allowed channels
        assert!(
            checker
                .check_channel_access(&user, b"chat:general")
                .is_allowed()
        );

        // Denied channels
        let result = checker.check_channel_access(&user, b"private:123");
        assert!(!result.is_allowed());
        assert!(matches!(
            result,
            PermissionResult::Denied(AclError::NoPermissionChannel)
        ));
    }

    #[test]
    fn test_full_checker_subcommand_denied_uses_lowercase_pipe() {
        // The NOPERM reply (AclError Display) must use the lowercase `cmd|sub` fullname,
        // matching Redis and the ACL LOG object.
        let checker = FullAclChecker::new(true);
        let mut commands = CommandPermissions::allow_all();
        commands.subcommand_rules.push(SubcommandRule {
            command: "config".to_string(),
            subcommand: "set".to_string(),
            allowed: false,
        });
        let perms = PermissionSet {
            commands,
            key_patterns: vec![],
            channel_patterns: vec![],
            all_keys: true,
            all_channels: true,
        };
        let user = AuthenticatedUser::new("test", Arc::new(perms), None);

        let result = checker.check_command(&user, "CONFIG", Some("SET"));
        let err = result.error().expect("CONFIG|SET should be denied").clone();
        assert_eq!(
            err,
            AclError::NoPermissionSubcommand {
                command: "config".to_string(),
                subcommand: "set".to_string(),
            }
        );
        assert_eq!(
            err.to_string(),
            "NOPERM this user has no permissions to run the 'config|set' command"
        );
    }

    #[test]
    fn test_full_checker_command_denied_uses_lowercase() {
        let checker = FullAclChecker::new(true);
        let user = create_restricted_user();
        let err = checker
            .check_command(&user, "FLUSHALL", None)
            .error()
            .expect("FLUSHALL should be denied")
            .clone();
        assert_eq!(
            err.to_string(),
            "NOPERM this user has no permissions to run the 'flushall' command"
        );
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
