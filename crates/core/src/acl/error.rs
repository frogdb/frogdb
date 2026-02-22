//! ACL error types.

use thiserror::Error;

/// Errors that can occur during ACL operations.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AclError {
    /// User not found.
    #[error("ERR User {username} not found")]
    UserNotFound { username: String },

    /// User already exists.
    #[error("ERR User {username} already exists")]
    UserAlreadyExists { username: String },

    /// Cannot delete the default user.
    #[error("ERR The 'default' user cannot be removed")]
    CannotDeleteDefaultUser,

    /// Invalid password format.
    #[error("ERR Invalid password format")]
    InvalidPasswordFormat,

    /// Authentication failed - wrong password.
    #[error("WRONGPASS invalid username-password pair or user is disabled.")]
    WrongPassword,

    /// User is disabled.
    #[error("WRONGPASS invalid username-password pair or user is disabled.")]
    UserDisabled,

    /// Authentication required.
    #[error("NOAUTH Authentication required.")]
    NoAuth,

    /// Permission denied for command.
    #[error("NOPERM this user has no permissions to run the '{command}' command")]
    NoPermissionCommand { command: String },

    /// Permission denied for subcommand.
    #[error("NOPERM this user has no permissions to run the '{command}|{subcommand}' command")]
    NoPermissionSubcommand { command: String, subcommand: String },

    /// Permission denied for key access.
    #[error("NOPERM this user has no permissions to access one of the keys used as arguments")]
    NoPermissionKey,

    /// Permission denied for channel access.
    #[error("NOPERM this user has no permissions to access one of the channels used as arguments")]
    NoPermissionChannel,

    /// Parse error in ACL rule.
    #[error("ERR Error in ACL SETUSER modifier '{modifier}': {reason}")]
    ParseError { modifier: String, reason: String },

    /// Unknown ACL rule.
    #[error("ERR Unrecognized ACL rule: {rule}")]
    UnknownRule { rule: String },

    /// ACL file error.
    #[error("ERR Error loading ACL file: {message}")]
    FileError { message: String },

    /// ACL file not configured.
    #[error(
        "ERR This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (if you have a Redis config file) in order to store users in the Redis configuration."
    )]
    NoAclFile,

    /// Internal error.
    #[error("ERR {message}")]
    Internal { message: String },
}

impl AclError {
    /// Convert to bytes for RESP encoding.
    pub fn to_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from(self.to_string())
    }
}

impl From<crate::sync::LockError> for AclError {
    fn from(err: crate::sync::LockError) -> Self {
        AclError::Internal {
            message: format!("Lock error: {}", err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_messages() {
        let err = AclError::UserNotFound {
            username: "alice".to_string(),
        };
        assert_eq!(err.to_string(), "ERR User alice not found");

        let err = AclError::WrongPassword;
        assert_eq!(
            err.to_string(),
            "WRONGPASS invalid username-password pair or user is disabled."
        );

        let err = AclError::NoAuth;
        assert_eq!(err.to_string(), "NOAUTH Authentication required.");

        let err = AclError::NoPermissionCommand {
            command: "SET".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "NOPERM this user has no permissions to run the 'SET' command"
        );

        let err = AclError::ParseError {
            modifier: ">".to_string(),
            reason: "empty password".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "ERR Error in ACL SETUSER modifier '>': empty password"
        );
    }
}
