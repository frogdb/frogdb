//! Unified error type for FrogDB operations.
//!
//! `CommandError` and `RespError` are defined in `frogdb-types` and re-exported
//! from the crate root. This module provides `FrogDbError`, which unifies errors
//! from all core subsystems.

use bytes::Bytes;
use frogdb_protocol::Response;

// Re-export base error types from frogdb-types so `crate::error::CommandError` works
pub use frogdb_types::error::{CommandError, RespError};

// =============================================================================
// Unified Error Type
// =============================================================================

/// Unified error type for FrogDB operations.
///
/// This enum provides a single error type that can represent any error
/// that may occur during FrogDB operations, with consistent RESP conversion.
///
/// # Usage
///
/// ```rust,ignore
/// use frogdb_core::error::FrogDbError;
///
/// fn operation() -> Result<(), FrogDbError> {
///     // Errors from different subsystems automatically convert
///     let user = acl_manager.authenticate("user", "pass", "client")?;
///     let result = script_runner.execute(script)?;
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub enum FrogDbError {
    /// Command execution error.
    Command(CommandError),
    /// Scripting error.
    Script(crate::scripting::ScriptError),
    /// ACL/authentication error.
    Acl(crate::acl::error::AclError),
    /// Function/library error.
    Function(crate::functions::FunctionError),
    /// Cluster operation error.
    Cluster(crate::cluster::ClusterError),
    /// Lock poisoning error.
    Lock(frogdb_types::sync::LockError),
    /// JSON parsing/manipulation error (stored as string due to non-Clone inner type).
    Json(String),
    /// Persistence/storage error.
    Storage(String),
}

impl std::fmt::Display for FrogDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrogDbError::Command(e) => write!(f, "{}", e),
            FrogDbError::Script(e) => write!(f, "{}", e),
            FrogDbError::Acl(e) => write!(f, "{}", e),
            FrogDbError::Function(e) => write!(f, "{}", e),
            FrogDbError::Cluster(e) => write!(f, "{}", e),
            FrogDbError::Lock(e) => write!(f, "{}", e),
            FrogDbError::Json(msg) => write!(f, "{}", msg),
            FrogDbError::Storage(msg) => write!(f, "ERR storage error: {}", msg),
        }
    }
}

impl std::error::Error for FrogDbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FrogDbError::Command(e) => Some(e),
            FrogDbError::Script(e) => Some(e),
            FrogDbError::Acl(e) => Some(e),
            FrogDbError::Function(e) => Some(e),
            FrogDbError::Cluster(e) => Some(e),
            FrogDbError::Lock(e) => Some(e),
            FrogDbError::Json(_) => None,
            FrogDbError::Storage(_) => None,
        }
    }
}

impl FrogDbError {
    /// Convert to bytes for RESP encoding.
    pub fn to_bytes(&self) -> Bytes {
        match self {
            FrogDbError::Command(e) => e.to_bytes(),
            FrogDbError::Script(e) => e.to_bytes(),
            FrogDbError::Acl(e) => e.to_bytes(),
            FrogDbError::Function(e) => Bytes::from(e.to_string()),
            FrogDbError::Cluster(e) => Bytes::from(e.to_string()),
            FrogDbError::Lock(e) => Bytes::from(format!("ERR lock error: {}", e)),
            FrogDbError::Json(msg) => Bytes::from(msg.clone()),
            FrogDbError::Storage(msg) => Bytes::from(format!("ERR storage error: {}", msg)),
        }
    }

    /// Convert to RESP error response.
    pub fn to_response(&self) -> Response {
        Response::Error(self.to_bytes())
    }

    /// Create a storage error.
    pub fn storage(message: impl Into<String>) -> Self {
        FrogDbError::Storage(message.into())
    }

    /// Create a JSON error.
    pub fn json(message: impl Into<String>) -> Self {
        FrogDbError::Json(message.into())
    }
}

// =============================================================================
// From implementations for automatic conversion
// =============================================================================

impl From<CommandError> for FrogDbError {
    fn from(err: CommandError) -> Self {
        FrogDbError::Command(err)
    }
}

impl From<crate::scripting::ScriptError> for FrogDbError {
    fn from(err: crate::scripting::ScriptError) -> Self {
        FrogDbError::Script(err)
    }
}

impl From<crate::acl::error::AclError> for FrogDbError {
    fn from(err: crate::acl::error::AclError) -> Self {
        FrogDbError::Acl(err)
    }
}

impl From<crate::functions::FunctionError> for FrogDbError {
    fn from(err: crate::functions::FunctionError) -> Self {
        FrogDbError::Function(err)
    }
}

impl From<crate::cluster::ClusterError> for FrogDbError {
    fn from(err: crate::cluster::ClusterError) -> Self {
        FrogDbError::Cluster(err)
    }
}

impl From<frogdb_types::sync::LockError> for FrogDbError {
    fn from(err: frogdb_types::sync::LockError) -> Self {
        FrogDbError::Lock(err)
    }
}

impl From<frogdb_types::json::JsonError> for FrogDbError {
    fn from(err: frogdb_types::json::JsonError) -> Self {
        FrogDbError::Json(err.to_string())
    }
}

// Implement RespError trait
impl RespError for FrogDbError {
    fn to_bytes(&self) -> Bytes {
        FrogDbError::to_bytes(self)
    }

    fn to_response(&self) -> Response {
        FrogDbError::to_response(self)
    }
}
