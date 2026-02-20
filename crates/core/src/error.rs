//! Command execution errors.
//!
//! This module uses the `define_command_errors!` macro to define error variants
//! with a single source of truth for error messages, eliminating duplication
//! between Display impl and RESP encoding.

use bytes::Bytes;
use frogdb_protocol::Response;
use tracing::error;

/// Trait for error types that can be converted to RESP error responses.
///
/// This provides a unified interface for all FrogDB error types to convert
/// themselves to wire-format bytes or `Response` objects.
pub trait RespError: std::error::Error {
    /// Convert to bytes for RESP encoding.
    ///
    /// The default implementation uses the `Display` implementation.
    fn to_bytes(&self) -> Bytes {
        Bytes::from(self.to_string())
    }

    /// Convert to RESP error response.
    ///
    /// The default implementation wraps `to_bytes()` in `Response::Error`.
    fn to_response(&self) -> Response {
        Response::Error(self.to_bytes())
    }
}

/// Macro to define command errors with a single source of truth for error messages.
///
/// This macro generates:
/// - The enum variant definition
/// - Display implementation using the error message
/// - `to_bytes()` method that uses `Bytes::from_static()` for static messages
///
/// # Syntax
///
/// ```rust,ignore
/// define_command_errors! {
///     /// Documentation for the error.
///     VariantName => "static error message",
///
///     /// Documentation for error with fields.
///     VariantWithField { field: Type } => "ERR message with {field}",
/// }
/// ```
macro_rules! define_command_errors {
    (
        $(
            $(#[$meta:meta])*
            $variant:ident $({ $($field:ident : $field_ty:ty),* $(,)? })? => $msg:literal
        ),* $(,)?
    ) => {
        /// Core error variants for command execution.
        #[derive(Debug, Clone)]
        pub enum CommandError {
            $(
                $(#[$meta])*
                $variant $({ $($field : $field_ty),* })?,
            )*
        }

        impl std::fmt::Display for CommandError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        define_command_errors!(@pattern $variant $({ $($field),* })?) => {
                            write!(f, $msg $(, $($field = $field),*)?)
                        }
                    )*
                }
            }
        }

        impl std::error::Error for CommandError {}

        impl CommandError {
            /// Convert to bytes for RESP encoding.
            ///
            /// Uses `Bytes::from_static()` for static messages to avoid allocation.
            pub fn to_bytes(&self) -> Bytes {
                match self {
                    $(
                        define_command_errors!(@pattern $variant $({ $($field),* })?) => {
                            define_command_errors!(@to_bytes $msg $(, $($field),*)?)
                        }
                    )*
                }
            }
        }
    };

    // Helper to generate the match pattern
    (@pattern $variant:ident) => {
        Self::$variant
    };
    (@pattern $variant:ident { $($field:ident),* }) => {
        Self::$variant { $($field),* }
    };

    // Helper to generate Bytes - static version (no format args)
    (@to_bytes $msg:literal) => {
        Bytes::from_static($msg.as_bytes())
    };
    // Helper to generate Bytes - dynamic version (with format args)
    (@to_bytes $msg:literal, $($field:ident),+) => {
        Bytes::from(format!($msg, $($field = $field),+))
    };
}

define_command_errors! {
    // === Syntax/Argument Errors ===
    /// Wrong number of arguments for command.
    WrongArity { command: &'static str } => "ERR wrong number of arguments for '{command}' command",

    /// Invalid argument value or format.
    InvalidArgument { message: String } => "ERR {message}",

    /// General syntax error.
    SyntaxError => "ERR syntax error",

    /// Unknown command.
    UnknownCommand { name: String } => "ERR unknown command '{name}', with args beginning with:",

    // === Type Errors ===
    /// Operation against wrong value type.
    WrongType => "WRONGTYPE Operation against a key holding the wrong kind of value",

    /// Value is not an integer or out of range.
    NotInteger => "ERR value is not an integer or out of range",

    /// Value is not a valid float.
    NotFloat => "ERR value is not a valid float",

    // === Routing Errors ===
    /// Keys in request don't hash to the same slot.
    CrossSlot => "CROSSSLOT Keys in request don't hash to the same slot",

    // === Cluster Errors ===
    /// Cluster mode is not enabled.
    ClusterDisabled => "ERR This instance has cluster support disabled",

    /// Cluster command requires being the leader.
    ClusterDown => "CLUSTERDOWN The cluster is down",

    // === Replication Errors ===
    /// Replication is not enabled.
    ReplicationDisabled => "ERR This instance has replication support disabled",

    /// Wrong number of arguments for a dynamic command string.
    WrongArgCount { command: String } => "ERR wrong number of arguments for '{command}' command",

    // === Key State Errors ===
    /// Target key already exists (for RESTORE without REPLACE).
    BusyKey => "BUSYKEY Target key name already exists",

    // === Stream Errors ===
    /// Consumer group already exists.
    BusyGroup => "BUSYGROUP Consumer Group name already exists",

    /// Consumer group doesn't exist.
    NoGroup => "NOGROUP No such consumer group",

    // === System Errors ===
    /// Out of memory.
    OutOfMemory => "OOM command not allowed when used memory > 'maxmemory'",

    /// Internal server error.
    Internal { message: String } => "ERR {message}",

    /// Command is recognized but not yet implemented.
    NotImplemented { command: &'static str } => "ERR command '{command}' is not yet implemented",
}

impl CommandError {
    /// Create an internal error with logging.
    ///
    /// This should be used for unexpected internal errors that indicate a bug
    /// or system issue, not for user-facing errors.
    pub fn internal(cmd: &str, message: impl Into<String>) -> Self {
        let message = message.into();
        error!(command = cmd, error = %message, "Command internal error");
        Self::Internal { message }
    }

    /// Convert to RESP error response.
    pub fn to_response(&self) -> Response {
        Response::Error(self.to_bytes())
    }
}

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
    Lock(crate::sync::LockError),
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

impl From<crate::sync::LockError> for FrogDbError {
    fn from(err: crate::sync::LockError) -> Self {
        FrogDbError::Lock(err)
    }
}

impl From<crate::json::JsonError> for FrogDbError {
    fn from(err: crate::json::JsonError) -> Self {
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

// =============================================================================
// From implementations for type-specific errors to CommandError
// =============================================================================

impl From<crate::types::IncrementError> for CommandError {
    fn from(err: crate::types::IncrementError) -> Self {
        match err {
            crate::types::IncrementError::NotInteger => CommandError::NotInteger,
            crate::types::IncrementError::NotFloat => CommandError::NotFloat,
            crate::types::IncrementError::Overflow => CommandError::InvalidArgument {
                message: "increment or decrement would overflow".to_string(),
            },
        }
    }
}

impl From<crate::types::StreamIdParseError> for CommandError {
    fn from(_err: crate::types::StreamIdParseError) -> Self {
        CommandError::InvalidArgument {
            message: "Invalid stream ID specified as stream command argument".to_string(),
        }
    }
}

impl From<crate::types::StreamAddError> for CommandError {
    fn from(_err: crate::types::StreamAddError) -> Self {
        CommandError::InvalidArgument {
            message: "The ID specified in XADD is equal or smaller than the target stream top item"
                .to_string(),
        }
    }
}

impl From<crate::types::StreamGroupError> for CommandError {
    fn from(err: crate::types::StreamGroupError) -> Self {
        match err {
            crate::types::StreamGroupError::GroupExists => CommandError::BusyGroup,
            crate::types::StreamGroupError::NoGroup => CommandError::NoGroup,
        }
    }
}
