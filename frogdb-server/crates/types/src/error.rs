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

    /// Command is recognized but intentionally not supported by FrogDB.
    NotSupported { command: &'static str, reason: &'static str } => "ERR command '{command}' is not supported by FrogDB: {reason}",

    /// Command requires multiple databases, which FrogDB does not support.
    DatabaseNotSupported { command: &'static str } => "ERR {command} is not supported. FrogDB uses a single database per instance.",

    // === Event Sourcing Errors ===
    /// Optimistic concurrency control version mismatch (ES.APPEND).
    VersionMismatch { expected: u64, actual: u64 } => "VERSIONMISMATCH expected {expected} actual {actual}",
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
    fn from(err: crate::types::StreamAddError) -> Self {
        match err {
            crate::types::StreamAddError::IdTooSmall => CommandError::InvalidArgument {
                message:
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
            },
            crate::types::StreamAddError::IdOverflow => CommandError::InvalidArgument {
                message: "The stream has exhausted the last possible ID, unable to add more items"
                    .to_string(),
            },
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
