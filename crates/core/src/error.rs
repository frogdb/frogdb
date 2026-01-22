//! Command execution errors.

use bytes::Bytes;
use frogdb_protocol::Response;
use thiserror::Error;

/// Core error variants for command execution.
#[derive(Debug, Clone, Error)]
pub enum CommandError {
    // === Syntax/Argument Errors ===
    /// Wrong number of arguments for command.
    #[error("ERR wrong number of arguments for '{command}' command")]
    WrongArity { command: &'static str },

    /// Invalid argument value or format.
    #[error("ERR {message}")]
    InvalidArgument { message: String },

    /// General syntax error.
    #[error("ERR syntax error")]
    SyntaxError,

    /// Unknown command.
    #[error("ERR unknown command '{name}'")]
    UnknownCommand { name: String },

    // === Type Errors ===
    /// Operation against wrong value type.
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    /// Value is not an integer or out of range.
    #[error("ERR value is not an integer or out of range")]
    NotInteger,

    /// Value is not a valid float.
    #[error("ERR value is not a valid float")]
    NotFloat,

    // === Routing Errors ===
    /// Keys in request don't hash to the same slot.
    #[error("CROSSSLOT Keys in request don't hash to the same slot")]
    CrossSlot,

    // === Key State Errors ===
    /// Target key already exists (for RESTORE without REPLACE).
    #[error("BUSYKEY Target key name already exists")]
    BusyKey,

    // === Stream Errors ===
    /// Consumer group already exists.
    #[error("BUSYGROUP Consumer Group name already exists")]
    BusyGroup,

    /// Consumer group doesn't exist.
    #[error("NOGROUP No such consumer group")]
    NoGroup,

    // === System Errors ===
    /// Out of memory.
    #[error("OOM command not allowed when used memory > 'maxmemory'")]
    OutOfMemory,

    /// Internal server error.
    #[error("ERR {message}")]
    Internal { message: String },

    /// Command is recognized but not yet implemented.
    #[error("ERR command '{command}' is not yet implemented")]
    NotImplemented { command: &'static str },
}

impl CommandError {
    /// Convert to RESP error response.
    pub fn to_response(&self) -> Response {
        Response::Error(self.to_bytes())
    }

    /// Convert to bytes for RESP encoding.
    pub fn to_bytes(&self) -> Bytes {
        match self {
            Self::WrongArity { command } => {
                Bytes::from(format!("ERR wrong number of arguments for '{}' command", command))
            }
            Self::InvalidArgument { message } => Bytes::from(format!("ERR {}", message)),
            Self::SyntaxError => Bytes::from_static(b"ERR syntax error"),
            Self::UnknownCommand { name } => {
                Bytes::from(format!("ERR unknown command '{}', with args beginning with:", name))
            }
            Self::WrongType => {
                Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            Self::NotInteger => {
                Bytes::from_static(b"ERR value is not an integer or out of range")
            }
            Self::NotFloat => Bytes::from_static(b"ERR value is not a valid float"),
            Self::CrossSlot => {
                Bytes::from_static(b"CROSSSLOT Keys in request don't hash to the same slot")
            }
            Self::BusyKey => Bytes::from_static(b"BUSYKEY Target key name already exists"),
            Self::BusyGroup => {
                Bytes::from_static(b"BUSYGROUP Consumer Group name already exists")
            }
            Self::NoGroup => Bytes::from_static(b"NOGROUP No such consumer group"),
            Self::OutOfMemory => {
                Bytes::from_static(b"OOM command not allowed when used memory > 'maxmemory'")
            }
            Self::Internal { message } => Bytes::from(format!("ERR {}", message)),
            Self::NotImplemented { command } => {
                Bytes::from(format!("ERR command '{}' is not yet implemented", command))
            }
        }
    }
}
