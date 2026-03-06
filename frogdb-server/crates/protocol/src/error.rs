//! Protocol error types.

use thiserror::Error;

/// Errors that can occur during RESP parsing.
#[derive(Debug, Error, Clone)]
pub enum ProtocolError {
    /// Command array was empty (no command name).
    #[error("empty command")]
    EmptyCommand,

    /// Expected an array frame for command.
    #[error("commands must be arrays")]
    ExpectedArray,

    /// Frame was malformed or invalid.
    #[error("invalid frame")]
    InvalidFrame,

    /// Incomplete frame (need more data).
    #[error("incomplete frame")]
    Incomplete,

    /// Frame exceeded maximum size.
    #[error("frame too large")]
    FrameTooLarge,
}
