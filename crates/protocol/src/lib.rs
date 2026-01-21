//! FrogDB Protocol Layer
//!
//! This crate handles RESP2/RESP3 wire protocol parsing and encoding using the
//! `redis-protocol` crate. It provides the bridge between raw bytes and FrogDB's
//! internal command representation.

mod command;
mod error;
mod response;
mod version;

pub use command::ParsedCommand;
pub use error::ProtocolError;
pub use response::{BlockingOp, Direction, Response};
pub use version::ProtocolVersion;
