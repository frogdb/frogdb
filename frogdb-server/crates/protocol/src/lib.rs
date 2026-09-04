//! FrogDB Protocol Layer
//!
//! This crate handles RESP2/RESP3 wire protocol parsing and encoding using the
//! `redis-protocol` crate. It provides the bridge between raw bytes and FrogDB's
//! internal command representation.

mod command;
mod error;
mod format;
mod limits;
mod reply;
mod response;
mod unknown_command;
mod version;

pub use command::{ParsedCommand, detach_bytes, detach_frame};
pub use error::ProtocolError;
pub use format::format_float;
pub use limits::{MAX_INTERNAL_FRAME_LEN, PROTO_MAX_BULK_LEN, PROTO_MAX_MULTIBULK_LEN};
pub use reply::MapReply;
pub use response::{
    BlockingOp, BytesFrame, Direction, InternalAction, RaftClusterOp, Response, SafeStatus,
    SlotMigrationKind, WireResponse, WireResult, sanitize_error_message,
};
pub use unknown_command::format_unknown_command_error;
pub use version::ProtocolVersion;

// Re-export RESP3 frame type for protocol-aware sending
pub use redis_protocol::resp3::types::BytesFrame as Resp3BytesFrame;
