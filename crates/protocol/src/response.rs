//! Response types for RESP2/RESP3.

use bytes::Bytes;
use bytes_utils::Str;
use redis_protocol::resp2::types::BytesFrame;

/// Direction for list operations (BLPOP, BRPOP, BLMOVE, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Pop/push from the left (front).
    Left,
    /// Pop/push from the right (back).
    Right,
}

impl Direction {
    /// Parse a direction from a byte slice.
    pub fn parse(arg: &[u8]) -> Option<Self> {
        if arg.eq_ignore_ascii_case(b"LEFT") {
            Some(Direction::Left)
        } else if arg.eq_ignore_ascii_case(b"RIGHT") {
            Some(Direction::Right)
        } else {
            None
        }
    }
}

/// Blocking operation type for responses.
///
/// This is a simplified version for use in Response::BlockingNeeded.
/// The connection handler converts this to the full BlockingOp in frogdb_core.
#[derive(Debug, Clone, PartialEq)]
pub enum BlockingOp {
    /// BLPOP operation.
    BLPop,
    /// BRPOP operation.
    BRPop,
    /// BLMOVE operation.
    BLMove {
        /// Destination key.
        dest: Bytes,
        /// Source direction (where to pop from).
        src_dir: Direction,
        /// Destination direction (where to push to).
        dest_dir: Direction,
    },
    /// BLMPOP operation.
    BLMPop {
        /// Pop direction.
        direction: Direction,
        /// Number of elements to pop.
        count: usize,
    },
    /// BZPOPMIN operation.
    BZPopMin,
    /// BZPOPMAX operation.
    BZPopMax,
    /// BZMPOP operation.
    BZMPop {
        /// Whether to pop minimum (true) or maximum (false).
        min: bool,
        /// Number of elements to pop.
        count: usize,
    },
}

/// Response types that can be sent to clients.
///
/// Includes both RESP2 types (fully implemented) and RESP3 types
/// (defined for future use).
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    // === RESP2 Types (Implemented) ===
    /// Simple string (+OK\r\n)
    Simple(Bytes),

    /// Error (-ERR message\r\n)
    Error(Bytes),

    /// Integer (:1000\r\n)
    Integer(i64),

    /// Bulk string ($5\r\nhello\r\n) or null ($-1\r\n)
    Bulk(Option<Bytes>),

    /// Array (*2\r\n...)
    Array(Vec<Response>),

    // === RESP3 Types (Defined, Not Yet Serialized) ===
    /// Null (_\r\n)
    Null,

    /// Double (,3.14159\r\n)
    Double(f64),

    /// Boolean (#t\r\n or #f\r\n)
    Boolean(bool),

    /// Blob error (!<len>\r\n<bytes>\r\n)
    BlobError(Bytes),

    /// Verbatim string (=<len>\r\n<fmt>:<data>\r\n)
    VerbatimString {
        format: [u8; 3],
        data: Bytes,
    },

    /// Map (%<count>\r\n<key><value>...)
    Map(Vec<(Response, Response)>),

    /// Set (~<count>\r\n<elements>...)
    Set(Vec<Response>),

    /// Attribute (|<count>\r\n<attr-map><data>)
    Attribute(Box<Response>),

    /// Push (><count>\r\n<elements>...)
    Push(Vec<Response>),

    /// Big number ((<big-integer>\r\n)
    BigNumber(Bytes),

    // === Internal Types (Not Wire-Serialized) ===
    /// Signal that a blocking command needs to wait for data.
    /// This is intercepted by the connection handler and never sent on the wire.
    BlockingNeeded {
        /// Keys to wait on.
        keys: Vec<Bytes>,
        /// Timeout in seconds (0 = block forever).
        timeout: f64,
        /// The blocking operation to perform when data arrives.
        op: BlockingOp,
    },
}

impl Response {
    /// Create a simple "OK" response.
    pub fn ok() -> Self {
        Response::Simple(Bytes::from_static(b"OK"))
    }

    /// Create an error response.
    pub fn error(msg: impl Into<Bytes>) -> Self {
        Response::Error(msg.into())
    }

    /// Create a null bulk string response.
    pub fn null() -> Self {
        Response::Bulk(None)
    }

    /// Create a bulk string response.
    pub fn bulk(data: impl Into<Bytes>) -> Self {
        Response::Bulk(Some(data.into()))
    }

    /// Create a "PONG" response.
    pub fn pong() -> Self {
        Response::Simple(Bytes::from_static(b"PONG"))
    }

    /// Create a "QUEUED" response (for transactions).
    pub fn queued() -> Self {
        Response::Simple(Bytes::from_static(b"QUEUED"))
    }
}

impl From<Response> for BytesFrame {
    fn from(response: Response) -> Self {
        match response {
            Response::Simple(s) => BytesFrame::SimpleString(s),
            Response::Error(e) => {
                BytesFrame::Error(Str::from_inner(e).expect("error messages must be valid UTF-8"))
            }
            Response::Integer(i) => BytesFrame::Integer(i),
            Response::Bulk(Some(b)) => BytesFrame::BulkString(b),
            Response::Bulk(None) => BytesFrame::Null,
            Response::Array(items) => {
                BytesFrame::Array(items.into_iter().map(Into::into).collect())
            }
            // RESP3 types - implement in Phase 12
            Response::Null
            | Response::Double(_)
            | Response::Boolean(_)
            | Response::BlobError(_)
            | Response::VerbatimString { .. }
            | Response::Map(_)
            | Response::Set(_)
            | Response::Attribute(_)
            | Response::Push(_)
            | Response::BigNumber(_) => {
                unimplemented!("RESP3 encoding - implement in Phase 12")
            }
            // Internal types - should never reach serialization
            Response::BlockingNeeded { .. } => {
                panic!("BlockingNeeded response should be intercepted by connection handler")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_ok() {
        let resp = Response::ok();
        assert!(matches!(resp, Response::Simple(s) if s.as_ref() == b"OK"));
    }

    #[test]
    fn test_response_to_frame() {
        let resp = Response::Integer(42);
        let frame: BytesFrame = resp.into();
        assert!(matches!(frame, BytesFrame::Integer(42)));
    }

    #[test]
    fn test_response_null() {
        let resp = Response::null();
        let frame: BytesFrame = resp.into();
        assert!(matches!(frame, BytesFrame::Null));
    }
}
