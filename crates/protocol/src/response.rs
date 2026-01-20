//! Response types for RESP2/RESP3.

use bytes::Bytes;
use bytes_utils::Str;
use redis_protocol::resp2::types::BytesFrame;

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
