//! Protocol version handling.

/// Protocol version negotiated per connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    /// RESP2 - default protocol version.
    #[default]
    Resp2,

    /// RESP3 - negotiated via HELLO command.
    Resp3,
}

impl ProtocolVersion {
    /// Check if this is RESP3.
    pub fn is_resp3(&self) -> bool {
        matches!(self, ProtocolVersion::Resp3)
    }
}
