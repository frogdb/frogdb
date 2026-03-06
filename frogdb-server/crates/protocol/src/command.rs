//! Parsed command representation.

use bytes::Bytes;
use redis_protocol::resp2::types::{BytesFrame, Resp2Frame};

use crate::ProtocolError;

/// A parsed Redis command with name and arguments.
///
/// Commands are received as RESP arrays and converted to this internal
/// representation for easier processing.
#[derive(Debug, Clone)]
pub struct ParsedCommand {
    /// Command name (e.g., "GET", "SET")
    pub name: Bytes,
    /// Command arguments
    pub args: Vec<Bytes>,
}

impl ParsedCommand {
    /// Create a new parsed command.
    pub fn new(name: Bytes, args: Vec<Bytes>) -> Self {
        Self { name, args }
    }

    /// Get the command name as uppercase bytes for lookup.
    pub fn name_uppercase(&self) -> Vec<u8> {
        self.name.to_ascii_uppercase()
    }

    /// Get the command name as an uppercase `String`.
    ///
    /// Performs ASCII uppercase conversion and UTF-8 conversion in one step.
    /// More efficient than `String::from_utf8_lossy(&self.name).to_uppercase()`
    /// since command names are always ASCII.
    pub fn name_uppercase_string(&self) -> String {
        // SAFETY: Redis command names are always ASCII, so to_ascii_uppercase
        // produces valid UTF-8. from_utf8 is infallible here but we use
        // the unchecked variant to avoid the redundant validation.
        let bytes = self.name.to_ascii_uppercase();
        // Command names come from the wire and are validated ASCII;
        // use from_utf8_lossy for safety against malformed input.
        String::from_utf8(bytes)
            .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
    }
}

impl TryFrom<BytesFrame> for ParsedCommand {
    type Error = ProtocolError;

    fn try_from(frame: BytesFrame) -> Result<Self, Self::Error> {
        match frame {
            BytesFrame::Array(frames) => {
                let mut iter = frames.into_iter();

                let name = iter
                    .next()
                    .ok_or(ProtocolError::EmptyCommand)?
                    .as_bytes()
                    .map(Bytes::copy_from_slice)
                    .ok_or(ProtocolError::InvalidFrame)?;

                let args: Vec<Bytes> = iter
                    .filter_map(|f| f.as_bytes().map(Bytes::copy_from_slice))
                    .collect();

                Ok(ParsedCommand { name, args })
            }
            _ => Err(ProtocolError::ExpectedArray),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_command() {
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from_static(b"GET")),
            BytesFrame::BulkString(Bytes::from_static(b"mykey")),
        ]);

        let cmd = ParsedCommand::try_from(frame).unwrap();
        assert_eq!(cmd.name.as_ref(), b"GET");
        assert_eq!(cmd.args.len(), 1);
        assert_eq!(cmd.args[0].as_ref(), b"mykey");
    }

    #[test]
    fn test_parse_empty_array_fails() {
        let frame = BytesFrame::Array(vec![]);
        let result = ParsedCommand::try_from(frame);
        assert!(matches!(result, Err(ProtocolError::EmptyCommand)));
    }

    #[test]
    fn test_parse_non_array_fails() {
        let frame = BytesFrame::SimpleString(Bytes::from_static(b"PING"));
        let result = ParsedCommand::try_from(frame);
        assert!(matches!(result, Err(ProtocolError::ExpectedArray)));
    }
}
