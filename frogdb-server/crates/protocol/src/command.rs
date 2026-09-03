//! Parsed command representation.

use bytes::Bytes;
use redis_protocol::resp2::types::BytesFrame;

use crate::ProtocolError;

/// Return a `Bytes` guaranteed not to alias a shared buffer.
///
/// Command arguments are zero-copy slices of the connection's pooled read
/// buffer (see [`ParsedCommand::try_from`]). That is safe while the command
/// executes on its home core, but a reference that outlives the immediate
/// execution — crossing to another core, parking with a blocking command,
/// being queued by MULTI, or being installed into the keyspace — would pin
/// the network buffer and let a foreign thread hold memory owned by another
/// core's pool. Callers at those escape points run their `Bytes` through
/// here.
///
/// `is_unique()` is the discriminator: the connection's `Framed` codec
/// co-holds the read-buffer allocation for as long as it is live, so a
/// `Bytes` that aliases it is never unique. A unique `Bytes` is already
/// privately owned and passes through untouched — internal callers
/// (replication apply, recovery, tests) pay nothing.
#[inline]
pub fn detach_bytes(bytes: Bytes) -> Bytes {
    if bytes.is_unique() {
        bytes
    } else {
        Bytes::copy_from_slice(&bytes)
    }
}

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

    /// Copy any name/argument that still aliases a shared buffer, in place.
    ///
    /// See [`detach_bytes`] for when this is required. Arguments that are
    /// already privately owned are left untouched, so calling this on an
    /// already-detached command is free.
    pub fn detach(&mut self) {
        let name = std::mem::take(&mut self.name);
        self.name = detach_bytes(name);
        for arg in &mut self.args {
            let bytes = std::mem::take(arg);
            *arg = detach_bytes(bytes);
        }
    }

    /// Like [`Self::detach`], but returns a detached clone and leaves `self`
    /// untouched. For callers holding `Arc<ParsedCommand>`.
    pub fn detached(&self) -> Self {
        Self {
            name: detach_bytes(self.name.clone()),
            args: self
                .args
                .iter()
                .map(|arg| detach_bytes(arg.clone()))
                .collect(),
        }
    }
}

/// Extract the payload `Bytes` from a frame without copying.
///
/// Mirrors [`redis_protocol::resp2::types::Resp2Frame::as_bytes`]'s variant
/// coverage exactly (bulk strings, simple strings, errors) so the zero-copy
/// path accepts and rejects the same frames the copying path did.
#[inline]
fn frame_into_bytes(frame: BytesFrame) -> Option<Bytes> {
    match frame {
        BytesFrame::BulkString(b) | BytesFrame::SimpleString(b) => Some(b),
        BytesFrame::Error(s) => Some(s.into_inner()),
        _ => None,
    }
}

impl TryFrom<BytesFrame> for ParsedCommand {
    type Error = ProtocolError;

    /// Convert a decoded frame into a command **without copying**.
    ///
    /// The returned name and arguments are refcounted slices of whatever
    /// buffer the frame was decoded from — for the network path, the
    /// connection's pooled read buffer. That buffer cannot be recycled while
    /// any argument is alive, so any reference that outlives the command's
    /// immediate execution must first be copied out via
    /// [`ParsedCommand::detach`] / [`detach_bytes`].
    fn try_from(frame: BytesFrame) -> Result<Self, Self::Error> {
        match frame {
            BytesFrame::Array(frames) => {
                let mut iter = frames.into_iter();

                let name = iter
                    .next()
                    .ok_or(ProtocolError::EmptyCommand)
                    .and_then(|f| frame_into_bytes(f).ok_or(ProtocolError::InvalidFrame))?;

                let args: Vec<Bytes> = iter.filter_map(frame_into_bytes).collect();

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

    /// The whole point of the zero-copy path: parsed args must share the
    /// decode buffer's allocation, not copy it.
    #[test]
    fn test_try_from_is_zero_copy() {
        let buffer = Bytes::from(b"SET somekey somevalue".to_vec());
        let name = buffer.slice(0..3);
        let key = buffer.slice(4..11);
        let value = buffer.slice(12..21);
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(name.clone()),
            BytesFrame::BulkString(key.clone()),
            BytesFrame::BulkString(value.clone()),
        ]);

        let cmd = ParsedCommand::try_from(frame).unwrap();

        // Pointer equality proves the args alias the original allocation.
        assert_eq!(cmd.name.as_ptr(), name.as_ptr());
        assert_eq!(cmd.args[0].as_ptr(), key.as_ptr());
        assert_eq!(cmd.args[1].as_ptr(), value.as_ptr());
        // And the source buffer is demonstrably still co-owned.
        assert!(!buffer.is_unique());
    }

    // FM-MEMORY-003
    #[test]
    fn test_detach_bytes_copies_only_when_shared() {
        let buffer = Bytes::from(b"shared-backing-buffer".to_vec());
        let slice = buffer.slice(0..6);
        let detached = detach_bytes(slice);
        assert_eq!(detached.as_ref(), b"shared");
        // Copied: no longer points into the shared allocation.
        assert_ne!(detached.as_ptr(), buffer.as_ptr());

        let owned = Bytes::from(b"private".to_vec());
        let ptr = owned.as_ptr();
        let detached = detach_bytes(owned);
        // Unique: passed through untouched.
        assert_eq!(detached.as_ptr(), ptr);
    }

    // FM-MEMORY-003
    #[test]
    fn test_command_detach_releases_source_buffer() {
        let buffer = Bytes::from(b"GET mykey".to_vec());
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(buffer.slice(0..3)),
            BytesFrame::BulkString(buffer.slice(4..9)),
        ]);
        let mut cmd = ParsedCommand::try_from(frame).unwrap();
        assert!(!buffer.is_unique());

        cmd.detach();

        // Every reference into the original allocation is gone.
        assert!(buffer.is_unique());
        assert_eq!(cmd.name.as_ref(), b"GET");
        assert_eq!(cmd.args[0].as_ref(), b"mykey");
    }

    #[test]
    fn test_detached_clone_releases_source_buffer_when_original_drops() {
        let buffer = Bytes::from(b"GET mykey".to_vec());
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(buffer.slice(0..3)),
            BytesFrame::BulkString(buffer.slice(4..9)),
        ]);
        let cmd = ParsedCommand::try_from(frame).unwrap();

        let detached = cmd.detached();
        drop(cmd);

        // The detached copy holds nothing from the original allocation.
        assert!(buffer.is_unique());
        assert_eq!(detached.name.as_ref(), b"GET");
        assert_eq!(detached.args[0].as_ref(), b"mykey");
    }

    /// Error frames carry their payload as a `Str`; the zero-copy extractor
    /// must keep accepting them, matching `Resp2Frame::as_bytes` coverage.
    #[test]
    fn test_try_from_accepts_simple_string_and_error_frames() {
        let frame = BytesFrame::Array(vec![
            BytesFrame::SimpleString(Bytes::from_static(b"PING")),
            BytesFrame::Error("oops".into()),
        ]);
        let cmd = ParsedCommand::try_from(frame).unwrap();
        assert_eq!(cmd.name.as_ref(), b"PING");
        assert_eq!(cmd.args[0].as_ref(), b"oops");
    }
}
