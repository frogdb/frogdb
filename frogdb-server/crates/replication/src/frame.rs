//! Replication frame encoding and decoding.
//!
//! Frames are the unit of data transferred during WAL streaming.
//! Each frame contains a sequence of WAL entries.
//!
//! # Frame Format
//!
//! ```text
//! +--------+--------+--------+--------+----------+----------+-------------+
//! | Magic  | Version| Flags  | Shard  | Sequence | Length   | Payload     |
//! | 4 bytes| 1 byte | 1 byte | 2 bytes| 8 bytes  | 4 bytes  | Length bytes|
//! +--------+--------+--------+--------+----------+----------+-------------+
//! ```
//!
//! - Magic: `FRPL` (0x4652504C) - identifies FrogDB replication frames
//! - Version: Protocol version (currently 2)
//! - Flags: Reserved for future use
//! - Shard: Origin shard id — the shard on which the write executed on the
//!   primary. The replica applies the frame on *this* shard instead of
//!   re-deriving routing from `args[0]` (which is wrong for keyless commands
//!   and MULTI/EXEC framing). [`CONTROL_SHARD`] tags control/global frames
//!   (GETACK, etc.) that are never routed to a shard on the replica.
//! - Sequence: WAL sequence number
//! - Length: Payload length in bytes
//! - Payload: Serialized WAL operations

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Serialize a command to RESP format for replication.
///
/// This converts a command name and arguments into the RESP wire protocol format
/// that replicas can parse and execute.
///
/// # Arguments
/// * `cmd_name` - The command name (e.g., "SET")
/// * `args` - The command arguments
///
/// # Returns
/// The serialized RESP bytes
///
/// # Example
/// ```ignore
/// let resp = serialize_command_to_resp("SET", &[Bytes::from("key"), Bytes::from("value")]);
/// // Returns: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
/// ```
pub fn serialize_command_to_resp(cmd_name: &str, args: &[Bytes]) -> Bytes {
    let total_elements = 1 + args.len();

    // Estimate capacity: array header + command + args
    // Each element has: $<len>\r\n<data>\r\n
    let estimated_size = 16 + cmd_name.len() + args.iter().map(|a| 16 + a.len()).sum::<usize>();
    let mut buf = BytesMut::with_capacity(estimated_size);

    // RESP array header
    buf.extend_from_slice(format!("*{}\r\n", total_elements).as_bytes());

    // Command name as bulk string
    buf.extend_from_slice(format!("${}\r\n", cmd_name.len()).as_bytes());
    buf.extend_from_slice(cmd_name.as_bytes());
    buf.extend_from_slice(b"\r\n");

    // Arguments as bulk strings
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }

    buf.freeze()
}

/// The REPLCONF ACK / GETACK control-message grammar, as one symmetric codec.
///
/// Single definition of the wire shapes previously scattered across
/// `request_acks` (GETACK encode), `send_ack` (ACK encode), `parse_replconf_ack`
/// (ACK decode) and `is_getack_frame` (GETACK decode). The encode side composes
/// the crate's [`serialize_command_to_resp`]; the decode side owns the parsers.
/// Framing only — offset stamping, frame headers, and backlog recording stay in
/// the callers (`OffsetCoordinator` / [`ReplicationFrameCodec`] / `replay`).
///
/// This is the ACK/GETACK analogue of [`crate::fullsync::CheckpointStreamCodec`]:
/// a wire grammar that was realized by hand across several call sites, collapsed
/// to one owner with a golden round-trip test binding each encoder to its
/// inverse.
pub(crate) struct ReplconfCodec;

impl ReplconfCodec {
    // --- encode (delegates to serialize_command_to_resp) ---

    /// `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n`
    ///
    /// The offset is emitted as decimal ASCII (`offset.to_string()`), so its
    /// `$<len>` bulk-string prefix reflects the digit count (20 for `u64::MAX`).
    pub(crate) fn encode_ack(offset: u64) -> Bytes {
        serialize_command_to_resp(
            "REPLCONF",
            &[Bytes::from_static(b"ACK"), Bytes::from(offset.to_string())],
        )
    }

    /// `*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n`
    pub(crate) fn encode_getack() -> Bytes {
        serialize_command_to_resp(
            "REPLCONF",
            &[Bytes::from_static(b"GETACK"), Bytes::from_static(b"*")],
        )
    }

    // --- decode (inverses) ---

    /// Parse a leading REPLCONF ACK frame from a (possibly streaming) buffer.
    ///
    /// Returns `Some((offset, consumed))` on a complete, valid frame; `None` if
    /// the buffer is incomplete or does not hold a REPLCONF ACK. The `consumed`
    /// return is load-bearing for the primary's streaming read loop
    /// (`replica_session.rs`, `buf.advance(consumed)`).
    ///
    /// Expected wire format: `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n`
    pub(crate) fn parse_ack(data: &[u8]) -> Option<(u64, usize)> {
        use redis_protocol::resp2::decode::decode;
        use redis_protocol::resp2::types::{OwnedFrame, Resp2Frame};

        let (frame, consumed) = decode(data).ok()??;
        if let OwnedFrame::Array(parts) = frame
            && parts.len() >= 3
        {
            let is_replconf = parts[0]
                .as_bytes()
                .is_some_and(|b: &[u8]| b.eq_ignore_ascii_case(b"REPLCONF"));
            let is_ack = parts[1]
                .as_bytes()
                .is_some_and(|b: &[u8]| b.eq_ignore_ascii_case(b"ACK"));
            if is_replconf && is_ack {
                let offset_str = std::str::from_utf8(parts[2].as_bytes()?).ok()?;
                let offset = offset_str.parse::<u64>().ok()?;
                return Some((offset, consumed));
            }
        }
        None
    }

    /// Structural fast-path: true iff `payload` is a `REPLCONF GETACK *`
    /// solicitation (`*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n...`).
    ///
    /// Matched structurally (case-insensitive tokens) rather than by full RESP
    /// decode: this runs once per ingested frame on the replica hot path, and
    /// the solicitation is the only REPLCONF the primary puts on the stream.
    pub(crate) fn is_getack(payload: &[u8]) -> bool {
        let Some(rest) = payload.strip_prefix(b"*3\r\n$8\r\n") else {
            return false;
        };
        if rest.len() < 8 + 6 + 6 {
            return false;
        }
        let (name, rest) = rest.split_at(8);
        if !name.eq_ignore_ascii_case(b"REPLCONF") {
            return false;
        }
        let Some(rest) = rest.strip_prefix(b"\r\n$6\r\n") else {
            return false;
        };
        rest.len() >= 6 && rest[..6].eq_ignore_ascii_case(b"GETACK")
    }
}

/// Frame magic bytes: "FRPL"
pub const FRAME_MAGIC: [u8; 4] = [0x46, 0x52, 0x50, 0x4C]; // "FRPL"

/// Current frame protocol version.
///
/// Bumped to 2 when the origin-shard tag was added to the header (see the
/// module-level frame format).
pub const FRAME_VERSION: u8 = 2;

/// Frame header size in bytes
pub const FRAME_HEADER_SIZE: usize = 20; // 4 + 1 + 1 + 2 + 8 + 4

/// Sentinel origin-shard for control/global frames that are not routed to a
/// shard on the replica (e.g. `REPLCONF GETACK`). Any frame carrying this shard
/// id is a control frame; the consumer handles it without shard routing.
pub const CONTROL_SHARD: u16 = u16::MAX;

/// Maximum frame payload size (64 MB)
pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

/// Frame flags
#[derive(Debug, Clone, Copy, Default)]
pub struct FrameFlags(u8);

impl FrameFlags {
    /// No flags set
    pub const NONE: Self = Self(0);

    /// Frame contains compressed payload
    pub const COMPRESSED: Self = Self(1 << 0);

    /// Frame is the last in a batch
    pub const END_OF_BATCH: Self = Self(1 << 1);

    /// Frame requires acknowledgment
    pub const REQUIRE_ACK: Self = Self(1 << 2);

    /// Check if a flag is set
    pub fn contains(&self, flag: Self) -> bool {
        (self.0 & flag.0) == flag.0
    }

    /// Set a flag
    pub fn set(&mut self, flag: Self) {
        self.0 |= flag.0;
    }

    /// Get raw value
    pub fn bits(&self) -> u8 {
        self.0
    }

    /// Create from raw value
    pub fn from_bits(bits: u8) -> Self {
        Self(bits)
    }
}

/// A replication frame containing WAL data.
#[derive(Debug, Clone)]
pub struct ReplicationFrame {
    /// Protocol version
    pub version: u8,

    /// Frame flags
    pub flags: FrameFlags,

    /// Origin shard — the shard the write executed on at the primary. The
    /// replica applies the frame on this shard instead of re-deriving routing
    /// from `args[0]`. [`CONTROL_SHARD`] marks a control/global frame.
    pub shard_id: u16,

    /// WAL sequence number
    pub sequence: u64,

    /// Payload data (serialized WAL operations)
    pub payload: Bytes,
}

impl ReplicationFrame {
    /// Create a new control/global replication frame (shard = [`CONTROL_SHARD`]).
    ///
    /// Use [`Self::new_on_shard`] for a data frame that must carry the origin
    /// shard where the write executed.
    pub fn new(sequence: u64, payload: Bytes) -> Self {
        Self::new_on_shard(sequence, CONTROL_SHARD, payload)
    }

    /// Create a new data frame tagged with the shard the write executed on.
    pub fn new_on_shard(sequence: u64, shard_id: u16, payload: Bytes) -> Self {
        Self {
            version: FRAME_VERSION,
            flags: FrameFlags::NONE,
            shard_id,
            sequence,
            payload,
        }
    }

    /// Create a frame with flags (shard = [`CONTROL_SHARD`]).
    pub fn with_flags(sequence: u64, payload: Bytes, flags: FrameFlags) -> Self {
        Self {
            version: FRAME_VERSION,
            flags,
            shard_id: CONTROL_SHARD,
            sequence,
            payload,
        }
    }

    /// Encode frame to bytes.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(FRAME_HEADER_SIZE + self.payload.len());

        buf.put_slice(&FRAME_MAGIC);
        buf.put_u8(self.version);
        buf.put_u8(self.flags.bits());
        buf.put_u16(self.shard_id);
        buf.put_u64(self.sequence);
        buf.put_u32(self.payload.len() as u32);
        buf.put_slice(&self.payload);

        buf.freeze()
    }

    /// Decode frame from bytes.
    pub fn decode(mut buf: Bytes) -> Result<Self, FrameDecodeError> {
        if buf.len() < FRAME_HEADER_SIZE {
            return Err(FrameDecodeError::InsufficientData);
        }

        // Check magic
        let magic = buf.copy_to_bytes(4);
        if magic.as_ref() != FRAME_MAGIC {
            return Err(FrameDecodeError::InvalidMagic);
        }

        let version = buf.get_u8();
        if version > FRAME_VERSION {
            return Err(FrameDecodeError::UnsupportedVersion(version));
        }

        let flags = FrameFlags::from_bits(buf.get_u8());
        let shard_id = buf.get_u16();
        let sequence = buf.get_u64();
        let length = buf.get_u32() as usize;

        if length > MAX_FRAME_SIZE {
            return Err(FrameDecodeError::PayloadTooLarge(length));
        }

        if buf.len() < length {
            return Err(FrameDecodeError::InsufficientData);
        }

        let payload = buf.copy_to_bytes(length);

        Ok(Self {
            version,
            flags,
            shard_id,
            sequence,
            payload,
        })
    }

    /// Get the total size of the encoded frame.
    pub fn encoded_size(&self) -> usize {
        FRAME_HEADER_SIZE + self.payload.len()
    }

    /// The replication-offset advance unit: RESP payload bytes only, never the
    /// 20-byte transport header ([`FRAME_HEADER_SIZE`]: magic 4 + version 1 +
    /// flags 1 + shard 2 + sequence 8 + length 4). This is the one definition
    /// both ends count by — the primary's advance gate and the replica's ingest
    /// path — so an ACK stays directly comparable to the live offset.
    #[inline]
    pub fn stream_advance(&self) -> u64 {
        self.payload.len() as u64
    }
}

/// Errors that can occur during frame decoding.
#[derive(Debug, Clone, thiserror::Error)]
pub enum FrameDecodeError {
    #[error("insufficient data for frame")]
    InsufficientData,

    #[error("invalid frame magic")]
    InvalidMagic,

    #[error("unsupported frame version: {0}")]
    UnsupportedVersion(u8),

    #[error("payload too large: {0} bytes")]
    PayloadTooLarge(usize),

    #[error("IO error: {0}")]
    Io(String),
}

impl From<io::Error> for FrameDecodeError {
    fn from(err: io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

/// Tokio codec for encoding/decoding replication frames.
#[derive(Debug, Default)]
pub struct ReplicationFrameCodec {
    /// State for partial frame decoding
    state: DecodeState,
}

#[derive(Debug, Default)]
enum DecodeState {
    #[default]
    ReadingHeader,
    ReadingPayload {
        version: u8,
        flags: FrameFlags,
        shard_id: u16,
        sequence: u64,
        length: usize,
    },
}

impl ReplicationFrameCodec {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Decoder for ReplicationFrameCodec {
    type Item = ReplicationFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match &self.state {
                DecodeState::ReadingHeader => {
                    if src.len() < FRAME_HEADER_SIZE {
                        return Ok(None);
                    }

                    // Check magic
                    if src[0..4] != FRAME_MAGIC {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid frame magic",
                        ));
                    }

                    let version = src[4];
                    if version > FRAME_VERSION {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unsupported frame version: {}", version),
                        ));
                    }

                    let flags = FrameFlags::from_bits(src[5]);
                    let shard_id = u16::from_be_bytes(src[6..8].try_into().unwrap());
                    let sequence = u64::from_be_bytes(src[8..16].try_into().unwrap());
                    let length = u32::from_be_bytes(src[16..20].try_into().unwrap()) as usize;

                    if length > MAX_FRAME_SIZE {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("payload too large: {} bytes", length),
                        ));
                    }

                    // Advance past header
                    src.advance(FRAME_HEADER_SIZE);

                    self.state = DecodeState::ReadingPayload {
                        version,
                        flags,
                        shard_id,
                        sequence,
                        length,
                    };
                }
                DecodeState::ReadingPayload {
                    version,
                    flags,
                    shard_id,
                    sequence,
                    length,
                } => {
                    if src.len() < *length {
                        return Ok(None);
                    }

                    let payload = src.split_to(*length).freeze();
                    let frame = ReplicationFrame {
                        version: *version,
                        flags: *flags,
                        shard_id: *shard_id,
                        sequence: *sequence,
                        payload,
                    };

                    self.state = DecodeState::ReadingHeader;
                    return Ok(Some(frame));
                }
            }
        }
    }
}

impl Encoder<ReplicationFrame> for ReplicationFrameCodec {
    type Error = io::Error;

    fn encode(&mut self, item: ReplicationFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(FRAME_HEADER_SIZE + item.payload.len());

        dst.put_slice(&FRAME_MAGIC);
        dst.put_u8(item.version);
        dst.put_u8(item.flags.bits());
        dst.put_u16(item.shard_id);
        dst.put_u64(item.sequence);
        dst.put_u32(item.payload.len() as u32);
        dst.put_slice(&item.payload);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_encode_decode() {
        let payload = Bytes::from("test payload data");
        let frame = ReplicationFrame::new(12345, payload.clone());

        let encoded = frame.encode();
        let decoded = ReplicationFrame::decode(encoded).unwrap();

        assert_eq!(decoded.version, FRAME_VERSION);
        assert_eq!(decoded.sequence, 12345);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn test_frame_with_flags() {
        let payload = Bytes::from("data");
        let mut flags = FrameFlags::NONE;
        flags.set(FrameFlags::COMPRESSED);
        flags.set(FrameFlags::END_OF_BATCH);

        let frame = ReplicationFrame::with_flags(100, payload, flags);
        let encoded = frame.encode();
        let decoded = ReplicationFrame::decode(encoded).unwrap();

        assert!(decoded.flags.contains(FrameFlags::COMPRESSED));
        assert!(decoded.flags.contains(FrameFlags::END_OF_BATCH));
        assert!(!decoded.flags.contains(FrameFlags::REQUIRE_ACK));
    }

    #[test]
    fn test_frame_shard_id_round_trips() {
        // A data frame tagged with an origin shard survives encode/decode and
        // the streaming codec, and defaults to CONTROL_SHARD via `new`.
        let payload = Bytes::from("data");
        let tagged = ReplicationFrame::new_on_shard(7, 3, payload.clone());
        let decoded = ReplicationFrame::decode(tagged.encode()).unwrap();
        assert_eq!(decoded.shard_id, 3);
        assert_eq!(decoded.sequence, 7);
        assert_eq!(decoded.payload, payload);

        // Control frames default to the sentinel.
        assert_eq!(
            ReplicationFrame::new(1, payload.clone()).shard_id,
            CONTROL_SHARD
        );

        // Same via the tokio codec.
        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                ReplicationFrame::new_on_shard(9, 5, payload.clone()),
                &mut buf,
            )
            .unwrap();
        let via_codec = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(via_codec.shard_id, 5);
        assert_eq!(via_codec.sequence, 9);
    }

    #[test]
    fn test_frame_decode_invalid_magic() {
        let mut buf = BytesMut::new();
        buf.put_slice(b"XXXX"); // Invalid magic
        buf.put_u8(FRAME_VERSION); // version
        buf.put_u8(0); // flags
        buf.put_u16(0); // shard
        buf.put_u64(0); // sequence
        buf.put_u32(0); // length

        let result = ReplicationFrame::decode(buf.freeze());
        assert!(matches!(result, Err(FrameDecodeError::InvalidMagic)));
    }

    #[test]
    fn test_frame_decode_insufficient_data() {
        let buf = Bytes::from_static(b"FRPL"); // Only magic, missing rest
        let result = ReplicationFrame::decode(buf);
        assert!(matches!(result, Err(FrameDecodeError::InsufficientData)));
    }

    #[test]
    fn test_codec_decode() {
        let mut codec = ReplicationFrameCodec::new();
        let payload = Bytes::from("test data");
        let frame = ReplicationFrame::new(999, payload.clone());

        // Encode
        let mut encoded = BytesMut::new();
        codec.encode(frame, &mut encoded).unwrap();

        // Decode
        let decoded = codec.decode(&mut encoded).unwrap().unwrap();
        assert_eq!(decoded.sequence, 999);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn test_codec_partial_decode() {
        let mut codec = ReplicationFrameCodec::new();
        let payload = Bytes::from("test data");
        let frame = ReplicationFrame::new(999, payload.clone());

        let mut full = BytesMut::new();
        codec.encode(frame, &mut full).unwrap();

        // Split into partial chunks
        let mut partial = full.split_to(10);

        // Should return None for incomplete frame
        assert!(codec.decode(&mut partial).unwrap().is_none());

        // Add rest of the data
        partial.unsplit(full);

        // Now should decode successfully
        let decoded = codec.decode(&mut partial).unwrap().unwrap();
        assert_eq!(decoded.sequence, 999);
    }

    #[test]
    fn test_frame_flags() {
        let mut flags = FrameFlags::NONE;
        assert!(!flags.contains(FrameFlags::COMPRESSED));

        flags.set(FrameFlags::COMPRESSED);
        assert!(flags.contains(FrameFlags::COMPRESSED));
        assert!(!flags.contains(FrameFlags::END_OF_BATCH));

        flags.set(FrameFlags::END_OF_BATCH);
        assert!(flags.contains(FrameFlags::COMPRESSED));
        assert!(flags.contains(FrameFlags::END_OF_BATCH));
    }

    #[test]
    fn test_serialize_command_to_resp() {
        // Test simple SET command
        let args = vec![Bytes::from("key"), Bytes::from("value")];
        let resp = super::serialize_command_to_resp("SET", &args);

        // Expected: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
        assert_eq!(
            resp.as_ref(),
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        );
    }

    #[test]
    fn test_serialize_command_to_resp_no_args() {
        // Test PING command with no arguments
        let args: Vec<Bytes> = vec![];
        let resp = super::serialize_command_to_resp("PING", &args);

        // Expected: *1\r\n$4\r\nPING\r\n
        assert_eq!(resp.as_ref(), b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn test_serialize_command_to_resp_binary_data() {
        // Test with binary data containing special characters
        let args = vec![
            Bytes::from("key"),
            Bytes::from_static(b"value\r\nwith\x00newlines"),
        ];
        let resp = super::serialize_command_to_resp("SET", &args);

        // Binary data should be preserved correctly (20 bytes: value\r\nwith\x00newlines)
        assert!(resp.starts_with(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$20\r\n"));
        assert!(resp.ends_with(b"\r\n"));
    }

    // --- ReplconfCodec: the golden-bytes round-trip suite for the ACK/GETACK
    // control grammar. Each encoder is bound to its inverse in one place. ---

    #[test]
    fn replconf_ack_round_trips() {
        // parse_ack(encode_ack(x)) == Some((x, encoded.len())) for the boundary
        // offsets. u64::MAX pins that the offset is emitted as decimal ASCII so
        // its 20-digit form round-trips through the `$<len>\r\n<offset>\r\n`
        // bulk-string framing — the boundary a hand-rolled `format!` most
        // easily gets wrong.
        for offset in [0u64, 1, u64::MAX] {
            let encoded = ReplconfCodec::encode_ack(offset);
            assert_eq!(
                ReplconfCodec::parse_ack(&encoded),
                Some((offset, encoded.len())),
                "ACK round-trip failed for offset {offset}"
            );
        }

        // Explicit wire shape for a representative offset, decimal ASCII.
        assert_eq!(
            ReplconfCodec::encode_ack(12345).as_ref(),
            b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$5\r\n12345\r\n"
        );
        // u64::MAX is 20 decimal digits — the `$20` length prefix must reflect it.
        assert_eq!(
            ReplconfCodec::encode_ack(u64::MAX).as_ref(),
            b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$20\r\n18446744073709551615\r\n"
        );
    }

    #[test]
    fn replconf_getack_round_trips() {
        // is_getack(encode_getack()) — GETACK producer/parser pin, anchored to
        // the real encoder rather than a re-typed literal.
        let encoded = ReplconfCodec::encode_getack();
        assert!(ReplconfCodec::is_getack(&encoded));
        // Explicit wire shape.
        assert_eq!(
            encoded.as_ref(),
            b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
        );
    }

    #[test]
    fn replconf_cross_discriminator_rejection() {
        // ACK and GETACK cannot be confused for one another.
        assert!(!ReplconfCodec::is_getack(&ReplconfCodec::encode_ack(100)));
        assert_eq!(
            ReplconfCodec::parse_ack(&ReplconfCodec::encode_getack()),
            None
        );
    }

    #[test]
    fn replconf_parse_ack_streaming_invariants() {
        // Incomplete buffers → None, no panic (ported from
        // test_parse_replconf_ack_incomplete).
        assert_eq!(ReplconfCodec::parse_ack(b"*3\r\n$8\r\nREPLCONF\r\n"), None);
        assert_eq!(
            ReplconfCodec::parse_ack(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$5\r\n123"),
            None
        );

        // Two concatenated ACK frames: first parsed, consumed == frame1.len(),
        // remainder re-parses (ported from test_parse_replconf_ack_with_trailing_data).
        let frame1 = ReplconfCodec::encode_ack(100);
        let frame2 = ReplconfCodec::encode_ack(200);
        let mut combined = Vec::new();
        combined.extend_from_slice(&frame1);
        combined.extend_from_slice(&frame2);

        let (offset, consumed) = ReplconfCodec::parse_ack(&combined).unwrap();
        assert_eq!(offset, 100);
        assert_eq!(consumed, frame1.len());
        let (offset2, _) = ReplconfCodec::parse_ack(&combined[consumed..]).unwrap();
        assert_eq!(offset2, 200);
    }

    #[test]
    fn replconf_parse_ack_rejects_wrong_command() {
        // Valid RESP array that is not a REPLCONF ACK (ported from
        // test_parse_replconf_ack_wrong_command).
        let set = serialize_command_to_resp(
            "SET",
            &[Bytes::from_static(b"foo"), Bytes::from_static(b"bar")],
        );
        assert_eq!(ReplconfCodec::parse_ack(&set), None);
        // Non-RESP garbage.
        assert_eq!(ReplconfCodec::parse_ack(b"INVALID"), None);
    }

    #[test]
    fn replconf_is_getack_recognizes_variants_and_rejects_others() {
        // Case-insensitivity (ported from matches_case_insensitively).
        assert!(ReplconfCodec::is_getack(
            b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n"
        ));

        // Wrong command / wrong subcommand rejected (ported from
        // rejects_other_commands_and_other_replconf_subcommands).
        let set =
            serialize_command_to_resp("SET", &[Bytes::from_static(b"k"), Bytes::from_static(b"v")]);
        assert!(!ReplconfCodec::is_getack(&set));
        assert!(!ReplconfCodec::is_getack(&ReplconfCodec::encode_ack(100)));
        assert!(!ReplconfCodec::is_getack(b""));
        assert!(!ReplconfCodec::is_getack(b"*3\r\n$8\r\nREPLCONF\r\n"));
    }
}
