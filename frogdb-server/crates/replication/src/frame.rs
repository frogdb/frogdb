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
    //
    // A hint, not a bound: `BytesMut` grows when it is wrong, so the three
    // `+ -> *` mutations of this expression are equivalent — they change how
    // much is reserved up front and nothing any caller can observe. The
    // *underflowing* form is not: `16 - cmd_name.len()` panics on a command
    // name longer than the per-element estimate, which
    // `test_serialize_command_to_resp_long_command_name` pins.
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
        // `REPLCONF` (8) + `\r\n$6\r\n` (6) + `GETACK` (6): the shortest prefix
        // that can answer the question, and the bound that makes the
        // `split_at(8)` below infallible. Mutating either `+` to `-` leaves 8,
        // which is equivalent: 8 still makes the split safe, and the checks
        // that follow re-derive the same 20-byte requirement, so no input can
        // be classified differently — only more work is done before saying no.
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

/// Maximum frame payload size — **derived**, not chosen.
///
/// A replication frame carries the RESP encoding of a command the connection
/// layer already accepted and the primary already committed, so this ceiling
/// cannot be picked independently of the ceiling that admitted the write:
/// anything the client layer takes and this codec refuses is a write that is
/// acknowledged and unreplicable, and the link wedges re-sending it from the
/// backlog forever (round-2 issue 69, which is what a private 64 MB constant
/// bought against a 512 MB [`frogdb_protocol::PROTO_MAX_BULK_LEN`]).
///
/// So it is [`frogdb_protocol::MAX_INTERNAL_FRAME_LEN`], the shared internal
/// ceiling, and both directions enforce it: [`ReplicationFrame::encode`]
/// refuses an oversized payload instead of truncating its length prefix through
/// an unchecked `as u32`, and `decode` refuses a header that claims more.
pub const MAX_FRAME_SIZE: usize = frogdb_protocol::MAX_INTERNAL_FRAME_LEN;

/// Frame flags
#[derive(Debug, Clone, Copy, Default)]
pub struct FrameFlags(u8);

impl FrameFlags {
    /// No flags set
    pub const NONE: Self = Self(0);

    /// Frame contains compressed payload
    // Written as a shift for symmetry with its siblings; `1 << 0` and `1 >> 0`
    // are the same value, so mutating the shift direction here is equivalent.
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

    /// Whether a payload of `len` bytes fits a frame.
    ///
    /// The predicate the encoders share, exposed so the boundary can be pinned
    /// at exactly [`MAX_FRAME_SIZE`] without allocating a gigabyte to do it.
    #[inline]
    pub fn payload_fits(len: usize) -> bool {
        len <= MAX_FRAME_SIZE
    }

    /// Encode frame to bytes, or refuse a payload no frame can carry.
    ///
    /// The length prefix is a `u32`, so an unchecked cast of an oversized
    /// payload would wrap and put a frame on the wire whose header disagrees
    /// with its bytes — the receiver would decode the truncated prefix and then
    /// read the remainder as the next frame's header, desynchronising the link
    /// for good. Refusing is the honest failure: the caller drops the
    /// connection and the replica comes back for a full resync.
    pub fn encode(&self) -> Result<Bytes, FrameEncodeError> {
        if !Self::payload_fits(self.payload.len()) {
            return Err(FrameEncodeError::PayloadTooLarge {
                size: self.payload.len(),
            });
        }
        let mut buf = BytesMut::with_capacity(FRAME_HEADER_SIZE + self.payload.len());

        buf.put_slice(&FRAME_MAGIC);
        buf.put_u8(self.version);
        buf.put_u8(self.flags.bits());
        buf.put_u16(self.shard_id);
        buf.put_u64(self.sequence);
        buf.put_u32(self.payload.len() as u32);
        buf.put_slice(&self.payload);

        Ok(buf.freeze())
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

        // Equality, not a ceiling: version 1 had no `shard_id` field, so a v1
        // frame parsed under today's 20-byte layout yields a bogus shard id and
        // a bogus sequence — and the sequence IS the replication offset. There
        // is no code path in this crate that understands any layout but the
        // current one, so "older" is exactly as unreadable as "newer".
        let version = buf.get_u8();
        if version != FRAME_VERSION {
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

/// The one way encoding a frame can fail: a payload larger than the link can
/// carry ([`MAX_FRAME_SIZE`]).
#[derive(Debug, Clone, thiserror::Error)]
pub enum FrameEncodeError {
    #[error(
        "payload too large to replicate: {size} bytes exceeds the {} byte frame ceiling",
        MAX_FRAME_SIZE
    )]
    PayloadTooLarge { size: usize },
}

impl From<FrameEncodeError> for io::Error {
    fn from(err: FrameEncodeError) -> Self {
        io::Error::new(io::ErrorKind::InvalidInput, err.to_string())
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

                    // Same equality gate as `ReplicationFrame::decode`, and for
                    // the same reason: this build's header layout is the only
                    // one it can parse, in either direction.
                    let version = src[4];
                    if version != FRAME_VERSION {
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
        // Same ceiling as `ReplicationFrame::encode`, refused the same way: a
        // `u32` length prefix cannot describe a larger payload, and writing the
        // wrapped value would desynchronise the reader.
        if !ReplicationFrame::payload_fits(item.payload.len()) {
            return Err(FrameEncodeError::PayloadTooLarge {
                size: item.payload.len(),
            }
            .into());
        }
        // Capacity hint only — `put_slice` grows `dst` regardless — so the
        // `+ -> *` mutation of this expression is equivalent. The underflowing
        // `-` form is not, and any payload longer than the header catches it.
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

        let encoded = frame.encode().unwrap();
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
        let encoded = frame.encode().unwrap();
        let decoded = ReplicationFrame::decode(encoded).unwrap();

        assert!(decoded.flags.contains(FrameFlags::COMPRESSED));
        assert!(decoded.flags.contains(FrameFlags::END_OF_BATCH));
        assert!(!decoded.flags.contains(FrameFlags::REQUIRE_ACK));
    }

    // FM-REPLICATION-034
    // FM-REPLICATION-032
    #[test]
    fn test_frame_shard_id_round_trips() {
        // A data frame tagged with an origin shard survives encode/decode and
        // the streaming codec, and defaults to CONTROL_SHARD via `new`.
        let payload = Bytes::from("data");
        let tagged = ReplicationFrame::new_on_shard(7, 3, payload.clone());
        let decoded = ReplicationFrame::decode(tagged.encode().unwrap()).unwrap();
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

    // --- the link carries what the connection layer accepted (issue 69) ------

    /// The ceiling is derived from the RESP bulk ceiling, not chosen next to
    /// it. Pinned as a relation rather than a number so the two cannot drift
    /// back into a band where a write is accepted and then unreplicable — which
    /// is what a private 64 MB frame constant did against a 512 MB bulk limit.
    // FM-REPLICATION-011
    #[test]
    fn the_frame_ceiling_is_derived_from_the_resp_bulk_ceiling() {
        assert_eq!(MAX_FRAME_SIZE, frogdb_protocol::MAX_INTERNAL_FRAME_LEN);
        // Both relations are compile-time truths, so they are asserted in const
        // blocks: a drift between the two ceilings fails the build rather than
        // the test run.
        const {
            assert!(
                MAX_FRAME_SIZE > frogdb_protocol::PROTO_MAX_BULK_LEN,
                "a maximal bulk value plus its command framing must fit a frame"
            );
        }
        // The header's length prefix is a `u32`; a ceiling above that could not
        // be described on the wire even if the encoder allowed it.
        const {
            assert!(
                MAX_FRAME_SIZE <= u32::MAX as usize,
                "the frame ceiling must be describable by the header's u32 length"
            );
        }

        // The boundary itself, without allocating a gigabyte to touch it.
        assert!(ReplicationFrame::payload_fits(0));
        assert!(ReplicationFrame::payload_fits(MAX_FRAME_SIZE - 1));
        assert!(ReplicationFrame::payload_fits(MAX_FRAME_SIZE));
        assert!(!ReplicationFrame::payload_fits(MAX_FRAME_SIZE + 1));
    }

    /// A payload no `u32` length prefix can describe is refused, not truncated.
    /// The unchecked `as u32` this replaces wrote a wrapped length, so the peer
    /// decoded a short frame and then read the rest of the payload as the next
    /// frame's header — a link that never resynchronises.
    ///
    /// The oversized buffer is never written to, so its pages stay unmapped:
    /// the allocation is virtual, and `encode` refuses before it copies.
    // FM-REPLICATION-011
    #[test]
    fn encode_refuses_a_payload_larger_than_the_frame_ceiling() {
        let oversized = Bytes::from(vec![0u8; MAX_FRAME_SIZE + 1]);
        let frame = ReplicationFrame::new(1, oversized.clone());

        assert!(matches!(
            frame.encode(),
            Err(FrameEncodeError::PayloadTooLarge { size }) if size == MAX_FRAME_SIZE + 1
        ));

        // The tokio codec enforces the same ceiling the same way.
        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::new();
        let err = codec
            .encode(ReplicationFrame::new(1, oversized), &mut buf)
            .expect_err("the streaming encoder must refuse it too");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(buf.is_empty(), "a refused frame writes no bytes");
    }

    /// The regression proper: a value inside the documented client limit but
    /// above the old private 64 MB frame ceiling crosses the link intact. Before
    /// the ceilings were related, the primary accepted this write, committed it,
    /// and then emitted a frame its replica's decoder rejected on sight.
    // FM-REPLICATION-011
    #[test]
    fn a_payload_over_the_old_ceiling_round_trips_across_the_link() {
        const OVER_OLD_CEILING: usize = 64 * 1024 * 1024 + 1;
        let payload = Bytes::from(vec![7u8; OVER_OLD_CEILING]);
        let frame = ReplicationFrame::new_on_shard(42, 3, payload.clone());

        let decoded = ReplicationFrame::decode(frame.encode().unwrap()).unwrap();
        assert_eq!(decoded.payload.len(), OVER_OLD_CEILING);
        assert_eq!(decoded.payload, payload);
        assert_eq!(decoded.shard_id, 3);

        // And through the streaming codec both ends actually use.
        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();
        let via_codec = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(via_codec.payload.len(), OVER_OLD_CEILING);
        assert_eq!(via_codec.sequence, 42);
    }

    // FM-REPLICATION-032
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

    // FM-REPLICATION-032
    #[test]
    fn test_frame_decode_insufficient_data() {
        let buf = Bytes::from_static(b"FRPL"); // Only magic, missing rest
        let result = ReplicationFrame::decode(buf);
        assert!(matches!(result, Err(FrameDecodeError::InsufficientData)));
    }

    // FM-REPLICATION-032
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

    // FM-REPLICATION-032
    #[test]
    fn test_codec_partial_decode() {
        // Every split point, not one fixed offset: a decoder that only handled
        // a particular cut (inside the header, say) would still pass a single
        // arbitrarily-chosen offset. The claim this test makes is that no TCP
        // segmentation boundary can desynchronise the two-state machine, so it
        // has to try every position a real socket read could land on.
        let payload = Bytes::from("test data");
        let frame = ReplicationFrame::new(999, payload.clone());
        let mut full = BytesMut::new();
        ReplicationFrameCodec::new()
            .encode(frame, &mut full)
            .unwrap();

        for cut in 1..full.len() {
            let mut codec = ReplicationFrameCodec::new();
            let mut partial = BytesMut::from(&full[..cut]);

            assert!(
                codec.decode(&mut partial).unwrap().is_none(),
                "a buffer holding only {cut} of {} bytes must not yield a frame",
                full.len()
            );

            partial.extend_from_slice(&full[cut..]);
            let decoded = codec.decode(&mut partial).unwrap().unwrap_or_else(|| {
                panic!("the whole frame must decode once the rest arrives (cut={cut})")
            });
            assert_eq!(decoded.sequence, 999);
            assert_eq!(decoded.payload, payload);
        }
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

    // FM-REPLICATION-031
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

    // FM-REPLICATION-031
    #[test]
    fn test_serialize_command_to_resp_no_args() {
        // Test PING command with no arguments
        let args: Vec<Bytes> = vec![];
        let resp = super::serialize_command_to_resp("PING", &args);

        // Expected: *1\r\n$4\r\nPING\r\n
        assert_eq!(resp.as_ref(), b"*1\r\n$4\r\nPING\r\n");
    }

    // FM-REPLICATION-031
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

    /// A command name longer than the 16-byte-per-element capacity estimate.
    /// `GEORADIUSBYMEMBER_RO` is 20 bytes, so the estimate under-counts the
    /// name alone — the serializer must be sizing a hint, not a bound.
    // FM-REPLICATION-031
    #[test]
    fn test_serialize_command_to_resp_long_command_name() {
        let resp = super::serialize_command_to_resp(
            "GEORADIUSBYMEMBER_RO",
            &[Bytes::from("key"), Bytes::from("m")],
        );
        assert_eq!(
            resp.as_ref(),
            b"*3\r\n$20\r\nGEORADIUSBYMEMBER_RO\r\n$3\r\nkey\r\n$1\r\nm\r\n"
        );
    }

    // --- ReplconfCodec: the golden-bytes round-trip suite for the ACK/GETACK
    // control grammar. Each encoder is bound to its inverse in one place. ---

    // FM-REPLICATION-033
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

    // FM-REPLICATION-033
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

    // FM-REPLICATION-033
    #[test]
    fn replconf_cross_discriminator_rejection() {
        // ACK and GETACK cannot be confused for one another.
        assert!(!ReplconfCodec::is_getack(&ReplconfCodec::encode_ack(100)));
        assert_eq!(
            ReplconfCodec::parse_ack(&ReplconfCodec::encode_getack()),
            None
        );
    }

    // FM-REPLICATION-033
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

    // FM-REPLICATION-033
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

    // FM-REPLICATION-033
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

    // FM-REPLICATION-033
    #[test]
    fn replconf_parse_ack_checks_the_command_and_the_subcommand() {
        // Both tokens are load-bearing, and neither alone is enough. The
        // dangerous shape is a *real* REPLCONF whose third field happens to
        // parse as a number: `REPLCONF listening-port 6379` arrives on the same
        // socket during the handshake, and crediting the replica with an ACK
        // for offset 6379 would let `WAIT` return on a write nobody applied —
        // or, worse, park the acked head ahead of the true one.
        let listening_port = serialize_command_to_resp(
            "REPLCONF",
            &[
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(b"6379"),
            ],
        );
        assert_eq!(ReplconfCodec::parse_ack(&listening_port), None);

        // And the mirror: an `ACK` subcommand under some other command name is
        // not an ACK either.
        let foreign_ack = serialize_command_to_resp(
            "NOTREPLCONF",
            &[Bytes::from_static(b"ACK"), Bytes::from_static(b"123")],
        );
        assert_eq!(ReplconfCodec::parse_ack(&foreign_ack), None);
    }

    // FM-REPLICATION-033
    #[test]
    fn replconf_is_getack_decides_on_the_command_and_subcommand_tokens_alone() {
        // The discriminator is a prefix test: `REPLCONF` + `$6` + `GETACK` is
        // the whole decision, and the trailing `$1\r\n*\r\n` is not re-checked.
        // Exactly those 20 bytes past the `*3\r\n$8\r\n` prefix are enough...
        const MINIMAL: &[u8] = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK";
        assert_eq!(MINIMAL.len(), 8 + 8 + 6 + 6, "the boundary case is exact");
        assert!(ReplconfCodec::is_getack(MINIMAL));

        // ...and one byte fewer is not: a buffer cut inside the subcommand
        // cannot be classified, and must not be guessed at.
        assert!(!ReplconfCodec::is_getack(&MINIMAL[..MINIMAL.len() - 1]));
    }

    // FM-REPLICATION-033
    #[test]
    fn replconf_is_getack_rejects_a_frame_cut_inside_the_command_name() {
        // This runs on every ingested frame, over payloads the peer controls,
        // so every short prefix must answer `false` rather than index past the
        // end. The lengths below straddle the `split_at(8)` the command-name
        // comparison does.
        for cut in 0..=b"REPLCONF".len() {
            let mut payload = b"*3\r\n$8\r\n".to_vec();
            payload.extend_from_slice(&b"REPLCONF"[..cut]);
            assert!(
                !ReplconfCodec::is_getack(&payload),
                "a frame holding only {cut} bytes of the command name is not a GETACK"
            );
        }
    }

    // FM-REPLICATION-033
    #[test]
    fn replconf_is_getack_rejects_a_six_byte_subcommand_that_is_not_getack() {
        // The length check that guards the subcommand comparison must not be
        // allowed to stand in for the comparison: a REPLCONF whose subcommand
        // is six bytes of something else is not a solicitation.
        assert!(!ReplconfCodec::is_getack(
            b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nFOOBAR\r\n$1\r\n*\r\n"
        ));
        assert!(!ReplconfCodec::is_getack(
            b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACX\r\n$1\r\n*\r\n"
        ));
    }

    // --- the decoder's rejection ladder and its boundaries -------------------
    //
    // Every comparison below is pinned as a *pair*: the value the decoder must
    // accept and the adjacent one it must refuse. A ceiling tested only from
    // the far side is a ceiling that can move a byte in either direction
    // unnoticed.

    /// A 20-byte frame header, built by hand so a test can claim a length it
    /// does not supply (the oversize and truncation cases would otherwise cost
    /// a gigabyte of payload to express).
    fn raw_header(version: u8, sequence: u64, length: u32) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_slice(&FRAME_MAGIC);
        buf.put_u8(version);
        buf.put_u8(0); // flags
        buf.put_u16(CONTROL_SHARD);
        buf.put_u64(sequence);
        buf.put_u32(length);
        assert_eq!(buf.len(), FRAME_HEADER_SIZE);
        buf
    }

    // FM-REPLICATION-032
    #[test]
    fn decode_accepts_only_this_builds_frame_version() {
        // The gate is equality, not a ceiling: this build has exactly one
        // header layout, so a version that is not its own — older or newer —
        // cannot be parsed by it. Version 1 had no `shard_id` field, so a v1
        // frame read under today's 20-byte layout would yield a bogus shard id
        // and a bogus sequence (the sequence IS the replication offset), which
        // is a worse failure than a refused link.
        for version in 0..FRAME_VERSION {
            let older = raw_header(version, 1, 0).freeze();
            assert!(
                matches!(
                    ReplicationFrame::decode(older),
                    Err(FrameDecodeError::UnsupportedVersion(v)) if v == version
                ),
                "version {version} predates this build's header layout and must be refused"
            );
        }

        let current = ReplicationFrame::decode(raw_header(FRAME_VERSION, 1, 0).freeze())
            .unwrap_or_else(|e| panic!("this build's own version must decode, got {e}"));
        assert_eq!(current.version, FRAME_VERSION);

        let newer = raw_header(FRAME_VERSION + 1, 1, 0).freeze();
        assert!(matches!(
            ReplicationFrame::decode(newer),
            Err(FrameDecodeError::UnsupportedVersion(v)) if v == FRAME_VERSION + 1
        ));
    }

    // FM-REPLICATION-032
    #[test]
    fn decode_refuses_a_claimed_length_above_the_ceiling_but_not_at_it() {
        // A header claiming exactly the ceiling is legal and merely
        // unsatisfied — the payload has not arrived — while one byte more is a
        // frame no encoder could have produced. Neither case allocates: the
        // claim is in the header, not in the buffer.
        let at_ceiling = raw_header(FRAME_VERSION, 1, MAX_FRAME_SIZE as u32).freeze();
        assert!(
            matches!(
                ReplicationFrame::decode(at_ceiling),
                Err(FrameDecodeError::InsufficientData)
            ),
            "a header at the ceiling is short of data, not over the limit"
        );

        let over_ceiling = raw_header(FRAME_VERSION, 1, MAX_FRAME_SIZE as u32 + 1).freeze();
        assert!(matches!(
            ReplicationFrame::decode(over_ceiling),
            Err(FrameDecodeError::PayloadTooLarge(n)) if n == MAX_FRAME_SIZE + 1
        ));
    }

    // FM-REPLICATION-032
    #[test]
    fn decode_takes_exactly_the_payload_its_header_claims() {
        let payload = Bytes::from_static(b"nine byte");
        let encoded = ReplicationFrame::new(7, payload.clone()).encode().unwrap();

        // Exactly one frame's worth: the whole payload, no more.
        let decoded = ReplicationFrame::decode(encoded.clone()).unwrap();
        assert_eq!(decoded.payload, payload);

        // A buffer holding the frame *and* the head of the next one still
        // yields this frame's payload and nothing of its neighbour.
        let mut with_trailer = BytesMut::from(&encoded[..]);
        with_trailer.put_slice(b"FRPL-next-frame-starts-here");
        let decoded = ReplicationFrame::decode(with_trailer.freeze()).unwrap();
        assert_eq!(decoded.payload, payload);

        // One byte short of the claimed payload is incomplete, never a short
        // payload handed on as if it were whole.
        let truncated = encoded.slice(..encoded.len() - 1);
        assert!(matches!(
            ReplicationFrame::decode(truncated),
            Err(FrameDecodeError::InsufficientData)
        ));
    }

    // FM-REPLICATION-031
    #[test]
    fn encoded_size_is_the_header_plus_the_payload_and_matches_the_encoding() {
        for len in [0usize, 1, 17, 4096] {
            let frame = ReplicationFrame::new(1, Bytes::from(vec![0xABu8; len]));
            assert_eq!(frame.encoded_size(), FRAME_HEADER_SIZE + len);
            assert_eq!(
                frame.encoded_size(),
                frame.encode().unwrap().len(),
                "the predicted size must match the bytes actually produced"
            );
            // The offset unit is the payload alone — `encoded_size` is the
            // transport cost and the two must not be confused.
            assert_eq!(frame.stream_advance(), len as u64);
            assert_eq!(
                frame.encoded_size() as u64 - frame.stream_advance(),
                FRAME_HEADER_SIZE as u64
            );
        }
    }

    // FM-REPLICATION-032
    #[test]
    fn codec_yields_a_frame_the_moment_its_last_byte_arrives() {
        // The zero-payload frame is the boundary of the header check: a buffer
        // holding exactly 20 bytes is a whole frame, and 19 is not.
        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::new();
        codec
            .encode(ReplicationFrame::new(11, Bytes::new()), &mut buf)
            .unwrap();
        assert_eq!(buf.len(), FRAME_HEADER_SIZE);

        let mut short = buf.split_to(FRAME_HEADER_SIZE - 1);
        assert!(
            codec.decode(&mut short).unwrap().is_none(),
            "a header one byte short yields nothing and consumes nothing"
        );
        assert_eq!(short.len(), FRAME_HEADER_SIZE - 1);

        short.unsplit(buf);
        let frame = codec
            .decode(&mut short)
            .unwrap()
            .expect("the twentieth byte completes the frame");
        assert_eq!(frame.sequence, 11);
        assert!(frame.payload.is_empty());
        assert!(short.is_empty(), "a decoded frame is consumed whole");
    }

    // FM-REPLICATION-032
    #[test]
    fn codec_accepts_only_this_builds_frame_version() {
        // Same equality gate as `ReplicationFrame::decode`, on the streaming
        // path: the two must not be able to disagree about which frames the
        // link will carry, in either direction.
        for version in 0..FRAME_VERSION {
            let mut codec = ReplicationFrameCodec::new();
            let mut buf = raw_header(version, 3, 0);
            let err = codec
                .decode(&mut buf)
                .expect_err("a version older than this build's layout must be refused");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "version {version}");
        }

        let mut codec = ReplicationFrameCodec::new();
        let mut buf = raw_header(FRAME_VERSION, 3, 0);
        let frame = codec
            .decode(&mut buf)
            .unwrap_or_else(|e| panic!("this build's own version must decode, got {e}"))
            .expect("a zero-length payload completes the frame");
        assert_eq!(frame.version, FRAME_VERSION);

        let mut codec = ReplicationFrameCodec::new();
        let mut buf = raw_header(FRAME_VERSION + 1, 3, 0);
        let err = codec
            .decode(&mut buf)
            .expect_err("a newer version is refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    // FM-REPLICATION-032
    #[test]
    fn codec_refuses_a_claimed_length_above_the_ceiling_but_not_at_it() {
        // At the ceiling the decoder is simply waiting for bytes; above it the
        // link is unusable and must be failed rather than parked forever.
        let mut codec = ReplicationFrameCodec::new();
        let mut at_ceiling = raw_header(FRAME_VERSION, 4, MAX_FRAME_SIZE as u32);
        assert!(
            codec.decode(&mut at_ceiling).unwrap().is_none(),
            "a frame at the ceiling is awaited, not rejected"
        );

        let mut codec = ReplicationFrameCodec::new();
        let mut over_ceiling = raw_header(FRAME_VERSION, 4, MAX_FRAME_SIZE as u32 + 1);
        let err = codec
            .decode(&mut over_ceiling)
            .expect_err("a length above the ceiling is refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    // FM-REPLICATION-032
    #[test]
    fn codec_round_trips_a_payload_longer_than_the_header() {
        // Anything the reservation arithmetic could get wrong shows up once the
        // payload outgrows the 20-byte header it is added to.
        let payload = Bytes::from_static(b"a payload comfortably longer than twenty bytes");
        assert!(payload.len() > FRAME_HEADER_SIZE);

        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                ReplicationFrame::new_on_shard(21, 2, payload.clone()),
                &mut buf,
            )
            .unwrap();
        assert_eq!(buf.len(), FRAME_HEADER_SIZE + payload.len());

        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.payload, payload);
        assert_eq!(frame.shard_id, 2);
        assert!(buf.is_empty());
    }

    // FM-REPLICATION-032
    #[test]
    fn codec_refuses_a_header_whose_magic_is_not_frpl() {
        // The magic is the streaming decoder's only defence against a buffer it
        // has lost its place in: every other header field is a plausible number
        // whatever bytes arrive. A frame one byte out of phase must fail the
        // link here rather than be parsed as a plausible header.
        let payload = Bytes::from_static(b"payload");
        let mut good = BytesMut::new();
        ReplicationFrameCodec::new()
            .encode(ReplicationFrame::new(5, payload), &mut good)
            .unwrap();

        for (label, corrupt) in [
            ("wrong magic", {
                let mut buf = good.clone();
                buf[..4].copy_from_slice(b"XXXX");
                buf
            }),
            ("one byte of the magic flipped", {
                let mut buf = good.clone();
                buf[3] ^= 0x01;
                buf
            }),
            ("a stream one byte out of phase", {
                let mut buf = BytesMut::from(&b"\0"[..]);
                buf.extend_from_slice(&good);
                buf
            }),
        ] {
            let mut codec = ReplicationFrameCodec::new();
            let mut buf = corrupt;
            let err = match codec.decode(&mut buf) {
                Err(err) => err,
                Ok(other) => panic!(
                    "{label} must fail the link, got {:?}",
                    other.map(|f| f.sequence)
                ),
            };
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{label}");
        }

        // The control: the same bytes with their magic intact still decode, so
        // the three cases above are refused for their magic and nothing else.
        let mut codec = ReplicationFrameCodec::new();
        let mut buf = good;
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap().sequence, 5);
    }
}
