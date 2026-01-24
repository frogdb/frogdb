//! Replication frame encoding and decoding.
//!
//! Frames are the unit of data transferred during WAL streaming.
//! Each frame contains a sequence of WAL entries.
//!
//! # Frame Format
//!
//! ```text
//! +--------+--------+--------+----------+----------+-------------+
//! | Magic  | Version| Flags  | Sequence | Length   | Payload     |
//! | 4 bytes| 1 byte | 1 byte | 8 bytes  | 4 bytes  | Length bytes|
//! +--------+--------+--------+----------+----------+-------------+
//! ```
//!
//! - Magic: `FRPL` (0x4652504C) - identifies FrogDB replication frames
//! - Version: Protocol version (currently 1)
//! - Flags: Reserved for future use
//! - Sequence: WAL sequence number
//! - Length: Payload length in bytes
//! - Payload: Serialized WAL operations

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Frame magic bytes: "FRPL"
pub const FRAME_MAGIC: [u8; 4] = [0x46, 0x52, 0x50, 0x4C]; // "FRPL"

/// Current frame protocol version
pub const FRAME_VERSION: u8 = 1;

/// Frame header size in bytes
pub const FRAME_HEADER_SIZE: usize = 18; // 4 + 1 + 1 + 8 + 4

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

    /// WAL sequence number
    pub sequence: u64,

    /// Payload data (serialized WAL operations)
    pub payload: Bytes,
}

impl ReplicationFrame {
    /// Create a new replication frame.
    pub fn new(sequence: u64, payload: Bytes) -> Self {
        Self {
            version: FRAME_VERSION,
            flags: FrameFlags::NONE,
            sequence,
            payload,
        }
    }

    /// Create a frame with flags.
    pub fn with_flags(sequence: u64, payload: Bytes, flags: FrameFlags) -> Self {
        Self {
            version: FRAME_VERSION,
            flags,
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
            sequence,
            payload,
        })
    }

    /// Get the total size of the encoded frame.
    pub fn encoded_size(&self) -> usize {
        FRAME_HEADER_SIZE + self.payload.len()
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
                    if &src[0..4] != FRAME_MAGIC {
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
                    let sequence = u64::from_be_bytes(src[6..14].try_into().unwrap());
                    let length = u32::from_be_bytes(src[14..18].try_into().unwrap()) as usize;

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
                        sequence,
                        length,
                    };
                }
                DecodeState::ReadingPayload {
                    version,
                    flags,
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
    fn test_frame_decode_invalid_magic() {
        let mut buf = BytesMut::new();
        buf.put_slice(b"XXXX"); // Invalid magic
        buf.put_u8(1); // version
        buf.put_u8(0); // flags
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
}
