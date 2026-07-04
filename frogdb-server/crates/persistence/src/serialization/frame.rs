//! A forward-only cursor over a serialized payload.
//!
//! Every persisted type's decode half walks a byte buffer with a running offset,
//! reading little-endian scalars and length-prefixed byte runs. Done by hand that
//! is `payload[o..o + N].try_into().unwrap()` guarded by an open-coded
//! `if N > payload.len() - o { return Err(..) }` at every field — the same three
//! lines repeated dozens of times across the module, where one wrong `+= N`
//! silently shifts every field after it. [`FrameReader`] centralizes the bounds
//! check: each read advances the cursor by exactly the bytes consumed or returns a
//! clean [`SerializationError`] — it never panics and never reads out of bounds,
//! however truncated or oversized the input length prefixes claim to be.

use bytes::Bytes;

use super::SerializationError;

/// A cursor that reads little-endian frames from a byte slice, bounds-checking
/// every access. The borrowed reads return sub-slices of the backing buffer, so
/// there is no copy until a caller asks for owned [`Bytes`].
pub(crate) struct FrameReader<'a> {
    data: &'a [u8],
    pos: usize,
}

/// Generate a little-endian scalar reader that consumes `size_of::<$ty>()` bytes.
macro_rules! read_le {
    ($name:ident, $ty:ty) => {
        #[doc = concat!("Read a little-endian `", stringify!($ty), "`.")]
        pub(crate) fn $name(&mut self) -> Result<$ty, SerializationError> {
            let bytes = self.take(std::mem::size_of::<$ty>())?;
            // `take` guarantees exactly the requested length, so the conversion
            // cannot fail.
            Ok(<$ty>::from_le_bytes(bytes.try_into().unwrap()))
        }
    };
}

impl<'a> FrameReader<'a> {
    /// Wrap `data`, positioned at its first byte.
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// Bytes not yet consumed.
    pub(crate) fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    /// Borrow the next `n` bytes and advance the cursor. Returns
    /// [`SerializationError::Truncated`] — never panics — if fewer than `n` bytes
    /// remain, and rejects a length prefix so large it overflows `usize`.
    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], SerializationError> {
        let end = self.pos.checked_add(n).ok_or_else(|| {
            SerializationError::InvalidPayload("length prefix overflows usize".to_string())
        })?;
        if end > self.data.len() {
            return Err(SerializationError::Truncated {
                expected: end,
                actual: self.data.len(),
            });
        }
        let slice = &self.data[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    /// Read a single byte.
    pub(crate) fn read_u8(&mut self) -> Result<u8, SerializationError> {
        Ok(self.take(1)?[0])
    }

    read_le!(read_le_u16, u16);
    read_le!(read_le_u32, u32);
    read_le!(read_le_u64, u64);
    read_le!(read_le_i64, i64);
    read_le!(read_le_f32, f32);
    read_le!(read_le_f64, f64);

    /// Read a `u32` length prefix, then borrow that many bytes.
    pub(crate) fn read_u32_len_prefixed(&mut self) -> Result<&'a [u8], SerializationError> {
        let len = self.read_le_u32()? as usize;
        self.take(len)
    }

    /// Read a `u32`-length-prefixed run and copy it into an owned [`Bytes`].
    pub(crate) fn read_bytes_u32(&mut self) -> Result<Bytes, SerializationError> {
        Ok(Bytes::copy_from_slice(self.read_u32_len_prefixed()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_scalars_in_sequence() {
        let mut buf = Vec::new();
        buf.push(7u8);
        buf.extend_from_slice(&0x1234u16.to_le_bytes());
        buf.extend_from_slice(&0xdead_beefu32.to_le_bytes());
        buf.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        buf.extend_from_slice(&(-5i64).to_le_bytes());
        buf.extend_from_slice(&1.5f32.to_le_bytes());
        buf.extend_from_slice(&2.5f64.to_le_bytes());

        let mut r = FrameReader::new(&buf);
        assert_eq!(r.read_u8().unwrap(), 7);
        assert_eq!(r.read_le_u16().unwrap(), 0x1234);
        assert_eq!(r.read_le_u32().unwrap(), 0xdead_beef);
        assert_eq!(r.read_le_u64().unwrap(), 0x0102_0304_0506_0708);
        assert_eq!(r.read_le_i64().unwrap(), -5);
        assert_eq!(r.read_le_f32().unwrap(), 1.5);
        assert_eq!(r.read_le_f64().unwrap(), 2.5);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn take_on_empty_buffer_errors_without_panic() {
        let mut r = FrameReader::new(&[]);
        assert!(matches!(
            r.take(1),
            Err(SerializationError::Truncated { .. })
        ));
        // The cursor did not advance past the end.
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn scalar_read_past_end_errors() {
        // Only 4 bytes, but a u64 needs 8.
        let buf = [0u8; 4];
        let mut r = FrameReader::new(&buf);
        assert!(matches!(
            r.read_le_u64(),
            Err(SerializationError::Truncated {
                expected: 8,
                actual: 4
            })
        ));
        // A failed read must not consume input.
        assert_eq!(r.remaining(), 4);
    }

    #[test]
    fn length_prefix_reads_exact_run() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_le_bytes());
        buf.extend_from_slice(b"abc");
        buf.extend_from_slice(&99u8.to_le_bytes());

        let mut r = FrameReader::new(&buf);
        assert_eq!(r.read_u32_len_prefixed().unwrap(), b"abc");
        assert_eq!(r.read_u8().unwrap(), 99);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn oversized_length_prefix_errors_without_oom() {
        // A prefix that claims u32::MAX bytes with only a few present must error
        // cleanly rather than allocate or panic.
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_le_bytes());
        buf.extend_from_slice(b"short");

        let mut r = FrameReader::new(&buf);
        assert!(matches!(
            r.read_u32_len_prefixed(),
            Err(SerializationError::Truncated { .. })
        ));
    }

    #[test]
    fn read_bytes_u32_copies_owned() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(b"hi");
        let mut r = FrameReader::new(&buf);
        let owned = r.read_bytes_u32().unwrap();
        assert_eq!(owned.as_ref(), b"hi");
    }

    #[test]
    fn truncated_length_prefixed_run_errors() {
        // Prefix says 10 bytes, only 2 follow.
        let mut buf = Vec::new();
        buf.extend_from_slice(&10u32.to_le_bytes());
        buf.extend_from_slice(b"xy");
        let mut r = FrameReader::new(&buf);
        assert!(matches!(
            r.read_bytes_u32(),
            Err(SerializationError::Truncated {
                expected: 14,
                actual: 6
            })
        ));
    }
}
