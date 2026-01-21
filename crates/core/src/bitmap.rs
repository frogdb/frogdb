//! Bitmap operations for string values.
//!
//! Provides bit-level operations on string data with Redis-compatible MSB-first bit ordering.
//! In this ordering, bit offset 0 is the most significant bit of byte 0.

/// Bitmap operation type for BITOP command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOp {
    /// Bitwise AND of all source keys.
    And,
    /// Bitwise OR of all source keys.
    Or,
    /// Bitwise XOR of all source keys.
    Xor,
    /// Bitwise NOT of a single source key.
    Not,
}

impl BitOp {
    /// Parse a bitop type from bytes.
    pub fn parse(s: &[u8]) -> Option<Self> {
        let upper: Vec<u8> = s.iter().map(|b| b.to_ascii_uppercase()).collect();
        match upper.as_slice() {
            b"AND" => Some(BitOp::And),
            b"OR" => Some(BitOp::Or),
            b"XOR" => Some(BitOp::Xor),
            b"NOT" => Some(BitOp::Not),
            _ => None,
        }
    }
}

/// Perform a bitwise operation on multiple byte slices.
///
/// For NOT, only the first source is used.
/// The result is the length of the longest source.
pub fn bitop(op: BitOp, sources: &[&[u8]]) -> Vec<u8> {
    if sources.is_empty() {
        return Vec::new();
    }

    let max_len = sources.iter().map(|s| s.len()).max().unwrap_or(0);
    if max_len == 0 {
        return Vec::new();
    }

    let mut result = vec![0u8; max_len];

    match op {
        BitOp::And => {
            // Initialize with all 1s, then AND each source
            result.fill(0xFF);
            for source in sources {
                for (i, byte) in result.iter_mut().enumerate() {
                    let src_byte = source.get(i).copied().unwrap_or(0);
                    *byte &= src_byte;
                }
            }
        }
        BitOp::Or => {
            // Initialize with all 0s, then OR each source
            for source in sources {
                for (i, byte) in result.iter_mut().enumerate() {
                    let src_byte = source.get(i).copied().unwrap_or(0);
                    *byte |= src_byte;
                }
            }
        }
        BitOp::Xor => {
            // Initialize with all 0s, then XOR each source
            for source in sources {
                for (i, byte) in result.iter_mut().enumerate() {
                    let src_byte = source.get(i).copied().unwrap_or(0);
                    *byte ^= src_byte;
                }
            }
        }
        BitOp::Not => {
            // NOT only uses the first source
            let source = sources[0];
            for (i, byte) in result.iter_mut().enumerate() {
                let src_byte = source.get(i).copied().unwrap_or(0);
                *byte = !src_byte;
            }
        }
    }

    result
}

/// Overflow handling mode for BITFIELD operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverflowMode {
    /// Wrap around on overflow (default).
    #[default]
    Wrap,
    /// Saturate at min/max value on overflow.
    Sat,
    /// Fail (return nil) on overflow.
    Fail,
}

impl OverflowMode {
    /// Parse overflow mode from bytes.
    pub fn parse(s: &[u8]) -> Option<Self> {
        let upper: Vec<u8> = s.iter().map(|b| b.to_ascii_uppercase()).collect();
        match upper.as_slice() {
            b"WRAP" => Some(OverflowMode::Wrap),
            b"SAT" => Some(OverflowMode::Sat),
            b"FAIL" => Some(OverflowMode::Fail),
            _ => None,
        }
    }
}

/// Encoding type for BITFIELD operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitfieldEncoding {
    /// Signed integer with specified number of bits.
    Signed(u8),
    /// Unsigned integer with specified number of bits.
    Unsigned(u8),
}

impl BitfieldEncoding {
    /// Parse encoding from bytes (e.g., "i8", "u16").
    pub fn parse(s: &[u8]) -> Option<Self> {
        if s.is_empty() {
            return None;
        }

        let s_str = std::str::from_utf8(s).ok()?;
        let (signed, bits_str) = match s_str.chars().next()? {
            'i' | 'I' => (true, &s_str[1..]),
            'u' | 'U' => (false, &s_str[1..]),
            _ => return None,
        };

        let bits: u8 = bits_str.parse().ok()?;
        // Signed can use up to 64 bits (i64), unsigned up to 63 bits
        let max_bits = if signed { 64 } else { 63 };
        if bits == 0 || bits > max_bits {
            return None;
        }

        if signed {
            Some(BitfieldEncoding::Signed(bits))
        } else {
            Some(BitfieldEncoding::Unsigned(bits))
        }
    }

    /// Get the number of bits in this encoding.
    pub fn bits(&self) -> u8 {
        match self {
            BitfieldEncoding::Signed(b) | BitfieldEncoding::Unsigned(b) => *b,
        }
    }

    /// Check if this encoding is signed.
    pub fn is_signed(&self) -> bool {
        matches!(self, BitfieldEncoding::Signed(_))
    }
}

/// Offset specification for BITFIELD operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitfieldOffset {
    /// Absolute bit offset.
    Absolute(u64),
    /// Indexed offset (multiplied by encoding size).
    Indexed(u64),
}

impl BitfieldOffset {
    /// Parse offset from bytes. "#N" is indexed, plain "N" is absolute.
    pub fn parse(s: &[u8]) -> Option<Self> {
        let s_str = std::str::from_utf8(s).ok()?;

        if let Some(rest) = s_str.strip_prefix('#') {
            let index: u64 = rest.parse().ok()?;
            Some(BitfieldOffset::Indexed(index))
        } else {
            let offset: u64 = s_str.parse().ok()?;
            Some(BitfieldOffset::Absolute(offset))
        }
    }

    /// Resolve the offset to an absolute bit position given an encoding.
    pub fn resolve(&self, encoding: BitfieldEncoding) -> u64 {
        match self {
            BitfieldOffset::Absolute(off) => *off,
            BitfieldOffset::Indexed(idx) => idx * encoding.bits() as u64,
        }
    }
}

/// A single BITFIELD subcommand.
#[derive(Debug, Clone)]
pub enum BitfieldSubCommand {
    /// GET encoding offset - read a value
    Get {
        encoding: BitfieldEncoding,
        offset: BitfieldOffset,
    },
    /// SET encoding offset value - write a value
    Set {
        encoding: BitfieldEncoding,
        offset: BitfieldOffset,
        value: i64,
    },
    /// INCRBY encoding offset increment - increment a value
    IncrBy {
        encoding: BitfieldEncoding,
        offset: BitfieldOffset,
        increment: i64,
    },
    /// OVERFLOW mode - set overflow behavior for subsequent operations
    Overflow(OverflowMode),
}

/// Get a bit from a byte slice using MSB-first bit ordering.
///
/// Bit offset 0 is the most significant bit of byte 0.
#[inline]
pub fn getbit(data: &[u8], offset: u64) -> u8 {
    let byte_idx = (offset / 8) as usize;
    if byte_idx >= data.len() {
        return 0;
    }
    let bit_idx = 7 - (offset % 8) as u8; // MSB-first
    (data[byte_idx] >> bit_idx) & 1
}

/// Set a bit in a byte vector using MSB-first bit ordering.
///
/// Returns the previous bit value.
/// Auto-extends the vector with zeros if offset is beyond the end.
#[inline]
pub fn setbit(data: &mut Vec<u8>, offset: u64, value: u8) -> u8 {
    let byte_idx = (offset / 8) as usize;
    let bit_idx = 7 - (offset % 8) as u8; // MSB-first

    // Extend with zeros if necessary
    if byte_idx >= data.len() {
        data.resize(byte_idx + 1, 0);
    }

    let old_bit = (data[byte_idx] >> bit_idx) & 1;

    if value != 0 {
        data[byte_idx] |= 1 << bit_idx;
    } else {
        data[byte_idx] &= !(1 << bit_idx);
    }

    old_bit
}

/// Count the number of set bits in a byte slice.
///
/// If start and end are provided, only count bits in that byte range.
/// Start and end are inclusive and support negative indices (from end).
pub fn bitcount(data: &[u8], start: Option<i64>, end: Option<i64>, bit_mode: bool) -> u64 {
    if data.is_empty() {
        return 0;
    }

    let len = data.len() as i64;

    // Default range is entire string
    let (start_pos, end_pos) = if bit_mode {
        // BIT mode: positions are bit indices
        let bit_len = len * 8;
        let s = start.unwrap_or(0);
        let e = end.unwrap_or(bit_len - 1);

        // Handle negative indices
        let s = if s < 0 { (bit_len + s).max(0) } else { s.min(bit_len - 1) };
        let e = if e < 0 { (bit_len + e).max(0) } else { e.min(bit_len - 1) };

        if s > e {
            return 0;
        }

        (s as u64, e as u64)
    } else {
        // BYTE mode (default): positions are byte indices
        let s = start.unwrap_or(0);
        let e = end.unwrap_or(len - 1);

        // Handle negative indices
        let s = if s < 0 { (len + s).max(0) } else { s.min(len - 1) };
        let e = if e < 0 { (len + e).max(0) } else { e.min(len - 1) };

        if s > e {
            return 0;
        }

        // Convert to bit positions
        (s as u64 * 8, (e as u64 + 1) * 8 - 1)
    };

    // Count bits in range
    let mut count = 0u64;
    let start_byte = (start_pos / 8) as usize;
    let end_byte = (end_pos / 8) as usize;

    for byte_idx in start_byte..=end_byte.min(data.len() - 1) {
        let byte = data[byte_idx];

        // Determine which bits in this byte are in range
        let byte_start_bit = byte_idx as u64 * 8;
        let byte_end_bit = byte_start_bit + 7;

        let range_start = start_pos.max(byte_start_bit) - byte_start_bit;
        let range_end = end_pos.min(byte_end_bit) - byte_start_bit;

        if range_start == 0 && range_end == 7 {
            // Entire byte is in range - use popcount
            count += byte.count_ones() as u64;
        } else {
            // Partial byte - count individual bits
            for bit in range_start..=range_end {
                let bit_pos = 7 - bit; // MSB-first
                if (byte >> bit_pos) & 1 != 0 {
                    count += 1;
                }
            }
        }
    }

    count
}

/// Find the position of the first bit set to the given value.
///
/// Returns None if no such bit is found.
pub fn bitpos(data: &[u8], bit: u8, start: Option<i64>, end: Option<i64>, bit_mode: bool) -> Option<i64> {
    if data.is_empty() {
        // For empty string, if looking for 0, return 0; if looking for 1, return -1 (not found)
        return if bit == 0 { Some(0) } else { None };
    }

    let len = data.len() as i64;

    // Determine search range
    let (start_pos, end_pos, range_specified) = if bit_mode {
        // BIT mode: positions are bit indices
        let bit_len = len * 8;
        let s = start.unwrap_or(0);
        let e = end.unwrap_or(bit_len - 1);
        let range_specified = start.is_some() || end.is_some();

        // Handle negative indices
        let s = if s < 0 { (bit_len + s).max(0) } else { s.min(bit_len) };
        let e = if e < 0 { (bit_len + e).max(0) } else { e.min(bit_len - 1) };

        if s > e {
            return None;
        }

        (s as u64, e as u64, range_specified)
    } else {
        // BYTE mode (default): positions are byte indices
        let s = start.unwrap_or(0);
        let e = end.unwrap_or(len - 1);
        let range_specified = start.is_some() || end.is_some();

        // Handle negative indices
        let s = if s < 0 { (len + s).max(0) } else { s.min(len) };
        let e = if e < 0 { (len + e).max(0) } else { e.min(len - 1) };

        if s > e {
            return None;
        }

        // Convert to bit positions
        (s as u64 * 8, (e as u64 + 1) * 8 - 1, range_specified)
    };

    // Search for the bit
    for bit_offset in start_pos..=end_pos {
        let found_bit = getbit(data, bit_offset);
        if found_bit == bit {
            return Some(bit_offset as i64);
        }
    }

    // Not found in range
    if bit == 0 && !range_specified {
        // If looking for 0 without explicit range, return the first position after the string
        Some(len * 8)
    } else {
        None
    }
}

/// Read a value from a bitfield.
pub fn bitfield_get(data: &[u8], encoding: BitfieldEncoding, offset: u64) -> i64 {
    let bits = encoding.bits() as u64;
    let mut value: u64 = 0;

    for i in 0..bits {
        let bit = getbit(data, offset + i);
        value = (value << 1) | (bit as u64);
    }

    if encoding.is_signed() {
        // Sign-extend if the high bit is set
        let sign_bit = 1u64 << (bits - 1);
        if value & sign_bit != 0 {
            // Extend sign to 64 bits
            let mask = !((1u64 << bits) - 1);
            value |= mask;
        }
        value as i64
    } else {
        value as i64
    }
}

/// Write a value to a bitfield.
///
/// Returns the old value.
pub fn bitfield_set(data: &mut Vec<u8>, encoding: BitfieldEncoding, offset: u64, value: i64) -> i64 {
    let old_value = bitfield_get(data, encoding, offset);

    let bits = encoding.bits() as u64;
    let value_bits = value as u64;

    for i in 0..bits {
        let bit_pos = bits - 1 - i;
        let bit = ((value_bits >> bit_pos) & 1) as u8;
        setbit(data, offset + i, bit);
    }

    old_value
}

/// Increment a bitfield value.
///
/// Returns (new_value, overflowed).
pub fn bitfield_incrby(
    data: &mut Vec<u8>,
    encoding: BitfieldEncoding,
    offset: u64,
    increment: i64,
    overflow: OverflowMode,
) -> (Option<i64>, bool) {
    let old_value = bitfield_get(data, encoding, offset);
    let bits = encoding.bits();

    let (new_value, overflowed) = if encoding.is_signed() {
        let min = -(1i64 << (bits - 1));
        let max = (1i64 << (bits - 1)) - 1;

        match old_value.checked_add(increment) {
            Some(v) if v >= min && v <= max => (v, false),
            Some(v) => {
                // Overflow occurred
                match overflow {
                    OverflowMode::Wrap => {
                        let range = 1i128 << bits;
                        let wrapped = ((v as i128 - min as i128) % range + range) % range + min as i128;
                        (wrapped as i64, true)
                    }
                    OverflowMode::Sat => {
                        if v > max {
                            (max, true)
                        } else {
                            (min, true)
                        }
                    }
                    OverflowMode::Fail => return (None, true),
                }
            }
            None => {
                // Arithmetic overflow - apply overflow mode
                match overflow {
                    OverflowMode::Wrap => {
                        let v = (old_value as i128).wrapping_add(increment as i128);
                        let range = 1i128 << bits;
                        let min128 = min as i128;
                        let wrapped = ((v - min128) % range + range) % range + min128;
                        (wrapped as i64, true)
                    }
                    OverflowMode::Sat => {
                        if increment > 0 {
                            (max, true)
                        } else {
                            (min, true)
                        }
                    }
                    OverflowMode::Fail => return (None, true),
                }
            }
        }
    } else {
        let max = (1u64 << bits) - 1;

        let old_u = old_value as u64 & max;
        let inc_u = increment as u64;

        match old_u.checked_add(inc_u) {
            Some(v) if v <= max => (v as i64, false),
            Some(v) => {
                match overflow {
                    OverflowMode::Wrap => ((v & max) as i64, true),
                    OverflowMode::Sat => (max as i64, true),
                    OverflowMode::Fail => return (None, true),
                }
            }
            None => {
                match overflow {
                    OverflowMode::Wrap => {
                        let v = old_u.wrapping_add(inc_u) & max;
                        (v as i64, true)
                    }
                    OverflowMode::Sat => (max as i64, true),
                    OverflowMode::Fail => return (None, true),
                }
            }
        }
    };

    bitfield_set(data, encoding, offset, new_value);
    (Some(new_value), overflowed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_getbit() {
        let data = vec![0b10110100, 0b01001011];
        assert_eq!(getbit(&data, 0), 1);
        assert_eq!(getbit(&data, 1), 0);
        assert_eq!(getbit(&data, 2), 1);
        assert_eq!(getbit(&data, 3), 1);
        assert_eq!(getbit(&data, 4), 0);
        assert_eq!(getbit(&data, 5), 1);
        assert_eq!(getbit(&data, 6), 0);
        assert_eq!(getbit(&data, 7), 0);
        // Second byte
        assert_eq!(getbit(&data, 8), 0);
        assert_eq!(getbit(&data, 9), 1);
        // Beyond end
        assert_eq!(getbit(&data, 100), 0);
    }

    #[test]
    fn test_setbit() {
        let mut data = vec![0u8];
        assert_eq!(setbit(&mut data, 0, 1), 0);
        assert_eq!(data[0], 0b10000000);

        assert_eq!(setbit(&mut data, 7, 1), 0);
        assert_eq!(data[0], 0b10000001);

        assert_eq!(setbit(&mut data, 0, 0), 1);
        assert_eq!(data[0], 0b00000001);

        // Auto-extend
        assert_eq!(setbit(&mut data, 15, 1), 0);
        assert_eq!(data.len(), 2);
        assert_eq!(data[1], 0b00000001);
    }

    #[test]
    fn test_bitcount() {
        let data = vec![0b11111111, 0b00000000, 0b10101010];
        assert_eq!(bitcount(&data, None, None, false), 12);
        assert_eq!(bitcount(&data, Some(0), Some(0), false), 8);
        assert_eq!(bitcount(&data, Some(1), Some(1), false), 0);
        assert_eq!(bitcount(&data, Some(2), Some(2), false), 4);
        assert_eq!(bitcount(&data, Some(-1), Some(-1), false), 4);
    }

    #[test]
    fn test_bitpos() {
        let data = vec![0b00000000, 0b11111111];
        // First 1 bit
        assert_eq!(bitpos(&data, 1, None, None, false), Some(8));
        // First 0 bit
        assert_eq!(bitpos(&data, 0, None, None, false), Some(0));

        let data2 = vec![0b11111111];
        // Looking for 0 without range - returns position after string
        assert_eq!(bitpos(&data2, 0, None, None, false), Some(8));
    }

    #[test]
    fn test_bitop_and() {
        let a = [0b11110000u8];
        let b = [0b10101010u8];
        let result = bitop(BitOp::And, &[&a, &b]);
        assert_eq!(result, vec![0b10100000]);
    }

    #[test]
    fn test_bitop_or() {
        let a = [0b11110000u8];
        let b = [0b10101010u8];
        let result = bitop(BitOp::Or, &[&a, &b]);
        assert_eq!(result, vec![0b11111010]);
    }

    #[test]
    fn test_bitop_xor() {
        let a = [0b11110000u8];
        let b = [0b10101010u8];
        let result = bitop(BitOp::Xor, &[&a, &b]);
        assert_eq!(result, vec![0b01011010]);
    }

    #[test]
    fn test_bitop_not() {
        let a = [0b11110000u8];
        let result = bitop(BitOp::Not, &[&a]);
        assert_eq!(result, vec![0b00001111]);
    }

    #[test]
    fn test_bitfield_encoding() {
        assert_eq!(
            BitfieldEncoding::parse(b"i8"),
            Some(BitfieldEncoding::Signed(8))
        );
        assert_eq!(
            BitfieldEncoding::parse(b"u16"),
            Some(BitfieldEncoding::Unsigned(16))
        );
        assert_eq!(BitfieldEncoding::parse(b"u0"), None);
        assert_eq!(BitfieldEncoding::parse(b"u64"), None); // Unsigned max is 63
        assert_eq!(
            BitfieldEncoding::parse(b"i64"),
            Some(BitfieldEncoding::Signed(64))
        );
    }

    #[test]
    fn test_bitfield_offset() {
        assert_eq!(
            BitfieldOffset::parse(b"0"),
            Some(BitfieldOffset::Absolute(0))
        );
        assert_eq!(
            BitfieldOffset::parse(b"#0"),
            Some(BitfieldOffset::Indexed(0))
        );
        assert_eq!(
            BitfieldOffset::parse(b"#5"),
            Some(BitfieldOffset::Indexed(5))
        );
    }

    #[test]
    fn test_bitfield_get_set() {
        let mut data = vec![0u8; 4];

        // Set 8-bit unsigned value
        bitfield_set(&mut data, BitfieldEncoding::Unsigned(8), 0, 200);
        assert_eq!(data[0], 200);

        // Get it back
        let val = bitfield_get(&data, BitfieldEncoding::Unsigned(8), 0);
        assert_eq!(val, 200);

        // Signed 8-bit
        bitfield_set(&mut data, BitfieldEncoding::Signed(8), 8, -50);
        let val = bitfield_get(&data, BitfieldEncoding::Signed(8), 8);
        assert_eq!(val, -50);
    }

    #[test]
    fn test_bitfield_incrby_wrap() {
        let mut data = vec![0xFFu8]; // 255 in unsigned 8-bit

        let (result, overflowed) = bitfield_incrby(
            &mut data,
            BitfieldEncoding::Unsigned(8),
            0,
            1,
            OverflowMode::Wrap,
        );
        assert_eq!(result, Some(0));
        assert!(overflowed);
    }

    #[test]
    fn test_bitfield_incrby_sat() {
        let mut data = vec![0xFFu8]; // 255 in unsigned 8-bit

        let (result, overflowed) = bitfield_incrby(
            &mut data,
            BitfieldEncoding::Unsigned(8),
            0,
            1,
            OverflowMode::Sat,
        );
        assert_eq!(result, Some(255));
        assert!(overflowed);
    }

    #[test]
    fn test_bitfield_incrby_fail() {
        let mut data = vec![0xFFu8]; // 255 in unsigned 8-bit

        let (result, overflowed) = bitfield_incrby(
            &mut data,
            BitfieldEncoding::Unsigned(8),
            0,
            1,
            OverflowMode::Fail,
        );
        assert_eq!(result, None);
        assert!(overflowed);
    }
}
