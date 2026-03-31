//\! String value type with optional integer encoding and bitmap operations.

use bytes::Bytes;

/// String value with optional integer encoding.
#[derive(Debug, Clone)]
pub struct StringValue {
    data: StringData,
}

#[derive(Debug, Clone)]
enum StringData {
    /// Raw byte string.
    Raw(Bytes),
    /// Integer value (for efficient INCR/DECR).
    Integer(i64),
}

impl StringValue {
    /// Create a new string value from bytes.
    pub fn new(data: impl Into<Bytes>) -> Self {
        let bytes = data.into();
        // Try to parse as integer for efficient storage.
        // The round-trip check (i.to_string() == original) ensures we don't
        // lose information like leading zeros (e.g. "007" → 7 → "7").
        if let Ok(s) = std::str::from_utf8(&bytes)
            && let Ok(i) = s.parse::<i64>()
            && i.to_string().as_bytes() == bytes.as_ref()
        {
            return Self {
                data: StringData::Integer(i),
            };
        }
        Self {
            data: StringData::Raw(bytes),
        }
    }

    /// Create a string value from an integer.
    pub fn from_integer(i: i64) -> Self {
        Self {
            data: StringData::Integer(i),
        }
    }

    /// Get the value as bytes.
    pub fn as_bytes(&self) -> Bytes {
        match &self.data {
            StringData::Raw(b) => b.clone(),
            StringData::Integer(i) => Bytes::from(i.to_string()),
        }
    }

    /// Try to get the value as an integer.
    pub fn as_integer(&self) -> Option<i64> {
        match &self.data {
            StringData::Integer(i) => Some(*i),
            StringData::Raw(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        }
    }

    /// Calculate memory size.
    pub fn memory_size(&self) -> usize {
        match &self.data {
            StringData::Raw(b) => b.len(),
            StringData::Integer(_) => 8, // i64
        }
    }

    /// Get the byte length of the string.
    pub fn len(&self) -> usize {
        match &self.data {
            StringData::Raw(b) => b.len(),
            StringData::Integer(i) => i.to_string().len(),
        }
    }

    /// Check if the string is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append bytes to the string.
    ///
    /// Returns the new length of the string.
    /// Note: This converts integer-encoded values to raw bytes.
    pub fn append(&mut self, data: &[u8]) -> usize {
        let mut current = self.as_bytes().to_vec();
        current.extend_from_slice(data);
        let new_len = current.len();

        // After append, we might still have an integer but it's unlikely,
        // and appending typically makes it non-integer anyway
        *self = Self::new(Bytes::from(current));
        new_len
    }

    /// Get a substring by byte indices.
    ///
    /// Supports negative indices (like Python/Redis):
    /// - -1 is the last character
    /// - -2 is second-to-last, etc.
    pub fn get_range(&self, start: i64, end: i64) -> Bytes {
        let bytes = self.as_bytes();
        let len = bytes.len() as i64;

        if len == 0 {
            return Bytes::new();
        }

        // Redis early check: both negative and start > end → empty
        if start < 0 && end < 0 && start > end {
            return Bytes::new();
        }

        // Convert negative indices to positive
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            (start as usize).min(len as usize)
        };

        let end = if end < 0 {
            (len + end).max(0) as usize
        } else {
            (end as usize).min(len as usize - 1)
        };

        // If start > end after conversion, return empty
        if start > end {
            return Bytes::new();
        }

        // end is inclusive in Redis GETRANGE
        bytes.slice(start..=end)
    }

    /// Overwrite part of the string at the given offset.
    ///
    /// If offset is past the end, the string is padded with null bytes.
    /// Returns the new length of the string.
    pub fn set_range(&mut self, offset: usize, data: &[u8]) -> usize {
        let mut current = self.as_bytes().to_vec();

        // Pad with zeros if offset is past end
        if offset > current.len() {
            current.resize(offset, 0);
        }

        // Replace or extend
        let end_pos = offset + data.len();
        if end_pos > current.len() {
            current.resize(end_pos, 0);
        }

        current[offset..end_pos].copy_from_slice(data);
        let new_len = current.len();

        *self = Self::new(Bytes::from(current));
        new_len
    }

    /// Increment the value by delta.
    ///
    /// Returns the new value or an error if not an integer.
    pub fn increment(&mut self, delta: i64) -> Result<i64, IncrementError> {
        let current = self.as_integer().ok_or(IncrementError::NotInteger)?;
        let new_val = current.checked_add(delta).ok_or(IncrementError::Overflow)?;
        self.data = StringData::Integer(new_val);
        Ok(new_val)
    }

    /// Increment the value by a float delta.
    ///
    /// Returns the new value or an error if not a valid float.
    pub fn increment_float(&mut self, delta: f64) -> Result<f64, IncrementError> {
        let current = self.as_float().ok_or(IncrementError::NotFloat)?;

        // Reject stored values that are already infinite or NaN (e.g. "inf", "nan")
        if current.is_infinite() || current.is_nan() {
            return Err(IncrementError::NotFloat);
        }

        let new_val = current + delta;

        // Check for infinity or NaN result
        if new_val.is_infinite() || new_val.is_nan() {
            return Err(IncrementError::Overflow);
        }

        // Store as raw string (floats are always stored as strings in Redis)
        self.data = StringData::Raw(Bytes::from(format_float(new_val)));
        Ok(new_val)
    }

    /// Try to get the value as a float.
    pub fn as_float(&self) -> Option<f64> {
        match &self.data {
            StringData::Integer(i) => Some(*i as f64),
            StringData::Raw(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        }
    }

    // ========================================================================
    // Bitmap operations
    // ========================================================================

    /// Get a bit value at the given offset.
    ///
    /// Uses MSB-first bit ordering (offset 0 = bit 7 of byte 0).
    /// Returns 0 for offsets beyond the string length.
    pub fn getbit(&self, offset: u64) -> u8 {
        let bytes = self.as_bytes();
        crate::bitmap::getbit(&bytes, offset)
    }

    /// Set a bit value at the given offset.
    ///
    /// Returns the previous bit value.
    /// Auto-extends the string with zeros if offset is beyond the end.
    pub fn setbit(&mut self, offset: u64, value: u8) -> u8 {
        let mut bytes = self.as_bytes().to_vec();
        let old_bit = crate::bitmap::setbit(&mut bytes, offset, value);
        self.data = StringData::Raw(Bytes::from(bytes));
        old_bit
    }

    /// Count the number of set bits in a range.
    ///
    /// If bit_mode is true, start/end are bit positions; otherwise byte positions.
    pub fn bitcount(&self, start: Option<i64>, end: Option<i64>, bit_mode: bool) -> u64 {
        let bytes = self.as_bytes();
        crate::bitmap::bitcount(&bytes, start, end, bit_mode)
    }

    /// Find the position of the first bit set to the given value.
    pub fn bitpos(
        &self,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        bit_mode: bool,
        end_given: bool,
    ) -> Option<i64> {
        let bytes = self.as_bytes();
        crate::bitmap::bitpos(&bytes, bit, start, end, bit_mode, end_given)
    }

    /// Get the raw bytes as a mutable vector for bitfield operations.
    pub fn as_bytes_vec(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    /// Set the raw bytes from a vector (used after bitfield operations).
    pub fn set_bytes(&mut self, bytes: Vec<u8>) {
        self.data = StringData::Raw(Bytes::from(bytes));
    }
}

/// Error type for increment operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementError {
    /// Value is not an integer.
    NotInteger,
    /// Value is not a valid float.
    NotFloat,
    /// Operation would overflow.
    Overflow,
}

impl std::fmt::Display for IncrementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncrementError::NotInteger => write!(f, "ERR value is not an integer or out of range"),
            IncrementError::NotFloat => write!(f, "ERR value is not a valid float"),
            IncrementError::Overflow => write!(f, "ERR increment or decrement would overflow"),
        }
    }
}

impl std::error::Error for IncrementError {}

/// Format a float for Redis compatibility.
///
/// Redis uses specific formatting rules:
/// - No trailing zeros after decimal point
/// - Scientific notation for very large/small numbers
pub(super) fn format_float(f: f64) -> String {
    // Handle special cases
    if f == 0.0 {
        return "0".to_string();
    }

    // Check if it's a whole number
    if f.fract() == 0.0 && f.abs() < 1e15 {
        return format!("{:.0}", f);
    }

    // Use standard formatting, then trim trailing zeros
    let s = format!("{:.17}", f);
    let s = s.trim_end_matches('0');
    let s = s.trim_end_matches('.');
    s.to_string()
}
