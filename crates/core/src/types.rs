//! Value types and key metadata.

use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Value types stored in FrogDB.
#[derive(Debug, Clone)]
pub enum Value {
    /// String value (the only type for Phase 1).
    String(StringValue),
    // Future types:
    // List(ListValue),
    // Set(SetValue),
    // Hash(HashValue),
    // SortedSet(SortedSetValue),
    // Stream(StreamValue),
}

impl Value {
    /// Create a string value from bytes.
    pub fn string(data: impl Into<Bytes>) -> Self {
        Value::String(StringValue::new(data))
    }

    /// Get the key type.
    pub fn key_type(&self) -> KeyType {
        match self {
            Value::String(_) => KeyType::String,
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        match self {
            Value::String(s) => s.memory_size(),
        }
    }

    /// Try to get as a string value.
    pub fn as_string(&self) -> Option<&StringValue> {
        match self {
            Value::String(s) => Some(s),
        }
    }

    /// Try to get as a mutable string value.
    pub fn as_string_mut(&mut self) -> Option<&mut StringValue> {
        match self {
            Value::String(s) => Some(s),
        }
    }
}

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
        // Try to parse as integer for efficient storage
        if let Ok(s) = std::str::from_utf8(&bytes) {
            if let Ok(i) = s.parse::<i64>() {
                return Self {
                    data: StringData::Integer(i),
                };
            }
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
        let new_val = current
            .checked_add(delta)
            .ok_or(IncrementError::Overflow)?;
        self.data = StringData::Integer(new_val);
        Ok(new_val)
    }

    /// Increment the value by a float delta.
    ///
    /// Returns the new value or an error if not a valid float.
    pub fn increment_float(&mut self, delta: f64) -> Result<f64, IncrementError> {
        let current = self.as_float().ok_or(IncrementError::NotFloat)?;
        let new_val = current + delta;

        // Check for infinity or NaN
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

/// Format a float for Redis compatibility.
///
/// Redis uses specific formatting rules:
/// - No trailing zeros after decimal point
/// - Scientific notation for very large/small numbers
fn format_float(f: f64) -> String {
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

/// Type identifier for TYPE command and WRONGTYPE errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    /// Key doesn't exist.
    None,
    /// String type.
    String,
    /// List type.
    List,
    /// Set type.
    Set,
    /// Hash type.
    Hash,
    /// Sorted set type.
    SortedSet,
    /// Stream type.
    Stream,
}

impl KeyType {
    /// Get the Redis type name.
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyType::None => "none",
            KeyType::String => "string",
            KeyType::List => "list",
            KeyType::Set => "set",
            KeyType::Hash => "hash",
            KeyType::SortedSet => "zset",
            KeyType::Stream => "stream",
        }
    }
}

/// Metadata tracked per key.
#[derive(Debug, Clone)]
pub struct KeyMetadata {
    /// Expiration time (None = no expiry).
    pub expires_at: Option<Instant>,

    /// Last access time (for LRU eviction).
    pub last_access: Instant,

    /// Access frequency counter (for LFU eviction).
    pub lfu_counter: u8,

    /// Approximate memory size of this entry.
    pub memory_size: usize,
}

impl KeyMetadata {
    /// Create new metadata with defaults.
    pub fn new(memory_size: usize) -> Self {
        Self {
            expires_at: None,
            last_access: Instant::now(),
            lfu_counter: 5, // New keys start at 5 (not immediately evicted)
            memory_size,
        }
    }

    /// Check if the key is expired.
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|exp| exp <= Instant::now())
            .unwrap_or(false)
    }

    /// Update last access time.
    pub fn touch(&mut self) {
        self.last_access = Instant::now();
    }
}

// ============================================================================
// SET Command Options
// ============================================================================

/// Options for the SET command.
#[derive(Debug, Clone, Default)]
pub struct SetOptions {
    /// Expiration setting.
    pub expiry: Option<Expiry>,
    /// Conditional set (NX/XX).
    pub condition: SetCondition,
    /// Keep existing TTL when updating.
    pub keep_ttl: bool,
    /// Return the old value (GET flag).
    pub return_old: bool,
}

/// Expiration specification for SET command.
#[derive(Debug, Clone, Copy)]
pub enum Expiry {
    /// Expire in N seconds (EX).
    Ex(u64),
    /// Expire in N milliseconds (PX).
    Px(u64),
    /// Expire at Unix timestamp in seconds (EXAT).
    ExAt(u64),
    /// Expire at Unix timestamp in milliseconds (PXAT).
    PxAt(u64),
}

impl Expiry {
    /// Convert to an absolute Instant for storage.
    pub fn to_instant(&self) -> Option<Instant> {
        let now = Instant::now();
        let system_now = SystemTime::now();

        match self {
            Expiry::Ex(secs) => Some(now + Duration::from_secs(*secs)),
            Expiry::Px(ms) => Some(now + Duration::from_millis(*ms)),
            Expiry::ExAt(ts) => {
                let target = UNIX_EPOCH + Duration::from_secs(*ts);
                if let Ok(duration) = target.duration_since(system_now) {
                    Some(now + duration)
                } else {
                    // Already expired
                    Some(now)
                }
            }
            Expiry::PxAt(ts) => {
                let target = UNIX_EPOCH + Duration::from_millis(*ts);
                if let Ok(duration) = target.duration_since(system_now) {
                    Some(now + duration)
                } else {
                    // Already expired
                    Some(now)
                }
            }
        }
    }
}

/// Conditional set behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SetCondition {
    /// Always set (default behavior).
    #[default]
    Always,
    /// Only set if key does Not eXist.
    NX,
    /// Only set if key already eXists.
    XX,
}

/// Result of a SET operation.
#[derive(Debug, Clone)]
pub enum SetResult {
    /// Set was successful, returns OK.
    Ok,
    /// Set was successful, returns the old value (for GET flag).
    OkWithOldValue(Option<Value>),
    /// Set was not performed (NX/XX condition not met).
    NotSet,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_value_raw() {
        let sv = StringValue::new("hello");
        assert_eq!(sv.as_bytes().as_ref(), b"hello");
        assert!(sv.as_integer().is_none());
    }

    #[test]
    fn test_string_value_integer() {
        let sv = StringValue::new("42");
        assert_eq!(sv.as_integer(), Some(42));
        assert_eq!(sv.as_bytes().as_ref(), b"42");
    }

    #[test]
    fn test_string_value_from_integer() {
        let sv = StringValue::from_integer(-100);
        assert_eq!(sv.as_integer(), Some(-100));
        assert_eq!(sv.as_bytes().as_ref(), b"-100");
    }

    #[test]
    fn test_key_type_as_str() {
        assert_eq!(KeyType::String.as_str(), "string");
        assert_eq!(KeyType::None.as_str(), "none");
    }

    #[test]
    fn test_string_value_len() {
        let sv = StringValue::new("hello");
        assert_eq!(sv.len(), 5);
        assert!(!sv.is_empty());

        let sv_int = StringValue::from_integer(12345);
        assert_eq!(sv_int.len(), 5);

        let sv_empty = StringValue::new("");
        assert_eq!(sv_empty.len(), 0);
        assert!(sv_empty.is_empty());
    }

    #[test]
    fn test_string_value_append() {
        let mut sv = StringValue::new("hello");
        let new_len = sv.append(b" world");
        assert_eq!(new_len, 11);
        assert_eq!(sv.as_bytes().as_ref(), b"hello world");
    }

    #[test]
    fn test_string_value_get_range() {
        let sv = StringValue::new("hello world");

        // Positive indices
        assert_eq!(sv.get_range(0, 4).as_ref(), b"hello");
        assert_eq!(sv.get_range(6, 10).as_ref(), b"world");

        // Negative indices
        assert_eq!(sv.get_range(-5, -1).as_ref(), b"world");
        assert_eq!(sv.get_range(0, -1).as_ref(), b"hello world");

        // Out of range
        assert_eq!(sv.get_range(0, 100).as_ref(), b"hello world");

        // Empty result
        assert_eq!(sv.get_range(5, 2).as_ref(), b"");
    }

    #[test]
    fn test_string_value_set_range() {
        let mut sv = StringValue::new("hello world");
        let new_len = sv.set_range(6, b"WORLD");
        assert_eq!(new_len, 11);
        assert_eq!(sv.as_bytes().as_ref(), b"hello WORLD");

        // Extend past end
        let mut sv2 = StringValue::new("hello");
        let new_len = sv2.set_range(10, b"world");
        assert_eq!(new_len, 15);
        // Should have null padding between "hello" and "world"
        let bytes = sv2.as_bytes();
        assert_eq!(&bytes[0..5], b"hello");
        assert_eq!(&bytes[10..15], b"world");
    }

    #[test]
    fn test_string_value_increment() {
        let mut sv = StringValue::from_integer(10);
        assert_eq!(sv.increment(5).unwrap(), 15);
        assert_eq!(sv.as_integer(), Some(15));

        // Negative increment
        assert_eq!(sv.increment(-20).unwrap(), -5);
        assert_eq!(sv.as_integer(), Some(-5));
    }

    #[test]
    fn test_string_value_increment_overflow() {
        let mut sv = StringValue::from_integer(i64::MAX);
        assert_eq!(sv.increment(1), Err(IncrementError::Overflow));
    }

    #[test]
    fn test_string_value_increment_not_integer() {
        let mut sv = StringValue::new("not a number");
        assert_eq!(sv.increment(1), Err(IncrementError::NotInteger));
    }

    #[test]
    fn test_string_value_increment_float() {
        let mut sv = StringValue::from_integer(10);
        let result = sv.increment_float(0.5).unwrap();
        assert!((result - 10.5).abs() < 0.001);

        // Increment a float value
        let result = sv.increment_float(1.5).unwrap();
        assert!((result - 12.0).abs() < 0.001);
    }

    #[test]
    fn test_string_value_as_float() {
        let sv_int = StringValue::from_integer(42);
        assert_eq!(sv_int.as_float(), Some(42.0));

        let sv_float = StringValue::new("3.14159");
        assert!((sv_float.as_float().unwrap() - 3.14159).abs() < 0.0001);

        let sv_invalid = StringValue::new("not a float");
        assert!(sv_invalid.as_float().is_none());
    }
}
