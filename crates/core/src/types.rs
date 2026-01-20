//! Value types and key metadata.

use bytes::Bytes;
use std::time::Instant;

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
}
