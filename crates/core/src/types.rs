//! Value types and key metadata.

use bytes::Bytes;
use ordered_float::OrderedFloat;
use rand::seq::SliceRandom;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::bloom::BloomFilterValue;
use crate::hyperloglog::HyperLogLogValue;
use crate::json::JsonValue;
use crate::timeseries::TimeSeriesValue;

/// Value types stored in FrogDB.
#[derive(Debug, Clone)]
pub enum Value {
    /// String value.
    String(StringValue),
    /// Sorted set value.
    SortedSet(SortedSetValue),
    /// Hash value.
    Hash(HashValue),
    /// List value.
    List(ListValue),
    /// Set value.
    Set(SetValue),
    /// Stream value.
    Stream(StreamValue),
    /// Bloom filter value.
    BloomFilter(BloomFilterValue),
    /// HyperLogLog value.
    HyperLogLog(HyperLogLogValue),
    /// Time series value.
    TimeSeries(TimeSeriesValue),
    /// JSON document value.
    Json(JsonValue),
}

impl Value {
    /// Create a string value from bytes.
    pub fn string(data: impl Into<Bytes>) -> Self {
        Value::String(StringValue::new(data))
    }

    /// Create a sorted set value.
    pub fn sorted_set() -> Self {
        Value::SortedSet(SortedSetValue::new())
    }

    /// Create a hash value.
    pub fn hash() -> Self {
        Value::Hash(HashValue::new())
    }

    /// Create a list value.
    pub fn list() -> Self {
        Value::List(ListValue::new())
    }

    /// Create a set value.
    pub fn set() -> Self {
        Value::Set(SetValue::new())
    }

    /// Create a stream value.
    pub fn stream() -> Self {
        Value::Stream(StreamValue::new())
    }

    /// Create a bloom filter value with default settings.
    pub fn bloom_filter(capacity: u64, error_rate: f64) -> Self {
        Value::BloomFilter(BloomFilterValue::new(capacity, error_rate))
    }

    /// Create a HyperLogLog value.
    pub fn hyperloglog() -> Self {
        Value::HyperLogLog(HyperLogLogValue::new())
    }

    /// Create a time series value.
    pub fn timeseries() -> Self {
        Value::TimeSeries(TimeSeriesValue::new())
    }

    /// Create a JSON value.
    pub fn json(data: serde_json::Value) -> Self {
        Value::Json(JsonValue::new(data))
    }

    /// Get the key type.
    pub fn key_type(&self) -> KeyType {
        match self {
            Value::String(_) => KeyType::String,
            Value::SortedSet(_) => KeyType::SortedSet,
            Value::Hash(_) => KeyType::Hash,
            Value::List(_) => KeyType::List,
            Value::Set(_) => KeyType::Set,
            Value::Stream(_) => KeyType::Stream,
            Value::BloomFilter(_) => KeyType::BloomFilter,
            Value::HyperLogLog(_) => KeyType::HyperLogLog,
            Value::TimeSeries(_) => KeyType::TimeSeries,
            Value::Json(_) => KeyType::Json,
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        match self {
            Value::String(s) => s.memory_size(),
            Value::SortedSet(z) => z.memory_size(),
            Value::Hash(h) => h.memory_size(),
            Value::List(l) => l.memory_size(),
            Value::Set(s) => s.memory_size(),
            Value::Stream(st) => st.memory_size(),
            Value::BloomFilter(bf) => bf.memory_size(),
            Value::HyperLogLog(hll) => hll.memory_size(),
            Value::TimeSeries(ts) => ts.memory_size(),
            Value::Json(j) => j.memory_size(),
        }
    }

    /// Try to get as a string value.
    pub fn as_string(&self) -> Option<&StringValue> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as a mutable string value.
    pub fn as_string_mut(&mut self) -> Option<&mut StringValue> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as a sorted set value.
    pub fn as_sorted_set(&self) -> Option<&SortedSetValue> {
        match self {
            Value::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    /// Try to get as a mutable sorted set value.
    pub fn as_sorted_set_mut(&mut self) -> Option<&mut SortedSetValue> {
        match self {
            Value::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    /// Try to get as a hash value.
    pub fn as_hash(&self) -> Option<&HashValue> {
        match self {
            Value::Hash(h) => Some(h),
            _ => None,
        }
    }

    /// Try to get as a mutable hash value.
    pub fn as_hash_mut(&mut self) -> Option<&mut HashValue> {
        match self {
            Value::Hash(h) => Some(h),
            _ => None,
        }
    }

    /// Try to get as a list value.
    pub fn as_list(&self) -> Option<&ListValue> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    /// Try to get as a mutable list value.
    pub fn as_list_mut(&mut self) -> Option<&mut ListValue> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    /// Try to get as a set value.
    pub fn as_set(&self) -> Option<&SetValue> {
        match self {
            Value::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as a mutable set value.
    pub fn as_set_mut(&mut self) -> Option<&mut SetValue> {
        match self {
            Value::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as a stream value.
    pub fn as_stream(&self) -> Option<&StreamValue> {
        match self {
            Value::Stream(st) => Some(st),
            _ => None,
        }
    }

    /// Try to get as a mutable stream value.
    pub fn as_stream_mut(&mut self) -> Option<&mut StreamValue> {
        match self {
            Value::Stream(st) => Some(st),
            _ => None,
        }
    }

    /// Try to get as a bloom filter value.
    pub fn as_bloom_filter(&self) -> Option<&BloomFilterValue> {
        match self {
            Value::BloomFilter(bf) => Some(bf),
            _ => None,
        }
    }

    /// Try to get as a mutable bloom filter value.
    pub fn as_bloom_filter_mut(&mut self) -> Option<&mut BloomFilterValue> {
        match self {
            Value::BloomFilter(bf) => Some(bf),
            _ => None,
        }
    }

    /// Try to get as a HyperLogLog value.
    pub fn as_hyperloglog(&self) -> Option<&HyperLogLogValue> {
        match self {
            Value::HyperLogLog(hll) => Some(hll),
            _ => None,
        }
    }

    /// Try to get as a mutable HyperLogLog value.
    pub fn as_hyperloglog_mut(&mut self) -> Option<&mut HyperLogLogValue> {
        match self {
            Value::HyperLogLog(hll) => Some(hll),
            _ => None,
        }
    }

    /// Try to get as a time series value.
    pub fn as_timeseries(&self) -> Option<&TimeSeriesValue> {
        match self {
            Value::TimeSeries(ts) => Some(ts),
            _ => None,
        }
    }

    /// Try to get as a mutable time series value.
    pub fn as_timeseries_mut(&mut self) -> Option<&mut TimeSeriesValue> {
        match self {
            Value::TimeSeries(ts) => Some(ts),
            _ => None,
        }
    }

    /// Try to get as a JSON value.
    pub fn as_json(&self) -> Option<&JsonValue> {
        match self {
            Value::Json(j) => Some(j),
            _ => None,
        }
    }

    /// Try to get as a mutable JSON value.
    pub fn as_json_mut(&mut self) -> Option<&mut JsonValue> {
        match self {
            Value::Json(j) => Some(j),
            _ => None,
        }
    }

    /// Serialize this value for cross-shard copy operations.
    /// Returns (type_string, serialized_bytes).
    pub fn serialize_for_copy(&self) -> (&'static str, Bytes) {
        match self {
            Value::String(s) => ("string", s.as_bytes()),
            Value::Hash(h) => {
                // Serialize as: num_fields || (key_len || key || value_len || value)*
                let mut buf = Vec::new();
                let fields: Vec<_> = h.iter().collect();
                buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
                for (k, v) in fields {
                    buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
                    buf.extend_from_slice(k);
                    buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    buf.extend_from_slice(v);
                }
                ("hash", Bytes::from(buf))
            }
            Value::List(l) => {
                // Serialize as: num_elements || (element_len || element)*
                let mut buf = Vec::new();
                let elements: Vec<_> = l.iter().collect();
                buf.extend_from_slice(&(elements.len() as u32).to_le_bytes());
                for elem in elements {
                    buf.extend_from_slice(&(elem.len() as u32).to_le_bytes());
                    buf.extend_from_slice(elem);
                }
                ("list", Bytes::from(buf))
            }
            Value::Set(s) => {
                // Serialize as: num_members || (member_len || member)*
                let mut buf = Vec::new();
                let members: Vec<_> = s.members().collect();
                buf.extend_from_slice(&(members.len() as u32).to_le_bytes());
                for member in members {
                    buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
                    buf.extend_from_slice(member);
                }
                ("set", Bytes::from(buf))
            }
            Value::SortedSet(z) => {
                // Serialize as: num_members || (member_len || member || score)*
                let mut buf = Vec::new();
                let members: Vec<(&Bytes, f64)> = z.iter().collect();
                buf.extend_from_slice(&(members.len() as u32).to_le_bytes());
                for (member, score) in members {
                    buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
                    buf.extend_from_slice(member);
                    buf.extend_from_slice(&score.to_le_bytes());
                }
                ("zset", Bytes::from(buf))
            }
            Value::Stream(_) => {
                // Streams are complex - for now return empty
                // TODO: implement proper stream serialization
                ("stream", Bytes::new())
            }
            Value::BloomFilter(_) => {
                // Bloom filters have internal state that's hard to serialize simply
                // TODO: implement proper bloom filter serialization
                ("bloom", Bytes::new())
            }
            Value::HyperLogLog(hll) => {
                // Serialize the raw registers
                ("hll", Bytes::from(hll.serialize()))
            }
            Value::TimeSeries(_) => {
                // Time series has complex state
                // TODO: implement proper time series serialization
                ("timeseries", Bytes::new())
            }
            Value::Json(j) => {
                // Serialize JSON to string using serde_json
                let json_str = serde_json::to_string(j.data()).unwrap_or_default();
                ("json", Bytes::from(json_str))
            }
        }
    }

    /// Deserialize a value from cross-shard copy data.
    /// Returns None if deserialization fails.
    pub fn deserialize_for_copy(type_str: &[u8], data: &[u8]) -> Option<Self> {
        match type_str {
            b"string" => Some(Value::String(StringValue::new(Bytes::copy_from_slice(data)))),
            b"hash" => {
                if data.len() < 4 {
                    return None;
                }
                let mut pos = 0;
                let num_fields = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut hash = HashValue::new();
                for _ in 0..num_fields {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + key_len > data.len() {
                        return None;
                    }
                    let key = Bytes::copy_from_slice(&data[pos..pos + key_len]);
                    pos += key_len;

                    if pos + 4 > data.len() {
                        return None;
                    }
                    let value_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + value_len > data.len() {
                        return None;
                    }
                    let value = Bytes::copy_from_slice(&data[pos..pos + value_len]);
                    pos += value_len;

                    hash.set(key, value);
                }
                Some(Value::Hash(hash))
            }
            b"list" => {
                if data.len() < 4 {
                    return None;
                }
                let mut pos = 0;
                let num_elements =
                    u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut list = ListValue::new();
                for _ in 0..num_elements {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let elem_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + elem_len > data.len() {
                        return None;
                    }
                    let elem = Bytes::copy_from_slice(&data[pos..pos + elem_len]);
                    pos += elem_len;
                    list.push_back(elem);
                }
                Some(Value::List(list))
            }
            b"set" => {
                if data.len() < 4 {
                    return None;
                }
                let mut pos = 0;
                let num_members = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut set = SetValue::new();
                for _ in 0..num_members {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let member_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + member_len > data.len() {
                        return None;
                    }
                    let member = Bytes::copy_from_slice(&data[pos..pos + member_len]);
                    pos += member_len;
                    set.add(member);
                }
                Some(Value::Set(set))
            }
            b"zset" => {
                if data.len() < 4 {
                    return None;
                }
                let mut pos = 0;
                let num_members = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut zset = SortedSetValue::new();
                for _ in 0..num_members {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let member_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + member_len > data.len() {
                        return None;
                    }
                    let member = Bytes::copy_from_slice(&data[pos..pos + member_len]);
                    pos += member_len;

                    if pos + 8 > data.len() {
                        return None;
                    }
                    let score = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    zset.add(member, score);
                }
                Some(Value::SortedSet(zset))
            }
            b"hll" => {
                let hll = HyperLogLogValue::deserialize(data)?;
                Some(Value::HyperLogLog(hll))
            }
            b"json" => {
                let json_str = std::str::from_utf8(data).ok()?;
                let json_value: serde_json::Value = serde_json::from_str(json_str).ok()?;
                Some(Value::Json(JsonValue::new(json_value)))
            }
            // Stream, bloom filter, and time series are not yet supported for cross-shard copy
            _ => None,
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
    pub fn bitpos(&self, bit: u8, start: Option<i64>, end: Option<i64>, bit_mode: bool) -> Option<i64> {
        let bytes = self.as_bytes();
        crate::bitmap::bitpos(&bytes, bit, start, end, bit_mode)
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
    /// Bloom filter type.
    BloomFilter,
    /// HyperLogLog type.
    HyperLogLog,
    /// Time series type.
    TimeSeries,
    /// JSON type.
    Json,
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
            KeyType::BloomFilter => "bloom",
            KeyType::HyperLogLog => "hyperloglog",
            KeyType::TimeSeries => "TSDB-TYPE",
            KeyType::Json => "ReJSON-RL",
        }
    }
}

/// Direction for list operations (BLPOP, BRPOP, BLMOVE, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Pop/push from the left (front).
    Left,
    /// Pop/push from the right (back).
    Right,
}

impl Direction {
    /// Parse direction from bytes (LEFT or RIGHT).
    pub fn parse(arg: &[u8]) -> Option<Self> {
        match arg.to_ascii_uppercase().as_slice() {
            b"LEFT" => Some(Direction::Left),
            b"RIGHT" => Some(Direction::Right),
            _ => None,
        }
    }
}

/// Blocking operation type for wait queue entries.
#[derive(Debug, Clone)]
pub enum BlockingOp {
    /// BLPOP operation.
    BLPop,
    /// BRPOP operation.
    BRPop,
    /// BLMOVE operation.
    BLMove {
        /// Destination key.
        dest: Bytes,
        /// Source direction (where to pop from).
        src_dir: Direction,
        /// Destination direction (where to push to).
        dest_dir: Direction,
    },
    /// BLMPOP operation.
    BLMPop {
        /// Pop direction.
        direction: Direction,
        /// Number of elements to pop.
        count: usize,
    },
    /// BZPOPMIN operation.
    BZPopMin,
    /// BZPOPMAX operation.
    BZPopMax,
    /// BZMPOP operation.
    BZMPop {
        /// Whether to pop minimum (true) or maximum (false).
        min: bool,
        /// Number of elements to pop.
        count: usize,
    },
    /// XREAD blocking operation.
    XRead {
        /// Stream IDs to read after (resolved from $ at block time).
        after_ids: Vec<StreamId>,
        /// Maximum entries per stream.
        count: Option<usize>,
    },
    /// XREADGROUP blocking operation.
    XReadGroup {
        /// Consumer group name.
        group: Bytes,
        /// Consumer name.
        consumer: Bytes,
        /// Skip PEL updates (NOACK flag).
        noack: bool,
        /// Maximum entries to return.
        count: Option<usize>,
    },
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

// ============================================================================
// Sorted Set Types
// ============================================================================

/// Score boundary for range queries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScoreBound {
    /// Inclusive bound.
    Inclusive(f64),
    /// Exclusive bound.
    Exclusive(f64),
    /// Negative infinity.
    NegInf,
    /// Positive infinity.
    PosInf,
}

impl ScoreBound {
    /// Check if a score satisfies this bound as a minimum.
    pub fn satisfies_min(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => true,
            ScoreBound::PosInf => false,
            ScoreBound::Inclusive(bound) => score >= *bound,
            ScoreBound::Exclusive(bound) => score > *bound,
        }
    }

    /// Check if a score satisfies this bound as a maximum.
    pub fn satisfies_max(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => false,
            ScoreBound::PosInf => true,
            ScoreBound::Inclusive(bound) => score <= *bound,
            ScoreBound::Exclusive(bound) => score < *bound,
        }
    }

    /// Get the value for BTreeMap range queries (minimum bound).
    pub fn start_bound_value(&self) -> Option<OrderedFloat<f64>> {
        match self {
            ScoreBound::NegInf => None,
            ScoreBound::PosInf => Some(OrderedFloat(f64::INFINITY)),
            ScoreBound::Inclusive(v) | ScoreBound::Exclusive(v) => Some(OrderedFloat(*v)),
        }
    }
}

/// Lexicographic boundary for range queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexBound {
    /// Inclusive bound.
    Inclusive(Bytes),
    /// Exclusive bound.
    Exclusive(Bytes),
    /// Minimum (unbounded).
    Min,
    /// Maximum (unbounded).
    Max,
}

impl LexBound {
    /// Check if a member satisfies this bound as a minimum.
    pub fn satisfies_min(&self, member: &[u8]) -> bool {
        match self {
            LexBound::Min => true,
            LexBound::Max => false,
            LexBound::Inclusive(bound) => member >= bound.as_ref(),
            LexBound::Exclusive(bound) => member > bound.as_ref(),
        }
    }

    /// Check if a member satisfies this bound as a maximum.
    pub fn satisfies_max(&self, member: &[u8]) -> bool {
        match self {
            LexBound::Min => false,
            LexBound::Max => true,
            LexBound::Inclusive(bound) => member <= bound.as_ref(),
            LexBound::Exclusive(bound) => member < bound.as_ref(),
        }
    }
}

/// Result of adding a member to a sorted set.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZAddResult {
    /// Whether a new member was added.
    pub added: bool,
    /// Whether the score was changed (for existing members).
    pub changed: bool,
    /// The previous score (if member existed).
    pub old_score: Option<f64>,
}

/// Sorted set value with dual indexing for O(1) score lookup and O(log n) range queries.
#[derive(Debug, Clone)]
pub struct SortedSetValue {
    /// O(1) lookup: member -> score
    members: HashMap<Bytes, f64>,
    /// O(log n) range queries: (score, member) -> ()
    /// Using (OrderedFloat, Bytes) ensures proper ordering by score, then member.
    scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
}

impl Default for SortedSetValue {
    fn default() -> Self {
        Self::new()
    }
}

impl SortedSetValue {
    /// Create a new empty sorted set.
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            scores: BTreeMap::new(),
        }
    }

    /// Get the number of members.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Add or update a member with a score.
    ///
    /// Returns information about what changed.
    pub fn add(&mut self, member: Bytes, score: f64) -> ZAddResult {
        if let Some(&old_score) = self.members.get(&member) {
            if (old_score - score).abs() < f64::EPSILON
                || (old_score.is_nan() && score.is_nan())
                || (old_score == score)
            {
                // Score unchanged
                return ZAddResult {
                    added: false,
                    changed: false,
                    old_score: Some(old_score),
                };
            }
            // Remove old entry from scores index
            self.scores.remove(&(OrderedFloat(old_score), member.clone()));
            // Insert new entry
            self.scores.insert((OrderedFloat(score), member.clone()), ());
            self.members.insert(member, score);
            ZAddResult {
                added: false,
                changed: true,
                old_score: Some(old_score),
            }
        } else {
            // New member
            self.members.insert(member.clone(), score);
            self.scores.insert((OrderedFloat(score), member), ());
            ZAddResult {
                added: true,
                changed: false,
                old_score: None,
            }
        }
    }

    /// Remove a member from the set.
    ///
    /// Returns the score if the member existed.
    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        if let Some(score) = self.members.remove(member) {
            self.scores
                .remove(&(OrderedFloat(score), Bytes::copy_from_slice(member)));
            Some(score)
        } else {
            None
        }
    }

    /// Get the score of a member.
    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        self.members.get(member).copied()
    }

    /// Check if a member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        self.members.contains_key(member)
    }

    /// Get the 0-based rank of a member (ascending by score).
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.members.get(member)?;
        let key = (OrderedFloat(*score), Bytes::copy_from_slice(member));
        Some(self.scores.range(..&key).count())
    }

    /// Get the 0-based rank of a member (descending by score).
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    /// Increment the score of a member.
    ///
    /// If the member doesn't exist, it's created with the given increment as its score.
    /// Returns the new score.
    pub fn incr(&mut self, member: Bytes, increment: f64) -> f64 {
        let old_score = self.members.get(&member).copied().unwrap_or(0.0);
        let new_score = old_score + increment;

        // Check for overflow to infinity
        if new_score.is_infinite() && !old_score.is_infinite() && !increment.is_infinite() {
            // This would be an error in Redis, but we'll handle it
            return new_score;
        }

        // Remove old entry if exists
        if self.members.contains_key(&member) {
            self.scores
                .remove(&(OrderedFloat(old_score), member.clone()));
        }

        // Insert new entry
        self.members.insert(member.clone(), new_score);
        self.scores.insert((OrderedFloat(new_score), member), ());

        new_score
    }

    /// Get members by rank range (inclusive).
    ///
    /// `start` and `end` are 0-based indices. Negative indices count from the end.
    pub fn range_by_rank(&self, start: i64, end: i64) -> Vec<(Bytes, f64)> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            return vec![];
        }

        let end = end as usize;

        self.scores
            .iter()
            .skip(start)
            .take(end - start + 1)
            .map(|((score, member), _)| (member.clone(), score.0))
            .collect()
    }

    /// Get members by rank range in reverse order (descending by score).
    pub fn rev_range_by_rank(&self, start: i64, end: i64) -> Vec<(Bytes, f64)> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            return vec![];
        }

        let end = end as usize;

        self.scores
            .iter()
            .rev()
            .skip(start)
            .take(end - start + 1)
            .map(|((score, member), _)| (member.clone(), score.0))
            .collect()
    }

    /// Get members by score range.
    pub fn range_by_score(
        &self,
        min: &ScoreBound,
        max: &ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self.scores.iter().filter(|((score, _), _)| {
            min.satisfies_min(score.0) && max.satisfies_max(score.0)
        });

        let iter = iter.skip(offset);

        let results: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        };

        results
    }

    /// Get members by score range in reverse order.
    pub fn rev_range_by_score(
        &self,
        min: &ScoreBound,
        max: &ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self.scores.iter().rev().filter(|((score, _), _)| {
            min.satisfies_min(score.0) && max.satisfies_max(score.0)
        });

        let iter = iter.skip(offset);

        let results: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        };

        results
    }

    /// Get members by lexicographic range (requires all scores to be equal).
    pub fn range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        // For lex range, we iterate in (score, member) order
        // This naturally gives us lexicographic order for same scores
        let iter = self.scores.iter().filter(|((_, member), _)| {
            min.satisfies_min(member) && max.satisfies_max(member)
        });

        let iter = iter.skip(offset);

        let results: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        };

        results
    }

    /// Get members by lexicographic range in reverse order.
    pub fn rev_range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self.scores.iter().rev().filter(|((_, member), _)| {
            min.satisfies_min(member) && max.satisfies_max(member)
        });

        let iter = iter.skip(offset);

        let results: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        };

        results
    }

    /// Count members in score range.
    pub fn count_by_score(&self, min: &ScoreBound, max: &ScoreBound) -> usize {
        self.scores
            .iter()
            .filter(|((score, _), _)| min.satisfies_min(score.0) && max.satisfies_max(score.0))
            .count()
    }

    /// Count members in lex range.
    pub fn count_by_lex(&self, min: &LexBound, max: &LexBound) -> usize {
        self.scores
            .iter()
            .filter(|((_, member), _)| min.satisfies_min(member) && max.satisfies_max(member))
            .count()
    }

    /// Pop members with minimum scores.
    pub fn pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count.min(self.len()));
        for _ in 0..count {
            if let Some(((score, member), _)) = self.scores.pop_first() {
                self.members.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }
        result
    }

    /// Pop members with maximum scores.
    pub fn pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count.min(self.len()));
        for _ in 0..count {
            if let Some(((score, member), _)) = self.scores.pop_last() {
                self.members.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }
        result
    }

    /// Remove members by rank range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_rank(&mut self, start: i64, end: i64) -> usize {
        let to_remove = self.range_by_rank(start, end);
        let count = to_remove.len();
        for (member, _) in to_remove {
            self.remove(&member);
        }
        count
    }

    /// Remove members by score range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_score(&mut self, min: &ScoreBound, max: &ScoreBound) -> usize {
        let to_remove = self.range_by_score(min, max, 0, None);
        let count = to_remove.len();
        for (member, _) in to_remove {
            self.remove(&member);
        }
        count
    }

    /// Remove members by lex range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_lex(&mut self, min: &LexBound, max: &LexBound) -> usize {
        let to_remove = self.range_by_lex(min, max, 0, None);
        let count = to_remove.len();
        for (member, _) in to_remove {
            self.remove(&member);
        }
        count
    }

    /// Get random members.
    ///
    /// If `count` is positive, returns that many unique members.
    /// If `count` is negative, returns abs(count) members with possible duplicates.
    pub fn random_members(&self, count: i64) -> Vec<(Bytes, f64)> {
        if self.is_empty() {
            return vec![];
        }

        let members: Vec<_> = self
            .scores
            .iter()
            .map(|((score, member), _)| (member.clone(), score.0))
            .collect();

        if count == 0 {
            return vec![];
        }

        if count > 0 {
            // Return unique members
            let count = (count as usize).min(members.len());
            // Simple reservoir sampling or just take first N for now
            // In production, we'd use proper random sampling
            members.into_iter().take(count).collect()
        } else {
            // Allow duplicates
            let count = (-count) as usize;
            // Return members (allowing duplicates)
            let mut result = Vec::with_capacity(count);
            for i in 0..count {
                let idx = i % members.len();
                result.push(members[idx].clone());
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();

        // HashMap overhead + entries
        let members_size: usize = self
            .members
            .keys()
            .map(|k| k.len() + std::mem::size_of::<f64>() + 32) // 32 for HashMap node overhead
            .sum();

        // BTreeMap overhead + entries
        let scores_size: usize = self
            .scores
            .iter()
            .map(|((_, member), _)| member.len() + std::mem::size_of::<OrderedFloat<f64>>() + 32)
            .sum();

        base_size + members_size + scores_size
    }

    /// Iterate over all members in score order.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, f64)> {
        self.scores
            .iter()
            .map(|((score, member), _)| (member, score.0))
    }

    /// Get all members and scores as a vec for serialization.
    pub fn to_vec(&self) -> Vec<(Bytes, f64)> {
        self.scores
            .iter()
            .map(|((score, member), _)| (member.clone(), score.0))
            .collect()
    }
}

// ============================================================================
// Hash Type
// ============================================================================

/// Hash value - a mapping from field names to values.
#[derive(Debug, Clone)]
pub struct HashValue {
    data: HashMap<Bytes, Bytes>,
}

impl Default for HashValue {
    fn default() -> Self {
        Self::new()
    }
}

impl HashValue {
    /// Create a new empty hash.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Get the number of fields.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the hash is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Set a field value.
    ///
    /// Returns true if the field is new, false if it was updated.
    pub fn set(&mut self, field: Bytes, value: Bytes) -> bool {
        self.data.insert(field, value).is_none()
    }

    /// Set a field value only if it doesn't exist.
    ///
    /// Returns true if the field was set, false if it already existed.
    pub fn set_nx(&mut self, field: Bytes, value: Bytes) -> bool {
        use std::collections::hash_map::Entry;
        if let Entry::Vacant(e) = self.data.entry(field) {
            e.insert(value);
            true
        } else {
            false
        }
    }

    /// Get a field value.
    pub fn get(&self, field: &[u8]) -> Option<&Bytes> {
        self.data.get(field)
    }

    /// Remove a field.
    ///
    /// Returns true if the field existed.
    pub fn remove(&mut self, field: &[u8]) -> bool {
        self.data.remove(field).is_some()
    }

    /// Check if a field exists.
    pub fn contains(&self, field: &[u8]) -> bool {
        self.data.contains_key(field)
    }

    /// Get all field names.
    pub fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.data.keys()
    }

    /// Get all values.
    pub fn values(&self) -> impl Iterator<Item = &Bytes> {
        self.data.values()
    }

    /// Iterate over all field-value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Bytes)> {
        self.data.iter()
    }

    /// Increment an integer field by delta.
    ///
    /// If the field doesn't exist, it's created with the delta value.
    /// Returns the new value or an error if the field is not a valid integer.
    pub fn incr_by(&mut self, field: Bytes, delta: i64) -> Result<i64, IncrementError> {
        let current = if let Some(val) = self.data.get(&field) {
            std::str::from_utf8(val)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or(IncrementError::NotInteger)?
        } else {
            0
        };

        let new_val = current
            .checked_add(delta)
            .ok_or(IncrementError::Overflow)?;
        self.data.insert(field, Bytes::from(new_val.to_string()));
        Ok(new_val)
    }

    /// Increment a float field by delta.
    ///
    /// If the field doesn't exist, it's created with the delta value.
    /// Returns the new value or an error if the field is not a valid float.
    pub fn incr_by_float(&mut self, field: Bytes, delta: f64) -> Result<f64, IncrementError> {
        let current = if let Some(val) = self.data.get(&field) {
            std::str::from_utf8(val)
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or(IncrementError::NotFloat)?
        } else {
            0.0
        };

        let new_val = current + delta;

        if new_val.is_infinite() || new_val.is_nan() {
            return Err(IncrementError::Overflow);
        }

        self.data.insert(field, Bytes::from(format_float(new_val)));
        Ok(new_val)
    }

    /// Get random fields from the hash.
    ///
    /// If count > 0: return up to count unique fields
    /// If count < 0: return |count| fields, allowing duplicates
    pub fn random_fields(&self, count: i64, with_values: bool) -> Vec<(Bytes, Option<Bytes>)> {
        if self.is_empty() || count == 0 {
            return vec![];
        }

        let entries: Vec<_> = self.data.iter().collect();
        let mut rng = rand::thread_rng();

        if count > 0 {
            // Unique fields
            let count = (count as usize).min(entries.len());
            let mut indices: Vec<usize> = (0..entries.len()).collect();
            indices.shuffle(&mut rng);
            indices
                .into_iter()
                .take(count)
                .map(|i| {
                    let (k, v) = entries[i];
                    (k.clone(), if with_values { Some(v.clone()) } else { None })
                })
                .collect()
        } else {
            // Allow duplicates
            let count = (-count) as usize;
            let mut result = Vec::with_capacity(count);
            for _ in 0..count {
                let idx = rand::random::<usize>() % entries.len();
                let (k, v) = entries[idx];
                result.push((k.clone(), if with_values { Some(v.clone()) } else { None }));
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let entries_size: usize = self
            .data
            .iter()
            .map(|(k, v)| k.len() + v.len() + 32) // 32 for HashMap node overhead
            .sum();
        base_size + entries_size
    }

    /// Get all field-value pairs as a vec for serialization.
    pub fn to_vec(&self) -> Vec<(Bytes, Bytes)> {
        self.data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

// ============================================================================
// Set Type
// ============================================================================

/// Set value - an unordered collection of unique members.
#[derive(Debug, Clone)]
pub struct SetValue {
    data: HashSet<Bytes>,
}

impl Default for SetValue {
    fn default() -> Self {
        Self::new()
    }
}

impl SetValue {
    /// Create a new empty set.
    pub fn new() -> Self {
        Self {
            data: HashSet::new(),
        }
    }

    /// Get the number of members.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Add a member to the set.
    ///
    /// Returns true if the member was new, false if it already existed.
    pub fn add(&mut self, member: Bytes) -> bool {
        self.data.insert(member)
    }

    /// Remove a member from the set.
    ///
    /// Returns true if the member existed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        self.data.remove(member)
    }

    /// Check if a member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        self.data.contains(member)
    }

    /// Get all members.
    pub fn members(&self) -> impl Iterator<Item = &Bytes> {
        self.data.iter()
    }

    /// Compute the union of this set with others.
    pub fn union<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result = self.data.clone();
        for other in others {
            for member in &other.data {
                result.insert(member.clone());
            }
        }
        SetValue { data: result }
    }

    /// Compute the intersection of this set with others.
    pub fn intersection<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result = self.data.clone();
        for other in others {
            result.retain(|m| other.data.contains(m));
        }
        SetValue { data: result }
    }

    /// Compute the difference of this set minus others.
    pub fn difference<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result = self.data.clone();
        for other in others {
            for member in &other.data {
                result.remove(member);
            }
        }
        SetValue { data: result }
    }

    /// Pop a random member from the set.
    ///
    /// Returns None if the set is empty.
    pub fn pop(&mut self) -> Option<Bytes> {
        if self.is_empty() {
            return None;
        }

        // Get random member
        let members: Vec<_> = self.data.iter().cloned().collect();
        let idx = rand::random::<usize>() % members.len();
        let member = members[idx].clone();
        self.data.remove(&member);
        Some(member)
    }

    /// Pop multiple random members from the set.
    pub fn pop_many(&mut self, count: usize) -> Vec<Bytes> {
        let count = count.min(self.len());
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(member) = self.pop() {
                result.push(member);
            } else {
                break;
            }
        }
        result
    }

    /// Get random members without removing them.
    ///
    /// If count > 0: return up to count unique members
    /// If count < 0: return |count| members, allowing duplicates
    pub fn random_members(&self, count: i64) -> Vec<Bytes> {
        if self.is_empty() || count == 0 {
            return vec![];
        }

        let members: Vec<_> = self.data.iter().cloned().collect();
        let mut rng = rand::thread_rng();

        if count > 0 {
            // Unique members
            let count = (count as usize).min(members.len());
            let mut shuffled = members;
            shuffled.shuffle(&mut rng);
            shuffled.into_iter().take(count).collect()
        } else {
            // Allow duplicates
            let count = (-count) as usize;
            let mut result = Vec::with_capacity(count);
            for _ in 0..count {
                let idx = rand::random::<usize>() % members.len();
                result.push(members[idx].clone());
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let entries_size: usize = self
            .data
            .iter()
            .map(|m| m.len() + 24) // 24 for HashSet node overhead
            .sum();
        base_size + entries_size
    }

    /// Get all members as a vec for serialization.
    pub fn to_vec(&self) -> Vec<Bytes> {
        self.data.iter().cloned().collect()
    }
}

// ============================================================================
// List Type
// ============================================================================

/// List value - a doubly-linked list of values.
#[derive(Debug, Clone)]
pub struct ListValue {
    data: VecDeque<Bytes>,
}

impl Default for ListValue {
    fn default() -> Self {
        Self::new()
    }
}

impl ListValue {
    /// Create a new empty list.
    pub fn new() -> Self {
        Self {
            data: VecDeque::new(),
        }
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Push an element to the front (left).
    pub fn push_front(&mut self, value: Bytes) {
        self.data.push_front(value);
    }

    /// Push an element to the back (right).
    pub fn push_back(&mut self, value: Bytes) {
        self.data.push_back(value);
    }

    /// Pop an element from the front (left).
    pub fn pop_front(&mut self) -> Option<Bytes> {
        self.data.pop_front()
    }

    /// Pop an element from the back (right).
    pub fn pop_back(&mut self) -> Option<Bytes> {
        self.data.pop_back()
    }

    /// Normalize a Redis index (supports negative indices).
    fn normalize_index(&self, index: i64) -> Option<usize> {
        let len = self.len() as i64;
        if len == 0 {
            return None;
        }
        let normalized = if index < 0 { len + index } else { index };
        if normalized < 0 || normalized >= len {
            None
        } else {
            Some(normalized as usize)
        }
    }

    /// Get an element by index (supports negative indices).
    pub fn get(&self, index: i64) -> Option<&Bytes> {
        self.normalize_index(index)
            .and_then(|i| self.data.get(i))
    }

    /// Set an element by index (supports negative indices).
    ///
    /// Returns true if the index was valid and the element was set.
    pub fn set(&mut self, index: i64, value: Bytes) -> bool {
        if let Some(i) = self.normalize_index(index) {
            if let Some(elem) = self.data.get_mut(i) {
                *elem = value;
                return true;
            }
        }
        false
    }

    /// Get a range of elements (inclusive, supports negative indices).
    pub fn range(&self, start: i64, end: i64) -> Vec<Bytes> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            return vec![];
        }

        let end = end as usize;

        self.data
            .iter()
            .skip(start)
            .take(end - start + 1)
            .cloned()
            .collect()
    }

    /// Trim the list to only contain elements in the specified range.
    pub fn trim(&mut self, start: i64, end: i64) {
        let len = self.len() as i64;
        if len == 0 {
            return;
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            // Empty range - clear the list
            self.data.clear();
            return;
        }

        let end = end as usize;

        // Keep only elements in range [start, end]
        let new_data: VecDeque<_> = self.data
            .iter()
            .skip(start)
            .take(end - start + 1)
            .cloned()
            .collect();
        self.data = new_data;
    }

    /// Find the position of an element.
    ///
    /// Returns the first position where element is found, or None.
    /// `rank`: how many matches to skip (0 = first, 1 = second, etc.)
    /// `count`: maximum number of positions to return
    /// `maxlen`: maximum number of elements to scan
    pub fn position(
        &self,
        element: &[u8],
        rank: i64,
        count: usize,
        maxlen: Option<usize>,
    ) -> Vec<usize> {
        let maxlen = maxlen.unwrap_or(self.len());

        if rank >= 0 {
            // Forward scan
            let rank = rank as usize;
            let mut matches = 0;
            let mut positions = Vec::new();

            for (i, item) in self.data.iter().enumerate().take(maxlen) {
                if item.as_ref() == element {
                    if matches >= rank {
                        positions.push(i);
                        if positions.len() >= count {
                            break;
                        }
                    }
                    matches += 1;
                }
            }
            positions
        } else {
            // Backward scan
            let rank = (-rank - 1) as usize;
            let mut matches = 0;
            let mut positions = Vec::new();
            let scan_start = if maxlen < self.len() {
                self.len() - maxlen
            } else {
                0
            };

            for (i, item) in self.data.iter().enumerate().rev() {
                if i < scan_start {
                    break;
                }
                if item.as_ref() == element {
                    if matches >= rank {
                        positions.push(i);
                        if positions.len() >= count {
                            break;
                        }
                    }
                    matches += 1;
                }
            }
            positions
        }
    }

    /// Insert an element before or after a pivot element.
    ///
    /// Returns the new length of the list, -1 if pivot not found, 0 if list is empty.
    pub fn insert(&mut self, before: bool, pivot: &[u8], element: Bytes) -> i64 {
        if self.is_empty() {
            return 0;
        }

        // Find pivot position
        let pos = self.data.iter().position(|e| e.as_ref() == pivot);

        match pos {
            Some(i) => {
                let insert_pos = if before { i } else { i + 1 };
                self.data.insert(insert_pos, element);
                self.len() as i64
            }
            None => -1,
        }
    }

    /// Remove elements equal to value.
    ///
    /// `count` determines direction and number:
    /// - count > 0: Remove first count occurrences (head to tail)
    /// - count < 0: Remove first |count| occurrences (tail to head)
    /// - count = 0: Remove all occurrences
    ///
    /// Returns the number of elements removed.
    pub fn remove(&mut self, count: i64, element: &[u8]) -> usize {
        if self.is_empty() {
            return 0;
        }

        let mut removed = 0;

        if count == 0 {
            // Remove all
            let original_len = self.len();
            self.data.retain(|e| e.as_ref() != element);
            removed = original_len - self.len();
        } else if count > 0 {
            // Remove from head
            let max_remove = count as usize;
            let mut new_data = VecDeque::with_capacity(self.len());
            for item in self.data.drain(..) {
                if removed < max_remove && item.as_ref() == element {
                    removed += 1;
                } else {
                    new_data.push_back(item);
                }
            }
            self.data = new_data;
        } else {
            // Remove from tail
            let max_remove = (-count) as usize;
            let mut indices_to_remove = Vec::new();
            for (i, item) in self.data.iter().enumerate().rev() {
                if item.as_ref() == element {
                    indices_to_remove.push(i);
                    if indices_to_remove.len() >= max_remove {
                        break;
                    }
                }
            }
            // Sort indices in descending order to remove from end first
            indices_to_remove.sort_by(|a, b| b.cmp(a));
            for i in indices_to_remove {
                self.data.remove(i);
                removed += 1;
            }
        }

        removed
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let entries_size: usize = self
            .data
            .iter()
            .map(|e| e.len() + 8) // 8 for VecDeque node overhead
            .sum();
        base_size + entries_size
    }

    /// Get all elements as a vec for serialization.
    pub fn to_vec(&self) -> Vec<Bytes> {
        self.data.iter().cloned().collect()
    }

    /// Iterate over all elements.
    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.data.iter()
    }
}

// ============================================================================
// Stream Type
// ============================================================================

/// Stream entry ID consisting of millisecond timestamp and sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct StreamId {
    /// Unix timestamp in milliseconds.
    pub ms: u64,
    /// Sequence number within the millisecond.
    pub seq: u64,
}

impl StreamId {
    /// Create a new stream ID.
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// Minimum possible stream ID (0-0).
    pub fn min() -> Self {
        Self { ms: 0, seq: 0 }
    }

    /// Maximum possible stream ID.
    pub fn max() -> Self {
        Self {
            ms: u64::MAX,
            seq: u64::MAX,
        }
    }

    /// Check if this is the zero ID (0-0).
    pub fn is_zero(&self) -> bool {
        self.ms == 0 && self.seq == 0
    }

    /// Parse a stream ID from bytes.
    ///
    /// Format: "ms-seq" where ms and seq are unsigned integers.
    /// Also accepts just "ms" (sequence defaults to 0 for explicit IDs).
    pub fn parse(s: &[u8]) -> Result<Self, StreamIdParseError> {
        let s = std::str::from_utf8(s).map_err(|_| StreamIdParseError::InvalidFormat)?;

        if let Some((ms_str, seq_str)) = s.split_once('-') {
            let ms = ms_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            let seq = seq_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(Self { ms, seq })
        } else {
            // Just milliseconds, sequence is 0
            let ms = s
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(Self { ms, seq: 0 })
        }
    }

    /// Parse a stream ID for use as a range bound.
    ///
    /// Supports special values:
    /// - "-" means minimum ID
    /// - "+" means maximum ID
    /// - Otherwise parses as normal ID
    pub fn parse_range_bound(s: &[u8]) -> Result<StreamRangeBound, StreamIdParseError> {
        match s {
            b"-" => Ok(StreamRangeBound::Min),
            b"+" => Ok(StreamRangeBound::Max),
            _ => {
                // Check for exclusive prefix
                if s.starts_with(b"(") {
                    let id = Self::parse(&s[1..])?;
                    Ok(StreamRangeBound::Exclusive(id))
                } else {
                    let id = Self::parse(s)?;
                    Ok(StreamRangeBound::Inclusive(id))
                }
            }
        }
    }

    /// Parse a stream ID for XADD.
    ///
    /// Supports special values:
    /// - "*" means auto-generate
    /// - "ms-*" means auto-generate sequence for given timestamp
    /// - Otherwise parses as explicit ID
    pub fn parse_for_add(s: &[u8]) -> Result<StreamIdSpec, StreamIdParseError> {
        let s_str = std::str::from_utf8(s).map_err(|_| StreamIdParseError::InvalidFormat)?;

        if s_str == "*" {
            return Ok(StreamIdSpec::Auto);
        }

        if let Some((ms_str, seq_str)) = s_str.split_once('-') {
            let ms = ms_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            if seq_str == "*" {
                return Ok(StreamIdSpec::AutoSeq(ms));
            }
            let seq = seq_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(StreamIdSpec::Explicit(Self { ms, seq }))
        } else {
            let ms = s_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(StreamIdSpec::Explicit(Self { ms, seq: 0 }))
        }
    }

    /// Generate a new stream ID based on current time and the last ID.
    pub fn generate(last: &StreamId) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        if now_ms > last.ms {
            Self { ms: now_ms, seq: 0 }
        } else {
            // Same or earlier timestamp, increment sequence
            Self {
                ms: last.ms,
                seq: last.seq.saturating_add(1),
            }
        }
    }

    /// Generate a new stream ID with auto-sequence for a given timestamp.
    pub fn generate_with_ms(ms: u64, last: &StreamId) -> Option<Self> {
        if ms > last.ms {
            Some(Self { ms, seq: 0 })
        } else if ms == last.ms {
            Some(Self {
                ms,
                seq: last.seq.saturating_add(1),
            })
        } else {
            // Timestamp is in the past
            None
        }
    }

    /// Check if this ID is valid as a new entry after the last ID.
    pub fn is_valid_after(&self, last: &StreamId) -> bool {
        self > last
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl Ord for StreamId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.ms.cmp(&other.ms) {
            std::cmp::Ordering::Equal => self.seq.cmp(&other.seq),
            ord => ord,
        }
    }
}

impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Stream ID parsing error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamIdParseError {
    /// Invalid format (not "ms-seq" or "ms").
    InvalidFormat,
}

/// Stream ID specification for XADD.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamIdSpec {
    /// Auto-generate ID based on current time.
    Auto,
    /// Auto-generate sequence for given millisecond timestamp.
    AutoSeq(u64),
    /// Explicit ID.
    Explicit(StreamId),
}

/// Stream range bound for XRANGE/XREVRANGE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRangeBound {
    /// Minimum ID ("-").
    Min,
    /// Maximum ID ("+").
    Max,
    /// Inclusive bound.
    Inclusive(StreamId),
    /// Exclusive bound (prefixed with "(").
    Exclusive(StreamId),
}

impl StreamRangeBound {
    /// Check if an ID satisfies this bound as a minimum.
    pub fn satisfies_min(&self, id: &StreamId) -> bool {
        match self {
            StreamRangeBound::Min => true,
            StreamRangeBound::Max => false,
            StreamRangeBound::Inclusive(bound) => id >= bound,
            StreamRangeBound::Exclusive(bound) => id > bound,
        }
    }

    /// Check if an ID satisfies this bound as a maximum.
    pub fn satisfies_max(&self, id: &StreamId) -> bool {
        match self {
            StreamRangeBound::Min => false,
            StreamRangeBound::Max => true,
            StreamRangeBound::Inclusive(bound) => id <= bound,
            StreamRangeBound::Exclusive(bound) => id < bound,
        }
    }
}

/// Stream entry with ID and field-value pairs.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Entry ID.
    pub id: StreamId,
    /// Field-value pairs.
    pub fields: Vec<(Bytes, Bytes)>,
}

impl StreamEntry {
    /// Create a new stream entry.
    pub fn new(id: StreamId, fields: Vec<(Bytes, Bytes)>) -> Self {
        Self { id, fields }
    }

    /// Calculate memory size of this entry.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let fields_size: usize = self
            .fields
            .iter()
            .map(|(k, v)| k.len() + v.len() + 16) // 16 for Vec overhead
            .sum();
        base + fields_size
    }
}

/// Pending entry in a consumer group's PEL (Pending Entries List).
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// Consumer that owns this pending entry.
    pub consumer: Bytes,
    /// Time when the entry was delivered.
    pub delivery_time: Instant,
    /// Number of times this entry has been delivered.
    pub delivery_count: u32,
}

impl PendingEntry {
    /// Create a new pending entry.
    pub fn new(consumer: Bytes) -> Self {
        Self {
            consumer,
            delivery_time: Instant::now(),
            delivery_count: 1,
        }
    }

    /// Get idle time in milliseconds.
    pub fn idle_ms(&self) -> u64 {
        self.delivery_time.elapsed().as_millis() as u64
    }
}

/// Consumer in a consumer group.
#[derive(Debug, Clone)]
pub struct Consumer {
    /// Consumer name.
    pub name: Bytes,
    /// Number of pending entries for this consumer.
    pub pending_count: usize,
    /// Last time this consumer was seen (read or claimed).
    pub last_seen: Instant,
}

impl Consumer {
    /// Create a new consumer.
    pub fn new(name: Bytes) -> Self {
        Self {
            name,
            pending_count: 0,
            last_seen: Instant::now(),
        }
    }

    /// Get idle time in milliseconds.
    pub fn idle_ms(&self) -> u64 {
        self.last_seen.elapsed().as_millis() as u64
    }

    /// Touch the consumer (update last_seen).
    pub fn touch(&mut self) {
        self.last_seen = Instant::now();
    }
}

/// Consumer group for a stream.
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// Group name.
    pub name: Bytes,
    /// Last delivered ID (entries after this are "new").
    pub last_delivered_id: StreamId,
    /// Pending entries list (PEL) - entries delivered but not acknowledged.
    pub pending: BTreeMap<StreamId, PendingEntry>,
    /// Consumers in this group.
    pub consumers: HashMap<Bytes, Consumer>,
    /// Number of entries read by this group (for XINFO).
    pub entries_read: Option<u64>,
}

impl ConsumerGroup {
    /// Create a new consumer group.
    pub fn new(name: Bytes, last_delivered_id: StreamId) -> Self {
        Self {
            name,
            last_delivered_id,
            pending: BTreeMap::new(),
            consumers: HashMap::new(),
            entries_read: None,
        }
    }

    /// Get or create a consumer.
    pub fn get_or_create_consumer(&mut self, name: Bytes) -> &mut Consumer {
        self.consumers
            .entry(name.clone())
            .or_insert_with(|| Consumer::new(name))
    }

    /// Create a consumer if it doesn't exist.
    ///
    /// Returns true if the consumer was created, false if it already existed.
    pub fn create_consumer(&mut self, name: Bytes) -> bool {
        if self.consumers.contains_key(&name) {
            false
        } else {
            self.consumers.insert(name.clone(), Consumer::new(name));
            true
        }
    }

    /// Delete a consumer.
    ///
    /// Returns the number of pending entries that were deleted with the consumer.
    pub fn delete_consumer(&mut self, name: &[u8]) -> usize {
        if self.consumers.remove(name).is_some() {
            // Remove all pending entries for this consumer
            let to_remove: Vec<StreamId> = self
                .pending
                .iter()
                .filter(|(_, pe)| pe.consumer.as_ref() == name)
                .map(|(id, _)| *id)
                .collect();
            let count = to_remove.len();
            for id in to_remove {
                self.pending.remove(&id);
            }
            count
        } else {
            0
        }
    }

    /// Add a pending entry for a consumer.
    pub fn add_pending(&mut self, id: StreamId, consumer: Bytes) {
        // Update consumer's pending count
        if let Some(c) = self.consumers.get_mut(&consumer) {
            c.pending_count += 1;
            c.touch();
        }
        self.pending.insert(id, PendingEntry::new(consumer));
    }

    /// Acknowledge entries (remove from PEL).
    ///
    /// Returns the number of entries acknowledged.
    pub fn ack(&mut self, ids: &[StreamId]) -> usize {
        let mut count = 0;
        for id in ids {
            if let Some(pe) = self.pending.remove(id) {
                count += 1;
                // Update consumer's pending count
                if let Some(c) = self.consumers.get_mut(&pe.consumer) {
                    c.pending_count = c.pending_count.saturating_sub(1);
                }
            }
        }
        count
    }

    /// Get pending entry count summary.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get the smallest and largest pending IDs.
    pub fn pending_range(&self) -> Option<(StreamId, StreamId)> {
        let first = self.pending.first_key_value()?.0;
        let last = self.pending.last_key_value()?.0;
        Some((*first, *last))
    }

    /// Calculate memory size of this group.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let pending_size: usize = self
            .pending
            .values()
            .map(|pe| std::mem::size_of::<StreamId>() + pe.consumer.len() + 32)
            .sum();
        let consumers_size: usize = self
            .consumers
            .keys()
            .map(|k| k.len() + std::mem::size_of::<Consumer>() + 32)
            .sum();
        base + pending_size + consumers_size + self.name.len()
    }
}

/// Trimming strategy for streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTrimStrategy {
    /// Trim by maximum length.
    MaxLen(u64),
    /// Trim by minimum ID (entries older than this ID are removed).
    MinId(StreamId),
}

/// Trimming mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTrimMode {
    /// Exact trimming (remove exactly to the threshold).
    Exact,
    /// Approximate trimming (may keep slightly more entries for efficiency).
    Approximate,
}

/// Trimming options for XTRIM and XADD.
#[derive(Debug, Clone, Copy)]
pub struct StreamTrimOptions {
    /// Trimming strategy.
    pub strategy: StreamTrimStrategy,
    /// Trimming mode.
    pub mode: StreamTrimMode,
    /// Maximum number of entries to trim in one operation (0 = unlimited).
    pub limit: usize,
}

/// Stream value - an append-only log of entries with consumer groups.
#[derive(Debug, Clone)]
pub struct StreamValue {
    /// Entries ordered by ID.
    entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    /// Last generated/added ID (for auto-generation).
    last_id: StreamId,
    /// Consumer groups.
    groups: HashMap<Bytes, ConsumerGroup>,
    /// First entry ID (cached for efficiency).
    first_id: Option<StreamId>,
}

impl Default for StreamValue {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamValue {
    /// Create a new empty stream.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamId::default(),
            groups: HashMap::new(),
            first_id: None,
        }
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the stream is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the last entry ID.
    pub fn last_id(&self) -> StreamId {
        self.last_id
    }

    /// Get the first entry ID.
    pub fn first_id(&self) -> Option<StreamId> {
        self.first_id
    }

    /// Get the first entry.
    pub fn first_entry(&self) -> Option<StreamEntry> {
        self.entries.first_key_value().map(|(id, fields)| {
            StreamEntry::new(*id, fields.clone())
        })
    }

    /// Get the last entry.
    pub fn last_entry(&self) -> Option<StreamEntry> {
        self.entries.last_key_value().map(|(id, fields)| {
            StreamEntry::new(*id, fields.clone())
        })
    }

    /// Add an entry to the stream.
    ///
    /// Returns the ID of the added entry, or an error if the ID is invalid.
    pub fn add(&mut self, id_spec: StreamIdSpec, fields: Vec<(Bytes, Bytes)>) -> Result<StreamId, StreamAddError> {
        let id = match id_spec {
            StreamIdSpec::Auto => StreamId::generate(&self.last_id),
            StreamIdSpec::AutoSeq(ms) => {
                StreamId::generate_with_ms(ms, &self.last_id)
                    .ok_or(StreamAddError::IdTooSmall)?
            }
            StreamIdSpec::Explicit(id) => {
                if !id.is_valid_after(&self.last_id) {
                    return Err(StreamAddError::IdTooSmall);
                }
                id
            }
        };

        // Check for zero ID (not allowed as first entry)
        if self.is_empty() && id.is_zero() {
            return Err(StreamAddError::IdTooSmall);
        }

        self.entries.insert(id, fields);
        self.last_id = id;

        // Update first_id if this is the first entry
        if self.first_id.is_none() {
            self.first_id = Some(id);
        }

        Ok(id)
    }

    /// Delete entries by ID.
    ///
    /// Returns the number of entries deleted.
    pub fn delete(&mut self, ids: &[StreamId]) -> usize {
        let mut count = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                count += 1;
            }
        }

        // Update first_id if needed
        if count > 0 {
            self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
        }

        count
    }

    /// Get entries in a range.
    pub fn range(
        &self,
        start: StreamRangeBound,
        end: StreamRangeBound,
        count: Option<usize>,
    ) -> Vec<StreamEntry> {
        let iter = self.entries.iter().filter(|(id, _)| {
            start.satisfies_min(id) && end.satisfies_max(id)
        });

        let entries: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        } else {
            iter.map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        };

        entries
    }

    /// Get entries in a range (reverse order).
    pub fn range_rev(
        &self,
        start: StreamRangeBound,
        end: StreamRangeBound,
        count: Option<usize>,
    ) -> Vec<StreamEntry> {
        // For reverse range, start and end are swapped (end is the "start" bound)
        let iter = self.entries.iter().rev().filter(|(id, _)| {
            end.satisfies_min(id) && start.satisfies_max(id)
        });

        let entries: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        } else {
            iter.map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        };

        entries
    }

    /// Read entries after a given ID (for XREAD).
    pub fn read_after(&self, after: &StreamId, count: Option<usize>) -> Vec<StreamEntry> {
        let iter = self.entries.range((std::ops::Bound::Excluded(*after), std::ops::Bound::Unbounded));

        let entries: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        } else {
            iter.map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        };

        entries
    }

    /// Trim the stream.
    ///
    /// Returns the number of entries removed.
    pub fn trim(&mut self, options: StreamTrimOptions) -> usize {
        let mut removed = 0;
        let limit = if options.limit == 0 { usize::MAX } else { options.limit };

        match options.strategy {
            StreamTrimStrategy::MaxLen(max_len) => {
                let max_len = max_len as usize;
                while self.entries.len() > max_len && removed < limit {
                    if let Some((id, _)) = self.entries.pop_first() {
                        removed += 1;
                        // Update first_id
                        if Some(id) == self.first_id {
                            self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
                        }
                    } else {
                        break;
                    }
                }
            }
            StreamTrimStrategy::MinId(min_id) => {
                // Remove all entries with ID < min_id
                let to_remove: Vec<StreamId> = self
                    .entries
                    .range(..min_id)
                    .take(limit)
                    .map(|(id, _)| *id)
                    .collect();
                for id in to_remove {
                    self.entries.remove(&id);
                    removed += 1;
                }
                // Update first_id
                if removed > 0 {
                    self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
                }
            }
        }

        removed
    }

    // ==================== Consumer Group Methods ====================

    /// Create a consumer group.
    ///
    /// Returns an error if the group already exists.
    pub fn create_group(&mut self, name: Bytes, last_delivered_id: StreamId, entries_read: Option<u64>) -> Result<(), StreamGroupError> {
        if self.groups.contains_key(&name) {
            return Err(StreamGroupError::GroupExists);
        }
        let mut group = ConsumerGroup::new(name.clone(), last_delivered_id);
        group.entries_read = entries_read;
        self.groups.insert(name, group);
        Ok(())
    }

    /// Destroy a consumer group.
    ///
    /// Returns true if the group was destroyed, false if it didn't exist.
    pub fn destroy_group(&mut self, name: &[u8]) -> bool {
        self.groups.remove(name).is_some()
    }

    /// Get a consumer group.
    pub fn get_group(&self, name: &[u8]) -> Option<&ConsumerGroup> {
        self.groups.get(name)
    }

    /// Get a mutable consumer group.
    pub fn get_group_mut(&mut self, name: &[u8]) -> Option<&mut ConsumerGroup> {
        self.groups.get_mut(name)
    }

    /// Get all consumer groups.
    pub fn groups(&self) -> impl Iterator<Item = &ConsumerGroup> {
        self.groups.values()
    }

    /// Number of consumer groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Set a consumer group's last delivered ID.
    pub fn set_group_id(&mut self, name: &[u8], id: StreamId, entries_read: Option<u64>) -> Result<(), StreamGroupError> {
        let group = self.groups.get_mut(name).ok_or(StreamGroupError::NoGroup)?;
        group.last_delivered_id = id;
        if entries_read.is_some() {
            group.entries_read = entries_read;
        }
        Ok(())
    }

    /// Get an entry by ID.
    pub fn get(&self, id: &StreamId) -> Option<StreamEntry> {
        self.entries.get(id).map(|fields| StreamEntry::new(*id, fields.clone()))
    }

    /// Check if an entry exists.
    pub fn contains(&self, id: &StreamId) -> bool {
        self.entries.contains_key(id)
    }

    /// Calculate memory size of this stream.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();

        let entries_size: usize = self
            .entries
            .values()
            .map(|fields| {
                let fields_size: usize = fields.iter().map(|(k, v)| k.len() + v.len() + 16).sum();
                std::mem::size_of::<StreamId>() + fields_size + 32
            })
            .sum();

        let groups_size: usize = self.groups.values().map(|g| g.memory_size()).sum();

        base + entries_size + groups_size
    }

    /// Convert to vec for serialization.
    pub fn to_vec(&self) -> Vec<StreamEntry> {
        self.entries
            .iter()
            .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
            .collect()
    }
}

/// Error when adding to a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamAddError {
    /// The ID is equal to or smaller than the last ID.
    IdTooSmall,
}

/// Error for consumer group operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamGroupError {
    /// Consumer group already exists.
    GroupExists,
    /// Consumer group doesn't exist.
    NoGroup,
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

        let sv_float = StringValue::new("1.2345");
        assert!((sv_float.as_float().unwrap() - 1.2345).abs() < 0.0001);

        let sv_invalid = StringValue::new("not a float");
        assert!(sv_invalid.as_float().is_none());
    }

    // ==================== Stream Tests ====================

    #[test]
    fn test_stream_id_parse() {
        // Valid formats
        let id = StreamId::parse(b"1234567890123-0").unwrap();
        assert_eq!(id.ms, 1234567890123);
        assert_eq!(id.seq, 0);

        let id = StreamId::parse(b"1-99").unwrap();
        assert_eq!(id.ms, 1);
        assert_eq!(id.seq, 99);

        // Just milliseconds
        let id = StreamId::parse(b"1234").unwrap();
        assert_eq!(id.ms, 1234);
        assert_eq!(id.seq, 0);

        // Invalid formats
        assert!(StreamId::parse(b"invalid").is_err());
        assert!(StreamId::parse(b"-1-0").is_err());
        assert!(StreamId::parse(b"1--0").is_err());
    }

    #[test]
    fn test_stream_id_ordering() {
        let id1 = StreamId::new(1000, 0);
        let id2 = StreamId::new(1000, 1);
        let id3 = StreamId::new(1001, 0);

        assert!(id1 < id2);
        assert!(id2 < id3);
        assert!(id1 < id3);
    }

    #[test]
    fn test_stream_id_display() {
        let id = StreamId::new(1234567890123, 42);
        assert_eq!(id.to_string(), "1234567890123-42");
    }

    #[test]
    fn test_stream_id_range_bound() {
        // Min bound
        let bound = StreamId::parse_range_bound(b"-").unwrap();
        assert_eq!(bound, StreamRangeBound::Min);

        // Max bound
        let bound = StreamId::parse_range_bound(b"+").unwrap();
        assert_eq!(bound, StreamRangeBound::Max);

        // Inclusive bound
        let bound = StreamId::parse_range_bound(b"1000-0").unwrap();
        assert_eq!(bound, StreamRangeBound::Inclusive(StreamId::new(1000, 0)));

        // Exclusive bound
        let bound = StreamId::parse_range_bound(b"(1000-0").unwrap();
        assert_eq!(bound, StreamRangeBound::Exclusive(StreamId::new(1000, 0)));
    }

    #[test]
    fn test_stream_value_add_auto() {
        let mut stream = StreamValue::new();

        let fields = vec![(Bytes::from("field1"), Bytes::from("value1"))];
        let id = stream.add(StreamIdSpec::Auto, fields.clone()).unwrap();

        assert!(id.ms > 0);
        assert_eq!(stream.len(), 1);

        // Add another entry
        let id2 = stream.add(StreamIdSpec::Auto, fields).unwrap();
        assert!(id2 > id);
        assert_eq!(stream.len(), 2);
    }

    #[test]
    fn test_stream_value_add_explicit() {
        let mut stream = StreamValue::new();

        let fields = vec![(Bytes::from("field1"), Bytes::from("value1"))];
        let id = stream.add(StreamIdSpec::Explicit(StreamId::new(1000, 0)), fields.clone()).unwrap();

        assert_eq!(id, StreamId::new(1000, 0));
        assert_eq!(stream.len(), 1);

        // Add with larger ID
        let id2 = stream.add(StreamIdSpec::Explicit(StreamId::new(1000, 1)), fields.clone()).unwrap();
        assert_eq!(id2, StreamId::new(1000, 1));

        // Cannot add with smaller ID
        let result = stream.add(StreamIdSpec::Explicit(StreamId::new(999, 0)), fields);
        assert!(result.is_err());
    }

    #[test]
    fn test_stream_value_range() {
        let mut stream = StreamValue::new();

        for i in 0..5 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream.add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields).unwrap();
        }

        // Full range
        let entries = stream.range(StreamRangeBound::Min, StreamRangeBound::Max, None);
        assert_eq!(entries.len(), 5);

        // Range with count
        let entries = stream.range(StreamRangeBound::Min, StreamRangeBound::Max, Some(2));
        assert_eq!(entries.len(), 2);

        // Specific range
        let entries = stream.range(
            StreamRangeBound::Inclusive(StreamId::new(1001, 0)),
            StreamRangeBound::Inclusive(StreamId::new(1003, 0)),
            None
        );
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_stream_value_delete() {
        let mut stream = StreamValue::new();

        for i in 0..5 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream.add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields).unwrap();
        }

        let deleted = stream.delete(&[StreamId::new(1001, 0), StreamId::new(1003, 0)]);
        assert_eq!(deleted, 2);
        assert_eq!(stream.len(), 3);

        // Try to delete non-existent
        let deleted = stream.delete(&[StreamId::new(9999, 0)]);
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_stream_value_trim_maxlen() {
        let mut stream = StreamValue::new();

        for i in 0..10 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream.add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields).unwrap();
        }

        let trimmed = stream.trim(StreamTrimOptions {
            strategy: StreamTrimStrategy::MaxLen(5),
            mode: StreamTrimMode::Exact,
            limit: 0,
        });

        assert_eq!(trimmed, 5);
        assert_eq!(stream.len(), 5);

        // First entry should now be 1005-0
        assert_eq!(stream.first_id(), Some(StreamId::new(1005, 0)));
    }

    #[test]
    fn test_stream_value_trim_minid() {
        let mut stream = StreamValue::new();

        for i in 0..10 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream.add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields).unwrap();
        }

        let trimmed = stream.trim(StreamTrimOptions {
            strategy: StreamTrimStrategy::MinId(StreamId::new(1005, 0)),
            mode: StreamTrimMode::Exact,
            limit: 0,
        });

        assert_eq!(trimmed, 5);
        assert_eq!(stream.len(), 5);
        assert_eq!(stream.first_id(), Some(StreamId::new(1005, 0)));
    }

    #[test]
    fn test_consumer_group_basic() {
        let mut stream = StreamValue::new();

        // Add some entries
        for i in 0..5 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream.add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields).unwrap();
        }

        // Create a group
        stream.create_group(Bytes::from("mygroup"), StreamId::new(0, 0), None).unwrap();

        // Try to create duplicate group
        let result = stream.create_group(Bytes::from("mygroup"), StreamId::new(0, 0), None);
        assert!(result.is_err());

        // Get the group
        let group = stream.get_group(b"mygroup").unwrap();
        assert_eq!(group.name, Bytes::from("mygroup"));
        assert_eq!(group.last_delivered_id, StreamId::new(0, 0));

        // Destroy the group
        assert!(stream.destroy_group(b"mygroup"));
        assert!(!stream.destroy_group(b"mygroup")); // Already destroyed
    }

    #[test]
    fn test_consumer_group_pending() {
        let mut stream = StreamValue::new();

        // Add some entries
        for i in 0..5 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream.add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields).unwrap();
        }

        // Create a group and add pending entries
        stream.create_group(Bytes::from("mygroup"), StreamId::new(0, 0), None).unwrap();

        let group = stream.get_group_mut(b"mygroup").unwrap();
        group.get_or_create_consumer(Bytes::from("consumer1"));
        group.add_pending(StreamId::new(1000, 0), Bytes::from("consumer1"));
        group.add_pending(StreamId::new(1001, 0), Bytes::from("consumer1"));

        assert_eq!(group.pending_count(), 2);

        // Acknowledge one
        let acked = group.ack(&[StreamId::new(1000, 0)]);
        assert_eq!(acked, 1);
        assert_eq!(group.pending_count(), 1);

        // Acknowledge non-existent
        let acked = group.ack(&[StreamId::new(9999, 0)]);
        assert_eq!(acked, 0);
    }

    #[test]
    fn test_consumer_group_consumer_management() {
        let mut group = ConsumerGroup::new(Bytes::from("mygroup"), StreamId::new(0, 0));

        // Create consumers
        assert!(group.create_consumer(Bytes::from("consumer1")));
        assert!(!group.create_consumer(Bytes::from("consumer1"))); // Already exists
        assert!(group.create_consumer(Bytes::from("consumer2")));

        assert_eq!(group.consumers.len(), 2);

        // Delete consumer
        let pending_deleted = group.delete_consumer(b"consumer1");
        assert_eq!(pending_deleted, 0);
        assert_eq!(group.consumers.len(), 1);

        // Delete with pending entries
        group.get_or_create_consumer(Bytes::from("consumer2"));
        group.add_pending(StreamId::new(1000, 0), Bytes::from("consumer2"));
        let pending_deleted = group.delete_consumer(b"consumer2");
        assert_eq!(pending_deleted, 1);
    }

    #[test]
    fn test_stream_value_memory_size() {
        let mut stream = StreamValue::new();

        let initial_size = stream.memory_size();

        // Add an entry
        let fields = vec![(Bytes::from("field"), Bytes::from("value"))];
        stream.add(StreamIdSpec::Explicit(StreamId::new(1000, 0)), fields).unwrap();

        let after_add = stream.memory_size();
        assert!(after_add > initial_size);

        // Add a group
        stream.create_group(Bytes::from("mygroup"), StreamId::new(0, 0), None).unwrap();

        let after_group = stream.memory_size();
        assert!(after_group > after_add);
    }

    #[test]
    fn test_value_stream_accessors() {
        let value = Value::stream();

        // Test as_stream
        assert!(value.as_stream().is_some());
        assert!(value.as_string().is_none());

        // Test key_type
        assert_eq!(value.key_type(), KeyType::Stream);
    }

    // ==================== Property Tests for INCR/DECR ====================

    mod proptest_increment {
        use super::*;
        use proptest::prelude::*;

        /// Strategy for i64 values with room for operations (avoid overflow in tests)
        fn safe_i64() -> impl Strategy<Value = i64> {
            (i64::MIN / 2)..=(i64::MAX / 2)
        }

        /// Strategy for delta values that won't cause immediate overflow
        fn safe_delta() -> impl Strategy<Value = i64> {
            -1_000_000i64..=1_000_000i64
        }

        /// Strategy for small f64 values
        fn small_f64() -> impl Strategy<Value = f64> {
            -1e10f64..1e10f64
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(1000))]

            /// Roundtrip invariant: increment(d) followed by increment(-d) returns to original
            #[test]
            fn test_increment_roundtrip(initial in safe_i64(), delta in safe_delta()) {
                let mut sv = StringValue::from_integer(initial);

                // Skip if delta would cause overflow
                if initial.checked_add(delta).is_none() {
                    return Ok(());
                }

                let after_inc = sv.increment(delta).unwrap();
                prop_assert_eq!(after_inc, initial + delta);

                let after_dec = sv.increment(-delta).unwrap();
                prop_assert_eq!(after_dec, initial);
                prop_assert_eq!(sv.as_integer(), Some(initial));
            }

            /// Overflow at MAX: increment(+n) on values near i64::MAX returns Overflow error
            #[test]
            fn test_increment_overflow_at_max(n in 1i64..=1000i64) {
                let mut sv = StringValue::from_integer(i64::MAX);
                let result = sv.increment(n);
                prop_assert_eq!(result, Err(IncrementError::Overflow));
            }

            /// Underflow at MIN: increment(-n) on values near i64::MIN returns Overflow error
            #[test]
            fn test_increment_underflow_at_min(n in 1i64..=1000i64) {
                let mut sv = StringValue::from_integer(i64::MIN);
                let result = sv.increment(-n);
                prop_assert_eq!(result, Err(IncrementError::Overflow));
            }

            /// Type preservation: Integer encoding stays Integer after increment
            #[test]
            fn test_increment_type_preservation(initial in safe_i64(), delta in safe_delta()) {
                let mut sv = StringValue::from_integer(initial);

                // Skip if delta would cause overflow
                if initial.checked_add(delta).is_none() {
                    return Ok(());
                }

                sv.increment(delta).unwrap();

                // After increment, the value should still be accessible as an integer
                prop_assert!(sv.as_integer().is_some());
            }

            /// Commutativity: Order of increments doesn't affect final result
            #[test]
            fn test_increment_commutativity(
                initial in safe_i64(),
                delta1 in -10_000i64..=10_000i64,
                delta2 in -10_000i64..=10_000i64
            ) {
                // Skip if any operation would overflow
                let Some(step1) = initial.checked_add(delta1) else { return Ok(()); };
                let Some(final1) = step1.checked_add(delta2) else { return Ok(()); };

                let Some(step2) = initial.checked_add(delta2) else { return Ok(()); };
                let Some(final2) = step2.checked_add(delta1) else { return Ok(()); };

                let mut sv1 = StringValue::from_integer(initial);
                sv1.increment(delta1).unwrap();
                sv1.increment(delta2).unwrap();

                let mut sv2 = StringValue::from_integer(initial);
                sv2.increment(delta2).unwrap();
                sv2.increment(delta1).unwrap();

                prop_assert_eq!(sv1.as_integer(), sv2.as_integer());
                prop_assert_eq!(sv1.as_integer(), Some(final1));
                prop_assert_eq!(final1, final2);
            }

            /// Associativity: increment(n) == increment(1) repeated n times (for small n)
            #[test]
            fn test_increment_associativity(initial in safe_i64(), n in 1i64..=100i64) {
                // Skip if operations would overflow
                if initial.checked_add(n).is_none() {
                    return Ok(());
                }

                // Single increment by n
                let mut sv1 = StringValue::from_integer(initial);
                let result1 = sv1.increment(n).unwrap();

                // n increments by 1
                let mut sv2 = StringValue::from_integer(initial);
                for _ in 0..n {
                    sv2.increment(1).unwrap();
                }

                prop_assert_eq!(result1, sv2.as_integer().unwrap());
            }

            /// Float roundtrip: increment_float(d) followed by increment_float(-d) within epsilon
            #[test]
            fn test_increment_float_roundtrip(initial in small_f64(), delta in small_f64()) {
                // Skip zero delta to avoid precision issues
                if delta.abs() < 1e-15 {
                    return Ok(());
                }

                let mut sv = StringValue::new(initial.to_string());

                // Do increment and decrement
                let after_inc = sv.increment_float(delta);
                if after_inc.is_err() {
                    // Skip if increment causes overflow (infinity)
                    return Ok(());
                }

                let after_dec = sv.increment_float(-delta);
                if after_dec.is_err() {
                    return Ok(());
                }

                let result = after_dec.unwrap();

                // Check within epsilon (relative for large values, absolute for small)
                let epsilon = if initial.abs() > 1.0 {
                    initial.abs() * 1e-10
                } else {
                    1e-10
                };

                prop_assert!(
                    (result - initial).abs() < epsilon,
                    "Expected {} to be within {} of {}, but difference was {}",
                    result, epsilon, initial, (result - initial).abs()
                );
            }

            /// NaN rejection: increment_float rejects NaN input
            #[test]
            fn test_increment_float_rejects_nan(_dummy in 0i32..1i32) {
                let mut sv = StringValue::new("nan");
                let result = sv.increment_float(1.0);
                // NaN values should fail to parse as float
                prop_assert!(result.is_err());
            }

            /// Infinity rejection: increment_float rejects infinity result
            #[test]
            fn test_increment_float_rejects_infinity_result(_dummy in 0i32..1i32) {
                let mut sv = StringValue::new(f64::MAX.to_string());
                let result = sv.increment_float(f64::MAX);
                prop_assert_eq!(result, Err(IncrementError::Overflow));
            }

            /// NotInteger error: Non-numeric strings return NotInteger error
            #[test]
            fn test_increment_not_integer(s in "[a-zA-Z]{1,10}") {
                let mut sv = StringValue::new(s.clone());
                let result = sv.increment(1);
                prop_assert_eq!(result, Err(IncrementError::NotInteger));
            }

            /// NotFloat error: Non-numeric strings return NotFloat error for float increment
            #[test]
            fn test_increment_float_not_float(s in "[a-zA-Z]{1,10}") {
                let mut sv = StringValue::new(s.clone());
                let result = sv.increment_float(1.0);
                prop_assert_eq!(result, Err(IncrementError::NotFloat));
            }
        }
    }
}
