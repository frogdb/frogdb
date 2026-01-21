//! Binary serialization for persistent storage.
//!
//! Header format (24 bytes):
//! ```text
//! [type:u8][flags:u8][expires_at_ms:i64][lfu:u8][padding:5][payload_len:u64]
//! ```
//!
//! Type bytes:
//! - 0: String (raw bytes)
//! - 1: String (integer-encoded)
//! - 2: SortedSet
//!
//! Payload formats:
//! - String (raw): raw bytes
//! - String (integer): 8 bytes i64 little-endian
//! - SortedSet: [len:u32]([score:f64][member_len:u32][member_bytes]...)

use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

use crate::bloom::{BloomFilterValue, BloomLayer};
use crate::types::{HashValue, KeyMetadata, ListValue, SetValue, SortedSetValue, StreamId, StreamIdSpec, StreamValue, StringValue, Value};
use bitvec::prelude::*;

/// Size of the serialization header in bytes.
pub const HEADER_SIZE: usize = 24;

/// Marker for raw string type.
const TYPE_STRING_RAW: u8 = 0;
/// Marker for integer-encoded string type.
const TYPE_STRING_INT: u8 = 1;
/// Marker for sorted set type.
const TYPE_SORTED_SET: u8 = 2;
/// Marker for hash type.
const TYPE_HASH: u8 = 3;
/// Marker for list type.
const TYPE_LIST: u8 = 4;
/// Marker for set type.
const TYPE_SET: u8 = 5;
/// Marker for stream type.
const TYPE_STREAM: u8 = 6;
/// Marker for bloom filter type.
const TYPE_BLOOM: u8 = 7;

/// Errors that can occur during deserialization.
#[derive(Debug, Error)]
pub enum SerializationError {
    #[error("Invalid header: {0}")]
    InvalidHeader(String),

    #[error("Unknown type marker: {0}")]
    UnknownType(u8),

    #[error("Invalid payload: {0}")]
    InvalidPayload(String),

    #[error("Data truncated: expected {expected} bytes, got {actual}")]
    Truncated { expected: usize, actual: usize },
}

/// Serialize a key-value pair with metadata for persistent storage.
///
/// Returns a byte vector containing the header and payload.
pub fn serialize(value: &Value, metadata: &KeyMetadata) -> Vec<u8> {
    let (type_byte, payload) = serialize_value(value);

    let mut result = Vec::with_capacity(HEADER_SIZE + payload.len());

    // Type (1 byte)
    result.push(type_byte);

    // Flags (1 byte) - reserved for future use
    result.push(0);

    // Expires at (8 bytes) - Unix timestamp in milliseconds, -1 for no expiry
    let expires_ms = metadata
        .expires_at
        .map(instant_to_unix_ms)
        .unwrap_or(-1);
    result.extend_from_slice(&expires_ms.to_le_bytes());

    // LFU counter (1 byte)
    result.push(metadata.lfu_counter);

    // Padding (5 bytes)
    result.extend_from_slice(&[0u8; 5]);

    // Payload length (8 bytes)
    result.extend_from_slice(&(payload.len() as u64).to_le_bytes());

    // Payload
    result.extend_from_slice(&payload);

    result
}

/// Deserialize a value and metadata from persistent storage.
///
/// Returns the value and metadata, or an error if deserialization fails.
pub fn deserialize(data: &[u8]) -> Result<(Value, KeyMetadata), SerializationError> {
    if data.len() < HEADER_SIZE {
        return Err(SerializationError::Truncated {
            expected: HEADER_SIZE,
            actual: data.len(),
        });
    }

    // Parse header
    let type_byte = data[0];
    let _flags = data[1];

    let expires_ms = i64::from_le_bytes(data[2..10].try_into().unwrap());
    let lfu_counter = data[10];
    // Skip padding (5 bytes)
    let payload_len = u64::from_le_bytes(data[16..24].try_into().unwrap()) as usize;

    let total_len = HEADER_SIZE + payload_len;
    if data.len() < total_len {
        return Err(SerializationError::Truncated {
            expected: total_len,
            actual: data.len(),
        });
    }

    let payload = &data[HEADER_SIZE..total_len];

    // Deserialize value
    let value = deserialize_value(type_byte, payload)?;

    // Build metadata
    let expires_at = if expires_ms < 0 {
        None
    } else {
        Some(unix_ms_to_instant(expires_ms))
    };

    let metadata = KeyMetadata {
        expires_at,
        last_access: Instant::now(),
        lfu_counter,
        memory_size: value.memory_size(),
    };

    Ok((value, metadata))
}

/// Serialize a value to its type byte and payload.
fn serialize_value(value: &Value) -> (u8, Vec<u8>) {
    match value {
        Value::String(sv) => serialize_string(sv),
        Value::SortedSet(zset) => serialize_sorted_set(zset),
        Value::Hash(hash) => serialize_hash(hash),
        Value::List(list) => serialize_list(list),
        Value::Set(set) => serialize_set(set),
        Value::Stream(stream) => serialize_stream(stream),
        Value::BloomFilter(bf) => serialize_bloom_filter(bf),
    }
}

/// Serialize a string value.
fn serialize_string(sv: &StringValue) -> (u8, Vec<u8>) {
    // Check if it's integer-encoded by trying to parse as integer
    if let Some(i) = sv.as_integer() {
        // Verify it's actually stored as integer (not a string that happens to parse as int)
        let bytes = sv.as_bytes();
        if let Ok(s) = std::str::from_utf8(&bytes) {
            if let Ok(parsed) = s.parse::<i64>() {
                if parsed == i {
                    // It's integer-encoded
                    return (TYPE_STRING_INT, i.to_le_bytes().to_vec());
                }
            }
        }
    }

    // Raw bytes
    (TYPE_STRING_RAW, sv.as_bytes().to_vec())
}

/// Serialize a sorted set.
fn serialize_sorted_set(zset: &SortedSetValue) -> (u8, Vec<u8>) {
    let entries = zset.to_vec();
    let len = entries.len() as u32;

    // Calculate size: 4 (len) + sum of (8 (score) + 4 (member_len) + member_bytes)
    let payload_size: usize =
        4 + entries.iter().map(|(m, _)| 8 + 4 + m.len()).sum::<usize>();

    let mut payload = Vec::with_capacity(payload_size);

    // Number of entries
    payload.extend_from_slice(&len.to_le_bytes());

    // Each entry: score (f64) + member_len (u32) + member_bytes
    for (member, score) in entries {
        payload.extend_from_slice(&score.to_le_bytes());
        payload.extend_from_slice(&(member.len() as u32).to_le_bytes());
        payload.extend_from_slice(&member);
    }

    (TYPE_SORTED_SET, payload)
}

/// Serialize a hash.
fn serialize_hash(hash: &HashValue) -> (u8, Vec<u8>) {
    let entries = hash.to_vec();
    let len = entries.len() as u32;

    // Calculate size: 4 (len) + sum of (4 (field_len) + field + 4 (value_len) + value)
    let payload_size: usize =
        4 + entries.iter().map(|(f, v)| 4 + f.len() + 4 + v.len()).sum::<usize>();

    let mut payload = Vec::with_capacity(payload_size);

    // Number of entries
    payload.extend_from_slice(&len.to_le_bytes());

    // Each entry: field_len (u32) + field + value_len (u32) + value
    for (field, value) in entries {
        payload.extend_from_slice(&(field.len() as u32).to_le_bytes());
        payload.extend_from_slice(&field);
        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
        payload.extend_from_slice(&value);
    }

    (TYPE_HASH, payload)
}

/// Serialize a list.
fn serialize_list(list: &ListValue) -> (u8, Vec<u8>) {
    let entries = list.to_vec();
    let len = entries.len() as u32;

    // Calculate size: 4 (len) + sum of (4 (elem_len) + elem)
    let payload_size: usize =
        4 + entries.iter().map(|e| 4 + e.len()).sum::<usize>();

    let mut payload = Vec::with_capacity(payload_size);

    // Number of entries
    payload.extend_from_slice(&len.to_le_bytes());

    // Each entry: elem_len (u32) + elem
    for elem in entries {
        payload.extend_from_slice(&(elem.len() as u32).to_le_bytes());
        payload.extend_from_slice(&elem);
    }

    (TYPE_LIST, payload)
}

/// Serialize a set.
fn serialize_set(set: &SetValue) -> (u8, Vec<u8>) {
    let entries = set.to_vec();
    let len = entries.len() as u32;

    // Calculate size: 4 (len) + sum of (4 (member_len) + member)
    let payload_size: usize =
        4 + entries.iter().map(|m| 4 + m.len()).sum::<usize>();

    let mut payload = Vec::with_capacity(payload_size);

    // Number of entries
    payload.extend_from_slice(&len.to_le_bytes());

    // Each entry: member_len (u32) + member
    for member in entries {
        payload.extend_from_slice(&(member.len() as u32).to_le_bytes());
        payload.extend_from_slice(&member);
    }

    (TYPE_SET, payload)
}

/// Serialize a stream.
///
/// Format:
/// - last_id_ms (8 bytes u64)
/// - last_id_seq (8 bytes u64)
/// - num_entries (4 bytes u32)
/// - for each entry:
///   - id_ms (8 bytes u64)
///   - id_seq (8 bytes u64)
///   - num_fields (4 bytes u32)
///   - for each field:
///     - field_len (4 bytes u32)
///     - field bytes
///     - value_len (4 bytes u32)
///     - value bytes
///
/// Note: Consumer groups are not persisted (they are ephemeral state)
fn serialize_stream(stream: &StreamValue) -> (u8, Vec<u8>) {
    let entries = stream.to_vec();
    let last_id = stream.last_id();

    // Calculate size
    let mut payload_size = 8 + 8 + 4; // last_id_ms + last_id_seq + num_entries
    for entry in &entries {
        payload_size += 8 + 8 + 4; // id_ms + id_seq + num_fields
        for (field, value) in &entry.fields {
            payload_size += 4 + field.len() + 4 + value.len();
        }
    }

    let mut payload = Vec::with_capacity(payload_size);

    // Last ID
    payload.extend_from_slice(&last_id.ms.to_le_bytes());
    payload.extend_from_slice(&last_id.seq.to_le_bytes());

    // Number of entries
    payload.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    // Each entry
    for entry in entries {
        payload.extend_from_slice(&entry.id.ms.to_le_bytes());
        payload.extend_from_slice(&entry.id.seq.to_le_bytes());
        payload.extend_from_slice(&(entry.fields.len() as u32).to_le_bytes());

        for (field, value) in &entry.fields {
            payload.extend_from_slice(&(field.len() as u32).to_le_bytes());
            payload.extend_from_slice(field);
            payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
            payload.extend_from_slice(value);
        }
    }

    (TYPE_STREAM, payload)
}

/// Serialize a bloom filter.
///
/// Format:
/// - error_rate (8 bytes f64)
/// - expansion (4 bytes u32)
/// - non_scaling (1 byte bool)
/// - num_layers (4 bytes u32)
/// - for each layer:
///   - k (4 bytes u32) - number of hash functions
///   - count (8 bytes u64) - items in this layer
///   - capacity (8 bytes u64) - layer capacity
///   - bits_len (8 bytes u64) - number of bits
///   - bits_bytes (bits_len/8 rounded up)
fn serialize_bloom_filter(bf: &BloomFilterValue) -> (u8, Vec<u8>) {
    // Calculate size
    let mut payload_size = 8 + 4 + 1 + 4; // error_rate + expansion + non_scaling + num_layers
    for layer in bf.layers() {
        payload_size += 4 + 8 + 8 + 8; // k + count + capacity + bits_len
        payload_size += layer.bits_as_bytes().len();
    }

    let mut payload = Vec::with_capacity(payload_size);

    // Error rate
    payload.extend_from_slice(&bf.error_rate().to_le_bytes());

    // Expansion
    payload.extend_from_slice(&bf.expansion().to_le_bytes());

    // Non-scaling flag
    payload.push(if bf.is_non_scaling() { 1 } else { 0 });

    // Number of layers
    payload.extend_from_slice(&(bf.num_layers() as u32).to_le_bytes());

    // Each layer
    for layer in bf.layers() {
        payload.extend_from_slice(&layer.k().to_le_bytes());
        payload.extend_from_slice(&layer.count().to_le_bytes());
        payload.extend_from_slice(&layer.capacity().to_le_bytes());
        let bits_bytes = layer.bits_as_bytes();
        payload.extend_from_slice(&(layer.size_bits() as u64).to_le_bytes());
        payload.extend_from_slice(bits_bytes);
    }

    (TYPE_BLOOM, payload)
}

/// Deserialize a value from its type byte and payload.
fn deserialize_value(type_byte: u8, payload: &[u8]) -> Result<Value, SerializationError> {
    match type_byte {
        TYPE_STRING_RAW => {
            let sv = StringValue::new(Bytes::copy_from_slice(payload));
            Ok(Value::String(sv))
        }
        TYPE_STRING_INT => {
            if payload.len() != 8 {
                return Err(SerializationError::InvalidPayload(format!(
                    "Integer string expected 8 bytes, got {}",
                    payload.len()
                )));
            }
            let i = i64::from_le_bytes(payload.try_into().unwrap());
            let sv = StringValue::from_integer(i);
            Ok(Value::String(sv))
        }
        TYPE_SORTED_SET => {
            let zset = deserialize_sorted_set(payload)?;
            Ok(Value::SortedSet(zset))
        }
        TYPE_HASH => {
            let hash = deserialize_hash(payload)?;
            Ok(Value::Hash(hash))
        }
        TYPE_LIST => {
            let list = deserialize_list(payload)?;
            Ok(Value::List(list))
        }
        TYPE_SET => {
            let set = deserialize_set(payload)?;
            Ok(Value::Set(set))
        }
        TYPE_STREAM => {
            let stream = deserialize_stream(payload)?;
            Ok(Value::Stream(stream))
        }
        TYPE_BLOOM => {
            let bf = deserialize_bloom_filter(payload)?;
            Ok(Value::BloomFilter(bf))
        }
        _ => Err(SerializationError::UnknownType(type_byte)),
    }
}

/// Deserialize a sorted set from payload.
fn deserialize_sorted_set(payload: &[u8]) -> Result<SortedSetValue, SerializationError> {
    if payload.len() < 4 {
        return Err(SerializationError::InvalidPayload(
            "Sorted set payload too short for length".to_string(),
        ));
    }

    let len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut zset = SortedSetValue::new();

    for _ in 0..len {
        // Read score (8 bytes)
        if offset + 8 > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Sorted set payload truncated at score".to_string(),
            ));
        }
        let score = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read member length (4 bytes)
        if offset + 4 > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Sorted set payload truncated at member length".to_string(),
            ));
        }
        let member_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read member bytes
        if offset + member_len > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Sorted set payload truncated at member data".to_string(),
            ));
        }
        let member = Bytes::copy_from_slice(&payload[offset..offset + member_len]);
        offset += member_len;

        zset.add(member, score);
    }

    Ok(zset)
}

/// Deserialize a hash from payload.
fn deserialize_hash(payload: &[u8]) -> Result<HashValue, SerializationError> {
    if payload.len() < 4 {
        return Err(SerializationError::InvalidPayload(
            "Hash payload too short for length".to_string(),
        ));
    }

    let len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut hash = HashValue::new();

    for _ in 0..len {
        // Read field length (4 bytes)
        if offset + 4 > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at field length".to_string(),
            ));
        }
        let field_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read field bytes
        if offset + field_len > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at field data".to_string(),
            ));
        }
        let field = Bytes::copy_from_slice(&payload[offset..offset + field_len]);
        offset += field_len;

        // Read value length (4 bytes)
        if offset + 4 > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at value length".to_string(),
            ));
        }
        let value_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read value bytes
        if offset + value_len > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at value data".to_string(),
            ));
        }
        let value = Bytes::copy_from_slice(&payload[offset..offset + value_len]);
        offset += value_len;

        hash.set(field, value);
    }

    Ok(hash)
}

/// Deserialize a list from payload.
fn deserialize_list(payload: &[u8]) -> Result<ListValue, SerializationError> {
    if payload.len() < 4 {
        return Err(SerializationError::InvalidPayload(
            "List payload too short for length".to_string(),
        ));
    }

    let len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut list = ListValue::new();

    for _ in 0..len {
        // Read element length (4 bytes)
        if offset + 4 > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "List payload truncated at element length".to_string(),
            ));
        }
        let elem_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read element bytes
        if offset + elem_len > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "List payload truncated at element data".to_string(),
            ));
        }
        let elem = Bytes::copy_from_slice(&payload[offset..offset + elem_len]);
        offset += elem_len;

        list.push_back(elem);
    }

    Ok(list)
}

/// Deserialize a set from payload.
fn deserialize_set(payload: &[u8]) -> Result<SetValue, SerializationError> {
    if payload.len() < 4 {
        return Err(SerializationError::InvalidPayload(
            "Set payload too short for length".to_string(),
        ));
    }

    let len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut set = SetValue::new();

    for _ in 0..len {
        // Read member length (4 bytes)
        if offset + 4 > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Set payload truncated at member length".to_string(),
            ));
        }
        let member_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read member bytes
        if offset + member_len > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Set payload truncated at member data".to_string(),
            ));
        }
        let member = Bytes::copy_from_slice(&payload[offset..offset + member_len]);
        offset += member_len;

        set.add(member);
    }

    Ok(set)
}

/// Deserialize a stream from payload.
fn deserialize_stream(payload: &[u8]) -> Result<StreamValue, SerializationError> {
    if payload.len() < 20 {
        return Err(SerializationError::InvalidPayload(
            "Stream payload too short for header".to_string(),
        ));
    }

    // Read last ID
    let last_id_ms = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let last_id_seq = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    let _last_id = StreamId::new(last_id_ms, last_id_seq);

    // Read number of entries
    let num_entries = u32::from_le_bytes(payload[16..20].try_into().unwrap()) as usize;
    let mut offset = 20;
    let mut stream = StreamValue::new();

    for _ in 0..num_entries {
        // Read entry ID
        if offset + 20 > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Stream payload truncated at entry header".to_string(),
            ));
        }
        let id_ms = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let id_seq = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let id = StreamId::new(id_ms, id_seq);

        // Read number of fields
        let num_fields = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut fields = Vec::with_capacity(num_fields);
        for _ in 0..num_fields {
            // Read field length
            if offset + 4 > payload.len() {
                return Err(SerializationError::InvalidPayload(
                    "Stream payload truncated at field length".to_string(),
                ));
            }
            let field_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            // Read field bytes
            if offset + field_len > payload.len() {
                return Err(SerializationError::InvalidPayload(
                    "Stream payload truncated at field data".to_string(),
                ));
            }
            let field = Bytes::copy_from_slice(&payload[offset..offset + field_len]);
            offset += field_len;

            // Read value length
            if offset + 4 > payload.len() {
                return Err(SerializationError::InvalidPayload(
                    "Stream payload truncated at value length".to_string(),
                ));
            }
            let value_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            // Read value bytes
            if offset + value_len > payload.len() {
                return Err(SerializationError::InvalidPayload(
                    "Stream payload truncated at value data".to_string(),
                ));
            }
            let value = Bytes::copy_from_slice(&payload[offset..offset + value_len]);
            offset += value_len;

            fields.push((field, value));
        }

        // Add entry to stream with explicit ID
        let _ = stream.add(StreamIdSpec::Explicit(id), fields);
    }

    Ok(stream)
}

/// Deserialize a bloom filter from payload.
fn deserialize_bloom_filter(payload: &[u8]) -> Result<BloomFilterValue, SerializationError> {
    if payload.len() < 17 {
        return Err(SerializationError::InvalidPayload(
            "Bloom filter payload too short for header".to_string(),
        ));
    }

    // Read error_rate (8 bytes)
    let error_rate = f64::from_le_bytes(payload[0..8].try_into().unwrap());

    // Read expansion (4 bytes)
    let expansion = u32::from_le_bytes(payload[8..12].try_into().unwrap());

    // Read non_scaling (1 byte)
    let non_scaling = payload[12] != 0;

    // Read num_layers (4 bytes)
    let num_layers = u32::from_le_bytes(payload[13..17].try_into().unwrap()) as usize;

    let mut offset = 17;
    let mut layers = Vec::with_capacity(num_layers);

    for _ in 0..num_layers {
        // Read k (4 bytes)
        if offset + 4 > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at k".to_string(),
            ));
        }
        let k = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // Read count (8 bytes)
        if offset + 8 > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at count".to_string(),
            ));
        }
        let count = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read capacity (8 bytes)
        if offset + 8 > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at capacity".to_string(),
            ));
        }
        let capacity = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read bits_len (8 bytes)
        if offset + 8 > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at bits_len".to_string(),
            ));
        }
        let bits_len = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        // Read bits bytes (bits_len / 8 rounded up)
        let bytes_needed = (bits_len + 7) / 8;
        if offset + bytes_needed > payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at bits data".to_string(),
            ));
        }
        let bits_bytes = &payload[offset..offset + bytes_needed];
        offset += bytes_needed;

        // Reconstruct the bitvec
        let mut bits: BitVec<u8, Lsb0> = BitVec::from_slice(bits_bytes);
        bits.truncate(bits_len);

        layers.push(BloomLayer::from_raw(bits, k, count, capacity));
    }

    Ok(BloomFilterValue::from_raw(layers, error_rate, expansion, non_scaling))
}

/// Convert an Instant to Unix timestamp in milliseconds.
///
/// This is tricky because Instant is monotonic and not tied to wall clock.
/// We calculate the offset from now.
pub fn instant_to_unix_ms(instant: Instant) -> i64 {
    let now_instant = Instant::now();
    let now_system = SystemTime::now();

    if instant >= now_instant {
        // Future instant
        let duration = instant.duration_since(now_instant);
        let target = now_system + duration;
        target
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(i64::MAX)
    } else {
        // Past instant
        let duration = now_instant.duration_since(instant);
        if let Some(target) = now_system.checked_sub(duration) {
            target
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        } else {
            0
        }
    }
}

/// Convert a Unix timestamp in milliseconds to an Instant.
///
/// This is the inverse of instant_to_unix_ms.
pub fn unix_ms_to_instant(unix_ms: i64) -> Instant {
    let now_instant = Instant::now();
    let now_system = SystemTime::now();

    let target = UNIX_EPOCH + Duration::from_millis(unix_ms as u64);

    match target.duration_since(now_system) {
        Ok(duration) => {
            // Target is in the future
            now_instant + duration
        }
        Err(e) => {
            // Target is in the past
            let duration = e.duration();
            now_instant.checked_sub(duration).unwrap_or(now_instant)
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_string_raw() {
        let value = Value::string("hello world");
        let metadata = KeyMetadata::new(11);

        let data = serialize(&value, &metadata);
        let (value2, metadata2) = deserialize(&data).unwrap();

        assert_eq!(value2.as_string().unwrap().as_bytes().as_ref(), b"hello world");
        assert_eq!(metadata2.lfu_counter, metadata.lfu_counter);
        assert!(metadata2.expires_at.is_none());
    }

    #[test]
    fn test_serialize_deserialize_string_integer() {
        let value = Value::String(StringValue::from_integer(42));
        let metadata = KeyMetadata::new(8);

        let data = serialize(&value, &metadata);

        // Check type byte is integer
        assert_eq!(data[0], TYPE_STRING_INT);

        let (value2, _) = deserialize(&data).unwrap();
        assert_eq!(value2.as_string().unwrap().as_integer(), Some(42));
    }

    #[test]
    fn test_serialize_deserialize_sorted_set() {
        let mut zset = SortedSetValue::new();
        zset.add(Bytes::from("one"), 1.0);
        zset.add(Bytes::from("two"), 2.0);
        zset.add(Bytes::from("three"), 3.0);

        let value = Value::SortedSet(zset);
        let metadata = KeyMetadata::new(100);

        let data = serialize(&value, &metadata);
        let (value2, _) = deserialize(&data).unwrap();

        let zset2 = value2.as_sorted_set().unwrap();
        assert_eq!(zset2.len(), 3);
        assert_eq!(zset2.get_score(b"one"), Some(1.0));
        assert_eq!(zset2.get_score(b"two"), Some(2.0));
        assert_eq!(zset2.get_score(b"three"), Some(3.0));
    }

    #[test]
    fn test_serialize_deserialize_with_expiry() {
        let value = Value::string("test");
        let mut metadata = KeyMetadata::new(4);
        metadata.expires_at = Some(Instant::now() + Duration::from_secs(60));

        let data = serialize(&value, &metadata);
        let (_, metadata2) = deserialize(&data).unwrap();

        assert!(metadata2.expires_at.is_some());
        // The expiry should be approximately 60 seconds from now
        let expires_at = metadata2.expires_at.unwrap();
        let duration = expires_at.duration_since(Instant::now());
        assert!(duration.as_secs() >= 58 && duration.as_secs() <= 62);
    }

    #[test]
    fn test_serialize_deserialize_empty_sorted_set() {
        let zset = SortedSetValue::new();
        let value = Value::SortedSet(zset);
        let metadata = KeyMetadata::new(0);

        let data = serialize(&value, &metadata);
        let (value2, _) = deserialize(&data).unwrap();

        let zset2 = value2.as_sorted_set().unwrap();
        assert!(zset2.is_empty());
    }

    #[test]
    fn test_deserialize_truncated() {
        let data = vec![0u8; 10]; // Too short for header
        let result = deserialize(&data);
        assert!(matches!(result, Err(SerializationError::Truncated { .. })));
    }

    #[test]
    fn test_deserialize_unknown_type() {
        let mut data = vec![0u8; HEADER_SIZE];
        data[0] = 255; // Unknown type
        // Set payload_len to 0
        data[16..24].copy_from_slice(&0u64.to_le_bytes());

        let result = deserialize(&data);
        assert!(matches!(result, Err(SerializationError::UnknownType(255))));
    }

    #[test]
    fn test_instant_unix_ms_roundtrip() {
        let now = Instant::now();
        let future = now + Duration::from_secs(3600);

        let ms = instant_to_unix_ms(future);
        let back = unix_ms_to_instant(ms);

        // Should be within a few milliseconds
        let diff = if back > future {
            back.duration_since(future)
        } else {
            future.duration_since(back)
        };
        assert!(diff.as_millis() < 100);
    }
}
