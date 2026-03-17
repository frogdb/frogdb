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

use bitvec::prelude::*;
use frogdb_types::bloom::{BloomFilterValue, BloomLayer};
use frogdb_types::hyperloglog::{HLL_DENSE_SIZE, HyperLogLogValue};
use frogdb_types::json::JsonValue;
use frogdb_types::timeseries::{CompressedChunk, DuplicatePolicy, TimeSeriesValue};
use frogdb_types::types::{
    HashValue, IdempotencyState, KeyMetadata, ListValue, ListpackThresholds, SetValue,
    SortedSetValue, StreamId, StreamIdSpec, StreamValue, StringValue, Value,
};

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
/// Marker for HyperLogLog type.
const TYPE_HYPERLOGLOG: u8 = 8;
/// Marker for TimeSeries type.
const TYPE_TIMESERIES: u8 = 9;
/// Marker for JSON type.
const TYPE_JSON: u8 = 10;

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
    let expires_ms = metadata.expires_at.map(instant_to_unix_ms).unwrap_or(-1);
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

    if payload_len > data.len() - HEADER_SIZE {
        return Err(SerializationError::Truncated {
            expected: HEADER_SIZE.saturating_add(payload_len),
            actual: data.len(),
        });
    }

    let total_len = HEADER_SIZE + payload_len;
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
        Value::HyperLogLog(hll) => serialize_hyperloglog(hll),
        Value::TimeSeries(ts) => serialize_timeseries(ts),
        Value::Json(json) => serialize_json(json),
    }
}

/// Serialize a string value.
fn serialize_string(sv: &StringValue) -> (u8, Vec<u8>) {
    // Check if it's integer-encoded by trying to parse as integer
    if let Some(i) = sv.as_integer() {
        // Verify it's actually stored as integer (not a string that happens to parse as int)
        let bytes = sv.as_bytes();
        if let Ok(s) = std::str::from_utf8(&bytes)
            && let Ok(parsed) = s.parse::<i64>()
            && parsed == i
        {
            // It's integer-encoded
            return (TYPE_STRING_INT, i.to_le_bytes().to_vec());
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
    let payload_size: usize = 4 + entries.iter().map(|(m, _)| 8 + 4 + m.len()).sum::<usize>();

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
    let payload_size: usize = 4 + entries
        .iter()
        .map(|(f, v)| 4 + f.len() + 4 + v.len())
        .sum::<usize>();

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
    let payload_size: usize = 4 + entries.iter().map(|e| 4 + e.len()).sum::<usize>();

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
    let payload_size: usize = 4 + entries.iter().map(|m| 4 + m.len()).sum::<usize>();

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

    // Event sourcing extension: total_appended + idempotency keys
    // (backward-compatible: old readers stop after entries)
    payload.extend_from_slice(&stream.total_appended().to_le_bytes()); // 8 bytes
    if let Some(idem) = stream.idempotency() {
        let count = idem.len() as u32;
        payload.extend_from_slice(&count.to_le_bytes()); // 4 bytes
        for key in idem.iter() {
            payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
            payload.extend_from_slice(key);
        }
    } else {
        payload.extend_from_slice(&0u32.to_le_bytes()); // 0 idempotency keys
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

/// Serialize a HyperLogLog.
///
/// Format:
/// - encoding (1 byte): 0 = sparse, 1 = dense
/// - if sparse:
///   - num_entries (4 bytes u32)
///   - for each entry: (index: u16, value: u8) = 3 bytes
/// - if dense:
///   - 12288 bytes raw packed registers
fn serialize_hyperloglog(hll: &HyperLogLogValue) -> (u8, Vec<u8>) {
    if let Some(pairs) = hll.as_sparse() {
        // Sparse encoding
        let payload_size = 1 + 4 + pairs.len() * 3;
        let mut payload = Vec::with_capacity(payload_size);

        // Encoding byte (0 = sparse)
        payload.push(0);

        // Number of entries
        payload.extend_from_slice(&(pairs.len() as u32).to_le_bytes());

        // Each entry: index (u16) + value (u8)
        for (index, value) in pairs {
            payload.extend_from_slice(&index.to_le_bytes());
            payload.push(*value);
        }

        (TYPE_HYPERLOGLOG, payload)
    } else if let Some(registers) = hll.as_dense() {
        // Dense encoding
        let mut payload = Vec::with_capacity(1 + HLL_DENSE_SIZE);

        // Encoding byte (1 = dense)
        payload.push(1);

        // Raw registers
        payload.extend_from_slice(registers.as_slice());

        (TYPE_HYPERLOGLOG, payload)
    } else {
        // Shouldn't happen, but fallback to empty sparse
        (TYPE_HYPERLOGLOG, vec![0, 0, 0, 0, 0])
    }
}

/// Serialize a time series.
///
/// Format:
/// - retention_ms (8 bytes u64)
/// - duplicate_policy (1 byte)
/// - chunk_size (4 bytes u32)
/// - num_labels (4 bytes u32)
/// - for each label:
///   - name_len (4 bytes u32) + name bytes
///   - value_len (4 bytes u32) + value bytes
/// - num_chunks (4 bytes u32)
/// - for each chunk:
///   - start_time (8 bytes i64)
///   - end_time (8 bytes i64)
///   - sample_count (4 bytes u32)
///   - data_len (4 bytes u32)
///   - data bytes
/// - num_active (4 bytes u32)
/// - for each active sample:
///   - timestamp (8 bytes i64)
///   - value (8 bytes f64)
fn serialize_timeseries(ts: &TimeSeriesValue) -> (u8, Vec<u8>) {
    let mut payload = Vec::new();

    // Retention
    payload.extend_from_slice(&ts.retention_ms().to_le_bytes());

    // Duplicate policy
    let policy_byte = match ts.duplicate_policy() {
        DuplicatePolicy::Block => 0u8,
        DuplicatePolicy::First => 1u8,
        DuplicatePolicy::Last => 2u8,
        DuplicatePolicy::Min => 3u8,
        DuplicatePolicy::Max => 4u8,
        DuplicatePolicy::Sum => 5u8,
    };
    payload.push(policy_byte);

    // Chunk size
    payload.extend_from_slice(&(ts.chunk_size() as u32).to_le_bytes());

    // Labels
    let labels = ts.labels();
    payload.extend_from_slice(&(labels.len() as u32).to_le_bytes());
    for (name, value) in labels {
        payload.extend_from_slice(&(name.len() as u32).to_le_bytes());
        payload.extend_from_slice(name.as_bytes());
        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
        payload.extend_from_slice(value.as_bytes());
    }

    // Chunks
    let chunks = ts.chunks();
    payload.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    for chunk in chunks {
        payload.extend_from_slice(&chunk.start_time().to_le_bytes());
        payload.extend_from_slice(&chunk.end_time().to_le_bytes());
        payload.extend_from_slice(&chunk.sample_count().to_le_bytes());
        let data = chunk.data();
        payload.extend_from_slice(&(data.len() as u32).to_le_bytes());
        payload.extend_from_slice(data);
    }

    // Active samples
    let active = ts.active_samples();
    payload.extend_from_slice(&(active.len() as u32).to_le_bytes());
    for (&ts_val, &val) in active {
        payload.extend_from_slice(&ts_val.to_le_bytes());
        payload.extend_from_slice(&val.to_le_bytes());
    }

    (TYPE_TIMESERIES, payload)
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
        TYPE_HYPERLOGLOG => {
            let hll = deserialize_hyperloglog(payload)?;
            Ok(Value::HyperLogLog(hll))
        }
        TYPE_TIMESERIES => {
            let ts = deserialize_timeseries(payload)?;
            Ok(Value::TimeSeries(ts))
        }
        TYPE_JSON => {
            let json = deserialize_json(payload)?;
            Ok(Value::Json(json))
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
        if 8 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Sorted set payload truncated at score".to_string(),
            ));
        }
        let score = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read member length (4 bytes)
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Sorted set payload truncated at member length".to_string(),
            ));
        }
        let member_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read member bytes
        if member_len > payload.len() - offset {
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
    let mut entries = Vec::with_capacity(len);

    for _ in 0..len {
        // Read field length (4 bytes)
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at field length".to_string(),
            ));
        }
        let field_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read field bytes
        if field_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at field data".to_string(),
            ));
        }
        let field = Bytes::copy_from_slice(&payload[offset..offset + field_len]);
        offset += field_len;

        // Read value length (4 bytes)
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at value length".to_string(),
            ));
        }
        let value_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read value bytes
        if value_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at value data".to_string(),
            ));
        }
        let value = Bytes::copy_from_slice(&payload[offset..offset + value_len]);
        offset += value_len;

        entries.push((field, value));
    }

    Ok(HashValue::from_entries(
        entries,
        ListpackThresholds::DEFAULT_HASH,
    ))
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
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "List payload truncated at element length".to_string(),
            ));
        }
        let elem_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read element bytes
        if elem_len > payload.len() - offset {
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
    let mut members = Vec::with_capacity(len);

    for _ in 0..len {
        // Read member length (4 bytes)
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Set payload truncated at member length".to_string(),
            ));
        }
        let member_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read member bytes
        if member_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Set payload truncated at member data".to_string(),
            ));
        }
        let member = Bytes::copy_from_slice(&payload[offset..offset + member_len]);
        offset += member_len;

        members.push(member);
    }

    Ok(SetValue::from_members(
        members,
        ListpackThresholds::DEFAULT_SET,
    ))
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
        if 20 > payload.len() - offset {
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
        let num_fields =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut fields = Vec::with_capacity(num_fields);
        for _ in 0..num_fields {
            // Read field length
            if 4 > payload.len() - offset {
                return Err(SerializationError::InvalidPayload(
                    "Stream payload truncated at field length".to_string(),
                ));
            }
            let field_len =
                u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            // Read field bytes
            if field_len > payload.len() - offset {
                return Err(SerializationError::InvalidPayload(
                    "Stream payload truncated at field data".to_string(),
                ));
            }
            let field = Bytes::copy_from_slice(&payload[offset..offset + field_len]);
            offset += field_len;

            // Read value length
            if 4 > payload.len() - offset {
                return Err(SerializationError::InvalidPayload(
                    "Stream payload truncated at value length".to_string(),
                ));
            }
            let value_len =
                u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            // Read value bytes
            if value_len > payload.len() - offset {
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

    // Event sourcing extension: read total_appended + idempotency keys if present
    // (backward-compatible: old format ends here, so check remaining bytes)
    if offset + 8 <= payload.len() {
        let total_appended = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        stream.set_total_appended(total_appended);

        if offset + 4 <= payload.len() {
            let num_idem_keys =
                u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            if num_idem_keys > 0 {
                let mut idem = IdempotencyState::new();
                for _ in 0..num_idem_keys {
                    if offset + 4 > payload.len() {
                        break;
                    }
                    let key_len =
                        u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap())
                            as usize;
                    offset += 4;
                    if offset + key_len > payload.len() {
                        break;
                    }
                    let key = Bytes::copy_from_slice(&payload[offset..offset + key_len]);
                    offset += key_len;
                    idem.record(key);
                }
                stream.set_idempotency(idem);
            }
        }
    } else {
        // Old format: default total_appended to number of entries
        stream.set_total_appended(num_entries as u64);
    }

    let _ = offset; // suppress unused warning

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
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at k".to_string(),
            ));
        }
        let k = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // Read count (8 bytes)
        if 8 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at count".to_string(),
            ));
        }
        let count = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read capacity (8 bytes)
        if 8 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at capacity".to_string(),
            ));
        }
        let capacity = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read bits_len (8 bytes)
        if 8 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at bits_len".to_string(),
            ));
        }
        let bits_len = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        // Read bits bytes (bits_len / 8 rounded up)
        let bytes_needed = bits_len.div_ceil(8);
        if bytes_needed > payload.len() - offset {
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

    Ok(BloomFilterValue::from_raw(
        layers,
        error_rate,
        expansion,
        non_scaling,
    ))
}

/// Deserialize a HyperLogLog from payload.
fn deserialize_hyperloglog(payload: &[u8]) -> Result<HyperLogLogValue, SerializationError> {
    if payload.is_empty() {
        return Err(SerializationError::InvalidPayload(
            "HyperLogLog payload empty".to_string(),
        ));
    }

    let encoding = payload[0];

    match encoding {
        0 => {
            // Sparse encoding
            if payload.len() < 5 {
                return Err(SerializationError::InvalidPayload(
                    "HyperLogLog sparse payload too short".to_string(),
                ));
            }

            let num_entries = u32::from_le_bytes(payload[1..5].try_into().unwrap()) as usize;

            // Each entry is 3 bytes (u16 index + u8 value)
            let expected_len = num_entries
                .checked_mul(3)
                .and_then(|v| v.checked_add(5))
                .ok_or(SerializationError::InvalidPayload(
                    "HyperLogLog sparse payload size overflow".to_string(),
                ))?;
            if payload.len() < expected_len {
                return Err(SerializationError::InvalidPayload(
                    "HyperLogLog sparse payload truncated".to_string(),
                ));
            }

            let mut pairs = Vec::with_capacity(num_entries);
            let mut offset = 5;

            for _ in 0..num_entries {
                let index = u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap());
                let value = payload[offset + 2];
                pairs.push((index, value));
                offset += 3;
            }

            Ok(HyperLogLogValue::from_sparse(pairs))
        }
        1 => {
            // Dense encoding
            if payload.len() < 1 + HLL_DENSE_SIZE {
                return Err(SerializationError::InvalidPayload(
                    "HyperLogLog dense payload truncated".to_string(),
                ));
            }

            let mut registers = Box::new([0u8; HLL_DENSE_SIZE]);
            registers.copy_from_slice(&payload[1..1 + HLL_DENSE_SIZE]);

            Ok(HyperLogLogValue::from_dense(registers))
        }
        _ => Err(SerializationError::InvalidPayload(format!(
            "Unknown HyperLogLog encoding: {}",
            encoding
        ))),
    }
}

/// Deserialize a time series from payload.
fn deserialize_timeseries(payload: &[u8]) -> Result<TimeSeriesValue, SerializationError> {
    if payload.len() < 17 {
        return Err(SerializationError::InvalidPayload(
            "TimeSeries payload too short for header".to_string(),
        ));
    }

    let mut offset = 0;

    // Retention
    let retention_ms = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
    offset += 8;

    // Duplicate policy
    let policy = match payload[offset] {
        0 => DuplicatePolicy::Block,
        1 => DuplicatePolicy::First,
        2 => DuplicatePolicy::Last,
        3 => DuplicatePolicy::Min,
        4 => DuplicatePolicy::Max,
        5 => DuplicatePolicy::Sum,
        _ => DuplicatePolicy::Last, // Default fallback
    };
    offset += 1;

    // Chunk size
    let chunk_size = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    // Labels
    if 4 > payload.len() - offset {
        return Err(SerializationError::InvalidPayload(
            "TimeSeries payload truncated at labels count".to_string(),
        ));
    }
    let num_labels = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut labels = Vec::with_capacity(num_labels);
    for _ in 0..num_labels {
        // Name
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at label name length".to_string(),
            ));
        }
        let name_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if name_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at label name".to_string(),
            ));
        }
        let name = String::from_utf8_lossy(&payload[offset..offset + name_len]).to_string();
        offset += name_len;

        // Value
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at label value length".to_string(),
            ));
        }
        let value_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if value_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at label value".to_string(),
            ));
        }
        let value = String::from_utf8_lossy(&payload[offset..offset + value_len]).to_string();
        offset += value_len;

        labels.push((name, value));
    }

    // Chunks
    if 4 > payload.len() - offset {
        return Err(SerializationError::InvalidPayload(
            "TimeSeries payload truncated at chunks count".to_string(),
        ));
    }
    let num_chunks = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut chunks = Vec::with_capacity(num_chunks);
    for _ in 0..num_chunks {
        if 24 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at chunk header".to_string(),
            ));
        }

        let start_time = i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let end_time = i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let sample_count = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let data_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if data_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at chunk data".to_string(),
            ));
        }
        let data = payload[offset..offset + data_len].to_vec();
        offset += data_len;

        chunks.push(CompressedChunk::from_raw(
            data,
            start_time,
            end_time,
            sample_count,
        ));
    }

    // Active samples
    if 4 > payload.len() - offset {
        return Err(SerializationError::InvalidPayload(
            "TimeSeries payload truncated at active samples count".to_string(),
        ));
    }
    let num_active = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut active_samples = std::collections::BTreeMap::new();
    for _ in 0..num_active {
        if 16 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at active sample".to_string(),
            ));
        }

        let ts = i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let val = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        active_samples.insert(ts, val);
    }

    Ok(TimeSeriesValue::from_raw(
        active_samples,
        chunks,
        labels,
        retention_ms,
        policy,
        chunk_size,
    ))
}

/// Serialize a JSON value.
fn serialize_json(json: &JsonValue) -> (u8, Vec<u8>) {
    let payload = json.to_bytes();
    (TYPE_JSON, payload)
}

/// Deserialize a JSON value.
fn deserialize_json(payload: &[u8]) -> Result<JsonValue, SerializationError> {
    JsonValue::parse(payload).map_err(|e| SerializationError::InvalidPayload(e.to_string()))
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

        assert_eq!(
            value2.as_string().unwrap().as_bytes().as_ref(),
            b"hello world"
        );
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

    #[test]
    fn test_serialize_deserialize_hyperloglog_sparse() {
        let mut hll = HyperLogLogValue::new();
        hll.add(b"test1");
        hll.add(b"test2");
        hll.add(b"test3");
        let initial_count = hll.count();
        assert!(hll.is_sparse());

        let value = Value::HyperLogLog(hll);
        let metadata = KeyMetadata::new(100);

        let data = serialize(&value, &metadata);
        let (value2, _) = deserialize(&data).unwrap();

        let hll2 = value2.as_hyperloglog().unwrap();
        assert!(hll2.is_sparse());
        assert_eq!(hll2.count_no_cache(), initial_count);
    }

    #[test]
    fn test_serialize_deserialize_hyperloglog_dense() {
        let mut hll = HyperLogLogValue::new();
        // Add enough elements to promote to dense
        for i in 0..5000 {
            hll.add(format!("element:{}", i).as_bytes());
        }
        assert!(!hll.is_sparse());
        let initial_count = hll.count();

        let value = Value::HyperLogLog(hll);
        let metadata = KeyMetadata::new(12500);

        let data = serialize(&value, &metadata);
        let (value2, _) = deserialize(&data).unwrap();

        let hll2 = value2.as_hyperloglog().unwrap();
        assert!(!hll2.is_sparse());
        // Allow some tolerance due to floating point calculations
        let count2 = hll2.count_no_cache();
        assert!(
            count2 >= initial_count - 50 && count2 <= initial_count + 50,
            "Count {} not in expected range around {}",
            count2,
            initial_count
        );
    }

    #[test]
    fn test_serialize_deserialize_hyperloglog_empty() {
        let hll = HyperLogLogValue::new();
        let value = Value::HyperLogLog(hll);
        let metadata = KeyMetadata::new(0);

        let data = serialize(&value, &metadata);
        let (value2, _) = deserialize(&data).unwrap();

        let hll2 = value2.as_hyperloglog().unwrap();
        assert!(hll2.is_sparse());
        assert_eq!(hll2.count_no_cache(), 0);
    }
}
