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

mod collections;
mod probabilistic;
mod search;
mod stream;
mod string;
mod timeseries;

use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

use frogdb_types::types::{KeyMetadata, StringValue, Value};

use collections::{
    deserialize_hash, deserialize_hash_with_field_expiry, deserialize_list, deserialize_set,
    deserialize_sorted_set, serialize_hash, serialize_hash_with_field_expiry, serialize_list,
    serialize_set, serialize_sorted_set,
};
use probabilistic::{
    deserialize_bloom_filter, deserialize_cms, deserialize_cuckoo_filter, deserialize_hyperloglog,
    deserialize_tdigest, deserialize_topk, serialize_bloom_filter, serialize_cms,
    serialize_cuckoo_filter, serialize_hyperloglog, serialize_tdigest, serialize_topk,
};
use search::{deserialize_json, deserialize_vectorset, serialize_json, serialize_vectorset};
use stream::{deserialize_stream, serialize_stream};
use string::serialize_string;
use timeseries::{deserialize_timeseries, serialize_timeseries};

/// Size of the serialization header in bytes.
pub const HEADER_SIZE: usize = 24;

/// Cap pre-allocation to a plausible maximum based on available bytes.
/// Prevents OOM from malicious length prefixes in untrusted input.
pub(crate) fn safe_capacity(count: usize, min_element_bytes: usize, available_bytes: usize) -> usize {
    if min_element_bytes == 0 {
        return count;
    }
    count.min(available_bytes / min_element_bytes)
}

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
/// Marker for hash with per-field expiry.
const TYPE_HASH_WITH_FIELD_EXPIRY: u8 = 11;
/// Marker for cuckoo filter type.
const TYPE_CUCKOO: u8 = 12;
/// Marker for Top-K type.
const TYPE_TOPK: u8 = 13;
/// Marker for t-digest type.
const TYPE_TDIGEST: u8 = 14;
/// Marker for Count-Min Sketch type.
const TYPE_CMS: u8 = 15;
/// Marker for vector set type.
const TYPE_VECTORSET: u8 = 16;

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
        Value::Hash(hash) => {
            if hash.has_field_expiries() {
                serialize_hash_with_field_expiry(hash)
            } else {
                serialize_hash(hash)
            }
        }
        Value::List(list) => serialize_list(list),
        Value::Set(set) => serialize_set(set),
        Value::Stream(stream) => serialize_stream(stream),
        Value::BloomFilter(bf) => serialize_bloom_filter(bf),
        Value::HyperLogLog(hll) => serialize_hyperloglog(hll),
        Value::TimeSeries(ts) => serialize_timeseries(ts),
        Value::Json(json) => serialize_json(json),
        Value::CuckooFilter(cf) => serialize_cuckoo_filter(cf),
        Value::TopK(tk) => serialize_topk(tk),
        Value::TDigest(td) => serialize_tdigest(td),
        Value::CountMinSketch(cms) => serialize_cms(cms),
        Value::VectorSet(vs) => serialize_vectorset(vs),
    }
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
        TYPE_HASH_WITH_FIELD_EXPIRY => {
            let hash = deserialize_hash_with_field_expiry(payload)?;
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
        TYPE_CUCKOO => {
            let cf = deserialize_cuckoo_filter(payload)?;
            Ok(Value::CuckooFilter(cf))
        }
        TYPE_TOPK => {
            let tk = deserialize_topk(payload)?;
            Ok(Value::TopK(tk))
        }
        TYPE_TDIGEST => {
            let td = deserialize_tdigest(payload)?;
            Ok(Value::TDigest(td))
        }
        TYPE_CMS => {
            let cms = deserialize_cms(payload)?;
            Ok(Value::CountMinSketch(cms))
        }
        TYPE_VECTORSET => {
            let vs = deserialize_vectorset(payload)?;
            Ok(Value::VectorSet(Box::new(vs)))
        }
        _ => Err(SerializationError::UnknownType(type_byte)),
    }
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

    use frogdb_types::hyperloglog::HyperLogLogValue;
    use frogdb_types::types::SortedSetValue;

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
    fn test_deserialize_huge_length_prefix_no_oom() {
        // Hash payload with huge element count but only 8 bytes of actual data
        let mut data = vec![0u8; HEADER_SIZE + 8];
        data[0] = 3; // TYPE_HASH
        data[16..24].copy_from_slice(&8u64.to_le_bytes()); // payload_len = 8
        // Write huge element count in payload
        data[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&u32::MAX.to_le_bytes());

        let result = deserialize(&data);
        // Should fail with truncation error, not OOM
        assert!(result.is_err());
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
