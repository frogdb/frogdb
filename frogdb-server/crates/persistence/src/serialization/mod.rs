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
mod frame;
mod marker;
mod probabilistic;
mod registry;
mod search;
mod stream;
mod string;
mod timeseries;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

use frogdb_types::types::{KeyMetadata, Value};

pub(crate) use frame::FrameReader;
pub(crate) use marker::TypeMarker;

pub use probabilistic::{merge_hll_serialized, partial_merge_hll_deltas, serialize_hll_delta};

/// Size of the serialization header in bytes.
pub const HEADER_SIZE: usize = 24;

/// Cap pre-allocation to a plausible maximum based on available bytes.
/// Prevents OOM from malicious length prefixes in untrusted input.
pub(crate) fn safe_capacity(
    count: usize,
    min_element_bytes: usize,
    available_bytes: usize,
) -> usize {
    if min_element_bytes == 0 {
        return count;
    }
    count.min(available_bytes / min_element_bytes)
}

/// The persisted type markers are now a closed enum, [`TypeMarker`], owned in
/// `marker.rs`. The codec for each marker lives in `registry.rs`.
///
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
    let (marker, payload) = serialize_value(value);
    build_frame(marker, metadata, &payload)
}

/// Wrap a raw payload in the 24-byte header for `marker`, deriving expiry and LFU
/// from `metadata` exactly as [`serialize`] does. Shared by [`serialize`] and the
/// HLL delta-operand codec so every persisted frame writes an identical header.
pub(crate) fn build_frame(marker: TypeMarker, metadata: &KeyMetadata, payload: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(HEADER_SIZE + payload.len());

    // Type (1 byte)
    result.push(marker.as_byte());

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
    result.extend_from_slice(payload);

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

/// Serialize a value to its type marker and payload by scanning the codec
/// [`registry`]. Each codec claims only the value it owns, so the scan is
/// order-independent; the first match wins.
fn serialize_value(value: &Value) -> (TypeMarker, Vec<u8>) {
    for codec in registry::REGISTRY {
        if let Some(payload) = (codec.encode)(value) {
            return (codec.marker, payload);
        }
    }
    // Every `Value` variant is claimed by a codec; the registry-coverage and
    // round-trip tests prove it. Reaching here means a variant was added without a
    // codec.
    unreachable!(
        "serialization registry has no codec for value of type {:?}",
        value.key_type()
    )
}

/// Deserialize a value from its type byte and payload.
///
/// Unknown *bytes* error in [`TypeMarker::from_byte`]; the marker is then decoded
/// via [`registry::decode_for`], whose match over the closed [`TypeMarker`] enum
/// has **no wildcard**, so a marker without a decode arm is a compile error rather
/// than a silent `UnknownType` at load time.
fn deserialize_value(type_byte: u8, payload: &[u8]) -> Result<Value, SerializationError> {
    let marker = TypeMarker::from_byte(type_byte)?;
    (registry::decode_for(marker))(payload)
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

    use bytes::Bytes;
    use frogdb_types::hyperloglog::HyperLogLogValue;
    use frogdb_types::types::{SortedSetValue, StringValue};

    /// Round-trip one representative value for every `Value` variant through the
    /// public `serialize`/`deserialize` pair — the exact codec that cross-shard
    /// COPY now routes through (replacing the old hand-rolled `serialize_for_copy`
    /// pair). A break here is a COPY *and* a persistence regression. Deep per-marker
    /// coverage lives in `registry::tests::every_marker_round_trips`; this pins the
    /// COPY entry points and the collection payloads reframed onto `FrameReader`.
    #[test]
    fn copy_codec_round_trips_all_value_variants() {
        use frogdb_types::bloom::BloomFilterValue;
        use frogdb_types::cms::CountMinSketchValue;
        use frogdb_types::cuckoo::CuckooFilterValue;
        use frogdb_types::json::JsonValue;
        use frogdb_types::tdigest::TDigestValue;
        use frogdb_types::timeseries::TimeSeriesValue;
        use frogdb_types::topk::TopKValue;
        use frogdb_types::types::{
            HashValue, ListValue, ListpackThresholds, SetValue, StreamId, StreamIdSpec, StreamValue,
        };
        use frogdb_types::vectorset::{VectorDistanceMetric, VectorQuantization, VectorSetValue};

        let values: Vec<Value> = vec![
            Value::String(StringValue::new(Bytes::from_static(b"hello"))),
            {
                let mut z = SortedSetValue::new();
                z.add(Bytes::from_static(b"m"), 1.5);
                Value::SortedSet(z)
            },
            {
                let mut h = HashValue::new();
                h.set(
                    Bytes::from_static(b"f"),
                    Bytes::from_static(b"v"),
                    ListpackThresholds::DEFAULT_HASH,
                );
                Value::Hash(h)
            },
            {
                let mut l = ListValue::new();
                l.push_back(Bytes::from_static(b"a"));
                l.push_back(Bytes::from_static(b"b"));
                Value::List(l)
            },
            {
                let mut s = SetValue::new();
                s.add(Bytes::from_static(b"m"), ListpackThresholds::DEFAULT_SET);
                Value::Set(s)
            },
            {
                let mut st = StreamValue::new();
                let _ = st.add(
                    StreamIdSpec::Explicit(StreamId::new(1, 0)),
                    vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
                );
                Value::Stream(st)
            },
            {
                let mut bf = BloomFilterValue::new(100, 0.01);
                bf.add(b"a");
                Value::BloomFilter(bf)
            },
            {
                let mut hll = HyperLogLogValue::new();
                hll.add(b"a");
                Value::HyperLogLog(hll)
            },
            {
                let mut ts = TimeSeriesValue::new();
                let _ = ts.add(1000, 1.5);
                Value::TimeSeries(ts)
            },
            Value::Json(JsonValue::parse(br#"{"a":1}"#).unwrap()),
            {
                let mut cf = CuckooFilterValue::new(64);
                let _ = cf.add(b"a");
                Value::CuckooFilter(cf)
            },
            {
                let mut tk = TopKValue::new(3, 8, 7, 0.9);
                tk.add(b"a", 1);
                Value::TopK(tk)
            },
            {
                let mut td = TDigestValue::new(100.0);
                td.add(1.0);
                Value::TDigest(td)
            },
            {
                let mut cms = CountMinSketchValue::new(16, 4);
                cms.increment(b"a", 1);
                Value::CountMinSketch(cms)
            },
            {
                let mut vs = VectorSetValue::new(
                    VectorDistanceMetric::Cosine,
                    VectorQuantization::NoQuant,
                    4,
                    16,
                    200,
                )
                .unwrap();
                vs.add(Bytes::from_static(b"e"), vec![1.0, 0.0, 0.0, 0.0])
                    .unwrap();
                Value::VectorSet(Box::new(vs))
            },
        ];

        assert_eq!(values.len(), 15, "expected one sample per Value variant");

        for value in values {
            let bytes = serialize(&value, &KeyMetadata::new(value.memory_size()));
            let (back, _) = deserialize(&bytes)
                .unwrap_or_else(|e| panic!("{:?} failed to deserialize: {e}", value.key_type()));
            assert_eq!(
                value.key_type(),
                back.key_type(),
                "key_type changed through round-trip for {:?}",
                value.key_type()
            );
        }
    }

    /// The collection payloads reframed onto `FrameReader` must preserve their
    /// contents byte-for-byte, not merely their type — this is what COPY relies on.
    #[test]
    fn collection_contents_survive_round_trip() {
        use frogdb_types::types::{HashValue, ListValue, ListpackThresholds, SetValue};

        // Sorted set: scores and members.
        let mut z = SortedSetValue::new();
        z.add(Bytes::from_static(b"one"), 1.0);
        z.add(Bytes::from_static(b"two"), 2.0);
        let value = Value::SortedSet(z);
        let (back, _) = deserialize(&serialize(&value, &KeyMetadata::new(0))).unwrap();
        let z2 = back.as_sorted_set().unwrap();
        assert_eq!(z2.len(), 2);
        assert_eq!(z2.get_score(b"one"), Some(1.0));
        assert_eq!(z2.get_score(b"two"), Some(2.0));

        // Hash: field/value pairs.
        let mut h = HashValue::new();
        h.set(
            Bytes::from_static(b"f1"),
            Bytes::from_static(b"v1"),
            ListpackThresholds::DEFAULT_HASH,
        );
        let value = Value::Hash(h);
        let (back, _) = deserialize(&serialize(&value, &KeyMetadata::new(0))).unwrap();
        let h2 = back.as_hash().unwrap();
        assert_eq!(h2.get(b"f1").as_deref(), Some(&b"v1"[..]));

        // List: ordered elements.
        let mut l = ListValue::new();
        l.push_back(Bytes::from_static(b"a"));
        l.push_back(Bytes::from_static(b"b"));
        let value = Value::List(l);
        let (back, _) = deserialize(&serialize(&value, &KeyMetadata::new(0))).unwrap();
        let l2 = back.as_list().unwrap();
        assert_eq!(l2.len(), 2);

        // Set: membership.
        let mut s = SetValue::new();
        s.add(Bytes::from_static(b"m1"), ListpackThresholds::DEFAULT_SET);
        s.add(Bytes::from_static(b"m2"), ListpackThresholds::DEFAULT_SET);
        let value = Value::Set(s);
        let (back, _) = deserialize(&serialize(&value, &KeyMetadata::new(0))).unwrap();
        let s2 = back.as_set().unwrap();
        assert_eq!(s2.len(), 2);
        assert!(s2.contains(b"m1"));
        assert!(s2.contains(b"m2"));
    }

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
        assert_eq!(data[0], TypeMarker::StringInt.as_byte());

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
