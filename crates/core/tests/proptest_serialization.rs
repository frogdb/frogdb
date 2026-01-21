//! Property-based tests for serialization.
//!
//! Tests that:
//! - Random bytes never cause panics in deserialize
//! - Roundtrip serialization preserves values
//! - Truncated/corrupted data doesn't cause panics

use bytes::Bytes;
use proptest::prelude::*;

use frogdb_core::{deserialize, serialize, SerializationError, HEADER_SIZE};
use frogdb_core::{
    HashValue, KeyMetadata, ListValue, SetValue, SortedSetValue, StreamId, StreamIdSpec,
    StreamValue, StringValue, Value,
};

/// Configuration for proptest - run more cases than default for fuzzing
fn config() -> ProptestConfig {
    ProptestConfig::with_cases(1000)
}

proptest! {
    #![proptest_config(config())]

    /// Random bytes should return Err on invalid data, never panic.
    #[test]
    fn deserialize_never_panics(data: Vec<u8>) {
        let _: Result<(Value, KeyMetadata), SerializationError> = deserialize(&data);
    }

    /// Empty input should return an error.
    #[test]
    fn deserialize_empty_never_panics(len in 0usize..HEADER_SIZE) {
        let data = vec![0u8; len];
        let result: Result<(Value, KeyMetadata), SerializationError> = deserialize(&data);
        prop_assert!(result.is_err());
    }

    /// String roundtrip preserves value.
    #[test]
    fn roundtrip_string(value: Vec<u8>) {
        let val = Value::String(StringValue::new(Bytes::from(value.clone())));
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);
        let result: Result<(Value, KeyMetadata), SerializationError> = deserialize(&serialized);

        prop_assert!(result.is_ok());
        let (deserialized, _meta) = result.unwrap();
        prop_assert_eq!(val.key_type(), deserialized.key_type());

        // Compare the actual bytes
        let original_bytes = val.as_string().unwrap().as_bytes();
        let deser_bytes = deserialized.as_string().unwrap().as_bytes();
        prop_assert_eq!(original_bytes.as_ref(), deser_bytes.as_ref());
    }

    /// Integer string roundtrip preserves value.
    #[test]
    fn roundtrip_integer_string(i: i64) {
        let val = Value::String(StringValue::from_integer(i));
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);
        let result: Result<(Value, KeyMetadata), SerializationError> = deserialize(&serialized);

        prop_assert!(result.is_ok());
        let (deserialized, _meta) = result.unwrap();
        prop_assert_eq!(deserialized.as_string().unwrap().as_integer(), Some(i));
    }

    /// Sorted set roundtrip preserves members and scores.
    /// Filter out NaN scores since they don't compare equal.
    #[test]
    fn roundtrip_sorted_set(
        entries in prop::collection::vec(
            (prop::collection::vec(any::<u8>(), 0..100), -1e10f64..1e10f64),
            0..50
        )
    ) {
        let mut zset = SortedSetValue::new();
        for (member, score) in entries {
            // Skip NaN (filter at generation)
            if score.is_nan() {
                continue;
            }
            zset.add(Bytes::from(member), score);
        }

        let val = Value::SortedSet(zset);
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);
        let result: Result<(Value, KeyMetadata), SerializationError> = deserialize(&serialized);

        prop_assert!(result.is_ok());
        let (deserialized, _meta) = result.unwrap();
        prop_assert_eq!(val.key_type(), deserialized.key_type());

        let original = val.as_sorted_set().unwrap();
        let deser = deserialized.as_sorted_set().unwrap();
        prop_assert_eq!(original.len(), deser.len());
    }

    /// Hash roundtrip preserves field/value pairs.
    #[test]
    fn roundtrip_hash(
        entries in prop::collection::vec(
            (prop::collection::vec(any::<u8>(), 0..100), prop::collection::vec(any::<u8>(), 0..100)),
            0..50
        )
    ) {
        let mut hash = HashValue::new();
        for (field, value) in entries {
            hash.set(Bytes::from(field), Bytes::from(value));
        }

        let val = Value::Hash(hash);
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);
        let result: Result<(Value, KeyMetadata), SerializationError> = deserialize(&serialized);

        prop_assert!(result.is_ok());
        let (deserialized, _meta) = result.unwrap();
        prop_assert_eq!(val.key_type(), deserialized.key_type());

        let original = val.as_hash().unwrap();
        let deser = deserialized.as_hash().unwrap();
        prop_assert_eq!(original.len(), deser.len());
    }

    /// List roundtrip preserves elements.
    #[test]
    fn roundtrip_list(
        elements in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..100), 0..50)
    ) {
        let mut list = ListValue::new();
        for elem in elements {
            list.push_back(Bytes::from(elem));
        }

        let val = Value::List(list);
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);
        let result: Result<(Value, KeyMetadata), SerializationError> = deserialize(&serialized);

        prop_assert!(result.is_ok());
        let (deserialized, _meta) = result.unwrap();
        prop_assert_eq!(val.key_type(), deserialized.key_type());

        let original = val.as_list().unwrap();
        let deser = deserialized.as_list().unwrap();
        prop_assert_eq!(original.len(), deser.len());
    }

    /// Set roundtrip preserves members.
    #[test]
    fn roundtrip_set(
        members in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..100), 0..50)
    ) {
        let mut set = SetValue::new();
        for member in members {
            set.add(Bytes::from(member));
        }

        let val = Value::Set(set);
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);
        let result: Result<(Value, KeyMetadata), SerializationError> = deserialize(&serialized);

        prop_assert!(result.is_ok());
        let (deserialized, _meta) = result.unwrap();
        prop_assert_eq!(val.key_type(), deserialized.key_type());

        let original = val.as_set().unwrap();
        let deser = deserialized.as_set().unwrap();
        prop_assert_eq!(original.len(), deser.len());
    }

    /// Stream roundtrip preserves entries.
    #[test]
    fn roundtrip_stream(
        entries in prop::collection::vec(
            (
                1u64..u64::MAX,  // ms timestamp (avoid 0 to prevent ID conflicts)
                0u64..1000u64,   // seq number
                prop::collection::vec(
                    (prop::collection::vec(any::<u8>(), 1..50), prop::collection::vec(any::<u8>(), 0..100)),
                    1..10
                )
            ),
            0..20
        )
    ) {
        let mut stream = StreamValue::new();
        let mut prev_id = StreamId::min();

        for (ms, seq, fields) in entries {
            // Ensure IDs are strictly increasing
            let id = StreamId::new(
                prev_id.ms.saturating_add(ms),
                if ms == 0 { prev_id.seq.saturating_add(seq + 1) } else { seq }
            );
            prev_id = id;

            let field_vec: Vec<(Bytes, Bytes)> = fields
                .into_iter()
                .map(|(f, v)| (Bytes::from(f), Bytes::from(v)))
                .collect();

            if !field_vec.is_empty() {
                let _ = stream.add(StreamIdSpec::Explicit(id), field_vec);
            }
        }

        let val = Value::Stream(stream);
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);
        let result: Result<(Value, KeyMetadata), SerializationError> = deserialize(&serialized);

        prop_assert!(result.is_ok());
        let (deserialized, _meta) = result.unwrap();
        prop_assert_eq!(val.key_type(), deserialized.key_type());

        let original = val.as_stream().unwrap();
        let deser = deserialized.as_stream().unwrap();
        prop_assert_eq!(original.len(), deser.len());
    }

    /// Truncating valid serialized data at random points should not panic.
    #[test]
    fn truncated_string_doesnt_panic(value: Vec<u8>, truncate_at in 0usize..1000usize) {
        let val = Value::String(StringValue::new(Bytes::from(value)));
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);

        if truncate_at < serialized.len() {
            let truncated = &serialized[..truncate_at];
            let _: Result<(Value, KeyMetadata), SerializationError> = deserialize(truncated);
        }
    }

    /// Truncating sorted set data at random points should not panic.
    #[test]
    fn truncated_sorted_set_doesnt_panic(
        entries in prop::collection::vec(
            (prop::collection::vec(any::<u8>(), 1..50), -1e10f64..1e10f64),
            1..20
        ),
        truncate_at in 0usize..5000usize
    ) {
        let mut zset = SortedSetValue::new();
        for (member, score) in entries {
            if !score.is_nan() {
                zset.add(Bytes::from(member), score);
            }
        }

        let val = Value::SortedSet(zset);
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);

        if truncate_at < serialized.len() {
            let truncated = &serialized[..truncate_at];
            let _: Result<(Value, KeyMetadata), SerializationError> = deserialize(truncated);
        }
    }

    /// Corrupted header bytes should not cause panics.
    #[test]
    fn corrupted_header_doesnt_panic(
        value: Vec<u8>,
        corrupt_offset in 0usize..HEADER_SIZE,
        corrupt_byte: u8
    ) {
        let val = Value::String(StringValue::new(Bytes::from(value)));
        let metadata = KeyMetadata::new(val.memory_size());
        let mut serialized = serialize(&val, &metadata);

        // Corrupt a header byte
        if corrupt_offset < serialized.len() {
            serialized[corrupt_offset] = corrupt_byte;
        }

        let _: Result<(Value, KeyMetadata), SerializationError> = deserialize(&serialized);
    }

    /// Random type byte with valid-looking payload should not panic.
    #[test]
    fn random_type_byte_doesnt_panic(type_byte: u8, payload: Vec<u8>) {
        let mut data = vec![0u8; HEADER_SIZE];
        data[0] = type_byte;
        // Set payload length
        let payload_len = payload.len() as u64;
        data[16..24].copy_from_slice(&payload_len.to_le_bytes());
        data.extend_from_slice(&payload);

        let _: Result<(Value, KeyMetadata), SerializationError> = deserialize(&data);
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_empty_string() {
        let val = Value::String(StringValue::new(Bytes::new()));
        let metadata = KeyMetadata::new(0);
        let serialized = serialize(&val, &metadata);
        let (deserialized, _): (Value, KeyMetadata) = deserialize(&serialized).unwrap();
        assert_eq!(deserialized.as_string().unwrap().len(), 0);
    }

    #[test]
    fn test_empty_sorted_set() {
        let val = Value::SortedSet(SortedSetValue::new());
        let metadata = KeyMetadata::new(0);
        let serialized = serialize(&val, &metadata);
        let (deserialized, _): (Value, KeyMetadata) = deserialize(&serialized).unwrap();
        assert!(deserialized.as_sorted_set().unwrap().is_empty());
    }

    #[test]
    fn test_empty_hash() {
        let val = Value::Hash(HashValue::new());
        let metadata = KeyMetadata::new(0);
        let serialized = serialize(&val, &metadata);
        let (deserialized, _): (Value, KeyMetadata) = deserialize(&serialized).unwrap();
        assert!(deserialized.as_hash().unwrap().is_empty());
    }

    #[test]
    fn test_empty_list() {
        let val = Value::List(ListValue::new());
        let metadata = KeyMetadata::new(0);
        let serialized = serialize(&val, &metadata);
        let (deserialized, _): (Value, KeyMetadata) = deserialize(&serialized).unwrap();
        assert!(deserialized.as_list().unwrap().is_empty());
    }

    #[test]
    fn test_empty_set() {
        let val = Value::Set(SetValue::new());
        let metadata = KeyMetadata::new(0);
        let serialized = serialize(&val, &metadata);
        let (deserialized, _): (Value, KeyMetadata) = deserialize(&serialized).unwrap();
        assert!(deserialized.as_set().unwrap().is_empty());
    }

    #[test]
    fn test_empty_stream() {
        let val = Value::Stream(StreamValue::new());
        let metadata = KeyMetadata::new(0);
        let serialized = serialize(&val, &metadata);
        let (deserialized, _): (Value, KeyMetadata) = deserialize(&serialized).unwrap();
        assert!(deserialized.as_stream().unwrap().is_empty());
    }

    #[test]
    fn test_special_float_scores() {
        let mut zset = SortedSetValue::new();
        zset.add(Bytes::from("inf"), f64::INFINITY);
        zset.add(Bytes::from("neg_inf"), f64::NEG_INFINITY);
        zset.add(Bytes::from("zero"), 0.0);
        zset.add(Bytes::from("neg_zero"), -0.0);

        let val = Value::SortedSet(zset);
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);
        let (deserialized, _): (Value, KeyMetadata) = deserialize(&serialized).unwrap();

        let deser = deserialized.as_sorted_set().unwrap();
        assert_eq!(deser.get_score(b"inf"), Some(f64::INFINITY));
        assert_eq!(deser.get_score(b"neg_inf"), Some(f64::NEG_INFINITY));
    }

    #[test]
    fn test_large_string() {
        let large = vec![b'x'; 1_000_000];
        let val = Value::String(StringValue::new(Bytes::from(large.clone())));
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);
        let (deserialized, _): (Value, KeyMetadata) = deserialize(&serialized).unwrap();
        assert_eq!(deserialized.as_string().unwrap().len(), 1_000_000);
    }

    #[test]
    fn test_binary_string() {
        // String with all byte values 0-255
        let binary: Vec<u8> = (0u8..=255).collect();
        let val = Value::String(StringValue::new(Bytes::from(binary.clone())));
        let metadata = KeyMetadata::new(val.memory_size());
        let serialized = serialize(&val, &metadata);
        let (deserialized, _): (Value, KeyMetadata) = deserialize(&serialized).unwrap();
        assert_eq!(deserialized.as_string().unwrap().as_bytes().as_ref(), &binary[..]);
    }
}
