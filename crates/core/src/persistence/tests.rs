//! Integration tests for the persistence module.

#[cfg(test)]
mod integration {
    use crate::bloom::BloomFilterValue;
    use crate::noop::NoopMetricsRecorder;
    use crate::persistence::*;
    use crate::store::Store;
    use crate::types::{
        HashValue, KeyMetadata, ListValue, SetValue, SortedSetValue, StreamId, StreamIdSpec,
        StreamValue, Value,
    };
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    #[test]
    fn test_roundtrip_persistence() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(RocksStore::open(tmp.path(), 4, &RocksConfig::default()).unwrap());

        // Write various types of data
        let test_data = [
            (b"string_raw".to_vec(), Value::string("hello world")),
            (
                b"string_int".to_vec(),
                Value::String(crate::types::StringValue::from_integer(12345)),
            ),
            (
                b"string_neg".to_vec(),
                Value::String(crate::types::StringValue::from_integer(-999)),
            ),
        ];

        // Add a sorted set
        let mut zset = SortedSetValue::new();
        zset.add(Bytes::from("alice"), 100.0);
        zset.add(Bytes::from("bob"), 50.5);
        zset.add(Bytes::from("charlie"), 75.25);

        let zset_value = Value::SortedSet(zset);

        // Write to different shards
        for (i, (key, value)) in test_data.iter().enumerate() {
            let shard_id = i % 4;
            let metadata = KeyMetadata::new(value.memory_size());
            let serialized = serialize(value, &metadata);
            rocks.put(shard_id, key, &serialized).unwrap();
        }

        // Write sorted set
        let metadata = KeyMetadata::new(zset_value.memory_size());
        rocks
            .put(0, b"myzset", &serialize(&zset_value, &metadata))
            .unwrap();

        // Recover and verify
        for (i, (key, expected)) in test_data.iter().enumerate() {
            let shard_id = i % 4;
            let data = rocks.get(shard_id, key).unwrap().unwrap();
            let (value, _) = deserialize(&data).unwrap();

            match (expected, &value) {
                (Value::String(exp), Value::String(got)) => {
                    assert_eq!(exp.as_bytes(), got.as_bytes());
                }
                _ => panic!("Type mismatch"),
            }
        }

        // Verify sorted set
        let data = rocks.get(0, b"myzset").unwrap().unwrap();
        let (value, _) = deserialize(&data).unwrap();
        let zset = value.as_sorted_set().unwrap();
        assert_eq!(zset.len(), 3);
        assert_eq!(zset.get_score(b"alice"), Some(100.0));
        assert_eq!(zset.get_score(b"bob"), Some(50.5));
        assert_eq!(zset.get_score(b"charlie"), Some(75.25));
    }

    #[test]
    fn test_expiry_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

        let value = Value::string("expiring");
        let mut metadata = KeyMetadata::new(8);
        metadata.expires_at = Some(Instant::now() + Duration::from_secs(3600));

        let serialized = serialize(&value, &metadata);
        rocks.put(0, b"exp_key", &serialized).unwrap();

        // Recover
        let data = rocks.get(0, b"exp_key").unwrap().unwrap();
        let (_, recovered_meta) = deserialize(&data).unwrap();

        // Expiry should be preserved (within reasonable tolerance)
        assert!(recovered_meta.expires_at.is_some());
        let exp = recovered_meta.expires_at.unwrap();
        let now = Instant::now();
        let remaining = exp.duration_since(now);
        assert!(remaining.as_secs() > 3500 && remaining.as_secs() < 3700);
    }

    #[test]
    fn test_lfu_counter_preserved() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let value = Value::string("test");
        let mut metadata = KeyMetadata::new(4);
        metadata.lfu_counter = 42;

        let serialized = serialize(&value, &metadata);
        rocks.put(0, b"lfu_key", &serialized).unwrap();

        let data = rocks.get(0, b"lfu_key").unwrap().unwrap();
        let (_, recovered_meta) = deserialize(&data).unwrap();

        assert_eq!(recovered_meta.lfu_counter, 42);
    }

    #[tokio::test]
    async fn test_wal_writer_persistence() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
        let metrics = Arc::new(NoopMetricsRecorder::new());

        let wal = RocksWalWriter::new(
            rocks.clone(),
            0,
            WalConfig {
                mode: DurabilityMode::Async,
                batch_size_threshold: 1024 * 1024,
                batch_timeout_ms: 1000,
            },
            metrics,
        );

        // Write through WAL
        let value = Value::string("wal_test");
        let metadata = KeyMetadata::new(8);
        wal.write_set(b"wal_key", &value, &metadata).await.unwrap();
        wal.flush_async().await.unwrap();

        // Verify via direct read
        let data = rocks.get(0, b"wal_key").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();
        assert_eq!(
            recovered.as_string().unwrap().as_bytes().as_ref(),
            b"wal_test"
        );
    }

    #[test]
    fn test_full_recovery_cycle() {
        let tmp = TempDir::new().unwrap();

        // Phase 1: Write data
        {
            let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());

            for i in 0..100 {
                let key = format!("key_{}", i);
                let value = Value::string(format!("value_{}", i));
                let metadata = KeyMetadata::new(value.memory_size());
                let shard_id = i % 2;

                rocks
                    .put(shard_id, key.as_bytes(), &serialize(&value, &metadata))
                    .unwrap();
            }

            rocks.flush().unwrap();
        }

        // Phase 2: Reopen and recover
        {
            let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
            let (stores, stats) = recover_all_shards(&rocks).unwrap();

            assert_eq!(stores.len(), 2);
            assert_eq!(stats.keys_loaded, 100);

            // Verify some keys
            for i in 0..100 {
                let key = format!("key_{}", i);
                let shard_id = i % 2;
                let value = stores[shard_id].0.get(key.as_bytes());
                assert!(
                    value.is_some(),
                    "Key {} not found in shard {}",
                    key,
                    shard_id
                );

                let expected = format!("value_{}", i);
                assert_eq!(
                    value.unwrap().as_string().unwrap().as_bytes().as_ref(),
                    expected.as_bytes()
                );
            }
        }
    }

    #[test]
    fn test_large_sorted_set_persistence() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        // Create a large sorted set
        let mut zset = SortedSetValue::new();
        for i in 0..1000 {
            let member = format!("member_{:05}", i);
            zset.add(Bytes::from(member), i as f64);
        }

        let value = Value::SortedSet(zset);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"large_zset", &serialize(&value, &metadata))
            .unwrap();

        // Recover
        let data = rocks.get(0, b"large_zset").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let zset = recovered.as_sorted_set().unwrap();
        assert_eq!(zset.len(), 1000);

        // Spot check some values
        assert_eq!(zset.get_score(b"member_00000"), Some(0.0));
        assert_eq!(zset.get_score(b"member_00500"), Some(500.0));
        assert_eq!(zset.get_score(b"member_00999"), Some(999.0));
    }

    #[test]
    fn test_binary_data_persistence() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        // Test with binary data containing null bytes
        let binary_data: Vec<u8> = (0..=255).collect();
        let value = Value::String(crate::types::StringValue::new(Bytes::from(
            binary_data.clone(),
        )));
        let metadata = KeyMetadata::new(256);

        rocks
            .put(0, b"binary_key", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"binary_key").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        assert_eq!(
            recovered.as_string().unwrap().as_bytes().as_ref(),
            binary_data.as_slice()
        );
    }

    #[test]
    fn test_special_float_scores() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut zset = SortedSetValue::new();
        zset.add(Bytes::from("neg_inf"), f64::NEG_INFINITY);
        zset.add(Bytes::from("pos_inf"), f64::INFINITY);
        zset.add(Bytes::from("zero"), 0.0);
        zset.add(Bytes::from("neg_zero"), -0.0);
        zset.add(Bytes::from("small"), 1e-300);
        zset.add(Bytes::from("large"), 1e300);

        let value = Value::SortedSet(zset);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"special_zset", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"special_zset").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let zset = recovered.as_sorted_set().unwrap();
        assert_eq!(zset.len(), 6);
        assert_eq!(zset.get_score(b"neg_inf"), Some(f64::NEG_INFINITY));
        assert_eq!(zset.get_score(b"pos_inf"), Some(f64::INFINITY));
        assert_eq!(zset.get_score(b"zero"), Some(0.0));
        assert_eq!(zset.get_score(b"small"), Some(1e-300));
        assert_eq!(zset.get_score(b"large"), Some(1e300));
    }

    // ========================================================================
    // Hash type persistence tests
    // ========================================================================

    #[test]
    fn test_hash_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut hash = HashValue::new();
        hash.set(Bytes::from("field1"), Bytes::from("value1"));
        hash.set(Bytes::from("field2"), Bytes::from("value2"));
        hash.set(Bytes::from("field3"), Bytes::from("value3"));

        let value = Value::Hash(hash);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"myhash", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"myhash").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let hash = recovered.as_hash().unwrap();
        assert_eq!(hash.len(), 3);
        assert_eq!(hash.get(b"field1"), Some(&Bytes::from("value1")));
        assert_eq!(hash.get(b"field2"), Some(&Bytes::from("value2")));
        assert_eq!(hash.get(b"field3"), Some(&Bytes::from("value3")));
    }

    #[test]
    fn test_hash_empty() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let hash = HashValue::new();
        let value = Value::Hash(hash);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"empty_hash", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"empty_hash").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let hash = recovered.as_hash().unwrap();
        assert_eq!(hash.len(), 0);
        assert!(hash.is_empty());
    }

    #[test]
    fn test_hash_with_binary_data() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut hash = HashValue::new();
        // Field with binary data including null bytes
        let binary_field: Vec<u8> = (0..=255).collect();
        let binary_value: Vec<u8> = (0..=255).rev().collect();
        hash.set(
            Bytes::from(binary_field.clone()),
            Bytes::from(binary_value.clone()),
        );

        let value = Value::Hash(hash);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"binary_hash", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"binary_hash").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let hash = recovered.as_hash().unwrap();
        assert_eq!(
            hash.get(&binary_field[..]),
            Some(&Bytes::from(binary_value))
        );
    }

    // ========================================================================
    // List type persistence tests
    // ========================================================================

    #[test]
    fn test_list_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut list = ListValue::new();
        list.push_back(Bytes::from("first"));
        list.push_back(Bytes::from("second"));
        list.push_back(Bytes::from("third"));

        let value = Value::List(list);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"mylist", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"mylist").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let list = recovered.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list.get(0), Some(&Bytes::from("first")));
        assert_eq!(list.get(1), Some(&Bytes::from("second")));
        assert_eq!(list.get(2), Some(&Bytes::from("third")));
    }

    #[test]
    fn test_list_empty() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let list = ListValue::new();
        let value = Value::List(list);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"empty_list", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"empty_list").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let list = recovered.as_list().unwrap();
        assert_eq!(list.len(), 0);
        assert!(list.is_empty());
    }

    #[test]
    fn test_large_list_persistence() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut list = ListValue::new();
        for i in 0..1000 {
            list.push_back(Bytes::from(format!("item_{:05}", i)));
        }

        let value = Value::List(list);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"large_list", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"large_list").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let list = recovered.as_list().unwrap();
        assert_eq!(list.len(), 1000);
        assert_eq!(list.get(0), Some(&Bytes::from("item_00000")));
        assert_eq!(list.get(500), Some(&Bytes::from("item_00500")));
        assert_eq!(list.get(999), Some(&Bytes::from("item_00999")));
    }

    // ========================================================================
    // Set type persistence tests
    // ========================================================================

    #[test]
    fn test_set_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut set = SetValue::new();
        set.add(Bytes::from("member1"));
        set.add(Bytes::from("member2"));
        set.add(Bytes::from("member3"));

        let value = Value::Set(set);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"myset", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"myset").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let set = recovered.as_set().unwrap();
        assert_eq!(set.len(), 3);
        assert!(set.contains(b"member1"));
        assert!(set.contains(b"member2"));
        assert!(set.contains(b"member3"));
    }

    #[test]
    fn test_set_empty() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let set = SetValue::new();
        let value = Value::Set(set);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"empty_set", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"empty_set").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let set = recovered.as_set().unwrap();
        assert_eq!(set.len(), 0);
        assert!(set.is_empty());
    }

    #[test]
    fn test_large_set_persistence() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut set = SetValue::new();
        for i in 0..1000 {
            set.add(Bytes::from(format!("member_{:05}", i)));
        }

        let value = Value::Set(set);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"large_set", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"large_set").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let set = recovered.as_set().unwrap();
        assert_eq!(set.len(), 1000);
        assert!(set.contains(b"member_00000"));
        assert!(set.contains(b"member_00500"));
        assert!(set.contains(b"member_00999"));
    }

    // ========================================================================
    // Mixed type recovery tests
    // ========================================================================

    #[test]
    fn test_mixed_types_recovery() {
        let tmp = TempDir::new().unwrap();

        // Phase 1: Write different types
        {
            let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());

            // String
            let str_value = Value::string("hello");
            rocks
                .put(0, b"str_key", &serialize(&str_value, &KeyMetadata::new(5)))
                .unwrap();

            // Hash
            let mut hash = HashValue::new();
            hash.set(Bytes::from("f1"), Bytes::from("v1"));
            let hash_value = Value::Hash(hash);
            rocks
                .put(
                    0,
                    b"hash_key",
                    &serialize(&hash_value, &KeyMetadata::new(hash_value.memory_size())),
                )
                .unwrap();

            // List
            let mut list = ListValue::new();
            list.push_back(Bytes::from("a"));
            list.push_back(Bytes::from("b"));
            let list_value = Value::List(list);
            rocks
                .put(
                    1,
                    b"list_key",
                    &serialize(&list_value, &KeyMetadata::new(list_value.memory_size())),
                )
                .unwrap();

            // Set
            let mut set = SetValue::new();
            set.add(Bytes::from("x"));
            set.add(Bytes::from("y"));
            let set_value = Value::Set(set);
            rocks
                .put(
                    1,
                    b"set_key",
                    &serialize(&set_value, &KeyMetadata::new(set_value.memory_size())),
                )
                .unwrap();

            // Sorted set
            let mut zset = SortedSetValue::new();
            zset.add(Bytes::from("alice"), 100.0);
            let zset_value = Value::SortedSet(zset);
            rocks
                .put(
                    0,
                    b"zset_key",
                    &serialize(&zset_value, &KeyMetadata::new(zset_value.memory_size())),
                )
                .unwrap();

            rocks.flush().unwrap();
        }

        // Phase 2: Recover and verify all types
        {
            let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
            let (stores, stats) = recover_all_shards(&rocks).unwrap();

            assert_eq!(stats.keys_loaded, 5);

            // Verify string
            let value = stores[0].0.get(b"str_key").unwrap();
            assert_eq!(value.as_string().unwrap().as_bytes().as_ref(), b"hello");

            // Verify hash
            let value = stores[0].0.get(b"hash_key").unwrap();
            let hash = value.as_hash().unwrap();
            assert_eq!(hash.get(b"f1"), Some(&Bytes::from("v1")));

            // Verify list
            let value = stores[1].0.get(b"list_key").unwrap();
            let list = value.as_list().unwrap();
            assert_eq!(list.len(), 2);

            // Verify set
            let value = stores[1].0.get(b"set_key").unwrap();
            let set = value.as_set().unwrap();
            assert!(set.contains(b"x"));
            assert!(set.contains(b"y"));

            // Verify sorted set
            let value = stores[0].0.get(b"zset_key").unwrap();
            let zset = value.as_sorted_set().unwrap();
            assert_eq!(zset.get_score(b"alice"), Some(100.0));
        }
    }

    // ========================================================================
    // Stream type persistence tests
    // ========================================================================

    #[test]
    fn test_stream_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut stream = StreamValue::new();
        stream
            .add(
                StreamIdSpec::Explicit(StreamId::new(1000, 0)),
                vec![
                    (Bytes::from("field1"), Bytes::from("value1")),
                    (Bytes::from("field2"), Bytes::from("value2")),
                ],
            )
            .unwrap();
        stream
            .add(
                StreamIdSpec::Explicit(StreamId::new(1001, 0)),
                vec![(Bytes::from("name"), Bytes::from("alice"))],
            )
            .unwrap();

        let value = Value::Stream(stream);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"mystream", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"mystream").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let stream = recovered.as_stream().unwrap();
        assert_eq!(stream.len(), 2);
        assert_eq!(stream.first_id(), Some(StreamId::new(1000, 0)));
        assert_eq!(stream.last_id(), StreamId::new(1001, 0));

        // Verify entry contents
        let entry = stream.first_entry().unwrap();
        assert_eq!(entry.id, StreamId::new(1000, 0));
        assert_eq!(entry.fields.len(), 2);
    }

    #[test]
    fn test_stream_empty() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let stream = StreamValue::new();
        let value = Value::Stream(stream);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"empty_stream", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"empty_stream").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let stream = recovered.as_stream().unwrap();
        assert_eq!(stream.len(), 0);
        assert!(stream.is_empty());
    }

    #[test]
    fn test_stream_with_many_fields() {
        // Test a stream entry with many field-value pairs
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut stream = StreamValue::new();

        // Add entry with many fields
        let fields: Vec<(Bytes, Bytes)> = (0..20)
            .map(|i| {
                (
                    Bytes::from(format!("field{}", i)),
                    Bytes::from(format!("value{}", i)),
                )
            })
            .collect();
        stream
            .add(StreamIdSpec::Explicit(StreamId::new(1000, 0)), fields)
            .unwrap();

        let value = Value::Stream(stream);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"stream_many_fields", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"stream_many_fields").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let stream = recovered.as_stream().unwrap();
        assert_eq!(stream.len(), 1);

        let entry = stream.first_entry().unwrap();
        assert_eq!(entry.fields.len(), 20);

        // Verify some fields
        assert!(entry
            .fields
            .iter()
            .any(|(k, v)| k.as_ref() == b"field0" && v.as_ref() == b"value0"));
        assert!(entry
            .fields
            .iter()
            .any(|(k, v)| k.as_ref() == b"field19" && v.as_ref() == b"value19"));
    }

    #[test]
    fn test_large_stream_persistence() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut stream = StreamValue::new();
        // Stream IDs must be > 0-0 for the first entry
        for i in 1..=1000 {
            stream
                .add(
                    StreamIdSpec::Explicit(StreamId::new(i, 0)),
                    vec![(Bytes::from("idx"), Bytes::from(format!("{}", i)))],
                )
                .unwrap();
        }

        let value = Value::Stream(stream);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"large_stream", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"large_stream").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let stream = recovered.as_stream().unwrap();
        assert_eq!(stream.len(), 1000);
        assert_eq!(stream.first_id(), Some(StreamId::new(1, 0)));
        assert_eq!(stream.last_id(), StreamId::new(1000, 0));
    }

    // ========================================================================
    // BloomFilter type persistence tests
    // ========================================================================

    #[test]
    fn test_bloom_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut bloom = BloomFilterValue::new(1000, 0.01);
        bloom.add(b"item1");
        bloom.add(b"item2");
        bloom.add(b"item3");

        let value = Value::BloomFilter(bloom);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"mybloom", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"mybloom").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let bloom = recovered.as_bloom_filter().unwrap();
        assert!(bloom.contains(b"item1"));
        assert!(bloom.contains(b"item2"));
        assert!(bloom.contains(b"item3"));
        assert!(!bloom.contains(b"not_present"));
        assert_eq!(bloom.count(), 3);
    }

    #[test]
    fn test_bloom_empty() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        // A "new" bloom filter has one layer but no items
        let bloom = BloomFilterValue::new(100, 0.01);
        let value = Value::BloomFilter(bloom);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"empty_bloom", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"empty_bloom").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let bloom = recovered.as_bloom_filter().unwrap();
        assert_eq!(bloom.count(), 0);
        assert_eq!(bloom.num_layers(), 1);
        assert!(!bloom.contains(b"anything"));
    }

    #[test]
    fn test_bloom_with_scaling() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        // Create a small capacity bloom that will scale
        let mut bloom = BloomFilterValue::new(10, 0.01);
        for i in 0..50 {
            bloom.add(format!("item{}", i).as_bytes());
        }

        // Should have created multiple layers
        assert!(bloom.num_layers() > 1);

        let value = Value::BloomFilter(bloom);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"scaled_bloom", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"scaled_bloom").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let bloom = recovered.as_bloom_filter().unwrap();

        // Verify all items are still found
        for i in 0..50 {
            assert!(
                bloom.contains(format!("item{}", i).as_bytes()),
                "item{} not found after recovery",
                i
            );
        }

        // Verify layer count is preserved
        assert!(bloom.num_layers() > 1);
    }

    #[test]
    fn test_bloom_with_options() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        let mut bloom = BloomFilterValue::with_options(100, 0.001, 4, true);
        bloom.add(b"test");

        let value = Value::BloomFilter(bloom);
        let metadata = KeyMetadata::new(value.memory_size());

        rocks
            .put(0, b"options_bloom", &serialize(&value, &metadata))
            .unwrap();

        let data = rocks.get(0, b"options_bloom").unwrap().unwrap();
        let (recovered, _) = deserialize(&data).unwrap();

        let bloom = recovered.as_bloom_filter().unwrap();
        assert!((bloom.error_rate() - 0.001).abs() < 0.0001);
        assert_eq!(bloom.expansion(), 4);
        assert!(bloom.is_non_scaling());
        assert!(bloom.contains(b"test"));
    }
}
