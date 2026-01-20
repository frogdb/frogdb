//! Integration tests for the persistence module.

#[cfg(test)]
mod integration {
    use crate::persistence::*;
    use crate::store::Store;
    use crate::types::{KeyMetadata, SortedSetValue, Value};
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    #[test]
    fn test_roundtrip_persistence() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(RocksStore::open(tmp.path(), 4, &RocksConfig::default()).unwrap());

        // Write various types of data
        let test_data = vec![
            (b"string_raw".to_vec(), Value::string("hello world")),
            (
                b"string_int".to_vec(),
                Value::String(crate::types::StringValue::from_integer(12345)),
            ),
            (b"string_neg".to_vec(), Value::String(crate::types::StringValue::from_integer(-999))),
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

        let wal = RocksWalWriter::new(
            rocks.clone(),
            0,
            WalConfig {
                mode: DurabilityMode::Async,
                batch_size_threshold: 1024 * 1024,
                batch_timeout_ms: 1000,
            },
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
                assert!(value.is_some(), "Key {} not found in shard {}", key, shard_id);

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
        let value = Value::String(crate::types::StringValue::new(Bytes::from(binary_data.clone())));
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
}
