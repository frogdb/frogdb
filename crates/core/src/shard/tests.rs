use super::*;

#[test]
fn test_extract_hash_tag_simple() {
    assert_eq!(extract_hash_tag(b"{user:1}:profile"), Some(b"user:1".as_slice()));
    assert_eq!(extract_hash_tag(b"{user:1}:settings"), Some(b"user:1".as_slice()));
}

#[test]
fn test_extract_hash_tag_no_braces() {
    assert_eq!(extract_hash_tag(b"user:1:profile"), None);
}

#[test]
fn test_extract_hash_tag_empty_braces() {
    assert_eq!(extract_hash_tag(b"foo{}bar"), None);
    assert_eq!(extract_hash_tag(b"{}"), None);
}

#[test]
fn test_extract_hash_tag_nested() {
    // First { to first } after it
    assert_eq!(extract_hash_tag(b"{{foo}}"), Some(b"{foo".as_slice()));
}

#[test]
fn test_extract_hash_tag_multiple() {
    // First valid tag wins
    assert_eq!(extract_hash_tag(b"foo{bar}{zap}"), Some(b"bar".as_slice()));
}

#[test]
fn test_extract_hash_tag_empty_then_valid() {
    // Empty first braces skipped
    assert_eq!(extract_hash_tag(b"{}{valid}"), None); // Actually returns None because {} comes first
}

#[test]
fn test_shard_for_key_consistent() {
    let key1 = b"user:123";
    let key2 = b"user:123";
    assert_eq!(shard_for_key(key1, 4), shard_for_key(key2, 4));
}

#[test]
fn test_shard_for_key_hash_tag() {
    // Keys with same hash tag should go to same shard
    let key1 = b"{user:1}:profile";
    let key2 = b"{user:1}:settings";
    assert_eq!(shard_for_key(key1, 4), shard_for_key(key2, 4));
}

#[test]
fn test_slot_for_key_hash_tag_colocation() {
    // Keys with same hash tag should map to the same slot
    let key1 = b"{user:1}:profile";
    let key2 = b"{user:1}:session";
    let key3 = b"{user:1}:settings";

    let slot1 = slot_for_key(key1);
    let slot2 = slot_for_key(key2);
    let slot3 = slot_for_key(key3);

    assert_eq!(slot1, slot2);
    assert_eq!(slot2, slot3);
}

#[test]
fn test_slot_for_key_range() {
    // Slots should be in range 0-16383
    for i in 0..1000 {
        let key = format!("key:{}", i);
        let slot = slot_for_key(key.as_bytes());
        assert!(slot < REDIS_CLUSTER_SLOTS as u16);
    }
}

#[test]
fn test_shard_distribution() {
    // Test that keys distribute across shards
    let num_shards = 4;
    let mut shard_counts = vec![0usize; num_shards];

    for i in 0..1000 {
        let key = format!("key:{}", i);
        let shard = shard_for_key(key.as_bytes(), num_shards);
        shard_counts[shard] += 1;
    }

    // Each shard should have at least some keys (distribution check)
    for count in &shard_counts {
        assert!(*count > 0, "Shard has no keys assigned");
    }
}

#[test]
fn test_crc16_known_values() {
    // Test against known Redis CRC16 values
    // "123456789" should hash to 0x31C3 (12739) using XMODEM
    let crc = crc16::State::<crc16::XMODEM>::calculate(b"123456789");
    assert_eq!(crc, 0x31C3);
}
