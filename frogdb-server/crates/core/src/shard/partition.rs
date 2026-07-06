//! Shard partitioning: the load-bearing "which internal shard owns a key" logic.
//!
//! FrogDB routes every key to an internal shard with
//! `internal_shard = CRC16(key) % REDIS_CLUSTER_SLOTS % num_shards`, using the
//! XMODEM CRC16 variant so slot assignment is byte-for-byte compatible with
//! Redis cluster. Hash tags (`{...}`) let clients co-locate related keys on the
//! same slot (and therefore the same shard).
//!
//! These functions are pure and deterministic, which makes them both easy to
//! test and safe to call from anywhere that needs to know where a key lives.

/// Number of Redis cluster hash slots.
pub const REDIS_CLUSTER_SLOTS: usize = 16384;

/// Extract hash tag from a key (Redis-compatible).
///
/// Rules:
/// - First `{` that has a matching `}` with at least one character between
/// - Nested braces: outer wins (first valid match)
/// - Empty braces `{}` are ignored (hash entire key)
pub fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|&b| b == b'{')?;
    let close_offset = key[open + 1..].iter().position(|&b| b == b'}')?;
    let tag = &key[open + 1..open + 1 + close_offset];
    if tag.is_empty() { None } else { Some(tag) }
}

/// Determine which shard owns a key using Redis-compatible CRC16 hashing.
///
/// Uses the XMODEM variant of CRC16, same as Redis cluster.
/// The shard is calculated as: CRC16(key) % 16384 % num_shards
pub fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    let slot = crc16::State::<crc16::XMODEM>::calculate(hash_key) as usize % REDIS_CLUSTER_SLOTS;
    slot % num_shards
}

/// Calculate the Redis cluster slot for a key (0-16383).
pub fn slot_for_key(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16::State::<crc16::XMODEM>::calculate(hash_key) % REDIS_CLUSTER_SLOTS as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_hash_tag_simple() {
        assert_eq!(
            extract_hash_tag(b"{user:1}:profile"),
            Some(b"user:1".as_slice())
        );
        assert_eq!(
            extract_hash_tag(b"{user:1}:settings"),
            Some(b"user:1".as_slice())
        );
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
    fn test_extract_hash_tag_open_without_close() {
        // An unmatched `{` hashes the whole key.
        assert_eq!(extract_hash_tag(b"foo{bar"), None);
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
    fn test_shard_for_key_in_range() {
        // The assigned shard is always a valid index into `num_shards`.
        for num_shards in 1..=16 {
            for i in 0..256 {
                let key = format!("key:{i}");
                assert!(shard_for_key(key.as_bytes(), num_shards) < num_shards);
            }
        }
    }

    #[test]
    fn test_shard_for_key_single_shard() {
        // With one shard everything maps to shard 0.
        assert_eq!(shard_for_key(b"anything", 1), 0);
        assert_eq!(shard_for_key(b"{tag}rest", 1), 0);
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
    fn test_slot_matches_hash_tag_of_bare_key() {
        // A hash-tagged key lands on the same slot as its bare tag.
        assert_eq!(slot_for_key(b"{user:1}:profile"), slot_for_key(b"user:1"));
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
}
