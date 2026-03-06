//! Script cache with LRU eviction.

use bytes::Bytes;
use linked_hash_map::LinkedHashMap;
use sha1::{Digest, Sha1};

/// SHA1 hash type (20 bytes).
pub type ScriptSha = [u8; 20];

/// A cached Lua script.
#[derive(Debug, Clone)]
pub struct CachedScript {
    /// The Lua source code.
    pub source: Bytes,
    /// SHA1 hash of the source.
    pub sha: ScriptSha,
}

impl CachedScript {
    /// Create a new cached script.
    pub fn new(source: Bytes) -> Self {
        let sha = compute_sha(&source);
        Self { source, sha }
    }

    /// Get the size of this script in bytes.
    pub fn size(&self) -> usize {
        self.source.len()
    }
}

/// Compute SHA1 hash of script source.
pub fn compute_sha(source: &[u8]) -> ScriptSha {
    let mut hasher = Sha1::new();
    hasher.update(source);
    let result = hasher.finalize();
    let mut sha = [0u8; 20];
    sha.copy_from_slice(&result);
    sha
}

/// Convert SHA1 hash to hex string.
pub fn sha_to_hex(sha: &ScriptSha) -> String {
    sha.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Parse hex string to SHA1 hash.
pub fn hex_to_sha(hex: &[u8]) -> Option<ScriptSha> {
    if hex.len() != 40 {
        return None;
    }

    let mut sha = [0u8; 20];
    for (i, chunk) in hex.chunks(2).enumerate() {
        let high = hex_digit(chunk[0])?;
        let low = hex_digit(chunk[1])?;
        sha[i] = (high << 4) | low;
    }
    Some(sha)
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Script cache with LRU eviction.
pub struct ScriptCache {
    /// Scripts indexed by SHA1, ordered by LRU.
    scripts: LinkedHashMap<ScriptSha, CachedScript>,
    /// Maximum number of scripts.
    max_scripts: usize,
    /// Maximum total bytes.
    max_bytes: usize,
    /// Current total bytes.
    current_bytes: usize,
}

impl ScriptCache {
    /// Create a new script cache.
    pub fn new(max_scripts: usize, max_bytes: usize) -> Self {
        Self {
            scripts: LinkedHashMap::new(),
            max_scripts,
            max_bytes,
            current_bytes: 0,
        }
    }

    /// Get a script by SHA1 hash.
    pub fn get(&mut self, sha: &ScriptSha) -> Option<&CachedScript> {
        // Move to back (most recently used) on access
        if self.scripts.contains_key(sha) {
            self.scripts.get_refresh(sha).map(|s| &*s)
        } else {
            None
        }
    }

    /// Get a script by SHA1 hash without updating LRU order.
    pub fn peek(&self, sha: &ScriptSha) -> Option<&CachedScript> {
        self.scripts.get(sha)
    }

    /// Insert a script into the cache.
    pub fn insert(&mut self, script: CachedScript) {
        let sha = script.sha;
        let size = script.size();

        // Remove if already exists
        if let Some(old) = self.scripts.remove(&sha) {
            self.current_bytes -= old.size();
        }

        // Evict until we have space
        while (self.scripts.len() >= self.max_scripts || self.current_bytes + size > self.max_bytes)
            && !self.scripts.is_empty()
        {
            if let Some((_, evicted)) = self.scripts.pop_front() {
                self.current_bytes -= evicted.size();
            }
        }

        // Insert new script
        self.current_bytes += size;
        self.scripts.insert(sha, script);
    }

    /// Load a script (insert and return the SHA).
    pub fn load(&mut self, source: Bytes) -> ScriptSha {
        let script = CachedScript::new(source);
        let sha = script.sha;
        self.insert(script);
        sha
    }

    /// Check if a script exists by SHA.
    pub fn exists(&self, sha: &ScriptSha) -> bool {
        self.scripts.contains_key(sha)
    }

    /// Check existence of multiple SHAs.
    pub fn exists_many(&self, shas: &[ScriptSha]) -> Vec<bool> {
        shas.iter().map(|sha| self.exists(sha)).collect()
    }

    /// Clear all scripts from the cache.
    pub fn flush(&mut self) {
        self.scripts.clear();
        self.current_bytes = 0;
    }

    /// Get the number of cached scripts.
    pub fn len(&self) -> usize {
        self.scripts.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.scripts.is_empty()
    }

    /// Get current cache size in bytes.
    pub fn current_bytes(&self) -> usize {
        self.current_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_sha() {
        let source = b"return 1";
        let sha = compute_sha(source);
        // SHA1 is deterministic
        let sha2 = compute_sha(source);
        assert_eq!(sha, sha2);
    }

    #[test]
    fn test_sha_hex_roundtrip() {
        let source = b"return 'hello'";
        let sha = compute_sha(source);
        let hex = sha_to_hex(&sha);
        let parsed = hex_to_sha(hex.as_bytes()).unwrap();
        assert_eq!(sha, parsed);
    }

    #[test]
    fn test_hex_to_sha_invalid() {
        assert!(hex_to_sha(b"invalid").is_none());
        assert!(hex_to_sha(b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_none());
    }

    #[test]
    fn test_cache_insert_and_get() {
        let mut cache = ScriptCache::new(100, 1024 * 1024);
        let source = Bytes::from_static(b"return 1");
        let sha = cache.load(source.clone());

        let script = cache.get(&sha).unwrap();
        assert_eq!(script.source, source);
    }

    #[test]
    fn test_cache_lru_eviction_by_count() {
        let mut cache = ScriptCache::new(2, 1024 * 1024);

        let sha1 = cache.load(Bytes::from_static(b"return 1"));
        let sha2 = cache.load(Bytes::from_static(b"return 2"));

        // Both should exist
        assert!(cache.exists(&sha1));
        assert!(cache.exists(&sha2));

        // Insert third script - should evict first
        let sha3 = cache.load(Bytes::from_static(b"return 3"));

        assert!(!cache.exists(&sha1)); // Evicted
        assert!(cache.exists(&sha2));
        assert!(cache.exists(&sha3));
    }

    #[test]
    fn test_cache_lru_access_order() {
        let mut cache = ScriptCache::new(2, 1024 * 1024);

        let sha1 = cache.load(Bytes::from_static(b"return 1"));
        let sha2 = cache.load(Bytes::from_static(b"return 2"));

        // Access sha1 to make it recently used
        cache.get(&sha1);

        // Insert third script - should evict sha2 (least recently used)
        let sha3 = cache.load(Bytes::from_static(b"return 3"));

        assert!(cache.exists(&sha1)); // Still exists (was accessed)
        assert!(!cache.exists(&sha2)); // Evicted
        assert!(cache.exists(&sha3));
    }

    #[test]
    fn test_cache_lru_eviction_by_size() {
        let mut cache = ScriptCache::new(100, 20);

        let sha1 = cache.load(Bytes::from_static(b"return 1")); // 8 bytes
        let sha2 = cache.load(Bytes::from_static(b"return 2")); // 8 bytes

        // Both fit (16 bytes < 20)
        assert!(cache.exists(&sha1));
        assert!(cache.exists(&sha2));

        // Insert larger script - should evict older ones
        let sha3 = cache.load(Bytes::from_static(b"return 'hello'")); // 15 bytes

        // sha1 and sha2 should be evicted to make room
        assert!(!cache.exists(&sha1));
        assert!(!cache.exists(&sha2));
        assert!(cache.exists(&sha3));
    }

    #[test]
    fn test_cache_flush() {
        let mut cache = ScriptCache::new(100, 1024 * 1024);
        cache.load(Bytes::from_static(b"return 1"));
        cache.load(Bytes::from_static(b"return 2"));

        assert_eq!(cache.len(), 2);

        cache.flush();

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.current_bytes(), 0);
    }

    #[test]
    fn test_cache_exists_many() {
        let mut cache = ScriptCache::new(100, 1024 * 1024);
        let sha1 = cache.load(Bytes::from_static(b"return 1"));
        let sha2 = compute_sha(b"not cached");

        let results = cache.exists_many(&[sha1, sha2]);
        assert_eq!(results, vec![true, false]);
    }
}
