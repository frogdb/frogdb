//! Storage trait and implementations.

use bytes::Bytes;
use griddle::HashMap;

use crate::types::{KeyMetadata, KeyType, Value};

/// Storage trait for key-value operations.
pub trait Store: Send {
    /// Get a value by key.
    fn get(&self, key: &[u8]) -> Option<Value>;

    /// Set a value, returns previous value if any.
    fn set(&mut self, key: Bytes, value: Value) -> Option<Value>;

    /// Delete a key, returns true if existed.
    fn delete(&mut self, key: &[u8]) -> bool;

    /// Check if key exists.
    fn contains(&self, key: &[u8]) -> bool;

    /// Get key type.
    fn key_type(&self, key: &[u8]) -> KeyType;

    /// Number of keys.
    fn len(&self) -> usize;

    /// Check if store is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Memory used by store (bytes).
    fn memory_used(&self) -> usize;

    /// Iterate keys (for SCAN).
    fn scan(&self, cursor: u64, count: usize, pattern: Option<&[u8]>) -> (u64, Vec<Bytes>);
}

/// Entry in the store with value and metadata.
#[derive(Debug)]
struct Entry {
    value: Value,
    metadata: KeyMetadata,
}

/// Default store implementation using griddle::HashMap.
#[derive(Debug)]
pub struct HashMapStore {
    data: HashMap<Bytes, Entry>,
    memory_used: usize,
}

impl HashMapStore {
    /// Create a new empty store.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            memory_used: 0,
        }
    }

    /// Calculate memory size for an entry.
    fn entry_memory_size(key: &[u8], value: &Value) -> usize {
        key.len()
            + value.memory_size()
            + std::mem::size_of::<KeyMetadata>()
            + std::mem::size_of::<Entry>()
    }
}

impl Default for HashMapStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Store for HashMapStore {
    fn get(&self, key: &[u8]) -> Option<Value> {
        self.data.get(key).map(|e| e.value.clone())
    }

    fn set(&mut self, key: Bytes, value: Value) -> Option<Value> {
        let new_size = Self::entry_memory_size(&key, &value);

        let old_value = if let Some(old_entry) = self.data.get(&key) {
            let old_size = Self::entry_memory_size(&key, &old_entry.value);
            self.memory_used = self.memory_used.saturating_sub(old_size);
            Some(old_entry.value.clone())
        } else {
            None
        };

        self.memory_used += new_size;

        let entry = Entry {
            value,
            metadata: KeyMetadata::new(new_size),
        };
        self.data.insert(key, entry);

        old_value
    }

    fn delete(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.remove(key) {
            let size = Self::entry_memory_size(key, &entry.value);
            self.memory_used = self.memory_used.saturating_sub(size);
            true
        } else {
            false
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    fn key_type(&self, key: &[u8]) -> KeyType {
        self.data
            .get(key)
            .map(|e| e.value.key_type())
            .unwrap_or(KeyType::None)
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn memory_used(&self) -> usize {
        self.memory_used
    }

    fn scan(&self, cursor: u64, count: usize, pattern: Option<&[u8]>) -> (u64, Vec<Bytes>) {
        // Simple implementation: cursor is just an index
        let keys: Vec<_> = self.data.keys().cloned().collect();
        let start = cursor as usize;

        if start >= keys.len() {
            return (0, vec![]);
        }

        let mut results = Vec::with_capacity(count);
        let mut current = start;

        while results.len() < count && current < keys.len() {
            let key = &keys[current];

            // Pattern matching (simple glob)
            let matches = match pattern {
                Some(p) => glob_match(p, key),
                None => true,
            };

            if matches {
                results.push(key.clone());
            }
            current += 1;
        }

        let next_cursor = if current >= keys.len() { 0 } else { current as u64 };

        (next_cursor, results)
    }
}

/// Simple glob pattern matching for SCAN.
fn glob_match(pattern: &[u8], key: &[u8]) -> bool {
    // Very simple implementation - only supports * at start/end
    if pattern == b"*" {
        return true;
    }

    if pattern.starts_with(b"*") {
        return key.ends_with(&pattern[1..]);
    }

    if pattern.ends_with(b"*") {
        return key.starts_with(&pattern[..pattern.len() - 1]);
    }

    pattern == key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_set_get() {
        let mut store = HashMapStore::new();

        store.set(Bytes::from("key1"), Value::string("value1"));
        let value = store.get(b"key1").unwrap();

        assert!(matches!(value, Value::String(_)));
        if let Value::String(sv) = value {
            assert_eq!(sv.as_bytes().as_ref(), b"value1");
        }
    }

    #[test]
    fn test_store_delete() {
        let mut store = HashMapStore::new();

        store.set(Bytes::from("key1"), Value::string("value1"));
        assert!(store.contains(b"key1"));

        assert!(store.delete(b"key1"));
        assert!(!store.contains(b"key1"));
        assert!(!store.delete(b"key1"));
    }

    #[test]
    fn test_store_key_type() {
        let mut store = HashMapStore::new();

        assert_eq!(store.key_type(b"missing"), KeyType::None);

        store.set(Bytes::from("string_key"), Value::string("value"));
        assert_eq!(store.key_type(b"string_key"), KeyType::String);
    }

    #[test]
    fn test_store_memory_tracking() {
        let mut store = HashMapStore::new();

        assert_eq!(store.memory_used(), 0);

        store.set(Bytes::from("key"), Value::string("value"));
        assert!(store.memory_used() > 0);

        store.delete(b"key");
        assert_eq!(store.memory_used(), 0);
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"prefix*", b"prefix123"));
        assert!(!glob_match(b"prefix*", b"notprefix"));
        assert!(glob_match(b"*suffix", b"anysuffix"));
        assert!(!glob_match(b"*suffix", b"suffixnot"));
        assert!(glob_match(b"exact", b"exact"));
        assert!(!glob_match(b"exact", b"notexact"));
    }
}
