//! The incumbent: `griddle::HashMap<Bytes, Entry>`, the shipped `HashMapStore`'s table.
//!
//! `Entry` is reproduced field-for-field from
//! `frogdb-server/crates/core/src/store/hashmap.rs` and
//! `frogdb-server/crates/types/src/types/mod.rs`, because a standalone spike crate
//! cannot depend on `frogdb-types` without dragging `usearch` and the rest of the
//! workspace in. What that costs in fidelity, precisely:
//!
//! - `size_of::<Entry>()` is **exact**. `ValueLocation` holds `Arc<Value>`, a
//!   pointer, so `Entry`'s size does not depend on `Value`'s size at all — only on
//!   `KeyMetadata` and `KeyType`, both reproduced exactly.
//! - `size_of::<Value>()` is **a lower bound**. The real enum has fifteen variants;
//!   this one has the String variant the sweep exercises. So the per-entry
//!   `Arc<Value>` allocation measured here is the smallest the incumbent can be, and
//!   every baseline number below is *generous to the incumbent*.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use griddle::HashMap;

/// Mirrors `frogdb_types::KeyType` — a fieldless enum, one byte.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum KeyType {
    String,
    List,
    Set,
    Hash,
    ZSet,
    Stream,
}

/// Mirrors `frogdb_types::KeyMetadata`.
#[derive(Debug, Clone)]
pub struct KeyMetadata {
    pub expires_at: Option<Instant>,
    pub last_access: Instant,
    pub lfu_counter: u8,
    pub memory_size: usize,
}

/// Mirrors `frogdb_types::StringValue`'s inner enum.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum StringData {
    Raw(Bytes),
    Integer(i64),
}

#[derive(Debug, Clone)]
pub struct StringValue {
    #[allow(dead_code)]
    data: StringData,
}

/// The String arm of `frogdb_types::Value`. See the module note on fidelity.
#[derive(Debug, Clone)]
pub enum Value {
    String(StringValue),
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum ValueLocation {
    Hot(Arc<Value>),
    Warm,
}

/// Mirrors `HashMapStore`'s `Entry`.
#[derive(Debug)]
pub struct Entry {
    #[allow(dead_code)]
    pub location: ValueLocation,
    #[allow(dead_code)]
    pub metadata: KeyMetadata,
    #[allow(dead_code)]
    pub key_type: KeyType,
}

/// The shipped table type.
pub type Baseline = HashMap<Bytes, Entry>;

pub fn insert(map: &mut Baseline, key: &[u8], int: Option<i64>, bytes: &[u8]) {
    let data = match int {
        Some(v) => StringData::Integer(v),
        None => StringData::Raw(Bytes::copy_from_slice(bytes)),
    };
    let value = Arc::new(Value::String(StringValue { data }));
    let memory_size = key.len() + bytes.len();
    map.insert(
        Bytes::copy_from_slice(key),
        Entry {
            location: ValueLocation::Hot(value),
            metadata: KeyMetadata {
                expires_at: None,
                last_access: Instant::now(),
                lfu_counter: 5,
                memory_size,
            },
            key_type: KeyType::String,
        },
    );
}

/// Sizes the report quotes for the incumbent's per-entry structure.
pub fn sizes() -> (usize, usize, usize, usize) {
    use std::mem::size_of;
    (
        size_of::<Bytes>(),
        size_of::<Entry>(),
        size_of::<(Bytes, Entry)>(),
        size_of::<Value>(),
    )
}
