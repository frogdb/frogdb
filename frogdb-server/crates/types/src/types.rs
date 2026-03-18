//! Value types and key metadata.

use bytes::{Bytes, BytesMut};
use ordered_float::OrderedFloat;
use rand::seq::SliceRandom;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bitvec::prelude::*;

use crate::bloom::{BloomFilterValue, BloomLayer};
use crate::cms::CountMinSketchValue;
use crate::cuckoo::{CuckooFilterValue, CuckooLayer};
use crate::tdigest::TDigestValue;
use crate::hyperloglog::HyperLogLogValue;
use crate::json::JsonValue;
use crate::skiplist::SkipList;
use crate::timeseries::{CompressedChunk, DuplicatePolicy, TimeSeriesValue};
use crate::topk::TopKValue;
use std::sync::atomic::{AtomicU8, Ordering as AtomicOrdering};

/// Value types stored in FrogDB.
#[derive(Debug, Clone)]
pub enum Value {
    /// String value.
    String(StringValue),
    /// Sorted set value.
    SortedSet(SortedSetValue),
    /// Hash value.
    Hash(HashValue),
    /// List value.
    List(ListValue),
    /// Set value.
    Set(SetValue),
    /// Stream value.
    Stream(StreamValue),
    /// Bloom filter value.
    BloomFilter(BloomFilterValue),
    /// HyperLogLog value.
    HyperLogLog(HyperLogLogValue),
    /// Time series value.
    TimeSeries(TimeSeriesValue),
    /// JSON document value.
    Json(JsonValue),
    /// Cuckoo filter value.
    CuckooFilter(CuckooFilterValue),
    /// Top-K probabilistic data structure.
    TopK(TopKValue),
    /// T-Digest value.
    TDigest(TDigestValue),
    /// Count-Min Sketch probabilistic data structure.
    CountMinSketch(CountMinSketchValue),
}

/// Macro to generate accessor methods for Value enum variants.
///
/// This generates `as_<name>(&self) -> Option<&Type>` and
/// `as_<name>_mut(&mut self) -> Option<&mut Type>` methods.
macro_rules! impl_value_accessors {
    ($(
        $variant:ident => $type:ty, $method:ident, $method_mut:ident
    );* $(;)?) => {
        impl Value {
            $(
                #[doc = concat!("Try to get as a ", stringify!($method), " value.")]
                pub fn $method(&self) -> Option<&$type> {
                    match self {
                        Value::$variant(v) => Some(v),
                        _ => None,
                    }
                }

                #[doc = concat!("Try to get as a mutable ", stringify!($method), " value.")]
                pub fn $method_mut(&mut self) -> Option<&mut $type> {
                    match self {
                        Value::$variant(v) => Some(v),
                        _ => None,
                    }
                }
            )*
        }
    };
}

impl_value_accessors! {
    String => StringValue, as_string, as_string_mut;
    SortedSet => SortedSetValue, as_sorted_set, as_sorted_set_mut;
    Hash => HashValue, as_hash, as_hash_mut;
    List => ListValue, as_list, as_list_mut;
    Set => SetValue, as_set, as_set_mut;
    Stream => StreamValue, as_stream, as_stream_mut;
    BloomFilter => BloomFilterValue, as_bloom_filter, as_bloom_filter_mut;
    HyperLogLog => HyperLogLogValue, as_hyperloglog, as_hyperloglog_mut;
    TimeSeries => TimeSeriesValue, as_timeseries, as_timeseries_mut;
    Json => JsonValue, as_json, as_json_mut;
    CuckooFilter => CuckooFilterValue, as_cuckoo_filter, as_cuckoo_filter_mut;
    TopK => TopKValue, as_topk, as_topk_mut;
    TDigest => TDigestValue, as_tdigest, as_tdigest_mut;
    CountMinSketch => CountMinSketchValue, as_cms, as_cms_mut;
}

impl Value {
    /// Create a string value from bytes.
    pub fn string(data: impl Into<Bytes>) -> Self {
        Value::String(StringValue::new(data))
    }

    /// Create a sorted set value.
    pub fn sorted_set() -> Self {
        Value::SortedSet(SortedSetValue::new())
    }

    /// Create a hash value.
    pub fn hash() -> Self {
        Value::Hash(HashValue::new())
    }

    /// Create a list value.
    pub fn list() -> Self {
        Value::List(ListValue::new())
    }

    /// Create a set value.
    pub fn set() -> Self {
        Value::Set(SetValue::new())
    }

    /// Create a stream value.
    pub fn stream() -> Self {
        Value::Stream(StreamValue::new())
    }

    /// Create a bloom filter value with default settings.
    pub fn bloom_filter(capacity: u64, error_rate: f64) -> Self {
        Value::BloomFilter(BloomFilterValue::new(capacity, error_rate))
    }

    /// Create a HyperLogLog value.
    pub fn hyperloglog() -> Self {
        Value::HyperLogLog(HyperLogLogValue::new())
    }

    /// Create a time series value.
    pub fn timeseries() -> Self {
        Value::TimeSeries(TimeSeriesValue::new())
    }

    /// Create a JSON value.
    pub fn json(data: serde_json::Value) -> Self {
        Value::Json(JsonValue::new(data))
    }

    /// Get the key type.
    pub fn key_type(&self) -> KeyType {
        match self {
            Value::String(_) => KeyType::String,
            Value::SortedSet(_) => KeyType::SortedSet,
            Value::Hash(_) => KeyType::Hash,
            Value::List(_) => KeyType::List,
            Value::Set(_) => KeyType::Set,
            Value::Stream(_) => KeyType::Stream,
            Value::BloomFilter(_) => KeyType::BloomFilter,
            Value::HyperLogLog(_) => KeyType::HyperLogLog,
            Value::TimeSeries(_) => KeyType::TimeSeries,
            Value::Json(_) => KeyType::Json,
            Value::CuckooFilter(_) => KeyType::CuckooFilter,
            Value::TopK(_) => KeyType::TopK,
            Value::TDigest(_) => KeyType::TDigest,
            Value::CountMinSketch(_) => KeyType::CountMinSketch,
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        match self {
            Value::String(s) => s.memory_size(),
            Value::SortedSet(z) => z.memory_size(),
            Value::Hash(h) => h.memory_size(),
            Value::List(l) => l.memory_size(),
            Value::Set(s) => s.memory_size(),
            Value::Stream(st) => st.memory_size(),
            Value::BloomFilter(bf) => bf.memory_size(),
            Value::HyperLogLog(hll) => hll.memory_size(),
            Value::TimeSeries(ts) => ts.memory_size(),
            Value::Json(j) => j.memory_size(),
            Value::CuckooFilter(cf) => cf.memory_size(),
            Value::TopK(tk) => tk.memory_size(),
            Value::TDigest(td) => td.memory_size(),
            Value::CountMinSketch(cms) => cms.memory_size(),
        }
    }

    /// Serialize this value for cross-shard copy operations.
    /// Returns (type_string, serialized_bytes).
    pub fn serialize_for_copy(&self) -> (&'static str, Bytes) {
        match self {
            Value::String(s) => ("string", s.as_bytes()),
            Value::Hash(h) => {
                // Serialize as: num_fields || (key_len || key || value_len || value)*
                let mut buf = Vec::new();
                let fields: Vec<_> = h.iter().collect();
                buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
                for (k, v) in fields {
                    buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&k);
                    buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&v);
                }
                ("hash", Bytes::from(buf))
            }
            Value::List(l) => {
                // Serialize as: num_elements || (element_len || element)*
                let mut buf = Vec::new();
                let elements: Vec<_> = l.iter().collect();
                buf.extend_from_slice(&(elements.len() as u32).to_le_bytes());
                for elem in elements {
                    buf.extend_from_slice(&(elem.len() as u32).to_le_bytes());
                    buf.extend_from_slice(elem);
                }
                ("list", Bytes::from(buf))
            }
            Value::Set(s) => {
                // Serialize as: num_members || (member_len || member)*
                let mut buf = Vec::new();
                let members: Vec<_> = s.members().collect();
                buf.extend_from_slice(&(members.len() as u32).to_le_bytes());
                for member in members {
                    buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&member);
                }
                ("set", Bytes::from(buf))
            }
            Value::SortedSet(z) => {
                // Serialize as: num_members || (member_len || member || score)*
                let mut buf = Vec::new();
                let members: Vec<(&Bytes, f64)> = z.iter().collect();
                buf.extend_from_slice(&(members.len() as u32).to_le_bytes());
                for (member, score) in members {
                    buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
                    buf.extend_from_slice(member);
                    buf.extend_from_slice(&score.to_le_bytes());
                }
                ("zset", Bytes::from(buf))
            }
            Value::Stream(stream) => {
                let mut buf = Vec::new();
                let last_id = stream.last_id();
                buf.extend_from_slice(&last_id.ms.to_le_bytes());
                buf.extend_from_slice(&last_id.seq.to_le_bytes());
                let entries = stream.to_vec();
                buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                for entry in &entries {
                    buf.extend_from_slice(&entry.id.ms.to_le_bytes());
                    buf.extend_from_slice(&entry.id.seq.to_le_bytes());
                    buf.extend_from_slice(&(entry.fields.len() as u32).to_le_bytes());
                    for (field, value) in &entry.fields {
                        buf.extend_from_slice(&(field.len() as u32).to_le_bytes());
                        buf.extend_from_slice(field);
                        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                        buf.extend_from_slice(value);
                    }
                }
                ("stream", Bytes::from(buf))
            }
            Value::BloomFilter(bf) => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&bf.error_rate().to_le_bytes());
                buf.extend_from_slice(&bf.expansion().to_le_bytes());
                buf.push(u8::from(bf.is_non_scaling()));
                buf.extend_from_slice(&(bf.layers().len() as u32).to_le_bytes());
                for layer in bf.layers() {
                    buf.extend_from_slice(&layer.k().to_le_bytes());
                    buf.extend_from_slice(&layer.count().to_le_bytes());
                    buf.extend_from_slice(&layer.capacity().to_le_bytes());
                    buf.extend_from_slice(&(layer.size_bits() as u64).to_le_bytes());
                    let bits_bytes = layer.bits_as_bytes();
                    buf.extend_from_slice(&(bits_bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(bits_bytes);
                }
                ("bloom", Bytes::from(buf))
            }
            Value::HyperLogLog(hll) => {
                // Serialize the raw registers
                ("hll", Bytes::from(hll.serialize()))
            }
            Value::TimeSeries(ts) => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&ts.retention_ms().to_le_bytes());
                let dup_policy: u8 = match ts.duplicate_policy() {
                    DuplicatePolicy::Block => 0,
                    DuplicatePolicy::First => 1,
                    DuplicatePolicy::Last => 2,
                    DuplicatePolicy::Min => 3,
                    DuplicatePolicy::Max => 4,
                    DuplicatePolicy::Sum => 5,
                };
                buf.push(dup_policy);
                buf.extend_from_slice(&(ts.chunk_size() as u32).to_le_bytes());
                let labels = ts.labels();
                buf.extend_from_slice(&(labels.len() as u32).to_le_bytes());
                for (name, value) in labels {
                    buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
                    buf.extend_from_slice(name.as_bytes());
                    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    buf.extend_from_slice(value.as_bytes());
                }
                let chunks = ts.chunks();
                buf.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
                for chunk in chunks {
                    buf.extend_from_slice(&chunk.start_time().to_le_bytes());
                    buf.extend_from_slice(&chunk.end_time().to_le_bytes());
                    buf.extend_from_slice(&chunk.sample_count().to_le_bytes());
                    let data = chunk.data();
                    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                    buf.extend_from_slice(data);
                }
                let active = ts.active_samples();
                buf.extend_from_slice(&(active.len() as u32).to_le_bytes());
                for (&timestamp, &value) in active {
                    buf.extend_from_slice(&timestamp.to_le_bytes());
                    buf.extend_from_slice(&value.to_le_bytes());
                }
                ("timeseries", Bytes::from(buf))
            }
            Value::Json(j) => {
                // Serialize JSON to string using serde_json
                let json_str = serde_json::to_string(j.data()).unwrap_or_default();
                ("json", Bytes::from(json_str))
            }
            Value::CuckooFilter(cf) => {
                let mut buf = Vec::new();
                buf.push(cf.bucket_size());
                buf.extend_from_slice(&cf.max_iterations().to_le_bytes());
                buf.extend_from_slice(&cf.expansion().to_le_bytes());
                buf.extend_from_slice(&cf.delete_count().to_le_bytes());
                buf.extend_from_slice(&(cf.num_layers() as u32).to_le_bytes());
                for layer in cf.layers() {
                    buf.extend_from_slice(&(layer.num_buckets() as u64).to_le_bytes());
                    buf.push(layer.bucket_size());
                    buf.extend_from_slice(&layer.total_count().to_le_bytes());
                    buf.extend_from_slice(&layer.capacity().to_le_bytes());
                    for bucket in layer.buckets() {
                        for &fp in bucket {
                            buf.extend_from_slice(&fp.to_le_bytes());
                        }
                    }
                }
                ("cuckoo", Bytes::from(buf))
            }
            Value::TopK(tk) => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&tk.k().to_le_bytes());
                buf.extend_from_slice(&tk.width().to_le_bytes());
                buf.extend_from_slice(&tk.depth().to_le_bytes());
                buf.extend_from_slice(&tk.decay().to_le_bytes());
                // Buckets: depth * width * (fingerprint:u32 + counter:u32)
                for row in &tk.buckets_raw() {
                    for &(fp, ctr) in row {
                        buf.extend_from_slice(&fp.to_le_bytes());
                        buf.extend_from_slice(&ctr.to_le_bytes());
                    }
                }
                // Heap items
                let heap = tk.heap_items();
                buf.extend_from_slice(&(heap.len() as u32).to_le_bytes());
                for (item, count) in heap {
                    buf.extend_from_slice(&(item.len() as u32).to_le_bytes());
                    buf.extend_from_slice(item);
                    buf.extend_from_slice(&count.to_le_bytes());
                }
                ("topk", Bytes::from(buf))
            }
            Value::TDigest(td) => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&td.compression().to_le_bytes());
                buf.extend_from_slice(&td.raw_min().to_le_bytes());
                buf.extend_from_slice(&td.raw_max().to_le_bytes());
                buf.extend_from_slice(&td.merged_weight().to_le_bytes());
                buf.extend_from_slice(&td.unmerged_weight().to_le_bytes());
                buf.extend_from_slice(&(td.centroids().len() as u32).to_le_bytes());
                buf.extend_from_slice(&(td.unmerged().len() as u32).to_le_bytes());
                for c in td.centroids() {
                    buf.extend_from_slice(&c.mean.to_le_bytes());
                    buf.extend_from_slice(&c.weight.to_le_bytes());
                }
                for c in td.unmerged() {
                    buf.extend_from_slice(&c.mean.to_le_bytes());
                    buf.extend_from_slice(&c.weight.to_le_bytes());
                }
                ("tdigest", Bytes::from(buf))
            }
            Value::CountMinSketch(cms) => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&cms.width().to_le_bytes());
                buf.extend_from_slice(&cms.depth().to_le_bytes());
                buf.extend_from_slice(&cms.count().to_le_bytes());
                for row in cms.counters_raw() {
                    for &val in row {
                        buf.extend_from_slice(&val.to_le_bytes());
                    }
                }
                ("cms", Bytes::from(buf))
            }
        }
    }

    /// Deserialize a value from cross-shard copy data.
    /// Returns None if deserialization fails.
    pub fn deserialize_for_copy(type_str: &[u8], data: &[u8]) -> Option<Self> {
        match type_str {
            b"string" => Some(Value::String(StringValue::new(Bytes::copy_from_slice(
                data,
            )))),
            b"hash" => {
                if data.len() < 4 {
                    return None;
                }
                let mut pos = 0;
                let num_fields = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut hash = HashValue::new();
                for _ in 0..num_fields {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + key_len > data.len() {
                        return None;
                    }
                    let key = Bytes::copy_from_slice(&data[pos..pos + key_len]);
                    pos += key_len;

                    if pos + 4 > data.len() {
                        return None;
                    }
                    let value_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + value_len > data.len() {
                        return None;
                    }
                    let value = Bytes::copy_from_slice(&data[pos..pos + value_len]);
                    pos += value_len;

                    hash.set(key, value, ListpackThresholds::DEFAULT_HASH);
                }
                Some(Value::Hash(hash))
            }
            b"list" => {
                if data.len() < 4 {
                    return None;
                }
                let mut pos = 0;
                let num_elements = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut list = ListValue::new();
                for _ in 0..num_elements {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let elem_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + elem_len > data.len() {
                        return None;
                    }
                    let elem = Bytes::copy_from_slice(&data[pos..pos + elem_len]);
                    pos += elem_len;
                    list.push_back(elem);
                }
                Some(Value::List(list))
            }
            b"set" => {
                if data.len() < 4 {
                    return None;
                }
                let mut pos = 0;
                let num_members = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut set = SetValue::new();
                for _ in 0..num_members {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let member_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + member_len > data.len() {
                        return None;
                    }
                    let member = Bytes::copy_from_slice(&data[pos..pos + member_len]);
                    pos += member_len;
                    set.add(member, ListpackThresholds::DEFAULT_SET);
                }
                Some(Value::Set(set))
            }
            b"zset" => {
                if data.len() < 4 {
                    return None;
                }
                let mut pos = 0;
                let num_members = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut zset = SortedSetValue::new();
                for _ in 0..num_members {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let member_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + member_len > data.len() {
                        return None;
                    }
                    let member = Bytes::copy_from_slice(&data[pos..pos + member_len]);
                    pos += member_len;

                    if pos + 8 > data.len() {
                        return None;
                    }
                    let score = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    zset.add(member, score);
                }
                Some(Value::SortedSet(zset))
            }
            b"hll" => {
                let hll = HyperLogLogValue::deserialize(data)?;
                Some(Value::HyperLogLog(hll))
            }
            b"json" => {
                let json_str = std::str::from_utf8(data).ok()?;
                let json_value: serde_json::Value = serde_json::from_str(json_str).ok()?;
                Some(Value::Json(JsonValue::new(json_value)))
            }
            b"stream" => {
                let mut pos = 0;
                if pos + 16 > data.len() {
                    return None;
                }
                let last_ms = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                let last_seq = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                if pos + 4 > data.len() {
                    return None;
                }
                let num_entries = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut stream = StreamValue::new();
                for _ in 0..num_entries {
                    if pos + 20 > data.len() {
                        return None;
                    }
                    let ms = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let seq = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let num_fields =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;

                    let mut fields = Vec::with_capacity(num_fields);
                    for _ in 0..num_fields {
                        if pos + 4 > data.len() {
                            return None;
                        }
                        let flen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                        pos += 4;
                        if pos + flen > data.len() {
                            return None;
                        }
                        let field = Bytes::copy_from_slice(&data[pos..pos + flen]);
                        pos += flen;
                        if pos + 4 > data.len() {
                            return None;
                        }
                        let vlen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                        pos += 4;
                        if pos + vlen > data.len() {
                            return None;
                        }
                        let value = Bytes::copy_from_slice(&data[pos..pos + vlen]);
                        pos += vlen;
                        fields.push((field, value));
                    }

                    let id = StreamId::new(ms, seq);
                    stream.add(StreamIdSpec::Explicit(id), fields).ok()?;
                }

                // Preserve last_id even if entries were deleted (stream tracks high-water mark)
                let _ = last_ms;
                let _ = last_seq;
                Some(Value::Stream(stream))
            }
            b"bloom" => {
                let mut pos = 0;
                if pos + 8 > data.len() {
                    return None;
                }
                let error_rate = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                if pos + 4 > data.len() {
                    return None;
                }
                let expansion = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                pos += 4;
                if pos + 1 > data.len() {
                    return None;
                }
                let non_scaling = data[pos] != 0;
                pos += 1;
                if pos + 4 > data.len() {
                    return None;
                }
                let num_layers = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut layers = Vec::with_capacity(num_layers);
                for _ in 0..num_layers {
                    if pos + 24 > data.len() {
                        return None;
                    }
                    let k = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                    pos += 4;
                    let count = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let capacity = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let size_bits =
                        u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?) as usize;
                    pos += 8;
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let byte_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + byte_len > data.len() {
                        return None;
                    }
                    let raw_bytes = data[pos..pos + byte_len].to_vec();
                    pos += byte_len;

                    let mut bits = BitVec::<u8, Lsb0>::from_vec(raw_bytes);
                    bits.truncate(size_bits);
                    layers.push(BloomLayer::from_raw(bits, k, count, capacity));
                }

                Some(Value::BloomFilter(BloomFilterValue::from_raw(
                    layers,
                    error_rate,
                    expansion,
                    non_scaling,
                )))
            }
            b"timeseries" => {
                let mut pos = 0;
                if pos + 8 > data.len() {
                    return None;
                }
                let retention_ms = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                if pos + 1 > data.len() {
                    return None;
                }
                let dup_policy = match data[pos] {
                    0 => DuplicatePolicy::Block,
                    1 => DuplicatePolicy::First,
                    2 => DuplicatePolicy::Last,
                    3 => DuplicatePolicy::Min,
                    4 => DuplicatePolicy::Max,
                    5 => DuplicatePolicy::Sum,
                    _ => return None,
                };
                pos += 1;
                if pos + 4 > data.len() {
                    return None;
                }
                let chunk_size = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;
                if pos + 4 > data.len() {
                    return None;
                }
                let num_labels = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut labels = Vec::with_capacity(num_labels);
                for _ in 0..num_labels {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let nlen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + nlen > data.len() {
                        return None;
                    }
                    let name = String::from_utf8(data[pos..pos + nlen].to_vec()).ok()?;
                    pos += nlen;
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let vlen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + vlen > data.len() {
                        return None;
                    }
                    let value = String::from_utf8(data[pos..pos + vlen].to_vec()).ok()?;
                    pos += vlen;
                    labels.push((name, value));
                }

                if pos + 4 > data.len() {
                    return None;
                }
                let num_chunks = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut chunks = Vec::with_capacity(num_chunks);
                for _ in 0..num_chunks {
                    if pos + 20 > data.len() {
                        return None;
                    }
                    let start = i64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let end = i64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                    pos += 4;
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let dlen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + dlen > data.len() {
                        return None;
                    }
                    let cdata = data[pos..pos + dlen].to_vec();
                    pos += dlen;
                    chunks.push(CompressedChunk::from_raw(cdata, start, end, count));
                }

                if pos + 4 > data.len() {
                    return None;
                }
                let num_active = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut active_samples = std::collections::BTreeMap::new();
                for _ in 0..num_active {
                    if pos + 16 > data.len() {
                        return None;
                    }
                    let timestamp = i64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let value = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    active_samples.insert(timestamp, value);
                }

                Some(Value::TimeSeries(TimeSeriesValue::from_raw(
                    active_samples,
                    chunks,
                    labels,
                    retention_ms,
                    dup_policy,
                    chunk_size,
                )))
            }
            b"cuckoo" => {
                let mut pos = 0;
                if pos + 1 > data.len() {
                    return None;
                }
                let bucket_size = data[pos];
                pos += 1;
                if pos + 2 > data.len() {
                    return None;
                }
                let max_iterations = u16::from_le_bytes(data[pos..pos + 2].try_into().ok()?);
                pos += 2;
                if pos + 4 > data.len() {
                    return None;
                }
                let expansion = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                pos += 4;
                if pos + 8 > data.len() {
                    return None;
                }
                let delete_count = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                if pos + 4 > data.len() {
                    return None;
                }
                let num_layers = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut layers = Vec::with_capacity(num_layers);
                for _ in 0..num_layers {
                    if pos + 8 + 1 + 8 + 8 > data.len() {
                        return None;
                    }
                    let num_buckets =
                        u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?) as usize;
                    pos += 8;
                    let layer_bucket_size = data[pos];
                    pos += 1;
                    let count = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let capacity = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;

                    let fp_count = num_buckets * layer_bucket_size as usize;
                    let fp_bytes = fp_count * 2;
                    if pos + fp_bytes > data.len() {
                        return None;
                    }
                    let mut buckets = Vec::with_capacity(num_buckets);
                    for _ in 0..num_buckets {
                        let mut bucket = Vec::with_capacity(layer_bucket_size as usize);
                        for _ in 0..layer_bucket_size {
                            let fp =
                                u16::from_le_bytes(data[pos..pos + 2].try_into().ok()?);
                            pos += 2;
                            bucket.push(fp);
                        }
                        buckets.push(bucket);
                    }

                    layers.push(CuckooLayer::from_raw(
                        buckets,
                        num_buckets,
                        layer_bucket_size,
                        count,
                        capacity,
                    ));
                }

                Some(Value::CuckooFilter(CuckooFilterValue::from_raw(
                    layers,
                    bucket_size,
                    max_iterations,
                    expansion,
                    delete_count,
                )))
            }
            b"topk" => {
                let mut pos = 0;
                if pos + 20 > data.len() {
                    return None;
                }
                let k = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                pos += 4;
                let width = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                pos += 4;
                let depth = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                pos += 4;
                let decay = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;

                // Read buckets: depth * width * (fp:u32 + ctr:u32)
                let mut buckets = Vec::with_capacity(depth as usize);
                for _ in 0..depth {
                    let mut row = Vec::with_capacity(width as usize);
                    for _ in 0..width {
                        if pos + 8 > data.len() {
                            return None;
                        }
                        let fp = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                        pos += 4;
                        let ctr = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                        pos += 4;
                        row.push((fp, ctr));
                    }
                    buckets.push(row);
                }

                // Read heap items
                if pos + 4 > data.len() {
                    return None;
                }
                let heap_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;
                let mut heap_items = Vec::with_capacity(heap_len);
                for _ in 0..heap_len {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let item_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + item_len > data.len() {
                        return None;
                    }
                    let item = Bytes::copy_from_slice(&data[pos..pos + item_len]);
                    pos += item_len;
                    if pos + 8 > data.len() {
                        return None;
                    }
                    let count = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    heap_items.push((item, count));
                }

                Some(Value::TopK(TopKValue::from_raw(
                    k, width, depth, decay, buckets, heap_items,
                )))
            }
            b"tdigest" => {
                use crate::tdigest::Centroid;
                let mut pos = 0;
                if pos + 8 * 5 + 4 + 4 > data.len() {
                    return None;
                }
                let compression = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                let min = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                let max = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                let merged_weight = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                let unmerged_weight = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                let num_centroids =
                    u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;
                let num_unmerged =
                    u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut centroids = Vec::with_capacity(num_centroids);
                for _ in 0..num_centroids {
                    if pos + 16 > data.len() {
                        return None;
                    }
                    let mean = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let weight = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    centroids.push(Centroid { mean, weight });
                }

                let mut unmerged = Vec::with_capacity(num_unmerged);
                for _ in 0..num_unmerged {
                    if pos + 16 > data.len() {
                        return None;
                    }
                    let mean = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let weight = f64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    unmerged.push(Centroid { mean, weight });
                }

                Some(Value::TDigest(TDigestValue::from_raw(
                    compression,
                    centroids,
                    unmerged,
                    min,
                    max,
                    merged_weight,
                    unmerged_weight,
                )))
            }
            b"cms" => {
                let mut pos = 0;
                if pos + 16 > data.len() {
                    return None;
                }
                let width = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                pos += 4;
                let depth = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                pos += 4;
                let count = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;

                let mut counters = Vec::with_capacity(depth as usize);
                for _ in 0..depth {
                    let mut row = Vec::with_capacity(width as usize);
                    for _ in 0..width {
                        if pos + 8 > data.len() {
                            return None;
                        }
                        let val = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                        pos += 8;
                        row.push(val);
                    }
                    counters.push(row);
                }

                Some(Value::CountMinSketch(CountMinSketchValue::from_raw(
                    width, depth, count, counters,
                )))
            }
            _ => None,
        }
    }
}

/// String value with optional integer encoding.
#[derive(Debug, Clone)]
pub struct StringValue {
    data: StringData,
}

#[derive(Debug, Clone)]
enum StringData {
    /// Raw byte string.
    Raw(Bytes),
    /// Integer value (for efficient INCR/DECR).
    Integer(i64),
}

impl StringValue {
    /// Create a new string value from bytes.
    pub fn new(data: impl Into<Bytes>) -> Self {
        let bytes = data.into();
        // Try to parse as integer for efficient storage.
        // The round-trip check (i.to_string() == original) ensures we don't
        // lose information like leading zeros (e.g. "007" → 7 → "7").
        if let Ok(s) = std::str::from_utf8(&bytes)
            && let Ok(i) = s.parse::<i64>()
            && i.to_string().as_bytes() == bytes.as_ref()
        {
            return Self {
                data: StringData::Integer(i),
            };
        }
        Self {
            data: StringData::Raw(bytes),
        }
    }

    /// Create a string value from an integer.
    pub fn from_integer(i: i64) -> Self {
        Self {
            data: StringData::Integer(i),
        }
    }

    /// Get the value as bytes.
    pub fn as_bytes(&self) -> Bytes {
        match &self.data {
            StringData::Raw(b) => b.clone(),
            StringData::Integer(i) => Bytes::from(i.to_string()),
        }
    }

    /// Try to get the value as an integer.
    pub fn as_integer(&self) -> Option<i64> {
        match &self.data {
            StringData::Integer(i) => Some(*i),
            StringData::Raw(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        }
    }

    /// Calculate memory size.
    pub fn memory_size(&self) -> usize {
        match &self.data {
            StringData::Raw(b) => b.len(),
            StringData::Integer(_) => 8, // i64
        }
    }

    /// Get the byte length of the string.
    pub fn len(&self) -> usize {
        match &self.data {
            StringData::Raw(b) => b.len(),
            StringData::Integer(i) => i.to_string().len(),
        }
    }

    /// Check if the string is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append bytes to the string.
    ///
    /// Returns the new length of the string.
    /// Note: This converts integer-encoded values to raw bytes.
    pub fn append(&mut self, data: &[u8]) -> usize {
        let mut current = self.as_bytes().to_vec();
        current.extend_from_slice(data);
        let new_len = current.len();

        // After append, we might still have an integer but it's unlikely,
        // and appending typically makes it non-integer anyway
        *self = Self::new(Bytes::from(current));
        new_len
    }

    /// Get a substring by byte indices.
    ///
    /// Supports negative indices (like Python/Redis):
    /// - -1 is the last character
    /// - -2 is second-to-last, etc.
    pub fn get_range(&self, start: i64, end: i64) -> Bytes {
        let bytes = self.as_bytes();
        let len = bytes.len() as i64;

        if len == 0 {
            return Bytes::new();
        }

        // Convert negative indices to positive
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            (start as usize).min(len as usize)
        };

        let end = if end < 0 {
            (len + end).max(0) as usize
        } else {
            (end as usize).min(len as usize - 1)
        };

        // If start > end after conversion, return empty
        if start > end {
            return Bytes::new();
        }

        // end is inclusive in Redis GETRANGE
        bytes.slice(start..=end)
    }

    /// Overwrite part of the string at the given offset.
    ///
    /// If offset is past the end, the string is padded with null bytes.
    /// Returns the new length of the string.
    pub fn set_range(&mut self, offset: usize, data: &[u8]) -> usize {
        let mut current = self.as_bytes().to_vec();

        // Pad with zeros if offset is past end
        if offset > current.len() {
            current.resize(offset, 0);
        }

        // Replace or extend
        let end_pos = offset + data.len();
        if end_pos > current.len() {
            current.resize(end_pos, 0);
        }

        current[offset..end_pos].copy_from_slice(data);
        let new_len = current.len();

        *self = Self::new(Bytes::from(current));
        new_len
    }

    /// Increment the value by delta.
    ///
    /// Returns the new value or an error if not an integer.
    pub fn increment(&mut self, delta: i64) -> Result<i64, IncrementError> {
        let current = self.as_integer().ok_or(IncrementError::NotInteger)?;
        let new_val = current.checked_add(delta).ok_or(IncrementError::Overflow)?;
        self.data = StringData::Integer(new_val);
        Ok(new_val)
    }

    /// Increment the value by a float delta.
    ///
    /// Returns the new value or an error if not a valid float.
    pub fn increment_float(&mut self, delta: f64) -> Result<f64, IncrementError> {
        let current = self.as_float().ok_or(IncrementError::NotFloat)?;

        // Reject stored values that are already infinite or NaN (e.g. "inf", "nan")
        if current.is_infinite() || current.is_nan() {
            return Err(IncrementError::NotFloat);
        }

        let new_val = current + delta;

        // Check for infinity or NaN result
        if new_val.is_infinite() || new_val.is_nan() {
            return Err(IncrementError::Overflow);
        }

        // Store as raw string (floats are always stored as strings in Redis)
        self.data = StringData::Raw(Bytes::from(format_float(new_val)));
        Ok(new_val)
    }

    /// Try to get the value as a float.
    pub fn as_float(&self) -> Option<f64> {
        match &self.data {
            StringData::Integer(i) => Some(*i as f64),
            StringData::Raw(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        }
    }

    // ========================================================================
    // Bitmap operations
    // ========================================================================

    /// Get a bit value at the given offset.
    ///
    /// Uses MSB-first bit ordering (offset 0 = bit 7 of byte 0).
    /// Returns 0 for offsets beyond the string length.
    pub fn getbit(&self, offset: u64) -> u8 {
        let bytes = self.as_bytes();
        crate::bitmap::getbit(&bytes, offset)
    }

    /// Set a bit value at the given offset.
    ///
    /// Returns the previous bit value.
    /// Auto-extends the string with zeros if offset is beyond the end.
    pub fn setbit(&mut self, offset: u64, value: u8) -> u8 {
        let mut bytes = self.as_bytes().to_vec();
        let old_bit = crate::bitmap::setbit(&mut bytes, offset, value);
        self.data = StringData::Raw(Bytes::from(bytes));
        old_bit
    }

    /// Count the number of set bits in a range.
    ///
    /// If bit_mode is true, start/end are bit positions; otherwise byte positions.
    pub fn bitcount(&self, start: Option<i64>, end: Option<i64>, bit_mode: bool) -> u64 {
        let bytes = self.as_bytes();
        crate::bitmap::bitcount(&bytes, start, end, bit_mode)
    }

    /// Find the position of the first bit set to the given value.
    pub fn bitpos(
        &self,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        bit_mode: bool,
        end_given: bool,
    ) -> Option<i64> {
        let bytes = self.as_bytes();
        crate::bitmap::bitpos(&bytes, bit, start, end, bit_mode, end_given)
    }

    /// Get the raw bytes as a mutable vector for bitfield operations.
    pub fn as_bytes_vec(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    /// Set the raw bytes from a vector (used after bitfield operations).
    pub fn set_bytes(&mut self, bytes: Vec<u8>) {
        self.data = StringData::Raw(Bytes::from(bytes));
    }
}

/// Error type for increment operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementError {
    /// Value is not an integer.
    NotInteger,
    /// Value is not a valid float.
    NotFloat,
    /// Operation would overflow.
    Overflow,
}

impl std::fmt::Display for IncrementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncrementError::NotInteger => write!(f, "ERR value is not an integer or out of range"),
            IncrementError::NotFloat => write!(f, "ERR value is not a valid float"),
            IncrementError::Overflow => write!(f, "ERR increment or decrement would overflow"),
        }
    }
}

impl std::error::Error for IncrementError {}

/// Format a float for Redis compatibility.
///
/// Redis uses specific formatting rules:
/// - No trailing zeros after decimal point
/// - Scientific notation for very large/small numbers
fn format_float(f: f64) -> String {
    // Handle special cases
    if f == 0.0 {
        return "0".to_string();
    }

    // Check if it's a whole number
    if f.fract() == 0.0 && f.abs() < 1e15 {
        return format!("{:.0}", f);
    }

    // Use standard formatting, then trim trailing zeros
    let s = format!("{:.17}", f);
    let s = s.trim_end_matches('0');
    let s = s.trim_end_matches('.');
    s.to_string()
}

/// Type identifier for TYPE command and WRONGTYPE errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    /// Key doesn't exist.
    None,
    /// String type.
    String,
    /// List type.
    List,
    /// Set type.
    Set,
    /// Hash type.
    Hash,
    /// Sorted set type.
    SortedSet,
    /// Stream type.
    Stream,
    /// Bloom filter type.
    BloomFilter,
    /// HyperLogLog type.
    HyperLogLog,
    /// Time series type.
    TimeSeries,
    /// JSON type.
    Json,
    /// Cuckoo filter type.
    CuckooFilter,
    /// Top-K type.
    TopK,
    /// T-Digest type.
    TDigest,
    /// Count-Min Sketch type.
    CountMinSketch,
}

impl KeyType {
    /// Get the Redis type name.
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyType::None => "none",
            KeyType::String => "string",
            KeyType::List => "list",
            KeyType::Set => "set",
            KeyType::Hash => "hash",
            KeyType::SortedSet => "zset",
            KeyType::Stream => "stream",
            KeyType::BloomFilter => "bloom",
            KeyType::HyperLogLog => "hyperloglog",
            KeyType::TimeSeries => "TSDB-TYPE",
            KeyType::Json => "ReJSON-RL",
            KeyType::CuckooFilter => "cuckoo",
            KeyType::TopK => "topk",
            KeyType::TDigest => "tdigest",
            KeyType::CountMinSketch => "cms",
        }
    }
}

/// Direction for list operations (BLPOP, BRPOP, BLMOVE, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Pop/push from the left (front).
    Left,
    /// Pop/push from the right (back).
    Right,
}

impl Direction {
    /// Parse direction from bytes (LEFT or RIGHT).
    pub fn parse(arg: &[u8]) -> Option<Self> {
        match arg.to_ascii_uppercase().as_slice() {
            b"LEFT" => Some(Direction::Left),
            b"RIGHT" => Some(Direction::Right),
            _ => None,
        }
    }
}

/// Blocking operation type for wait queue entries.
#[derive(Debug, Clone)]
pub enum BlockingOp {
    /// BLPOP operation.
    BLPop,
    /// BRPOP operation.
    BRPop,
    /// BLMOVE operation.
    BLMove {
        /// Destination key.
        dest: Bytes,
        /// Source direction (where to pop from).
        src_dir: Direction,
        /// Destination direction (where to push to).
        dest_dir: Direction,
    },
    /// BLMPOP operation.
    BLMPop {
        /// Pop direction.
        direction: Direction,
        /// Number of elements to pop.
        count: usize,
    },
    /// BZPOPMIN operation.
    BZPopMin,
    /// BZPOPMAX operation.
    BZPopMax,
    /// BZMPOP operation.
    BZMPop {
        /// Whether to pop minimum (true) or maximum (false).
        min: bool,
        /// Number of elements to pop.
        count: usize,
    },
    /// XREAD blocking operation.
    XRead {
        /// Stream IDs to read after (resolved from $ at block time).
        after_ids: Vec<StreamId>,
        /// Maximum entries per stream.
        count: Option<usize>,
    },
    /// XREADGROUP blocking operation.
    XReadGroup {
        /// Consumer group name.
        group: Bytes,
        /// Consumer name.
        consumer: Bytes,
        /// Skip PEL updates (NOACK flag).
        noack: bool,
        /// Maximum entries to return.
        count: Option<usize>,
    },
    /// WAIT command - wait for replica acknowledgments.
    Wait {
        /// Number of replicas that must acknowledge.
        num_replicas: u32,
        /// Timeout in milliseconds (0 = block forever).
        timeout_ms: u64,
    },
}

/// Metadata tracked per key.
#[derive(Debug, Clone)]
pub struct KeyMetadata {
    /// Expiration time (None = no expiry).
    pub expires_at: Option<Instant>,

    /// Last access time (for LRU eviction).
    pub last_access: Instant,

    /// Access frequency counter (for LFU eviction).
    pub lfu_counter: u8,

    /// Approximate memory size of this entry.
    pub memory_size: usize,
}

impl KeyMetadata {
    /// Create new metadata with defaults.
    pub fn new(memory_size: usize) -> Self {
        Self {
            expires_at: None,
            last_access: Instant::now(),
            lfu_counter: 5, // New keys start at 5 (not immediately evicted)
            memory_size,
        }
    }

    /// Check if the key is expired.
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|exp| exp <= Instant::now())
            .unwrap_or(false)
    }

    /// Update last access time.
    pub fn touch(&mut self) {
        self.last_access = Instant::now();
    }
}

// ============================================================================
// SET Command Options
// ============================================================================

/// Options for the SET command.
#[derive(Debug, Clone, Default)]
pub struct SetOptions {
    /// Expiration setting.
    pub expiry: Option<Expiry>,
    /// Conditional set (NX/XX).
    pub condition: SetCondition,
    /// Keep existing TTL when updating.
    pub keep_ttl: bool,
    /// Return the old value (GET flag).
    pub return_old: bool,
}

/// Expiration specification for SET command.
#[derive(Debug, Clone, Copy)]
pub enum Expiry {
    /// Expire in N seconds (EX).
    Ex(u64),
    /// Expire in N milliseconds (PX).
    Px(u64),
    /// Expire at Unix timestamp in seconds (EXAT).
    ExAt(u64),
    /// Expire at Unix timestamp in milliseconds (PXAT).
    PxAt(u64),
}

impl Expiry {
    /// Convert to an absolute Instant for storage.
    pub fn to_instant(&self) -> Option<Instant> {
        let now = Instant::now();
        let system_now = SystemTime::now();

        match self {
            Expiry::Ex(secs) => Some(now + Duration::from_secs(*secs)),
            Expiry::Px(ms) => Some(now + Duration::from_millis(*ms)),
            Expiry::ExAt(ts) => {
                let target = UNIX_EPOCH + Duration::from_secs(*ts);
                if let Ok(duration) = target.duration_since(system_now) {
                    Some(now + duration)
                } else {
                    // Already expired
                    Some(now)
                }
            }
            Expiry::PxAt(ts) => {
                let target = UNIX_EPOCH + Duration::from_millis(*ts);
                if let Ok(duration) = target.duration_since(system_now) {
                    Some(now + duration)
                } else {
                    // Already expired
                    Some(now)
                }
            }
        }
    }
}

/// Conditional set behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SetCondition {
    /// Always set (default behavior).
    #[default]
    Always,
    /// Only set if key does Not eXist.
    NX,
    /// Only set if key already eXists.
    XX,
}

/// Result of a SET operation.
#[derive(Debug, Clone)]
pub enum SetResult {
    /// Set was successful, returns OK.
    Ok,
    /// Set was successful, returns the old value (for GET flag).
    OkWithOldValue(Option<Value>),
    /// Set was not performed (NX/XX condition not met).
    NotSet,
}

// ============================================================================
// Sorted Set Types
// ============================================================================

/// Score boundary for range queries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScoreBound {
    /// Inclusive bound.
    Inclusive(f64),
    /// Exclusive bound.
    Exclusive(f64),
    /// Negative infinity.
    NegInf,
    /// Positive infinity.
    PosInf,
}

impl ScoreBound {
    /// Check if a score satisfies this bound as a minimum.
    pub fn satisfies_min(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => true,
            ScoreBound::PosInf => false,
            ScoreBound::Inclusive(bound) => score >= *bound,
            ScoreBound::Exclusive(bound) => score > *bound,
        }
    }

    /// Check if a score satisfies this bound as a maximum.
    pub fn satisfies_max(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => false,
            ScoreBound::PosInf => true,
            ScoreBound::Inclusive(bound) => score <= *bound,
            ScoreBound::Exclusive(bound) => score < *bound,
        }
    }

    /// Get the value for BTreeMap range queries (minimum bound).
    pub fn start_bound_value(&self) -> Option<OrderedFloat<f64>> {
        match self {
            ScoreBound::NegInf => None,
            ScoreBound::PosInf => Some(OrderedFloat(f64::INFINITY)),
            ScoreBound::Inclusive(v) | ScoreBound::Exclusive(v) => Some(OrderedFloat(*v)),
        }
    }

    /// Convert to a BTreeMap lower bound for `(OrderedFloat<f64>, Bytes)` keys.
    fn to_btree_lower(self) -> std::ops::Bound<(OrderedFloat<f64>, Bytes)> {
        match self {
            ScoreBound::NegInf => std::ops::Bound::Unbounded,
            ScoreBound::PosInf => {
                std::ops::Bound::Excluded((OrderedFloat(f64::INFINITY), Bytes::new()))
            }
            ScoreBound::Inclusive(v) => std::ops::Bound::Included((OrderedFloat(v), Bytes::new())),
            ScoreBound::Exclusive(v) => {
                // No floats between v and next_up, so Included(next_up) excludes v
                std::ops::Bound::Included((OrderedFloat(v.next_up()), Bytes::new()))
            }
        }
    }

    /// Convert to a BTreeMap upper bound for `(OrderedFloat<f64>, Bytes)` keys.
    fn to_btree_upper(self) -> std::ops::Bound<(OrderedFloat<f64>, Bytes)> {
        match self {
            ScoreBound::PosInf => std::ops::Bound::Unbounded,
            ScoreBound::NegInf => {
                std::ops::Bound::Excluded((OrderedFloat(f64::NEG_INFINITY), Bytes::new()))
            }
            ScoreBound::Inclusive(v) => {
                let next = v.next_up();
                if next.is_infinite() && !v.is_infinite() {
                    // v is f64::MAX, next_up would be INFINITY — use Unbounded
                    std::ops::Bound::Unbounded
                } else {
                    // Excluded(next_up, empty) — empty Bytes sorts first, so all entries at score v are included
                    std::ops::Bound::Excluded((OrderedFloat(next), Bytes::new()))
                }
            }
            ScoreBound::Exclusive(v) => {
                // Excluded(v, empty) — empty Bytes sorts first, so all entries at score v are excluded
                std::ops::Bound::Excluded((OrderedFloat(v), Bytes::new()))
            }
        }
    }
}

/// Lexicographic boundary for range queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexBound {
    /// Inclusive bound.
    Inclusive(Bytes),
    /// Exclusive bound.
    Exclusive(Bytes),
    /// Minimum (unbounded).
    Min,
    /// Maximum (unbounded).
    Max,
}

impl LexBound {
    /// Check if a member satisfies this bound as a minimum.
    pub fn satisfies_min(&self, member: &[u8]) -> bool {
        match self {
            LexBound::Min => true,
            LexBound::Max => false,
            LexBound::Inclusive(bound) => member >= bound.as_ref(),
            LexBound::Exclusive(bound) => member > bound.as_ref(),
        }
    }

    /// Check if a member satisfies this bound as a maximum.
    pub fn satisfies_max(&self, member: &[u8]) -> bool {
        match self {
            LexBound::Min => false,
            LexBound::Max => true,
            LexBound::Inclusive(bound) => member <= bound.as_ref(),
            LexBound::Exclusive(bound) => member < bound.as_ref(),
        }
    }
}

/// Result of adding a member to a sorted set.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZAddResult {
    /// Whether a new member was added.
    pub added: bool,
    /// Whether the score was changed (for existing members).
    pub changed: bool,
    /// The previous score (if member existed).
    pub old_score: Option<f64>,
}

// ============================================================================
// ScoreIndex: pluggable backend for sorted set score ordering
// ============================================================================

static SCORE_INDEX_BACKEND: AtomicU8 = AtomicU8::new(1); // 0=BTree, 1=SkipList (default)

/// Which index backend new sorted sets should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScoreIndexBackend {
    BTree = 0,
    SkipList = 1,
}

/// Set the default score index backend (called once at startup).
pub fn set_default_score_index(backend: ScoreIndexBackend) {
    SCORE_INDEX_BACKEND.store(backend as u8, AtomicOrdering::Relaxed);
}

fn default_score_index_backend() -> ScoreIndexBackend {
    match SCORE_INDEX_BACKEND.load(AtomicOrdering::Relaxed) {
        0 => ScoreIndexBackend::BTree,
        _ => ScoreIndexBackend::SkipList,
    }
}

/// Score index that dispatches to either BTreeMap or SkipList.
#[derive(Debug, Clone)]
enum ScoreIndex {
    BTree(BTreeMap<(OrderedFloat<f64>, Bytes), ()>),
    SkipList(SkipList),
}

impl ScoreIndex {
    fn new(backend: ScoreIndexBackend) -> Self {
        match backend {
            ScoreIndexBackend::BTree => ScoreIndex::BTree(BTreeMap::new()),
            ScoreIndexBackend::SkipList => ScoreIndex::SkipList(SkipList::new()),
        }
    }

    fn insert(&mut self, score: OrderedFloat<f64>, member: Bytes) {
        match self {
            ScoreIndex::BTree(bt) => {
                bt.insert((score, member), ());
            }
            ScoreIndex::SkipList(sl) => {
                sl.insert(score, member);
            }
        }
    }

    fn remove(&mut self, score: OrderedFloat<f64>, member: &Bytes) {
        match self {
            ScoreIndex::BTree(bt) => {
                bt.remove(&(score, member.clone()));
            }
            ScoreIndex::SkipList(sl) => {
                sl.remove(score, member);
            }
        }
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        match self {
            ScoreIndex::BTree(bt) => bt.len(),
            ScoreIndex::SkipList(sl) => sl.len(),
        }
    }

    fn rank(&self, score: OrderedFloat<f64>, member: &Bytes) -> Option<usize> {
        match self {
            ScoreIndex::BTree(bt) => {
                let key = (score, member.clone());
                Some(bt.range(..&key).count())
            }
            ScoreIndex::SkipList(sl) => sl.rank(score, member),
        }
    }

    fn pop_first(&mut self) -> Option<(OrderedFloat<f64>, Bytes)> {
        match self {
            ScoreIndex::BTree(bt) => bt.pop_first().map(|((s, m), _)| (s, m)),
            ScoreIndex::SkipList(sl) => sl.pop_first(),
        }
    }

    fn pop_last(&mut self) -> Option<(OrderedFloat<f64>, Bytes)> {
        match self {
            ScoreIndex::BTree(bt) => bt.pop_last().map(|((s, m), _)| (s, m)),
            ScoreIndex::SkipList(sl) => sl.pop_last(),
        }
    }

    fn iter(&self) -> ScoreIndexIter<'_> {
        match self {
            ScoreIndex::BTree(bt) => ScoreIndexIter::BTree(bt.iter()),
            ScoreIndex::SkipList(sl) => ScoreIndexIter::SkipList(sl.iter()),
        }
    }

    fn rev_iter(&self) -> ScoreIndexRevIter<'_> {
        match self {
            ScoreIndex::BTree(bt) => ScoreIndexRevIter::BTree(bt.iter().rev()),
            ScoreIndex::SkipList(sl) => ScoreIndexRevIter::SkipList(sl.rev_iter()),
        }
    }

    fn range_by_score_iter<'a>(&'a self, min: &ScoreBound, max: &ScoreBound) -> ScoreIndexIter<'a> {
        match self {
            ScoreIndex::BTree(bt) => {
                ScoreIndexIter::BTreeRange(bt.range((min.to_btree_lower(), max.to_btree_upper())))
            }
            ScoreIndex::SkipList(sl) => {
                let (min_score, min_inclusive) = match min {
                    ScoreBound::NegInf => (OrderedFloat(f64::NEG_INFINITY), true),
                    ScoreBound::PosInf => {
                        return ScoreIndexIter::Empty;
                    }
                    ScoreBound::Inclusive(v) => (OrderedFloat(*v), true),
                    ScoreBound::Exclusive(v) => (OrderedFloat(*v), false),
                };
                let max_score = match max {
                    ScoreBound::PosInf => None,
                    ScoreBound::NegInf => return ScoreIndexIter::Empty,
                    ScoreBound::Inclusive(v) => Some((OrderedFloat(*v), true)),
                    ScoreBound::Exclusive(v) => Some((OrderedFloat(*v), false)),
                };
                ScoreIndexIter::SkipListBounded {
                    inner: sl.range_by_score(min_score, min_inclusive),
                    max_score,
                }
            }
        }
    }

    fn rev_range_by_score_iter<'a>(
        &'a self,
        min: &ScoreBound,
        max: &ScoreBound,
    ) -> ScoreIndexRevIter<'a> {
        match self {
            ScoreIndex::BTree(bt) => ScoreIndexRevIter::BTreeRange(
                bt.range((min.to_btree_lower(), max.to_btree_upper())).rev(),
            ),
            ScoreIndex::SkipList(_) => {
                // For reverse score range on skip list, we use the forward range
                // then collect and reverse. This matches the BTreeMap approach.
                // A more optimal approach would be a reverse score-bounded iterator,
                // but for now this is correct and matches BTreeMap's complexity.
                ScoreIndexRevIter::Collected(
                    self.range_by_score_iter(min, max)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            }
        }
    }

    fn range_by_rank_iter(&self, start: usize, count: usize) -> ScoreIndexIter<'_> {
        match self {
            ScoreIndex::BTree(bt) => ScoreIndexIter::BTreeSkip {
                inner: bt.iter(),
                skip: start,
                remaining: count,
            },
            ScoreIndex::SkipList(sl) => ScoreIndexIter::SkipListTake {
                inner: sl.range_by_rank_iter(start),
                remaining: count,
            },
        }
    }

    fn memory_size(&self) -> usize {
        match self {
            ScoreIndex::BTree(bt) => bt
                .iter()
                .map(|((_, member), _)| {
                    member.len() + std::mem::size_of::<OrderedFloat<f64>>() + 32
                })
                .sum(),
            ScoreIndex::SkipList(sl) => sl.memory_size(),
        }
    }
}

/// Forward iterator over ScoreIndex entries.
enum ScoreIndexIter<'a> {
    BTree(std::collections::btree_map::Iter<'a, (OrderedFloat<f64>, Bytes), ()>),
    BTreeRange(std::collections::btree_map::Range<'a, (OrderedFloat<f64>, Bytes), ()>),
    BTreeSkip {
        inner: std::collections::btree_map::Iter<'a, (OrderedFloat<f64>, Bytes), ()>,
        skip: usize,
        remaining: usize,
    },
    SkipList(crate::skiplist::SkipListIter<'a>),
    SkipListBounded {
        inner: crate::skiplist::SkipListIter<'a>,
        max_score: Option<(OrderedFloat<f64>, bool)>, // (score, inclusive)
    },
    SkipListTake {
        inner: crate::skiplist::SkipListIter<'a>,
        remaining: usize,
    },
    Empty,
}

impl<'a> Iterator for ScoreIndexIter<'a> {
    type Item = (OrderedFloat<f64>, &'a Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ScoreIndexIter::BTree(it) => it.next().map(|((s, m), _)| (*s, m)),
            ScoreIndexIter::BTreeRange(it) => it.next().map(|((s, m), _)| (*s, m)),
            ScoreIndexIter::BTreeSkip {
                inner,
                skip,
                remaining,
            } => {
                while *skip > 0 {
                    inner.next()?;
                    *skip -= 1;
                }
                if *remaining == 0 {
                    return None;
                }
                *remaining -= 1;
                inner.next().map(|((s, m), _)| (*s, m))
            }
            ScoreIndexIter::SkipList(it) => it.next(),
            ScoreIndexIter::SkipListBounded { inner, max_score } => {
                let (score, member) = inner.next()?;
                if let Some((max_s, inclusive)) = max_score {
                    if *inclusive {
                        if score > *max_s {
                            return None;
                        }
                    } else if score >= *max_s {
                        return None;
                    }
                }
                Some((score, member))
            }
            ScoreIndexIter::SkipListTake { inner, remaining } => {
                if *remaining == 0 {
                    return None;
                }
                *remaining -= 1;
                inner.next()
            }
            ScoreIndexIter::Empty => None,
        }
    }
}

/// Reverse iterator over ScoreIndex entries.
enum ScoreIndexRevIter<'a> {
    BTree(std::iter::Rev<std::collections::btree_map::Iter<'a, (OrderedFloat<f64>, Bytes), ()>>),
    BTreeRange(
        std::iter::Rev<std::collections::btree_map::Range<'a, (OrderedFloat<f64>, Bytes), ()>>,
    ),
    SkipList(crate::skiplist::SkipListRevIter<'a>),
    Collected(std::vec::IntoIter<(OrderedFloat<f64>, &'a Bytes)>),
}

impl<'a> Iterator for ScoreIndexRevIter<'a> {
    type Item = (OrderedFloat<f64>, &'a Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ScoreIndexRevIter::BTree(it) => it.next().map(|((s, m), _)| (*s, m)),
            ScoreIndexRevIter::BTreeRange(it) => it.next().map(|((s, m), _)| (*s, m)),
            ScoreIndexRevIter::SkipList(it) => it.next(),
            ScoreIndexRevIter::Collected(it) => it.next(),
        }
    }
}

/// Sorted set value with dual indexing for O(1) score lookup and O(log n) range queries.
#[derive(Debug, Clone)]
pub struct SortedSetValue {
    /// O(1) lookup: member -> score
    members: HashMap<Bytes, f64>,
    /// Ordered index for range queries (BTreeMap or SkipList).
    index: ScoreIndex,
}

impl Default for SortedSetValue {
    fn default() -> Self {
        Self::new()
    }
}

impl SortedSetValue {
    /// Create a new empty sorted set using the configured default backend.
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            index: ScoreIndex::new(default_score_index_backend()),
        }
    }

    /// Create a new empty sorted set with a specific backend.
    pub fn with_backend(backend: ScoreIndexBackend) -> Self {
        Self {
            members: HashMap::new(),
            index: ScoreIndex::new(backend),
        }
    }

    /// Get the number of members.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Add or update a member with a score.
    ///
    /// Returns information about what changed.
    pub fn add(&mut self, member: Bytes, score: f64) -> ZAddResult {
        if let Some(&old_score) = self.members.get(&member) {
            if (old_score - score).abs() < f64::EPSILON
                || (old_score.is_nan() && score.is_nan())
                || (old_score == score)
            {
                // Score unchanged
                return ZAddResult {
                    added: false,
                    changed: false,
                    old_score: Some(old_score),
                };
            }
            // Remove old entry from index, insert new entry
            self.index.remove(OrderedFloat(old_score), &member);
            self.index.insert(OrderedFloat(score), member.clone());
            self.members.insert(member, score);
            ZAddResult {
                added: false,
                changed: true,
                old_score: Some(old_score),
            }
        } else {
            // New member: insert into index first (needs clone), then move into members
            self.index.insert(OrderedFloat(score), member.clone());
            self.members.insert(member, score);
            ZAddResult {
                added: true,
                changed: false,
                old_score: None,
            }
        }
    }

    /// Remove a member from the set.
    ///
    /// Returns the score if the member existed.
    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        if let Some((member_key, score)) = self.members.remove_entry(member) {
            self.index.remove(OrderedFloat(score), &member_key);
            Some(score)
        } else {
            None
        }
    }

    /// Get the score of a member.
    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        self.members.get(member).copied()
    }

    /// Check if a member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        self.members.contains_key(member)
    }

    /// Get the 0-based rank of a member (ascending by score).
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let (member_key, &score) = self.members.get_key_value(member)?;
        self.index.rank(OrderedFloat(score), member_key)
    }

    /// Get the 0-based rank of a member (descending by score).
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    /// Increment the score of a member.
    ///
    /// If the member doesn't exist, it's created with the given increment as its score.
    /// Returns the new score.
    pub fn incr(&mut self, member: Bytes, increment: f64) -> f64 {
        let existing = self.members.get(&member).copied();
        let old_score = existing.unwrap_or(0.0);
        let new_score = old_score + increment;

        // Check for overflow to infinity
        if new_score.is_infinite() && !old_score.is_infinite() && !increment.is_infinite() {
            // This would be an error in Redis, but we'll handle it
            return new_score;
        }

        if existing.is_some() {
            self.index.remove(OrderedFloat(old_score), &member);
            self.index.insert(OrderedFloat(new_score), member.clone());
            self.members.insert(member, new_score);
        } else {
            self.members.insert(member.clone(), new_score);
            self.index.insert(OrderedFloat(new_score), member);
        }

        new_score
    }

    /// Get members by rank range (inclusive).
    ///
    /// `start` and `end` are 0-based indices. Negative indices count from the end.
    pub fn range_by_rank(&self, start: i64, end: i64) -> Vec<(Bytes, f64)> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            return vec![];
        }

        let end = end as usize;

        self.index
            .range_by_rank_iter(start, end - start + 1)
            .map(|(score, member)| (member.clone(), score.0))
            .collect()
    }

    /// Get members by rank range in reverse order (descending by score).
    pub fn rev_range_by_rank(&self, start: i64, end: i64) -> Vec<(Bytes, f64)> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            return vec![];
        }

        let end = end as usize;

        self.index
            .rev_iter()
            .skip(start)
            .take(end - start + 1)
            .map(|(score, member)| (member.clone(), score.0))
            .collect()
    }

    /// Get members by score range.
    pub fn range_by_score(
        &self,
        min: &ScoreBound,
        max: &ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self.index.range_by_score_iter(min, max).skip(offset);

        if let Some(count) = count {
            iter.take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|(score, member)| (member.clone(), score.0))
                .collect()
        }
    }

    /// Get members by score range in reverse order.
    pub fn rev_range_by_score(
        &self,
        min: &ScoreBound,
        max: &ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self.index.rev_range_by_score_iter(min, max).skip(offset);

        if let Some(count) = count {
            iter.take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|(score, member)| (member.clone(), score.0))
                .collect()
        }
    }

    /// Get members by lexicographic range (requires all scores to be equal).
    pub fn range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        // For lex range, we iterate in (score, member) order
        // This naturally gives us lexicographic order for same scores
        let iter = self
            .index
            .iter()
            .filter(|(_, member)| min.satisfies_min(member) && max.satisfies_max(member))
            .skip(offset);

        if let Some(count) = count {
            iter.take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|(score, member)| (member.clone(), score.0))
                .collect()
        }
    }

    /// Get members by lexicographic range in reverse order.
    pub fn rev_range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self
            .index
            .rev_iter()
            .filter(|(_, member)| min.satisfies_min(member) && max.satisfies_max(member))
            .skip(offset);

        if let Some(count) = count {
            iter.take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|(score, member)| (member.clone(), score.0))
                .collect()
        }
    }

    /// Count members in score range.
    pub fn count_by_score(&self, min: &ScoreBound, max: &ScoreBound) -> usize {
        self.index.range_by_score_iter(min, max).count()
    }

    /// Count members in lex range.
    pub fn count_by_lex(&self, min: &LexBound, max: &LexBound) -> usize {
        self.index
            .iter()
            .filter(|(_, member)| min.satisfies_min(member) && max.satisfies_max(member))
            .count()
    }

    /// Pop members with minimum scores.
    pub fn pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count.min(self.len()));
        for _ in 0..count {
            if let Some((score, member)) = self.index.pop_first() {
                self.members.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }
        result
    }

    /// Pop members with maximum scores.
    pub fn pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count.min(self.len()));
        for _ in 0..count {
            if let Some((score, member)) = self.index.pop_last() {
                self.members.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }
        result
    }

    /// Remove members by rank range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_rank(&mut self, start: i64, end: i64) -> usize {
        let len = self.len() as i64;
        if len == 0 {
            return 0;
        }

        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            return 0;
        }

        let end = end as usize;

        let to_remove: Vec<_> = self
            .index
            .range_by_rank_iter(start, end - start + 1)
            .map(|(score, member)| (score, member.clone()))
            .collect();

        let count = to_remove.len();
        for (score, member) in to_remove {
            self.index.remove(score, &member);
            self.members.remove(&member);
        }
        count
    }

    /// Remove members by score range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_score(&mut self, min: &ScoreBound, max: &ScoreBound) -> usize {
        let to_remove: Vec<_> = self
            .index
            .range_by_score_iter(min, max)
            .map(|(score, member)| (score, member.clone()))
            .collect();

        let count = to_remove.len();
        for (score, member) in to_remove {
            self.index.remove(score, &member);
            self.members.remove(&member);
        }
        count
    }

    /// Remove members by lex range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_lex(&mut self, min: &LexBound, max: &LexBound) -> usize {
        let to_remove: Vec<_> = self
            .index
            .iter()
            .filter(|(_, member)| min.satisfies_min(member) && max.satisfies_max(member))
            .map(|(score, member)| (score, member.clone()))
            .collect();

        let count = to_remove.len();
        for (score, member) in to_remove {
            self.index.remove(score, &member);
            self.members.remove(&member);
        }
        count
    }

    /// Get random members.
    ///
    /// If `count` is positive, returns that many unique members.
    /// If `count` is negative, returns abs(count) members with possible duplicates.
    pub fn random_members(&self, count: i64) -> Vec<(Bytes, f64)> {
        if count == 0 || self.is_empty() {
            return vec![];
        }

        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();

        if count > 0 {
            // Return unique members (no duplicates), up to self.len()
            let count = (count as usize).min(self.len());
            self.index
                .iter()
                .choose_multiple(&mut rng, count)
                .into_iter()
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            // Allow duplicates: pick randomly with replacement
            use rand::Rng;
            let members: Vec<_> = self
                .index
                .iter()
                .map(|(score, member)| (member, score.0))
                .collect();
            let n = (-count) as usize;
            let mut result = Vec::with_capacity(n);
            for _ in 0..n {
                let idx = rng.gen_range(0..members.len());
                let (member, score) = members[idx];
                result.push((member.clone(), score));
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();

        // HashMap overhead + entries
        let members_size: usize = self
            .members
            .keys()
            .map(|k| k.len() + std::mem::size_of::<f64>() + 32) // 32 for HashMap node overhead
            .sum();

        let index_size = self.index.memory_size();

        base_size + members_size + index_size
    }

    /// Iterate over all members in score order.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, f64)> + '_ {
        self.index.iter().map(|(score, member)| (member, score.0))
    }

    /// Get all members and scores as a vec for serialization.
    pub fn to_vec(&self) -> Vec<(Bytes, f64)> {
        self.index
            .iter()
            .map(|(score, member)| (member.clone(), score.0))
            .collect()
    }
}

// ============================================================================
// Listpack Thresholds
// ============================================================================

/// Thresholds for listpack encoding. When exceeded, the encoding is promoted
/// from listpack to the standard hash table / hash set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListpackThresholds {
    /// Maximum number of entries before promotion.
    pub max_entries: usize,
    /// Maximum byte length of any single field or value before promotion.
    pub max_value_bytes: usize,
}

impl ListpackThresholds {
    /// Default thresholds for hash values (matches Redis defaults).
    pub const DEFAULT_HASH: Self = Self {
        max_entries: 128,
        max_value_bytes: 64,
    };
    /// Default thresholds for set values (matches Redis defaults).
    pub const DEFAULT_SET: Self = Self {
        max_entries: 128,
        max_value_bytes: 64,
    };
}

impl Default for ListpackThresholds {
    fn default() -> Self {
        Self::DEFAULT_HASH
    }
}

// ============================================================================
// Either Iterator (local utility)
// ============================================================================

/// Simple either type for dispatching between two iterator implementations.
enum EitherIter<L, R> {
    Left(L),
    Right(R),
}

impl<L, R, Item> Iterator for EitherIter<L, R>
where
    L: Iterator<Item = Item>,
    R: Iterator<Item = Item>,
{
    type Item = Item;

    #[inline]
    fn next(&mut self) -> Option<Item> {
        match self {
            EitherIter::Left(l) => l.next(),
            EitherIter::Right(r) => r.next(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            EitherIter::Left(l) => l.size_hint(),
            EitherIter::Right(r) => r.size_hint(),
        }
    }
}

// ============================================================================
// Hash Listpack Helpers
// ============================================================================

/// Iterator over field-value pairs in a hash listpack buffer.
/// Layout: [flen:u16][field_bytes][vlen:u16][value_bytes]... repeated.
struct ListpackHashIter<'a> {
    buf: &'a Bytes,
    pos: usize,
}

impl<'a> ListpackHashIter<'a> {
    fn new(buf: &'a Bytes) -> Self {
        Self { buf, pos: 0 }
    }
}

impl Iterator for ListpackHashIter<'_> {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<(Bytes, Bytes)> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let flen = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]) as usize;
        self.pos += 2;
        let field = self.buf.slice(self.pos..self.pos + flen);
        self.pos += flen;
        let vlen = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]) as usize;
        self.pos += 2;
        let value = self.buf.slice(self.pos..self.pos + vlen);
        self.pos += vlen;
        Some((field, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

/// Find a field in a hash listpack buffer, returning its value as a zero-copy slice.
fn lp_hash_get(buf: &Bytes, field: &[u8]) -> Option<Bytes> {
    let mut pos = 0;
    while pos < buf.len() {
        let flen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        let matches = &buf[pos..pos + flen] == field;
        pos += flen;
        let vlen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        if matches {
            return Some(buf.slice(pos..pos + vlen));
        }
        pos += vlen;
    }
    None
}

/// Check if a field exists in a hash listpack buffer.
fn lp_hash_contains(buf: &[u8], field: &[u8]) -> bool {
    let mut pos = 0;
    while pos < buf.len() {
        let flen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        if &buf[pos..pos + flen] == field {
            return true;
        }
        pos += flen;
        let vlen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2 + vlen;
    }
    false
}

/// Rebuild a hash listpack buffer with a field set or updated.
/// Returns (new_buf, new_len, was_new_field).
fn lp_hash_set(old_buf: &[u8], old_len: u16, field: &[u8], value: &[u8]) -> (Bytes, u16, bool) {
    let mut new_buf = BytesMut::with_capacity(old_buf.len() + 2 + field.len() + 2 + value.len());
    let mut pos = 0;
    let mut found = false;

    while pos < old_buf.len() {
        let flen = u16::from_le_bytes([old_buf[pos], old_buf[pos + 1]]) as usize;
        pos += 2;
        let existing_field = &old_buf[pos..pos + flen];
        pos += flen;
        let vlen = u16::from_le_bytes([old_buf[pos], old_buf[pos + 1]]) as usize;
        pos += 2;

        if existing_field == field {
            new_buf.extend_from_slice(&(flen as u16).to_le_bytes());
            new_buf.extend_from_slice(existing_field);
            new_buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
            new_buf.extend_from_slice(value);
            found = true;
        } else {
            new_buf.extend_from_slice(&(flen as u16).to_le_bytes());
            new_buf.extend_from_slice(existing_field);
            new_buf.extend_from_slice(&(vlen as u16).to_le_bytes());
            new_buf.extend_from_slice(&old_buf[pos..pos + vlen]);
        }
        pos += vlen;
    }

    if !found {
        new_buf.extend_from_slice(&(field.len() as u16).to_le_bytes());
        new_buf.extend_from_slice(field);
        new_buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
        new_buf.extend_from_slice(value);
    }

    let new_len = if found { old_len } else { old_len + 1 };
    (new_buf.freeze(), new_len, !found)
}

/// Rebuild a hash listpack buffer with a field removed.
/// Returns (new_buf, new_len, was_removed).
fn lp_hash_remove(old_buf: &[u8], old_len: u16, field: &[u8]) -> (Bytes, u16, bool) {
    let mut new_buf = BytesMut::with_capacity(old_buf.len());
    let mut pos = 0;
    let mut found = false;

    while pos < old_buf.len() {
        let flen = u16::from_le_bytes([old_buf[pos], old_buf[pos + 1]]) as usize;
        pos += 2;
        let existing_field = &old_buf[pos..pos + flen];
        pos += flen;
        let vlen = u16::from_le_bytes([old_buf[pos], old_buf[pos + 1]]) as usize;
        pos += 2;

        if existing_field == field {
            found = true;
        } else {
            new_buf.extend_from_slice(&(flen as u16).to_le_bytes());
            new_buf.extend_from_slice(existing_field);
            new_buf.extend_from_slice(&(vlen as u16).to_le_bytes());
            new_buf.extend_from_slice(&old_buf[pos..pos + vlen]);
        }
        pos += vlen;
    }

    let new_len = if found { old_len - 1 } else { old_len };
    (new_buf.freeze(), new_len, found)
}

// ============================================================================
// Set Listpack Helpers
// ============================================================================

/// Iterator over members in a set listpack buffer.
/// Layout: [mlen:u16][member_bytes]... repeated.
struct ListpackSetIter<'a> {
    buf: &'a Bytes,
    pos: usize,
}

impl<'a> ListpackSetIter<'a> {
    fn new(buf: &'a Bytes) -> Self {
        Self { buf, pos: 0 }
    }
}

impl Iterator for ListpackSetIter<'_> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Bytes> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let mlen = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]) as usize;
        self.pos += 2;
        let member = self.buf.slice(self.pos..self.pos + mlen);
        self.pos += mlen;
        Some(member)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

/// Check if a member exists in a set listpack buffer.
fn lp_set_contains(buf: &[u8], member: &[u8]) -> bool {
    let mut pos = 0;
    while pos < buf.len() {
        let mlen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        if &buf[pos..pos + mlen] == member {
            return true;
        }
        pos += mlen;
    }
    false
}

// ============================================================================
// Hash Type
// ============================================================================

/// Internal encoding for hash values.
#[derive(Debug, Clone)]
enum HashEncoding {
    /// Contiguous byte buffer for small hashes.
    /// Layout: [flen:u16][field_bytes][vlen:u16][value_bytes]... repeated per entry.
    /// O(n) lookups — fast for small N due to cache locality.
    /// Per-entry overhead: 4 bytes (two u16 length prefixes).
    Listpack { buf: Bytes, len: u16 },

    /// Standard hash table for large hashes. O(1) lookups.
    HashMap(HashMap<Bytes, Bytes>),
}

impl Default for HashEncoding {
    fn default() -> Self {
        HashEncoding::Listpack {
            buf: Bytes::new(),
            len: 0,
        }
    }
}

/// Hash value - a mapping from field names to values.
#[derive(Debug, Clone)]
pub struct HashValue {
    data: HashEncoding,
    field_expiries: Option<HashMap<Bytes, Instant>>,
}

impl Default for HashValue {
    fn default() -> Self {
        Self::new()
    }
}

impl HashValue {
    /// Create a new empty hash (starts as listpack).
    pub fn new() -> Self {
        Self {
            data: HashEncoding::default(),
            field_expiries: None,
        }
    }

    /// Create a hash from an iterator of field-value pairs, choosing encoding
    /// based on thresholds.
    pub fn from_entries(
        entries: impl IntoIterator<Item = (Bytes, Bytes)>,
        thresholds: ListpackThresholds,
    ) -> Self {
        let entries: Vec<(Bytes, Bytes)> = entries.into_iter().collect();
        let use_listpack = entries.len() <= thresholds.max_entries
            && entries.iter().all(|(k, v)| {
                k.len() <= thresholds.max_value_bytes && v.len() <= thresholds.max_value_bytes
            });

        if use_listpack {
            let mut buf = BytesMut::new();
            for (k, v) in &entries {
                buf.extend_from_slice(&(k.len() as u16).to_le_bytes());
                buf.extend_from_slice(k);
                buf.extend_from_slice(&(v.len() as u16).to_le_bytes());
                buf.extend_from_slice(v);
            }
            Self {
                data: HashEncoding::Listpack {
                    buf: buf.freeze(),
                    len: entries.len() as u16,
                },
                field_expiries: None,
            }
        } else {
            Self {
                data: HashEncoding::HashMap(entries.into_iter().collect()),
                field_expiries: None,
            }
        }
    }

    /// Whether this hash uses listpack encoding.
    pub fn is_listpack(&self) -> bool {
        matches!(self.data, HashEncoding::Listpack { .. })
    }

    /// Get the number of fields.
    pub fn len(&self) -> usize {
        match &self.data {
            HashEncoding::Listpack { len, .. } => *len as usize,
            HashEncoding::HashMap(map) => map.len(),
        }
    }

    /// Check if the hash is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set a field value. Promotes to HashMap if thresholds are exceeded.
    ///
    /// Returns true if the field is new, false if it was updated.
    pub fn set(&mut self, field: Bytes, value: Bytes, thresholds: ListpackThresholds) -> bool {
        self.remove_field_expiry(&field);
        let data = std::mem::take(&mut self.data);
        match data {
            HashEncoding::Listpack { buf, len } => {
                let would_be_new = !lp_hash_contains(&buf, &field);
                let new_count = if would_be_new {
                    len as usize + 1
                } else {
                    len as usize
                };

                if new_count > thresholds.max_entries
                    || field.len() > thresholds.max_value_bytes
                    || value.len() > thresholds.max_value_bytes
                {
                    // Promote to HashMap
                    let mut map: HashMap<Bytes, Bytes> = ListpackHashIter::new(&buf).collect();
                    let was_new = map.insert(field, value).is_none();
                    self.data = HashEncoding::HashMap(map);
                    was_new
                } else {
                    let (new_buf, new_len, was_new) = lp_hash_set(&buf, len, &field, &value);
                    self.data = HashEncoding::Listpack {
                        buf: new_buf,
                        len: new_len,
                    };
                    was_new
                }
            }
            HashEncoding::HashMap(mut map) => {
                let was_new = map.insert(field, value).is_none();
                self.data = HashEncoding::HashMap(map);
                was_new
            }
        }
    }

    /// Set a field value only if it doesn't exist.
    ///
    /// Returns true if the field was set, false if it already existed.
    pub fn set_nx(&mut self, field: Bytes, value: Bytes, thresholds: ListpackThresholds) -> bool {
        if self.contains(&field) {
            return false;
        }
        self.set(field, value, thresholds)
    }

    /// Get a field value (zero-copy for listpack encoding).
    pub fn get(&self, field: &[u8]) -> Option<Bytes> {
        match &self.data {
            HashEncoding::Listpack { buf, .. } => lp_hash_get(buf, field),
            HashEncoding::HashMap(map) => map.get(field).cloned(),
        }
    }

    /// Remove a field.
    ///
    /// Returns true if the field existed.
    pub fn remove(&mut self, field: &[u8]) -> bool {
        self.remove_field_expiry(field);
        let data = std::mem::take(&mut self.data);
        match data {
            HashEncoding::Listpack { buf, len } => {
                let (new_buf, new_len, was_removed) = lp_hash_remove(&buf, len, field);
                self.data = HashEncoding::Listpack {
                    buf: new_buf,
                    len: new_len,
                };
                was_removed
            }
            HashEncoding::HashMap(mut map) => {
                let was_removed = map.remove(field).is_some();
                self.data = HashEncoding::HashMap(map);
                was_removed
            }
        }
    }

    /// Check if a field exists.
    pub fn contains(&self, field: &[u8]) -> bool {
        match &self.data {
            HashEncoding::Listpack { buf, .. } => lp_hash_contains(buf, field),
            HashEncoding::HashMap(map) => map.contains_key(field),
        }
    }

    /// Get all field names.
    pub fn keys(&self) -> impl Iterator<Item = Bytes> + '_ {
        match &self.data {
            HashEncoding::Listpack { buf, .. } => {
                EitherIter::Left(ListpackHashIter::new(buf).map(|(k, _)| k))
            }
            HashEncoding::HashMap(map) => EitherIter::Right(map.keys().cloned()),
        }
    }

    /// Get all values.
    pub fn values(&self) -> impl Iterator<Item = Bytes> + '_ {
        match &self.data {
            HashEncoding::Listpack { buf, .. } => {
                EitherIter::Left(ListpackHashIter::new(buf).map(|(_, v)| v))
            }
            HashEncoding::HashMap(map) => EitherIter::Right(map.values().cloned()),
        }
    }

    /// Iterate over all field-value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (Bytes, Bytes)> + '_ {
        match &self.data {
            HashEncoding::Listpack { buf, .. } => EitherIter::Left(ListpackHashIter::new(buf)),
            HashEncoding::HashMap(map) => {
                EitherIter::Right(map.iter().map(|(k, v)| (k.clone(), v.clone())))
            }
        }
    }

    /// Increment an integer field by delta.
    ///
    /// If the field doesn't exist, it's created with the delta value.
    /// Returns the new value or an error if the field is not a valid integer.
    pub fn incr_by(
        &mut self,
        field: Bytes,
        delta: i64,
        thresholds: ListpackThresholds,
    ) -> Result<i64, IncrementError> {
        let current = match self.get(&field) {
            Some(val) => std::str::from_utf8(&val)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or(IncrementError::NotInteger)?,
            None => 0,
        };

        let new_val = current.checked_add(delta).ok_or(IncrementError::Overflow)?;
        self.set(field, Bytes::from(new_val.to_string()), thresholds);
        Ok(new_val)
    }

    /// Increment a float field by delta.
    ///
    /// If the field doesn't exist, it's created with the delta value.
    /// Returns the new value or an error if the field is not a valid float.
    pub fn incr_by_float(
        &mut self,
        field: Bytes,
        delta: f64,
        thresholds: ListpackThresholds,
    ) -> Result<f64, IncrementError> {
        let current = match self.get(&field) {
            Some(val) => std::str::from_utf8(&val)
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or(IncrementError::NotFloat)?,
            None => 0.0,
        };

        let new_val = current + delta;

        if new_val.is_infinite() || new_val.is_nan() {
            return Err(IncrementError::Overflow);
        }

        self.set(field, Bytes::from(format_float(new_val)), thresholds);
        Ok(new_val)
    }

    /// Get random fields from the hash.
    ///
    /// If count > 0: return up to count unique fields
    /// If count < 0: return |count| fields, allowing duplicates
    pub fn random_fields(&self, count: i64, with_values: bool) -> Vec<(Bytes, Option<Bytes>)> {
        if self.is_empty() || count == 0 {
            return vec![];
        }

        let entries: Vec<(Bytes, Bytes)> = self.iter().collect();
        let mut rng = rand::thread_rng();

        if count > 0 {
            let count = (count as usize).min(entries.len());
            let mut indices: Vec<usize> = (0..entries.len()).collect();
            indices.shuffle(&mut rng);
            indices
                .into_iter()
                .take(count)
                .map(|i| {
                    let (ref k, ref v) = entries[i];
                    (k.clone(), if with_values { Some(v.clone()) } else { None })
                })
                .collect()
        } else {
            let abs_count = count.unsigned_abs() as usize;
            let mut result = Vec::with_capacity(abs_count);
            for _ in 0..abs_count {
                let idx = rand::random::<usize>() % entries.len();
                let (ref k, ref v) = entries[idx];
                result.push((k.clone(), if with_values { Some(v.clone()) } else { None }));
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let data_size = match &self.data {
            HashEncoding::Listpack { buf, .. } => buf.len(),
            HashEncoding::HashMap(map) => {
                map.iter().map(|(k, v)| k.len() + v.len() + 32).sum()
            }
        };
        let expiry_size = self
            .field_expiries
            .as_ref()
            .map(|expiries| {
                expiries
                    .keys()
                    .map(|k| k.len() + 16 + 32)
                    .sum::<usize>()
            })
            .unwrap_or(0);
        base_size + data_size + expiry_size
    }

    /// Get all field-value pairs as a vec for serialization.
    pub fn to_vec(&self) -> Vec<(Bytes, Bytes)> {
        self.iter().collect()
    }

    /// Create a hash from entries with per-field expiry times (for deserialization).
    pub fn from_entries_with_expiries(
        entries: impl IntoIterator<Item = (Bytes, Bytes, Option<Instant>)>,
        thresholds: ListpackThresholds,
    ) -> Self {
        let entries: Vec<(Bytes, Bytes, Option<Instant>)> = entries.into_iter().collect();
        let mut field_expiries: HashMap<Bytes, Instant> = HashMap::new();
        let mut data_entries = Vec::with_capacity(entries.len());

        for (field, value, expiry) in entries {
            if let Some(expires_at) = expiry {
                field_expiries.insert(field.clone(), expires_at);
            }
            data_entries.push((field, value));
        }

        let mut hash = Self::from_entries(data_entries, thresholds);
        if !field_expiries.is_empty() {
            hash.field_expiries = Some(field_expiries);
        }
        hash
    }

    /// Set field expiry time.
    pub fn set_field_expiry(&mut self, field: &[u8], expires_at: Instant) {
        let expiries = self.field_expiries.get_or_insert_with(HashMap::new);
        expiries.insert(Bytes::copy_from_slice(field), expires_at);
    }

    /// Remove field expiry. Returns true if the field had an expiry.
    pub fn remove_field_expiry(&mut self, field: &[u8]) -> bool {
        if let Some(ref mut expiries) = self.field_expiries {
            let removed = expiries.remove(field).is_some();
            if expiries.is_empty() {
                self.field_expiries = None;
            }
            removed
        } else {
            false
        }
    }

    /// Get expiry time for a field.
    pub fn get_field_expiry(&self, field: &[u8]) -> Option<Instant> {
        self.field_expiries.as_ref()?.get(field).copied()
    }

    /// Check if any field has an expiry set.
    pub fn has_field_expiries(&self) -> bool {
        self.field_expiries.as_ref().is_some_and(|e| !e.is_empty())
    }

    /// Access the field expiries map.
    pub fn field_expiries(&self) -> Option<&HashMap<Bytes, Instant>> {
        self.field_expiries.as_ref()
    }

    /// Remove all expired fields from data and field_expiries.
    /// Returns the names of removed fields.
    pub fn remove_expired_fields(&mut self, now: Instant) -> Vec<Bytes> {
        let expiries = match self.field_expiries.take() {
            Some(e) => e,
            None => return vec![],
        };

        let mut removed = Vec::new();
        let mut remaining = HashMap::new();

        for (field, expires_at) in expiries {
            if expires_at <= now {
                self.remove(&field);
                removed.push(field);
            } else {
                remaining.insert(field, expires_at);
            }
        }

        if !remaining.is_empty() {
            self.field_expiries = Some(remaining);
        }

        removed
    }

    /// Get all field-value pairs with their expiry times, for serialization.
    pub fn to_vec_with_expiries(&self) -> Vec<(Bytes, Bytes, Option<Instant>)> {
        self.iter()
            .map(|(field, value)| {
                let expiry = self.get_field_expiry(&field);
                (field, value, expiry)
            })
            .collect()
    }
}

// ============================================================================
// Set Type
// ============================================================================

/// Internal encoding for set values.
#[derive(Debug, Clone)]
enum SetEncoding {
    /// Contiguous byte buffer for small sets.
    /// Layout: [mlen:u16][member_bytes]... repeated per member.
    /// Per-entry overhead: 2 bytes (one u16 length prefix).
    Listpack { buf: Bytes, len: u16 },

    /// Standard hash set for large sets. O(1) lookups.
    HashSet(HashSet<Bytes>),
}

impl Default for SetEncoding {
    fn default() -> Self {
        SetEncoding::Listpack {
            buf: Bytes::new(),
            len: 0,
        }
    }
}

/// Set value - an unordered collection of unique members.
#[derive(Debug, Clone)]
pub struct SetValue {
    data: SetEncoding,
}

impl Default for SetValue {
    fn default() -> Self {
        Self::new()
    }
}

impl SetValue {
    /// Create a new empty set (starts as listpack).
    pub fn new() -> Self {
        Self {
            data: SetEncoding::default(),
        }
    }

    /// Create a set from an iterator of members, choosing encoding
    /// based on thresholds.
    pub fn from_members(
        members: impl IntoIterator<Item = Bytes>,
        thresholds: ListpackThresholds,
    ) -> Self {
        let members: Vec<Bytes> = members.into_iter().collect();
        let use_listpack = members.len() <= thresholds.max_entries
            && members
                .iter()
                .all(|m| m.len() <= thresholds.max_value_bytes);

        if use_listpack {
            let mut buf = BytesMut::new();
            for m in &members {
                buf.extend_from_slice(&(m.len() as u16).to_le_bytes());
                buf.extend_from_slice(m);
            }
            Self {
                data: SetEncoding::Listpack {
                    buf: buf.freeze(),
                    len: members.len() as u16,
                },
            }
        } else {
            Self {
                data: SetEncoding::HashSet(members.into_iter().collect()),
            }
        }
    }

    /// Whether this set uses listpack encoding.
    pub fn is_listpack(&self) -> bool {
        matches!(self.data, SetEncoding::Listpack { .. })
    }

    /// Get the number of members.
    pub fn len(&self) -> usize {
        match &self.data {
            SetEncoding::Listpack { len, .. } => *len as usize,
            SetEncoding::HashSet(set) => set.len(),
        }
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Add a member to the set. Promotes to HashSet if thresholds are exceeded.
    ///
    /// Returns true if the member was new, false if it already existed.
    pub fn add(&mut self, member: Bytes, thresholds: ListpackThresholds) -> bool {
        let data = std::mem::take(&mut self.data);
        match data {
            SetEncoding::Listpack { buf, len } => {
                if lp_set_contains(&buf, &member) {
                    self.data = SetEncoding::Listpack { buf, len };
                    return false;
                }
                let new_count = len as usize + 1;
                if new_count > thresholds.max_entries || member.len() > thresholds.max_value_bytes {
                    // Promote to HashSet
                    let mut set: HashSet<Bytes> = ListpackSetIter::new(&buf).collect();
                    set.insert(member);
                    self.data = SetEncoding::HashSet(set);
                } else {
                    let mut new_buf = BytesMut::with_capacity(buf.len() + 2 + member.len());
                    new_buf.extend_from_slice(&buf);
                    new_buf.extend_from_slice(&(member.len() as u16).to_le_bytes());
                    new_buf.extend_from_slice(&member);
                    self.data = SetEncoding::Listpack {
                        buf: new_buf.freeze(),
                        len: len + 1,
                    };
                }
                true
            }
            SetEncoding::HashSet(mut set) => {
                let was_new = set.insert(member);
                self.data = SetEncoding::HashSet(set);
                was_new
            }
        }
    }

    /// Remove a member from the set.
    ///
    /// Returns true if the member existed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        let data = std::mem::take(&mut self.data);
        match data {
            SetEncoding::Listpack { buf, len } => {
                if !lp_set_contains(&buf, member) {
                    self.data = SetEncoding::Listpack { buf, len };
                    return false;
                }
                let mut new_buf = BytesMut::with_capacity(buf.len());
                let mut pos = 0;
                while pos < buf.len() {
                    let mlen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
                    pos += 2;
                    if &buf[pos..pos + mlen] != member {
                        new_buf.extend_from_slice(&(mlen as u16).to_le_bytes());
                        new_buf.extend_from_slice(&buf[pos..pos + mlen]);
                    }
                    pos += mlen;
                }
                self.data = SetEncoding::Listpack {
                    buf: new_buf.freeze(),
                    len: len - 1,
                };
                true
            }
            SetEncoding::HashSet(mut set) => {
                let was_removed = set.remove(member);
                self.data = SetEncoding::HashSet(set);
                was_removed
            }
        }
    }

    /// Check if a member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        match &self.data {
            SetEncoding::Listpack { buf, .. } => lp_set_contains(buf, member),
            SetEncoding::HashSet(set) => set.contains(member),
        }
    }

    /// Get all members.
    pub fn members(&self) -> impl Iterator<Item = Bytes> + '_ {
        match &self.data {
            SetEncoding::Listpack { buf, .. } => EitherIter::Left(ListpackSetIter::new(buf)),
            SetEncoding::HashSet(set) => EitherIter::Right(set.iter().cloned()),
        }
    }

    /// Compute the union of this set with others.
    pub fn union<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result: HashSet<Bytes> = self.members().collect();
        for other in others {
            for member in other.members() {
                result.insert(member);
            }
        }
        SetValue {
            data: SetEncoding::HashSet(result),
        }
    }

    /// Compute the intersection of this set with others.
    pub fn intersection<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result: HashSet<Bytes> = self.members().collect();
        for other in others {
            result.retain(|m| other.contains(m));
        }
        SetValue {
            data: SetEncoding::HashSet(result),
        }
    }

    /// Compute the difference of this set minus others.
    pub fn difference<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result: HashSet<Bytes> = self.members().collect();
        for other in others {
            result.retain(|m| !other.contains(m));
        }
        SetValue {
            data: SetEncoding::HashSet(result),
        }
    }

    /// Pop a random member from the set.
    ///
    /// Returns None if the set is empty.
    pub fn pop(&mut self) -> Option<Bytes> {
        if self.is_empty() {
            return None;
        }
        let members: Vec<Bytes> = self.members().collect();
        let idx = rand::random::<usize>() % members.len();
        let member = members[idx].clone();
        self.remove(&member);
        Some(member)
    }

    /// Pop multiple random members from the set.
    pub fn pop_many(&mut self, count: usize) -> Vec<Bytes> {
        let count = count.min(self.len());
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(member) = self.pop() {
                result.push(member);
            } else {
                break;
            }
        }
        result
    }

    /// Get random members without removing them.
    ///
    /// If count > 0: return up to count unique members
    /// If count < 0: return |count| members, allowing duplicates
    pub fn random_members(&self, count: i64) -> Vec<Bytes> {
        if self.is_empty() || count == 0 {
            return vec![];
        }

        let members: Vec<Bytes> = self.members().collect();
        let mut rng = rand::thread_rng();

        if count > 0 {
            let count = (count as usize).min(members.len());
            let mut shuffled = members;
            shuffled.shuffle(&mut rng);
            shuffled.into_iter().take(count).collect()
        } else {
            let count = (-count) as usize;
            let mut result = Vec::with_capacity(count);
            for _ in 0..count {
                let idx = rand::random::<usize>() % members.len();
                result.push(members[idx].clone());
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        match &self.data {
            SetEncoding::Listpack { buf, .. } => base_size + buf.len(),
            SetEncoding::HashSet(set) => {
                let entries_size: usize = set.iter().map(|m| m.len() + 24).sum();
                base_size + entries_size
            }
        }
    }

    /// Get all members as a vec for serialization.
    pub fn to_vec(&self) -> Vec<Bytes> {
        self.members().collect()
    }
}

// ============================================================================
// List Type
// ============================================================================

/// List value - a doubly-linked list of values.
#[derive(Debug, Clone)]
pub struct ListValue {
    data: VecDeque<Bytes>,
}

impl Default for ListValue {
    fn default() -> Self {
        Self::new()
    }
}

impl ListValue {
    /// Create a new empty list.
    pub fn new() -> Self {
        Self {
            data: VecDeque::new(),
        }
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Push an element to the front (left).
    pub fn push_front(&mut self, value: Bytes) {
        self.data.push_front(value);
    }

    /// Push an element to the back (right).
    pub fn push_back(&mut self, value: Bytes) {
        self.data.push_back(value);
    }

    /// Pop an element from the front (left).
    pub fn pop_front(&mut self) -> Option<Bytes> {
        self.data.pop_front()
    }

    /// Pop an element from the back (right).
    pub fn pop_back(&mut self) -> Option<Bytes> {
        self.data.pop_back()
    }

    /// Normalize a Redis index (supports negative indices).
    fn normalize_index(&self, index: i64) -> Option<usize> {
        let len = self.len() as i64;
        if len == 0 {
            return None;
        }
        let normalized = if index < 0 { len + index } else { index };
        if normalized < 0 || normalized >= len {
            None
        } else {
            Some(normalized as usize)
        }
    }

    /// Get an element by index (supports negative indices).
    pub fn get(&self, index: i64) -> Option<&Bytes> {
        self.normalize_index(index).and_then(|i| self.data.get(i))
    }

    /// Set an element by index (supports negative indices).
    ///
    /// Returns true if the index was valid and the element was set.
    pub fn set(&mut self, index: i64, value: Bytes) -> bool {
        if let Some(i) = self.normalize_index(index)
            && let Some(elem) = self.data.get_mut(i)
        {
            *elem = value;
            return true;
        }
        false
    }

    /// Resolve start/end into (skip, take) counts. Returns (0, 0) for empty ranges.
    fn resolve_range(&self, start: i64, end: i64) -> (usize, usize) {
        let len = self.len() as i64;
        if len == 0 {
            return (0, 0);
        }

        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            return (0, 0);
        }

        (start, end as usize - start + 1)
    }

    /// Get a range of elements (inclusive, supports negative indices).
    pub fn range(&self, start: i64, end: i64) -> Vec<Bytes> {
        let (skip, take) = self.resolve_range(start, end);
        self.data.iter().skip(skip).take(take).cloned().collect()
    }

    /// Iterate over a range of elements without intermediate allocation.
    pub fn range_iter(&self, start: i64, end: i64) -> impl Iterator<Item = &Bytes> {
        let (skip, take) = self.resolve_range(start, end);
        self.data.iter().skip(skip).take(take)
    }

    /// Trim the list to only contain elements in the specified range.
    pub fn trim(&mut self, start: i64, end: i64) {
        let len = self.len() as i64;
        if len == 0 {
            return;
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            // Empty range - clear the list
            self.data.clear();
            return;
        }

        let end = end as usize;

        // Keep only elements in range [start, end]
        let new_data: VecDeque<_> = self
            .data
            .iter()
            .skip(start)
            .take(end - start + 1)
            .cloned()
            .collect();
        self.data = new_data;
    }

    /// Find the position of an element.
    ///
    /// Returns the first position where element is found, or None.
    /// `rank`: how many matches to skip (0 = first, 1 = second, etc.)
    /// `count`: maximum number of positions to return
    /// `maxlen`: maximum number of elements to scan
    pub fn position(
        &self,
        element: &[u8],
        rank: i64,
        count: usize,
        maxlen: Option<usize>,
    ) -> Vec<usize> {
        let maxlen = maxlen.unwrap_or(self.len());

        if rank >= 0 {
            // Forward scan
            let rank = rank as usize;
            let mut matches = 0;
            let mut positions = Vec::new();

            for (i, item) in self.data.iter().enumerate().take(maxlen) {
                if item.as_ref() == element {
                    if matches >= rank {
                        positions.push(i);
                        if positions.len() >= count {
                            break;
                        }
                    }
                    matches += 1;
                }
            }
            positions
        } else {
            // Backward scan
            let rank = (-rank - 1) as usize;
            let mut matches = 0;
            let mut positions = Vec::new();
            let scan_start = if maxlen < self.len() {
                self.len() - maxlen
            } else {
                0
            };

            for (i, item) in self.data.iter().enumerate().rev() {
                if i < scan_start {
                    break;
                }
                if item.as_ref() == element {
                    if matches >= rank {
                        positions.push(i);
                        if positions.len() >= count {
                            break;
                        }
                    }
                    matches += 1;
                }
            }
            positions
        }
    }

    /// Insert an element before or after a pivot element.
    ///
    /// Returns the new length of the list, -1 if pivot not found, 0 if list is empty.
    pub fn insert(&mut self, before: bool, pivot: &[u8], element: Bytes) -> i64 {
        if self.is_empty() {
            return 0;
        }

        // Find pivot position
        let pos = self.data.iter().position(|e| e.as_ref() == pivot);

        match pos {
            Some(i) => {
                let insert_pos = if before { i } else { i + 1 };
                self.data.insert(insert_pos, element);
                self.len() as i64
            }
            None => -1,
        }
    }

    /// Remove elements equal to value.
    ///
    /// `count` determines direction and number:
    /// - count > 0: Remove first count occurrences (head to tail)
    /// - count < 0: Remove first |count| occurrences (tail to head)
    /// - count = 0: Remove all occurrences
    ///
    /// Returns the number of elements removed.
    pub fn remove(&mut self, count: i64, element: &[u8]) -> usize {
        if self.is_empty() {
            return 0;
        }

        let mut removed = 0;

        if count == 0 {
            // Remove all
            let original_len = self.len();
            self.data.retain(|e| e.as_ref() != element);
            removed = original_len - self.len();
        } else if count > 0 {
            // Remove from head
            let max_remove = count as usize;
            let mut new_data = VecDeque::with_capacity(self.len());
            for item in self.data.drain(..) {
                if removed < max_remove && item.as_ref() == element {
                    removed += 1;
                } else {
                    new_data.push_back(item);
                }
            }
            self.data = new_data;
        } else {
            // Remove from tail
            let max_remove = (-count) as usize;
            let mut indices_to_remove = Vec::new();
            for (i, item) in self.data.iter().enumerate().rev() {
                if item.as_ref() == element {
                    indices_to_remove.push(i);
                    if indices_to_remove.len() >= max_remove {
                        break;
                    }
                }
            }
            // Sort indices in descending order to remove from end first
            indices_to_remove.sort_by(|a, b| b.cmp(a));
            for i in indices_to_remove {
                self.data.remove(i);
                removed += 1;
            }
        }

        removed
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let entries_size: usize = self
            .data
            .iter()
            .map(|e| e.len() + 8) // 8 for VecDeque node overhead
            .sum();
        base_size + entries_size
    }

    /// Get all elements as a vec for serialization.
    pub fn to_vec(&self) -> Vec<Bytes> {
        self.data.iter().cloned().collect()
    }

    /// Iterate over all elements.
    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.data.iter()
    }
}

// ============================================================================
// Stream Type
// ============================================================================

/// Stream entry ID consisting of millisecond timestamp and sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct StreamId {
    /// Unix timestamp in milliseconds.
    pub ms: u64,
    /// Sequence number within the millisecond.
    pub seq: u64,
}

impl StreamId {
    /// Create a new stream ID.
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// Minimum possible stream ID (0-0).
    pub fn min() -> Self {
        Self { ms: 0, seq: 0 }
    }

    /// Maximum possible stream ID.
    pub fn max() -> Self {
        Self {
            ms: u64::MAX,
            seq: u64::MAX,
        }
    }

    /// Check if this is the zero ID (0-0).
    pub fn is_zero(&self) -> bool {
        self.ms == 0 && self.seq == 0
    }

    /// Parse a stream ID from bytes.
    ///
    /// Format: "ms-seq" where ms and seq are unsigned integers.
    /// Also accepts just "ms" (sequence defaults to 0 for explicit IDs).
    pub fn parse(s: &[u8]) -> Result<Self, StreamIdParseError> {
        let s = std::str::from_utf8(s).map_err(|_| StreamIdParseError::InvalidFormat)?;

        if let Some((ms_str, seq_str)) = s.split_once('-') {
            let ms = ms_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            let seq = seq_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(Self { ms, seq })
        } else {
            // Just milliseconds, sequence is 0
            let ms = s
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(Self { ms, seq: 0 })
        }
    }

    /// Parse a stream ID for use as a range bound.
    ///
    /// Supports special values:
    /// - "-" means minimum ID
    /// - "+" means maximum ID
    /// - Otherwise parses as normal ID
    pub fn parse_range_bound(s: &[u8]) -> Result<StreamRangeBound, StreamIdParseError> {
        match s {
            b"-" => Ok(StreamRangeBound::Min),
            b"+" => Ok(StreamRangeBound::Max),
            _ => {
                // Check for exclusive prefix
                if s.starts_with(b"(") {
                    let id = Self::parse(&s[1..])?;
                    Ok(StreamRangeBound::Exclusive(id))
                } else {
                    let id = Self::parse(s)?;
                    Ok(StreamRangeBound::Inclusive(id))
                }
            }
        }
    }

    /// Parse a stream ID for XADD.
    ///
    /// Supports special values:
    /// - "*" means auto-generate
    /// - "ms-*" means auto-generate sequence for given timestamp
    /// - Otherwise parses as explicit ID
    pub fn parse_for_add(s: &[u8]) -> Result<StreamIdSpec, StreamIdParseError> {
        let s_str = std::str::from_utf8(s).map_err(|_| StreamIdParseError::InvalidFormat)?;

        if s_str == "*" {
            return Ok(StreamIdSpec::Auto);
        }

        if let Some((ms_str, seq_str)) = s_str.split_once('-') {
            let ms = ms_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            if seq_str == "*" {
                return Ok(StreamIdSpec::AutoSeq(ms));
            }
            let seq = seq_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(StreamIdSpec::Explicit(Self { ms, seq }))
        } else {
            let ms = s_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(StreamIdSpec::Explicit(Self { ms, seq: 0 }))
        }
    }

    /// Generate a new stream ID based on current time and the last ID.
    pub fn generate(last: &StreamId) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        if now_ms > last.ms {
            Self { ms: now_ms, seq: 0 }
        } else {
            // Same or earlier timestamp, increment sequence
            Self {
                ms: last.ms,
                seq: last.seq.saturating_add(1),
            }
        }
    }

    /// Generate a new stream ID with auto-sequence for a given timestamp.
    pub fn generate_with_ms(ms: u64, last: &StreamId) -> Option<Self> {
        if ms > last.ms {
            Some(Self { ms, seq: 0 })
        } else if ms == last.ms {
            Some(Self {
                ms,
                seq: last.seq.saturating_add(1),
            })
        } else {
            // Timestamp is in the past
            None
        }
    }

    /// Check if this ID is valid as a new entry after the last ID.
    pub fn is_valid_after(&self, last: &StreamId) -> bool {
        self > last
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl Ord for StreamId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.ms.cmp(&other.ms) {
            std::cmp::Ordering::Equal => self.seq.cmp(&other.seq),
            ord => ord,
        }
    }
}

impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Stream ID parsing error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamIdParseError {
    /// Invalid format (not "ms-seq" or "ms").
    InvalidFormat,
}

impl std::fmt::Display for StreamIdParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamIdParseError::InvalidFormat => {
                write!(
                    f,
                    "ERR Invalid stream ID specified as stream command argument"
                )
            }
        }
    }
}

impl std::error::Error for StreamIdParseError {}

/// Stream ID specification for XADD.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamIdSpec {
    /// Auto-generate ID based on current time.
    Auto,
    /// Auto-generate sequence for given millisecond timestamp.
    AutoSeq(u64),
    /// Explicit ID.
    Explicit(StreamId),
}

/// Stream range bound for XRANGE/XREVRANGE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRangeBound {
    /// Minimum ID ("-").
    Min,
    /// Maximum ID ("+").
    Max,
    /// Inclusive bound.
    Inclusive(StreamId),
    /// Exclusive bound (prefixed with "(").
    Exclusive(StreamId),
}

impl StreamRangeBound {
    /// Check if an ID satisfies this bound as a minimum.
    pub fn satisfies_min(&self, id: &StreamId) -> bool {
        match self {
            StreamRangeBound::Min => true,
            StreamRangeBound::Max => false,
            StreamRangeBound::Inclusive(bound) => id >= bound,
            StreamRangeBound::Exclusive(bound) => id > bound,
        }
    }

    /// Check if an ID satisfies this bound as a maximum.
    pub fn satisfies_max(&self, id: &StreamId) -> bool {
        match self {
            StreamRangeBound::Min => false,
            StreamRangeBound::Max => true,
            StreamRangeBound::Inclusive(bound) => id <= bound,
            StreamRangeBound::Exclusive(bound) => id < bound,
        }
    }
}

/// Stream entry with ID and field-value pairs.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Entry ID.
    pub id: StreamId,
    /// Field-value pairs.
    pub fields: Vec<(Bytes, Bytes)>,
}

impl StreamEntry {
    /// Create a new stream entry.
    pub fn new(id: StreamId, fields: Vec<(Bytes, Bytes)>) -> Self {
        Self { id, fields }
    }

    /// Calculate memory size of this entry.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let fields_size: usize = self
            .fields
            .iter()
            .map(|(k, v)| k.len() + v.len() + 16) // 16 for Vec overhead
            .sum();
        base + fields_size
    }
}

/// Pending entry in a consumer group's PEL (Pending Entries List).
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// Consumer that owns this pending entry.
    pub consumer: Bytes,
    /// Time when the entry was delivered.
    pub delivery_time: Instant,
    /// Number of times this entry has been delivered.
    pub delivery_count: u32,
}

impl PendingEntry {
    /// Create a new pending entry.
    pub fn new(consumer: Bytes) -> Self {
        Self {
            consumer,
            delivery_time: Instant::now(),
            delivery_count: 1,
        }
    }

    /// Get idle time in milliseconds.
    pub fn idle_ms(&self) -> u64 {
        self.delivery_time.elapsed().as_millis() as u64
    }
}

/// Consumer in a consumer group.
#[derive(Debug, Clone)]
pub struct Consumer {
    /// Consumer name.
    pub name: Bytes,
    /// Number of pending entries for this consumer.
    pub pending_count: usize,
    /// Last time this consumer was seen (read or claimed).
    pub last_seen: Instant,
}

impl Consumer {
    /// Create a new consumer.
    pub fn new(name: Bytes) -> Self {
        Self {
            name,
            pending_count: 0,
            last_seen: Instant::now(),
        }
    }

    /// Get idle time in milliseconds.
    pub fn idle_ms(&self) -> u64 {
        self.last_seen.elapsed().as_millis() as u64
    }

    /// Touch the consumer (update last_seen).
    pub fn touch(&mut self) {
        self.last_seen = Instant::now();
    }
}

/// Consumer group for a stream.
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// Group name.
    pub name: Bytes,
    /// Last delivered ID (entries after this are "new").
    pub last_delivered_id: StreamId,
    /// Pending entries list (PEL) - entries delivered but not acknowledged.
    pub pending: BTreeMap<StreamId, PendingEntry>,
    /// Consumers in this group.
    pub consumers: HashMap<Bytes, Consumer>,
    /// Number of entries read by this group (for XINFO).
    pub entries_read: Option<u64>,
}

impl ConsumerGroup {
    /// Create a new consumer group.
    pub fn new(name: Bytes, last_delivered_id: StreamId) -> Self {
        Self {
            name,
            last_delivered_id,
            pending: BTreeMap::new(),
            consumers: HashMap::new(),
            entries_read: None,
        }
    }

    /// Get or create a consumer.
    pub fn get_or_create_consumer(&mut self, name: Bytes) -> &mut Consumer {
        self.consumers
            .entry(name.clone())
            .or_insert_with(|| Consumer::new(name))
    }

    /// Create a consumer if it doesn't exist.
    ///
    /// Returns true if the consumer was created, false if it already existed.
    pub fn create_consumer(&mut self, name: Bytes) -> bool {
        if self.consumers.contains_key(&name) {
            false
        } else {
            self.consumers.insert(name.clone(), Consumer::new(name));
            true
        }
    }

    /// Delete a consumer.
    ///
    /// Returns the number of pending entries that were deleted with the consumer.
    pub fn delete_consumer(&mut self, name: &[u8]) -> usize {
        if self.consumers.remove(name).is_some() {
            // Remove all pending entries for this consumer
            let to_remove: Vec<StreamId> = self
                .pending
                .iter()
                .filter(|(_, pe)| pe.consumer.as_ref() == name)
                .map(|(id, _)| *id)
                .collect();
            let count = to_remove.len();
            for id in to_remove {
                self.pending.remove(&id);
            }
            count
        } else {
            0
        }
    }

    /// Add a pending entry for a consumer.
    pub fn add_pending(&mut self, id: StreamId, consumer: Bytes) {
        // Update consumer's pending count
        if let Some(c) = self.consumers.get_mut(&consumer) {
            c.pending_count += 1;
            c.touch();
        }
        self.pending.insert(id, PendingEntry::new(consumer));
    }

    /// Acknowledge entries (remove from PEL).
    ///
    /// Returns the number of entries acknowledged.
    pub fn ack(&mut self, ids: &[StreamId]) -> usize {
        let mut count = 0;
        for id in ids {
            if let Some(pe) = self.pending.remove(id) {
                count += 1;
                // Update consumer's pending count
                if let Some(c) = self.consumers.get_mut(&pe.consumer) {
                    c.pending_count = c.pending_count.saturating_sub(1);
                }
            }
        }
        count
    }

    /// Get pending entry count summary.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get the smallest and largest pending IDs.
    pub fn pending_range(&self) -> Option<(StreamId, StreamId)> {
        let first = self.pending.first_key_value()?.0;
        let last = self.pending.last_key_value()?.0;
        Some((*first, *last))
    }

    /// Calculate memory size of this group.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let pending_size: usize = self
            .pending
            .values()
            .map(|pe| std::mem::size_of::<StreamId>() + pe.consumer.len() + 32)
            .sum();
        let consumers_size: usize = self
            .consumers
            .keys()
            .map(|k| k.len() + std::mem::size_of::<Consumer>() + 32)
            .sum();
        base + pending_size + consumers_size + self.name.len()
    }
}

/// Strategy for handling consumer group PEL references during stream entry deletion.
///
/// Used by XDELEX and XACKDEL commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeleteRefStrategy {
    /// Delete entry, preserve PEL references (same as XDEL behavior).
    #[default]
    KeepRef,
    /// Delete entry AND remove all PEL references across all groups.
    DelRef,
    /// Only delete entries acknowledged by ALL consumer groups.
    Acked,
}

/// Trimming strategy for streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTrimStrategy {
    /// Trim by maximum length.
    MaxLen(u64),
    /// Trim by minimum ID (entries older than this ID are removed).
    MinId(StreamId),
}

/// Trimming mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTrimMode {
    /// Exact trimming (remove exactly to the threshold).
    Exact,
    /// Approximate trimming (may keep slightly more entries for efficiency).
    Approximate,
}

/// Trimming options for XTRIM and XADD.
#[derive(Debug, Clone, Copy)]
pub struct StreamTrimOptions {
    /// Trimming strategy.
    pub strategy: StreamTrimStrategy,
    /// Trimming mode.
    pub mode: StreamTrimMode,
    /// Maximum number of entries to trim in one operation (0 = unlimited).
    pub limit: usize,
}

/// Idempotency deduplication state for event sourcing streams.
///
/// Tracks recently seen idempotency keys with bounded FIFO eviction.
/// Default capacity: 10,000 keys.
#[derive(Debug, Clone)]
pub struct IdempotencyState {
    /// Set of active idempotency keys for O(1) lookup.
    keys: HashSet<Bytes>,
    /// FIFO order for eviction — oldest key at front.
    order: VecDeque<Bytes>,
    /// Maximum number of idempotency keys to retain.
    limit: usize,
}

impl IdempotencyState {
    /// Default maximum number of idempotency keys.
    pub const DEFAULT_LIMIT: usize = 10_000;

    /// Create a new idempotency state with the default limit.
    pub fn new() -> Self {
        Self {
            keys: HashSet::new(),
            order: VecDeque::new(),
            limit: Self::DEFAULT_LIMIT,
        }
    }

    /// Check if a key exists in the dedup set.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.keys.contains(key)
    }

    /// Record an idempotency key, evicting the oldest if at capacity.
    pub fn record(&mut self, key: Bytes) {
        if self.keys.contains(&key) {
            return;
        }
        // Evict oldest if at capacity
        while self.order.len() >= self.limit {
            if let Some(old) = self.order.pop_front() {
                self.keys.remove(&old);
            }
        }
        self.keys.insert(key.clone());
        self.order.push_back(key);
    }

    /// Number of tracked idempotency keys.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the idempotency set is empty.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Iterate over all idempotency keys in FIFO order.
    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.order.iter()
    }

    /// Get the capacity limit.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

impl Default for IdempotencyState {
    fn default() -> Self {
        Self::new()
    }
}

/// Stream value - an append-only log of entries with consumer groups.
#[derive(Debug, Clone)]
pub struct StreamValue {
    /// Entries ordered by ID.
    entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    /// Last generated/added ID (for auto-generation).
    last_id: StreamId,
    /// Consumer groups.
    groups: HashMap<Bytes, ConsumerGroup>,
    /// First entry ID (cached for efficiency).
    first_id: Option<StreamId>,
    /// Monotonic version counter — incremented on every append, never decremented.
    /// Used by ES.* commands for optimistic concurrency control.
    total_appended: u64,
    /// Idempotency deduplication state for ES.APPEND IF_NOT_EXISTS.
    /// Lazy-allocated: `None` for non-event-sourcing streams (zero overhead).
    idempotency: Option<Box<IdempotencyState>>,
}

impl Default for StreamValue {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamValue {
    /// Create a new empty stream.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamId::default(),
            groups: HashMap::new(),
            first_id: None,
            total_appended: 0,
            idempotency: None,
        }
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the stream is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the last entry ID.
    pub fn last_id(&self) -> StreamId {
        self.last_id
    }

    /// Get the first entry ID.
    pub fn first_id(&self) -> Option<StreamId> {
        self.first_id
    }

    /// Get the first entry.
    pub fn first_entry(&self) -> Option<StreamEntry> {
        self.entries
            .first_key_value()
            .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
    }

    /// Get the last entry.
    pub fn last_entry(&self) -> Option<StreamEntry> {
        self.entries
            .last_key_value()
            .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
    }

    /// Add an entry to the stream.
    ///
    /// Returns the ID of the added entry, or an error if the ID is invalid.
    pub fn add(
        &mut self,
        id_spec: StreamIdSpec,
        fields: Vec<(Bytes, Bytes)>,
    ) -> Result<StreamId, StreamAddError> {
        let id = match id_spec {
            StreamIdSpec::Auto => StreamId::generate(&self.last_id),
            StreamIdSpec::AutoSeq(ms) => {
                StreamId::generate_with_ms(ms, &self.last_id).ok_or(StreamAddError::IdTooSmall)?
            }
            StreamIdSpec::Explicit(id) => {
                if !id.is_valid_after(&self.last_id) {
                    return Err(StreamAddError::IdTooSmall);
                }
                id
            }
        };

        // Check for zero ID (not allowed as first entry)
        if self.is_empty() && id.is_zero() {
            return Err(StreamAddError::IdTooSmall);
        }

        self.entries.insert(id, fields);
        self.last_id = id;
        self.total_appended += 1;

        // Update first_id if this is the first entry
        if self.first_id.is_none() {
            self.first_id = Some(id);
        }

        Ok(id)
    }

    // ==================== Event Sourcing Methods ====================

    /// Get the monotonic append counter (used as version for OCC).
    pub fn total_appended(&self) -> u64 {
        self.total_appended
    }

    /// Set the total_appended counter (used during deserialization).
    pub fn set_total_appended(&mut self, value: u64) {
        self.total_appended = value;
    }

    /// Get a reference to the idempotency state, if initialized.
    pub fn idempotency(&self) -> Option<&IdempotencyState> {
        self.idempotency.as_deref()
    }

    /// Set the idempotency state (used during deserialization).
    pub fn set_idempotency(&mut self, state: IdempotencyState) {
        self.idempotency = Some(Box::new(state));
    }

    /// Check if an idempotency key exists.
    pub fn has_idempotency_key(&self, key: &[u8]) -> bool {
        self.idempotency
            .as_ref()
            .is_some_and(|state| state.contains(key))
    }

    /// Record an idempotency key, initializing the state if needed.
    pub fn record_idempotency_key(&mut self, key: Bytes) {
        self.idempotency
            .get_or_insert_with(|| Box::new(IdempotencyState::new()))
            .record(key);
    }

    /// Append with optimistic concurrency control (for ES.APPEND).
    ///
    /// Checks `expected_version` against `total_appended`. If they match,
    /// appends the entry and increments the version. Optionally checks
    /// and records an idempotency key for IF_NOT_EXISTS dedup.
    ///
    /// Returns `(stream_id, new_version)` on success.
    pub fn add_with_version_check(
        &mut self,
        expected_version: u64,
        fields: Vec<(Bytes, Bytes)>,
        idempotency_key: Option<&Bytes>,
    ) -> Result<(StreamId, u64), EsAppendError> {
        // Idempotency check: if key already seen, return current version
        if let Some(idem_key) = idempotency_key
            && self.has_idempotency_key(idem_key)
        {
            return Err(EsAppendError::DuplicateIdempotencyKey {
                version: self.total_appended,
            });
        }

        // Version check (OCC)
        if expected_version != self.total_appended {
            return Err(EsAppendError::VersionMismatch {
                expected: expected_version,
                actual: self.total_appended,
            });
        }

        // Append entry with auto-generated ID
        let id = self
            .add(StreamIdSpec::Auto, fields)
            .map_err(|_| EsAppendError::Internal("Failed to generate stream ID".to_string()))?;

        // Record idempotency key if provided
        if let Some(idem_key) = idempotency_key {
            self.record_idempotency_key(idem_key.clone());
        }

        Ok((id, self.total_appended))
    }

    /// Read entries by version range (1-based inclusive).
    ///
    /// Version 1 corresponds to the first entry ever appended, version 2 to the
    /// second, etc. This is O(N) skip for v1 — acceptable for initial implementation.
    pub fn range_by_version(
        &self,
        start: u64,
        end: Option<u64>,
        count: Option<usize>,
    ) -> Vec<(u64, StreamEntry)> {
        if start == 0 || start > self.total_appended {
            return vec![];
        }

        let end = end.unwrap_or(self.total_appended);

        // We walk all entries and assign version numbers sequentially.
        // Deleted entries create version gaps — we skip over them.
        // This is correct because total_appended never decrements.
        let mut version = 0u64;
        let mut results = Vec::new();
        let limit = count.unwrap_or(usize::MAX);

        for (id, fields) in &self.entries {
            version += 1;
            if version > end || results.len() >= limit {
                break;
            }
            if version >= start {
                results.push((version, StreamEntry::new(*id, fields.clone())));
            }
        }

        results
    }

    /// Delete entries by ID.
    ///
    /// Returns the number of entries deleted.
    pub fn delete(&mut self, ids: &[StreamId]) -> usize {
        let mut count = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                count += 1;
            }
        }

        // Update first_id if needed
        if count > 0 {
            self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
        }

        count
    }

    /// Check if an entry is fully acknowledged by all consumer groups.
    ///
    /// Returns true if the ID is NOT in any group's PEL (i.e., either never delivered
    /// or already acked by all groups). Also returns true when there are no consumer groups.
    fn is_fully_acked(&self, id: &StreamId) -> bool {
        self.groups.values().all(|group| !group.pending.contains_key(id))
    }

    /// Remove all PEL references for given IDs across all consumer groups.
    fn remove_all_pel_refs(&mut self, ids: &[StreamId]) {
        for group in self.groups.values_mut() {
            for id in ids {
                if let Some(pe) = group.pending.remove(id)
                    && let Some(c) = group.consumers.get_mut(&pe.consumer)
                {
                    c.pending_count = c.pending_count.saturating_sub(1);
                }
            }
        }
    }

    /// Extended delete with per-ID result array and reference control.
    ///
    /// Returns per-ID results: `-1` (not found), `1` (deleted), `2` (not deleted, ACKED mode only).
    pub fn delete_ex(&mut self, ids: &[StreamId], strategy: DeleteRefStrategy) -> Vec<i64> {
        let mut results = Vec::with_capacity(ids.len());
        let mut any_deleted = false;

        for id in ids {
            if !self.entries.contains_key(id) {
                results.push(-1);
                continue;
            }

            match strategy {
                DeleteRefStrategy::KeepRef => {
                    self.entries.remove(id);
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::DelRef => {
                    self.entries.remove(id);
                    self.remove_all_pel_refs(&[*id]);
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::Acked => {
                    if self.is_fully_acked(id) {
                        self.entries.remove(id);
                        any_deleted = true;
                        results.push(1);
                    } else {
                        results.push(2);
                    }
                }
            }
        }

        if any_deleted {
            self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
        }

        results
    }

    /// Acknowledge in one group, then conditionally delete based on strategy.
    ///
    /// Returns per-ID results: `-1` (not found), `1` (acked+deleted), `2` (acked but not deleted).
    pub fn ack_and_delete(
        &mut self,
        group_name: &[u8],
        ids: &[StreamId],
        strategy: DeleteRefStrategy,
    ) -> Result<Vec<i64>, StreamGroupError> {
        if !self.groups.contains_key(group_name) {
            return Err(StreamGroupError::NoGroup);
        }

        let mut results = Vec::with_capacity(ids.len());
        let mut any_deleted = false;

        for id in ids {
            if !self.entries.contains_key(id) {
                // Still ack in the group even if entry doesn't exist (matching XACK behavior)
                if let Some(group) = self.groups.get_mut(group_name) {
                    group.ack(&[*id]);
                }
                results.push(-1);
                continue;
            }

            // Ack in the specified group
            if let Some(group) = self.groups.get_mut(group_name) {
                group.ack(&[*id]);
            }

            // Apply delete strategy
            match strategy {
                DeleteRefStrategy::KeepRef => {
                    self.entries.remove(id);
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::DelRef => {
                    self.entries.remove(id);
                    self.remove_all_pel_refs(&[*id]);
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::Acked => {
                    if self.is_fully_acked(id) {
                        self.entries.remove(id);
                        any_deleted = true;
                        results.push(1);
                    } else {
                        results.push(2);
                    }
                }
            }
        }

        if any_deleted {
            self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
        }

        Ok(results)
    }

    /// Get entries in a range.
    pub fn range(
        &self,
        start: StreamRangeBound,
        end: StreamRangeBound,
        count: Option<usize>,
    ) -> Vec<StreamEntry> {
        let iter = self
            .entries
            .iter()
            .filter(|(id, _)| start.satisfies_min(id) && end.satisfies_max(id));

        let entries: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        } else {
            iter.map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        };

        entries
    }

    /// Get entries in a range (reverse order).
    pub fn range_rev(
        &self,
        start: StreamRangeBound,
        end: StreamRangeBound,
        count: Option<usize>,
    ) -> Vec<StreamEntry> {
        // For reverse range, start and end are swapped (end is the "start" bound)
        let iter = self
            .entries
            .iter()
            .rev()
            .filter(|(id, _)| end.satisfies_min(id) && start.satisfies_max(id));

        let entries: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        } else {
            iter.map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        };

        entries
    }

    /// Read entries after a given ID (for XREAD).
    pub fn read_after(&self, after: &StreamId, count: Option<usize>) -> Vec<StreamEntry> {
        let iter = self.entries.range((
            std::ops::Bound::Excluded(*after),
            std::ops::Bound::Unbounded,
        ));

        let entries: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        } else {
            iter.map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        };

        entries
    }

    /// Trim the stream.
    ///
    /// Returns the number of entries removed.
    pub fn trim(&mut self, options: StreamTrimOptions) -> usize {
        let mut removed = 0;
        let limit = if options.limit == 0 {
            usize::MAX
        } else {
            options.limit
        };

        match options.strategy {
            StreamTrimStrategy::MaxLen(max_len) => {
                let max_len = max_len as usize;
                while self.entries.len() > max_len && removed < limit {
                    if let Some((id, _)) = self.entries.pop_first() {
                        removed += 1;
                        // Update first_id
                        if Some(id) == self.first_id {
                            self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
                        }
                    } else {
                        break;
                    }
                }
            }
            StreamTrimStrategy::MinId(min_id) => {
                // Remove all entries with ID < min_id
                let to_remove: Vec<StreamId> = self
                    .entries
                    .range(..min_id)
                    .take(limit)
                    .map(|(id, _)| *id)
                    .collect();
                for id in to_remove {
                    self.entries.remove(&id);
                    removed += 1;
                }
                // Update first_id
                if removed > 0 {
                    self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
                }
            }
        }

        removed
    }

    // ==================== Consumer Group Methods ====================

    /// Create a consumer group.
    ///
    /// Returns an error if the group already exists.
    pub fn create_group(
        &mut self,
        name: Bytes,
        last_delivered_id: StreamId,
        entries_read: Option<u64>,
    ) -> Result<(), StreamGroupError> {
        if self.groups.contains_key(&name) {
            return Err(StreamGroupError::GroupExists);
        }
        let mut group = ConsumerGroup::new(name.clone(), last_delivered_id);
        group.entries_read = entries_read;
        self.groups.insert(name, group);
        Ok(())
    }

    /// Destroy a consumer group.
    ///
    /// Returns true if the group was destroyed, false if it didn't exist.
    pub fn destroy_group(&mut self, name: &[u8]) -> bool {
        self.groups.remove(name).is_some()
    }

    /// Get a consumer group.
    pub fn get_group(&self, name: &[u8]) -> Option<&ConsumerGroup> {
        self.groups.get(name)
    }

    /// Get a mutable consumer group.
    pub fn get_group_mut(&mut self, name: &[u8]) -> Option<&mut ConsumerGroup> {
        self.groups.get_mut(name)
    }

    /// Get all consumer groups.
    pub fn groups(&self) -> impl Iterator<Item = &ConsumerGroup> {
        self.groups.values()
    }

    /// Number of consumer groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Set a consumer group's last delivered ID.
    pub fn set_group_id(
        &mut self,
        name: &[u8],
        id: StreamId,
        entries_read: Option<u64>,
    ) -> Result<(), StreamGroupError> {
        let group = self.groups.get_mut(name).ok_or(StreamGroupError::NoGroup)?;
        group.last_delivered_id = id;
        if entries_read.is_some() {
            group.entries_read = entries_read;
        }
        Ok(())
    }

    /// Get an entry by ID.
    pub fn get(&self, id: &StreamId) -> Option<StreamEntry> {
        self.entries
            .get(id)
            .map(|fields| StreamEntry::new(*id, fields.clone()))
    }

    /// Check if an entry exists.
    pub fn contains(&self, id: &StreamId) -> bool {
        self.entries.contains_key(id)
    }

    /// Calculate memory size of this stream.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();

        let entries_size: usize = self
            .entries
            .values()
            .map(|fields| {
                let fields_size: usize = fields.iter().map(|(k, v)| k.len() + v.len() + 16).sum();
                std::mem::size_of::<StreamId>() + fields_size + 32
            })
            .sum();

        let groups_size: usize = self.groups.values().map(|g| g.memory_size()).sum();

        base + entries_size + groups_size
    }

    /// Convert to vec for serialization.
    pub fn to_vec(&self) -> Vec<StreamEntry> {
        self.entries
            .iter()
            .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
            .collect()
    }
}

/// Error when adding to a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamAddError {
    /// The ID is equal to or smaller than the last ID.
    IdTooSmall,
}

impl std::fmt::Display for StreamAddError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamAddError::IdTooSmall => {
                write!(
                    f,
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                )
            }
        }
    }
}

impl std::error::Error for StreamAddError {}

/// Error for ES.APPEND operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EsAppendError {
    /// OCC version mismatch.
    VersionMismatch { expected: u64, actual: u64 },
    /// Idempotency key already exists (not a real error — returns current version).
    DuplicateIdempotencyKey { version: u64 },
    /// Internal failure (stream ID generation, etc.).
    Internal(String),
}

impl std::fmt::Display for EsAppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EsAppendError::VersionMismatch { expected, actual } => {
                write!(f, "VERSIONMISMATCH expected {expected} actual {actual}")
            }
            EsAppendError::DuplicateIdempotencyKey { version } => {
                write!(f, "duplicate idempotency key at version {version}")
            }
            EsAppendError::Internal(msg) => {
                write!(f, "ERR {msg}")
            }
        }
    }
}

impl std::error::Error for EsAppendError {}

/// Error for consumer group operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamGroupError {
    /// Consumer group already exists.
    GroupExists,
    /// Consumer group doesn't exist.
    NoGroup,
}

impl std::fmt::Display for StreamGroupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamGroupError::GroupExists => {
                write!(f, "BUSYGROUP Consumer Group name already exists")
            }
            StreamGroupError::NoGroup => {
                write!(f, "NOGROUP No such consumer group")
            }
        }
    }
}

impl std::error::Error for StreamGroupError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_value_raw() {
        let sv = StringValue::new("hello");
        assert_eq!(sv.as_bytes().as_ref(), b"hello");
        assert!(sv.as_integer().is_none());
    }

    #[test]
    fn test_string_value_integer() {
        let sv = StringValue::new("42");
        assert_eq!(sv.as_integer(), Some(42));
        assert_eq!(sv.as_bytes().as_ref(), b"42");
    }

    #[test]
    fn test_string_value_from_integer() {
        let sv = StringValue::from_integer(-100);
        assert_eq!(sv.as_integer(), Some(-100));
        assert_eq!(sv.as_bytes().as_ref(), b"-100");
    }

    #[test]
    fn test_key_type_as_str() {
        assert_eq!(KeyType::String.as_str(), "string");
        assert_eq!(KeyType::None.as_str(), "none");
    }

    #[test]
    fn test_string_value_len() {
        let sv = StringValue::new("hello");
        assert_eq!(sv.len(), 5);
        assert!(!sv.is_empty());

        let sv_int = StringValue::from_integer(12345);
        assert_eq!(sv_int.len(), 5);

        let sv_empty = StringValue::new("");
        assert_eq!(sv_empty.len(), 0);
        assert!(sv_empty.is_empty());
    }

    #[test]
    fn test_string_value_append() {
        let mut sv = StringValue::new("hello");
        let new_len = sv.append(b" world");
        assert_eq!(new_len, 11);
        assert_eq!(sv.as_bytes().as_ref(), b"hello world");
    }

    #[test]
    fn test_string_value_get_range() {
        let sv = StringValue::new("hello world");

        // Positive indices
        assert_eq!(sv.get_range(0, 4).as_ref(), b"hello");
        assert_eq!(sv.get_range(6, 10).as_ref(), b"world");

        // Negative indices
        assert_eq!(sv.get_range(-5, -1).as_ref(), b"world");
        assert_eq!(sv.get_range(0, -1).as_ref(), b"hello world");

        // Out of range
        assert_eq!(sv.get_range(0, 100).as_ref(), b"hello world");

        // Empty result
        assert_eq!(sv.get_range(5, 2).as_ref(), b"");
    }

    #[test]
    fn test_string_value_set_range() {
        let mut sv = StringValue::new("hello world");
        let new_len = sv.set_range(6, b"WORLD");
        assert_eq!(new_len, 11);
        assert_eq!(sv.as_bytes().as_ref(), b"hello WORLD");

        // Extend past end
        let mut sv2 = StringValue::new("hello");
        let new_len = sv2.set_range(10, b"world");
        assert_eq!(new_len, 15);
        // Should have null padding between "hello" and "world"
        let bytes = sv2.as_bytes();
        assert_eq!(&bytes[0..5], b"hello");
        assert_eq!(&bytes[10..15], b"world");
    }

    #[test]
    fn test_string_value_increment() {
        let mut sv = StringValue::from_integer(10);
        assert_eq!(sv.increment(5).unwrap(), 15);
        assert_eq!(sv.as_integer(), Some(15));

        // Negative increment
        assert_eq!(sv.increment(-20).unwrap(), -5);
        assert_eq!(sv.as_integer(), Some(-5));
    }

    #[test]
    fn test_string_value_increment_overflow() {
        let mut sv = StringValue::from_integer(i64::MAX);
        assert_eq!(sv.increment(1), Err(IncrementError::Overflow));
    }

    #[test]
    fn test_string_value_increment_not_integer() {
        let mut sv = StringValue::new("not a number");
        assert_eq!(sv.increment(1), Err(IncrementError::NotInteger));
    }

    #[test]
    fn test_string_value_increment_float() {
        let mut sv = StringValue::from_integer(10);
        let result = sv.increment_float(0.5).unwrap();
        assert!((result - 10.5).abs() < 0.001);

        // Increment a float value
        let result = sv.increment_float(1.5).unwrap();
        assert!((result - 12.0).abs() < 0.001);
    }

    #[test]
    fn test_string_value_as_float() {
        let sv_int = StringValue::from_integer(42);
        assert_eq!(sv_int.as_float(), Some(42.0));

        let sv_float = StringValue::new("1.2345");
        assert!((sv_float.as_float().unwrap() - 1.2345).abs() < 0.0001);

        let sv_invalid = StringValue::new("not a float");
        assert!(sv_invalid.as_float().is_none());
    }

    // ==================== Stream Tests ====================

    #[test]
    fn test_stream_id_parse() {
        // Valid formats
        let id = StreamId::parse(b"1234567890123-0").unwrap();
        assert_eq!(id.ms, 1234567890123);
        assert_eq!(id.seq, 0);

        let id = StreamId::parse(b"1-99").unwrap();
        assert_eq!(id.ms, 1);
        assert_eq!(id.seq, 99);

        // Just milliseconds
        let id = StreamId::parse(b"1234").unwrap();
        assert_eq!(id.ms, 1234);
        assert_eq!(id.seq, 0);

        // Invalid formats
        assert!(StreamId::parse(b"invalid").is_err());
        assert!(StreamId::parse(b"-1-0").is_err());
        assert!(StreamId::parse(b"1--0").is_err());
    }

    #[test]
    fn test_stream_id_ordering() {
        let id1 = StreamId::new(1000, 0);
        let id2 = StreamId::new(1000, 1);
        let id3 = StreamId::new(1001, 0);

        assert!(id1 < id2);
        assert!(id2 < id3);
        assert!(id1 < id3);
    }

    #[test]
    fn test_stream_id_display() {
        let id = StreamId::new(1234567890123, 42);
        assert_eq!(id.to_string(), "1234567890123-42");
    }

    #[test]
    fn test_stream_id_range_bound() {
        // Min bound
        let bound = StreamId::parse_range_bound(b"-").unwrap();
        assert_eq!(bound, StreamRangeBound::Min);

        // Max bound
        let bound = StreamId::parse_range_bound(b"+").unwrap();
        assert_eq!(bound, StreamRangeBound::Max);

        // Inclusive bound
        let bound = StreamId::parse_range_bound(b"1000-0").unwrap();
        assert_eq!(bound, StreamRangeBound::Inclusive(StreamId::new(1000, 0)));

        // Exclusive bound
        let bound = StreamId::parse_range_bound(b"(1000-0").unwrap();
        assert_eq!(bound, StreamRangeBound::Exclusive(StreamId::new(1000, 0)));
    }

    #[test]
    fn test_stream_value_add_auto() {
        let mut stream = StreamValue::new();

        let fields = vec![(Bytes::from("field1"), Bytes::from("value1"))];
        let id = stream.add(StreamIdSpec::Auto, fields.clone()).unwrap();

        assert!(id.ms > 0);
        assert_eq!(stream.len(), 1);

        // Add another entry
        let id2 = stream.add(StreamIdSpec::Auto, fields).unwrap();
        assert!(id2 > id);
        assert_eq!(stream.len(), 2);
    }

    #[test]
    fn test_stream_value_add_explicit() {
        let mut stream = StreamValue::new();

        let fields = vec![(Bytes::from("field1"), Bytes::from("value1"))];
        let id = stream
            .add(
                StreamIdSpec::Explicit(StreamId::new(1000, 0)),
                fields.clone(),
            )
            .unwrap();

        assert_eq!(id, StreamId::new(1000, 0));
        assert_eq!(stream.len(), 1);

        // Add with larger ID
        let id2 = stream
            .add(
                StreamIdSpec::Explicit(StreamId::new(1000, 1)),
                fields.clone(),
            )
            .unwrap();
        assert_eq!(id2, StreamId::new(1000, 1));

        // Cannot add with smaller ID
        let result = stream.add(StreamIdSpec::Explicit(StreamId::new(999, 0)), fields);
        assert!(result.is_err());
    }

    #[test]
    fn test_stream_value_range() {
        let mut stream = StreamValue::new();

        for i in 0..5 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream
                .add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields)
                .unwrap();
        }

        // Full range
        let entries = stream.range(StreamRangeBound::Min, StreamRangeBound::Max, None);
        assert_eq!(entries.len(), 5);

        // Range with count
        let entries = stream.range(StreamRangeBound::Min, StreamRangeBound::Max, Some(2));
        assert_eq!(entries.len(), 2);

        // Specific range
        let entries = stream.range(
            StreamRangeBound::Inclusive(StreamId::new(1001, 0)),
            StreamRangeBound::Inclusive(StreamId::new(1003, 0)),
            None,
        );
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_stream_value_delete() {
        let mut stream = StreamValue::new();

        for i in 0..5 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream
                .add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields)
                .unwrap();
        }

        let deleted = stream.delete(&[StreamId::new(1001, 0), StreamId::new(1003, 0)]);
        assert_eq!(deleted, 2);
        assert_eq!(stream.len(), 3);

        // Try to delete non-existent
        let deleted = stream.delete(&[StreamId::new(9999, 0)]);
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_stream_value_trim_maxlen() {
        let mut stream = StreamValue::new();

        for i in 0..10 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream
                .add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields)
                .unwrap();
        }

        let trimmed = stream.trim(StreamTrimOptions {
            strategy: StreamTrimStrategy::MaxLen(5),
            mode: StreamTrimMode::Exact,
            limit: 0,
        });

        assert_eq!(trimmed, 5);
        assert_eq!(stream.len(), 5);

        // First entry should now be 1005-0
        assert_eq!(stream.first_id(), Some(StreamId::new(1005, 0)));
    }

    #[test]
    fn test_stream_value_trim_minid() {
        let mut stream = StreamValue::new();

        for i in 0..10 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream
                .add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields)
                .unwrap();
        }

        let trimmed = stream.trim(StreamTrimOptions {
            strategy: StreamTrimStrategy::MinId(StreamId::new(1005, 0)),
            mode: StreamTrimMode::Exact,
            limit: 0,
        });

        assert_eq!(trimmed, 5);
        assert_eq!(stream.len(), 5);
        assert_eq!(stream.first_id(), Some(StreamId::new(1005, 0)));
    }

    #[test]
    fn test_consumer_group_basic() {
        let mut stream = StreamValue::new();

        // Add some entries
        for i in 0..5 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream
                .add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields)
                .unwrap();
        }

        // Create a group
        stream
            .create_group(Bytes::from("mygroup"), StreamId::new(0, 0), None)
            .unwrap();

        // Try to create duplicate group
        let result = stream.create_group(Bytes::from("mygroup"), StreamId::new(0, 0), None);
        assert!(result.is_err());

        // Get the group
        let group = stream.get_group(b"mygroup").unwrap();
        assert_eq!(group.name, Bytes::from("mygroup"));
        assert_eq!(group.last_delivered_id, StreamId::new(0, 0));

        // Destroy the group
        assert!(stream.destroy_group(b"mygroup"));
        assert!(!stream.destroy_group(b"mygroup")); // Already destroyed
    }

    #[test]
    fn test_consumer_group_pending() {
        let mut stream = StreamValue::new();

        // Add some entries
        for i in 0..5 {
            let fields = vec![(Bytes::from("field"), Bytes::from(format!("value{}", i)))];
            stream
                .add(StreamIdSpec::Explicit(StreamId::new(1000 + i, 0)), fields)
                .unwrap();
        }

        // Create a group and add pending entries
        stream
            .create_group(Bytes::from("mygroup"), StreamId::new(0, 0), None)
            .unwrap();

        let group = stream.get_group_mut(b"mygroup").unwrap();
        group.get_or_create_consumer(Bytes::from("consumer1"));
        group.add_pending(StreamId::new(1000, 0), Bytes::from("consumer1"));
        group.add_pending(StreamId::new(1001, 0), Bytes::from("consumer1"));

        assert_eq!(group.pending_count(), 2);

        // Acknowledge one
        let acked = group.ack(&[StreamId::new(1000, 0)]);
        assert_eq!(acked, 1);
        assert_eq!(group.pending_count(), 1);

        // Acknowledge non-existent
        let acked = group.ack(&[StreamId::new(9999, 0)]);
        assert_eq!(acked, 0);
    }

    #[test]
    fn test_consumer_group_consumer_management() {
        let mut group = ConsumerGroup::new(Bytes::from("mygroup"), StreamId::new(0, 0));

        // Create consumers
        assert!(group.create_consumer(Bytes::from("consumer1")));
        assert!(!group.create_consumer(Bytes::from("consumer1"))); // Already exists
        assert!(group.create_consumer(Bytes::from("consumer2")));

        assert_eq!(group.consumers.len(), 2);

        // Delete consumer
        let pending_deleted = group.delete_consumer(b"consumer1");
        assert_eq!(pending_deleted, 0);
        assert_eq!(group.consumers.len(), 1);

        // Delete with pending entries
        group.get_or_create_consumer(Bytes::from("consumer2"));
        group.add_pending(StreamId::new(1000, 0), Bytes::from("consumer2"));
        let pending_deleted = group.delete_consumer(b"consumer2");
        assert_eq!(pending_deleted, 1);
    }

    #[test]
    fn test_stream_value_memory_size() {
        let mut stream = StreamValue::new();

        let initial_size = stream.memory_size();

        // Add an entry
        let fields = vec![(Bytes::from("field"), Bytes::from("value"))];
        stream
            .add(StreamIdSpec::Explicit(StreamId::new(1000, 0)), fields)
            .unwrap();

        let after_add = stream.memory_size();
        assert!(after_add > initial_size);

        // Add a group
        stream
            .create_group(Bytes::from("mygroup"), StreamId::new(0, 0), None)
            .unwrap();

        let after_group = stream.memory_size();
        assert!(after_group > after_add);
    }

    #[test]
    fn test_value_stream_accessors() {
        let value = Value::stream();

        // Test as_stream
        assert!(value.as_stream().is_some());
        assert!(value.as_string().is_none());

        // Test key_type
        assert_eq!(value.key_type(), KeyType::Stream);
    }

    // ==================== Property Tests for INCR/DECR ====================

    mod proptest_increment {
        use super::*;
        use proptest::prelude::*;

        /// Strategy for i64 values with room for operations (avoid overflow in tests)
        fn safe_i64() -> impl Strategy<Value = i64> {
            (i64::MIN / 2)..=(i64::MAX / 2)
        }

        /// Strategy for delta values that won't cause immediate overflow
        fn safe_delta() -> impl Strategy<Value = i64> {
            -1_000_000i64..=1_000_000i64
        }

        /// Strategy for small f64 values
        fn small_f64() -> impl Strategy<Value = f64> {
            -1e10f64..1e10f64
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(1000))]

            /// Roundtrip invariant: increment(d) followed by increment(-d) returns to original
            #[test]
            fn test_increment_roundtrip(initial in safe_i64(), delta in safe_delta()) {
                let mut sv = StringValue::from_integer(initial);

                // Skip if delta would cause overflow
                if initial.checked_add(delta).is_none() {
                    return Ok(());
                }

                let after_inc = sv.increment(delta).unwrap();
                prop_assert_eq!(after_inc, initial + delta);

                let after_dec = sv.increment(-delta).unwrap();
                prop_assert_eq!(after_dec, initial);
                prop_assert_eq!(sv.as_integer(), Some(initial));
            }

            /// Overflow at MAX: increment(+n) on values near i64::MAX returns Overflow error
            #[test]
            fn test_increment_overflow_at_max(n in 1i64..=1000i64) {
                let mut sv = StringValue::from_integer(i64::MAX);
                let result = sv.increment(n);
                prop_assert_eq!(result, Err(IncrementError::Overflow));
            }

            /// Underflow at MIN: increment(-n) on values near i64::MIN returns Overflow error
            #[test]
            fn test_increment_underflow_at_min(n in 1i64..=1000i64) {
                let mut sv = StringValue::from_integer(i64::MIN);
                let result = sv.increment(-n);
                prop_assert_eq!(result, Err(IncrementError::Overflow));
            }

            /// Type preservation: Integer encoding stays Integer after increment
            #[test]
            fn test_increment_type_preservation(initial in safe_i64(), delta in safe_delta()) {
                let mut sv = StringValue::from_integer(initial);

                // Skip if delta would cause overflow
                if initial.checked_add(delta).is_none() {
                    return Ok(());
                }

                sv.increment(delta).unwrap();

                // After increment, the value should still be accessible as an integer
                prop_assert!(sv.as_integer().is_some());
            }

            /// Commutativity: Order of increments doesn't affect final result
            #[test]
            fn test_increment_commutativity(
                initial in safe_i64(),
                delta1 in -10_000i64..=10_000i64,
                delta2 in -10_000i64..=10_000i64
            ) {
                // Skip if any operation would overflow
                let Some(step1) = initial.checked_add(delta1) else { return Ok(()); };
                let Some(final1) = step1.checked_add(delta2) else { return Ok(()); };

                let Some(step2) = initial.checked_add(delta2) else { return Ok(()); };
                let Some(final2) = step2.checked_add(delta1) else { return Ok(()); };

                let mut sv1 = StringValue::from_integer(initial);
                sv1.increment(delta1).unwrap();
                sv1.increment(delta2).unwrap();

                let mut sv2 = StringValue::from_integer(initial);
                sv2.increment(delta2).unwrap();
                sv2.increment(delta1).unwrap();

                prop_assert_eq!(sv1.as_integer(), sv2.as_integer());
                prop_assert_eq!(sv1.as_integer(), Some(final1));
                prop_assert_eq!(final1, final2);
            }

            /// Associativity: increment(n) == increment(1) repeated n times (for small n)
            #[test]
            fn test_increment_associativity(initial in safe_i64(), n in 1i64..=100i64) {
                // Skip if operations would overflow
                if initial.checked_add(n).is_none() {
                    return Ok(());
                }

                // Single increment by n
                let mut sv1 = StringValue::from_integer(initial);
                let result1 = sv1.increment(n).unwrap();

                // n increments by 1
                let mut sv2 = StringValue::from_integer(initial);
                for _ in 0..n {
                    sv2.increment(1).unwrap();
                }

                prop_assert_eq!(result1, sv2.as_integer().unwrap());
            }

            /// Float roundtrip: increment_float(d) followed by increment_float(-d) within epsilon
            #[test]
            fn test_increment_float_roundtrip(initial in small_f64(), delta in small_f64()) {
                // Skip zero delta to avoid precision issues
                if delta.abs() < 1e-15 {
                    return Ok(());
                }

                let mut sv = StringValue::new(initial.to_string());

                // Do increment and decrement
                let after_inc = sv.increment_float(delta);
                if after_inc.is_err() {
                    // Skip if increment causes overflow (infinity)
                    return Ok(());
                }

                let after_dec = sv.increment_float(-delta);
                if after_dec.is_err() {
                    return Ok(());
                }

                let result = after_dec.unwrap();

                // Check within epsilon (relative for large values, absolute for small)
                let epsilon = if initial.abs() > 1.0 {
                    initial.abs() * 1e-10
                } else {
                    1e-10
                };

                prop_assert!(
                    (result - initial).abs() < epsilon,
                    "Expected {} to be within {} of {}, but difference was {}",
                    result, epsilon, initial, (result - initial).abs()
                );
            }

            /// NaN rejection: increment_float rejects NaN input
            #[test]
            fn test_increment_float_rejects_nan(_dummy in 0i32..1i32) {
                let mut sv = StringValue::new("nan");
                let result = sv.increment_float(1.0);
                // NaN values should fail to parse as float
                prop_assert!(result.is_err());
            }

            /// Infinity rejection: increment_float rejects infinity result
            #[test]
            fn test_increment_float_rejects_infinity_result(_dummy in 0i32..1i32) {
                let mut sv = StringValue::new(f64::MAX.to_string());
                let result = sv.increment_float(f64::MAX);
                prop_assert_eq!(result, Err(IncrementError::Overflow));
            }

            /// NotInteger error: Non-numeric strings return NotInteger error
            #[test]
            fn test_increment_not_integer(s in "[a-zA-Z]{1,10}") {
                let mut sv = StringValue::new(s.clone());
                let result = sv.increment(1);
                prop_assert_eq!(result, Err(IncrementError::NotInteger));
            }

            /// NotFloat error: Non-numeric strings return NotFloat error for float increment
            #[test]
            fn test_increment_float_not_float(s in "[a-zA-Z]{1,10}") {
                let mut sv = StringValue::new(s.clone());
                let result = sv.increment_float(1.0);
                prop_assert_eq!(result, Err(IncrementError::NotFloat));
            }
        }
    }

    // ========================================================================
    // Hash listpack encoding tests
    // ========================================================================

    mod hash_listpack {
        use super::*;

        const T: ListpackThresholds = ListpackThresholds::DEFAULT_HASH;

        #[test]
        fn new_hash_is_listpack() {
            let h = HashValue::new();
            assert!(h.is_listpack());
            assert!(h.is_empty());
            assert_eq!(h.len(), 0);
        }

        #[test]
        fn basic_set_get_remove() {
            let mut h = HashValue::new();
            assert!(h.set(Bytes::from("a"), Bytes::from("1"), T));
            assert!(!h.set(Bytes::from("a"), Bytes::from("2"), T)); // update
            assert_eq!(h.len(), 1);
            assert!(h.is_listpack());
            assert_eq!(h.get(b"a"), Some(Bytes::from("2")));
            assert!(h.contains(b"a"));
            assert!(!h.contains(b"z"));

            assert!(h.remove(b"a"));
            assert!(!h.remove(b"a"));
            assert!(h.is_empty());
            assert!(h.is_listpack());
        }

        #[test]
        fn set_nx() {
            let mut h = HashValue::new();
            assert!(h.set_nx(Bytes::from("k"), Bytes::from("v1"), T));
            assert!(!h.set_nx(Bytes::from("k"), Bytes::from("v2"), T));
            assert_eq!(h.get(b"k"), Some(Bytes::from("v1")));
        }

        #[test]
        fn iterators() {
            let mut h = HashValue::new();
            h.set(Bytes::from("a"), Bytes::from("1"), T);
            h.set(Bytes::from("b"), Bytes::from("2"), T);

            let mut keys: Vec<Bytes> = h.keys().collect();
            keys.sort();
            assert_eq!(keys, vec![Bytes::from("a"), Bytes::from("b")]);

            let mut vals: Vec<Bytes> = h.values().collect();
            vals.sort();
            assert_eq!(vals, vec![Bytes::from("1"), Bytes::from("2")]);

            let mut pairs: Vec<(Bytes, Bytes)> = h.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(
                pairs,
                vec![
                    (Bytes::from("a"), Bytes::from("1")),
                    (Bytes::from("b"), Bytes::from("2")),
                ]
            );
        }

        #[test]
        fn incr_by() {
            let mut h = HashValue::new();
            assert_eq!(h.incr_by(Bytes::from("x"), 5, T), Ok(5));
            assert_eq!(h.incr_by(Bytes::from("x"), -3, T), Ok(2));
            assert!(h.is_listpack());
        }

        #[test]
        fn incr_by_float() {
            let mut h = HashValue::new();
            assert_eq!(h.incr_by_float(Bytes::from("x"), 1.5, T), Ok(1.5));
            assert_eq!(h.incr_by_float(Bytes::from("x"), 0.5, T), Ok(2.0));
            assert!(h.is_listpack());
        }

        #[test]
        fn promotes_on_entry_count() {
            let t = ListpackThresholds {
                max_entries: 3,
                max_value_bytes: 64,
            };
            let mut h = HashValue::new();
            h.set(Bytes::from("a"), Bytes::from("1"), t);
            h.set(Bytes::from("b"), Bytes::from("2"), t);
            h.set(Bytes::from("c"), Bytes::from("3"), t);
            assert!(h.is_listpack()); // at threshold, not over

            h.set(Bytes::from("d"), Bytes::from("4"), t);
            assert!(!h.is_listpack()); // promoted
            assert_eq!(h.len(), 4);
            assert_eq!(h.get(b"d"), Some(Bytes::from("4")));
        }

        #[test]
        fn promotes_on_value_size() {
            let t = ListpackThresholds {
                max_entries: 128,
                max_value_bytes: 4,
            };
            let mut h = HashValue::new();
            h.set(Bytes::from("a"), Bytes::from("ok"), t);
            assert!(h.is_listpack());

            h.set(Bytes::from("b"), Bytes::from("toolong"), t);
            assert!(!h.is_listpack()); // value exceeds max_value_bytes
            assert_eq!(h.get(b"b"), Some(Bytes::from("toolong")));
        }

        #[test]
        fn promotes_on_field_size() {
            let t = ListpackThresholds {
                max_entries: 128,
                max_value_bytes: 4,
            };
            let mut h = HashValue::new();
            h.set(Bytes::from("longfield"), Bytes::from("v"), t);
            assert!(!h.is_listpack()); // field exceeds max_value_bytes
        }

        #[test]
        fn update_existing_does_not_promote() {
            let t = ListpackThresholds {
                max_entries: 2,
                max_value_bytes: 64,
            };
            let mut h = HashValue::new();
            h.set(Bytes::from("a"), Bytes::from("1"), t);
            h.set(Bytes::from("b"), Bytes::from("2"), t);
            assert!(h.is_listpack());

            // Updating existing field should NOT promote (count stays at 2)
            h.set(Bytes::from("a"), Bytes::from("updated"), t);
            assert!(h.is_listpack());
            assert_eq!(h.get(b"a"), Some(Bytes::from("updated")));
        }

        #[test]
        fn from_entries_picks_encoding() {
            let entries = vec![
                (Bytes::from("a"), Bytes::from("1")),
                (Bytes::from("b"), Bytes::from("2")),
            ];
            let h = HashValue::from_entries(entries, T);
            assert!(h.is_listpack());
            assert_eq!(h.len(), 2);

            // Large entry count forces HashMap
            let entries: Vec<_> = (0..200)
                .map(|i| (Bytes::from(format!("k{i}")), Bytes::from(format!("v{i}"))))
                .collect();
            let h = HashValue::from_entries(entries, T);
            assert!(!h.is_listpack());
            assert_eq!(h.len(), 200);
        }

        #[test]
        fn to_vec_roundtrips() {
            let mut h = HashValue::new();
            h.set(Bytes::from("a"), Bytes::from("1"), T);
            h.set(Bytes::from("b"), Bytes::from("2"), T);
            let v = h.to_vec();
            let h2 = HashValue::from_entries(v, T);
            assert_eq!(h2.get(b"a"), Some(Bytes::from("1")));
            assert_eq!(h2.get(b"b"), Some(Bytes::from("2")));
        }

        #[test]
        fn memory_size_listpack_smaller() {
            let t = ListpackThresholds {
                max_entries: 200,
                max_value_bytes: 64,
            };
            let mut lp = HashValue::new();
            for i in 0..50 {
                lp.set(
                    Bytes::from(format!("k{i:02}")),
                    Bytes::from(format!("v{i:02}")),
                    t,
                );
            }
            assert!(lp.is_listpack());

            // Force a HashMap version with same data
            let ht = HashValue::from_entries(
                lp.to_vec(),
                ListpackThresholds {
                    max_entries: 0,
                    max_value_bytes: 0,
                },
            );
            assert!(!ht.is_listpack());

            assert!(
                lp.memory_size() < ht.memory_size(),
                "listpack ({}) should be smaller than HashMap ({})",
                lp.memory_size(),
                ht.memory_size()
            );
        }

        #[test]
        fn random_fields_works() {
            let mut h = HashValue::new();
            h.set(Bytes::from("a"), Bytes::from("1"), T);
            h.set(Bytes::from("b"), Bytes::from("2"), T);

            let r = h.random_fields(1, true);
            assert_eq!(r.len(), 1);
            assert!(r[0].1.is_some());

            let r = h.random_fields(-4, false);
            assert_eq!(r.len(), 4); // allows duplicates
        }
    }

    // ========================================================================
    // Set listpack encoding tests
    // ========================================================================

    mod set_listpack {
        use super::*;

        const T: ListpackThresholds = ListpackThresholds::DEFAULT_SET;

        #[test]
        fn new_set_is_listpack() {
            let s = SetValue::new();
            assert!(s.is_listpack());
            assert!(s.is_empty());
        }

        #[test]
        fn basic_add_remove_contains() {
            let mut s = SetValue::new();
            assert!(s.add(Bytes::from("a"), T));
            assert!(!s.add(Bytes::from("a"), T)); // duplicate
            assert_eq!(s.len(), 1);
            assert!(s.is_listpack());
            assert!(s.contains(b"a"));

            assert!(s.remove(b"a"));
            assert!(!s.remove(b"a"));
            assert!(s.is_empty());
            assert!(s.is_listpack());
        }

        #[test]
        fn members_iterator() {
            let mut s = SetValue::new();
            s.add(Bytes::from("x"), T);
            s.add(Bytes::from("y"), T);

            let mut members: Vec<Bytes> = s.members().collect();
            members.sort();
            assert_eq!(members, vec![Bytes::from("x"), Bytes::from("y")]);
        }

        #[test]
        fn promotes_on_entry_count() {
            let t = ListpackThresholds {
                max_entries: 2,
                max_value_bytes: 64,
            };
            let mut s = SetValue::new();
            s.add(Bytes::from("a"), t);
            s.add(Bytes::from("b"), t);
            assert!(s.is_listpack());

            s.add(Bytes::from("c"), t);
            assert!(!s.is_listpack());
            assert_eq!(s.len(), 3);
            assert!(s.contains(b"c"));
        }

        #[test]
        fn promotes_on_member_size() {
            let t = ListpackThresholds {
                max_entries: 128,
                max_value_bytes: 3,
            };
            let mut s = SetValue::new();
            s.add(Bytes::from("ok"), t);
            assert!(s.is_listpack());

            s.add(Bytes::from("toolong"), t);
            assert!(!s.is_listpack());
        }

        #[test]
        fn duplicate_does_not_promote() {
            let t = ListpackThresholds {
                max_entries: 2,
                max_value_bytes: 64,
            };
            let mut s = SetValue::new();
            s.add(Bytes::from("a"), t);
            s.add(Bytes::from("b"), t);
            assert!(s.is_listpack());

            // Adding duplicate should NOT promote
            s.add(Bytes::from("a"), t);
            assert!(s.is_listpack());
            assert_eq!(s.len(), 2);
        }

        #[test]
        fn set_operations() {
            let mut s1 = SetValue::new();
            s1.add(Bytes::from("a"), T);
            s1.add(Bytes::from("b"), T);
            s1.add(Bytes::from("c"), T);

            let mut s2 = SetValue::new();
            s2.add(Bytes::from("b"), T);
            s2.add(Bytes::from("c"), T);
            s2.add(Bytes::from("d"), T);

            let union = s1.union([&s2].into_iter());
            assert_eq!(union.len(), 4);

            let inter = s1.intersection([&s2].into_iter());
            assert_eq!(inter.len(), 2);
            assert!(inter.contains(b"b"));
            assert!(inter.contains(b"c"));

            let diff = s1.difference([&s2].into_iter());
            assert_eq!(diff.len(), 1);
            assert!(diff.contains(b"a"));
        }

        #[test]
        fn pop_works() {
            let mut s = SetValue::new();
            s.add(Bytes::from("x"), T);
            s.add(Bytes::from("y"), T);

            let popped = s.pop().unwrap();
            assert!(popped == "x" || popped == "y");
            assert_eq!(s.len(), 1);
        }

        #[test]
        fn from_members_picks_encoding() {
            let members = vec![Bytes::from("a"), Bytes::from("b")];
            let s = SetValue::from_members(members, T);
            assert!(s.is_listpack());

            let members: Vec<_> = (0..200).map(|i| Bytes::from(format!("m{i}"))).collect();
            let s = SetValue::from_members(members, T);
            assert!(!s.is_listpack());
            assert_eq!(s.len(), 200);
        }

        #[test]
        fn memory_size_listpack_smaller() {
            let t = ListpackThresholds {
                max_entries: 200,
                max_value_bytes: 64,
            };
            let mut lp = SetValue::new();
            for i in 0..50 {
                lp.add(Bytes::from(format!("member{i:02}")), t);
            }
            assert!(lp.is_listpack());

            let ht = SetValue::from_members(
                lp.to_vec(),
                ListpackThresholds {
                    max_entries: 0,
                    max_value_bytes: 0,
                },
            );
            assert!(!ht.is_listpack());

            assert!(
                lp.memory_size() < ht.memory_size(),
                "listpack ({}) should be smaller than HashSet ({})",
                lp.memory_size(),
                ht.memory_size()
            );
        }

        #[test]
        fn random_members_works() {
            let mut s = SetValue::new();
            s.add(Bytes::from("a"), T);
            s.add(Bytes::from("b"), T);

            let r = s.random_members(1);
            assert_eq!(r.len(), 1);

            let r = s.random_members(-4);
            assert_eq!(r.len(), 4);
        }
    }
}
