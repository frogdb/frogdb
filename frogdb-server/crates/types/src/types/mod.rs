//! Value types and key metadata.

mod hash;
mod list;
mod set;
mod sorted_set;
mod stream;
mod string_value;

pub use hash::*;
pub use list::*;
pub use set::*;
pub use sorted_set::{
    LexBound, ScoreBound, ScoreIndexBackend, SortedSetValue, ZAddResult, set_default_score_index,
};
pub use stream::*;
pub use string_value::{IncrementError, StringValue};

use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bitvec::prelude::*;

use crate::bloom::{BloomFilterValue, BloomLayer};
use crate::cms::CountMinSketchValue;
use crate::cuckoo::{CuckooFilterValue, CuckooLayer};
use crate::hyperloglog::HyperLogLogValue;
use crate::json::JsonValue;
use crate::tdigest::TDigestValue;
use crate::timeseries::{CompressedChunk, DuplicatePolicy, TimeSeriesValue};
use crate::topk::TopKValue;
use crate::vectorset::VectorSetValue;

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
    /// Vector set for approximate nearest neighbor search.
    VectorSet(Box<VectorSetValue>),
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
    /// Try to get as a vector set value.
    pub fn as_vectorset(&self) -> Option<&VectorSetValue> {
        match self {
            Value::VectorSet(v) => Some(v),
            _ => None,
        }
    }

    /// Try to get as a mutable vector set value.
    pub fn as_vectorset_mut(&mut self) -> Option<&mut VectorSetValue> {
        match self {
            Value::VectorSet(v) => Some(v),
            _ => None,
        }
    }
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
            Value::VectorSet(_) => KeyType::VectorSet,
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
            Value::VectorSet(vs) => vs.memory_size(),
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
            Value::VectorSet(vs) => {
                let mut buf = Vec::new();
                // metric(1) + quant(1) + dim(4) + original_dim(4) + m(4) + ef(4) + next_id(8) + uid(8)
                buf.push(vs.metric() as u8);
                buf.push(vs.quantization() as u8);
                buf.extend_from_slice(&(vs.dim() as u32).to_le_bytes());
                buf.extend_from_slice(&(vs.original_dim() as u32).to_le_bytes());
                buf.extend_from_slice(&(vs.m() as u32).to_le_bytes());
                buf.extend_from_slice(&(vs.ef_construction() as u32).to_le_bytes());
                buf.extend_from_slice(&vs.next_id().to_le_bytes());
                buf.extend_from_slice(&vs.uid().to_le_bytes());
                // projection matrix
                let proj = vs.projection_matrix();
                buf.extend_from_slice(&(proj.len() as u32).to_le_bytes());
                for &v in proj {
                    buf.extend_from_slice(&v.to_le_bytes());
                }
                // elements
                let count = vs.card();
                buf.extend_from_slice(&(count as u32).to_le_bytes());
                for (name, id, vector, attr) in vs.elements() {
                    buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
                    buf.extend_from_slice(name);
                    buf.extend_from_slice(&id.to_le_bytes());
                    buf.extend_from_slice(&(vector.len() as u32).to_le_bytes());
                    for &v in vector {
                        buf.extend_from_slice(&v.to_le_bytes());
                    }
                    let has_attr = attr.is_some();
                    buf.push(has_attr as u8);
                    if let Some(a) = attr {
                        let attr_str = a.to_string();
                        buf.extend_from_slice(&(attr_str.len() as u32).to_le_bytes());
                        buf.extend_from_slice(attr_str.as_bytes());
                    }
                }
                ("vectorset", Bytes::from(buf))
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
                            let fp = u16::from_le_bytes(data[pos..pos + 2].try_into().ok()?);
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
                    let item_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
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
                let num_unmerged = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
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
            b"vectorset" => {
                use crate::vectorset::{VectorDistanceMetric, VectorQuantization};

                let mut pos = 0;
                if pos + 34 > data.len() {
                    return None;
                }
                let metric = match data[pos] {
                    0 => VectorDistanceMetric::Cosine,
                    1 => VectorDistanceMetric::L2,
                    2 => VectorDistanceMetric::InnerProduct,
                    _ => return None,
                };
                pos += 1;
                let quant = match data[pos] {
                    0 => VectorQuantization::NoQuant,
                    1 => VectorQuantization::Q8,
                    2 => VectorQuantization::Bin,
                    _ => return None,
                };
                pos += 1;
                let dim = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;
                let original_dim = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;
                let m = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;
                let ef = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;
                let next_id = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                let uid = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;

                // projection matrix
                if pos + 4 > data.len() {
                    return None;
                }
                let proj_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;
                let mut projection_matrix = Vec::with_capacity(proj_len);
                for _ in 0..proj_len {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let v = f32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                    pos += 4;
                    projection_matrix.push(v);
                }

                // elements
                if pos + 4 > data.len() {
                    return None;
                }
                let elem_count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;

                let mut elements = Vec::with_capacity(elem_count);
                for _ in 0..elem_count {
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let name_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + name_len > data.len() {
                        return None;
                    }
                    let name = Bytes::copy_from_slice(&data[pos..pos + name_len]);
                    pos += name_len;

                    if pos + 8 > data.len() {
                        return None;
                    }
                    let id = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;

                    if pos + 4 > data.len() {
                        return None;
                    }
                    let vec_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    let mut vector = Vec::with_capacity(vec_len);
                    for _ in 0..vec_len {
                        if pos + 4 > data.len() {
                            return None;
                        }
                        let v = f32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                        pos += 4;
                        vector.push(v);
                    }

                    if pos + 1 > data.len() {
                        return None;
                    }
                    let has_attr = data[pos] != 0;
                    pos += 1;
                    let attr = if has_attr {
                        if pos + 4 > data.len() {
                            return None;
                        }
                        let attr_len =
                            u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                        pos += 4;
                        if pos + attr_len > data.len() {
                            return None;
                        }
                        let attr_str = std::str::from_utf8(&data[pos..pos + attr_len]).ok()?;
                        pos += attr_len;
                        Some(serde_json::from_str(attr_str).ok()?)
                    } else {
                        None
                    };

                    elements.push((name, id, vector, attr));
                }

                VectorSetValue::from_parts(
                    metric,
                    quant,
                    dim,
                    original_dim,
                    m,
                    ef,
                    next_id,
                    uid,
                    projection_matrix,
                    elements,
                )
                .ok()
                .map(|vs| Value::VectorSet(Box::new(vs)))
            }
            _ => None,
        }
    }
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
    /// Vector set type.
    VectorSet,
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
            KeyType::VectorSet => "vectorset",
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
pub(super) enum EitherIter<L, R> {
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
