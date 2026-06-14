//! The per-marker codec registry: the single home for each persisted type's
//! marker, encode half, and decode half.
//!
//! Previously a type's encode lived in `serialize_value`, its decode in
//! `deserialize_value` (~70 lines apart, joined only by a hand-kept `u8`), and
//! its byte layout in a per-type module — five sites with no structural link, so
//! deleting a decode arm compiled cleanly and lost data on the next load. Here
//! each marker is one [`TypeCodec`] value; dispatch goes through the registry, and
//! [`decode_for`] matches the closed [`TypeMarker`] enum with **no wildcard**, so a
//! marker without a decode arm is a compile error.
//!
//! Wire compatibility: the per-type `serialize_*`/`deserialize_*` functions are
//! reused verbatim (their byte layout is untouched); a codec's `encode` just drops
//! the redundant marker the dispatcher supplies from `codec.marker`.

use super::*;

use collections::{
    deserialize_hash, deserialize_hash_with_field_expiry, deserialize_list, deserialize_set,
    deserialize_sorted_set, serialize_hash, serialize_hash_with_field_expiry, serialize_list,
    serialize_set, serialize_sorted_set,
};
use probabilistic::{
    deserialize_bloom_filter, deserialize_cms, deserialize_cuckoo_filter, deserialize_hyperloglog,
    deserialize_tdigest, deserialize_topk, serialize_bloom_filter, serialize_cms,
    serialize_cuckoo_filter, serialize_hyperloglog, serialize_tdigest, serialize_topk,
};
use search::{deserialize_json, deserialize_vectorset, serialize_json, serialize_vectorset};
use stream::{deserialize_stream, serialize_stream};
use string::{deserialize_string_int, deserialize_string_raw, serialize_string};
use timeseries::{deserialize_timeseries, serialize_timeseries};

/// One marker's full on-the-wire contract: which byte it writes, how a [`Value`]
/// becomes a payload (or declines, for the markers that share a `Value` variant),
/// and how a payload becomes a [`Value`].
pub(super) struct TypeCodec {
    /// The on-disk/replication marker byte this codec owns.
    pub(super) marker: TypeMarker,

    /// Encode `value` to a payload if this codec applies, else `None`. The
    /// `Some`/`None` answer resolves the markers that share a `Value` variant:
    /// `StringInt`/`StringRaw` split on integer-encoding, `Hash`/`HashWithFieldExpiry`
    /// split on whether any field has an expiry. Because each codec claims only its
    /// own case, encode dispatch is order-independent.
    pub(super) encode: fn(&Value) -> Option<Vec<u8>>,

    /// Decode a payload into a `Value`.
    pub(super) decode: fn(&[u8]) -> Result<Value, SerializationError>,
}

// --- encode halves -------------------------------------------------------------
//
// Each claims only the value(s) that serialize to its own marker, so the registry
// scan in `serialize_value` is order-independent.

fn encode_string_raw(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::String(sv) => {
            let (marker, payload) = serialize_string(sv);
            (marker == TypeMarker::StringRaw).then_some(payload)
        }
        _ => None,
    }
}

fn encode_string_int(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::String(sv) => {
            let (marker, payload) = serialize_string(sv);
            (marker == TypeMarker::StringInt).then_some(payload)
        }
        _ => None,
    }
}

fn encode_sorted_set(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::SortedSet(zset) => Some(serialize_sorted_set(zset).1),
        _ => None,
    }
}

fn encode_hash(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Hash(hash) if !hash.has_field_expiries() => Some(serialize_hash(hash).1),
        _ => None,
    }
}

fn encode_hash_with_field_expiry(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Hash(hash) if hash.has_field_expiries() => {
            Some(serialize_hash_with_field_expiry(hash).1)
        }
        _ => None,
    }
}

fn encode_list(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::List(list) => Some(serialize_list(list).1),
        _ => None,
    }
}

fn encode_set(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Set(set) => Some(serialize_set(set).1),
        _ => None,
    }
}

fn encode_stream(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Stream(stream) => Some(serialize_stream(stream).1),
        _ => None,
    }
}

fn encode_bloom(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::BloomFilter(bf) => Some(serialize_bloom_filter(bf).1),
        _ => None,
    }
}

fn encode_hyperloglog(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::HyperLogLog(hll) => Some(serialize_hyperloglog(hll).1),
        _ => None,
    }
}

fn encode_timeseries(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::TimeSeries(ts) => Some(serialize_timeseries(ts).1),
        _ => None,
    }
}

fn encode_json(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Json(json) => Some(serialize_json(json).1),
        _ => None,
    }
}

fn encode_cuckoo(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::CuckooFilter(cf) => Some(serialize_cuckoo_filter(cf).1),
        _ => None,
    }
}

fn encode_topk(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::TopK(tk) => Some(serialize_topk(tk).1),
        _ => None,
    }
}

fn encode_tdigest(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::TDigest(td) => Some(serialize_tdigest(td).1),
        _ => None,
    }
}

fn encode_cms(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::CountMinSketch(cms) => Some(serialize_cms(cms).1),
        _ => None,
    }
}

fn encode_vectorset(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::VectorSet(vs) => Some(serialize_vectorset(vs).1),
        _ => None,
    }
}

// --- decode halves -------------------------------------------------------------

fn decode_string_raw_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::String(deserialize_string_raw(payload)))
}

fn decode_string_int_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::String(deserialize_string_int(payload)?))
}

fn decode_sorted_set_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::SortedSet(deserialize_sorted_set(payload)?))
}

fn decode_hash_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::Hash(deserialize_hash(payload)?))
}

fn decode_hash_with_field_expiry_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::Hash(deserialize_hash_with_field_expiry(payload)?))
}

fn decode_list_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::List(deserialize_list(payload)?))
}

fn decode_set_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::Set(deserialize_set(payload)?))
}

fn decode_stream_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::Stream(deserialize_stream(payload)?))
}

fn decode_bloom_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::BloomFilter(deserialize_bloom_filter(payload)?))
}

fn decode_hyperloglog_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::HyperLogLog(deserialize_hyperloglog(payload)?))
}

fn decode_timeseries_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::TimeSeries(deserialize_timeseries(payload)?))
}

fn decode_json_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::Json(deserialize_json(payload)?))
}

fn decode_cuckoo_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::CuckooFilter(deserialize_cuckoo_filter(payload)?))
}

fn decode_topk_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::TopK(deserialize_topk(payload)?))
}

fn decode_tdigest_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::TDigest(deserialize_tdigest(payload)?))
}

fn decode_cms_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::CountMinSketch(deserialize_cms(payload)?))
}

fn decode_vectorset_value(payload: &[u8]) -> Result<Value, SerializationError> {
    Ok(Value::VectorSet(Box::new(deserialize_vectorset(payload)?)))
}

// --- the codecs ----------------------------------------------------------------

const STRING_RAW_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::StringRaw,
    encode: encode_string_raw,
    decode: decode_string_raw_value,
};
const STRING_INT_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::StringInt,
    encode: encode_string_int,
    decode: decode_string_int_value,
};
const SORTED_SET_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::SortedSet,
    encode: encode_sorted_set,
    decode: decode_sorted_set_value,
};
const HASH_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::Hash,
    encode: encode_hash,
    decode: decode_hash_value,
};
const HASH_FIELD_EXPIRY_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::HashWithFieldExpiry,
    encode: encode_hash_with_field_expiry,
    decode: decode_hash_with_field_expiry_value,
};
const LIST_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::List,
    encode: encode_list,
    decode: decode_list_value,
};
const SET_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::Set,
    encode: encode_set,
    decode: decode_set_value,
};
const STREAM_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::Stream,
    encode: encode_stream,
    decode: decode_stream_value,
};
const BLOOM_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::Bloom,
    encode: encode_bloom,
    decode: decode_bloom_value,
};
const HLL_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::HyperLogLog,
    encode: encode_hyperloglog,
    decode: decode_hyperloglog_value,
};
const TIMESERIES_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::TimeSeries,
    encode: encode_timeseries,
    decode: decode_timeseries_value,
};
const JSON_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::Json,
    encode: encode_json,
    decode: decode_json_value,
};
const CUCKOO_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::Cuckoo,
    encode: encode_cuckoo,
    decode: decode_cuckoo_value,
};
const TOPK_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::TopK,
    encode: encode_topk,
    decode: decode_topk_value,
};
const TDIGEST_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::TDigest,
    encode: encode_tdigest,
    decode: decode_tdigest_value,
};
const CMS_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::Cms,
    encode: encode_cms,
    decode: decode_cms_value,
};
const VECTORSET_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::VectorSet,
    encode: encode_vectorset,
    decode: decode_vectorset_value,
};

/// The whole persistence type system, in wire order. Encode dispatch scans this
/// (order-independent: each codec claims only its own case).
pub(super) const REGISTRY: &[TypeCodec] = &[
    STRING_RAW_CODEC,
    STRING_INT_CODEC,
    SORTED_SET_CODEC,
    HASH_CODEC,
    HASH_FIELD_EXPIRY_CODEC,
    LIST_CODEC,
    SET_CODEC,
    STREAM_CODEC,
    BLOOM_CODEC,
    HLL_CODEC,
    TIMESERIES_CODEC,
    JSON_CODEC,
    CUCKOO_CODEC,
    TOPK_CODEC,
    TDIGEST_CODEC,
    CMS_CODEC,
    VECTORSET_CODEC,
];

/// The decode half for a marker. This match is over the closed [`TypeMarker`] enum
/// with **no wildcard**, so adding a marker without a decode arm is a compile
/// error — the guarantee the old `u8` match (with its `_ => UnknownType`) could
/// not give. Each arm points at the registry entry, keeping it the single source
/// of truth.
pub(super) fn decode_for(marker: TypeMarker) -> fn(&[u8]) -> Result<Value, SerializationError> {
    match marker {
        TypeMarker::StringRaw => STRING_RAW_CODEC.decode,
        TypeMarker::StringInt => STRING_INT_CODEC.decode,
        TypeMarker::SortedSet => SORTED_SET_CODEC.decode,
        TypeMarker::Hash => HASH_CODEC.decode,
        TypeMarker::HashWithFieldExpiry => HASH_FIELD_EXPIRY_CODEC.decode,
        TypeMarker::List => LIST_CODEC.decode,
        TypeMarker::Set => SET_CODEC.decode,
        TypeMarker::Stream => STREAM_CODEC.decode,
        TypeMarker::Bloom => BLOOM_CODEC.decode,
        TypeMarker::HyperLogLog => HLL_CODEC.decode,
        TypeMarker::TimeSeries => TIMESERIES_CODEC.decode,
        TypeMarker::Json => JSON_CODEC.decode,
        TypeMarker::Cuckoo => CUCKOO_CODEC.decode,
        TypeMarker::TopK => TOPK_CODEC.decode,
        TypeMarker::TDigest => TDIGEST_CODEC.decode,
        TypeMarker::Cms => CMS_CODEC.decode,
        TypeMarker::VectorSet => VECTORSET_CODEC.decode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use frogdb_types::bloom::BloomFilterValue;
    use frogdb_types::cms::CountMinSketchValue;
    use frogdb_types::cuckoo::CuckooFilterValue;
    use frogdb_types::hyperloglog::HyperLogLogValue;
    use frogdb_types::json::JsonValue;
    use frogdb_types::tdigest::TDigestValue;
    use frogdb_types::timeseries::TimeSeriesValue;
    use frogdb_types::topk::TopKValue;
    use frogdb_types::types::{
        HashValue, KeyMetadata, ListValue, ListpackThresholds, SetValue, SortedSetValue, StreamId,
        StreamIdSpec, StreamValue, StringValue,
    };
    use frogdb_types::vectorset::{VectorDistanceMetric, VectorQuantization, VectorSetValue};

    /// At least one representative value per marker, guaranteed to encode to that
    /// marker. This match is exhaustive over [`TypeMarker`] (no wildcard), so a new
    /// marker cannot be added without supplying a round-trip sample.
    fn samples_for(marker: TypeMarker) -> Vec<Value> {
        match marker {
            TypeMarker::StringRaw => {
                vec![Value::String(StringValue::new(Bytes::from_static(
                    b"hello world",
                )))]
            }
            TypeMarker::StringInt => vec![Value::String(StringValue::from_integer(-42))],
            TypeMarker::SortedSet => {
                let mut z = SortedSetValue::new();
                z.add(Bytes::from_static(b"one"), 1.0);
                z.add(Bytes::from_static(b"two"), 2.0);
                vec![Value::SortedSet(z)]
            }
            TypeMarker::Hash => {
                let mut h = HashValue::new();
                h.set(
                    Bytes::from_static(b"f1"),
                    Bytes::from_static(b"v1"),
                    ListpackThresholds::DEFAULT_HASH,
                );
                assert!(!h.has_field_expiries());
                vec![Value::Hash(h)]
            }
            TypeMarker::HashWithFieldExpiry => {
                let mut h = HashValue::new();
                h.set(
                    Bytes::from_static(b"f1"),
                    Bytes::from_static(b"v1"),
                    ListpackThresholds::DEFAULT_HASH,
                );
                h.set(
                    Bytes::from_static(b"f2"),
                    Bytes::from_static(b"v2"),
                    ListpackThresholds::DEFAULT_HASH,
                );
                // Set an actual field expiry so this exercises marker 11.
                h.set_field_expiry(b"f1", Instant::now() + Duration::from_secs(3600));
                assert!(h.has_field_expiries());
                vec![Value::Hash(h)]
            }
            TypeMarker::List => {
                let mut l = ListValue::new();
                l.push_back(Bytes::from_static(b"a"));
                l.push_back(Bytes::from_static(b"b"));
                vec![Value::List(l)]
            }
            TypeMarker::Set => {
                let mut s = SetValue::new();
                s.add(Bytes::from_static(b"m1"), ListpackThresholds::DEFAULT_SET);
                s.add(Bytes::from_static(b"m2"), ListpackThresholds::DEFAULT_SET);
                vec![Value::Set(s)]
            }
            TypeMarker::Stream => {
                let mut st = StreamValue::new();
                let _ = st.add(
                    StreamIdSpec::Explicit(StreamId::new(1, 0)),
                    vec![(Bytes::from_static(b"field"), Bytes::from_static(b"value"))],
                );
                vec![Value::Stream(st)]
            }
            TypeMarker::Bloom => {
                let mut bf = BloomFilterValue::new(100, 0.01);
                bf.add(b"a");
                bf.add(b"b");
                vec![Value::BloomFilter(bf)]
            }
            TypeMarker::HyperLogLog => {
                let mut hll = HyperLogLogValue::new();
                hll.add(b"a");
                hll.add(b"b");
                vec![Value::HyperLogLog(hll)]
            }
            TypeMarker::TimeSeries => {
                let mut ts = TimeSeriesValue::new();
                let _ = ts.add(1000, 1.5);
                let _ = ts.add(2000, 2.5);
                vec![Value::TimeSeries(ts)]
            }
            TypeMarker::Json => {
                vec![Value::Json(
                    JsonValue::parse(br#"{"a":1,"b":[1,2,3]}"#).unwrap(),
                )]
            }
            TypeMarker::Cuckoo => {
                let mut cf = CuckooFilterValue::new(64);
                let _ = cf.add(b"a");
                let _ = cf.add(b"b");
                vec![Value::CuckooFilter(cf)]
            }
            TypeMarker::TopK => {
                let mut tk = TopKValue::new(3, 8, 7, 0.9);
                tk.add(b"a", 1);
                tk.add(b"b", 2);
                vec![Value::TopK(tk)]
            }
            TypeMarker::TDigest => {
                let mut td = TDigestValue::new(100.0);
                td.add(1.0);
                td.add(2.0);
                td.add(3.0);
                vec![Value::TDigest(td)]
            }
            TypeMarker::Cms => {
                let mut cms = CountMinSketchValue::new(16, 4);
                cms.increment(b"a", 1);
                cms.increment(b"b", 2);
                vec![Value::CountMinSketch(cms)]
            }
            TypeMarker::VectorSet => {
                let mut vs = VectorSetValue::new(
                    VectorDistanceMetric::Cosine,
                    VectorQuantization::NoQuant,
                    4,
                    16,
                    200,
                )
                .unwrap();
                vs.add(Bytes::from_static(b"e1"), vec![1.0, 0.0, 0.0, 0.0])
                    .unwrap();
                vec![Value::VectorSet(Box::new(vs))]
            }
        }
    }

    /// Every marker has a registry codec, exactly once.
    #[test]
    fn registry_covers_every_marker_once() {
        let mut seen: HashSet<u8> = HashSet::new();
        for codec in REGISTRY {
            assert!(
                seen.insert(codec.marker.as_byte()),
                "duplicate codec for {:?}",
                codec.marker
            );
        }
        for &m in TypeMarker::ALL {
            assert!(seen.contains(&m.as_byte()), "no codec for {m:?}");
        }
        assert_eq!(REGISTRY.len(), TypeMarker::ALL.len());
    }

    /// The headline guarantee: every marker's sample survives encode→decode, both
    /// at the value level and through the full header round-trip. A new type with a
    /// missing or broken decode is a failure here, not a production data-loss
    /// incident on the next snapshot load or replica full-sync.
    #[test]
    fn every_marker_round_trips() {
        for &marker in TypeMarker::ALL {
            for value in samples_for(marker) {
                // 1. encode selects this codec's marker.
                let (got_marker, payload) = super::super::serialize_value(&value);
                assert_eq!(
                    got_marker, marker,
                    "{marker:?} sample encoded to {got_marker:?}"
                );

                // 2. decode inverts encode.
                let back = (decode_for(marker))(&payload)
                    .unwrap_or_else(|e| panic!("{marker:?} decode failed: {e}"));
                assert_eq!(
                    value.key_type(),
                    back.key_type(),
                    "{marker:?} changed key_type through value round-trip"
                );

                // 3. full serialize/deserialize round-trip including the header.
                let bytes = serialize(&value, &KeyMetadata::new(value.memory_size()));
                assert_eq!(
                    bytes[0],
                    marker.as_byte(),
                    "{marker:?} wrote wrong header byte"
                );
                let (back2, _) = deserialize(&bytes)
                    .unwrap_or_else(|e| panic!("{marker:?} deserialize failed: {e}"));
                assert_eq!(
                    value.key_type(),
                    back2.key_type(),
                    "{marker:?} changed key_type through header round-trip"
                );
            }
        }
    }

    /// Pin the markers that share a `Value` variant — the encode side must pick the
    /// content-dependent marker, not just the variant.
    #[test]
    fn shared_variant_markers_are_selected_by_content() {
        // Integer-encoded vs raw string.
        let (m, _) = super::super::serialize_value(&Value::String(StringValue::from_integer(7)));
        assert_eq!(m, TypeMarker::StringInt);
        let (m, _) = super::super::serialize_value(&Value::String(StringValue::new(
            Bytes::from_static(b"not-an-int"),
        )));
        assert_eq!(m, TypeMarker::StringRaw);

        // Hash with vs without a field expiry.
        let mut plain = HashValue::new();
        plain.set(
            Bytes::from_static(b"f"),
            Bytes::from_static(b"v"),
            ListpackThresholds::DEFAULT_HASH,
        );
        let (m, _) = super::super::serialize_value(&Value::Hash(plain));
        assert_eq!(m, TypeMarker::Hash);

        let mut with_ttl = HashValue::new();
        with_ttl.set(
            Bytes::from_static(b"f"),
            Bytes::from_static(b"v"),
            ListpackThresholds::DEFAULT_HASH,
        );
        with_ttl.set_field_expiry(b"f", Instant::now() + Duration::from_secs(60));
        let (m, _) = super::super::serialize_value(&Value::Hash(with_ttl));
        assert_eq!(m, TypeMarker::HashWithFieldExpiry);
    }

    /// The per-field expiry hash (marker 11) must preserve its field TTL through a
    /// full round-trip — the branch that had no test before this registry.
    #[test]
    fn hash_with_field_expiry_preserves_ttl() {
        let mut h = HashValue::new();
        h.set(
            Bytes::from_static(b"keep"),
            Bytes::from_static(b"v"),
            ListpackThresholds::DEFAULT_HASH,
        );
        h.set_field_expiry(b"keep", Instant::now() + Duration::from_secs(3600));
        let value = Value::Hash(h);

        let bytes = serialize(&value, &KeyMetadata::new(value.memory_size()));
        let (back, _) = deserialize(&bytes).unwrap();
        let hash = back.as_hash().unwrap();
        let expiry = hash
            .get_field_expiry(b"keep")
            .expect("field expiry lost through round-trip");
        let secs = expiry.saturating_duration_since(Instant::now()).as_secs();
        assert!((3500..=3700).contains(&secs), "ttl drifted: {secs}s");
    }
}
