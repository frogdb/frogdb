use bitvec::prelude::*;
use frogdb_types::bloom::{BloomFilterValue, BloomLayer};
use frogdb_types::cms::CountMinSketchValue;
use frogdb_types::cuckoo::{CuckooFilterValue, CuckooLayer};
use frogdb_types::hyperloglog::{HLL_DENSE_SIZE, HyperLogLogValue};
use frogdb_types::tdigest::{Centroid, TDigestValue};
use frogdb_types::topk::TopKValue;
use frogdb_types::types::{KeyMetadata, Value};

use super::*;

/// Serialize a bloom filter.
///
/// Format:
/// - error_rate (8 bytes f64)
/// - expansion (4 bytes u32)
/// - non_scaling (1 byte bool)
/// - num_layers (4 bytes u32)
/// - for each layer:
///   - k (4 bytes u32) - number of hash functions
///   - count (8 bytes u64) - items in this layer
///   - capacity (8 bytes u64) - layer capacity
///   - bits_len (8 bytes u64) - number of bits
///   - bits_bytes (bits_len/8 rounded up)
pub(super) fn serialize_bloom_filter(bf: &BloomFilterValue) -> (TypeMarker, Vec<u8>) {
    // Calculate size
    let mut payload_size = 8 + 4 + 1 + 4; // error_rate + expansion + non_scaling + num_layers
    for layer in bf.layers() {
        payload_size += 4 + 8 + 8 + 8; // k + count + capacity + bits_len
        payload_size += layer.bits_as_bytes().len();
    }

    let mut payload = Vec::with_capacity(payload_size);

    // Error rate
    payload.extend_from_slice(&bf.error_rate().to_le_bytes());

    // Expansion
    payload.extend_from_slice(&bf.expansion().to_le_bytes());

    // Non-scaling flag
    payload.push(if bf.is_non_scaling() { 1 } else { 0 });

    // Number of layers
    payload.extend_from_slice(&(bf.num_layers() as u32).to_le_bytes());

    // Each layer
    for layer in bf.layers() {
        payload.extend_from_slice(&layer.k().to_le_bytes());
        payload.extend_from_slice(&layer.count().to_le_bytes());
        payload.extend_from_slice(&layer.capacity().to_le_bytes());
        let bits_bytes = layer.bits_as_bytes();
        payload.extend_from_slice(&(layer.size_bits() as u64).to_le_bytes());
        payload.extend_from_slice(bits_bytes);
    }

    (TypeMarker::Bloom, payload)
}

/// Serialize a cuckoo filter.
///
/// Format:
/// - bucket_size (1 byte u8)
/// - max_iterations (2 bytes u16)
/// - expansion (4 bytes u32)
/// - delete_count (8 bytes u64)
/// - num_layers (4 bytes u32)
/// - for each layer:
///   - num_buckets (8 bytes u64)
///   - bucket_size (1 byte u8)
///   - count (8 bytes u64)
///   - capacity (8 bytes u64)
///   - fingerprint data (num_buckets * bucket_size * 2 bytes)
pub(super) fn serialize_cuckoo_filter(cf: &CuckooFilterValue) -> (TypeMarker, Vec<u8>) {
    // Calculate size
    let mut payload_size = 1 + 2 + 4 + 8 + 4; // header
    for layer in cf.layers() {
        payload_size += 8 + 1 + 8 + 8; // layer header
        payload_size += layer.num_buckets() * layer.bucket_size() as usize * 2; // fingerprints
    }

    let mut payload = Vec::with_capacity(payload_size);

    payload.push(cf.bucket_size());
    payload.extend_from_slice(&cf.max_iterations().to_le_bytes());
    payload.extend_from_slice(&cf.expansion().to_le_bytes());
    payload.extend_from_slice(&cf.delete_count().to_le_bytes());
    payload.extend_from_slice(&(cf.num_layers() as u32).to_le_bytes());

    for layer in cf.layers() {
        payload.extend_from_slice(&(layer.num_buckets() as u64).to_le_bytes());
        payload.push(layer.bucket_size());
        payload.extend_from_slice(&layer.total_count().to_le_bytes());
        payload.extend_from_slice(&layer.capacity().to_le_bytes());
        for bucket in layer.buckets() {
            for &fp in bucket {
                payload.extend_from_slice(&fp.to_le_bytes());
            }
        }
    }

    (TypeMarker::Cuckoo, payload)
}

/// Serialize a t-digest.
///
/// Format:
/// - compression (8 bytes f64)
/// - min (8 bytes f64)
/// - max (8 bytes f64)
/// - merged_weight (8 bytes f64)
/// - unmerged_weight (8 bytes f64)
/// - num_centroids (4 bytes u32)
/// - num_unmerged (4 bytes u32)
/// - centroids: num_centroids * (mean: f64, weight: f64) = 16 bytes each
/// - unmerged: num_unmerged * (mean: f64, weight: f64) = 16 bytes each
pub(super) fn serialize_tdigest(td: &TDigestValue) -> (TypeMarker, Vec<u8>) {
    let payload_size = 8 * 5 + 4 + 4 + td.centroids().len() * 16 + td.unmerged().len() * 16;

    let mut payload = Vec::with_capacity(payload_size);

    payload.extend_from_slice(&td.compression().to_le_bytes());
    payload.extend_from_slice(&td.raw_min().to_le_bytes());
    payload.extend_from_slice(&td.raw_max().to_le_bytes());
    payload.extend_from_slice(&td.merged_weight().to_le_bytes());
    payload.extend_from_slice(&td.unmerged_weight().to_le_bytes());
    payload.extend_from_slice(&(td.centroids().len() as u32).to_le_bytes());
    payload.extend_from_slice(&(td.unmerged().len() as u32).to_le_bytes());

    for c in td.centroids() {
        payload.extend_from_slice(&c.mean.to_le_bytes());
        payload.extend_from_slice(&c.weight.to_le_bytes());
    }
    for c in td.unmerged() {
        payload.extend_from_slice(&c.mean.to_le_bytes());
        payload.extend_from_slice(&c.weight.to_le_bytes());
    }

    (TypeMarker::TDigest, payload)
}

/// Serialize a HyperLogLog.
///
/// Format:
/// - encoding (1 byte): 0 = sparse, 1 = dense
/// - if sparse:
///   - num_entries (4 bytes u32)
///   - for each entry: (index: u16, value: u8) = 3 bytes
/// - if dense:
///   - 12288 bytes raw packed registers
pub(super) fn serialize_hyperloglog(hll: &HyperLogLogValue) -> (TypeMarker, Vec<u8>) {
    if let Some(pairs) = hll.as_sparse() {
        // Sparse encoding
        let payload_size = 1 + 4 + pairs.len() * 3;
        let mut payload = Vec::with_capacity(payload_size);

        // Encoding byte (0 = sparse)
        payload.push(0);

        // Number of entries
        payload.extend_from_slice(&(pairs.len() as u32).to_le_bytes());

        // Each entry: index (u16) + value (u8)
        for (index, value) in pairs {
            payload.extend_from_slice(&index.to_le_bytes());
            payload.push(*value);
        }

        (TypeMarker::HyperLogLog, payload)
    } else if let Some(registers) = hll.as_dense() {
        // Dense encoding
        let mut payload = Vec::with_capacity(1 + HLL_DENSE_SIZE);

        // Encoding byte (1 = dense)
        payload.push(1);

        // Raw registers
        payload.extend_from_slice(registers.as_slice());

        (TypeMarker::HyperLogLog, payload)
    } else {
        // Shouldn't happen, but fallback to empty sparse
        (TypeMarker::HyperLogLog, vec![0, 0, 0, 0, 0])
    }
}

/// Serialize a Top-K value.
///
/// Format: [k:u32][width:u32][depth:u32][decay:f64][buckets: depth*width*(fp:u32+ctr:u32)][heap_len:u32][for each: item_len:u32, item_bytes, count:u64]
pub(super) fn serialize_topk(tk: &TopKValue) -> (TypeMarker, Vec<u8>) {
    let mut payload = Vec::new();
    payload.extend_from_slice(&tk.k().to_le_bytes());
    payload.extend_from_slice(&tk.width().to_le_bytes());
    payload.extend_from_slice(&tk.depth().to_le_bytes());
    payload.extend_from_slice(&tk.decay().to_le_bytes());

    for row in &tk.buckets_raw() {
        for &(fp, ctr) in row {
            payload.extend_from_slice(&fp.to_le_bytes());
            payload.extend_from_slice(&ctr.to_le_bytes());
        }
    }

    let heap = tk.heap_items();
    payload.extend_from_slice(&(heap.len() as u32).to_le_bytes());
    for (item, count) in heap {
        payload.extend_from_slice(&(item.len() as u32).to_le_bytes());
        payload.extend_from_slice(item);
        payload.extend_from_slice(&count.to_le_bytes());
    }

    (TypeMarker::TopK, payload)
}

/// Serialize a Count-Min Sketch value.
///
/// Format: [width:u32][depth:u32][count:u64][counters: depth*width u64 LE values]
pub(super) fn serialize_cms(cms: &CountMinSketchValue) -> (TypeMarker, Vec<u8>) {
    let mut payload = Vec::new();
    payload.extend_from_slice(&cms.width().to_le_bytes());
    payload.extend_from_slice(&cms.depth().to_le_bytes());
    payload.extend_from_slice(&cms.count().to_le_bytes());

    for row in cms.counters_raw() {
        for &val in row {
            payload.extend_from_slice(&val.to_le_bytes());
        }
    }

    (TypeMarker::Cms, payload)
}

/// Deserialize a bloom filter from payload.
pub(super) fn deserialize_bloom_filter(
    payload: &[u8],
) -> Result<BloomFilterValue, SerializationError> {
    let mut reader = FrameReader::new(payload);

    let error_rate = reader.read_le_f64()?;
    let expansion = reader.read_le_u32()?;
    let non_scaling = reader.read_u8()? != 0;
    let num_layers = reader.read_le_u32()? as usize;

    // Each layer needs at least 28 bytes for its header fields; reject a count that
    // cannot possibly fit before allocating.
    if num_layers > reader.remaining() / 28 {
        return Err(SerializationError::Truncated {
            expected: payload.len() - reader.remaining() + num_layers * 28,
            actual: payload.len(),
        });
    }

    let mut layers = Vec::with_capacity(safe_capacity(num_layers, 28, reader.remaining()));

    for _ in 0..num_layers {
        let k = reader.read_le_u32()?;
        let count = reader.read_le_u64()?;
        let capacity = reader.read_le_u64()?;
        let bits_len = reader.read_le_u64()? as usize;

        // Bits are packed one bit per position, rounded up to whole bytes.
        let bits_bytes = reader.take(bits_len.div_ceil(8))?;
        let mut bits: BitVec<u8, Lsb0> = BitVec::from_slice(bits_bytes);
        bits.truncate(bits_len);

        layers.push(BloomLayer::from_raw(bits, k, count, capacity));
    }

    Ok(BloomFilterValue::from_raw(
        layers,
        error_rate,
        expansion,
        non_scaling,
    ))
}

/// Deserialize a cuckoo filter from payload.
pub(super) fn deserialize_cuckoo_filter(
    payload: &[u8],
) -> Result<CuckooFilterValue, SerializationError> {
    let mut reader = FrameReader::new(payload);

    let bucket_size = reader.read_u8()?;
    let max_iterations = reader.read_le_u16()?;
    let expansion = reader.read_le_u32()?;
    let delete_count = reader.read_le_u64()?;
    let num_layers = reader.read_le_u32()? as usize;

    // Each layer needs at least 25 bytes for its header; reject a count that cannot
    // possibly fit before allocating.
    if num_layers > reader.remaining() / 25 {
        return Err(SerializationError::Truncated {
            expected: payload.len() - reader.remaining() + num_layers * 25,
            actual: payload.len(),
        });
    }

    let mut layers = Vec::with_capacity(safe_capacity(num_layers, 25, reader.remaining()));

    for _ in 0..num_layers {
        // Layer header: num_buckets(8) + bucket_size(1) + count(8) + capacity(8) = 25
        let num_buckets = reader.read_le_u64()? as usize;
        let layer_bucket_size = reader.read_u8()?;
        let count = reader.read_le_u64()?;
        let capacity = reader.read_le_u64()?;

        if layer_bucket_size == 0 && num_buckets > 0 {
            return Err(SerializationError::InvalidPayload(
                "Cuckoo filter layer has buckets but zero bucket size".to_string(),
            ));
        }

        // Guard the fingerprint-region size before it feeds Vec capacities.
        let fp_bytes = num_buckets
            .checked_mul(layer_bucket_size as usize)
            .and_then(|v| v.checked_mul(2))
            .ok_or_else(|| {
                SerializationError::InvalidPayload(
                    "Cuckoo filter fingerprint data size overflow".to_string(),
                )
            })?;
        if fp_bytes > reader.remaining() {
            return Err(SerializationError::InvalidPayload(
                "Cuckoo filter payload truncated at fingerprint data".to_string(),
            ));
        }

        let mut buckets = Vec::with_capacity(safe_capacity(num_buckets, 2, reader.remaining()));
        for _ in 0..num_buckets {
            let mut bucket = Vec::with_capacity(safe_capacity(
                layer_bucket_size as usize,
                2,
                reader.remaining(),
            ));
            for _ in 0..layer_bucket_size {
                bucket.push(reader.read_le_u16()?);
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

    Ok(CuckooFilterValue::from_raw(
        layers,
        bucket_size,
        max_iterations,
        expansion,
        delete_count,
    ))
}

/// Deserialize a t-digest from payload.
pub(super) fn deserialize_tdigest(payload: &[u8]) -> Result<TDigestValue, SerializationError> {
    let mut reader = FrameReader::new(payload);

    let compression = reader.read_le_f64()?;
    let min = reader.read_le_f64()?;
    let max = reader.read_le_f64()?;
    let merged_weight = reader.read_le_f64()?;
    let unmerged_weight = reader.read_le_f64()?;
    let num_centroids = reader.read_le_u32()? as usize;
    let num_unmerged = reader.read_le_u32()? as usize;

    let needed = (num_centroids + num_unmerged) * 16;
    if needed > reader.remaining() {
        return Err(SerializationError::InvalidPayload(
            "T-Digest payload truncated at centroid data".to_string(),
        ));
    }

    let mut centroids = Vec::with_capacity(safe_capacity(num_centroids, 16, reader.remaining()));
    for _ in 0..num_centroids {
        let mean = reader.read_le_f64()?;
        let weight = reader.read_le_f64()?;
        centroids.push(Centroid { mean, weight });
    }

    let mut unmerged = Vec::with_capacity(safe_capacity(num_unmerged, 16, reader.remaining()));
    for _ in 0..num_unmerged {
        let mean = reader.read_le_f64()?;
        let weight = reader.read_le_f64()?;
        unmerged.push(Centroid { mean, weight });
    }

    Ok(TDigestValue::from_raw(
        compression,
        centroids,
        unmerged,
        min,
        max,
        merged_weight,
        unmerged_weight,
    ))
}

/// Deserialize a HyperLogLog from payload.
pub(super) fn deserialize_hyperloglog(
    payload: &[u8],
) -> Result<HyperLogLogValue, SerializationError> {
    let mut reader = FrameReader::new(payload);
    let encoding = reader.read_u8()?;

    match encoding {
        0 => {
            // Sparse encoding: count followed by (u16 index + u8 value) triples.
            let num_entries = reader.read_le_u32()? as usize;
            let mut pairs = Vec::with_capacity(safe_capacity(num_entries, 3, reader.remaining()));
            for _ in 0..num_entries {
                let index = reader.read_le_u16()?;
                let value = reader.read_u8()?;
                pairs.push((index, value));
            }
            Ok(HyperLogLogValue::from_sparse(pairs))
        }
        1 => {
            // Dense encoding: HLL_DENSE_SIZE raw packed registers.
            let mut registers = Box::new([0u8; HLL_DENSE_SIZE]);
            registers.copy_from_slice(reader.take(HLL_DENSE_SIZE)?);
            Ok(HyperLogLogValue::from_dense(registers))
        }
        other => Err(SerializationError::InvalidPayload(format!(
            "Unknown HyperLogLog encoding: {other}"
        ))),
    }
}

/// Payload encoding byte identifying a HyperLogLog delta operand (Tier 2).
///
/// Shares the encoding-byte space with the full-value payload (0 = sparse,
/// 1 = dense); 2 = a register-max delta carrying only the pairs a write raised.
const HLL_DELTA_ENCODING: u8 = 2;

/// Build a delta payload: `[2][num_pairs u32 LE][(index u16 LE)(value u8)]*`.
fn build_hll_delta_payload(pairs: &[(u16, u8)]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 4 + pairs.len() * 3);
    payload.push(HLL_DELTA_ENCODING);
    payload.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
    for (index, value) in pairs {
        payload.extend_from_slice(&index.to_le_bytes());
        payload.push(*value);
    }
    payload
}

/// Parse a delta payload's pair list, following the crate's truncation
/// conventions (`safe_capacity` bounds the pre-allocation against remaining bytes).
///
/// Expects the leading encoding byte to already be [`HLL_DELTA_ENCODING`].
fn parse_hll_delta_payload(payload: &[u8]) -> Result<Vec<(u16, u8)>, SerializationError> {
    let mut reader = FrameReader::new(payload);
    let encoding = reader.read_u8()?;
    if encoding != HLL_DELTA_ENCODING {
        return Err(SerializationError::InvalidPayload(format!(
            "Expected HyperLogLog delta encoding {HLL_DELTA_ENCODING}, got {encoding}"
        )));
    }
    let num_pairs = reader.read_le_u32()? as usize;
    let mut pairs = Vec::with_capacity(safe_capacity(num_pairs, 3, reader.remaining()));
    for _ in 0..num_pairs {
        let index = reader.read_le_u16()?;
        let value = reader.read_u8()?;
        pairs.push((index, value));
    }
    Ok(pairs)
}

/// Borrow the payload region of a framed value, validating the 24-byte header and
/// declared payload length. Returns `None` on any truncation — merge callers map
/// that to a merge failure rather than panicking.
fn framed_payload(frame: &[u8]) -> Option<&[u8]> {
    if frame.len() < HEADER_SIZE {
        return None;
    }
    let payload_len = u64::from_le_bytes(frame[16..24].try_into().ok()?) as usize;
    let end = HEADER_SIZE.checked_add(payload_len)?;
    if end > frame.len() {
        return None;
    }
    Some(&frame[HEADER_SIZE..end])
}

/// Re-frame `payload` behind the header of an existing framed value, copying its
/// marker/flags/expiry/LFU verbatim (the newest-operand-wins metadata) and writing
/// the fresh payload length. Returns `None` if `header_src` is too short to hold a
/// header prefix.
fn reframe_with_header(header_src: &[u8], payload: &[u8]) -> Option<Vec<u8>> {
    // Bytes 0..16 carry marker(1) + flags(1) + expires(8) + lfu(1) + pad(5); only
    // the trailing payload length (16..24) changes.
    if header_src.len() < 16 {
        return None;
    }
    let mut out = Vec::with_capacity(HEADER_SIZE + payload.len());
    out.extend_from_slice(&header_src[..16]);
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(payload);
    Some(out)
}

/// Serialize a HyperLogLog register-max delta as a full persisted frame.
///
/// The header carries [`TypeMarker::HyperLogLog`] with `metadata`-derived expiry
/// and LFU exactly as [`serialize`](super::serialize) does; the payload is
/// `[2][num_pairs u32 LE][(index u16 LE)(value u8)]*`. Consumed as a RocksDB merge
/// operand (Tier 2) by [`merge_hll_serialized`] / [`partial_merge_hll_deltas`].
pub fn serialize_hll_delta(pairs: &[(u16, u8)], metadata: &KeyMetadata) -> Vec<u8> {
    build_frame(
        TypeMarker::HyperLogLog,
        metadata,
        &build_hll_delta_payload(pairs),
    )
}

/// Full RocksDB merge operator for HyperLogLog: fold `operands` (in order) onto
/// `base`, re-serializing the result as a full value.
///
/// `base` is the existing full value (`None` → a fresh empty HLL). Each operand is
/// either a delta (encoding 2 — its pairs are applied via
/// [`HyperLogLogValue::apply_register_max`], which owns sparse→dense promotion so
/// on-disk promotion matches in-memory) or a full value (encoding 0/1 — it replaces
/// the accumulated state defensively, matching `Put` semantics). The result is
/// framed behind the newest operand's header (last-write-wins). Returns `None` only
/// on undecodable input, which RocksDB surfaces as a merge failure.
pub fn merge_hll_serialized(base: Option<&[u8]>, operands: &[&[u8]]) -> Option<Vec<u8>> {
    let mut acc = match base {
        Some(frame) => match deserialize(frame).ok()?.0 {
            Value::HyperLogLog(hll) => hll,
            _ => return None,
        },
        None => HyperLogLogValue::new(),
    };

    for operand in operands {
        let payload = framed_payload(operand)?;
        match payload.first().copied() {
            Some(HLL_DELTA_ENCODING) => {
                for (index, value) in parse_hll_delta_payload(payload).ok()? {
                    acc.apply_register_max(index, value);
                }
            }
            // A full value (sparse/dense) replaces the accumulated base defensively.
            Some(0) | Some(1) => acc = deserialize_hyperloglog(payload).ok()?,
            _ => return None,
        }
    }

    // Newest operand's header wins; fall back to the base header if there are no
    // operands at all. This is correct only because TTL changes (EXPIRE family,
    // `WalStrategy::PersistFirstKey`) always rewrite the full base and clear the
    // operand chain -- so a delta operand's header never carries a stale TTL. A
    // future lighter-weight EXPIRE persistence path that appended a TTL-only
    // operand instead of rewriting the base would break this invariant.
    let header_src = operands.last().copied().or(base)?;
    let (_marker, payload) = serialize_hyperloglog(&acc);
    reframe_with_header(header_src, &payload)
}

/// Partial RocksDB merge for HyperLogLog: concatenate the pair lists of several
/// delta operands into a single delta operand.
///
/// Register-max is commutative and associative, so concatenation is a valid partial
/// merge. The combined operand keeps the newest operand's header. Returns `None` if
/// any operand is a full value (encoding 0/1) — RocksDB then falls back to the full
/// [`merge_hll_serialized`] — or if any operand is undecodable.
pub fn partial_merge_hll_deltas(operands: &[&[u8]]) -> Option<Vec<u8>> {
    let mut combined: Vec<(u16, u8)> = Vec::new();
    for operand in operands {
        let payload = framed_payload(operand)?;
        match payload.first().copied() {
            Some(HLL_DELTA_ENCODING) => combined.extend(parse_hll_delta_payload(payload).ok()?),
            // Any full value forces RocksDB to fall back to the full merge.
            _ => return None,
        }
    }

    let header_src = operands.last().copied()?;
    reframe_with_header(header_src, &build_hll_delta_payload(&combined))
}

/// Deserialize a Top-K value.
pub(super) fn deserialize_topk(payload: &[u8]) -> Result<TopKValue, SerializationError> {
    let mut reader = FrameReader::new(payload);
    let k = reader.read_le_u32()?;
    let width = reader.read_le_u32()?;
    let depth = reader.read_le_u32()?;
    let decay = reader.read_le_f64()?;

    if (width == 0) != (depth == 0) {
        return Err(SerializationError::InvalidPayload(
            "TopK width and depth must both be zero or both non-zero".to_string(),
        ));
    }

    // Guard the bucket-region size before it feeds Vec capacities.
    let bucket_bytes_needed = (depth as usize)
        .checked_mul(width as usize)
        .and_then(|v| v.checked_mul(8))
        .ok_or_else(|| {
            SerializationError::InvalidPayload("TopK bucket data size overflow".to_string())
        })?;
    if bucket_bytes_needed > reader.remaining() {
        return Err(SerializationError::Truncated {
            expected: payload.len() - reader.remaining() + bucket_bytes_needed,
            actual: payload.len(),
        });
    }

    let mut buckets = Vec::with_capacity(safe_capacity(depth as usize, 8, reader.remaining()));
    for _ in 0..depth {
        let mut row = Vec::with_capacity(safe_capacity(width as usize, 8, reader.remaining()));
        for _ in 0..width {
            let fp = reader.read_le_u32()?;
            let ctr = reader.read_le_u32()?;
            row.push((fp, ctr));
        }
        buckets.push(row);
    }

    let heap_len = reader.read_le_u32()? as usize;
    let mut heap_items = Vec::with_capacity(safe_capacity(heap_len, 12, reader.remaining()));
    for _ in 0..heap_len {
        let item = reader.read_bytes_u32()?;
        let count = reader.read_le_u64()?;
        heap_items.push((item, count));
    }

    Ok(TopKValue::from_raw(
        k, width, depth, decay, buckets, heap_items,
    ))
}

/// Deserialize a Count-Min Sketch value.
pub(super) fn deserialize_cms(payload: &[u8]) -> Result<CountMinSketchValue, SerializationError> {
    let mut reader = FrameReader::new(payload);
    let width = reader.read_le_u32()?;
    let depth = reader.read_le_u32()?;
    let count = reader.read_le_u64()?;

    if (width == 0) != (depth == 0) {
        return Err(SerializationError::InvalidPayload(
            "CMS width and depth must both be zero or both non-zero".to_string(),
        ));
    }

    // Guard the counter-region size before it feeds Vec capacities.
    let counter_bytes_needed = (depth as usize)
        .checked_mul(width as usize)
        .and_then(|v| v.checked_mul(8))
        .ok_or_else(|| {
            SerializationError::InvalidPayload("CMS counter data size overflow".to_string())
        })?;
    if counter_bytes_needed > reader.remaining() {
        return Err(SerializationError::Truncated {
            expected: payload.len() - reader.remaining() + counter_bytes_needed,
            actual: payload.len(),
        });
    }

    let mut counters = Vec::with_capacity(safe_capacity(depth as usize, 8, reader.remaining()));
    for _ in 0..depth {
        let mut row = Vec::with_capacity(safe_capacity(width as usize, 8, reader.remaining()));
        for _ in 0..width {
            row.push(reader.read_le_u64()?);
        }
        counters.push(row);
    }

    Ok(CountMinSketchValue::from_raw(width, depth, count, counters))
}

#[cfg(test)]
mod hll_delta_tests {
    use super::*;

    #[test]
    fn hll_delta_round_trip_equals_in_memory() {
        let meta = KeyMetadata::new(1);
        let mut reference = HyperLogLogValue::new();
        // Base: 10 elements, persisted full.
        for i in 0..10u32 {
            reference.add(&i.to_le_bytes());
        }
        let base = serialize(&Value::HyperLogLog(reference.clone()), &meta);
        // Two delta batches on top.
        let mut pairs1 = Vec::new();
        for i in 10..40u32 {
            if let Some(p) = reference.add_tracked(&i.to_le_bytes()) {
                pairs1.push(p);
            }
        }
        let mut pairs2 = Vec::new();
        for i in 40..80u32 {
            if let Some(p) = reference.add_tracked(&i.to_le_bytes()) {
                pairs2.push(p);
            }
        }
        let op1 = serialize_hll_delta(&pairs1, &meta);
        let op2 = serialize_hll_delta(&pairs2, &meta);

        let merged = merge_hll_serialized(Some(&base), &[&op1, &op2]).unwrap();
        let (value, _) = deserialize(&merged).unwrap();
        let Value::HyperLogLog(merged_hll) = value else {
            panic!("wrong type")
        };
        assert_eq!(merged_hll.count_no_cache(), reference.count_no_cache());
    }

    #[test]
    fn hll_merge_none_base_and_partial_merge() {
        let meta = KeyMetadata::new(1);
        let mut reference = HyperLogLogValue::new();
        let mut pairs = Vec::new();
        for i in 0..30u32 {
            if let Some(p) = reference.add_tracked(&i.to_le_bytes()) {
                pairs.push(p);
            }
        }
        let (a, b) = pairs.split_at(pairs.len() / 2);
        let op_a = serialize_hll_delta(a, &meta);
        let op_b = serialize_hll_delta(b, &meta);
        // Partial merge combines deltas into one delta.
        let combined = partial_merge_hll_deltas(&[&op_a, &op_b]).unwrap();
        // Full merge over a missing base materializes from empty.
        let merged = merge_hll_serialized(None, &[&combined]).unwrap();
        let (value, _) = deserialize(&merged).unwrap();
        let Value::HyperLogLog(hll) = value else {
            panic!("wrong type")
        };
        assert_eq!(hll.count_no_cache(), reference.count_no_cache());
    }

    #[test]
    fn hll_merge_promotes_to_dense_like_in_memory() {
        // Push a sparse base past the promotion threshold purely via deltas;
        // the merged on-disk encoding must be dense (encoding byte 1).
        let meta = KeyMetadata::new(1);
        let mut reference = HyperLogLogValue::new();
        let base = serialize(&Value::HyperLogLog(reference.clone()), &meta);
        let mut pairs = Vec::new();
        for i in 0..5000u32 {
            if let Some(p) = reference.add_tracked(&i.to_le_bytes()) {
                pairs.push(p);
            }
        }
        let op = serialize_hll_delta(&pairs, &meta);
        let merged = merge_hll_serialized(Some(&base), &[&op]).unwrap();
        // Payload starts after the 24-byte header; encoding byte must be dense.
        assert_eq!(
            merged[HEADER_SIZE], 1,
            "merge must promote sparse->dense at the same threshold"
        );
    }

    #[test]
    fn partial_merge_rejects_full_value_operand() {
        // A full value among the operands forces RocksDB to fall back to the
        // full merge, signalled by returning None from the partial merge.
        let meta = KeyMetadata::new(1);
        let mut hll = HyperLogLogValue::new();
        hll.add(b"x");
        let full = serialize(&Value::HyperLogLog(hll), &meta);
        let delta = serialize_hll_delta(&[(1, 2)], &meta);
        assert!(partial_merge_hll_deltas(&[&delta, &full]).is_none());
    }

    #[test]
    fn merge_full_value_operand_replaces_base() {
        // An operand carrying a full value (encoding 0/1) replaces the
        // accumulated base defensively, matching Put semantics.
        let meta = KeyMetadata::new(1);
        let mut base_hll = HyperLogLogValue::new();
        for i in 0..50u32 {
            base_hll.add(&i.to_le_bytes());
        }
        let base = serialize(&Value::HyperLogLog(base_hll), &meta);

        let mut replacement = HyperLogLogValue::new();
        replacement.add(b"only");
        let full_operand = serialize(&Value::HyperLogLog(replacement.clone()), &meta);

        let merged = merge_hll_serialized(Some(&base), &[&full_operand]).unwrap();
        let (value, _) = deserialize(&merged).unwrap();
        let Value::HyperLogLog(hll) = value else {
            panic!("wrong type")
        };
        assert_eq!(hll.count_no_cache(), replacement.count_no_cache());
    }

    #[test]
    fn merge_truncated_operand_returns_none() {
        let meta = KeyMetadata::new(1);
        let op = serialize_hll_delta(&[(1, 2), (3, 4)], &meta);
        // Chop the payload mid pair-list; must map to None, never panic.
        let truncated = &op[..op.len() - 1];
        assert!(merge_hll_serialized(None, &[truncated]).is_none());
    }

    #[test]
    fn serialize_hll_delta_uses_hyperloglog_marker_and_encoding() {
        let meta = KeyMetadata::new(1);
        let op = serialize_hll_delta(&[(7, 3)], &meta);
        assert_eq!(op[0], TypeMarker::HyperLogLog.as_byte());
        assert_eq!(op[HEADER_SIZE], HLL_DELTA_ENCODING);
    }
}
