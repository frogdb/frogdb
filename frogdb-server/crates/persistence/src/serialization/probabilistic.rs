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

    // The pre-size is only a `Vec::with_capacity` hint, so a wrong formula is
    // invisible in release — the vector reallocs and the bytes come out the same.
    // Asserting it makes the formula part of the contract the round-trip tests
    // already exercise, instead of a comment that silently rots when the wire
    // format gains a field.
    debug_assert_eq!(
        payload.len(),
        payload_size,
        "pre-sized payload does not match the bytes written"
    );
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

    debug_assert_eq!(
        payload.len(),
        payload_size,
        "pre-sized payload does not match the bytes written"
    );
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

    debug_assert_eq!(
        payload.len(),
        payload_size,
        "pre-sized payload does not match the bytes written"
    );
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

        debug_assert_eq!(
            payload.len(),
            payload_size,
            "pre-sized payload does not match the bytes written"
        );
        (TypeMarker::HyperLogLog, payload)
    } else if let Some(registers) = hll.as_dense() {
        // Dense encoding
        let mut payload = Vec::with_capacity(1 + HLL_DENSE_SIZE);

        // Encoding byte (1 = dense)
        payload.push(1);

        // Raw registers
        payload.extend_from_slice(registers.as_slice());

        debug_assert_eq!(
            payload.len(),
            1 + HLL_DENSE_SIZE,
            "a dense HLL payload is the encoding byte plus a fixed register array"
        );
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
    debug_assert_eq!(
        payload.len(),
        1 + 4 + pairs.len() * 3,
        "pre-sized delta payload does not match the bytes written"
    );
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
    debug_assert_eq!(
        out.len(),
        HEADER_SIZE + payload.len(),
        "a re-framed value is a full header plus the new payload"
    );
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

/// Every probabilistic decoder reads an element *count* off the wire before it
/// allocates for it, so each one carries a guard that refuses a count the
/// remaining bytes could not possibly hold. These tests drive the guards from
/// both sides: a hostile count must be refused with the arithmetic the error
/// reports, and a payload that fits *exactly* must still decode — an
/// off-by-one in the other direction turns a legitimate value into corruption.
#[cfg(test)]
mod alloc_guard_tests {
    use super::*;

    /// `[error_rate f64][expansion u32][non_scaling u8][num_layers u32]` + tail.
    fn bloom_payload(num_layers: u32, tail: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&0.01f64.to_le_bytes());
        p.extend_from_slice(&2u32.to_le_bytes());
        p.push(0);
        p.extend_from_slice(&num_layers.to_le_bytes());
        p.extend_from_slice(tail);
        p
    }

    /// `[bucket_size u8][max_iterations u16][expansion u32][delete_count u64][num_layers u32]` + tail.
    fn cuckoo_payload(num_layers: u32, tail: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        p.push(1);
        p.extend_from_slice(&20u16.to_le_bytes());
        p.extend_from_slice(&2u32.to_le_bytes());
        p.extend_from_slice(&0u64.to_le_bytes());
        p.extend_from_slice(&num_layers.to_le_bytes());
        p.extend_from_slice(tail);
        p
    }

    /// Five `f64` header fields, then the two counts, then the centroid data.
    fn tdigest_payload(num_centroids: u32, num_unmerged: u32, tail: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        for f in [100.0f64, 1.0, 9.0, 4.0, 2.0] {
            p.extend_from_slice(&f.to_le_bytes());
        }
        p.extend_from_slice(&num_centroids.to_le_bytes());
        p.extend_from_slice(&num_unmerged.to_le_bytes());
        p.extend_from_slice(tail);
        p
    }

    fn centroid_bytes(pairs: &[(f64, f64)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (mean, weight) in pairs {
            out.extend_from_slice(&mean.to_le_bytes());
            out.extend_from_slice(&weight.to_le_bytes());
        }
        out
    }

    /// A layer count that 27 trailing bytes cannot cover is refused *before*
    /// the allocation, and the error states how many bytes the claim needed
    /// (header already consumed + 28 per claimed layer) against how many
    /// arrived — not a generic "bad payload".
    #[test]
    fn a_bloom_layer_count_the_payload_cannot_cover_is_refused_with_its_arithmetic() {
        let payload = bloom_payload(5, &[0u8; 27]);
        let err = deserialize_bloom_filter(&payload).expect_err("5 layers cannot fit in 27 bytes");
        let SerializationError::Truncated { expected, actual } = err else {
            panic!("expected a truncation, got {err:?}");
        };
        assert_eq!(actual, payload.len(), "the error reports the bytes it had");
        assert_eq!(
            expected,
            17 + 5 * 28,
            "the claim needs the consumed header plus a minimal layer each"
        );
    }

    /// The guard is `>`, not `>=`: a payload holding exactly the bytes its
    /// layer count needs is legitimate and must decode, fields intact.
    #[test]
    fn a_bloom_payload_that_fits_exactly_still_decodes() {
        let mut tail = Vec::new();
        tail.extend_from_slice(&3u32.to_le_bytes()); // k
        tail.extend_from_slice(&7u64.to_le_bytes()); // count
        tail.extend_from_slice(&64u64.to_le_bytes()); // capacity
        tail.extend_from_slice(&8u64.to_le_bytes()); // bits_len
        tail.push(0b1010_0101);
        assert_eq!(tail.len(), 29, "one minimal layer plus a byte of bits");

        let bf = deserialize_bloom_filter(&bloom_payload(1, &tail)).expect("an exact fit decodes");

        assert_eq!(bf.error_rate(), 0.01);
        assert_eq!(bf.expansion(), 2);
        assert!(!bf.is_non_scaling());
        assert_eq!(bf.num_layers(), 1);
        let layer = &bf.layers()[0];
        assert_eq!(layer.k(), 3);
        assert_eq!(layer.count(), 7);
        assert_eq!(layer.capacity(), 64);
        assert_eq!(layer.size_bits(), 8);
        assert_eq!(layer.bits_as_bytes(), &[0b1010_0101]);
    }

    /// Same guard, cuckoo's 25-byte minimal layer.
    #[test]
    fn a_cuckoo_layer_count_the_payload_cannot_cover_is_refused_with_its_arithmetic() {
        let payload = cuckoo_payload(5, &[0u8; 24]);
        let err = deserialize_cuckoo_filter(&payload).expect_err("5 layers cannot fit in 24 bytes");
        let SerializationError::Truncated { expected, actual } = err else {
            panic!("expected a truncation, got {err:?}");
        };
        assert_eq!(actual, payload.len());
        assert_eq!(expected, 19 + 5 * 25);
    }

    #[test]
    fn a_cuckoo_payload_that_fits_exactly_still_decodes() {
        let mut tail = Vec::new();
        tail.extend_from_slice(&1u64.to_le_bytes()); // num_buckets
        tail.push(1); // bucket_size
        tail.extend_from_slice(&1u64.to_le_bytes()); // count
        tail.extend_from_slice(&8u64.to_le_bytes()); // capacity
        tail.extend_from_slice(&0xBEEFu16.to_le_bytes()); // the one fingerprint
        assert_eq!(tail.len(), 27, "one minimal layer plus one fingerprint");

        let cf =
            deserialize_cuckoo_filter(&cuckoo_payload(1, &tail)).expect("an exact fit decodes");

        assert_eq!(cf.num_layers(), 1);
        let layer = &cf.layers()[0];
        assert_eq!(layer.num_buckets(), 1);
        assert_eq!(layer.bucket_size(), 1);
        assert_eq!(layer.buckets().len(), 1);
        assert_eq!(layer.buckets()[0], vec![0xBEEFu16]);
    }

    /// A bucket size of zero paired with a positive bucket count is the
    /// pathological case: every bucket reads as empty, so the fingerprint
    /// region has no size and the count can be arbitrarily large for free.
    /// It is refused. A layer with *no* buckets and no bucket size is a
    /// different thing — a legitimately empty layer — and still decodes.
    #[test]
    fn a_cuckoo_layer_with_buckets_but_no_bucket_size_is_refused() {
        let mut hostile = Vec::new();
        hostile.extend_from_slice(&1u64.to_le_bytes()); // num_buckets
        hostile.push(0); // bucket_size
        hostile.extend_from_slice(&0u64.to_le_bytes()); // count
        hostile.extend_from_slice(&8u64.to_le_bytes()); // capacity
        let err = deserialize_cuckoo_filter(&cuckoo_payload(1, &hostile))
            .expect_err("buckets with no bucket size must be refused");
        let SerializationError::InvalidPayload(msg) = err else {
            panic!("expected an invalid payload, got {err:?}");
        };
        assert!(msg.contains("bucket size"), "{msg}");

        let mut empty = Vec::new();
        empty.extend_from_slice(&0u64.to_le_bytes()); // num_buckets
        empty.push(0); // bucket_size
        empty.extend_from_slice(&0u64.to_le_bytes()); // count
        empty.extend_from_slice(&0u64.to_le_bytes()); // capacity
        let cf = deserialize_cuckoo_filter(&cuckoo_payload(1, &empty))
            .expect("an empty layer is not a hostile one");
        assert_eq!(cf.num_layers(), 1);
        assert_eq!(cf.layers()[0].num_buckets(), 0);
    }

    /// The per-layer fingerprint guard compares against the bytes that remain
    /// for the *whole* payload, so a layer followed by more layers has far more
    /// remaining than it needs. Only "needs more than is left" is an error;
    /// "needs less" is the normal case for every layer but the last.
    #[test]
    fn a_cuckoo_layer_may_need_fewer_bytes_than_the_payload_has_left() {
        fn layer(fingerprint: u16) -> Vec<u8> {
            let mut l = Vec::new();
            l.extend_from_slice(&1u64.to_le_bytes()); // num_buckets
            l.push(1); // bucket_size
            l.extend_from_slice(&1u64.to_le_bytes()); // count
            l.extend_from_slice(&8u64.to_le_bytes()); // capacity
            l.extend_from_slice(&fingerprint.to_le_bytes());
            l
        }
        let mut tail = layer(0x1111);
        tail.extend_from_slice(&layer(0x2222));

        let cf = deserialize_cuckoo_filter(&cuckoo_payload(2, &tail))
            .expect("two layers decode, the first with bytes to spare");

        assert_eq!(cf.num_layers(), 2);
        assert_eq!(cf.layers()[0].buckets()[0], vec![0x1111u16]);
        assert_eq!(
            cf.layers()[1].buckets()[0],
            vec![0x2222u16],
            "the layers are read in order, not merged"
        );
    }

    /// The t-digest guard sizes *both* arrays at once — centroids plus
    /// unmerged, 16 bytes each — so a claim covering only one of them is still
    /// refused, and refused as an `InvalidPayload` naming the centroid region
    /// rather than as whatever the reader would have hit further in.
    #[test]
    fn a_tdigest_centroid_claim_larger_than_the_payload_is_refused() {
        let payload = tdigest_payload(3, 2, &[0u8; 32]);
        let err = deserialize_tdigest(&payload).expect_err("5 centroids need 80 bytes, not 32");
        let SerializationError::InvalidPayload(msg) = err else {
            panic!("expected an invalid payload, got {err:?}");
        };
        assert!(
            msg.contains("centroid"),
            "the message must name the region that came up short: {msg}"
        );
    }

    /// Exactly enough bytes for both arrays decodes, and the two arrays are
    /// read in order rather than merged: the last three pairs are `unmerged`.
    #[test]
    fn a_tdigest_payload_that_fits_exactly_still_decodes() {
        let centroids = [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)];
        let unmerged = [(7.0, 8.0), (9.0, 10.0)];
        let mut tail = centroid_bytes(&centroids);
        tail.extend_from_slice(&centroid_bytes(&unmerged));
        assert_eq!(tail.len(), 5 * 16, "exactly the bytes the counts claim");

        let td = deserialize_tdigest(&tdigest_payload(3, 2, &tail)).expect("an exact fit decodes");

        assert_eq!(td.compression(), 100.0);
        assert_eq!(
            td.centroids()
                .iter()
                .map(|c| (c.mean, c.weight))
                .collect::<Vec<_>>(),
            centroids.to_vec()
        );
        assert_eq!(
            td.unmerged()
                .iter()
                .map(|c| (c.mean, c.weight))
                .collect::<Vec<_>>(),
            unmerged.to_vec(),
            "the unmerged buffer is the tail of the payload, not part of the centroids"
        );
    }

    /// A digest that carries *merged* centroids as well as an unmerged tail
    /// re-serializes to exactly the bytes it was decoded from. A freshly built
    /// `TDigestValue` only ever fills the unmerged buffer, so without this the
    /// `centroids().len() * 16` term of the pre-sized payload is never
    /// exercised and a wrong figure there rots silently (the `debug_assert_eq!`
    /// on the written length is the only place a bad capacity is observable).
    #[test]
    fn a_digest_with_merged_centroids_round_trips_at_its_pre_sized_length() {
        let centroids = [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)];
        let unmerged = [(7.0, 8.0), (9.0, 10.0)];
        let mut tail = centroid_bytes(&centroids);
        tail.extend_from_slice(&centroid_bytes(&unmerged));
        let payload = tdigest_payload(3, 2, &tail);

        let td = deserialize_tdigest(&payload).expect("the fixture decodes");
        assert_eq!(td.centroids().len(), 3, "the merged array is populated");
        assert_eq!(td.unmerged().len(), 2, "and so is the unmerged tail");

        let (marker, bytes) = serialize_tdigest(&td);
        assert_eq!(marker, TypeMarker::TDigest);
        assert_eq!(
            bytes.len(),
            8 * 5 + 4 + 4 + 5 * 16,
            "header, both counts, then 16 bytes per centroid across both arrays"
        );
        assert_eq!(bytes, payload, "the encoding is byte-identical");
    }
}

/// The two framing helpers the HLL merge operator runs on every merge. Both
/// take *borrowed* bytes straight off RocksDB and both answer with `Option`,
/// so an off-by-one at either boundary turns a valid merge into a dropped one
/// (RocksDB treats a `None` merge result as a failed merge, not as a no-op).
#[cfg(test)]
mod frame_boundary_tests {
    use super::*;

    fn frame(payload: &[u8]) -> Vec<u8> {
        build_frame(TypeMarker::HyperLogLog, &KeyMetadata::new(1), payload)
    }

    /// A header with nothing after it is a *well-formed* frame carrying an
    /// empty payload — the boundary is "shorter than a header", not "no longer
    /// than a header".
    #[test]
    fn a_header_with_an_empty_payload_is_a_valid_frame() {
        let f = frame(&[]);
        assert_eq!(f.len(), HEADER_SIZE);
        assert_eq!(
            framed_payload(&f),
            Some(&[][..]),
            "an empty payload is a payload"
        );
        assert_eq!(
            framed_payload(&f[..HEADER_SIZE - 1]),
            None,
            "one byte short of a header is not a frame"
        );
    }

    /// The declared payload length delimits the value: a frame that ends
    /// exactly on it is complete, one byte less is truncated, and trailing
    /// bytes belong to whoever framed the buffer.
    #[test]
    fn the_declared_length_delimits_the_payload_exactly() {
        let f = frame(b"abc");
        assert_eq!(framed_payload(&f), Some(&b"abc"[..]));
        assert_eq!(
            framed_payload(&f[..f.len() - 1]),
            None,
            "a payload cut short is not readable"
        );

        let mut with_tail = f.clone();
        with_tail.extend_from_slice(b"not mine");
        assert_eq!(
            framed_payload(&with_tail),
            Some(&b"abc"[..]),
            "surplus bytes are not part of the value"
        );
    }

    /// Re-framing copies the first 16 header bytes verbatim — marker, flags,
    /// expiry, LFU — and rewrites only the payload length. Sixteen bytes is
    /// exactly enough to do that; fifteen is not.
    #[test]
    fn re_framing_keeps_the_metadata_and_rewrites_only_the_length() {
        let f = frame(b"abc");
        let out = reframe_with_header(&f, b"wxyz").expect("a full frame is a valid header source");
        assert_eq!(&out[..16], &f[..16], "the metadata half is copied verbatim");
        assert_eq!(
            u64::from_le_bytes(out[16..24].try_into().unwrap()),
            4,
            "the length is the new payload's"
        );
        assert_eq!(&out[HEADER_SIZE..], b"wxyz");

        assert!(
            reframe_with_header(&f[..16], b"wxyz").is_some(),
            "sixteen bytes is exactly the metadata half, so it is enough"
        );
        assert!(
            reframe_with_header(&f[..15], b"wxyz").is_none(),
            "fifteen bytes cannot supply the metadata half"
        );
    }
}

/// Top-K and CMS both size a `depth * width * 8` region off two wire counts,
/// which is the largest attacker-controlled allocation in the codec. Their
/// guards get the same two-sided treatment as the filters above.
#[cfg(test)]
mod sketch_guard_tests {
    use super::*;

    /// `[k u32][width u32][depth u32][decay f64]` + tail.
    fn topk_payload(k: u32, width: u32, depth: u32, tail: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&k.to_le_bytes());
        p.extend_from_slice(&width.to_le_bytes());
        p.extend_from_slice(&depth.to_le_bytes());
        p.extend_from_slice(&0.9f64.to_le_bytes());
        p.extend_from_slice(tail);
        p
    }

    /// `[width u32][depth u32][count u64]` + tail.
    fn cms_payload(width: u32, depth: u32, tail: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&width.to_le_bytes());
        p.extend_from_slice(&depth.to_le_bytes());
        p.extend_from_slice(&42u64.to_le_bytes());
        p.extend_from_slice(tail);
        p
    }

    /// A million-by-million sketch claimed by a 28-byte payload is refused
    /// before the allocation, and the error's arithmetic is the claim, not the
    /// bytes that happen to be left.
    #[test]
    fn a_topk_bucket_region_larger_than_the_payload_is_refused_with_its_arithmetic() {
        let payload = topk_payload(5, 1000, 1000, &[0u8; 8]);
        let err = deserialize_topk(&payload).expect_err("a 8 MB bucket claim cannot be honoured");
        let SerializationError::Truncated { expected, actual } = err else {
            panic!("expected a truncation, got {err:?}");
        };
        assert_eq!(actual, payload.len());
        assert_eq!(
            expected,
            20 + 1000 * 1000 * 8,
            "the claim is the consumed header plus the buckets it demands"
        );
    }

    /// The bucket region legitimately ends *before* the payload does — the heap
    /// follows it — so "needs fewer bytes than remain" is the normal case.
    #[test]
    fn a_topk_payload_with_room_to_spare_decodes() {
        let mut tail = Vec::new();
        tail.extend_from_slice(&7u32.to_le_bytes()); // fingerprint
        tail.extend_from_slice(&3u32.to_le_bytes()); // counter
        tail.extend_from_slice(&0u32.to_le_bytes()); // heap_len

        let tk = deserialize_topk(&topk_payload(5, 1, 1, &tail)).expect("one bucket, empty heap");

        assert_eq!(tk.k(), 5);
        assert_eq!(tk.width(), 1);
        assert_eq!(tk.depth(), 1);
        assert_eq!(tk.decay(), 0.9);
        assert_eq!(tk.buckets_raw(), vec![vec![(7u32, 3u32)]]);
        assert!(tk.heap_items().is_empty());
    }

    /// A bucket region that consumes the payload exactly leaves no heap length
    /// behind it, which is a truncation *after* the guard, not at it. The guard
    /// must let it through: reporting a shortfall of zero bytes here would be a
    /// truncation error that claims it needed nothing more than it had.
    #[test]
    fn a_topk_payload_whose_buckets_consume_it_fails_on_the_missing_heap() {
        let mut tail = Vec::new();
        tail.extend_from_slice(&7u32.to_le_bytes());
        tail.extend_from_slice(&3u32.to_le_bytes());
        let payload = topk_payload(5, 1, 1, &tail);

        let err = deserialize_topk(&payload).expect_err("the heap length is missing");
        let SerializationError::Truncated { expected, actual } = err else {
            panic!("expected a truncation, got {err:?}");
        };
        assert_eq!(actual, payload.len());
        assert_eq!(
            expected,
            payload.len() + 4,
            "what is missing is the heap-length prefix, not bucket bytes"
        );
    }

    #[test]
    fn a_cms_counter_region_larger_than_the_payload_is_refused_with_its_arithmetic() {
        let payload = cms_payload(1000, 1000, &[0u8; 8]);
        let err = deserialize_cms(&payload).expect_err("a 8 MB counter claim cannot be honoured");
        let SerializationError::Truncated { expected, actual } = err else {
            panic!("expected a truncation, got {err:?}");
        };
        assert_eq!(actual, payload.len());
        assert_eq!(expected, 16 + 1000 * 1000 * 8);
    }

    /// A CMS payload is header + counters with nothing after it, so a valid one
    /// sits exactly on the guard's boundary and must still decode.
    #[test]
    fn a_cms_payload_that_fits_exactly_still_decodes() {
        let cms = deserialize_cms(&cms_payload(1, 1, &99u64.to_le_bytes()))
            .expect("an exact fit decodes");

        assert_eq!(cms.width(), 1);
        assert_eq!(cms.depth(), 1);
        assert_eq!(cms.count(), 42);
        assert_eq!(cms.counters_raw(), vec![vec![99u64]]);
    }
}
