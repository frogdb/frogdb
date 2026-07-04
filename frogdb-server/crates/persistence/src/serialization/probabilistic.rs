use bitvec::prelude::*;
use frogdb_types::bloom::{BloomFilterValue, BloomLayer};
use frogdb_types::cms::CountMinSketchValue;
use frogdb_types::cuckoo::{CuckooFilterValue, CuckooLayer};
use frogdb_types::hyperloglog::{HLL_DENSE_SIZE, HyperLogLogValue};
use frogdb_types::tdigest::{Centroid, TDigestValue};
use frogdb_types::topk::TopKValue;

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
