use bytes::Bytes;

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
pub(super) fn serialize_bloom_filter(bf: &BloomFilterValue) -> (u8, Vec<u8>) {
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

    (TYPE_BLOOM, payload)
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
pub(super) fn serialize_cuckoo_filter(cf: &CuckooFilterValue) -> (u8, Vec<u8>) {
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

    (TYPE_CUCKOO, payload)
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
pub(super) fn serialize_tdigest(td: &TDigestValue) -> (u8, Vec<u8>) {
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

    (TYPE_TDIGEST, payload)
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
pub(super) fn serialize_hyperloglog(hll: &HyperLogLogValue) -> (u8, Vec<u8>) {
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

        (TYPE_HYPERLOGLOG, payload)
    } else if let Some(registers) = hll.as_dense() {
        // Dense encoding
        let mut payload = Vec::with_capacity(1 + HLL_DENSE_SIZE);

        // Encoding byte (1 = dense)
        payload.push(1);

        // Raw registers
        payload.extend_from_slice(registers.as_slice());

        (TYPE_HYPERLOGLOG, payload)
    } else {
        // Shouldn't happen, but fallback to empty sparse
        (TYPE_HYPERLOGLOG, vec![0, 0, 0, 0, 0])
    }
}

/// Serialize a Top-K value.
///
/// Format: [k:u32][width:u32][depth:u32][decay:f64][buckets: depth*width*(fp:u32+ctr:u32)][heap_len:u32][for each: item_len:u32, item_bytes, count:u64]
pub(super) fn serialize_topk(tk: &TopKValue) -> (u8, Vec<u8>) {
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

    (TYPE_TOPK, payload)
}

/// Serialize a Count-Min Sketch value.
///
/// Format: [width:u32][depth:u32][count:u64][counters: depth*width u64 LE values]
pub(super) fn serialize_cms(cms: &CountMinSketchValue) -> (u8, Vec<u8>) {
    let mut payload = Vec::new();
    payload.extend_from_slice(&cms.width().to_le_bytes());
    payload.extend_from_slice(&cms.depth().to_le_bytes());
    payload.extend_from_slice(&cms.count().to_le_bytes());

    for row in cms.counters_raw() {
        for &val in row {
            payload.extend_from_slice(&val.to_le_bytes());
        }
    }

    (TYPE_CMS, payload)
}

/// Deserialize a bloom filter from payload.
pub(super) fn deserialize_bloom_filter(
    payload: &[u8],
) -> Result<BloomFilterValue, SerializationError> {
    if payload.len() < 17 {
        return Err(SerializationError::InvalidPayload(
            "Bloom filter payload too short for header".to_string(),
        ));
    }

    // Read error_rate (8 bytes)
    let error_rate = f64::from_le_bytes(payload[0..8].try_into().unwrap());

    // Read expansion (4 bytes)
    let expansion = u32::from_le_bytes(payload[8..12].try_into().unwrap());

    // Read non_scaling (1 byte)
    let non_scaling = payload[12] != 0;

    // Read num_layers (4 bytes)
    let num_layers = u32::from_le_bytes(payload[13..17].try_into().unwrap()) as usize;

    let mut offset = 17;
    let mut layers = Vec::with_capacity(num_layers);

    for _ in 0..num_layers {
        // Read k (4 bytes)
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at k".to_string(),
            ));
        }
        let k = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // Read count (8 bytes)
        if 8 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at count".to_string(),
            ));
        }
        let count = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read capacity (8 bytes)
        if 8 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at capacity".to_string(),
            ));
        }
        let capacity = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read bits_len (8 bytes)
        if 8 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at bits_len".to_string(),
            ));
        }
        let bits_len = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        // Read bits bytes (bits_len / 8 rounded up)
        let bytes_needed = bits_len.div_ceil(8);
        if bytes_needed > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Bloom filter payload truncated at bits data".to_string(),
            ));
        }
        let bits_bytes = &payload[offset..offset + bytes_needed];
        offset += bytes_needed;

        // Reconstruct the bitvec
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
    // Header: bucket_size(1) + max_iterations(2) + expansion(4) + delete_count(8) + num_layers(4) = 19
    if payload.len() < 19 {
        return Err(SerializationError::InvalidPayload(
            "Cuckoo filter payload too short for header".to_string(),
        ));
    }

    let mut offset = 0;
    let bucket_size = payload[offset];
    offset += 1;

    let max_iterations = u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap());
    offset += 2;

    let expansion = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap());
    offset += 4;

    let delete_count = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let num_layers = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut layers = Vec::with_capacity(num_layers);

    for _ in 0..num_layers {
        // Layer header: num_buckets(8) + bucket_size(1) + count(8) + capacity(8) = 25
        if 25 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Cuckoo filter payload truncated at layer header".to_string(),
            ));
        }

        let num_buckets =
            u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        let layer_bucket_size = payload[offset];
        offset += 1;

        let count = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let capacity = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let fp_bytes = num_buckets
            .checked_mul(layer_bucket_size as usize)
            .and_then(|v| v.checked_mul(2))
            .ok_or_else(|| {
                SerializationError::InvalidPayload(
                    "Cuckoo filter fingerprint data size overflow".to_string(),
                )
            })?;
        if fp_bytes > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Cuckoo filter payload truncated at fingerprint data".to_string(),
            ));
        }

        let mut buckets = Vec::with_capacity(num_buckets);
        for _ in 0..num_buckets {
            let mut bucket = Vec::with_capacity(layer_bucket_size as usize);
            for _ in 0..layer_bucket_size {
                let fp = u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap());
                offset += 2;
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
    // Header: compression(8) + min(8) + max(8) + merged_weight(8) + unmerged_weight(8) + num_centroids(4) + num_unmerged(4) = 48
    if payload.len() < 48 {
        return Err(SerializationError::InvalidPayload(
            "T-Digest payload too short for header".to_string(),
        ));
    }

    let mut offset = 0;

    let compression = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let min = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let max = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let merged_weight = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let unmerged_weight = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let num_centroids =
        u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let num_unmerged = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let needed = (num_centroids + num_unmerged) * 16;
    if needed > payload.len() - offset {
        return Err(SerializationError::InvalidPayload(
            "T-Digest payload truncated at centroid data".to_string(),
        ));
    }

    let mut centroids = Vec::with_capacity(num_centroids);
    for _ in 0..num_centroids {
        let mean = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let weight = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        centroids.push(Centroid { mean, weight });
    }

    let mut unmerged = Vec::with_capacity(num_unmerged);
    for _ in 0..num_unmerged {
        let mean = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let weight = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
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
    if payload.is_empty() {
        return Err(SerializationError::InvalidPayload(
            "HyperLogLog payload empty".to_string(),
        ));
    }

    let encoding = payload[0];

    match encoding {
        0 => {
            // Sparse encoding
            if payload.len() < 5 {
                return Err(SerializationError::InvalidPayload(
                    "HyperLogLog sparse payload too short".to_string(),
                ));
            }

            let num_entries = u32::from_le_bytes(payload[1..5].try_into().unwrap()) as usize;

            // Each entry is 3 bytes (u16 index + u8 value)
            let expected_len = num_entries
                .checked_mul(3)
                .and_then(|v| v.checked_add(5))
                .ok_or(SerializationError::InvalidPayload(
                    "HyperLogLog sparse payload size overflow".to_string(),
                ))?;
            if payload.len() < expected_len {
                return Err(SerializationError::InvalidPayload(
                    "HyperLogLog sparse payload truncated".to_string(),
                ));
            }

            let mut pairs = Vec::with_capacity(num_entries);
            let mut offset = 5;

            for _ in 0..num_entries {
                let index = u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap());
                let value = payload[offset + 2];
                pairs.push((index, value));
                offset += 3;
            }

            Ok(HyperLogLogValue::from_sparse(pairs))
        }
        1 => {
            // Dense encoding
            if payload.len() < 1 + HLL_DENSE_SIZE {
                return Err(SerializationError::InvalidPayload(
                    "HyperLogLog dense payload truncated".to_string(),
                ));
            }

            let mut registers = Box::new([0u8; HLL_DENSE_SIZE]);
            registers.copy_from_slice(&payload[1..1 + HLL_DENSE_SIZE]);

            Ok(HyperLogLogValue::from_dense(registers))
        }
        _ => Err(SerializationError::InvalidPayload(format!(
            "Unknown HyperLogLog encoding: {}",
            encoding
        ))),
    }
}

/// Deserialize a Top-K value.
pub(super) fn deserialize_topk(payload: &[u8]) -> Result<TopKValue, SerializationError> {
    if payload.len() < 20 {
        return Err(SerializationError::InvalidPayload(
            "Top-K payload too short".to_string(),
        ));
    }

    let mut pos = 0;
    let k = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
    pos += 4;
    let width = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
    pos += 4;
    let depth = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
    pos += 4;
    let decay = f64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
    pos += 8;

    let bucket_bytes_needed = (depth as usize)
        .checked_mul(width as usize)
        .and_then(|v| v.checked_mul(8))
        .ok_or_else(|| {
            SerializationError::InvalidPayload("TopK bucket data size overflow".to_string())
        })?;
    if pos + bucket_bytes_needed > payload.len() {
        return Err(SerializationError::Truncated {
            expected: pos + bucket_bytes_needed,
            actual: payload.len(),
        });
    }

    let mut buckets = Vec::with_capacity(depth as usize);
    for _ in 0..depth {
        let mut row = Vec::with_capacity(width as usize);
        for _ in 0..width {
            let fp = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let ctr = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
            pos += 4;
            row.push((fp, ctr));
        }
        buckets.push(row);
    }

    if pos + 4 > payload.len() {
        return Err(SerializationError::Truncated {
            expected: pos + 4,
            actual: payload.len(),
        });
    }
    let heap_len = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    let mut heap_items = Vec::with_capacity(heap_len);
    for _ in 0..heap_len {
        if pos + 4 > payload.len() {
            return Err(SerializationError::Truncated {
                expected: pos + 4,
                actual: payload.len(),
            });
        }
        let item_len = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + item_len > payload.len() {
            return Err(SerializationError::Truncated {
                expected: pos + item_len,
                actual: payload.len(),
            });
        }
        let item = Bytes::copy_from_slice(&payload[pos..pos + item_len]);
        pos += item_len;
        if pos + 8 > payload.len() {
            return Err(SerializationError::Truncated {
                expected: pos + 8,
                actual: payload.len(),
            });
        }
        let count = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        heap_items.push((item, count));
    }

    Ok(TopKValue::from_raw(
        k, width, depth, decay, buckets, heap_items,
    ))
}

/// Deserialize a Count-Min Sketch value.
pub(super) fn deserialize_cms(payload: &[u8]) -> Result<CountMinSketchValue, SerializationError> {
    if payload.len() < 16 {
        return Err(SerializationError::InvalidPayload(
            "CMS payload too short".to_string(),
        ));
    }

    let mut pos = 0;
    let width = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
    pos += 4;
    let depth = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
    pos += 4;
    let count = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
    pos += 8;

    let counter_bytes_needed = (depth as usize)
        .checked_mul(width as usize)
        .and_then(|v| v.checked_mul(8))
        .ok_or_else(|| {
            SerializationError::InvalidPayload("CMS counter data size overflow".to_string())
        })?;
    if pos + counter_bytes_needed > payload.len() {
        return Err(SerializationError::Truncated {
            expected: pos + counter_bytes_needed,
            actual: payload.len(),
        });
    }

    let mut counters = Vec::with_capacity(depth as usize);
    for _ in 0..depth {
        let mut row = Vec::with_capacity(width as usize);
        for _ in 0..width {
            let val = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
            pos += 8;
            row.push(val);
        }
        counters.push(row);
    }

    Ok(CountMinSketchValue::from_raw(width, depth, count, counters))
}
