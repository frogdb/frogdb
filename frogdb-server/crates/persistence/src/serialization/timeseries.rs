use frogdb_types::timeseries::{CompressedChunk, DuplicatePolicy, TimeSeriesValue};

use super::*;

/// Serialize a time series.
///
/// Format:
/// - retention_ms (8 bytes u64)
/// - duplicate_policy (1 byte)
/// - chunk_size (4 bytes u32)
/// - num_labels (4 bytes u32)
/// - for each label:
///   - name_len (4 bytes u32) + name bytes
///   - value_len (4 bytes u32) + value bytes
/// - num_chunks (4 bytes u32)
/// - for each chunk:
///   - start_time (8 bytes i64)
///   - end_time (8 bytes i64)
///   - sample_count (4 bytes u32)
///   - data_len (4 bytes u32)
///   - data bytes
/// - num_active (4 bytes u32)
/// - for each active sample:
///   - timestamp (8 bytes i64)
///   - value (8 bytes f64)
pub(super) fn serialize_timeseries(ts: &TimeSeriesValue) -> (u8, Vec<u8>) {
    let mut payload = Vec::new();

    // Retention
    payload.extend_from_slice(&ts.retention_ms().to_le_bytes());

    // Duplicate policy
    let policy_byte = match ts.duplicate_policy() {
        DuplicatePolicy::Block => 0u8,
        DuplicatePolicy::First => 1u8,
        DuplicatePolicy::Last => 2u8,
        DuplicatePolicy::Min => 3u8,
        DuplicatePolicy::Max => 4u8,
        DuplicatePolicy::Sum => 5u8,
    };
    payload.push(policy_byte);

    // Chunk size
    payload.extend_from_slice(&(ts.chunk_size() as u32).to_le_bytes());

    // Labels
    let labels = ts.labels();
    payload.extend_from_slice(&(labels.len() as u32).to_le_bytes());
    for (name, value) in labels {
        payload.extend_from_slice(&(name.len() as u32).to_le_bytes());
        payload.extend_from_slice(name.as_bytes());
        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
        payload.extend_from_slice(value.as_bytes());
    }

    // Chunks
    let chunks = ts.chunks();
    payload.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    for chunk in chunks {
        payload.extend_from_slice(&chunk.start_time().to_le_bytes());
        payload.extend_from_slice(&chunk.end_time().to_le_bytes());
        payload.extend_from_slice(&chunk.sample_count().to_le_bytes());
        let data = chunk.data();
        payload.extend_from_slice(&(data.len() as u32).to_le_bytes());
        payload.extend_from_slice(data);
    }

    // Active samples
    let active = ts.active_samples();
    payload.extend_from_slice(&(active.len() as u32).to_le_bytes());
    for (&ts_val, &val) in active {
        payload.extend_from_slice(&ts_val.to_le_bytes());
        payload.extend_from_slice(&val.to_le_bytes());
    }

    (TYPE_TIMESERIES, payload)
}

/// Deserialize a time series from payload.
pub(super) fn deserialize_timeseries(
    payload: &[u8],
) -> Result<TimeSeriesValue, SerializationError> {
    if payload.len() < 17 {
        return Err(SerializationError::InvalidPayload(
            "TimeSeries payload too short for header".to_string(),
        ));
    }

    let mut offset = 0;

    // Retention
    let retention_ms = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
    offset += 8;

    // Duplicate policy
    let policy = match payload[offset] {
        0 => DuplicatePolicy::Block,
        1 => DuplicatePolicy::First,
        2 => DuplicatePolicy::Last,
        3 => DuplicatePolicy::Min,
        4 => DuplicatePolicy::Max,
        5 => DuplicatePolicy::Sum,
        _ => DuplicatePolicy::Last, // Default fallback
    };
    offset += 1;

    // Chunk size
    let chunk_size = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    // Labels
    if 4 > payload.len() - offset {
        return Err(SerializationError::InvalidPayload(
            "TimeSeries payload truncated at labels count".to_string(),
        ));
    }
    let num_labels = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut labels = Vec::with_capacity(safe_capacity(num_labels, 8, payload.len() - offset));
    for _ in 0..num_labels {
        // Name
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at label name length".to_string(),
            ));
        }
        let name_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if name_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at label name".to_string(),
            ));
        }
        let name = String::from_utf8_lossy(&payload[offset..offset + name_len]).to_string();
        offset += name_len;

        // Value
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at label value length".to_string(),
            ));
        }
        let value_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if value_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at label value".to_string(),
            ));
        }
        let value = String::from_utf8_lossy(&payload[offset..offset + value_len]).to_string();
        offset += value_len;

        labels.push((name, value));
    }

    // Chunks
    if 4 > payload.len() - offset {
        return Err(SerializationError::InvalidPayload(
            "TimeSeries payload truncated at chunks count".to_string(),
        ));
    }
    let num_chunks = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut chunks = Vec::with_capacity(safe_capacity(num_chunks, 24, payload.len() - offset));
    for _ in 0..num_chunks {
        if 24 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at chunk header".to_string(),
            ));
        }

        let start_time = i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let end_time = i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let sample_count = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let data_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if data_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at chunk data".to_string(),
            ));
        }
        let data = payload[offset..offset + data_len].to_vec();
        offset += data_len;

        chunks.push(CompressedChunk::from_raw(
            data,
            start_time,
            end_time,
            sample_count,
        ));
    }

    // Active samples
    if 4 > payload.len() - offset {
        return Err(SerializationError::InvalidPayload(
            "TimeSeries payload truncated at active samples count".to_string(),
        ));
    }
    let num_active = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut active_samples = std::collections::BTreeMap::new();
    for _ in 0..num_active {
        if 16 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "TimeSeries payload truncated at active sample".to_string(),
            ));
        }

        let ts = i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let val = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        active_samples.insert(ts, val);
    }

    Ok(TimeSeriesValue::from_raw(
        active_samples,
        chunks,
        labels,
        retention_ms,
        policy,
        chunk_size,
    ))
}
