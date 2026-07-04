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
pub(super) fn serialize_timeseries(ts: &TimeSeriesValue) -> (TypeMarker, Vec<u8>) {
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

    (TypeMarker::TimeSeries, payload)
}

/// Deserialize a time series from payload.
pub(super) fn deserialize_timeseries(
    payload: &[u8],
) -> Result<TimeSeriesValue, SerializationError> {
    let mut reader = FrameReader::new(payload);

    // Retention
    let retention_ms = reader.read_le_u64()?;

    // Duplicate policy. Reject unknown bytes rather than silently coercing to a
    // policy, matching every other enum decode in the module (e.g. VectorSet
    // metric/quantization) and preserving encode<->decode symmetry.
    let policy = match reader.read_u8()? {
        0 => DuplicatePolicy::Block,
        1 => DuplicatePolicy::First,
        2 => DuplicatePolicy::Last,
        3 => DuplicatePolicy::Min,
        4 => DuplicatePolicy::Max,
        5 => DuplicatePolicy::Sum,
        other => {
            return Err(SerializationError::InvalidPayload(format!(
                "Unknown TimeSeries duplicate policy byte: {other}"
            )));
        }
    };

    // Chunk size
    let chunk_size = reader.read_le_u32()? as usize;

    // Labels
    let num_labels = reader.read_le_u32()? as usize;
    let mut labels = Vec::with_capacity(safe_capacity(num_labels, 8, reader.remaining()));
    for _ in 0..num_labels {
        let name = String::from_utf8_lossy(reader.read_u32_len_prefixed()?).to_string();
        let value = String::from_utf8_lossy(reader.read_u32_len_prefixed()?).to_string();
        labels.push((name, value));
    }

    // Chunks
    let num_chunks = reader.read_le_u32()? as usize;
    let mut chunks = Vec::with_capacity(safe_capacity(num_chunks, 24, reader.remaining()));
    for _ in 0..num_chunks {
        let start_time = reader.read_le_i64()?;
        let end_time = reader.read_le_i64()?;
        let sample_count = reader.read_le_u32()?;
        let data = reader.read_u32_len_prefixed()?.to_vec();

        chunks.push(CompressedChunk::from_raw(
            data,
            start_time,
            end_time,
            sample_count,
        ));
    }

    // Active samples
    let num_active = reader.read_le_u32()? as usize;
    let mut active_samples = std::collections::BTreeMap::new();
    for _ in 0..num_active {
        let ts = reader.read_le_i64()?;
        let val = reader.read_le_f64()?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every valid duplicate policy survives a round-trip.
    #[test]
    fn duplicate_policy_round_trips() {
        for policy in [
            DuplicatePolicy::Block,
            DuplicatePolicy::First,
            DuplicatePolicy::Last,
            DuplicatePolicy::Min,
            DuplicatePolicy::Max,
            DuplicatePolicy::Sum,
        ] {
            let ts = TimeSeriesValue::with_options(0, policy, 4096, Vec::new());
            let (_marker, payload) = serialize_timeseries(&ts);
            let back = deserialize_timeseries(&payload).unwrap();
            assert_eq!(back.duplicate_policy(), policy);
        }
    }

    /// An unknown duplicate-policy byte must error, not silently coerce to `Last`.
    /// Otherwise corrupted/future-version data is accepted as a different policy.
    #[test]
    fn deserialize_rejects_unknown_duplicate_policy() {
        let ts = TimeSeriesValue::new();
        let (_marker, mut payload) = serialize_timeseries(&ts);
        // The policy byte sits right after the 8-byte retention prefix.
        payload[8] = 99;
        let err = deserialize_timeseries(&payload).unwrap_err();
        assert!(
            matches!(err, SerializationError::InvalidPayload(_)),
            "expected InvalidPayload, got {err:?}"
        );
    }
}
