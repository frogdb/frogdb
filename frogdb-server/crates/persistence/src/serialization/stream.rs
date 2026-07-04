use frogdb_types::types::{IdempotencyState, StreamId, StreamIdSpec, StreamValue};

use super::*;

/// Serialize a stream.
///
/// Format:
/// - last_id_ms (8 bytes u64)
/// - last_id_seq (8 bytes u64)
/// - num_entries (4 bytes u32)
/// - for each entry:
///   - id_ms (8 bytes u64)
///   - id_seq (8 bytes u64)
///   - num_fields (4 bytes u32)
///   - for each field:
///     - field_len (4 bytes u32)
///     - field bytes
///     - value_len (4 bytes u32)
///     - value bytes
///
/// Note: Consumer groups are not persisted (they are ephemeral state)
pub(super) fn serialize_stream(stream: &StreamValue) -> (TypeMarker, Vec<u8>) {
    let entries = stream.to_vec();
    let last_id = stream.last_id();

    // Calculate size
    let mut payload_size = 8 + 8 + 4; // last_id_ms + last_id_seq + num_entries
    for entry in &entries {
        payload_size += 8 + 8 + 4; // id_ms + id_seq + num_fields
        for (field, value) in &entry.fields {
            payload_size += 4 + field.len() + 4 + value.len();
        }
    }

    let mut payload = Vec::with_capacity(payload_size);

    // Last ID
    payload.extend_from_slice(&last_id.ms.to_le_bytes());
    payload.extend_from_slice(&last_id.seq.to_le_bytes());

    // Number of entries
    payload.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    // Each entry
    for entry in entries {
        payload.extend_from_slice(&entry.id.ms.to_le_bytes());
        payload.extend_from_slice(&entry.id.seq.to_le_bytes());
        payload.extend_from_slice(&(entry.fields.len() as u32).to_le_bytes());

        for (field, value) in &entry.fields {
            payload.extend_from_slice(&(field.len() as u32).to_le_bytes());
            payload.extend_from_slice(field);
            payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
            payload.extend_from_slice(value);
        }
    }

    // Event sourcing extension: total_appended + idempotency keys
    // (backward-compatible: old readers stop after entries)
    payload.extend_from_slice(&stream.total_appended().to_le_bytes()); // 8 bytes
    if let Some(idem) = stream.idempotency() {
        let count = idem.len() as u32;
        payload.extend_from_slice(&count.to_le_bytes()); // 4 bytes
        for key in idem.iter() {
            payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
            payload.extend_from_slice(key);
        }
    } else {
        payload.extend_from_slice(&0u32.to_le_bytes()); // 0 idempotency keys
    }

    (TypeMarker::Stream, payload)
}

/// Deserialize a stream from payload.
pub(super) fn deserialize_stream(payload: &[u8]) -> Result<StreamValue, SerializationError> {
    let mut reader = FrameReader::new(payload);

    // Header: last id (ms, seq) then entry count.
    let last_id_ms = reader.read_le_u64()?;
    let last_id_seq = reader.read_le_u64()?;
    let _last_id = StreamId::new(last_id_ms, last_id_seq);
    let num_entries = reader.read_le_u32()? as usize;

    let mut stream = StreamValue::new();
    for _ in 0..num_entries {
        let id = StreamId::new(reader.read_le_u64()?, reader.read_le_u64()?);
        let num_fields = reader.read_le_u32()? as usize;

        let mut fields = Vec::with_capacity(safe_capacity(num_fields, 8, reader.remaining()));
        for _ in 0..num_fields {
            let field = reader.read_bytes_u32()?;
            let value = reader.read_bytes_u32()?;
            fields.push((field, value));
        }

        // Add entry to stream with explicit ID.
        let _ = stream.add(StreamIdSpec::Explicit(id), fields);
    }

    // Event sourcing extension: read total_appended + idempotency keys if present.
    // Backward-compatible: the old format ends after the entries, so only read the
    // trailing fields when enough bytes remain.
    if reader.remaining() >= 8 {
        stream.set_total_appended(reader.read_le_u64()?);

        if reader.remaining() >= 4 {
            let num_idem_keys = reader.read_le_u32()? as usize;
            if num_idem_keys > 0 {
                let mut idem = IdempotencyState::new();
                for _ in 0..num_idem_keys {
                    // A truncated idempotency tail is tolerated: stop early rather
                    // than reject the whole stream (matches the pre-cursor logic).
                    let Ok(key) = reader.read_bytes_u32() else {
                        break;
                    };
                    idem.record(key);
                }
                stream.set_idempotency(idem);
            }
        }
    } else {
        // Old format: default total_appended to number of entries.
        stream.set_total_appended(num_entries as u64);
    }

    Ok(stream)
}
