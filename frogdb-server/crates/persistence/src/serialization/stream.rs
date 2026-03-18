use bytes::Bytes;

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
pub(super) fn serialize_stream(stream: &StreamValue) -> (u8, Vec<u8>) {
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

    (TYPE_STREAM, payload)
}

/// Deserialize a stream from payload.
pub(super) fn deserialize_stream(payload: &[u8]) -> Result<StreamValue, SerializationError> {
    if payload.len() < 20 {
        return Err(SerializationError::InvalidPayload(
            "Stream payload too short for header".to_string(),
        ));
    }

    // Read last ID
    let last_id_ms = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let last_id_seq = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    let _last_id = StreamId::new(last_id_ms, last_id_seq);

    // Read number of entries
    let num_entries = u32::from_le_bytes(payload[16..20].try_into().unwrap()) as usize;
    let mut offset = 20;
    let mut stream = StreamValue::new();

    for _ in 0..num_entries {
        // Read entry ID
        if 20 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Stream payload truncated at entry header".to_string(),
            ));
        }
        let id_ms = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let id_seq = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let id = StreamId::new(id_ms, id_seq);

        // Read number of fields
        let num_fields =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut fields = Vec::with_capacity(num_fields);
        for _ in 0..num_fields {
            // Read field length
            if 4 > payload.len() - offset {
                return Err(SerializationError::InvalidPayload(
                    "Stream payload truncated at field length".to_string(),
                ));
            }
            let field_len =
                u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            // Read field bytes
            if field_len > payload.len() - offset {
                return Err(SerializationError::InvalidPayload(
                    "Stream payload truncated at field data".to_string(),
                ));
            }
            let field = Bytes::copy_from_slice(&payload[offset..offset + field_len]);
            offset += field_len;

            // Read value length
            if 4 > payload.len() - offset {
                return Err(SerializationError::InvalidPayload(
                    "Stream payload truncated at value length".to_string(),
                ));
            }
            let value_len =
                u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            // Read value bytes
            if value_len > payload.len() - offset {
                return Err(SerializationError::InvalidPayload(
                    "Stream payload truncated at value data".to_string(),
                ));
            }
            let value = Bytes::copy_from_slice(&payload[offset..offset + value_len]);
            offset += value_len;

            fields.push((field, value));
        }

        // Add entry to stream with explicit ID
        let _ = stream.add(StreamIdSpec::Explicit(id), fields);
    }

    // Event sourcing extension: read total_appended + idempotency keys if present
    // (backward-compatible: old format ends here, so check remaining bytes)
    if offset + 8 <= payload.len() {
        let total_appended = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        stream.set_total_appended(total_appended);

        if offset + 4 <= payload.len() {
            let num_idem_keys =
                u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            if num_idem_keys > 0 {
                let mut idem = IdempotencyState::new();
                for _ in 0..num_idem_keys {
                    if offset + 4 > payload.len() {
                        break;
                    }
                    let key_len =
                        u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap())
                            as usize;
                    offset += 4;
                    if offset + key_len > payload.len() {
                        break;
                    }
                    let key = Bytes::copy_from_slice(&payload[offset..offset + key_len]);
                    offset += key_len;
                    idem.record(key);
                }
                stream.set_idempotency(idem);
            }
        }
    } else {
        // Old format: default total_appended to number of entries
        stream.set_total_appended(num_entries as u64);
    }

    let _ = offset; // suppress unused warning

    Ok(stream)
}
