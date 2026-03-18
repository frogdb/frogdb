use bytes::Bytes;

use frogdb_types::types::{HashValue, ListValue, ListpackThresholds, SetValue, SortedSetValue};

use super::*;

/// Serialize a sorted set.
pub(super) fn serialize_sorted_set(zset: &SortedSetValue) -> (u8, Vec<u8>) {
    let entries = zset.to_vec();
    let len = entries.len() as u32;

    // Calculate size: 4 (len) + sum of (8 (score) + 4 (member_len) + member_bytes)
    let payload_size: usize = 4 + entries.iter().map(|(m, _)| 8 + 4 + m.len()).sum::<usize>();

    let mut payload = Vec::with_capacity(payload_size);

    // Number of entries
    payload.extend_from_slice(&len.to_le_bytes());

    // Each entry: score (f64) + member_len (u32) + member_bytes
    for (member, score) in entries {
        payload.extend_from_slice(&score.to_le_bytes());
        payload.extend_from_slice(&(member.len() as u32).to_le_bytes());
        payload.extend_from_slice(&member);
    }

    (TYPE_SORTED_SET, payload)
}

/// Serialize a hash.
pub(super) fn serialize_hash(hash: &HashValue) -> (u8, Vec<u8>) {
    let entries = hash.to_vec();
    let len = entries.len() as u32;

    // Calculate size: 4 (len) + sum of (4 (field_len) + field + 4 (value_len) + value)
    let payload_size: usize = 4 + entries
        .iter()
        .map(|(f, v)| 4 + f.len() + 4 + v.len())
        .sum::<usize>();

    let mut payload = Vec::with_capacity(payload_size);

    // Number of entries
    payload.extend_from_slice(&len.to_le_bytes());

    // Each entry: field_len (u32) + field + value_len (u32) + value
    for (field, value) in entries {
        payload.extend_from_slice(&(field.len() as u32).to_le_bytes());
        payload.extend_from_slice(&field);
        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
        payload.extend_from_slice(&value);
    }

    (TYPE_HASH, payload)
}

/// Serialize a hash with per-field expiry data.
///
/// Format: [len:u32] per field: [field_len:u32][field][val_len:u32][val][has_expiry:u8][expiry_unix_ms:i64 if has_expiry=1]
pub(super) fn serialize_hash_with_field_expiry(hash: &HashValue) -> (u8, Vec<u8>) {
    let entries = hash.to_vec_with_expiries();
    let len = entries.len() as u32;

    // Calculate size
    let payload_size: usize = 4 + entries
        .iter()
        .map(|(f, v, exp)| 4 + f.len() + 4 + v.len() + 1 + if exp.is_some() { 8 } else { 0 })
        .sum::<usize>();

    let mut payload = Vec::with_capacity(payload_size);

    // Number of entries
    payload.extend_from_slice(&len.to_le_bytes());

    // Each entry
    for (field, value, expiry) in entries {
        payload.extend_from_slice(&(field.len() as u32).to_le_bytes());
        payload.extend_from_slice(&field);
        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
        payload.extend_from_slice(&value);
        match expiry {
            Some(expires_at) => {
                payload.push(1);
                payload.extend_from_slice(&instant_to_unix_ms(expires_at).to_le_bytes());
            }
            None => {
                payload.push(0);
            }
        }
    }

    (TYPE_HASH_WITH_FIELD_EXPIRY, payload)
}

/// Serialize a list.
pub(super) fn serialize_list(list: &ListValue) -> (u8, Vec<u8>) {
    let entries = list.to_vec();
    let len = entries.len() as u32;

    // Calculate size: 4 (len) + sum of (4 (elem_len) + elem)
    let payload_size: usize = 4 + entries.iter().map(|e| 4 + e.len()).sum::<usize>();

    let mut payload = Vec::with_capacity(payload_size);

    // Number of entries
    payload.extend_from_slice(&len.to_le_bytes());

    // Each entry: elem_len (u32) + elem
    for elem in entries {
        payload.extend_from_slice(&(elem.len() as u32).to_le_bytes());
        payload.extend_from_slice(&elem);
    }

    (TYPE_LIST, payload)
}

/// Serialize a set.
pub(super) fn serialize_set(set: &SetValue) -> (u8, Vec<u8>) {
    let entries = set.to_vec();
    let len = entries.len() as u32;

    // Calculate size: 4 (len) + sum of (4 (member_len) + member)
    let payload_size: usize = 4 + entries.iter().map(|m| 4 + m.len()).sum::<usize>();

    let mut payload = Vec::with_capacity(payload_size);

    // Number of entries
    payload.extend_from_slice(&len.to_le_bytes());

    // Each entry: member_len (u32) + member
    for member in entries {
        payload.extend_from_slice(&(member.len() as u32).to_le_bytes());
        payload.extend_from_slice(&member);
    }

    (TYPE_SET, payload)
}

/// Deserialize a sorted set from payload.
pub(super) fn deserialize_sorted_set(payload: &[u8]) -> Result<SortedSetValue, SerializationError> {
    if payload.len() < 4 {
        return Err(SerializationError::InvalidPayload(
            "Sorted set payload too short for length".to_string(),
        ));
    }

    let len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut zset = SortedSetValue::new();

    for _ in 0..len {
        // Read score (8 bytes)
        if 8 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Sorted set payload truncated at score".to_string(),
            ));
        }
        let score = f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read member length (4 bytes)
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Sorted set payload truncated at member length".to_string(),
            ));
        }
        let member_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read member bytes
        if member_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Sorted set payload truncated at member data".to_string(),
            ));
        }
        let member = Bytes::copy_from_slice(&payload[offset..offset + member_len]);
        offset += member_len;

        zset.add(member, score);
    }

    Ok(zset)
}

/// Deserialize a hash from payload.
pub(super) fn deserialize_hash(payload: &[u8]) -> Result<HashValue, SerializationError> {
    if payload.len() < 4 {
        return Err(SerializationError::InvalidPayload(
            "Hash payload too short for length".to_string(),
        ));
    }

    let len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut entries = Vec::with_capacity(len);

    for _ in 0..len {
        // Read field length (4 bytes)
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at field length".to_string(),
            ));
        }
        let field_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read field bytes
        if field_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at field data".to_string(),
            ));
        }
        let field = Bytes::copy_from_slice(&payload[offset..offset + field_len]);
        offset += field_len;

        // Read value length (4 bytes)
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at value length".to_string(),
            ));
        }
        let value_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read value bytes
        if value_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash payload truncated at value data".to_string(),
            ));
        }
        let value = Bytes::copy_from_slice(&payload[offset..offset + value_len]);
        offset += value_len;

        entries.push((field, value));
    }

    Ok(HashValue::from_entries(
        entries,
        ListpackThresholds::DEFAULT_HASH,
    ))
}

/// Deserialize a hash with per-field expiry data.
pub(super) fn deserialize_hash_with_field_expiry(
    payload: &[u8],
) -> Result<HashValue, SerializationError> {
    if payload.len() < 4 {
        return Err(SerializationError::InvalidPayload(
            "Hash with field expiry payload too short for length".to_string(),
        ));
    }

    let len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut entries = Vec::with_capacity(len);

    for _ in 0..len {
        // Read field length
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash field expiry payload truncated at field length".to_string(),
            ));
        }
        let field_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if field_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash field expiry payload truncated at field data".to_string(),
            ));
        }
        let field = Bytes::copy_from_slice(&payload[offset..offset + field_len]);
        offset += field_len;

        // Read value length
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash field expiry payload truncated at value length".to_string(),
            ));
        }
        let value_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if value_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Hash field expiry payload truncated at value data".to_string(),
            ));
        }
        let value = Bytes::copy_from_slice(&payload[offset..offset + value_len]);
        offset += value_len;

        // Read expiry flag
        if offset >= payload.len() {
            return Err(SerializationError::InvalidPayload(
                "Hash field expiry payload truncated at expiry flag".to_string(),
            ));
        }
        let has_expiry = payload[offset];
        offset += 1;

        let expiry = if has_expiry == 1 {
            if 8 > payload.len() - offset {
                return Err(SerializationError::InvalidPayload(
                    "Hash field expiry payload truncated at expiry timestamp".to_string(),
                ));
            }
            let expiry_ms = i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
            offset += 8;
            Some(unix_ms_to_instant(expiry_ms))
        } else {
            None
        };

        entries.push((field, value, expiry));
    }

    Ok(HashValue::from_entries_with_expiries(
        entries,
        ListpackThresholds::DEFAULT_HASH,
    ))
}

/// Deserialize a list from payload.
pub(super) fn deserialize_list(payload: &[u8]) -> Result<ListValue, SerializationError> {
    if payload.len() < 4 {
        return Err(SerializationError::InvalidPayload(
            "List payload too short for length".to_string(),
        ));
    }

    let len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut list = ListValue::new();

    for _ in 0..len {
        // Read element length (4 bytes)
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "List payload truncated at element length".to_string(),
            ));
        }
        let elem_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read element bytes
        if elem_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "List payload truncated at element data".to_string(),
            ));
        }
        let elem = Bytes::copy_from_slice(&payload[offset..offset + elem_len]);
        offset += elem_len;

        list.push_back(elem);
    }

    Ok(list)
}

/// Deserialize a set from payload.
pub(super) fn deserialize_set(payload: &[u8]) -> Result<SetValue, SerializationError> {
    if payload.len() < 4 {
        return Err(SerializationError::InvalidPayload(
            "Set payload too short for length".to_string(),
        ));
    }

    let len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut members = Vec::with_capacity(len);

    for _ in 0..len {
        // Read member length (4 bytes)
        if 4 > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Set payload truncated at member length".to_string(),
            ));
        }
        let member_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read member bytes
        if member_len > payload.len() - offset {
            return Err(SerializationError::InvalidPayload(
                "Set payload truncated at member data".to_string(),
            ));
        }
        let member = Bytes::copy_from_slice(&payload[offset..offset + member_len]);
        offset += member_len;

        members.push(member);
    }

    Ok(SetValue::from_members(
        members,
        ListpackThresholds::DEFAULT_SET,
    ))
}
