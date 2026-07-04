use frogdb_types::types::{HashValue, ListValue, ListpackThresholds, SetValue, SortedSetValue};

use super::*;

/// Serialize a sorted set.
pub(super) fn serialize_sorted_set(zset: &SortedSetValue) -> (TypeMarker, Vec<u8>) {
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

    (TypeMarker::SortedSet, payload)
}

/// Serialize a hash.
pub(super) fn serialize_hash(hash: &HashValue) -> (TypeMarker, Vec<u8>) {
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

    (TypeMarker::Hash, payload)
}

/// Serialize a hash with per-field expiry data.
///
/// Format: [len:u32] per field: [field_len:u32][field][val_len:u32][val][has_expiry:u8][expiry_unix_ms:i64 if has_expiry=1]
pub(super) fn serialize_hash_with_field_expiry(hash: &HashValue) -> (TypeMarker, Vec<u8>) {
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

    (TypeMarker::HashWithFieldExpiry, payload)
}

/// Serialize a list.
pub(super) fn serialize_list(list: &ListValue) -> (TypeMarker, Vec<u8>) {
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

    (TypeMarker::List, payload)
}

/// Serialize a set.
pub(super) fn serialize_set(set: &SetValue) -> (TypeMarker, Vec<u8>) {
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

    (TypeMarker::Set, payload)
}

/// Deserialize a sorted set from payload.
pub(super) fn deserialize_sorted_set(payload: &[u8]) -> Result<SortedSetValue, SerializationError> {
    let mut reader = FrameReader::new(payload);
    let len = reader.read_le_u32()? as usize;
    let mut zset = SortedSetValue::new();

    for _ in 0..len {
        // Each entry: score (f64) + member (u32-length-prefixed).
        let score = reader.read_le_f64()?;
        let member = reader.read_bytes_u32()?;
        zset.add(member, score);
    }

    Ok(zset)
}

/// Deserialize a hash from payload.
pub(super) fn deserialize_hash(payload: &[u8]) -> Result<HashValue, SerializationError> {
    let mut reader = FrameReader::new(payload);
    let len = reader.read_le_u32()? as usize;
    let mut entries = Vec::with_capacity(safe_capacity(len, 8, reader.remaining()));

    for _ in 0..len {
        // Each entry: field then value, both u32-length-prefixed.
        let field = reader.read_bytes_u32()?;
        let value = reader.read_bytes_u32()?;
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
    let mut reader = FrameReader::new(payload);
    let len = reader.read_le_u32()? as usize;
    let mut entries = Vec::with_capacity(safe_capacity(len, 9, reader.remaining()));

    for _ in 0..len {
        // Each entry: field, value, expiry flag, then the timestamp iff flag == 1.
        let field = reader.read_bytes_u32()?;
        let value = reader.read_bytes_u32()?;
        let has_expiry = reader.read_u8()?;
        let expiry = if has_expiry == 1 {
            Some(unix_ms_to_instant(reader.read_le_i64()?))
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
    let mut reader = FrameReader::new(payload);
    let len = reader.read_le_u32()? as usize;
    let mut list = ListValue::new();

    for _ in 0..len {
        list.push_back(reader.read_bytes_u32()?);
    }

    Ok(list)
}

/// Deserialize a set from payload.
pub(super) fn deserialize_set(payload: &[u8]) -> Result<SetValue, SerializationError> {
    let mut reader = FrameReader::new(payload);
    let len = reader.read_le_u32()? as usize;
    let mut members = Vec::with_capacity(safe_capacity(len, 4, reader.remaining()));

    for _ in 0..len {
        members.push(reader.read_bytes_u32()?);
    }

    Ok(SetValue::from_members(
        members,
        ListpackThresholds::DEFAULT_SET,
    ))
}
