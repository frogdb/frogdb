use bytes::Bytes;

use frogdb_types::types::StringValue;

use super::*;

/// Serialize a string value.
pub(super) fn serialize_string(sv: &StringValue) -> (TypeMarker, Vec<u8>) {
    // Only values actually stored integer-encoded take the StringInt marker.
    // Raw bytes that merely parse as an integer ("00", "+5", "-0") must stay
    // raw: StringInt decodes to the canonical rendering, which would drop the
    // original bytes.
    if let Some(i) = sv.stored_integer() {
        return (TypeMarker::StringInt, i.to_le_bytes().to_vec());
    }

    // Raw bytes
    (TypeMarker::StringRaw, sv.as_bytes().to_vec())
}

/// Deserialize a raw-bytes string.
pub(super) fn deserialize_string_raw(payload: &[u8]) -> StringValue {
    StringValue::new(Bytes::copy_from_slice(payload))
}

/// Deserialize an integer-encoded string (8-byte little-endian i64).
pub(super) fn deserialize_string_int(payload: &[u8]) -> Result<StringValue, SerializationError> {
    if payload.len() != 8 {
        return Err(SerializationError::InvalidPayload(format!(
            "Integer string expected 8 bytes, got {}",
            payload.len()
        )));
    }
    let i = i64::from_le_bytes(payload.try_into().unwrap());
    Ok(StringValue::from_integer(i))
}
