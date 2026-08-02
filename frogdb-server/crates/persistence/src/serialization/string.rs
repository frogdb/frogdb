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

#[cfg(test)]
mod tests {
    use super::*;

    /// The marker split is on how the value is *stored*, not on whether the
    /// bytes happen to parse as a number. `StringInt` decodes to the canonical
    /// rendering of the integer, so taking that marker for raw bytes like `00`
    /// or `+5` would silently rewrite the user's value on the next load.
    #[test]
    fn only_integer_stored_values_take_the_integer_marker() {
        let (marker, payload) = serialize_string(&StringValue::from_integer(-42));
        assert_eq!(marker, TypeMarker::StringInt);
        assert_eq!(payload, (-42i64).to_le_bytes().to_vec());
        assert_eq!(
            deserialize_string_int(&payload).unwrap().as_bytes(),
            Bytes::from_static(b"-42")
        );

        for raw in ["00", "+5", "-0", " 7", "7 ", "hello", ""] {
            let (marker, payload) = serialize_string(&StringValue::new(raw.as_bytes().to_vec()));
            assert_eq!(
                marker,
                TypeMarker::StringRaw,
                "{raw:?} is stored as raw bytes, so it stays raw"
            );
            assert_eq!(payload, raw.as_bytes());
            assert_eq!(
                deserialize_string_raw(&payload).as_bytes(),
                Bytes::copy_from_slice(raw.as_bytes()),
                "{raw:?} must come back byte for byte"
            );
        }
    }

    /// An integer payload is exactly 8 bytes. Anything else is a corrupt
    /// payload, not something to decode from whatever bytes are there.
    #[test]
    fn an_integer_payload_of_the_wrong_length_is_refused() {
        for len in [0usize, 4, 7, 9, 16] {
            let payload = vec![0u8; len];
            let err = deserialize_string_int(&payload)
                .expect_err("only 8 bytes decode as an integer string");
            assert!(
                matches!(err, SerializationError::InvalidPayload(ref m) if m.contains(&len.to_string())),
                "len {len}: unexpected error {err:?}"
            );
        }
        assert!(deserialize_string_int(&[0u8; 8]).is_ok());
    }
}
