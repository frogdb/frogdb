use super::*;

/// Serialize a string value.
pub(super) fn serialize_string(sv: &StringValue) -> (u8, Vec<u8>) {
    // Check if it's integer-encoded by trying to parse as integer
    if let Some(i) = sv.as_integer() {
        // Verify it's actually stored as integer (not a string that happens to parse as int)
        let bytes = sv.as_bytes();
        if let Ok(s) = std::str::from_utf8(&bytes)
            && let Ok(parsed) = s.parse::<i64>()
            && parsed == i
        {
            // It's integer-encoded
            return (TYPE_STRING_INT, i.to_le_bytes().to_vec());
        }
    }

    // Raw bytes
    (TYPE_STRING_RAW, sv.as_bytes().to_vec())
}
