use bytes::Bytes;

use frogdb_types::json::JsonValue;
use frogdb_types::vectorset::VectorSetValue;

use super::*;

/// Serialize a JSON value.
pub(super) fn serialize_json(json: &JsonValue) -> (u8, Vec<u8>) {
    let payload = json.to_bytes();
    (TYPE_JSON, payload)
}

/// Deserialize a JSON value.
pub(super) fn deserialize_json(payload: &[u8]) -> Result<JsonValue, SerializationError> {
    JsonValue::parse(payload).map_err(|e| SerializationError::InvalidPayload(e.to_string()))
}

/// Serialize a vector set value.
///
/// Format:
/// - metric (1 byte)
/// - quantization (1 byte)
/// - dim (4 bytes u32)
/// - original_dim (4 bytes u32)
/// - m (4 bytes u32)
/// - ef_construction (4 bytes u32)
/// - next_id (8 bytes u64)
/// - uid (8 bytes u64)
/// - projection_matrix_len (4 bytes u32)
/// - projection_matrix: len * f32
/// - element_count (4 bytes u32)
/// - elements: name_len(4) + name + id(8) + vec_len(4) + vec(len*4) + has_attr(1) + [attr_len(4) + attr]
pub(super) fn serialize_vectorset(vs: &VectorSetValue) -> (u8, Vec<u8>) {
    let mut payload = Vec::new();

    payload.push(vs.metric() as u8);
    payload.push(vs.quantization() as u8);
    payload.extend_from_slice(&(vs.dim() as u32).to_le_bytes());
    payload.extend_from_slice(&(vs.original_dim() as u32).to_le_bytes());
    payload.extend_from_slice(&(vs.m() as u32).to_le_bytes());
    payload.extend_from_slice(&(vs.ef_construction() as u32).to_le_bytes());
    payload.extend_from_slice(&vs.next_id().to_le_bytes());
    payload.extend_from_slice(&vs.uid().to_le_bytes());

    let proj = vs.projection_matrix();
    payload.extend_from_slice(&(proj.len() as u32).to_le_bytes());
    for &v in proj {
        payload.extend_from_slice(&v.to_le_bytes());
    }

    let count = vs.card();
    payload.extend_from_slice(&(count as u32).to_le_bytes());
    for (name, id, vector, attr) in vs.elements() {
        payload.extend_from_slice(&(name.len() as u32).to_le_bytes());
        payload.extend_from_slice(name);
        payload.extend_from_slice(&id.to_le_bytes());
        payload.extend_from_slice(&(vector.len() as u32).to_le_bytes());
        for &v in vector {
            payload.extend_from_slice(&v.to_le_bytes());
        }
        let has_attr = attr.is_some();
        payload.push(has_attr as u8);
        if let Some(a) = attr {
            let attr_str = a.to_string();
            payload.extend_from_slice(&(attr_str.len() as u32).to_le_bytes());
            payload.extend_from_slice(attr_str.as_bytes());
        }
    }

    (TYPE_VECTORSET, payload)
}

/// Deserialize a vector set value.
pub(super) fn deserialize_vectorset(payload: &[u8]) -> Result<VectorSetValue, SerializationError> {
    use frogdb_types::vectorset::{VectorDistanceMetric, VectorQuantization};

    if payload.len() < 34 {
        return Err(SerializationError::InvalidPayload(
            "VectorSet payload too short".to_string(),
        ));
    }

    let mut pos = 0;
    let metric = match payload[pos] {
        0 => VectorDistanceMetric::Cosine,
        1 => VectorDistanceMetric::L2,
        2 => VectorDistanceMetric::InnerProduct,
        v => {
            return Err(SerializationError::InvalidPayload(format!(
                "Unknown metric: {v}"
            )));
        }
    };
    pos += 1;
    let quant = match payload[pos] {
        0 => VectorQuantization::NoQuant,
        1 => VectorQuantization::Q8,
        2 => VectorQuantization::Bin,
        v => {
            return Err(SerializationError::InvalidPayload(format!(
                "Unknown quantization: {v}"
            )));
        }
    };
    pos += 1;
    let dim = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let original_dim = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let m = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let ef = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let next_id = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
    pos += 8;
    let uid = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
    pos += 8;

    // Projection matrix
    if pos + 4 > payload.len() {
        return Err(SerializationError::Truncated {
            expected: pos + 4,
            actual: payload.len(),
        });
    }
    let proj_len = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if pos + proj_len * 4 > payload.len() {
        return Err(SerializationError::Truncated {
            expected: pos + proj_len * 4,
            actual: payload.len(),
        });
    }
    let mut projection_matrix = Vec::with_capacity(proj_len);
    for _ in 0..proj_len {
        let v = f32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
        pos += 4;
        projection_matrix.push(v);
    }

    // Elements
    if pos + 4 > payload.len() {
        return Err(SerializationError::Truncated {
            expected: pos + 4,
            actual: payload.len(),
        });
    }
    let elem_count = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    let mut elements = Vec::with_capacity(elem_count);
    for _ in 0..elem_count {
        if pos + 4 > payload.len() {
            return Err(SerializationError::Truncated {
                expected: pos + 4,
                actual: payload.len(),
            });
        }
        let name_len = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + name_len > payload.len() {
            return Err(SerializationError::Truncated {
                expected: pos + name_len,
                actual: payload.len(),
            });
        }
        let name = Bytes::copy_from_slice(&payload[pos..pos + name_len]);
        pos += name_len;

        if pos + 8 > payload.len() {
            return Err(SerializationError::Truncated {
                expected: pos + 8,
                actual: payload.len(),
            });
        }
        let id = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;

        if pos + 4 > payload.len() {
            return Err(SerializationError::Truncated {
                expected: pos + 4,
                actual: payload.len(),
            });
        }
        let vec_len = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + vec_len * 4 > payload.len() {
            return Err(SerializationError::Truncated {
                expected: pos + vec_len * 4,
                actual: payload.len(),
            });
        }
        let mut vector = Vec::with_capacity(vec_len);
        for _ in 0..vec_len {
            let v = f32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
            pos += 4;
            vector.push(v);
        }

        if pos + 1 > payload.len() {
            return Err(SerializationError::Truncated {
                expected: pos + 1,
                actual: payload.len(),
            });
        }
        let has_attr = payload[pos] != 0;
        pos += 1;
        let attr = if has_attr {
            if pos + 4 > payload.len() {
                return Err(SerializationError::Truncated {
                    expected: pos + 4,
                    actual: payload.len(),
                });
            }
            let attr_len = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + attr_len > payload.len() {
                return Err(SerializationError::Truncated {
                    expected: pos + attr_len,
                    actual: payload.len(),
                });
            }
            let attr_str = std::str::from_utf8(&payload[pos..pos + attr_len]).map_err(|_| {
                SerializationError::InvalidPayload("Invalid UTF-8 in attribute".to_string())
            })?;
            pos += attr_len;
            Some(serde_json::from_str(attr_str).map_err(|_| {
                SerializationError::InvalidPayload("Invalid JSON in attribute".to_string())
            })?)
        } else {
            None
        };

        elements.push((name, id, vector, attr));
    }

    VectorSetValue::from_parts(
        metric,
        quant,
        dim,
        original_dim,
        m,
        ef,
        next_id,
        uid,
        projection_matrix,
        elements,
    )
    .map_err(|e| SerializationError::InvalidPayload(format!("Failed to create VectorSet: {e}")))
}
