use frogdb_types::json::JsonValue;
use frogdb_types::vectorset::VectorSetValue;

use super::*;

/// Serialize a JSON value.
pub(super) fn serialize_json(json: &JsonValue) -> (TypeMarker, Vec<u8>) {
    let payload = json.to_bytes();
    (TypeMarker::Json, payload)
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
pub(super) fn serialize_vectorset(vs: &VectorSetValue) -> (TypeMarker, Vec<u8>) {
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

    (TypeMarker::VectorSet, payload)
}

/// Deserialize a vector set value.
pub(super) fn deserialize_vectorset(payload: &[u8]) -> Result<VectorSetValue, SerializationError> {
    use frogdb_types::vectorset::{VectorDistanceMetric, VectorQuantization};

    let mut reader = FrameReader::new(payload);

    let metric = match reader.read_u8()? {
        0 => VectorDistanceMetric::Cosine,
        1 => VectorDistanceMetric::L2,
        2 => VectorDistanceMetric::InnerProduct,
        v => {
            return Err(SerializationError::InvalidPayload(format!(
                "Unknown metric: {v}"
            )));
        }
    };
    let quant = match reader.read_u8()? {
        0 => VectorQuantization::NoQuant,
        1 => VectorQuantization::Q8,
        2 => VectorQuantization::Bin,
        v => {
            return Err(SerializationError::InvalidPayload(format!(
                "Unknown quantization: {v}"
            )));
        }
    };
    let dim = reader.read_le_u32()? as usize;
    let original_dim = reader.read_le_u32()? as usize;
    let m = reader.read_le_u32()? as usize;
    let ef = reader.read_le_u32()? as usize;

    // Reject unreasonable parameters that would cause OOM in the HNSW index. These
    // bounds are shared with VectorSetValue creation (`VectorSetValue::MAX_*`) so
    // encode and decode agree on one definition of "valid" — anything that can be
    // created can always be reloaded. Checked up front, before allocating, to also
    // guard against malicious/corrupt payloads.
    if dim > VectorSetValue::MAX_DIM || original_dim > VectorSetValue::MAX_DIM {
        return Err(SerializationError::InvalidPayload(format!(
            "VectorSet dimension too large: dim={dim}, original_dim={original_dim}, max={}",
            VectorSetValue::MAX_DIM
        )));
    }
    if m > VectorSetValue::MAX_CONNECTIVITY {
        return Err(SerializationError::InvalidPayload(format!(
            "VectorSet connectivity too large: m={m}, max={}",
            VectorSetValue::MAX_CONNECTIVITY
        )));
    }
    if ef > VectorSetValue::MAX_EF {
        return Err(SerializationError::InvalidPayload(format!(
            "VectorSet ef_construction too large: ef={ef}, max={}",
            VectorSetValue::MAX_EF
        )));
    }
    let next_id = reader.read_le_u64()?;
    let uid = reader.read_le_u64()?;

    // Projection matrix
    let proj_len = reader.read_le_u32()? as usize;
    let mut projection_matrix = Vec::with_capacity(safe_capacity(proj_len, 4, reader.remaining()));
    for _ in 0..proj_len {
        projection_matrix.push(reader.read_le_f32()?);
    }

    // Elements
    let elem_count = reader.read_le_u32()? as usize;
    let mut elements = Vec::with_capacity(safe_capacity(elem_count, 13, reader.remaining()));
    for _ in 0..elem_count {
        let name = reader.read_bytes_u32()?;
        let id = reader.read_le_u64()?;

        let vec_len = reader.read_le_u32()? as usize;
        let mut vector = Vec::with_capacity(safe_capacity(vec_len, 4, reader.remaining()));
        for _ in 0..vec_len {
            vector.push(reader.read_le_f32()?);
        }

        let has_attr = reader.read_u8()? != 0;
        let attr = if has_attr {
            let attr_str = std::str::from_utf8(reader.read_u32_len_prefixed()?).map_err(|_| {
                SerializationError::InvalidPayload("Invalid UTF-8 in attribute".to_string())
            })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use frogdb_types::vectorset::{VectorDistanceMetric, VectorQuantization};

    /// A vector set created at the maximum M/EF round-trips: encode then decode
    /// succeeds. This pins the boundary of the encode<->decode bound agreement.
    #[test]
    fn boundary_m_ef_round_trips() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            4,
            VectorSetValue::MAX_CONNECTIVITY,
            VectorSetValue::MAX_EF,
        )
        .expect("boundary M/EF must be creatable");
        vs.add(Bytes::from_static(b"e1"), vec![1.0, 0.0, 0.0, 0.0])
            .unwrap();

        let (marker, payload) = serialize_vectorset(&vs);
        assert_eq!(marker, TypeMarker::VectorSet);
        let back = deserialize_vectorset(&payload).expect("boundary value must reload");
        assert_eq!(back.m(), VectorSetValue::MAX_CONNECTIVITY);
        assert_eq!(back.ef_construction(), VectorSetValue::MAX_EF);
        assert_eq!(back.card(), 1);
    }

    /// Oversized parameters that decode rejects can no longer be created, so the
    /// "serializes fine, fails to reload" asymmetry is gone. (Before the fix,
    /// creation succeeded and only decode rejected — silent key loss on reload.)
    #[test]
    fn oversized_parameters_cannot_be_created() {
        assert!(
            VectorSetValue::new(
                VectorDistanceMetric::Cosine,
                VectorQuantization::NoQuant,
                4,
                VectorSetValue::MAX_CONNECTIVITY + 1,
                200,
            )
            .is_err(),
            "oversized M must be rejected at creation"
        );
    }
}

#[cfg(test)]
mod vectorset_bound_tests {
    use super::*;

    /// A vector-set header with no projection matrix and no elements. The four
    /// HNSW parameters are the only variables — everything after them is the
    /// minimum a well-formed payload needs.
    fn vs_header(dim: u32, original_dim: u32, m: u32, ef: u32) -> Vec<u8> {
        let mut p = Vec::new();
        p.push(0); // metric: Cosine
        p.push(0); // quantization: NoQuant
        for v in [dim, original_dim, m, ef] {
            p.extend_from_slice(&v.to_le_bytes());
        }
        p.extend_from_slice(&0u64.to_le_bytes()); // next_id
        p.extend_from_slice(&0u64.to_le_bytes()); // uid
        p.extend_from_slice(&0u32.to_le_bytes()); // projection matrix length
        p.extend_from_slice(&0u32.to_le_bytes()); // element count
        p
    }

    fn refusal(payload: &[u8]) -> String {
        match deserialize_vectorset(payload) {
            Err(SerializationError::InvalidPayload(msg)) => msg,
            other => panic!("expected an InvalidPayload refusal, got {other:?}"),
        }
    }

    /// Either dimension field alone is enough to refuse the payload, and the
    /// refusal happens in the *decoder's* guard — before an HNSW index is
    /// allocated for a dimension a corrupt or hostile payload invented. Reaching
    /// `from_parts` instead would allocate first and report a different error.
    #[test]
    fn an_oversized_dimension_is_refused_by_the_pre_allocation_guard() {
        let too_big = (VectorSetValue::MAX_DIM + 1) as u32;

        let msg = refusal(&vs_header(too_big, 4, 16, 200));
        assert!(
            msg.contains("dimension too large") && msg.contains(&format!("dim={too_big}")),
            "the guard must name the offending dimension, got: {msg}"
        );

        let msg = refusal(&vs_header(4, too_big, 16, 200));
        assert!(
            msg.contains("dimension too large") && msg.contains(&format!("original_dim={too_big}")),
            "a REDUCE'd set's original dimension is bounded too, got: {msg}"
        );
    }

    /// The bound is inclusive: a set at exactly `MAX_DIM` is creatable, so it
    /// must also reload. An off-by-one here makes the largest legal vector set
    /// unloadable — silent key loss on the next snapshot restore.
    #[test]
    fn the_largest_legal_dimension_still_loads() {
        let max = VectorSetValue::MAX_DIM as u32;
        let vs = deserialize_vectorset(&vs_header(max, max, 16, 200))
            .expect("a set at the maximum dimension must reload");
        assert_eq!(vs.dim(), VectorSetValue::MAX_DIM);
        assert_eq!(vs.original_dim(), VectorSetValue::MAX_DIM);
        assert_eq!(vs.card(), 0);
    }
}
