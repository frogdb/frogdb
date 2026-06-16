//! Vector-field state manager.
//!
//! [`VectorFieldManager`] is the single owner of all vector-field state for a
//! [`ShardSearchIndex`](crate::index::ShardSearchIndex). Each vector field is a
//! [`usearch`] HNSW index plus the bookkeeping that maps usearch's `u64` ids to
//! Redis keys and back. The load-bearing invariant is a **bijection** between
//! live usearch ids and Redis keys, per field:
//!
//! ```text
//! reverse_map[key] == id   <=>   key_map[id] == key
//! ```
//!
//! The maps live behind private fields so the bijection can only be mutated
//! through this module's high-level operations (index / delete / lookup / knn /
//! save / load), each of which keeps the two maps in lockstep.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::error::SearchError;
use crate::schema::{FieldType, SearchIndexDef, VectorDistanceMetric};

/// Owns all vector-field state for one shard search index. The single source of
/// truth for the usearch-id <-> Redis-key bijection.
pub struct VectorFieldManager {
    /// Per-field state, keyed by field name. Only VECTOR fields appear here.
    fields: HashMap<String, VectorField>,
    /// Base directory for sidecar files (None for RAM-only indexes).
    path: Option<PathBuf>,
}

/// All state for ONE vector field. Private fields so the bijection cannot be
/// mutated out from under the invariant.
struct VectorField {
    /// The usearch HNSW index.
    index: usearch::Index,
    /// usearch id -> Redis key.
    key_map: HashMap<u64, String>,
    /// Redis key -> usearch id (for delete/update).
    reverse_map: HashMap<String, u64>,
    /// Next usearch id to allocate.
    next_id: u64,
    /// Vector dimensionality, for blob-length validation.
    dim: usize,
}

impl VectorFieldManager {
    /// Build from an index definition, loading per-field state from disk when a
    /// `path` is given and sidecars exist.
    pub fn new(def: &SearchIndexDef, path: Option<&Path>) -> Result<Self, SearchError> {
        let mut fields = HashMap::new();
        for field_def in &def.fields {
            if let FieldType::Vector {
                dim,
                distance_metric,
            } = &field_def.field_type
            {
                let field = VectorField::open(*dim, *distance_metric, &field_def.name, path)?;
                fields.insert(field_def.name.clone(), field);
            }
        }
        Ok(Self {
            fields,
            path: path.map(Path::to_path_buf),
        })
    }

    /// Index (add or replace) a vector for `field` under `key`. No-op if `field`
    /// is not a vector field.
    pub fn index(&mut self, field: &str, key: &str, blob: &[u8]) -> Result<(), SearchError> {
        match self.fields.get_mut(field) {
            Some(vf) => vf.index(key, blob),
            None => Ok(()),
        }
    }

    /// Remove `key` from every vector field. Idempotent.
    pub fn delete(&mut self, key: &str) {
        for vf in self.fields.values_mut() {
            vf.delete(key);
        }
    }

    /// Resolve a usearch id back to its Redis key.
    pub fn lookup_key(&self, field: &str, id: u64) -> Option<&str> {
        self.fields.get(field)?.key_map.get(&id).map(String::as_str)
    }

    /// Resolve a Redis key to its usearch id.
    pub fn lookup_id(&self, field: &str, key: &str) -> Option<u64> {
        self.fields.get(field)?.reverse_map.get(key).copied()
    }

    /// Run a KNN search and return `(redis_key, distance)` pairs, already mapped.
    /// Owns the usearch-handle + key_map pairing so callers never touch both.
    pub fn knn(
        &self,
        field: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(String, f32)>, SearchError> {
        let vf = self.fields.get(field).ok_or_else(|| {
            SearchError::SchemaError(format!("No vector index for field: {}", field))
        })?;
        vf.knn(query, k)
    }

    /// Persist every field's usearch index + map sidecar. No-op for RAM indexes.
    pub fn save(&self) -> Result<(), SearchError> {
        let Some(base) = &self.path else {
            return Ok(());
        };
        for (name, field) in &self.fields {
            field.save(base, name)?;
        }
        Ok(())
    }

    /// Debug-only invariant check; the bijection holds for every field.
    #[cfg(any(test, debug_assertions))]
    pub fn check_invariant(&self) -> Result<(), String> {
        for (name, field) in &self.fields {
            if !field.invariant_holds() {
                return Err(format!("bijection broken for field {}", name));
            }
        }
        Ok(())
    }
}

impl VectorField {
    /// Create the usearch index for one field, loading sidecars from disk when
    /// present.
    fn open(
        dim: usize,
        distance_metric: VectorDistanceMetric,
        name: &str,
        base_path: Option<&Path>,
    ) -> Result<Self, SearchError> {
        let index = new_usearch_index(dim, distance_metric, name)?;
        let mut key_map: HashMap<u64, String> = HashMap::new();
        let mut reverse_map: HashMap<String, u64> = HashMap::new();
        let mut next_id: u64 = 0;

        if let Some(base) = base_path {
            let vec_path = base.join(format!("__vec_{}.usearch", name));
            let map_path = base.join(format!("__vec_{}_map.json", name));
            if vec_path.exists()
                && let Err(e) = index.load(vec_path.to_str().unwrap_or(""))
            {
                tracing::warn!(error = %e, "Failed to load vector index, starting fresh");
            }
            if map_path.exists()
                && let Ok(data) = std::fs::read(&map_path)
                && let Ok(map_data) = serde_json::from_slice::<serde_json::Value>(&data)
            {
                if let Some(obj) = map_data.get("key_map").and_then(|v| v.as_object()) {
                    for (id_str, key_val) in obj {
                        if let Ok(id) = id_str.parse::<u64>()
                            && let Some(key) = key_val.as_str()
                        {
                            key_map.insert(id, key.to_string());
                            reverse_map.insert(key.to_string(), id);
                        }
                    }
                }
                next_id = map_data
                    .get("next_id")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
            }
        }

        // Reserve initial capacity.
        let _ = index.reserve(1024);

        Ok(Self {
            index,
            key_map,
            reverse_map,
            next_id,
            dim,
        })
    }

    /// Index (add or replace) a vector under `key`.
    fn index(&mut self, key: &str, blob: &[u8]) -> Result<(), SearchError> {
        let expected_len = self.dim * 4; // f32 = 4 bytes
        if blob.len() != expected_len {
            tracing::warn!(
                expected = expected_len,
                got = blob.len(),
                "Vector blob size mismatch"
            );
            return Ok(());
        }

        // Remove old vector if key was previously indexed.
        if let Some(&old_id) = self.reverse_map.get(key) {
            let _ = self.index.remove(old_id);
        }

        // Allocate new id.
        let vec_id = self.next_id;
        self.next_id += 1;

        // Reserve capacity if needed.
        let size = self.index.size();
        let capacity = self.index.capacity();
        if size >= capacity {
            let _ = self.index.reserve(capacity.max(64) * 2);
        }

        let floats = bytes_to_f32(blob);
        let _ = self.index.add(vec_id, &floats);

        self.key_map.insert(vec_id, key.to_string());
        self.reverse_map.insert(key.to_string(), vec_id);
        Ok(())
    }

    /// Remove `key` from this field, if present.
    fn delete(&mut self, key: &str) {
        if let Some(id) = self.reverse_map.remove(key) {
            let _ = self.index.remove(id);
            self.key_map.remove(&id);
        }
    }

    /// KNN search, mapping usearch ids back to Redis keys.
    fn knn(&self, query: &[f32], k: usize) -> Result<Vec<(String, f32)>, SearchError> {
        let results = self
            .index
            .search(query, k)
            .map_err(|e| SearchError::SchemaError(format!("Vector search failed: {}", e)))?;

        let mut hits = Vec::with_capacity(results.keys.len());
        for i in 0..results.keys.len() {
            if let Some(redis_key) = self.key_map.get(&results.keys[i]) {
                hits.push((redis_key.clone(), results.distances[i]));
            }
        }
        Ok(hits)
    }

    /// Persist this field's usearch index + map sidecar.
    fn save(&self, base: &Path, name: &str) -> Result<(), SearchError> {
        let vec_path = base.join(format!("__vec_{}.usearch", name));
        if let Err(e) = self.index.save(vec_path.to_str().unwrap_or("")) {
            tracing::error!(error = %e, field = name, "Failed to save vector index");
        }
        let map_path = base.join(format!("__vec_{}_map.json", name));
        let map_data = serde_json::json!({
            "key_map": self
                .key_map
                .iter()
                .map(|(id, key)| (id.to_string(), key))
                .collect::<HashMap<String, &String>>(),
            "next_id": self.next_id,
        });
        if let Ok(json) = serde_json::to_vec(&map_data) {
            let _ = std::fs::write(&map_path, json);
        }
        Ok(())
    }

    /// Whether the `reverse_map` <-> `key_map` bijection holds.
    #[cfg(any(test, debug_assertions))]
    fn invariant_holds(&self) -> bool {
        self.reverse_map.len() == self.key_map.len()
            && self
                .reverse_map
                .iter()
                .all(|(k, id)| self.key_map.get(id).map(String::as_str) == Some(k.as_str()))
    }
}

/// Build a fresh usearch index for a vector field.
fn new_usearch_index(
    dim: usize,
    distance_metric: VectorDistanceMetric,
    name: &str,
) -> Result<usearch::Index, SearchError> {
    let metric = match distance_metric {
        VectorDistanceMetric::Cosine => usearch::MetricKind::Cos,
        VectorDistanceMetric::L2 => usearch::MetricKind::L2sq,
        VectorDistanceMetric::IP => usearch::MetricKind::IP,
    };
    let opts = usearch::IndexOptions {
        dimensions: dim,
        metric,
        quantization: usearch::ScalarKind::F32,
        ..Default::default()
    };
    usearch::Index::new(&opts).map_err(|e| {
        SearchError::SchemaError(format!("Failed to create vector index for {}: {}", name, e))
    })
}

/// Cast raw bytes to f32 (little-endian, which is standard for x86/ARM).
fn bytes_to_f32(bytes: &[u8]) -> Vec<f32> {
    assert!(bytes.len().is_multiple_of(4));
    bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldDef, IndexSource};

    fn vec_def(dim: usize) -> SearchIndexDef {
        SearchIndexDef {
            name: "vidx".to_string(),
            prefix: vec![],
            fields: vec![FieldDef {
                name: "v".to_string(),
                field_type: FieldType::Vector {
                    dim,
                    distance_metric: VectorDistanceMetric::L2,
                },
                sortable: false,
                noindex: false,
                nostem: false,
                casesensitive: false,
                json_path: None,
            }],
            version: 1,
            synonym_groups: HashMap::new(),
            source: IndexSource::default(),
            stopwords: None,
            skip_initial_scan: false,
            language: None,
        }
    }

    fn blob(vals: &[f32]) -> Vec<u8> {
        vals.iter().flat_map(|f| f.to_le_bytes()).collect()
    }

    #[test]
    fn index_and_knn_round_trip() {
        let mut mgr = VectorFieldManager::new(&vec_def(3), None).unwrap();
        mgr.index("v", "a", &blob(&[1.0, 0.0, 0.0])).unwrap();
        mgr.index("v", "b", &blob(&[0.0, 1.0, 0.0])).unwrap();

        let hits = mgr.knn("v", &[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "a");

        assert_eq!(mgr.lookup_id("v", "a"), Some(0));
        assert_eq!(mgr.lookup_key("v", 0), Some("a"));
        mgr.check_invariant().unwrap();
    }

    #[test]
    fn delete_removes_key() {
        let mut mgr = VectorFieldManager::new(&vec_def(2), None).unwrap();
        mgr.index("v", "a", &blob(&[1.0, 0.0])).unwrap();
        mgr.delete("a");
        assert_eq!(mgr.lookup_id("v", "a"), None);
        mgr.check_invariant().unwrap();
    }

    #[test]
    fn index_non_vector_field_is_noop() {
        let mut mgr = VectorFieldManager::new(&vec_def(2), None).unwrap();
        mgr.index("missing", "a", &blob(&[1.0, 0.0])).unwrap();
        assert_eq!(mgr.lookup_id("missing", "a"), None);
    }
}
