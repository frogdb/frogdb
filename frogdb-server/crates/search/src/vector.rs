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
    ///
    /// All-or-nothing: the fallible usearch ops run *before* any map mutation,
    /// so a failure leaves the field exactly as it was (on replace, the prior
    /// `(key, id)` stays intact in both maps and in usearch). The new vector is
    /// committed to usearch before the old one is removed, so a failed `add`
    /// during replace can never half-delete the prior entry.
    fn index(&mut self, key: &str, blob: &[u8]) -> Result<(), SearchError> {
        let expected_len = self.dim * 4; // f32 = 4 bytes
        if blob.len() != expected_len {
            return Err(SearchError::SchemaError(format!(
                "vector blob size mismatch: expected {} bytes, got {}",
                expected_len,
                blob.len()
            )));
        }
        let floats = bytes_to_f32(blob);

        // Reserve capacity first; propagate failure instead of discarding it.
        if self.index.size() >= self.index.capacity() {
            self.index
                .reserve(self.index.capacity().max(64) * 2)
                .map_err(usearch_err)?;
        }

        // Snapshot the prior id so the replace can be reconciled after the add.
        let prior = self.reverse_map.get(key).copied();

        // usearch op FIRST; only mutate maps once it has committed. The new id
        // is added before the old one is removed so a failed `add` aborts the
        // whole operation with the prior entry untouched.
        let new_id = self.next_id;
        self.index.add(new_id, &floats).map_err(usearch_err)?;
        self.next_id += 1;

        // Map mutations are infallible and happen as one unit after the add.
        if let Some(old_id) = prior {
            // Best-effort: maps are authoritative for "absent". A failed remove
            // leaves a garbage usearch id that knn can never surface (it filters
            // every hit through key_map), and never breaks the bijection.
            let _ = self.index.remove(old_id);
            self.key_map.remove(&old_id); // the orphan the old code leaked
        }
        self.key_map.insert(new_id, key.to_string());
        self.reverse_map.insert(key.to_string(), new_id);

        debug_assert!(self.invariant_holds());
        Ok(())
    }

    /// Remove `key` from this field, if present.
    ///
    /// The usearch remove is best-effort (maps are authoritative); the bijection
    /// always holds afterward.
    fn delete(&mut self, key: &str) {
        if let Some(id) = self.reverse_map.remove(key) {
            let _ = self.index.remove(id);
            self.key_map.remove(&id);
        }
        debug_assert!(self.invariant_holds());
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

    /// Whether the field's invariant holds: `reverse_map` and `key_map` are
    /// mutual inverses of equal size, and every live id is present in usearch.
    #[cfg(any(test, debug_assertions))]
    fn invariant_holds(&self) -> bool {
        self.reverse_map.len() == self.key_map.len()
            && self.reverse_map.iter().all(|(k, id)| {
                self.key_map.get(id).map(String::as_str) == Some(k.as_str())
                    && self.index.contains(*id)
            })
    }
}

/// Convert a usearch error into a [`SearchError`].
fn usearch_err(e: impl std::fmt::Display) -> SearchError {
    SearchError::SchemaError(format!("usearch error: {}", e))
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

    #[test]
    fn blob_size_mismatch_is_err_and_noop() {
        let mut mgr = VectorFieldManager::new(&vec_def(3), None).unwrap();
        // 2 floats for a 3-dim field.
        let err = mgr.index("v", "a", &blob(&[1.0, 0.0]));
        assert!(err.is_err());
        let vf = mgr.fields.get("v").unwrap();
        assert!(vf.key_map.is_empty());
        assert!(vf.reverse_map.is_empty());
        assert_eq!(vf.next_id, 0);
        mgr.check_invariant().unwrap();
    }

    #[test]
    fn replace_does_not_leak_key_map_entry() {
        let mut mgr = VectorFieldManager::new(&vec_def(3), None).unwrap();
        mgr.index("v", "a", &blob(&[1.0, 0.0, 0.0])).unwrap();
        mgr.index("v", "a", &blob(&[0.0, 1.0, 0.0])).unwrap();

        let vf = mgr.fields.get("v").unwrap();
        assert_eq!(
            vf.key_map.len(),
            1,
            "re-index must not leak a key_map entry"
        );
        assert_eq!(vf.reverse_map.len(), 1);
        // The latest id maps back to the key, and the prior id is gone.
        let id = mgr.lookup_id("v", "a").unwrap();
        assert_eq!(mgr.lookup_key("v", id), Some("a"));
        mgr.check_invariant().unwrap();

        // knn returns the replacement vector, not the original.
        let hits = mgr.knn("v", &[0.0, 1.0, 0.0], 1).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "a");
    }

    /// White-box: a `VectorField` whose `dim` lies about its usearch index forces
    /// `usearch::add` to fail on a blob that passes the length check.
    fn lying_field(usearch_dim: usize, claimed_dim: usize) -> VectorField {
        let index = new_usearch_index(usearch_dim, VectorDistanceMetric::L2, "v").unwrap();
        let _ = index.reserve(64);
        VectorField {
            index,
            key_map: HashMap::new(),
            reverse_map: HashMap::new(),
            next_id: 0,
            dim: claimed_dim,
        }
    }

    #[test]
    fn forced_add_failure_rolls_back() {
        // usearch expects 3 dims; dim field claims 4 so a 4-float blob passes the
        // length check but the usearch add rejects it.
        let mut vf = lying_field(3, 4);
        let err = vf.index("a", &blob(&[1.0, 2.0, 3.0, 4.0]));
        assert!(err.is_err(), "usearch add of wrong-dim vector should fail");
        assert!(vf.key_map.is_empty(), "no map entry on add failure");
        assert!(vf.reverse_map.is_empty());
        assert_eq!(vf.next_id, 0, "next_id must not advance on add failure");
        assert!(vf.invariant_holds());
    }

    #[test]
    fn forced_add_failure_on_replace_keeps_prior() {
        let mut vf = lying_field(3, 3);
        vf.index("a", &blob(&[1.0, 2.0, 3.0])).unwrap();
        assert_eq!(vf.reverse_map.get("a"), Some(&0));

        // Now make subsequent adds fail by lying about the dimension.
        vf.dim = 4;
        let err = vf.index("a", &blob(&[1.0, 2.0, 3.0, 4.0]));
        assert!(err.is_err());

        // The prior (key, id) survives intact in both maps and in usearch.
        assert_eq!(vf.reverse_map.get("a"), Some(&0));
        assert_eq!(vf.key_map.get(&0).map(String::as_str), Some("a"));
        assert!(vf.index.contains(0));
        assert_eq!(vf.key_map.len(), 1);
        assert_eq!(vf.reverse_map.len(), 1);
        assert!(vf.invariant_holds());
    }

    #[test]
    fn bijection_holds_under_random_ops() {
        let mut mgr = VectorFieldManager::new(&vec_def(2), None).unwrap();
        let mut state: u64 = 0x1234_5678_9abc_def0;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state
        };

        for _ in 0..500 {
            let r = next();
            let key = format!("k{}", r % 8); // small key space => frequent replaces
            if r & 1 == 0 {
                let v = blob(&[(r >> 8) as f32, (r >> 16) as f32]);
                mgr.index("v", &key, &v).unwrap();
            } else {
                mgr.delete(&key);
            }
            mgr.check_invariant()
                .expect("bijection must hold after every op");
        }
    }
}
