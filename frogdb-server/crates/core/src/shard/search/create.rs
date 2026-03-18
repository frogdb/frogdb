use bytes::Bytes;
use frogdb_protocol::Response;

use super::super::worker::ShardWorker;
use crate::store::Store;

impl ShardWorker {
    pub(crate) async fn execute_ft_create(
        &mut self,
        index_def_json: &Bytes,
    ) -> Vec<(Bytes, Response)> {
        use frogdb_search::{SearchIndexDef, ShardSearchIndex};

        let def: SearchIndexDef = match serde_json::from_slice(index_def_json) {
            Ok(d) => d,
            Err(e) => {
                return vec![(
                    Bytes::from_static(b"__ft_create__"),
                    Response::error(format!("ERR invalid index definition: {}", e)),
                )];
            }
        };

        let index_name = def.name.clone();

        // Check for duplicate index
        if self.search.indexes.contains_key(&index_name) {
            return vec![(
                Bytes::from_static(b"__ft_create__"),
                Response::error(format!("Index already exists: {}", index_name)),
            )];
        }

        // Create tantivy directory
        let search_dir = self
            .data_dir()
            .join("search")
            .join(&index_name)
            .join(format!("shard_{}", self.identity.shard_id));

        let idx = match ShardSearchIndex::open(def.clone(), &search_dir) {
            Ok(idx) => idx,
            Err(e) => {
                return vec![(
                    Bytes::from_static(b"__ft_create__"),
                    Response::error(format!("ERR failed to create index: {}", e)),
                )];
            }
        };

        // Persist to RocksDB search_meta CF
        if let Some(ref rocks) = self.persistence.rocks_store
            && let Ok(json) = serde_json::to_vec(&def)
            && let Err(e) =
                rocks.put_search_meta(self.identity.shard_id, index_name.as_bytes(), &json)
        {
            tracing::error!(error = %e, "Failed to persist search index metadata");
        }

        self.search.indexes.insert(index_name.clone(), idx);

        // Index existing keys matching prefix (unless SKIPINITIALSCAN)
        if def.skip_initial_scan {
            return vec![(Bytes::from_static(b"__ft_create__"), Response::ok())];
        }
        let prefixes = def.prefix.clone();
        let all_keys = self.store.all_keys();
        let matches_prefix = |key: &Bytes| -> bool {
            if prefixes.is_empty() {
                return true;
            }
            let key_str = std::str::from_utf8(key).unwrap_or("");
            prefixes.iter().any(|p| key_str.starts_with(p))
        };

        if let Some(idx) = self.search.indexes.get_mut(&index_name) {
            let has_vectors = idx.has_vector_fields();
            let is_json = def.source == frogdb_search::IndexSource::Json;
            for key in &all_keys {
                if matches_prefix(key) {
                    let key_str = std::str::from_utf8(key).unwrap_or("");
                    if let Some(value) = self.store.get(key) {
                        if is_json {
                            if let Some(json_val) = value.as_json() {
                                let fields =
                                    frogdb_search::extract_json_fields(&def, json_val.data());
                                idx.index_document(key_str, &fields);
                            }
                        } else if let Some(hash) = value.as_hash() {
                            let fields: Vec<(String, String)> = hash
                                .iter()
                                .map(|(k, v)| {
                                    (
                                        String::from_utf8_lossy(&k).to_string(),
                                        String::from_utf8_lossy(&v).to_string(),
                                    )
                                })
                                .collect();
                            idx.index_document(key_str, &fields);
                            if has_vectors {
                                for (k, v) in hash.iter() {
                                    let fname = String::from_utf8_lossy(&k);
                                    idx.index_vector(&fname, key_str, &v);
                                }
                            }
                        }
                    }
                }
            }
            if let Err(e) = idx.commit() {
                tracing::error!(error = %e, "Failed to commit initial index");
            }
        }

        vec![(Bytes::from_static(b"__ft_create__"), Response::ok())]
    }
}
