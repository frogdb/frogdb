use bytes::Bytes;
use frogdb_protocol::Response;

use super::super::worker::ShardWorker;
use super::lifecycle::LifecycleError;
use crate::store::Store;

impl ShardWorker {
    pub(crate) async fn execute_ft_create(
        &mut self,
        index_def_json: &Bytes,
    ) -> Vec<(Bytes, Response)> {
        use frogdb_search::{IndexSource, SearchIndexDef};

        let tag = Bytes::from_static(b"__ft_create__");
        let def: SearchIndexDef = match serde_json::from_slice(index_def_json) {
            Ok(d) => d,
            Err(e) => {
                return vec![(
                    tag,
                    Response::error(format!("ERR invalid index definition: {}", e)),
                )];
            }
        };

        let skip_scan = def.skip_initial_scan;
        let store = &mut self.store;

        // The lifecycle manager owns validation, dir creation, commit, and the
        // persist-before-OK invariant; this closure is just the initial scan
        // over the live keyspace.
        let result = self.search.create(def, |idx| {
            if skip_scan {
                return;
            }
            let def = idx.definition().clone();
            let has_vectors = idx.has_vector_fields();
            let is_json = def.source == IndexSource::Json;
            for key in store.all_keys() {
                if !super::key_matches_prefix(&def.prefix, &key) {
                    continue;
                }
                let key_str = std::str::from_utf8(&key).unwrap_or("");
                if let Some(value) = store.get(&key) {
                    if is_json {
                        if let Some(json_val) = value.as_json() {
                            let fields = frogdb_search::extract_json_fields(&def, json_val.data());
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
        });

        let resp = match result {
            Ok(()) => Response::ok(),
            Err(LifecycleError::AlreadyExists(name)) => {
                Response::error(format!("Index already exists: {name}"))
            }
            Err(LifecycleError::Schema(e)) => {
                Response::error(format!("ERR failed to create index: {e}"))
            }
            Err(e) => Response::error(format!("ERR failed to create index: {e}")),
        };
        vec![(tag, resp)]
    }
}
