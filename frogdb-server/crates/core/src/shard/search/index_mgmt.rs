use bytes::Bytes;
use frogdb_protocol::Response;

use super::super::worker::ShardWorker;
use super::lifecycle::LifecycleError;
use crate::store::Store;

impl ShardWorker {
    pub(crate) async fn execute_ft_dropindex(
        &mut self,
        index_name: &Bytes,
    ) -> Vec<(Bytes, Response)> {
        let tag = Bytes::from_static(b"__ft_dropindex__");
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let resp = match self.search.drop_index(name) {
            Ok(()) => Response::ok(),
            Err(LifecycleError::NotFound(_)) => Response::error("Unknown index name"),
            Err(LifecycleError::Persist(e)) => {
                Response::error(format!("ERR failed to delete search index metadata: {e}"))
            }
            Err(LifecycleError::Schema(e)) => {
                Response::error(format!("ERR failed to destroy search index files: {e}"))
            }
            Err(e) => Response::error(format!("ERR failed to drop index: {e}")),
        };
        vec![(tag, resp)]
    }

    pub(crate) fn execute_ft_info(&self, index_name: &Bytes) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let idx = match self.search.info(name) {
            Ok(idx) => idx,
            Err(_) => {
                return vec![(
                    Bytes::from_static(b"__ft_info__"),
                    Response::error("Unknown index name"),
                )];
            }
        };

        let def = idx.definition();
        let num_docs = idx.num_docs();

        // Build RediSearch-compatible FT.INFO response
        let prefixes: Vec<Response> = def
            .prefix
            .iter()
            .map(|p| Response::bulk(Bytes::from(p.clone())))
            .collect();
        let index_def = vec![
            Response::bulk(Bytes::from_static(b"key_type")),
            Response::bulk(Bytes::from_static(b"HASH")),
            Response::bulk(Bytes::from_static(b"prefixes")),
            Response::Array(prefixes),
        ];

        // Field definitions
        let mut attrs = Vec::new();
        for field in &def.fields {
            let type_str = match &field.field_type {
                frogdb_search::FieldType::Text { .. } => "TEXT",
                frogdb_search::FieldType::Tag { .. } => "TAG",
                frogdb_search::FieldType::Numeric => "NUMERIC",
                frogdb_search::FieldType::Geo => "GEO",
                frogdb_search::FieldType::Vector { .. } => "VECTOR",
            };
            attrs.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"identifier")),
                Response::bulk(Bytes::from(field.name.clone())),
                Response::bulk(Bytes::from_static(b"attribute")),
                Response::bulk(Bytes::from(field.name.clone())),
                Response::bulk(Bytes::from_static(b"type")),
                Response::bulk(Bytes::from(type_str)),
            ]));
        }

        let info = vec![
            Response::bulk(Bytes::from_static(b"index_name")),
            Response::bulk(Bytes::from(def.name.clone())),
            Response::bulk(Bytes::from_static(b"index_options")),
            Response::Array(vec![]),
            Response::bulk(Bytes::from_static(b"index_definition")),
            Response::Array(index_def),
            Response::bulk(Bytes::from_static(b"attributes")),
            Response::Array(attrs),
            Response::bulk(Bytes::from_static(b"num_docs")),
            Response::Integer(num_docs as i64),
            Response::bulk(Bytes::from_static(b"num_synonym_groups")),
            Response::Integer(def.synonym_groups.len() as i64),
        ];

        vec![(Bytes::from_static(b"__ft_info__"), Response::Array(info))]
    }

    pub(crate) fn execute_ft_list(&self) -> Vec<(Bytes, Response)> {
        let names: Vec<Response> = self
            .search
            .indexes
            .keys()
            .map(|name| Response::bulk(Bytes::from(name.clone())))
            .collect();
        vec![(Bytes::from_static(b"__ft_list__"), Response::Array(names))]
    }

    pub(crate) async fn execute_ft_alter(
        &mut self,
        index_name: &Bytes,
        new_fields_json: &Bytes,
    ) -> Vec<(Bytes, Response)> {
        use frogdb_search::FieldDef;

        let tag = Bytes::from_static(b"__ft_alter__");
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let new_fields: Vec<FieldDef> = match serde_json::from_slice(new_fields_json) {
            Ok(f) => f,
            Err(e) => {
                return vec![(
                    tag,
                    Response::error(format!("ERR invalid field definitions: {}", e)),
                )];
            }
        };

        let store = &mut self.store;

        // The manager validates, persists-before-mutate, reopens, and commits;
        // this closure re-indexes the matching hash keys against the new schema.
        let result = self.search.alter(name, new_fields, |idx| {
            let prefixes = idx.definition().prefix.clone();
            for key in store.all_keys() {
                if !super::key_matches_prefix(&prefixes, &key) {
                    continue;
                }
                let key_str = std::str::from_utf8(&key).unwrap_or("");
                if let Some(value) = store.get(&key)
                    && let Some(hash) = value.as_hash()
                {
                    idx.index_hash(key_str, &hash.to_vec());
                }
            }
        });

        let resp = match result {
            Ok(()) => Response::ok(),
            Err(LifecycleError::NotFound(_)) => Response::error("Unknown index name"),
            Err(LifecycleError::DuplicateField(f)) => {
                Response::error(format!("Duplicate field in schema: {f}"))
            }
            Err(LifecycleError::Schema(e)) => {
                Response::error(format!("ERR failed to alter index: {e}"))
            }
            Err(e) => Response::error(format!("ERR failed to persist altered index: {e}")),
        };
        vec![(tag, resp)]
    }
}
