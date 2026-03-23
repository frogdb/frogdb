use bytes::Bytes;
use frogdb_protocol::Response;

use super::super::worker::ShardWorker;
use crate::store::Store;

impl ShardWorker {
    pub(crate) async fn execute_ft_dropindex(
        &mut self,
        index_name: &Bytes,
    ) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.resolve_index_name(name).to_string();

        // Remove any aliases pointing to this index
        self.search.aliases.retain(|_, v| *v != name);

        if let Some(idx) = self.search.indexes.remove(&name) {
            // Delete from RocksDB
            if let Err(e) = self
                .persistence
                .delete_search_meta(self.identity.shard_id, name.as_bytes())
            {
                tracing::error!(error = %e, "Failed to delete search index metadata");
            }

            // Destroy tantivy files
            let search_dir = self
                .data_dir()
                .join("search")
                .join(name)
                .join(format!("shard_{}", self.identity.shard_id));
            if let Err(e) = idx.destroy(&search_dir) {
                tracing::error!(error = %e, "Failed to destroy search index files");
            }

            vec![(Bytes::from_static(b"__ft_dropindex__"), Response::ok())]
        } else {
            vec![(
                Bytes::from_static(b"__ft_dropindex__"),
                Response::error("Unknown index name"),
            )]
        }
    }

    pub(crate) fn execute_ft_info(&self, index_name: &Bytes) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.resolve_index_name(name);
        let idx = match self.search.indexes.get(name) {
            Some(idx) => idx,
            None => {
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

        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.resolve_index_name(name).to_string();
        let new_fields: Vec<FieldDef> = match serde_json::from_slice(new_fields_json) {
            Ok(f) => f,
            Err(e) => {
                return vec![(
                    Bytes::from_static(b"__ft_alter__"),
                    Response::error(format!("ERR invalid field definitions: {}", e)),
                )];
            }
        };

        let idx = match self.search.indexes.get(&name) {
            Some(idx) => idx,
            None => {
                return vec![(
                    Bytes::from_static(b"__ft_alter__"),
                    Response::error("Unknown index name"),
                )];
            }
        };

        // Check for duplicate field names
        let existing_names: Vec<&str> = idx.def.fields.iter().map(|f| f.name.as_str()).collect();
        for f in &new_fields {
            if existing_names.contains(&f.name.as_str()) {
                return vec![(
                    Bytes::from_static(b"__ft_alter__"),
                    Response::error(format!("Duplicate field in schema: {}", f.name)),
                )];
            }
        }

        // Build expanded definition
        let mut new_def = idx.definition().clone();
        new_def.fields.extend(new_fields);

        // Reopen with expanded schema
        let idx = self.search.indexes.get_mut(&name).unwrap();
        if let Err(e) = idx.reopen_with_def(new_def.clone()) {
            return vec![(
                Bytes::from_static(b"__ft_alter__"),
                Response::error(format!("ERR failed to alter index: {}", e)),
            )];
        }

        // Re-index all matching keys to populate new fields
        let prefixes = new_def.prefix.clone();
        let all_keys = self.store.all_keys();
        let matches_prefix = |key: &Bytes| -> bool {
            if prefixes.is_empty() {
                return true;
            }
            let key_str = std::str::from_utf8(key).unwrap_or("");
            prefixes.iter().any(|p| key_str.starts_with(p))
        };

        let idx = self.search.indexes.get_mut(&name).unwrap();
        let has_vectors = idx.has_vector_fields();
        for key in &all_keys {
            if matches_prefix(key) {
                let key_str = std::str::from_utf8(key).unwrap_or("");
                if let Some(value) = self.store.get(key)
                    && let Some(hash) = value.as_hash()
                {
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
        if let Err(e) = idx.commit() {
            tracing::error!(error = %e, "Failed to commit after FT.ALTER re-index");
        }

        // Persist updated definition to RocksDB
        if let Some(ref rocks) = self.persistence.rocks_store
            && let Ok(json) = serde_json::to_vec(&new_def)
            && let Err(e) = rocks.put_search_meta(self.identity.shard_id, name.as_bytes(), &json)
        {
            tracing::error!(error = %e, "Failed to persist altered search index metadata");
        }

        vec![(Bytes::from_static(b"__ft_alter__"), Response::ok())]
    }
}
