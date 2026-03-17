//! Write-path hook for updating search indexes.
//!
//! Called after WAL persistence to keep search indexes in sync
//! with hash key mutations.

use bytes::Bytes;

use crate::store::Store;

use super::worker::ShardWorker;

impl ShardWorker {
    /// Update search indexes after a write command.
    ///
    /// Dispatches by command name to the appropriate index update logic.
    pub(crate) fn update_search_indexes(&mut self, cmd_name: &str, args: &[Bytes]) {
        match cmd_name {
            "HSET" | "HSETNX" | "HMSET" | "HINCRBY" | "HINCRBYFLOAT" => {
                if !args.is_empty() {
                    self.reindex_hash_key(&args[0]);
                }
            }
            "DEL" | "UNLINK" => {
                for key in args {
                    self.delete_from_search_indexes(key);
                }
            }
            "HDEL" => {
                if !args.is_empty() {
                    let key = &args[0];
                    if self.store.contains(key) {
                        self.reindex_hash_key(key);
                    } else {
                        self.delete_from_search_indexes(key);
                    }
                }
            }
            "RENAME" => {
                if args.len() >= 2 {
                    self.delete_from_search_indexes(&args[0]);
                    self.reindex_hash_key(&args[1]);
                }
            }
            // JSON mutation commands — reindex for ON JSON indexes
            "JSON.SET" | "JSON.MERGE" => {
                if !args.is_empty() {
                    self.reindex_json_key(&args[0]);
                }
            }
            "JSON.MSET" => {
                // args: key1 path1 val1 key2 path2 val2 ...
                let mut j = 0;
                while j + 2 < args.len() {
                    self.reindex_json_key(&args[j]);
                    j += 3;
                }
            }
            "JSON.DEL" | "JSON.CLEAR" => {
                if !args.is_empty() {
                    let key = &args[0];
                    if self.store.contains(key) {
                        self.reindex_json_key(key);
                    } else {
                        self.delete_from_search_indexes(key);
                    }
                }
            }
            "JSON.NUMINCRBY" | "JSON.NUMMULTBY" | "JSON.STRAPPEND" | "JSON.ARRAPPEND"
            | "JSON.ARRINSERT" | "JSON.ARRPOP" | "JSON.ARRTRIM" | "JSON.TOGGLE" => {
                if !args.is_empty() {
                    self.reindex_json_key(&args[0]);
                }
            }
            _ => {}
        }
    }

    /// Re-index a hash key in all matching search indexes.
    fn reindex_hash_key(&mut self, key: &Bytes) {
        let key_str = match std::str::from_utf8(key) {
            Ok(s) => s,
            Err(_) => return,
        };

        // Read hash fields from the store first to avoid borrow conflict.
        let hash = match self.store.get(key) {
            Some(value) => {
                let value_ref: &crate::types::Value = &value;
                match value_ref.as_hash() {
                    Some(h) => h
                        .iter()
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .collect::<Vec<_>>(),
                    None => return,
                }
            }
            None => return,
        };

        let hash_fields: Vec<(String, String)> = hash
            .iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(k).to_string(),
                    String::from_utf8_lossy(v).to_string(),
                )
            })
            .collect();

        for idx in self.search_indexes.values_mut() {
            if idx.matches_prefix(key_str) {
                idx.index_document(key_str, &hash_fields);
                if idx.has_vector_fields() {
                    for (field_name, raw_val) in &hash {
                        let fname = String::from_utf8_lossy(field_name);
                        idx.index_vector(&fname, key_str, raw_val);
                    }
                }
            }
        }
    }

    /// Re-index a JSON key in all matching JSON-source search indexes.
    fn reindex_json_key(&mut self, key: &Bytes) {
        let key_str = match std::str::from_utf8(key) {
            Ok(s) => s,
            Err(_) => return,
        };

        // Read JSON data from the store first to avoid borrow conflict.
        let json_data = match self.store.get(key) {
            Some(value) => {
                let value_ref: &crate::types::Value = &value;
                match value_ref.as_json() {
                    Some(jv) => jv.data().clone(),
                    None => return,
                }
            }
            None => return,
        };

        for idx in self.search_indexes.values_mut() {
            if idx.definition().source == frogdb_search::IndexSource::Json
                && idx.matches_prefix(key_str)
            {
                let fields = frogdb_search::extract_json_fields(idx.definition(), &json_data);
                idx.index_document(key_str, &fields);
            }
        }
    }

    /// Delete a key from all matching search indexes.
    pub(crate) fn delete_from_search_indexes(&mut self, key: &Bytes) {
        let key_str = match std::str::from_utf8(key) {
            Ok(s) => s,
            Err(_) => return,
        };

        for idx in self.search_indexes.values_mut() {
            if idx.matches_prefix(key_str) {
                idx.delete_document(key_str);
                idx.delete_vector(key_str);
            }
        }
    }
}
