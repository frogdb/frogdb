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
            "HSET" | "HSETNX" | "HMSET" | "HINCRBY" | "HINCRBYFLOAT" | "HSETEX" => {
                if !args.is_empty() {
                    self.reindex_hash_key(&args[0]);
                }
            }
            "DEL" | "UNLINK" => {
                for key in args {
                    self.delete_from_search_indexes(key);
                }
            }
            // Field-deleting hash writes: reindex the surviving key, else drop it.
            // `HGETDEL` deletes named fields; the `H(P)EXPIRE(AT)` family
            // synchronously deletes fields on a past/zero expiry time. Both can
            // empty the key, so they mirror `HDEL`'s reindex-if-exists-else-delete.
            "HDEL" | "HGETDEL" | "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT" => {
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

        // Read raw hash entries from the store first to avoid borrow conflict.
        // `Bytes` clones are cheap refcount bumps, so this is not a deep copy.
        let entries = match self.store.get(key) {
            Some(value) => {
                let value_ref: &crate::types::Value = &value;
                match value_ref.as_hash() {
                    Some(h) => h.to_vec(),
                    None => return,
                }
            }
            None => return,
        };

        for idx in self.search.indexes.values_mut() {
            if idx.matches_prefix(key_str) {
                idx.index_hash(key_str, &entries);
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

        for idx in self.search.indexes.values_mut() {
            if idx.definition().source == frogdb_search::IndexSource::Json
                && idx.matches_prefix(key_str)
            {
                idx.index_json(key_str, &json_data);
            }
        }
    }

    /// Delete a key from all matching search indexes.
    pub(crate) fn delete_from_search_indexes(&mut self, key: &Bytes) {
        let key_str = match std::str::from_utf8(key) {
            Ok(s) => s,
            Err(_) => return,
        };

        for idx in self.search.indexes.values_mut() {
            if idx.matches_prefix(key_str) {
                // `delete_document` already removes the key from the vector sidecar.
                idx.delete_document(key_str);
            }
        }
    }
}
