//! Write-path hook for updating search indexes.
//!
//! Called after WAL persistence to keep search indexes in sync
//! with hash key mutations.

use bytes::Bytes;

use crate::command_spec::{IndexKind, ReindexAction, ReindexSpec};
use crate::store::Store;

use super::worker::ShardWorker;

impl ShardWorker {
    /// Apply a write command's declared reindex fact to the search indexes.
    ///
    /// Resolves the [`ReindexSpec`] to typed [`ReindexAction`]s and applies each,
    /// replacing the former command-name `match`. The spec is a
    /// [`CommandSpec`](crate::command_spec::CommandSpec) fact declared at the
    /// command's own declaration site; the `ShardWorker` owns *how* each action
    /// touches the index.
    pub(crate) fn apply_reindex(&mut self, spec: ReindexSpec, args: &[Bytes]) {
        for action in spec.actions(args) {
            match action {
                ReindexAction::Reindex { key, kind } => self.reindex(key, kind),
                ReindexAction::ReindexOrDelete { key, kind } => {
                    if self.store.contains(key) {
                        self.reindex(key, kind);
                    } else {
                        self.delete_from_search_indexes(&Bytes::copy_from_slice(key));
                    }
                }
                ReindexAction::Delete { key } => {
                    self.delete_from_search_indexes(&Bytes::copy_from_slice(key));
                }
            }
        }
    }

    /// Reindex `key` as the given [`IndexKind`], dispatching to the hash or JSON
    /// projection body.
    fn reindex(&mut self, key: &[u8], kind: IndexKind) {
        match kind {
            IndexKind::Hash => self.reindex_hash_key(key),
            IndexKind::Json => self.reindex_json_key(key),
        }
    }

    /// Re-index a hash key in all matching search indexes.
    fn reindex_hash_key(&mut self, key: &[u8]) {
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
    fn reindex_json_key(&mut self, key: &[u8]) {
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
