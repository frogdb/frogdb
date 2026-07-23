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
                ReindexAction::Refresh { key } => self.refresh_key(key),
            }
        }
    }

    /// Reconcile a key's search-index presence to whatever type it now holds,
    /// after a write that may have clobbered it across types.
    ///
    /// Dispatches on the key's *current* value type, mirroring how the store
    /// projects documents on the normal write path:
    /// - a hash is (re)indexed into every matching hash-source index — identical
    ///   to [`Self::reindex_hash_key`];
    /// - a JSON document is (re)indexed into every matching JSON-source index —
    ///   identical to [`Self::reindex_json_key`], closing the gap where a `COPY`,
    ///   `RESTORE`, or `RENAME` of a JSON doc into a JSON-index prefix left the
    ///   destination unindexed;
    /// - anything else (a string/list/…, or an absent key) is dropped from every
    ///   matching index.
    ///
    /// This is the search analogue of a "reconcile to current state" write:
    /// [`ReindexAction::Reindex`] silently no-ops when the value's type does not
    /// match the reindex kind and so would leave a stale doc behind, which is
    /// exactly the bug for `SET`/`RESTORE`/`COPY`/`RENAME` overwriting an indexed
    /// key with a value of a different type. `delete_from_search_indexes` is
    /// source-agnostic (it clears the key from every prefix-matching index, hash-
    /// or JSON-source), so a cross-type overwrite of an indexed key is de-indexed
    /// too.
    fn refresh_key(&mut self, key: &[u8]) {
        let value_kind = self.store.get(key).and_then(|value| {
            let value_ref: &crate::types::Value = &value;
            if value_ref.as_hash().is_some() {
                Some(IndexKind::Hash)
            } else if value_ref.as_json().is_some() {
                Some(IndexKind::Json)
            } else {
                None
            }
        });
        match value_kind {
            Some(IndexKind::Hash) => self.reindex_hash_key(key),
            Some(IndexKind::Json) => self.reindex_json_key(key),
            None => self.delete_from_search_indexes(&Bytes::copy_from_slice(key)),
        }
    }

    /// Re-index a batch of hash keys whose contents were shrunk in place by a
    /// field-TTL purge (lazy read on a READONLY command, or the active-expiry
    /// sweep) but which still exist as hashes.
    ///
    /// Such a purge mutates the hash without going through a WRITE command's
    /// [`ReindexSpec`], so the search index would otherwise keep the reaped
    /// field's stale value. Whole-key and last-field-emptied removals are handled
    /// separately (they de-index via `Delete`); this covers only the survivors.
    /// No-op when no search index exists.
    pub(crate) fn reindex_shrunk_hash_keys(&mut self, keys: &[Bytes]) {
        if self.search.indexes.is_empty() {
            return;
        }
        for key in keys {
            self.reindex_hash_key(key);
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
