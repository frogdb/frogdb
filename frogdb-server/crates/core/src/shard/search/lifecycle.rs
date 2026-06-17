//! Per-shard search index lifecycle manager.
//!
//! Owns the lifecycle of every search index on one shard: create, drop, alter,
//! info, and crash recovery. The single seam between command execution / startup
//! and (tantivy + usearch) on disk + the RocksDB `search_meta` column family.
//!
//! Per-shard by construction: it holds non-`Send` tantivy `IndexWriter` and
//! `usearch::Index` handles (via [`ShardSearchIndex`]), so it is built and lives
//! inside the shard worker and is never shipped through the coordinator. Startup
//! recovery is an associated constructor ([`IndexLifecycleManager::recover`]) that
//! runs at worker-spawn time, so the open handles never cross a thread boundary.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use frogdb_search::{FieldDef, SearchError, SearchIndexDef, ShardSearchIndex};

use crate::persistence::RocksStore;
use crate::persistence::rocks::RocksError;

/// Reserved key in the `search_meta` CF holding the serialized alias map.
pub(crate) const ALIASES_KEY: &[u8] = b"__aliases__";
/// Reserved key in the `search_meta` CF holding the serialized search config map.
pub(crate) const CONFIG_KEY: &[u8] = b"__config__";
/// Reserved key prefix for per-dictionary entries in the `search_meta` CF.
pub(crate) const DICT_PREFIX: &str = "__dict__:";

/// Errors from a lifecycle operation. Each variant carries enough to map to a
/// RediSearch reply *and* to distinguish a rolled-back operation from a durable
/// one: `Persist`/`Serialize` mean the definition could not be made durable and
/// the operation was rolled back, never reported as OK.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("Index already exists: {0}")]
    AlreadyExists(String),
    #[error("Unknown index name: {0}")]
    NotFound(String),
    #[error("Duplicate field in schema: {0}")]
    DuplicateField(String),
    #[error("schema/open error: {0}")]
    Schema(#[from] SearchError),
    /// Definition could not be serialized; the operation was rolled back.
    #[error("failed to serialize index definition: {0}")]
    Serialize(String),
    /// Definition could not be made durable; the operation was rolled back.
    #[error("failed to persist index definition: {0}")]
    Persist(#[source] RocksError),
}

/// Owns the lifecycle of every search index on one shard.
///
/// Absorbs what used to be four loose per-shard maps plus the scattered path
/// layout, reserved-key namespace, and persist helpers. The maps are
/// `pub(crate)` so read-only query/dispatch paths can still reach the live
/// indexes directly; all *mutating* lifecycle behavior goes through the methods
/// on this type.
pub struct IndexLifecycleManager {
    shard_id: usize,
    data_dir: PathBuf,
    rocks: Option<Arc<RocksStore>>,
    /// Per-shard search indexes (index_name -> ShardSearchIndex).
    pub(crate) indexes: HashMap<String, ShardSearchIndex>,
    /// Search index aliases (alias_name -> index_name).
    pub(crate) aliases: HashMap<String, String>,
    /// Search dictionaries for FT.SPELLCHECK (dict_name -> terms).
    pub(crate) dictionaries: HashMap<String, HashSet<String>>,
    /// Search configuration parameters (param_name -> value).
    pub(crate) config: HashMap<String, String>,
}

impl IndexLifecycleManager {
    /// Build an empty manager for the given shard. `data_dir` and `rocks` are the
    /// roots used by every persist + path-layout operation; `rocks` is `None`
    /// when persistence is disabled (lifecycle still works, just non-durable).
    pub(crate) fn new(shard_id: usize, data_dir: PathBuf, rocks: Option<Arc<RocksStore>>) -> Self {
        Self {
            shard_id,
            data_dir,
            rocks,
            indexes: HashMap::new(),
            aliases: HashMap::new(),
            dictionaries: HashMap::new(),
            config: HashMap::new(),
        }
    }

    /// Update the data directory. Called during shard spawn before recovery, so
    /// the path layout resolves under the real server data dir.
    pub(crate) fn set_data_dir(&mut self, dir: PathBuf) {
        self.data_dir = dir;
    }

    /// On-disk directory for one index's tantivy + usearch files on this shard.
    /// The single source of truth for the search-dir path layout (previously
    /// re-typed in create, drop, and recovery).
    pub(crate) fn index_dir(&self, name: &str) -> PathBuf {
        self.data_dir
            .join("search")
            .join(name)
            .join(format!("shard_{}", self.shard_id))
    }

    /// Resolve an index name through the alias map.
    pub(crate) fn resolve_index_name<'a>(&'a self, name: &'a str) -> &'a str {
        self.aliases.get(name).map(|s| s.as_str()).unwrap_or(name)
    }

    /// Persist the alias map to the `search_meta` CF (best-effort).
    pub(crate) fn persist_aliases(&self) {
        if let Some(ref rocks) = self.rocks
            && let Ok(json) = serde_json::to_vec(&self.aliases)
            && let Err(e) = rocks.put_search_meta(self.shard_id, ALIASES_KEY, &json)
        {
            tracing::error!(error = %e, "Failed to persist search index aliases");
        }
    }

    /// Persist a dictionary to the `search_meta` CF (best-effort).
    pub(crate) fn persist_dict(&self, dict_name: &str) {
        if let Some(ref rocks) = self.rocks
            && let Some(dict) = self.dictionaries.get(dict_name)
        {
            let key = format!("{DICT_PREFIX}{dict_name}");
            let terms: Vec<&String> = dict.iter().collect();
            if let Ok(json) = serde_json::to_vec(&terms)
                && let Err(e) = rocks.put_search_meta(self.shard_id, key.as_bytes(), &json)
            {
                tracing::error!(error = %e, "Failed to persist search dictionary");
            }
        }
    }

    /// Persist the search config map to the `search_meta` CF (best-effort).
    pub(crate) fn persist_search_config(&self) {
        if let Some(ref rocks) = self.rocks
            && let Ok(json) = serde_json::to_vec(&self.config)
            && let Err(e) = rocks.put_search_meta(self.shard_id, CONFIG_KEY, &json)
        {
            tracing::error!(error = %e, "Failed to persist search config");
        }
    }

    /// Persist an index definition under `key` in the `search_meta` CF.
    /// Returns an error (never silently swallows) so callers can enforce the
    /// persist-before-OK invariant. A no-op success when persistence is off.
    fn persist_def(&self, key: &[u8], def: &SearchIndexDef) -> Result<(), LifecycleError> {
        let json = serde_json::to_vec(def).map_err(|e| LifecycleError::Serialize(e.to_string()))?;
        if let Some(ref rocks) = self.rocks {
            rocks
                .put_search_meta(self.shard_id, key, &json)
                .map_err(LifecycleError::Persist)?;
        }
        Ok(())
    }

    /// Delete an index definition from the `search_meta` CF. Returns an error so
    /// drop can fail rather than leave metadata behind. No-op when persistence
    /// is off.
    fn delete_meta(&self, key: &[u8]) -> Result<(), LifecycleError> {
        if let Some(ref rocks) = self.rocks {
            rocks
                .delete_search_meta(self.shard_id, key)
                .map_err(LifecycleError::Persist)?;
        }
        Ok(())
    }

    /// FT.CREATE. Validates, opens the tantivy dir, runs the initial scan via the
    /// caller's closure, commits, and **persists the definition before reporting
    /// success**. On a commit or persist failure the half-built index is rolled
    /// back (live map + on-disk dir) and an error is returned — never OK.
    pub(crate) fn create(
        &mut self,
        def: SearchIndexDef,
        scan: impl FnOnce(&mut ShardSearchIndex),
    ) -> Result<(), LifecycleError> {
        let name = def.name.clone();
        if self.indexes.contains_key(&name) {
            return Err(LifecycleError::AlreadyExists(name));
        }

        let dir = self.index_dir(&name);
        let mut idx = ShardSearchIndex::open(def, &dir)?;

        // Index existing keys (no-op closure for SKIPINITIALSCAN).
        scan(&mut idx);

        // Commit the scanned documents durably before reporting success, so a
        // restart reopens an index that actually contains what the client was
        // told was indexed.
        if let Err(e) = idx.commit() {
            let _ = idx.destroy(&dir);
            return Err(LifecycleError::Schema(e));
        }

        // Persist the definition before returning OK. Roll back the on-disk dir
        // on failure so a create that reports OK is always durable.
        if let Err(e) = self.persist_def(name.as_bytes(), idx.definition()) {
            let _ = idx.destroy(&dir);
            return Err(e);
        }

        self.indexes.insert(name, idx);
        Ok(())
    }

    /// FT.DROPINDEX. Deletes metadata first, then destroys files, then drops the
    /// live entry. A metadata-delete failure aborts before any state is mutated,
    /// so a failed drop leaves the index fully intact instead of resurrecting an
    /// empty index on the next restart.
    pub(crate) fn drop_index(&mut self, name: &str) -> Result<(), LifecycleError> {
        let name = self.resolve_index_name(name).to_string();
        if !self.indexes.contains_key(&name) {
            return Err(LifecycleError::NotFound(name));
        }

        // Delete metadata first: a crash after this orphans disk files (harmless,
        // re-droppable) rather than leaving metadata that resurrects the index.
        self.delete_meta(name.as_bytes())?;

        // Metadata is gone; now drop in-memory aliases + the live entry and
        // destroy the on-disk files.
        self.aliases.retain(|_, v| *v != name);
        let dir = self.index_dir(&name);
        let idx = self
            .indexes
            .remove(&name)
            .expect("presence checked above and &mut self prevents concurrent removal");
        idx.destroy(&dir)?;
        Ok(())
    }

    /// FT.ALTER. Expands the schema with `new_fields`, re-indexes via the caller's
    /// closure, and persists the new definition. The definition is persisted
    /// **before** the live index is mutated, so a persist failure leaves both the
    /// live index and the CF on the old definition and fails the command — the
    /// new field never silently vanishes on restart.
    pub(crate) fn alter(
        &mut self,
        name: &str,
        new_fields: Vec<FieldDef>,
        scan: impl FnOnce(&mut ShardSearchIndex),
    ) -> Result<(), LifecycleError> {
        let name = self.resolve_index_name(name).to_string();
        let idx = self
            .indexes
            .get(&name)
            .ok_or_else(|| LifecycleError::NotFound(name.clone()))?;

        // Reject fields that already exist.
        let existing: Vec<&str> = idx.def.fields.iter().map(|f| f.name.as_str()).collect();
        for f in &new_fields {
            if existing.contains(&f.name.as_str()) {
                return Err(LifecycleError::DuplicateField(f.name.clone()));
            }
        }

        let old_def = idx.definition().clone();
        let mut new_def = old_def.clone();
        new_def.fields.extend(new_fields);

        // Persist the expanded definition before touching live state. On failure
        // nothing has changed: live index + CF both still hold the old schema.
        self.persist_def(name.as_bytes(), &new_def)?;

        // Apply the expanded schema in-memory + on disk.
        let reopen = {
            let idx = self
                .indexes
                .get_mut(&name)
                .expect("presence checked above and &mut self prevents concurrent removal");
            idx.reopen_with_def(new_def)
        };
        if let Err(e) = reopen {
            // Roll the CF back to the old definition to stay consistent with the
            // (still-old) live index, then fail.
            let _ = self.persist_def(name.as_bytes(), &old_def);
            return Err(LifecycleError::Schema(e));
        }

        // Re-index against the new schema and commit durably before OK.
        let idx = self
            .indexes
            .get_mut(&name)
            .expect("present immediately after a successful reopen on &mut self");
        scan(idx);
        idx.commit().map_err(LifecycleError::Schema)?;
        Ok(())
    }

    /// FT.INFO. Resolve a name (through aliases) to the live index.
    pub(crate) fn info(&self, name: &str) -> Result<&ShardSearchIndex, LifecycleError> {
        let resolved = self.resolve_index_name(name);
        self.indexes
            .get(resolved)
            .ok_or_else(|| LifecycleError::NotFound(resolved.to_string()))
    }
}
