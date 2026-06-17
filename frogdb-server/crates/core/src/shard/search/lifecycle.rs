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
/// Reserved prefix marking non-index metadata keys, skipped when recovering the
/// index map (the writers above use the full keys; recovery uses this to tell an
/// index entry apart from `__aliases__` / `__config__` / `__dict__:*`).
pub(crate) const RESERVED_PREFIX: &str = "__";

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

    /// Serialize + persist a value under `key`, logging (but not failing) on
    /// error. The single home for the FT.ALIASADD / FT.DICTADD / FT.CONFIG SET /
    /// FT.SYNUPDATE persists, which are best-effort — unlike the lifecycle ops,
    /// which use [`Self::persist_def`] to enforce persist-before-OK. No-op when
    /// persistence is off.
    fn persist_meta_best_effort<T: serde::Serialize>(&self, key: &[u8], value: &T, what: &str) {
        let Some(ref rocks) = self.rocks else {
            return;
        };
        match serde_json::to_vec(value) {
            Ok(json) => {
                if let Err(e) = rocks.put_search_meta(self.shard_id, key, &json) {
                    tracing::error!(error = %e, "{what}");
                }
            }
            Err(e) => tracing::error!(error = %e, "{what}"),
        }
    }

    /// Persist the alias map to the `search_meta` CF (best-effort).
    pub(crate) fn persist_aliases(&self) {
        self.persist_meta_best_effort(
            ALIASES_KEY,
            &self.aliases,
            "Failed to persist search index aliases",
        );
    }

    /// Persist a dictionary to the `search_meta` CF (best-effort).
    pub(crate) fn persist_dict(&self, dict_name: &str) {
        if let Some(dict) = self.dictionaries.get(dict_name) {
            let key = format!("{DICT_PREFIX}{dict_name}");
            let terms: Vec<&String> = dict.iter().collect();
            self.persist_meta_best_effort(
                key.as_bytes(),
                &terms,
                "Failed to persist search dictionary",
            );
        }
    }

    /// Persist the search config map to the `search_meta` CF (best-effort).
    pub(crate) fn persist_search_config(&self) {
        self.persist_meta_best_effort(CONFIG_KEY, &self.config, "Failed to persist search config");
    }

    /// FT.SYNUPDATE. Replace a synonym group on an index and persist the updated
    /// definition (best-effort, matching the other synonym/dictionary commands).
    pub(crate) fn synupdate(
        &mut self,
        name: &str,
        group_id: &str,
        terms: Vec<String>,
    ) -> Result<(), LifecycleError> {
        let name = self.resolve_index_name(name).to_string();
        {
            let idx = self
                .indexes
                .get_mut(&name)
                .ok_or_else(|| LifecycleError::NotFound(name.clone()))?;
            idx.def.synonym_groups.insert(group_id.to_string(), terms);
        }
        // Borrow above dropped; persist the updated definition best-effort.
        let def = &self
            .indexes
            .get(&name)
            .expect("present immediately after the insert above")
            .def;
        self.persist_meta_best_effort(name.as_bytes(), def, "Failed to persist synonym update");
        Ok(())
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

    /// Startup recovery. Rebuilds the manager (all indexes + aliases /
    /// dictionaries / config) from the `search_meta` CF.
    ///
    /// Runs at worker-spawn time so the non-`Send` index handles it opens never
    /// cross a thread boundary. Idempotent: recovering twice over the same data
    /// dir yields the same state.
    ///
    /// Error policy is decided here, in one place:
    /// - A CF-level read failure (the column family itself cannot be iterated /
    ///   read) is **fatal** — returned as `Err` so the caller aborts startup
    ///   rather than silently bringing the shard up with zero indexes.
    /// - A per-index failure (tantivy dir won't open, or the def won't
    ///   deserialize) is **quarantined**: the metadata is kept in the CF, the
    ///   index is absent from the live map, and the outcome is surfaced in
    ///   [`RecoveryResult::outcomes`] so the caller can signal it instead of
    ///   losing it to a log line. Recovery continues for the other indexes.
    pub fn recover(
        rocks: Arc<RocksStore>,
        data_dir: PathBuf,
        shard_id: usize,
    ) -> Result<RecoveryResult, LifecycleError> {
        let mut manager = Self::new(shard_id, data_dir, Some(rocks.clone()));
        let mut outcomes = Vec::new();

        // Iterate the index definitions. A CF-level read failure is fatal.
        let iter = rocks
            .iter_search_meta(shard_id)
            .map_err(LifecycleError::Persist)?;
        for (key_bytes, value_bytes) in iter {
            let key = String::from_utf8_lossy(&key_bytes);
            // Skip reserved (non-index) metadata; handled below.
            if key.starts_with(RESERVED_PREFIX) {
                continue;
            }
            let index_name = key.into_owned();
            match serde_json::from_slice::<SearchIndexDef>(&value_bytes) {
                Ok(def) => {
                    let dir = manager.index_dir(&index_name);
                    match ShardSearchIndex::open(def, &dir) {
                        Ok(idx) => {
                            let num_docs = idx.num_docs();
                            manager.indexes.insert(index_name.clone(), idx);
                            outcomes.push((index_name, RecoveryOutcome::Recovered { num_docs }));
                        }
                        Err(e) => {
                            // Quarantine: metadata kept, index unavailable.
                            outcomes.push((index_name, RecoveryOutcome::Corrupt(e)));
                        }
                    }
                }
                Err(e) => {
                    outcomes.push((index_name, RecoveryOutcome::Undeserializable(e.to_string())));
                }
            }
        }

        // Restore aliases / config from their reserved keys. CF-level read
        // failures are fatal; a malformed value is ignored (best-effort), as
        // before.
        if let Some(alias_json) = rocks
            .get_search_meta(shard_id, ALIASES_KEY)
            .map_err(LifecycleError::Persist)?
            && let Ok(a) = serde_json::from_slice::<HashMap<String, String>>(&alias_json)
        {
            manager.aliases = a;
        }
        if let Some(config_json) = rocks
            .get_search_meta(shard_id, CONFIG_KEY)
            .map_err(LifecycleError::Persist)?
            && let Ok(c) = serde_json::from_slice::<HashMap<String, String>>(&config_json)
        {
            manager.config = c;
        }

        // Restore dictionaries (keys prefixed with `__dict__:`).
        let dict_iter = rocks
            .iter_search_meta(shard_id)
            .map_err(LifecycleError::Persist)?;
        for (key_bytes, value_bytes) in dict_iter {
            if let Ok(key_str) = std::str::from_utf8(&key_bytes)
                && let Some(dict_name) = key_str.strip_prefix(DICT_PREFIX)
                && let Ok(terms) = serde_json::from_slice::<Vec<String>>(&value_bytes)
            {
                manager
                    .dictionaries
                    .insert(dict_name.to_string(), terms.into_iter().collect());
            }
        }

        Ok(RecoveryResult { manager, outcomes })
    }

    /// Number of live indexes (for recovery logging / tests).
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    /// Whether there are no live indexes.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }
}

/// What [`IndexLifecycleManager::recover`] produces: the assembled manager plus
/// a per-index outcome so the caller can surface corruption rather than lose it
/// to a log line.
pub struct RecoveryResult {
    pub manager: IndexLifecycleManager,
    pub outcomes: Vec<(String, RecoveryOutcome)>,
}

/// The outcome of recovering one index entry from the `search_meta` CF.
pub enum RecoveryOutcome {
    /// Index opened and installed into the live map.
    Recovered { num_docs: u64 },
    /// Metadata present, tantivy directory failed to open. Quarantined: kept in
    /// the `search_meta` CF, absent from the live map, surfaced here.
    Corrupt(SearchError),
    /// Metadata bytes did not deserialize into a `SearchIndexDef`.
    Undeserializable(String),
}
