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

use frogdb_search::ShardSearchIndex;

use crate::persistence::RocksStore;

/// Reserved key in the `search_meta` CF holding the serialized alias map.
pub(crate) const ALIASES_KEY: &[u8] = b"__aliases__";
/// Reserved key in the `search_meta` CF holding the serialized search config map.
pub(crate) const CONFIG_KEY: &[u8] = b"__config__";
/// Reserved key prefix for per-dictionary entries in the `search_meta` CF.
pub(crate) const DICT_PREFIX: &str = "__dict__:";

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
}
