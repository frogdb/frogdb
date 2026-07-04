//! Tiered column-family operations: one resolver + one op set for the main,
//! warm, and search-metadata tiers.
//!
//! Each tier is a parallel list of per-shard column families (see
//! [`super::manifest::ColumnFamilyManifest`]). Rather than triplicating the
//! name-list + resolver + put/get/delete/iter surface per tier, the tier is a
//! parameter: [`CfTier`] selects the name list, [`RocksStore::tier_cf_handle`]
//! is the single resolver, and the public per-tier methods (`put`/`put_warm`/
//! `put_search_meta`, ...) are thin shims kept because callers are numerous.
use super::RocksStore;
use super::config::RocksError;
use rocksdb::BoundColumnFamily;
use std::sync::Arc as StdArc;
use tracing::error;

/// Which column-family tier an operation targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CfTier {
    /// Hot-path per-shard data (`shard_<n>`).
    Main,
    /// Warm tier for tiered storage (`warm_<n>`); only present when enabled.
    Warm,
    /// Search-index metadata sidecar (`search_meta_<n>`).
    SearchMeta,
}

impl CfTier {
    fn label(self) -> &'static str {
        match self {
            CfTier::Main => "main",
            CfTier::Warm => "warm",
            CfTier::SearchMeta => "search meta",
        }
    }
}

pub struct RocksIterator<'a> {
    pub(super) inner:
        rocksdb::DBIteratorWithThreadMode<'a, rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>>,
}
impl<'a> Iterator for RocksIterator<'a> {
    type Item = (Box<[u8]>, Box<[u8]>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().and_then(|r| r.ok())
    }
}

impl RocksStore {
    pub fn warm_enabled(&self) -> bool {
        self.warm_enabled
    }

    /// The single CF resolver: validates tier availability and shard id, then
    /// resolves the tier's name list entry to a live handle.
    pub(crate) fn tier_cf_handle(
        &self,
        tier: CfTier,
        sid: usize,
    ) -> Result<StdArc<BoundColumnFamily<'_>>, RocksError> {
        if tier == CfTier::Warm && !self.warm_enabled {
            return Err(RocksError::ColumnFamilyNotFound(
                "warm tier not enabled".into(),
            ));
        }
        if sid >= self.num_shards {
            return Err(RocksError::InvalidShardId(sid));
        }
        let names = match tier {
            CfTier::Main => &self.cf_names,
            CfTier::Warm => &self.warm_cf_names,
            CfTier::SearchMeta => &self.search_meta_cf_names,
        };
        let n = &names[sid];
        self.db
            .cf_handle(n)
            .ok_or_else(|| RocksError::ColumnFamilyNotFound(n.clone()))
    }

    pub fn put_tier(
        &self,
        tier: CfTier,
        sid: usize,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), RocksError> {
        let cf = self.tier_cf_handle(tier, sid)?;
        self.db.put_cf(&cf, key, value).map_err(|e| {
            error!(shard_id = sid, tier = tier.label(), key_len = key.len(), error = %e, "RocksDB put failed");
            RocksError::from(e)
        })?;
        Ok(())
    }

    pub fn get_tier(
        &self,
        tier: CfTier,
        sid: usize,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, RocksError> {
        let cf = self.tier_cf_handle(tier, sid)?;
        Ok(self.db.get_cf(&cf, key)?)
    }

    pub fn delete_tier(&self, tier: CfTier, sid: usize, key: &[u8]) -> Result<(), RocksError> {
        let cf = self.tier_cf_handle(tier, sid)?;
        self.db.delete_cf(&cf, key).map_err(|e| {
            error!(shard_id = sid, tier = tier.label(), error = %e, "RocksDB delete failed");
            RocksError::from(e)
        })?;
        Ok(())
    }

    pub fn iter_tier(&self, tier: CfTier, sid: usize) -> Result<RocksIterator<'_>, RocksError> {
        let cf = self.tier_cf_handle(tier, sid)?;
        Ok(RocksIterator {
            inner: self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start),
        })
    }

    // --- Warm-tier shims ---
    pub fn put_warm(&self, sid: usize, key: &[u8], value: &[u8]) -> Result<(), RocksError> {
        self.put_tier(CfTier::Warm, sid, key, value)
    }
    pub fn get_warm(&self, sid: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        self.get_tier(CfTier::Warm, sid, key)
    }
    pub fn delete_warm(&self, sid: usize, key: &[u8]) -> Result<(), RocksError> {
        self.delete_tier(CfTier::Warm, sid, key)
    }
    pub fn iter_warm_cf(&self, sid: usize) -> Result<RocksIterator<'_>, RocksError> {
        self.iter_tier(CfTier::Warm, sid)
    }

    // --- Search-metadata shims ---
    pub fn put_search_meta(&self, sid: usize, key: &[u8], value: &[u8]) -> Result<(), RocksError> {
        self.put_tier(CfTier::SearchMeta, sid, key, value)
    }
    pub fn get_search_meta(&self, sid: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        self.get_tier(CfTier::SearchMeta, sid, key)
    }
    pub fn delete_search_meta(&self, sid: usize, key: &[u8]) -> Result<(), RocksError> {
        self.delete_tier(CfTier::SearchMeta, sid, key)
    }
    pub fn iter_search_meta(&self, sid: usize) -> Result<RocksIterator<'_>, RocksError> {
        self.iter_tier(CfTier::SearchMeta, sid)
    }
}
