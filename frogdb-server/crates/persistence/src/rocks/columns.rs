//! Warm tier and search metadata column family operations.
use super::RocksStore;
use super::config::RocksError;
use rocksdb::BoundColumnFamily;
use std::sync::Arc as StdArc;
use tracing::error;
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
    fn warm_cf_handle(&self, sid: usize) -> Result<StdArc<BoundColumnFamily<'_>>, RocksError> {
        if !self.warm_enabled {
            return Err(RocksError::ColumnFamilyNotFound(
                "warm tier not enabled".into(),
            ));
        }
        if sid >= self.num_shards {
            return Err(RocksError::InvalidShardId(sid));
        }
        let n = &self.warm_cf_names[sid];
        self.db
            .cf_handle(n)
            .ok_or_else(|| RocksError::ColumnFamilyNotFound(n.clone()))
    }
    pub fn put_warm(&self, sid: usize, key: &[u8], value: &[u8]) -> Result<(), RocksError> {
        let cf = self.warm_cf_handle(sid)?;
        self.db.put_cf(&cf, key, value).map_err(|e| {
            error!(shard_id = sid, error = %e, "RocksDB warm put failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    pub fn get_warm(&self, sid: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        let cf = self.warm_cf_handle(sid)?;
        Ok(self.db.get_cf(&cf, key)?)
    }
    pub fn delete_warm(&self, sid: usize, key: &[u8]) -> Result<(), RocksError> {
        let cf = self.warm_cf_handle(sid)?;
        self.db.delete_cf(&cf, key).map_err(|e| {
            error!(shard_id = sid, error = %e, "RocksDB warm delete failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    pub fn iter_warm_cf(&self, sid: usize) -> Result<RocksIterator<'_>, RocksError> {
        let cf = self.warm_cf_handle(sid)?;
        Ok(RocksIterator {
            inner: self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start),
        })
    }
    fn search_meta_cf_handle(
        &self,
        sid: usize,
    ) -> Result<StdArc<BoundColumnFamily<'_>>, RocksError> {
        if sid >= self.num_shards {
            return Err(RocksError::InvalidShardId(sid));
        }
        let n = &self.search_meta_cf_names[sid];
        self.db
            .cf_handle(n)
            .ok_or_else(|| RocksError::ColumnFamilyNotFound(n.clone()))
    }
    pub fn put_search_meta(&self, sid: usize, key: &[u8], value: &[u8]) -> Result<(), RocksError> {
        let cf = self.search_meta_cf_handle(sid)?;
        self.db.put_cf(&cf, key, value).map_err(|e| {
            error!(shard_id = sid, error = %e, "search meta put failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    pub fn get_search_meta(&self, sid: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        let cf = self.search_meta_cf_handle(sid)?;
        Ok(self.db.get_cf(&cf, key)?)
    }
    pub fn delete_search_meta(&self, sid: usize, key: &[u8]) -> Result<(), RocksError> {
        let cf = self.search_meta_cf_handle(sid)?;
        self.db.delete_cf(&cf, key).map_err(|e| {
            error!(shard_id = sid, error = %e, "search meta delete failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    pub fn iter_search_meta(&self, sid: usize) -> Result<RocksIterator<'_>, RocksError> {
        let cf = self.search_meta_cf_handle(sid)?;
        Ok(RocksIterator {
            inner: self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start),
        })
    }
}
