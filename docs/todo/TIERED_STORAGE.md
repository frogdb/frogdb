# Two-Tier Storage Implementation Plan

See `docs/spec/TIERED.md` for the full design spec.

## Phase 1: ValueLocation Enum and Entry Restructuring
- [x] Add `ValueLocation` enum (`Hot(Arc<Value>)`, `Warm`)
- [x] Add `key_type: KeyType` to `Entry`, replace `value` with `location`
- [x] Add `Entry::is_hot()`, `Entry::hot_value()`
- [x] Update `entry_memory_size()` — `Warm` entries contribute zero value bytes
- [x] Update all `Entry` accessors: get, set, delete, key_type, get_mut, restore_entry, scan_filtered, etc.
- [x] Verify: `just test frogdb-core`

## Phase 2: Change `StorageOps::get()` to `&mut self`
- [x] `StorageOps::get(&mut self)` in traits.rs
- [x] `Store::get(&mut self)` in mod.rs
- [x] `HashMapStore::get(&mut self)` in hashmap.rs
- [x] Add `pub fn get_hot(&self)` for non-promoting reads
- [x] Update persistence.rs → `get_hot()`
- [x] Update diagnostics.rs → `get_hot()`
- [x] Update blocking.rs → `&mut self` or `get_hot()`
- [x] Verify: `just check`, `just test frogdb-core`

## Phase 3: RocksDB Warm Column Family Support
- [x] Add `warm_enabled`, `warm_cf_names` to `RocksStore`
- [x] Extend `RocksStore::open()` with `warm_enabled` param
- [x] Add warm-tier methods: `put_warm`, `get_warm`, `delete_warm`, `iter_warm_cf`, `warm_enabled()`
- [x] Unit tests
- [x] Verify: `just test frogdb-persistence`

## Phase 4: Eviction Policy Variants
- [x] Add `TieredLru`, `TieredLfu` to `EvictionPolicy`
- [x] Update `as_str`, `FromStr`, `Display`, `all_names`, helpers
- [x] Add `is_tiered()` helper
- [x] Update `uses_lru()` and `uses_lfu()` to include tiered variants
- [x] Update memory config validation
- [x] Verify: `just test frogdb-core`

## Phase 5: Demotion and Promotion in HashMapStore
- [x] Add warm store fields to `HashMapStore`
- [x] `set_warm_store()` constructor
- [x] `demote_key()` — serialize value, write to warm CF, replace with Warm marker
- [x] `promote_key()` — read from warm CF, deserialize, replace with Hot, TTL check
- [x] Update `get()` to promote warm values
- [x] Update `get_with_expiry_check()`, `get_mut()`, `set()`, `delete()`, `get_and_delete()`, `clear()`
- [x] Stat accessors: `warm_key_count()`, `hot_key_count()`, etc.
- [x] Verify: `just test frogdb-core`

## Phase 6: Eviction Integration — Demote Instead of Delete
- [x] Add `TieredLru`/`TieredLfu` match arms in `evict_one()`
- [x] `demote_lru()`, `demote_lfu()`
- [x] `demote_for_eviction()` with metrics
- [x] Filter warm entries in `sample_keys()` and `random_key()`
- [x] Verify: `just test frogdb-core`

## Phase 7: Configuration
- [x] Add `TieredStorageConfig` to config
- [x] Validation: tiered requires persistence, policy must match
- [x] Verify: `just test frogdb-server`

## Phase 8: Server Initialization Wiring
- [x] Pass `warm_enabled` to `RocksStore::open()`
- [x] Wire warm store to shard workers
- [x] Verify: `just build`, `just test frogdb-server`

## Phase 9: Recovery with Warm Tier
- [x] `recover_warm_shard()` — iterate warm CF, insert Warm entries
- [x] Extend `recover_all_shards()` — reconcile warm + persistence
- [x] Add warm stats to `RecoveryStats`
- [x] Verify: `just test frogdb-core`

## Phase 10: Metrics and INFO
- [x] Prometheus metrics via eviction.rs: `frogdb_tiered_demotions_total`, `frogdb_tiered_bytes_demoted_total`
- [x] INFO tiered section: hot_keys, warm_keys, promotions, demotions, expired_on_promote
- [x] Store trait tiered stat methods with default no-ops
- [x] Verify: `just test frogdb-server`

## Phase 11: End-to-End Testing
- [x] Full cycle: SET → demotion → GET → promotion
- [x] Expired warm key cleanup
- [x] SET/DEL on warm key
- [x] SCAN/EXISTS/TYPE for warm keys
- [x] Memory accounting
- [x] Recovery with warm entries
- [x] FLUSHDB clears warm tier
- [x] 16 integration tests in `crates/core/tests/tiered_storage.rs`
