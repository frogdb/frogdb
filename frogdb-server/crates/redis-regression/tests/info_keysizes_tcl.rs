//! Rust port tracker for Redis 8.6.0 `unit/info-keysizes.tcl`.
//!
//! The upstream suite exercises Redis 8.6's keysize / key-memory
//! histogram machinery: `INFO keysizes` (which exposes
//! `distrib_strings_sizes`, `distrib_lists_items`, `distrib_sets_items`,
//! `distrib_zsets_items`, `distrib_hashes_items`), the
//! `DEBUG KEYSIZES-HIST-ASSERT` self-check subcommand, and the
//! `enable-key-memory-histograms` config flag. FrogDB does not
//! implement any of these:
//!
//! - `INFO keysizes` section is not emitted by FrogDB's `INFO` command.
//! - FrogDB's `DEBUG` handler supports `SLEEP`, `TRACING`, `STRUCTSIZE`,
//!   `HELP`, `VLL`, `PUBSUB`, `BUNDLE`, and `HASHING` only — no
//!   `KEYSIZES`, `KEYSIZES-HIST-ASSERT`, `ALLOCSIZE-SLOTS-ASSERT`, or
//!   `RELOAD`.
//! - FrogDB has no `enable-key-memory-histograms` config.
//! - FrogDB stores values in RocksDB, so power-of-2 per-key allocation
//!   bins are meaningless at the storage layer.
//!
//! This file exists so the audit (`audit_tcl.py`) knows the upstream
//! file is being tracked and can classify each of its 43 tests as
//! documented exclusions rather than unexplained gaps. When/if FrogDB
//! grows a keysize-histogram feature, tests should move out of this
//! exclusion list and into `#[tokio::test]` functions in this file.
//!
//! ## Intentional exclusions
//!
//! `INFO keysizes` histogram section (not implemented in FrogDB):
//! - `KEYSIZES - Test i'th bin counts keysizes between (2^i) and (2^(i+1)-1) as expected $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Histogram values of Bytes, Kilo and Mega $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test hyperloglog $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test List $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test SET $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test ZSET $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test STRING $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test complex dataset $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test DEBUG KEYSIZES-HIST-ASSERT command` — DEBUG KEYSIZES-HIST-ASSERT not implemented in FrogDB
//! - `KEYSIZES - Test HASH ($type) $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test Hash field lazy expiration ($type) $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test STRING BITS $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test RESTORE $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test RENAME $suffixRepl` — DEBUG KEYSIZES / INFO keysizes not implemented in FrogDB
//! - `KEYSIZES - Test MOVE $suffixRepl` — DEBUG KEYSIZES / INFO keysizes + MOVE (singledb) not implemented in FrogDB
//! - `KEYSIZES - Test SWAPDB $suffixRepl` — DEBUG KEYSIZES / INFO keysizes + SWAPDB (singledb) not implemented in FrogDB
//! - `KEYSIZES - DEBUG RELOAD reset keysizes $suffixRepl` — DEBUG RELOAD not implemented in FrogDB
//! - `KEYSIZES - Test RDB $suffixRepl` — DEBUG KEYSIZES / INFO keysizes + RDB not implemented in FrogDB
//!
//! `KEY-MEMORY-STATS` / `key-memory-histograms` config (not implemented
//! in FrogDB):
//! - `KEY-MEMORY-STATS - Empty database should have empty key memory histogram` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - List keys should appear in key memory histogram` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - All data types should appear in key memory histogram` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - Histogram bins should use power-of-2 labels` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - DEL should remove key from key memory histogram` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - Modifying a list should update key memory histogram` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - FLUSHALL clears key memory histogram` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - Larger allocations go to higher bins` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - EXPIRE eventually removes from histogram` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - Test RESTORE adds to histogram` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - DEBUG RELOAD preserves key memory histogram` — DEBUG RELOAD + key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - RENAME should preserve key memory histogram` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - Test DEBUG KEYSIZES-HIST-ASSERT command` — DEBUG KEYSIZES-HIST-ASSERT not implemented in FrogDB
//! - `KEY-MEMORY-STATS - RDB save and restart preserves key memory histogram` — RDB + key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - Hash field lazy expiration ($type)` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS disabled - key memory histogram should not appear` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - cannot enable key-memory-histograms at runtime when disabled at startup` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - can disable key-memory-histograms at runtime and distrib_*_sizes disappear` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - cannot re-enable key-memory-histograms at runtime after disabling` — key-memory-histograms not implemented in FrogDB
//! - `SLOT-ALLOCSIZE - Test DEBUG ALLOCSIZE-SLOTS-ASSERT command` — DEBUG ALLOCSIZE-SLOTS-ASSERT not implemented in FrogDB
//! - `KEY-MEMORY-STATS - key memory histogram should appear` — key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - Replication updates key memory stats on replica` — needs:repl — replication + key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - DEL on primary updates key memory stats on replica` — needs:repl — replication + key-memory-histograms not implemented in FrogDB
