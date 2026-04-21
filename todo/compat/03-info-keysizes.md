# 3. INFO Keysizes & Key-Memory-Stats (38 tests — `info_keysizes_tcl.rs`)

**Status**: Not implemented
**Scope**: Implement `INFO keysizes` section with per-type size histograms, key-memory-histograms,
and DEBUG assertion commands.

## Work

- Track key size distribution per data type (power-of-2 bins)
- Track key memory allocation distribution (power-of-2 bins)
- Add `key-memory-histograms` config toggle
- Implement `DEBUG KEYSIZES-HIST-ASSERT` and `DEBUG ALLOCSIZE-SLOTS-ASSERT`
- Update histograms on key create/modify/delete/expire/rename/restore
- Support runtime enable/disable with restriction on re-enable

**Key files to modify**: `commands/info.rs`, `core/src/shard/`, config module

## Tests

### KEYSIZES (16 tests)

- `KEYSIZES - Test i'th bin counts keysizes between (2^i) and (2^(i+1)-1) as expected $suffixRepl`
- `KEYSIZES - Histogram values of Bytes, Kilo and Mega $suffixRepl`
- `KEYSIZES - Test hyperloglog $suffixRepl`
- `KEYSIZES - Test List $suffixRepl`
- `KEYSIZES - Test SET $suffixRepl`
- `KEYSIZES - Test ZSET $suffixRepl`
- `KEYSIZES - Test STRING $suffixRepl`
- `KEYSIZES - Test STRING BITS $suffixRepl`
- `KEYSIZES - Test complex dataset $suffixRepl`
- `KEYSIZES - Test HASH ($type) $suffixRepl`
- `KEYSIZES - Test Hash field lazy expiration ($type) $suffixRepl`
- `KEYSIZES - Test RESTORE $suffixRepl`
- `KEYSIZES - Test RENAME $suffixRepl`
- `KEYSIZES - Test DEBUG KEYSIZES-HIST-ASSERT command`
- `KEYSIZES - Test RDB $suffixRepl` — **adapt**: test against RocksDB persistence
- `KEYSIZES - DEBUG RELOAD reset keysizes $suffixRepl` — **adapt**: test against server restart

### KEY-MEMORY-STATS (21 tests)

- `KEY-MEMORY-STATS - Empty database should have empty key memory histogram`
- `KEY-MEMORY-STATS - key memory histogram should appear`
- `KEY-MEMORY-STATS - List keys should appear in key memory histogram`
- `KEY-MEMORY-STATS - All data types should appear in key memory histogram`
- `KEY-MEMORY-STATS - Histogram bins should use power-of-2 labels`
- `KEY-MEMORY-STATS - DEL should remove key from key memory histogram`
- `KEY-MEMORY-STATS - Modifying a list should update key memory histogram`
- `KEY-MEMORY-STATS - FLUSHALL clears key memory histogram`
- `KEY-MEMORY-STATS - Larger allocations go to higher bins`
- `KEY-MEMORY-STATS - EXPIRE eventually removes from histogram`
- `KEY-MEMORY-STATS - Test RESTORE adds to histogram`
- `KEY-MEMORY-STATS - RENAME should preserve key memory histogram`
- `KEY-MEMORY-STATS - Hash field lazy expiration ($type)`
- `KEY-MEMORY-STATS - Test DEBUG KEYSIZES-HIST-ASSERT command`
- `KEY-MEMORY-STATS disabled - key memory histogram should not appear`
- `KEY-MEMORY-STATS - cannot enable key-memory-histograms at runtime when disabled at startup`
- `KEY-MEMORY-STATS - can disable key-memory-histograms at runtime and distrib_*_sizes disappear`
- `KEY-MEMORY-STATS - cannot re-enable key-memory-histograms at runtime after disabling`
- `KEY-MEMORY-STATS - DEBUG RELOAD preserves key memory histogram` — **adapt**: test against server restart
- `KEY-MEMORY-STATS - Replication updates key memory stats on replica` — **adapt**: test against FrogDB replication
- `KEY-MEMORY-STATS - DEL on primary updates key memory stats on replica` — **adapt**: test against FrogDB replication

### SLOT-ALLOCSIZE (1 test)

- `SLOT-ALLOCSIZE - Test DEBUG ALLOCSIZE-SLOTS-ASSERT command`
