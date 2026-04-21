# 3. INFO Keysizes & Key-Memory-Stats (38 tests — `info_keysizes_tcl.rs`)

**Status**: Not implemented
**Source files**: `frogdb-server/crates/redis-regression/tests/info_keysizes_tcl.rs`
**Upstream**: Redis 8.6.0 `unit/info-keysizes.tcl`

## Architecture

### Histogram Data Structure

Use a fixed-size array of 64 `u64` counters, one per power-of-2 bin. Bin `i` counts values
with logical size in the range `[2^i, 2^(i+1) - 1]`. Bin 0 is the special case for size 0
(empty collections). This matches the Dragonfly/Redis approach.

```rust
/// Fixed 64-bin power-of-2 histogram.
/// Bin i counts entries with logical_size in [2^i, 2^(i+1) - 1].
/// Bin 0 counts entries with logical_size == 0 (empty containers).
#[derive(Debug, Clone, Default)]
pub struct PowerOfTwoHistogram {
    bins: [u64; 64],
}

impl PowerOfTwoHistogram {
    /// Determine the bin index for a given size value.
    /// size=0 -> bin 0, size=1 -> bin 0, size=2 -> bin 1, size=3 -> bin 1,
    /// size=4 -> bin 2, etc.
    fn bin_for(size: usize) -> usize {
        if size <= 1 { 0 } else { (usize::BITS - (size - 1).leading_zeros()) as usize }
    }

    pub fn increment(&mut self, size: usize) { self.bins[Self::bin_for(size)] += 1; }
    pub fn decrement(&mut self, size: usize) { self.bins[Self::bin_for(size)] = self.bins[Self::bin_for(size)].saturating_sub(1); }
    pub fn migrate(&mut self, old_size: usize, new_size: usize) {
        let old_bin = Self::bin_for(old_size);
        let new_bin = Self::bin_for(new_size);
        if old_bin != new_bin {
            self.bins[old_bin] = self.bins[old_bin].saturating_sub(1);
            self.bins[new_bin] += 1;
        }
    }
    pub fn clear(&mut self) { self.bins = [0u64; 64]; }
    pub fn is_empty(&self) -> bool { self.bins.iter().all(|&c| c == 0) }
}
```

### "Logical Size" per Type (KEYSIZES)

The KEYSIZES histogram tracks the **element count** (not byte size) for collection types, and
**byte length** for strings:

| Type | Logical size metric | Method |
|------|-------------------|--------|
| String | byte length | `StringValue::len()` |
| List | element count | `ListValue::len()` |
| Set | cardinality | `SetValue::len()` |
| Hash | field count | `HashValue::len()` |
| SortedSet | cardinality | `SortedSetValue::len()` |
| Stream | entry count | `StreamValue::len()` |
| HyperLogLog | estimated cardinality (rounded) | `HyperLogLogValue::count()` |

### "Memory Size" per Key (KEY-MEMORY-STATS)

The KEY-MEMORY-STATS histogram tracks the **total memory** of each key (value + key bytes +
metadata overhead), using `Entry::memory_size(key)` which already exists. Same power-of-2
bin structure.

### Per-Shard vs Global

Both histograms live **per-shard** inside `HashMapStore`. This avoids any cross-shard
synchronization. When `INFO keysizes` is requested via scatter-gather, each shard contributes
its local histogram and the connection layer merges (element-wise add) before formatting.

Memory overhead: 2 histograms x 7 types x 64 bins x 8 bytes = ~7 KB per shard for KEYSIZES,
plus 1 histogram x 64 bins x 8 bytes = 512 bytes per shard for KEY-MEMORY-STATS. Negligible.

## Implementation Steps

### Step 1: Histogram Struct (`frogdb-server/crates/types/src/histogram.rs`)

Create `PowerOfTwoHistogram` as described above. Expose `bin_for`, `increment`, `decrement`,
`migrate`, `clear`, `is_empty`, `format_info_line`. The `format_info_line` method renders
the Redis-compatible format:

```
distrib_strings_sizes:0=0,1=5,2=3,...
```

Only bins with non-zero counts are emitted. Bin labels use human-readable power-of-2 format:
`0`, `2`, `4`, `8`, ..., `1K`, `2K`, ..., `1M`, `2M`, etc.

### Step 2: Add Histograms to Store (`frogdb-server/crates/core/src/store/hashmap.rs`)

Add fields to `HashMapStore`:

```rust
/// Per-type keysize histograms (element count distribution).
keysize_histograms: EnumMap<KeyType, PowerOfTwoHistogram>,
/// Key memory histogram (total allocation size distribution).
/// None when key-memory-histograms is disabled.
memory_histogram: Option<PowerOfTwoHistogram>,
```

Use an array or `enum_map` keyed by `KeyType` (7 trackable variants: String, List, Set,
Hash, SortedSet, Stream, HyperLogLog). Other types (BloomFilter, Json, etc.) can be grouped
under a catch-all or omitted initially (Redis only tracks the 7 core types).

### Step 3: Track Logical Size (`frogdb-server/crates/types/src/types/mod.rs`)

Add a `Value::logical_size() -> usize` method that returns the element count / byte length
depending on type. This delegates to existing `len()` methods:

```rust
impl Value {
    pub fn logical_size(&self) -> usize {
        match self {
            Value::String(s) => s.len(),
            Value::List(l) => l.len(),
            Value::Set(s) => s.len(),
            Value::Hash(h) => h.len(),
            Value::SortedSet(z) => z.len(),
            Value::Stream(st) => st.len(),
            Value::HyperLogLog(hll) => hll.count() as usize,
            // Non-histogram types
            _ => 0,
        }
    }
}
```

### Step 4: Histogram Update Hooks (`frogdb-server/crates/core/src/store/hashmap.rs`)

Instrument the following `Store` trait method implementations in `HashMapStore`:

| Operation | Keysizes action | Memory histogram action |
|-----------|----------------|------------------------|
| `set()` | If overwriting: `migrate(old_logical_size, new_logical_size)` for old type; if new type differs, `decrement` old + `increment` new. If new key: `increment(new_logical_size)` | Same pattern with `entry.memory_size()` |
| `delete()` | `decrement(logical_size)` for the key's type | `decrement(memory_size)` |
| `clear()` | Call `clear()` on all histograms | Call `clear()` |
| `restore_entry()` | `increment(logical_size)` | `increment(memory_size)` |
| `get_mut()` return site | Caller must call a new `notify_size_change(key, old_size, new_size)` after mutation | Same |
| `check_and_delete_expired()` | `decrement(logical_size)` | `decrement(memory_size)` |

For `get_mut()` updates (in-place list pushes, hash sets, etc.): add a helper method
`HashMapStore::update_histograms_for_mutation(key, old_logical_size, new_logical_size, old_mem, new_mem)`
that commands call after modifying a value in place. This is called from the shard execution
layer after `get_mut()`-based write commands complete.

### Step 5: RENAME Support

In `RenameCommand::execute()` (or a post-rename hook in the store), the histogram only needs
to handle the **key change** — since RENAME preserves the value, the logical size bin stays
the same. If the destination key existed and is being overwritten, its old entry must be
decremented first. The current RENAME implementation already calls `delete(old)` +
`set(new)`, so the hooks from Step 4 handle this automatically.

### Step 6: INFO Section (`frogdb-server/crates/server/src/commands/info.rs`)

Add `b"keysizes"` to `EXTRA_SECTIONS` (not default — matches Redis behavior where it
requires explicit request or `INFO all`).

Add `build_keysizes_info(ctx)`:

```rust
fn build_keysizes_info(ctx: &mut CommandContext) -> String {
    let mut info = "# Keysizes\r\n".to_string();
    // Format: distrib_<type>_sizes:<bin>=<count>,...
    // Only emit lines where histogram is non-empty
    for (type_name, histogram) in ctx.store.keysize_histograms() {
        if !histogram.is_empty() {
            info.push_str(&format!("distrib_{}_sizes:{}\r\n", type_name, histogram.format_bins()));
        }
    }
    // KEY-MEMORY-STATS lines (if enabled)
    if let Some(mem_hist) = ctx.store.memory_histogram() {
        if !mem_hist.is_empty() {
            info.push_str(&format!("distrib_key_memory_sizes:{}\r\n", mem_hist.format_bins()));
        }
    }
    info.push_str("\r\n");
    info
}
```

Because INFO runs per-shard in FrogDB (scatter-gather), the scatter strategy for this
section must aggregate histograms from all shards. Add the `keysizes` section to the
scatter-merge logic in `frogdb-server/crates/server/src/scatter/strategies.rs`.

### Step 7: Config Toggle (`frogdb-server/crates/config/src/`)

Add `key-memory-histograms` boolean to `MemoryConfig` (or a new `ObservabilityConfig`):

```rust
// In memory.rs or a new observability.rs
/// Whether to track per-key memory allocation histograms.
/// Can be disabled at startup. Cannot be re-enabled after runtime disable.
#[serde(default = "default_true")]
pub key_memory_histograms: bool,
```

Add the corresponding `ConfigParamInfo` entry in `params.rs` with `mutable: true`. The
runtime CONFIG SET handler must enforce:
- Cannot enable at runtime if disabled at startup (return error)
- Can disable at runtime (sets `memory_histogram` to `None`)
- Cannot re-enable after runtime disable (return error)

Track startup state with a flag in `HashMapStore`:
```rust
/// Whether key-memory-histograms was enabled at startup.
memory_histogram_enabled_at_startup: bool,
/// Whether it has been disabled at runtime (blocks re-enable).
memory_histogram_disabled_at_runtime: bool,
```

### Step 8: DEBUG Subcommands (`frogdb-server/crates/commands/src/generic.rs`)

Add two new arms to the `DebugCommand::execute()` match:

**`DEBUG KEYSIZES-HIST-ASSERT <type> <bin> <expected_count>`**:
Assert that the histogram bin for a given type has the expected count. Returns `+OK` on
success, `-ERR` with details on failure. Used by tests to verify histogram correctness
without parsing INFO output.

**`DEBUG ALLOCSIZE-SLOTS-ASSERT <slot> <expected_size>`**:
Assert the total memory allocated for keys in a given hash slot. This requires summing
`Entry::memory_size()` for all keys that hash to the specified slot. Returns `+OK` or
`-ERR`.

Both subcommands use `ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)` which
is already the strategy for `DebugCommand`.

### Step 9: Scatter-Gather for INFO keysizes

The `INFO keysizes` section needs histogram data from all shards. Options:

1. **Option A (preferred)**: Add a new `ScatterOp::InfoKeysizes` variant that each shard
   responds to with its serialized histograms. The connection handler merges them.
2. **Option B**: Use existing `ScatterOp::Info` with section-aware merge logic.

Since INFO already has scatter-gather support, extend the existing merge to element-wise
add histogram bins when the `keysizes` section is requested.

## Integration Points

### Store Operations Requiring Histogram Updates

All updates happen within `HashMapStore` (single-threaded per shard, no locking needed):

1. **`set()`** — new key or overwrite (line ~451 in hashmap.rs)
2. **`delete()`** — key removal (line ~492)
3. **`clear()`** — FLUSHDB/FLUSHALL (line ~596)
4. **`restore_entry()`** — recovery and RESTORE command (line ~171)
5. **`check_and_delete_expired()`** — lazy expiry (line ~245)
6. **Active expiry** — calls `delete()` externally via shard expiry loop
7. **In-place mutations** — `get_mut()` callers (list push, hash set, etc.)

For in-place mutations (7), the shard execution layer must capture `logical_size()` before
the command executes, then compare after. This can be done by wrapping mutating commands:

```rust
// In shard execution, before calling handler.execute():
let pre_size = if is_write { handler.keys(args).iter().map(|k| store.logical_size_of(k)).collect() } else { vec![] };
// After execute:
// Compare and call migrate() for any changes
```

Alternatively, add a `Store::notify_mutation(key)` method that recalculates and updates the
histogram, called at the end of each write command in the post-execution pipeline.

## FrogDB Adaptations

### RocksDB Persistence and Histogram State

Histograms are **ephemeral in-memory state** — they are rebuilt during recovery:

- On startup, `restore_entry()` already receives every key/value. The histogram `increment()`
  calls during restore will rebuild the full histogram state.
- No need to persist histograms to RocksDB or WAL.
- After restart, histograms are fully consistent because every key passes through
  `restore_entry()`.

This is analogous to Redis's `DEBUG RELOAD` behavior (which resets and rebuilds histograms
from the RDB). FrogDB achieves the same effect naturally.

### Warm Tier (Tiered Storage)

For warm keys (value on disk, metadata in RAM):
- **KEYSIZES**: Warm entries still have their `key_type` cached in RAM. However, we do NOT
  have the logical size without promoting. Options:
  - Store `logical_size` in `Entry` alongside `key_type` (adds 8 bytes per key, acceptable).
  - Remove warm keys from the keysizes histogram on demotion, re-add on promotion.
  - **Recommended**: Store logical_size in Entry. It's already a hot field for histograms.
- **KEY-MEMORY-STATS**: Warm keys report 0 value bytes (only metadata in RAM). They should
  be tracked separately or excluded from the histogram.

### Replication

Replicas receive write commands via the replication stream and execute them locally. Because
histogram updates are integrated into `set()`/`delete()`/`clear()` inside `HashMapStore`,
replicas automatically maintain correct histograms as they process replicated writes.

No special replication handling is needed — the tests for "Replication updates key memory
stats on replica" will pass naturally because the replica's store operations trigger the
same histogram hooks.

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
- `KEYSIZES - Test RDB $suffixRepl` — **adapt**: test against RocksDB persistence (restart + verify histograms rebuilt from recovery)
- `KEYSIZES - DEBUG RELOAD reset keysizes $suffixRepl` — **adapt**: test against server restart (histograms rebuilt from RocksDB recovery, not reset to empty)

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
- `KEY-MEMORY-STATS - DEBUG RELOAD preserves key memory histogram` — **adapt**: test against server restart (histograms rebuilt from recovery)
- `KEY-MEMORY-STATS - Replication updates key memory stats on replica` — **adapt**: test against FrogDB replication
- `KEY-MEMORY-STATS - DEL on primary updates key memory stats on replica` — **adapt**: test against FrogDB replication

### SLOT-ALLOCSIZE (1 test)

- `SLOT-ALLOCSIZE - Test DEBUG ALLOCSIZE-SLOTS-ASSERT command`

## Verification

### Unit Tests (in `frogdb-types`)

- `PowerOfTwoHistogram::bin_for()` correctness for edge cases (0, 1, 2, 3, 4, 2^63)
- `increment` / `decrement` / `migrate` / `clear`
- `format_bins()` output format matches Redis

### Integration Tests (in `frogdb-server`)

- Create keys of each type, verify `INFO keysizes` shows correct bins
- Delete keys, verify counts decrement
- FLUSHALL clears all histograms
- RENAME preserves histogram consistency
- RESTORE populates histogram
- Expire removes from histogram (wait for expiry)
- Server restart + recovery rebuilds histograms
- CONFIG SET key-memory-histograms toggle behavior (disable/cannot-re-enable)
- DEBUG KEYSIZES-HIST-ASSERT returns OK for correct assertions
- Multi-shard scatter-gather produces merged histogram

### Regression Tests

Port the 38 tests from `info_keysizes_tcl.rs` exclusion list into actual `#[tokio::test]`
functions. Remove the corresponding exclusion comments as tests are implemented.
