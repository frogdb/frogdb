# unit/lazyfree — 5 errors

## Category
UNLINK / async deletion: memory tracking and CONFIG RESETSTAT.

## Failures

### used_memory INFO field reports 0 (~3 errors)
- **Expected:** `INFO memory` reports non-zero `used_memory`, `used_memory_peak`, etc.
- **Actual:** `used_memory` is always 0 because FrogDB doesn't track memory usage
- **Root cause:** Memory usage tracking is not implemented — the INFO memory section returns placeholder values
- **Cascade:** Tests that create large keys, delete them with UNLINK, then check peak memory vs current memory fail because both values are 0
- **Fix:** Implement basic memory usage tracking (at minimum, track approximate memory for string/list/set/hash/zset values)

### CONFIG RESETSTAT not implemented (~2 errors, cascading)
- **Expected:** `CONFIG RESETSTAT` resets peak memory, command stats, and other counters
- **Actual:** Returns OK but doesn't reset anything (no-op at `admin.rs:154`)
- **Cascade:** Tests that: (1) set large keys, (2) check peak memory, (3) RESETSTAT, (4) check peak dropped — fail because RESETSTAT is a no-op
- **Fix:** Implement CONFIG RESETSTAT to reset:
  - Peak memory counters
  - Command statistics
  - Slow log
  - Latency histograms

## Source Files

| File | What to change |
|------|----------------|
| `crates/server/src/connection/handlers/admin.rs:154` | CONFIG RESETSTAT — implement instead of no-op |
| `crates/server/src/connection/handlers/info.rs` | INFO memory section — report actual memory usage |
| `crates/core/src/metrics.rs` | Memory tracking, stat reset methods |

## Recommended Fix Order
1. **Implement CONFIG RESETSTAT** (quick, also fixes `unit/latency-monitor`)
   - Reset peak memory, stats counters, latency histograms
2. Add memory tracking to INFO (3 errors — larger effort)
   - Even approximate tracking would fix these tests

## Cross-Suite Dependencies
- CONFIG RESETSTAT also fixes `unit/latency-monitor` (1 error) — see [latency-monitor.md](latency-monitor.md)
- Memory tracking would also benefit `unit/memefficiency` (currently passing, but may have edge cases)
