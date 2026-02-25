# unit/latency-monitor — 1 error

## Category
CONFIG RESETSTAT not implemented.

## Failure

### LATENCY HISTOGRAM after CONFIG RESETSTAT
- **Expected:** After `CONFIG RESETSTAT`, `LATENCY HISTOGRAM` returns empty/reset histograms
- **Actual:** CONFIG RESETSTAT is a no-op (returns OK but doesn't reset anything), so latency histograms retain old data and the assertion fails
- **Root cause:** `crates/server/src/connection/handlers/admin.rs:154` — CONFIG RESETSTAT returns `Response::ok()` without performing any reset
- **Fix:** Implement CONFIG RESETSTAT to reset latency histograms (and other stat counters)

## Source Files

| File | What to change |
|------|----------------|
| `crates/server/src/connection/handlers/admin.rs:154` | CONFIG RESETSTAT — implement the actual reset |
| `crates/core/src/metrics.rs` | Add `reset_stats()` method to metrics/latency tracking |

## Recommended Fix
Implement CONFIG RESETSTAT. This is shared with `unit/lazyfree` — see [lazyfree.md](lazyfree.md).

The reset should clear:
- Latency histogram data
- Peak memory counters
- Command statistics
- Slow log entries

## Cross-Suite Dependencies
- Shared fix with `unit/lazyfree` (2 of its 5 errors are caused by CONFIG RESETSTAT being a no-op)
