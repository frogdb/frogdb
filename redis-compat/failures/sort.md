# unit/sort — 24 errors

## Category
Missing SORT features and edge case handling.

## Failures

### SORT GET with hash field pattern (`->`)
- **Expected:** When a hash key or field doesn't exist, SORT GET `key_*->field` returns nil (null bulk string)
- **Actual:** FrogDB returns the raw element value instead of nil for missing hash lookups
- **Root cause:** `resolve_pattern()` in `sort.rs` returns `None` correctly for missing keys, but the STORE path substitutes empty strings instead of preserving nil semantics

### SORT_RO returns wrong result type
- **Expected:** SORT_RO returns an array of sorted elements
- **Actual:** Returns the raw value instead of applying the sort
- **Root cause:** SORT_RO delegates to `execute_sort()` which works correctly for SORT, but there may be an issue with how the read-only path handles the response

### SORT BY nosort with DESC on sorted sets
- **Expected:** `SORT key BY nosort DESC` returns elements in reverse insertion order for sorted sets (score-descending)
- **Actual:** DESC flag is ignored when NoSort is active because `SortKey::cmp()` returns `Ordering::Equal` for all NoSort comparisons, and the tiebreaker uses original index ascending
- **Fix:** When by_pattern is "nosort" and `ascending` is false, reverse the element order after extraction

### SORT BY constant with STORE
- **Expected:** `SORT key BY <non-matching-pattern> STORE dest` preserves natural order (original insertion order)
- **Actual:** When the BY pattern doesn't contain `*`, it acts like nosort but the ordering semantics differ from Redis
- **Fix:** Detect constant BY patterns (no `*`) and treat as nosort

### SORT error message format
- **Expected:** `ERR One or more scores can't be converted into double` (Redis format)
- **Actual:** `ERR value is not a valid float` (FrogDB's `CommandError::NotFloat` message)
- **Fix:** Change the error message in `CommandError::NotFloat` or add a SORT-specific error variant

### SORT BY sub-sort lexicographic tiebreaking
- **Expected:** When two elements have equal sort keys, Redis uses lexicographic comparison of the elements themselves as a tiebreaker
- **Actual:** FrogDB uses insertion order (index) as the tiebreaker
- **Fix:** Add element-value comparison as secondary sort key in the `sort_by` closure

### SORT speed benchmark timeout (1 error)
- **Expected:** SORT of 10000 elements completes within timeout
- **Actual:** Timeout during speed benchmark test
- **Note:** May be environment-dependent; not a correctness issue

## Source Files

| File | What to change |
|------|----------------|
| `crates/commands/src/sort.rs` | All SORT/SORT_RO logic |
| `crates/commands/src/sort.rs:231-255` | `SortKey::cmp()` — fix NoSort+DESC, add lex tiebreaking |
| `crates/commands/src/sort.rs:180-217` | `compute_sort_key()` — detect constant BY patterns |
| `crates/commands/src/sort.rs:130-176` | `resolve_pattern()` — verify nil return for missing hash fields |

## Recommended Fix Order
1. Fix error message format (quick, 1 error)
2. Fix NoSort+DESC ordering (3-5 errors)
3. Add lexicographic tiebreaking (2-3 errors)
4. Fix constant BY pattern detection (2-3 errors)
5. Verify SORT_RO end-to-end behavior (remaining errors)

## Cross-Suite Dependencies
None.
