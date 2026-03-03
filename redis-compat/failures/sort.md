# unit/sort — 1 error (timeout)

## Category
SORT performance benchmark timeout.

## Failures

### SORT speed benchmark timeout (1 error)
- **Test:** "SORT speed, 100 element list BY hash field, 100 times"
- **Expected:** SORT of 100 elements by hash field completes within timeout (100 iterations)
- **Actual:** Timeout during speed benchmark test — 48 other tests all pass
- **Note:** May be environment-dependent; not a correctness issue. The BY-key variant (non-hash) passes within 11ms.

## Passed Tests (48)
All correctness tests pass, including:
- SORT BY key (Listpack, Quicklist, Big Quicklist, Intset, Hash table, Big Hash table)
- SORT BY hash field (all encodings)
- SORT GET (#, const, key+hash)
- SORT BY key/hash STORE
- SORT DESC, ALPHA, sorted set variants
- SORT BY nosort (retain ordering, +LIMIT, from scripts)
- SORT_RO (successful case, STORE rejection)
- Edge cases (floats, empty results, sub-sort tiebreaking, double validation)

## Source Files

| File | What to change |
|------|----------------|
| `crates/commands/src/sort.rs` | SORT BY hash field performance — investigate why hash-field lookups are slower than key lookups |

## Cross-Suite Dependencies
None.
