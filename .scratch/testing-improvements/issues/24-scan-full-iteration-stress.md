# SCAN full-iteration guarantee only tested against a static, non-resizing table

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: basic-commands

## Context

FrogDB's SCAN cursor uses a content-hash-based cursor to guard Redis's "full iteration" guarantee (keys present for the entire scan duration are returned at least once, even across table resizes) — implemented in `frogdb-server/crates/core/src/store/hashmap.rs:1044-1085` (design rationale documented in the comment at :1044-1052). The only stress-style coverage is `tcl_scan_guarantees_under_write_load` (`frogdb-server/crates/redis-regression/tests/scan_tcl.rs:507-538`), which writes the *same* 10 keys idempotently in a loop — no key-count growth, no table resize/rehash is ever triggered, and the table stays around ~110 keys. A regression to a naive positional cursor (which breaks the full-iteration guarantee exactly when the table resizes mid-scan) would pass this test undetected. There is also no unit test that deliberately forces resizes mid-iteration, and the "collision at COUNT boundary" edge case noted in the hashmap design comment as a "vanishingly rare skip" has no test pinning it either way.

Per verdict: the hashmap test module (`frogdb-server/crates/core/src/store/hashmap.rs` test mod) has only `test_store_*` tests, and `scan_regression.rs`'s full-iteration tests do zero concurrent writes — both corroborate the gap.

## What to build

- Unit test in `hashmap.rs`: populate ~5000 keys, begin iteration, insert ~50,000 additional distinct keys between SCAN batches to force multiple resizes, assert all original N keys are returned at least once by the end of the scan.
- Property test (proptest): random interleavings of SCAN batches with concurrent inserts/deletes; assert the invariant "any key present for the entire scan duration is in the union of returned keys" holds across many randomized schedules.
- Strengthen the TCL-level stress test (`scan_tcl.rs:507-538`) to use distinct (growing) keys rather than idempotent rewrites of the same 10 keys, so it actually exercises resize-under-scan at the integration level too.

## Acceptance criteria

- [ ] New `hashmap.rs` unit test forces ≥1 resize mid-iteration via bulk insert between SCAN batches and asserts full-iteration guarantee holds.
- [ ] Proptest covers randomized insert/delete/scan-batch interleavings and asserts the present-throughout ⊆ returned invariant.
- [ ] `tcl_scan_guarantees_under_write_load` (or a new sibling test) uses distinct/growing keys sufficient to trigger a real resize during the TCL-level scan.
- [ ] Reverting the content-hash cursor to a naive positional cursor (by hand, for verification) makes at least one of the new tests fail.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/core/src/store/hashmap.rs:1044-1085` (content-hash cursor, full-iteration guarantee design)
- `frogdb-server/crates/redis-regression/tests/scan_tcl.rs:507-538` (`tcl_scan_guarantees_under_write_load`, weak — same 10 keys, no resize)
