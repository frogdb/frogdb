# Decorative tests in the replication integration suite

Status: done
Type: bug (test quality)
Severity: likelihood 3/3 (they pass on every run today), consequence 2/3 (eleven named tests
covering read-only enforcement, propagation, partial-sync ordering and WAIT assert nothing that can
fail, so the suite reports coverage it does not have) — score 6
Area: replication / tests

## Problem

`frogdb-server/crates/server/tests/integration_replication.rs` (7818 lines) contains 78 `eprintln!`
calls, most of them the `else` half of a conditional assertion. The pattern is a test that observes
an outcome, and if the outcome is wrong, prints a note and passes:

```rust
/// Test that writes to replica are rejected (read-only replica).
/// Note: This behavior may not be implemented yet.        // :1014
async fn test_replica_read_only(...)
```

```rust
/// NOTE: The replica may accept writes if the replication handshake hasn't
/// completed yet (it doesn't know it's a replica). We verify the expected
/// behavior when possible.                                 // :3876-3878
async fn test_replica_readonly_error()
```

"We verify the expected behavior when possible" is a test that cannot fail. Both of these name the
read-only guarantee — one of the load-bearing replica invariants (spec row FM-REPLICATION-026,
"a replica refuses writes by flag not by list") — and neither pins it.

Sites found by the Phase 3 survey, all in this file:

| Line | Test | What is unpinned |
|------|------|------------------|
| 282  | `test_write_propagation` | propagation asserted only if it already happened |
| 484  | `test_wait_blocks_until_ack` | `WAIT` return value not bound |
| 943  | `test_large_value_replication` | large-payload arrival conditional |
| 1019 | `test_replica_read_only` | read-only refusal conditional |
| 1833 | `test_partial_sync_preserves_ordering` | ordering asserted only when observed |
| 2581 | `test_replica_reconnect_within_buffer` | `+CONTINUE` not required |
| 2827 | `test_fullresync_data_integrity` | integrity conditional |
| 3199 | `test_wait_during_replica_resync` | `WAIT` behaviour during resync not bound |
| 3353 | `test_fullresync_interrupted_resume` | resume conditional |
| 3880 | `test_replica_readonly_error` | read-only refusal conditional |
| 4295 | `test_replica_repl_offset_catches_up_to_primary` | catch-up conditional |

This is why several of the bugs filed in this batch survived: the behaviour these tests are named
for is exactly the behaviour they decline to require.

Some of the conditionals are guarding a real race — a replica that has not finished its handshake
genuinely does not yet know it is a replica. The fix for those is to *wait for the precondition*
and then assert unconditionally, not to make the assertion optional.

## Candidate fix

Per test, in this order:

1. If the conditional guards a timing race, add an explicit wait for the precondition (the harness
   already has `wait_for_replica_online`-style helpers) and then assert unconditionally.
2. If the conditional guards behaviour that genuinely is not implemented, delete the test body and
   `#[ignore]` it with a pointer to the issue that tracks the gap — an ignored test is honest, a
   passing empty one is not.
3. If the outcome is legitimately non-deterministic, assert the invariant that *is* deterministic
   (e.g. "the offset never goes backwards" rather than "the offset equals N").

Then add a lint or a review checklist item: an `eprintln!` in an integration test is a smell worth
grepping for. 78 in one file is not something a reviewer will catch case by case.

## Forcing tests

This issue *is* the tests. Verification is that each of the eleven fails when the behaviour it
names is broken — check by reverting the relevant guard (e.g. flipping the replica read-only flag)
and confirming the test goes red. The spec rows these tests are cited under (FM-REPLICATION-013,
-023, -026, -027, and the WAIT rows) should not list a decorative test in `Forced by` until it has
been repaired.

## Resolution

Done. `frogdb-server/crates/server/tests/integration_replication.rs` rewritten in place
(+665/-925, one file): every conditional assertion in the eleven named tests, and in the rest of the
file's `eprintln!` sites, now either waits for the precondition and asserts unconditionally, or
asserts the deterministic invariant. **No test was `#[ignore]`d and none was renamed** — candidate
fix step 2 was never needed, because every behaviour a decorative test declined to require is either
now required by it or already pinned by a sibling test.

Repaired (the eleven): `test_write_propagation`, `test_wait_blocks_until_ack`,
`test_large_value_replication`, `test_replica_read_only`, `test_partial_sync_preserves_ordering`,
`test_replica_reconnect_within_buffer`, `test_fullresync_data_integrity`,
`test_wait_during_replica_resync`, `test_fullresync_interrupted_resume`,
`test_replica_readonly_error`, `test_replica_repl_offset_catches_up_to_primary`.

Swept beyond the eleven: `test_wal_overflow_triggers_full_resync`, `test_wait_multiple_replicas`,
`test_promoted_replica_serves_all_writes_after_promotion`, the
`test_{list,hash,set,sorted_set}_operations_replicate` family, `test_incr_decr_replicates`,
`test_stream_xadd_replicates`, `test_multiple_replicas_same_primary`,
`test_wait_returns_correct_count_with_partial_ack`, `test_replica_handles_rapid_reconnect`.
Three helpers added (`assert_batch_replicated`, `slave_offset`, `bulk_strings`); no `sleep` was
added — all waiting goes through the harness's existing poll-until-deadline helpers.

**One deliberate exception to "assert unconditionally".** `test_replica_reconnect_within_buffer`
still does not require `+CONTINUE` on the wire: the replica in that test restarts on a fresh data
dir, so it can only ever send `PSYNC ? -1`. The `+CONTINUE` arm is pinned by
`test_partial_resync_after_brief_disconnect_grants_continue`, which does keep its state.

### Red proofs

Each repaired guarantee was verified by breaking production code and confirming the test goes red
(all edits reverted afterwards):

- `guards.rs` READONLY guard -> `if false && …`: `test_replica_read_only` (both cases) and
  `test_replica_readonly_error` -> 3/3 FAIL.
- `info/mod.rs` `offset: replica.acked_offset.min(1)`: `test_replica_repl_offset_catches_up_to_primary`
  (both cases) FAIL.
- `tracker.rs` `count_acked` -> `* 0`: `test_wait_blocks_until_ack` (both cases,
  `left: Some(0) right: Some(1)`) and `test_wait_multiple_replicas` (`left: 0 right: 2`) FAIL.

### Two findings the repair produced

1. **A real bug, filed as issue 28**: `WAIT` counts a replica that is still installing its
   full-resync checkpoint — the primary renders it `state=online` at the current offset while the
   replica itself reports `master_link_status:down`, `master_repl_offset:0` and answers `nil`. Not
   fixed here (it is production code, not test code). `test_wait_during_replica_resync` carries a
   comment stating the exact claim it deliberately does not assert; that comment is issue 28's
   acceptance test.
2. **Four tests were green only because their writes were being refused.** With honest assertions
   they failed `-CLUSTERDOWN The cluster is down (quorum lost, writes rejected)`:
   `replication.self-fence-on-replica-loss` rejects writes issued while the replica is down, which
   is the premise of those tests. Fixed in-test with `replication_self_fence_on_replica_loss:
   Some(false)` plus a comment, in `test_replica_reconnect_within_buffer`,
   `test_replica_reconnect_outside_buffer`, `test_wal_overflow_triggers_full_resync`,
   `test_wait_during_replica_resync` and `test_fullresync_interrupted_resume`. Self-fence itself
   stays pinned by `test_self_fence_engages_on_replica_loss`.

### Spec reconciliation

This issue said a repaired test may be cited in `Forced by`, and not before. Four now are, each with
a `// FM-REPLICATION-NNN` tag line added above it:

- `test_replica_read_only`, `test_replica_readonly_error` -> FM-REPLICATION-028
- `test_wait_blocks_until_ack` -> FM-REPLICATION-037
- `test_replica_repl_offset_catches_up_to_primary` -> FM-REPLICATION-043

The other seven are load-bearing now but are left uncited: each duplicates coverage a row already
names, and citing them would inflate `Forced by` without sharpening a row.
`just lint-failure-modes`: 164 failure modes, 826 test references, 826 tags.

The `eprintln!`-in-an-integration-test lint suggested at the end of "Candidate fix" was **not**
added — deliberately. `eprintln!` is legitimate in a harness (the test servers log through it), so a
blanket grep-lint would fire on non-assertions; the durable guard is that the pattern is now absent
from this file and named in this issue.

### Verification

- `just test frogdb-server integration_replication` -> 192 passed, 0 failed, 1750 skipped
- `cargo nextest run -p frogdb-server --features cmd-stream -E 'test(/integration_replication::test_stream_xadd_replicates/)'` -> 2 passed
- `just check frogdb-server`, `cargo clippy -p frogdb-server --all-targets -- -D warnings`, `just fmt-check` -> clean
