# Decorative tests in the replication integration suite

Status: needs-triage
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
