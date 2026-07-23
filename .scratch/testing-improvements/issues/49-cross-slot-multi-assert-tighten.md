# Tighten cross-slot MULTI/EXEC assertion to pin exact queue-time + EXECABORT contract

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: Transactions / Cluster

## Context

The only cluster cross-slot MULTI test,
`test_multi_exec_cross_slot_returns_error`
(`frogdb-server/crates/server/tests/integration_cluster.rs:7846-7884`), accepts an error on *any* of
MULTI, either queued SET, or EXEC — its own in-test comment admits "the exact behavior depends on
implementation." This means the assertion is too weak to catch a regression.

The actual, current behavior is well-defined: FrogDB aborts at **queue time**, via
`try_queue_in_transaction` → `validate_cluster_slots` → `abort_transaction`
(`frogdb-server/crates/server/src/connection/guards.rs:454-461`). Specifically: a same-slot command
queues normally (`+QUEUED`), the different-slot command is rejected immediately with a `CROSSSLOT`
error at queue time (not deferred to EXEC), and the transaction is marked aborted so `EXEC` returns
`EXECABORT`. None of this specificity is pinned by the existing test — a regression that silently
moved slot-crossing detection from queue-time to EXEC-time, or that let EXEC continue with a partial
transaction, would still pass the current test (verdict CONFIRMED L2/C1: correct today, but the test
can't catch a break).

## What to build

Tighten `test_multi_exec_cross_slot_returns_error` (or replace it with a stricter test) to assert the
exact, specific contract instead of "an error occurs somewhere."

## Acceptance criteria

- [ ] Test asserts the first (same-slot) queued command returns `+QUEUED` (proves the failure is
      specifically about slot-crossing detection, not blanket rejection of the whole transaction).
- [ ] Test asserts the *second* (different-slot) queued command is the one that errors, at queue
      time, not deferred to EXEC.
- [ ] Test asserts the exact `CROSSSLOT` error prefix/text (lock the real FrogDB string).
- [ ] Test asserts `EXEC` subsequently returns `EXECABORT` (not a partial execution, not success).

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/B-transactions.md #3 (`cross-slot-multi-weak-assertion`)
- .scratch/testing-improvements/audit/verdicts-B.md #3 (CONFIRMED L2/C1)
- frogdb-server/crates/server/tests/integration_cluster.rs:7846-7884
- frogdb-server/crates/server/src/connection/guards.rs:454-461

## Resolution

`test_multi_exec_cross_slot_returns_error` (`integration_cluster.rs`) tightened, and a second
test added, after empirically verifying FrogDB's actual runtime behavior with an instrumented
experiment (not inferred from reading the code — the code has *two* distinct cross-slot paths
and the issue's narrative only matches one of them).

**Correction to the issue's premise.** The issue assumed two *separate* single-key commands
(`SET a`, `SET b` on different slots) hit `try_queue_in_transaction` -> `validate_cluster_slots`
and queue-time-abort. Verified empirically that this is **not** what happens: `validate_cluster_slots`
only checks a command's *own* keys for slot homogeneity (`SlotValidator::same_slot`) plus this
node's ownership of that command's slot (MOVED/ASK) — it never compares against previously
queued commands. So two separate single-key `SET`s on different (both locally-owned) slots
**both queue with `+QUEUED`**, and the mismatch is only caught at EXEC when
`TransactionTarget::resolve()` sees a `Multi` target, returning a plain `CROSSSLOT` — `exec_abort`
is never set, so this is **not** `EXECABORT`.

The queue-time-abort + `EXECABORT` contract described in the issue (`guards.rs:454-461`) is real
and correctly documented, but it fires on a different trigger than assumed: a *single* command
whose own keys already span two slots (e.g. `RENAME src dst`, `SMOVE`, `LMOVE`, ...). That
command's `SlotValidator::same_slot` check fails on its own key set, `validate_cluster_slots`
returns the CROSSSLOT reply as *that command's own queue-time reply* (it never becomes
`+QUEUED`), `abort_transaction` sets `exec_abort`, and the subsequent EXEC returns EXECABORT
without touching the shard.

**What changed:**
- `test_multi_exec_cross_slot_returns_error` rewritten to exercise the queue-time path with
  deterministic key/slot selection (a new `owner_node_with_two_slots` helper connects directly to
  a node and picks two slots it actually owns, so any CROSSSLOT observed is the slot-mismatch
  check, never a MOVED redirect in disguise — the old test connected to `node_ids()[0]` blind and
  accepted "any error"). It now asserts, in order: `MULTI` -> `OK`; a same-slot `SET` -> exact
  `+QUEUED`; a cross-slot `RENAME` -> exact `CROSSSLOT Keys in request don't hash to the same
  slot` **as its own queue reply** (not `QUEUED`); `EXEC` -> exact
  `EXECABORT Transaction discarded because of previous errors.`; the previously-queued `SET`
  never landed (`GET` -> nil, no partial commit); and that `DISCARD` after a queue-time abort
  clears connection state cleanly (a fresh `MULTI`/queue/`EXEC` right after behaves normally,
  with no lingering abort flag).
- New test `test_multi_exec_two_single_key_commands_different_slots_defers_crossslot_to_exec`
  pins the *other*, previously-untested cluster-mode path: two separate single-key commands on
  different slots both get exact `+QUEUED`, `EXEC` returns exact `CROSSSLOT` (asserted `!=` the
  `EXECABORT` string to nail down that this is *not* an abort), and neither write lands.
- Both tests assert exact error text (not `starts_with`/`is_error`), matching
  `frogdb-server/crates/types/src/redirect.rs::CROSSSLOT_MSG` and the `EXECABORT` literal in
  `frogdb-server/crates/server/src/connection/transaction.rs:156`.
- Reused the naming/shape of `cross_shard_key_pair` / `assert_crossslot` from issue #19
  (`integration_transactions.rs`) as the pattern to follow; the helpers themselves are
  file-local duplicates (`assert_crossslot`, `assert_execabort`, `assert_exact_error`,
  `owner_node_with_two_slots`) because each `tests/*.rs` file compiles as an independent test
  binary — there is no shared-code seam between `integration_cluster.rs` and
  `integration_transactions.rs` to reuse literally.

**Verification:** both tests + the full `integration_cluster` suite (147 tests) run green; the two
new/changed tests run clean across 5 repeated invocations (no flakiness observed). `just fmt
frogdb-server` and `cargo clippy -p frogdb-server --tests -- -D warnings` both clean.

No FrogDB string diverges from what's documented here; the divergence found was between the
issue's *assumed* code path and the actual one, not between FrogDB and Redis wire format (FrogDB's
`CROSSSLOT`/`EXECABORT` strings match Redis 8.6 verbatim).
