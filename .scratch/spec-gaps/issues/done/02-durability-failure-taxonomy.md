# Durability-failure taxonomy: fsyncgate latch, suffix restoration, readonly policy

Status: done

## Parent

[spec-review-persistence.md](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-persistence.md)
— Finding H1 ("FM-PERSISTENCE-007: a durability failure is un-latched by a later success
(fsyncgate)"), Finding H2 ("FM-PERSISTENCE-003: 'loss is always a suffix' is falsified by
FM-PERSISTENCE-005/007"), Finding A2 ("FM-PERSISTENCE-005: `continue` has no fail-stop escalation,
only a counter that grows forever").

## What is wrong

Three compounding gaps in the durability-failure story, all rooted in conflating a *write* error
(recoverable, data still in the buffer) with a *sync* error (poison, data may be unrecoverable):

**H1 — fsyncgate.** FM-PERSISTENCE-007's NOT observable states, verbatim: "the failure staying
latched forever after the sink recovers — once a later commit succeeds past the failed range,
`confirm_durable_through` stops reporting it, so a transient error does not poison every
subsequent write." On Linux, a writeback error marks the page clean-but-failed and reports the
error to *one* `fsync()` caller; a subsequent `fsync()` on the same file returns `0` while the
data is gone forever. "A later commit succeeded past the failed range" is not evidence the earlier
range is on the device. Concretely: sequence 100's commit fails; sequences 140-150 commit
successfully; a `Confirm` for `(140, 150]` finds `highest_failed_seq = 100` outside its range and
returns `Ok`. The caller acks. Sequence 100 is permanently gone and no client ever learned it.
Under `wal-failure-policy = rollback` — the policy an operator chooses specifically to avoid
acking lost writes — this is the exact hazard reached by a different door. The RocksDB options
builder (`frogdb-server/crates/persistence/src/rocks/mod.rs`) sets no `paranoid_checks` and no
error-handler/auto-resume configuration, so nothing backs the spec's "transient" framing either.

**H2 — gap holes falsify "loss is always a suffix".** FM-PERSISTENCE-003's NOT observable claims:
"a *gap* — a recovered keyspace holding a later write while a strictly earlier one from the same
shard is missing (the WAL commits in enqueue order, so loss is always a suffix)." That reasoning
covers only crash loss, not failure loss. FM-PERSISTENCE-005 creates gaps by construction: within
a batch, `stage_actions` under `FireAndForget` "logs each failure and continues to the next
action" — action 1 fails, action 2 succeeds, hole inside one command's WAL footprint (breaking
FM-PERSISTENCE-001's all-or-nothing property for multi-action commands like RENAME/MSET). Across
batches: batch N fails, batch N+1 succeeds, the recovered keyspace holds N+1 and not N, and H1's
un-latching then lets an operator's `Confirm` above the hole return `Ok`. This breaks prefix
consistency that replication/full-sync depend on.

**A2 — no fail-stop between "ack the loss" and "refuse every write".** The shipped default for a
permanently broken data device is: serve reads, ack every write, lose every write, never refuse
anything — worse than Redis' `MISCONF` default. FM-PERSISTENCE-046's PreChecks gate exists but is
scoped to *snapshot* failure only, explicitly not WAL failure (the deviations table says so).

## What to build

1. **Poison latch (H1).** Add `FM-PERSISTENCE-053 — a failed durability sync poisons the shard
   until an operator resolves it`. Replace the "un-latch on later success" NOT observable with the
   opposite invariant: `highest_failed_seq` (or a new `poisoned_seq`) is monotone and cleared
   **only** by process restart or an explicit operator `RESET`, never by a subsequent successful
   commit. This inverts FM-PERSISTENCE-007's current requirement to un-latch.
2. **Suffix restoration (H2).** Under `wal-failure-policy = continue`, the FIRST stage-failure
   prefix-truncates the shard's persisted stream and poisons the shard (using the H1 latch) —
   restoring "recovery loss is always a suffix" (FM-PERSISTENCE-003) so holes are no longer
   possible. Under `rollback`, a poisoned shard refuses writes (`-IOERR` or `-MISCONF` reusing
   FM-PERSISTENCE-046's gate) rather than silently resuming acks.
3. **`readonly` policy (A2).** Add a third `wal-failure-policy` value, `readonly`: fail-stop —
   refuse writes, serve reads, page the operator — enforced via FM-PERSISTENCE-046's PreChecks
   gate.
4. State explicitly in the spec what FrogDB relies on from RocksDB: `paranoid_checks` on/off,
   auto-resume enabled/disabled, and that a RocksDB background error surfaces as a persist error
   rather than being swallowed.
5. New FM rows plus forcing tests for each leg (poison latch, prefix truncation, `readonly`
   policy).

## Acceptance criteria

- [ ] `FM-PERSISTENCE-053` (poison latch) added with forcing test; `highest_failed_seq`/`poisoned_seq`
      monotonicity asserted; `just lint-spec` green
- [ ] First-stage-failure prefix truncation under `continue` implemented; FM-PERSISTENCE-003's gap
      NOT-observable restored to true; forcing test crashes/fails mid-batch and asserts a
      truncated-and-poisoned shard, not a hole
- [ ] `wal-failure-policy = readonly` added, wired through FM-PERSISTENCE-046's PreChecks gate,
      with forcing test and documented error text
- [ ] RocksDB `paranoid_checks`/auto-resume posture stated explicitly in the spec (set or
      documented as unset-and-relied-upon)
- [ ] `just mutants-diff frogdb-persistence frogdb-core` triaged on touched code

## Blocked by

Issue 01 (durability-mode/policy predicate the poison latch and `readonly` gate build on).

## Ruling (2026-08-13)

Full package. (1) Split write-failure (recoverable) from sync-failure (poison): a sync/fsync
failure latches the shard poisoned; the latch is cleared ONLY by restart or explicit operator
reset, NEVER by a later successful commit (2018 Postgres fsyncgate lesson: kernel may drop dirty
pages after a failed fsync; Postgres now PANICs, RocksDB latches). This inverts
FM-PERSISTENCE-007's current requirement to un-latch. (2) Under wal-failure-policy=continue, the
FIRST stage-failure prefix-truncates the shard's persisted stream and poisons the shard, restoring
the "recovery loss is a suffix of history" claim (FM-PERSISTENCE-003) that replication offset math
depends on — holes are no longer possible. (3) New third policy value `wal-failure-policy =
readonly`: fail-stop — refuse writes, serve reads, page the operator — enforced via
FM-PERSISTENCE-046's PreChecks gate. New FM rows + forcing tests for each leg; state explicitly
that RocksDB's paranoid_checks/auto-resume are unset today and what we rely on.
