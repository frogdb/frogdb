# Persistence advisory sweep: Confirm rename, function-load counter, database_id consumer

Status: ready-for-agent

## Parent

[spec-review-persistence.md](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-persistence.md)
— Finding A3 ("FM-PERSISTENCE-043: `Durability::Confirm` means 'committed', not 'durable', and the
name says otherwise"), Finding A5 ("FM-PERSISTENCE-037 / FM-PERSISTENCE-049: two acknowledged
blind spots worth re-rating").

## What is wrong

**A3.** `committed_seq` vs `synced_seq` is the right split, and `publish_synced_through`
snapshotting `committed_seq` before `flush()` (so it cannot over-claim) is correct. But the *name*
`Durability::Confirm` reads as a durability guarantee at every call site, while in
`periodic`/`async` it only confirms page-cache residency. The spec also never states which
client-visible primitive (if any) is wired to `synced_seq` — Redis `WAITAOF` counts fsynced
replicas specifically; if any FrogDB acknowledgement primitive is wired to `committed_seq` instead,
that is an unearned durability claim.

**A5.** Two self-documented blind spots, each one line from closure:
- FM-PERSISTENCE-037: "the known cost is a silently smaller `FUNCTION LIST` with no counter or
  metric to notice it by." Everything else in the spec that skips data increments a counter and
  surfaces it in `INFO` (FM-PERSISTENCE-033 is the model).
- FM-PERSISTENCE-049: `database_id` is minted, stamped, and never compared to anything ("nothing
  compares it across boots yet, because that needs out-of-band expected-id state"). Issue 02 of
  this campaign's persistence work (and the issue-24 ruling's R4) gives it a real consumer.

## What to build

1. Rename `Durability::Confirm` → `Committed` (or `Synced` — implementer picks with rationale
   recorded in the spec).
2. Decide and record which client-visible primitive (if any) binds to `synced_seq`; if none does
   yet, state that explicitly rather than leaving it silent.
3. Add `frogdb_recovery_functions_failed_total` counter for function-load failures at recovery,
   surfaced in `INFO` alongside the existing recovery counters (FM-PERSISTENCE-033 pattern).
4. Make `database_id` have a real first consumer per the report's pointer to R4 (see issue 02 and
   the issue-24 ruling) — coordinate with whichever issue lands that consumer first to avoid
   duplicate work.

## Acceptance criteria

- [ ] `Durability::Confirm` renamed (mechanical rename across call sites); FM-PERSISTENCE-043
      updated to state the committed-vs-synced-primitive binding explicitly; `just lint-spec` green
- [ ] `frogdb_recovery_functions_failed_total` counter added, wired into `INFO`, forcing test for a
      function-load failure incrementing it
- [ ] `database_id` given a real consumer (or explicitly deferred to the issue that lands one, with
      a cross-reference here)
- [ ] `just mutants-diff` triaged on touched locked crates

## Blocked by

Issue 01 (this rename touches the same `Durability` enum the ack-gating fix changes).

## Ruling (2026-08-13)

Rename `Durability::Confirm` → `Committed` (or `Synced`; implementer picks with rationale) and
decide/record which client-visible primitive binds to synced_seq. Add
`frogdb_recovery_functions_failed_total` counter (function-load failures at recovery are currently
silent) and make database_id's first consumer real (see report A5). Mechanical; lands with or
after issue 01.
