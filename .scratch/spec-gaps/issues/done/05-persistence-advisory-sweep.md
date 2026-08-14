# Persistence advisory sweep: Confirm rename, function-load counter, database_id consumer

Status: done

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

- [ ] `just mutants-diff` triaged on touched locked crates

## Blocked by

Issue 01 (this rename touches the same `Durability` enum the ack-gating fix changes).

## Ruling (2026-08-13)

Rename `Durability::Confirm` → `Committed` (or `Synced`; implementer picks with rationale) and
decide/record which client-visible primitive binds to synced_seq. Add
`frogdb_recovery_functions_failed_total` counter (function-load failures at recovery are currently
silent) and make database_id's first consumer real (see report A5). Mechanical; lands with or
after issue 01.

## Outcome (2026-08-14)

**1. Rename.** `Durability::Confirm` → `Durability::Committed` (16 call sites across
`frogdb-core` and `frogdb-persistence`). `Committed` over `Synced` because the wait target is
`committed_seq` at *every* durability mode: under `sync` the commit it waits on is the fsync,
under `periodic`/`async` it is a RocksDB write. `Synced` would have been true only in `sync`
mode; `Confirm` said nothing at all. Rationale recorded in TR-PERSISTENCE-005 and
FM-PERSISTENCE-043.

**2. `synced_seq` binding: none, stated explicitly.** No client-visible acknowledgment primitive
binds to `synced_seq` today. `WAITAOF` is a stub replying `-ERR ... not implemented`; `WAIT`
counts replica acknowledgments of the replication offset, a stream position with no fsync in it;
`synced_seq` is not even a field of `WalLagStats` (the operator-visible watermark is
`committed_sequence`, deliberately), and `RocksWalWriter::durable_sequence` has no reader outside
`frogdb-persistence`. TR-PERSISTENCE-005 and FM-PERSISTENCE-043 now say so, and say that the
first primitive claiming on-device durability to a client must wait on `synced_seq`.

**3. Function-load counter.** `RecoveryStats::functions_failed` +
`frogdb_recovery_functions_failed_total` + `INFO persistence` `functions_last_load_failed` (both
the connection-level renderer and the shard-local one the scripting path uses, so they cannot
disagree — the FM-PERSISTENCE-022/issue-42 rule). Counted at both halves of phase 4: an
unreadable/corrupt `functions.fdb` counts `1` (per *file*, not per library — the file did not
parse, so its library count is exactly what is unknown), and each library the wiring layer
cannot parse or register counts `1`. Forcing tests
`a_corrupt_functions_file_is_counted_and_exported` and
`a_boot_with_no_functions_file_counts_no_failures` in `frogdb-recovery`; FM-PERSISTENCE-037 and
TR-PERSISTENCE-041 updated (the latter loses its `Pending`).

**4. `database_id`: deferred, with the consumer named.** Deliberately no consumer added here.
The comparison needs expected-id state, and the issue that owns that state is
[replication-correctness issue 24](../../../replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md),
addendum R4 — demote `replication_state.json` to a cache keyed on `database_id` plus the
recovered sequence, ignored whenever either mismatches. Adding a comparison here first would
mean inventing a config knob with no reader.
[Cluster issue 35](../../../cluster-correctness/issues/open/35-node-identity-outlives-the-process.md)
reuses the mint-once-and-stamp shape for node identity but is not a consumer of this id. Recorded
in FM-PERSISTENCE-049 and TR-PERSISTENCE-052, whose `Pending` now names issue 24 alone.
