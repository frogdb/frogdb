# Script writes must pass the shard write seam

Status: ready-for-agent

## Parent

[spec-review-txn-vll-blocking.md](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-txn-vll-blocking.md)
— Finding C1 (CRITICAL): "`txn.md` FM-TXN-030 (with FM-TXN-007): a script's writes bypass both
gates, and the row claims they do not".

## What is wrong

FM-TXN-030 is titled "Scatter and script batches are slot-validated like any other" and its
Invariant states the keyless fast path is only reachable when no command in the batch names a key.
Its own `Bug refs` cell concedes: "a script's **undeclared** runtime writes are still unvalidated."
FM-TXN-007 concedes the parallel authorization hole: "the same gate is not applied to writes a Lua
script issues from inside a transaction" — so `NOPERM`, `NOREPLICAS` (`min-replicas-to-write`), and
the replication self-fence are all enforced against a queued `SET` and not against
`EVAL "redis.call('SET', …)"` in the same `MULTI`.

Two distinct defects hide behind one bug ref:

1. **Cluster**: an `EVAL` that writes a key it did not declare in `KEYS` produces exactly the
   orphan write FM-TXN-009 calls "the orphan-write shape this campaign exists to prevent" —
   applied and replicated on a node that does not own the slot.
2. **Authorization**: an ACL-restricted user can reach a denied key or command through `EVAL`
   inside `MULTI`, which the non-scripted path refuses with `-NOPERM` plus an `ACL LOG` entry.

A LOCKED spec asserting a guarantee its own bug ref retracts is worse than no row: the mutation
gate measures against the row, so a mutant that deletes the (nonexistent) script-write check has
nothing to survive.

## What to build

1. Add `FM-TXN-051 — a script's runtime write outside the declared key set`, with `Observable` =
   the write is refused at the shard write seam (slot ownership + ACL + write-admission
   [NOREPLICAS/self-fence] checked there, not at queue/validation time).
2. Enforce all three checks (slot ownership, ACL, write-admission) at the shard write seam so
   every producer — declared and undeclared script writes, MULTI, and internal callers — passes
   through it, closing the class structurally rather than per-producer.
3. Promote the repro in `.scratch/replication-cluster-rework/issues/03` (cross-shard continuation)
   to a forcing test for the new row.
4. Until the fix lands, mark FM-TXN-030's and FM-TXN-007's affected clauses `Status: KNOWN-VIOLATED`
   and soften their titles so the LOCKED contract does not assert a property the system lacks.
5. Add a seam lint (`agents/seam-lints.md` family) asserting the shard's mutation entry point is
   the only write path reachable from script execution, so the class cannot silently reopen via a
   new producer.

## Acceptance criteria

- [ ] `FM-TXN-051` added; `just lint-spec` green
- [ ] Forcing test (promoted from `.scratch/replication-cluster-rework/issues/03`) fails against
      today's code, passes after the fix
- [ ] Slot ownership + ACL + write-admission checks enforced at the shard write seam for script
      writes (declared and undeclared) and MULTI alike
- [ ] FM-TXN-030 and FM-TXN-007 clauses marked `KNOWN-VIOLATED` until the fix lands, then restored
      to unconditional once it does
- [ ] New seam lint added under `just lint-gates` (or the appropriate seam-lint family) enforcing
      the single-write-path invariant
- [ ] `just mutants-diff frogdb-txn frogdb-vll` triaged on touched code

## Blocked by

None.

## Ruling (2026-08-13)

Enforce slot ownership + ACL + write-admission (NOREPLICAS/self-fence) at the SHARD WRITE SEAM,
not at queue/validation time — every producer (declared+undeclared script writes, MULTI, internal
callers) passes through, closing the class. New FM-TXN row; FM-TXN-030/FM-TXN-007's contradicted
clauses are marked KNOWN-VIOLATED until the fix lands; promote the repro in
.scratch/replication-cluster-rework/issues/03 (cross-shard continuation) to a forcing test; add a
seam lint so the seam stays the only write path.
