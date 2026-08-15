# Cross-shard routing-epoch carry + post-pause watch re-check

Status: done

## Parent

[spec-review-txn-vll-blocking.md](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-txn-vll-blocking.md)
— Finding H8 ("`txn.md` FM-TXN-023/024/027: the presence probe is a TOCTOU against the shard
round-trip"), Finding A2 ("`txn.md` FM-TXN-040 vs FM-TXN-049: is the *watch* verdict retaken after
the pause barrier?").

## What is wrong

**H8.** The migration verdict is taken from "one snapshot for the whole batch, taken once per EXEC"
(FM-TXN-023), after which the batch is sent to the shard as a *separate* operation. Between the
probe and the apply, migration can complete, so FM-TXN-027's permissive arm ("every key still local
→ commit") can commit on the ex-owner. FM-TXN-022's bug refs already name this
(`.scratch/replication-cluster-rework/issues/02`, "residual commit/apply window"), but no row
states it as a NOT observable — the LOCKED spec reads as though the redirect gate closes the hole
completely, when it does not.

**A2.** FM-TXN-040 pins "exactly two `validate_queued_batch` calls when the barrier blocked … the
first verdict is never reused across a wait." FM-TXN-049 requires every watched key's slot to be
re-checked at EXEC. Neither row says whether the *watch-set* re-check is also retaken after the
park. A `CLIENT PAUSE` is exactly the window in which a slot changes hands, so a watched slot can
depart while the transaction is parked, and the CAS would then be decided against this node's stale
copy — the identical defect FM-TXN-040 exists to prevent, one gate over.

## What to build

1. **H8**: Carry the routing epoch observed at dispatch in shard messages; the shard refuses at
   apply on epoch mismatch (CockroachDB lease shape), and the coordinator retries. Row the residual
   window either way: FM-TXN-027 must state the window explicitly as a NOT observable rather than
   delegating it to a bug ref on a neighboring row, until the epoch-carry fix lands, and drop the
   NOT observable once it does.
2. **A2**: Restate FM-TXN-040's invariant as "each verdict covers the queue *and* the watch set."
   Add a forcing test that migrates a watched slot during the pause and asserts the CAS is decided
   against the post-pause topology, not the pre-pause snapshot.

## Acceptance criteria

- [ ] Shard messages carry the routing epoch observed at dispatch; shard refuses apply on mismatch;
      coordinator retries on refusal
- [ ] FM-TXN-027 NOT observable states the residual TOCTOU window (or is updated to reflect closure
      once the epoch-carry fix lands); `just lint-spec` green
- [ ] Forcing test: migration completes between probe and shard apply, epoch mismatch causes a
      refusal+retry rather than a commit on the ex-owner
- [ ] FM-TXN-040's invariant restated to cover both queue and watch set
- [ ] Forcing test: watched slot migrates during a `CLIENT PAUSE` park, CAS decided against
      post-pause topology
- [ ] `just mutants-diff frogdb-txn` triaged on touched code

## Blocked by

None.

## Ruling (2026-08-13)

Shard messages carry the routing epoch observed at dispatch; the shard refuses at apply on
mismatch (CRDB lease shape) and the coordinator retries; row the residual window either way. Plus
FM-TXN-040 restated: after a pause barrier lifts, each verdict covers the queue AND the watch set;
forcing test migrates a watched slot during the pause.
