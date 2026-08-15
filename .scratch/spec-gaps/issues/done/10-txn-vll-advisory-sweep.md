# Txn/VLL advisory sweep: watch clauses, deviations table, undo framing, ready_tx guard, cancellation

Status: done

## Parent

[spec-review-txn-vll-blocking.md](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-txn-vll-blocking.md)
— Finding A1 ("`txn.md` FM-TXN-050 cites a clause that does not exist"), Finding A3 ("`txn.md`
FM-TXN-039 is an unlisted Redis deviation"), Finding A4 ("`txn.md` FM-TXN-019/021: the cross-shard
refusal is framed as inherent when it is deferred"), Finding A5 ("`vll.md` FM-VLL-003: layered
timeouts as an ownership rule"), Finding A6 ("`vll.md` FM-VLL-002: release-by-`Drop` makes
exclusivity depend on an unrowed precondition").

## What is wrong

**A1.** FM-TXN-050 twice refers to `live_at_watch` and "FM-TXN-033's gap-4 clause (an
already-stale watch never aborts)". FM-TXN-033 contains neither the phrase "gap-4" nor any mention
of `live_at_watch` or stale watches — a dangling reference on the single most subtle question in
the WATCH contract.

**A3.** Redis `EXEC` is atomic including `FLUSHDB`/`KEYS`/`SCAN`; FrogDB runs them after the shard
batch, outside the atomicity envelope, returning a single array frame that looks atomic to the
client. FM-TXN-039 honestly lists "a claim of atomicity" as NOT observable, but the Redis
deviations table — the spec's single source of truth for drift — omits it.

**A4.** FM-TXN-019/021's rationale is "there is no cross-shard rollback story," framed as
inherent, while single-key ops get cross-shard atomicity from VLL and a `MULTI` batch's full key
set is known at `EXEC` — exactly the declare-all-locks-up-front precondition VLL/Calvin require,
and the cross-shard continuation machinery already exists and is used by scripts. The missing piece
is per-shard undo, not structural impossibility. FM-TXN-021 also makes standalone FrogDB refuse a
transaction that standalone Redis accepts — a real portability cost.

**A5.** FM-VLL-003 rests its cleanup-ownership rule on `CONTINUATION_DRAIN_TIMEOUT` (2s) sitting
under `DEFAULT_LOCK_ACQUISITION_TIMEOUT` (4s), a fragile nested-timeout margin that degrades under
scheduling delay (and per H2 in issue 07, the shard's event loop can itself be occupied by a long
script exactly when the margin is needed). The row already contains a better mechanism, currently
underweighted: `grant_continuation` "installs neither if the requester has already given up
(`ready_tx` closed)" — a structural guard making a lost race harmless regardless of which timer
fires first.

**A6.** "Every failure path drops `release_txs` … `acquire_continuation_and_run` owns the guard for
the whole run, so the release happens whether the work returns or panics." Panic is covered;
cancellation is not. If the connection-side future is dropped mid-`run`, the guard drops and locks
release while the shard (which received the script message over a channel) keeps executing it and
its cross-shard sub-commands — another continuation could be granted over a shard still running the
previous one's work. Today's exposure is small (connections are spawned as detached tasks with no
`JoinHandle` abort in the accept loop, `frogdb-server/crates/server/src/acceptor.rs:358-360`), but
nothing enforces that remains true.

## What to build

1. **A1**: Fold the stale-watch rule into FM-TXN-033 explicitly, stating the outcome for (i) key
   absent at `WATCH`, later created; (ii) key present but logically expired at `WATCH`; (iii) key
   live at `WATCH`, later expired. Have FM-TXN-050 reference the named clause instead of the
   dangling one.
2. **A3**: Add FM-TXN-039 to the Redis deviations table with the same rationale already in the row.
3. **A4**: Reword FM-TXN-019/021's rationale to name the missing piece (per-shard undo/rollback on
   the batch path) rather than implying structural impossibility, and file the follow-up work item
   the reworded rationale names.
4. **A5**: Promote the `ready_tx`-closed guard to *the* invariant of FM-VLL-003; demote the 2s < 4s
   relationship to a tuning note. Consider deriving the shard deadline from the coordinator's so the
   ordering cannot drift apart in a later edit.
5. **A6**: Make continuation-lock release explicit and shard-acknowledged — a release token matched
   against the granted `txid`, so a stale release cannot free a lock the shard has since regranted
   — or, at minimum, state the cancellation precondition in FM-VLL-002's Invariant and pin it with a
   test that aborts the task mid-`run` and asserts the shard still refuses a second continuation.

## Acceptance criteria

- [ ] FM-TXN-033 states the three watch-staleness cases explicitly; FM-TXN-050 references the named
      clause; `just lint-spec` green
- [ ] FM-TXN-039 added to the Redis deviations table
- [ ] FM-TXN-019/021 rationale reworded to name per-shard undo as the missing piece; follow-up item
      filed and cross-referenced
- [ ] FM-VLL-003 restated with `ready_tx`-closed guard as the primary invariant; 2s/4s relationship
      demoted to a tuning note (or shard deadline derived from coordinator's)
- [ ] FM-VLL-002 either implements shard-acknowledged release-by-token, or states the cancellation
      precondition explicitly with a forcing test that aborts mid-`run` and asserts a second
      continuation is still refused
- [ ] `just mutants-diff frogdb-txn frogdb-vll` triaged on touched code

## Blocked by

None.

## Ruling (2026-08-13)

Mechanical: fold FM-TXN-050's dangling "gap-4"/live_at_watch references into FM-TXN-033 with three
explicit cases (absent-then-created; logically-expired; live-then-expired); move FM-TXN-039 to the
deviations table; reword FM-TXN-019/021 (per-shard undo is the missing piece, not structure) + file
the follow-up it names; promote FM-VLL-003's ready_tx-closed guard to THE invariant and derive the
shard deadline from the coordinator's; pin cancellation-mid-run semantics (release token matched on
txid, or stated precondition + abort-mid-run test — acceptor.rs:358-360).
