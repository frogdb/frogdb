# 14: SCA lock table — wound-wait on rule-2 conflicts restores wait-graph acyclicity

Status: ready-for-agent

## Origin

Distsys-review CRIT-7 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **wound-wait on the SCA path** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

The VLL deadlock-freedom argument rests on a total order by txid: a transaction only
ever waits on *lower* txids, so the wait-for graph is acyclic. `try_grant`
(`vll/src/lock_table.rs:90-123`) breaks that order with two rules: (1) SCA — blocked by
conflicting lower-txid intents; (2) holder — blocked by any conflicting **granted**
intent from another txid **regardless of order** (`lock_table.rs:107-114`, pinned by the
crate's own test `lower_txid_writer_blocked_by_granted_higher_reader:284`). Rule 2
creates lower→higher wait edges.

Because `scatter` declares on each shard via independent async messages
(`coordinator.rs:219-240`) and then waits (`:247-266`) while holding grants already
obtained elsewhere, per-shard grant order can differ across shards. Concrete cycle: two
clients run `MSET kA kB` (kA on shard A, kB on shard B), txids 3 and 5. Shard A grants
txn5 first; txn3's declare passes SCA (nothing lower) but rule 2 parks it behind txn5's
grant. Shard B grants txn3 first; txn5 parks behind txn3 via SCA. txn3 holds B waiting
on A; txn5 holds A waiting on B — a cycle. Neither releases until its whole batch is
granted, so the only exit is the phase-2 timeout, after which *both* abort and both
retries are equally likely to re-interleave: mutual-abort livelock under sustained
contention. Ordinary two-key cross-shard write; no Lua needed.

`specs/vll.md` has no FM-VLL row for this. TR-VLL-014's existing wound-wait ruling is
explicitly scoped to the **continuation** lock (`ShardBusy` refusal), and
[issue 07](07-vll-continuation-package.md)'s H1 shares that continuation-only scope —
the SCA lock table is uncovered.

Mature-system context: Calvin/VLL is deadlock-free only because the sequencer inserts a
txn's entire lock request into all lock tables in one globally-ordered step; FrogDB has
the global txid but not the atomic multi-shard declaration. CockroachDB handles the
analogous case with wound-wait. The user considered and rejected the Calvin-style
sequencer (adds an ordering point / latency coupling to the multi-shard hot path,
against the shared-nothing async-scatter architecture); wound-wait is a local
lock-table change.

## What to build (spec-first)

1. New FM-VLL row for the cross-shard rule-2 cycle: trigger (opposite per-shard grant
   order for two overlapping multi-shard batches), invariant (wait-for graph acyclic —
   lower-txid txn is never parked behind a higher-txid holder), outcome (higher-txid
   holder wounded).
2. Extend TR-VLL-014's wound-wait ruling from the continuation lock to the SCA path:
   on a rule-2 conflict where the requester's txid is *lower* than the granted
   holder's, wound the holder (abort it shard-side, release its grants, error its
   coordinator with a retryable wound verdict) instead of parking the requester.
   Rule-2 conflicts against a *lower*-txid holder still park (that edge is
   order-respecting).
3. **Liveness requirement (must be in the spec row):** a wounded transaction's retry
   keeps its original txid — age-based priority. A retry that mints a fresh (higher)
   txid can be wounded forever by younger transactions; keeping the txid guarantees the
   oldest txn eventually wins.
4. Forcing test in `frogdb-vll` (mutation gate 0.90 — test must live in the crate):
   two coordinators, two shards, opposite arrival order; pre-fix deadlocks until
   phase-2 timeout with double abort, post-fix the lower-txid txn commits and the
   higher-txid txn gets wounded + retries to success.
5. Revisit `lower_txid_writer_blocked_by_granted_higher_reader` — it pins the exact
   edge wound-wait removes; rewrite to assert the wound instead.

## Cross-references

- [Issue 07](07-vll-continuation-package.md): same wound-wait vocabulary on the
  continuation lock; keep verdict/error surfaces consistent between the two paths.
- MAJ-20 (same review, not yet ruled): resolution latency `participants × timeout` —
  wound-wait removes the timeout-only exit for this cycle class, partially mooting it.

## Acceptance criteria

- [ ] FM-VLL row added; TR-VLL-014 extended; retry-keeps-txid liveness stated in spec;
      `just lint-spec` green
- [ ] Wound-wait on rule-2 lower-requester conflicts in `try_grant`; wounded holder's
      grants released on all shards; coordinator surfaces retryable wound verdict
- [ ] Forcing test fails (timeout/double-abort) pre-fix, passes post-fix
- [ ] Pinning test rewritten to assert the wound
- [ ] `just mutants-diff` on frogdb-vll (locked, gate 0.90) triaged

## Blocked by

None — can start immediately.
