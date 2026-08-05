# 31 — Raft-suite real failures: queued-txn MOVED redirect during slot migration, and
key-routing wrong-value reads under raft-chaos

Status: ready-for-agent (see "Root cause analysis (2026-08-05)" — all three reported
failures are harness/checker artifacts; the investigation found one *unreported* real
server bug in the same evidence, split out below as the priority item)
Type: bug (test harness — jepsen checkers) + bug (correctness — cluster slot migration)
Origin: split out of issue 26 while investigating jepsen-nightly manual dispatch run
30828159222 (2026-08-03). Issue 26 originally attributed the raft suite's 3 failures to a
harness NPE in `jepsen.frogdb.hash`; re-reading the raw job log (`gh run view --job
91735040211 --log`) shows that attribution was wrong — the NPEs are real but confined to the
unrelated "single" suite's `frogdb-hash-docker` test (silently swallowed by
`check-hash-history`, a weak checker that only look at `:type :ok` ops; see issue 26's
resolution). The raft suite's 3 failures have a different, genuine root cause: zero
`NullPointerException` occurrences exist anywhere in the isolated raft-suite log range.

## What happened

Raft suite summary from the run: `20 tests, 17 passed, 3 failed, in 22:46`.

### `slot-migration` (0:19 FAIL) and `slot-migration-partition` (1:45 FAIL)

Both fail identically on `:exec-queued-txn` with `[:exec-redirected "MOVED"]`:

```
{:key "{migration-test}:txn", :slot 4483, :queued-on "n1"/"n2", :queued 1,
 :outcome :redirected, :reply "MOVED 4483 172.21.0.3:6379"}
```

A transaction that was queued (via MULTI) before/during a slot migration is redirected with
MOVED when it comes time to EXEC, instead of either executing against the now-current owner or
failing the whole MULTI atomically with a clear error. Queued commands inside a MULTI are
supposed to be validated for routing before EXEC is reached (or the migration should not let a
QUEUED transaction's target slot move without fencing it) — a MOVED reply surfacing at EXEC time
for an already-queued transaction is a genuine slot-migration/routing correctness bug.

### `raft-chaos` (2:15 FAIL)

The `key-routing` workload's checker reports `:values-correct? false` with 7
`:wrong-value-reads` entries under raft/leader-election chaos, e.g.:

```
{:key "kr-53", :read 2775}
```

i.e. reads returning stale or incorrect values for at least 7 keys during/after raft chaos
(leader flapping, elections). This is a genuine data-correctness bug, not a harness artifact.

## Suggested next steps

- [ ] Reproduce `slot-migration`/`slot-migration-partition` locally (or via the same CI harness)
      and determine whether queued-but-not-yet-executed transactions are validated against slot
      ownership at QUEUE time, EXEC time, both, or neither; decide the correct semantics
      (redirect before queuing vs. abort atomically at EXEC) and fix.
- [ ] Reproduce `raft-chaos`'s wrong-value reads; capture the full op history around each of the
      7 flagged keys to determine whether this is a lost-update, stale-read, or split-brain
      class bug during leader elections.
- [ ] Consider whether `check-hash-history` (the plain hash workload's checker, unrelated to this
      issue but noted during the same investigation) should be tightened to also flag non-`:ok`
      ops, since it currently makes NPEs/failures invisible to CI (see issue 26's resolution).

## References

- Run: https://github.com/frogdb/frogdb/actions/runs/30828159222 (job 91735040211)
- `testing/jepsen/frogdb/src/jepsen/frogdb/slot_migration.clj`
- `testing/jepsen/frogdb/src/jepsen/frogdb/key_routing.clj`
- Split out of `.scratch/hardening/issues/done/26-jepsen-raft-suites-fail-on-harness-npe.md`

---

## Root cause analysis (2026-08-05)

Evidence source: `gh run view 30828159222 --log` (the `Upload jepsen-store` step of that run
**failed**, so no `store/` artifact exists — the job log is the only surviving record).
Raft-suite range isolated from the full 755k-line log at the `##[group]Jepsen suite: raft`
marker; all line numbers below are relative to that isolated, prefix-stripped range.

### Verdict summary

| Suite | Reported symptom | Verdict |
|---|---|---|
| `slot-migration` | `:exec-queued-txn` → `[:exec-redirected "MOVED"]` | **HARNESS/CHECKER ARTIFACT.** Server behavior is Redis-correct and the workload's own checker says `:valid? true`. |
| `slot-migration-partition` | same | **HARNESS/CHECKER ARTIFACT** (identical mechanism). |
| `raft-chaos` | 7 `:wrong-value-reads` | **HARNESS BUG** — cross-test keyspace leakage + an order-blind checker rule. No routing corruption occurred. |
| — (not reported) | acked writes destroyed by slot migration | **REAL SERVER BUG**, found in the same logs, currently ungated by every checker. See "Bug X" below. |

Explicitly evaluated and **rejected**: none of these are FM-CLUSTER-037 (the accepted
commit→apply window). That row's exposure is bounded by Raft apply latency (microseconds).
Bug X's loss window in the captured run is **2.2 seconds of steady-state writes with no fault
active**, and its mechanism is a missing pre-execution key-presence check, not a stale
snapshot. Issue 31 is therefore *not* evidence for the rework-02 barrier, and the barrier
would not fix Bug X.

---

### 1 & 2 — `slot-migration` / `slot-migration-partition`: checker artifact, not a bug

The premise in "What happened" above ("a MOVED reply surfacing at EXEC time for an
already-queued transaction is a genuine slot-migration/routing correctness bug") is **wrong**
on both the server side and the harness side.

**The workload checker passed.** From the run log (`slot-migration`, rel. line 2314):

```clojure
{:workload {:valid? true,                        ; <-- the data checker is GREEN
            :wrong-value-reads [], :values-correct? true,
            :lost-write-keys [], :lost-write-count 0,
            :redirects-ok? true, :durable? true,
            :orphaned-txn-keys [],               ; <-- the txn did NOT commit on the old owner
            :exec-queued-txn-outcomes {:redirected 1},
            :final-owner-correct? true, ...},
 :stats {:valid? false,                          ; <-- ONLY this fails the test
         :by-f {:exec-queued-txn {:valid? false, :count 1,
                                  :ok-count 0, :fail-count 1, :info-count 0}, ...}},
 :valid? false}
```

`slot-migration-partition` is byte-for-byte the same shape (rel. line 8517): workload
`:valid? true`, `:stats :valid? false`, `:exec-queued-txn` 0 ok / 1 fail.

So the failure is produced entirely by jepsen's generic `checker/stats`, composed in
`testing/jepsen/frogdb/src/jepsen/frogdb/core.clj:210`, which marks a run invalid when any
`:f` has ops but zero `:ok` ops. The generator emits **exactly one** `:exec-queued-txn`
(`gen/once`, `slot_migration.clj:563`), so a single legal `:fail` zeroes the `:f`.

**MOVED-at-EXEC is the contract, and the harness says so itself.**
`testing/jepsen/frogdb/src/jepsen/frogdb/slot_migration.clj:337-339`:

> `;; EXEC the transaction queued above, now that the slot may have moved.`
> `;; Both outcomes are legal; what is NOT legal is a write landing on a`
> `;; node that no longer owns the slot, which :read-orphans checks.`

and `slot_migration.clj:770-772`:

> `;; The straddling transaction's outcome (reported, not gated: both`
> `;; "refused with a redirect" and "executed on the current owner" are`
> `;; correct; only an orphan is a fault).`

`:orphaned-txn-keys []` in both runs proves the actual gated property held: the queued write
did **not** land on the former owner.

**The server matches Redis.** Redis `processCommand()` runs `getNodeByQuery()` over
`c->mstate` when the command is `EXEC`, and on a redirect calls `discardTransaction(c)` and
replies with the bare `MOVED`. FrogDB does the same:
`frogdb-server/crates/server/src/slot_migration/routing.rs:257-292` (`route_queued_batch`),
whose own doc-comment states *"Redis performs the identical decision at EXEC by re-running
`getNodeByQuery` over `c->mstate` (`cluster.c`), replying with the bare redirect and
discarding the queue (`server.c`)"*. FM-CLUSTER-029 covers the WATCH half of the same seam.

The client marking the op `:type :fail` (`slot_migration.clj:355`) is *semantically* right for
jepsen (the transaction definitely did not commit) — the defect is purely that a
known-legal-and-sometimes-universal `:fail` outcome is fed to a stats gate that demands one
`:ok`.

**Fix recommendation (harness only, no server change):** in
`testing/jepsen/frogdb/src/jepsen/frogdb/core.clj:208-213`, stop gating on the raw
`(checker/stats)` for workloads that have legitimately all-failing `:f`s. Preferred: wrap it
so `:valid?` ignores a configurable whitelist of `:f`s (`#{:exec-queued-txn}` here), keeping
the counts visible in the report. Acceptable alternative: have the generator emit the
queued-txn/exec pair more than once, which does not remove the flake (a run where *every*
EXEC is legally redirected still trips it). Do **not** "fix" this by changing the server or by
recording the redirect as `:type :ok` — the latter would blind jepsen to a genuinely lost
transaction.

---

### 3 — `raft-chaos`: harness bug (state leakage + an order-blind checker rule)

Checker output (rel. line 10731):

```clojure
:wrong-value-reads [{:key "kr-53", :read 2775} {:key "kr-53", :read 2775}
                    {:key "kr-60", :read 13621} {:key "kr-3", :read 3590}
                    {:key "kr-14", :read 13053} {:key "kr-48", :read 10430}
                    {:key "kr-65", :read 6}]
:values-correct? false, :wrong-value-count 7
:no-lost-writes? true, :lost-write-count 0, :process-killed? true
```

Every one of the 7 was traced through the full raft-suite log. **All 7 have the identical
shape: the flagged value was written to _that same key_ in an _earlier test of the same
batch_, and the flagged read happened _before this run's first write to that key_.** No
flagged value ever belonged to a different key, a different slot, or a different node — i.e.
**there is zero evidence of routing corruption, cross-slot contamination, or fabrication.**

Worked example, `kr-53` (the two duplicate entries):

```
16:38:19,520  :ok :write      {:key "kr-53", :written 2775,  :slot 8953}   <- test #1 (PASSED)
16:38:50,494  :ok :final-read {:key "kr-53", :value   2775,  :slot 8953}   <- test #1
16:41:14,669  :ok :final-read {:key "kr-53", :value   2775,  :slot 8953}   <- test #2 (PASSED)
--- raft-chaos starts 16:43:01 ---
16:43:07,694  :ok :read       {:key "kr-53", :value   2775,  :slot 8953}   <- FLAGGED
16:43:13,931  :ok :read       {:key "kr-53", :value   2775,  :slot 8953}   <- FLAGGED
16:44:05,369  :ok :write      {:key "kr-53", :written 16748, :slot 8953}   <- first write THIS run
16:44:27,506  :ok :write      {:key "kr-53", :written 23337, :slot 8953}
16:45:15,951  :ok :final-read {:key "kr-53", :value   23337, :slot 8953}   <- correct
```

The other six, same pattern (value written in an earlier test in the batch, read before this
run's first write):

| Key | Flagged value | Written by | Flagged read | First write this run |
|---|---|---|---|---|
| `kr-60` | 13621 | 16:40:30,225 (prev test) | 16:43:16,647 | 16:43:29,261 (8682) |
| `kr-3` | 3590 | 16:40:11,967 (prev test) | 16:43:18,768 | 16:44:19,531 (21651) |
| `kr-14` | 13053 | 16:38:36,006 (prev test) | 16:43:26,299 | 16:43:36,427 (12463) |
| `kr-48` | 10430 | 16:38:32,624 (prev test) | 16:43:27,125 | 16:43:29,106 (8678) |
| `kr-65` | 6 | 16:40:03,332 (prev test) | 16:44:37,596 | 16:44:42,327 (29784) |

Two compounding harness defects produce this:

**(a) The keyspace is never wiped between batch tests.**
`testing/jepsen/frogdb/src/jepsen/frogdb/cluster_db.clj:734-772` (`db/DB setup!`) starts the
containers, heals partitions and waits for Raft convergence. It never issues `FLUSHALL`, and
`clean-raft-state!` (`cluster_db.clj:123-127`) only `rm -rf`s `/data/raft` and `/data/cluster`
— and only on the *non-converged fallback* path, which did not fire here. `teardown!`
(`cluster_db.clj:774-787`) likewise only heals and restarts. So the bounded key pool
`kr-0..kr-99` (`key_routing.clj:55-60`) carries values across every test in the batch.

**(b) The value-correctness rule is order-blind.**
`testing/jepsen/frogdb/src/jepsen/frogdb/key_routing.clj:437-445` builds `acked` from the
*whole* history and then flags any read whose value is not in that key's set — with no
requirement that the read follow a write. Note the durability rule immediately above it
(`key_routing.clj:425-432`) *does* reason about ordering (it deliberately restricts to
`:final-read`s); the value rule was never given the same treatment. Its docstring at
`key_routing.clj:366-369` asserts the property is "timing-independent", which is only true if
the keyspace starts empty — precisely the assumption (a) violates.

A third, latent defect worth fixing in the same pass (not the cause here): `acked` is built
only from `:ok` writes, so the 27 `:info` (indeterminate) writes in this run
(`:write {:ok-count 55, :info-count 27}`) contribute nothing. An indeterminate write may well
have applied; a read returning its value would be flagged as fabrication. This will bite as
soon as leakage is fixed.

**Fix recommendations (harness only):**
1. Wipe data between batch tests — `FLUSHALL` on every node in `cluster_db.clj` `setup!`
   (after convergence), or, cheaper and race-free, prefix the pool keys with a per-run nonce
   in `key_routing.clj/pool-key` so no two tests share a keyspace. The `FLUSHALL` route is
   preferable: it also protects `slot_migration.clj`'s `{migration-test}:key` and every other
   fixed-name workload key in the batch.
2. Make the value rule ordering-aware in `key_routing.clj:437-445`: only consider reads that
   occur after the first acked write to that key in this history, and additionally admit
   values from `:info` writes to that key.
3. Guard against regression: assert in `setup!` that `DBSIZE` is 0 on every node.

---

### Bug X (NEW, REAL SERVER BUG) — a single-key write on a MIGRATING source is served locally after the key has migrated away, and is destroyed at finalization

This was not reported by any checker; it was found by reading the `slot-migration-partition`
op timeline while investigating the above. **Eleven acknowledged writes were silently lost,
with no fault active.**

```
16:41:19,514  :ok :start-migration {:slot 4483, :source "n2", :dest "n1"}
16:41:23,187  :ok :write  {:key "{migration-test}:key", :slot 4483, :written 89}
16:41:23,189  :ok :migrate-keys {:keys-migrated 1}      <- MIGRATE moved the key n2 -> n1
16:41:23,190  :ok :write  {... :written 90}   \
16:41:23,658  :ok :write  {... :written 93}    |
16:41:23,836  :ok :write  {... :written 96}    |  11 writes, ALL ACKED,
16:41:23,995  :ok :write  {... :written 99}    |  and all read back
16:41:24,158  :ok :write  {... :written 102}   |  successfully mid-run
16:41:24,424  :ok :write  {... :written 105}   |  (reads returned 90, 93,
16:41:24,606  :ok :write  {... :written 108}   |   102, 111, 114)
16:41:24,615  :ok :write  {... :written 111}   |
16:41:25,101  :ok :write  {... :written 114}   |
16:41:25,292  :ok :write  {... :written 117}   |
16:41:25,365  :ok :write  {... :written 120}  /
16:41:25,387  :ok :finish-migration {:slot 4483, :new-owner "n1"}
16:41:27,338  :nemesis :partition :primary-isolated       <- FIRST FAULT, 2s AFTER the loss
16:41:28,394  :ok :read-slot-owner {:slot 4483, :owner "n1"}
16:41:28,396  :ok :read-orphans {:slot 4483, :owner "n1", :orphans {"n2" ["{migration-test}:key"]}}
16:41:28,397  :ok :read  {:key "{migration-test}:key", :slot 4483, :value 89}   <- 90..120 GONE
   ... every subsequent read through 16:43:01 returns 89 ...
```

The nemesis partition began at 16:41:27,338 — **1.9 s after the last lost write and 1.9 s
after finalization**. The entire loss occurred in a healthy, fault-free cluster.

**Mechanism, confirmed in code:**

1. `MIGRATE` deleted the key on the source. Proof from the harness: `migrate-slot-keys!`
   (`slot_migration.clj:125-135`) loops `CLUSTER GETKEYSINSLOT` + `MIGRATE` until the slot
   reports empty; it returned `:keys-migrated 1`, so the second `GETKEYSINSLOT` on n2 came
   back empty. (`cluster_db.clj:304-309` issues a plain `MIGRATE … REPLACE`, no `COPY`.)
2. The next `SET` for that key arrives at n2, which still *owns* slot 4483 with a migration
   open. `route_with_snapshot`
   (`frogdb-server/crates/server/src/slot_migration/routing.rs:139-146`) returns
   `RouteDecision::LocalServeMigrating` **purely from ownership + "a migration exists"** — it
   never consults key presence:
   ```rust
   Some(&owner) if owner == self_node_id => {
       if snapshot.migrations.contains_key(&slot) {
           RouteDecision::LocalServeMigrating
       } else { RouteDecision::LocalServe }
   }
   ```
   and `to_response` (`routing.rs:84-87`) maps it straight to `RouteOutcome::ServeLocal`.
3. The pre-dispatch presence probe that *would* catch this,
   `PreDispatchView::check_migrating_multikey`
   (`frogdb-server/crates/server/src/connection/guards.rs:857-895`), **bails on single-key
   commands**: `if keys.len() < 2 { return None; }` (`guards.rs:871-873`). Its own comment
   defers those to `migrating_ask_for_nil`.
4. `ConnectionHandler::migrating_ask_for_nil` (`guards.rs:172-209`) runs *after* execution and
   fires **only when the reply is nil** (`if !is_nil_response(response) { return None; }`,
   `guards.rs:181-183`). That covers single-key **reads** (`GET` → nil → `ASK`). A `SET`
   replies `+OK`, never nil — so the write executes locally, re-creating the key on the
   source, and the client is told it succeeded.
5. `CLUSTER SETSLOT <slot> NODE <target>` then hands the slot to n1, whose copy is the
   pre-`MIGRATE` value 89. n2's copy (holding 120) becomes an orphan — exactly what
   `:read-orphans` reports.

Redis does not have this hole: `getNodeByQuery()` performs the presence check **before
execution, for every command regardless of arity**, and ASK-redirects a write to a key that
has already migrated away. FrogDB's *batch/EXEC* path is correct here —
`route_queued_batch` (`routing.rs:277-285`) turns `LocalServeMigrating` into
`BatchRoute::ProbeMigratingSource` and does probe presence. Only the single-key
non-transactional path is missing it.

**The same defect is present in the passing `slot-migration` test**, merely masked: its
generator kept issuing writes *after* finalization, so later writes on the new owner
overwrote the stale value and `:final-read-matches-last-acked-write` came out `true`. The
tell-tale is still in its report — `:orphaned-keys [{:node "n1", :slot 4483, :keys
["{migration-test}:key"]}]`.

**Why no checker caught it.** Both signals exist but neither gates:
- `:final-read-matches-last-acked-write false` (partition run, `last-acked-write 120` vs
  `:final-values [89]`) is explicitly reported-not-gated
  (`slot_migration.clj:665-669`), on the sound-in-isolation reasoning that the settled value
  of a concurrently-written register need not be the highest counter. That reasoning does not
  cover a 31-count regression to a value that predates 11 later acked writes.
- `:orphaned-keys` is gated **only for transaction keys** (`slot_migration.clj:773-789`), and
  the non-gating rationale in the docstring attributes non-txn orphans to "the documented
  raft-apply window … for a bounded interval". **That attribution is wrong here** — this
  orphan is not the microsecond FM-CLUSTER-037 window, it is 2.2 s of writes deliberately
  served on the source.

**Fix recommendation (server):** perform the key-presence probe on the migrating source for
**all** commands, before execution, not just `keys.len() >= 2`. The pieces already exist —
`PreDispatchView::probe_key_presence` (`guards.rs:903-...`) is shared with the EXEC path and
already fails closed. Concretely: drop the `keys.len() < 2` early-return in
`check_migrating_multikey` (`guards.rs:871-873`) so a single-key command on a MIGRATING
source that this node no longer holds gets `ASK` *before* it can execute, and retire
`migrating_ask_for_nil` (`guards.rs:172-209`), whose post-hoc nil-sniffing becomes redundant
and is unsound for writes by construction. Cost: one extra `ScatterOp::Exists` round-trip per
command on a migrating slot — bounded to the migration window, and this is exactly the
trade Redis makes.

**Spec impact:** FM-CLUSTER-028 ("the migrating source keeps serving and hands out `ASK`")
currently *documents the buggy behavior*. Its Observable cell describes unconditional local
serving with the ASK probe attributed only to batches, while its NOT-observable cell asserts
Redis "`ASK`s only what has moved". Those two cells contradict each other, and the row must be
rewritten as part of the fix, with a `Forced by` case for a single-key write to a
migrated-away key on the source. This is also **not** subsumed by
`.scratch/replication-cluster-rework/issues/open/02` (finalization pause barrier): the barrier
quiesces the source at *finalization*, whereas these writes were accepted seconds earlier.

**Recommendation: split Bug X into its own hardening issue** (severity: silent loss of
acknowledged writes, no fault required, on the normal resharding path) rather than leaving it
buried under a "raft suite failures" ticket. Also tighten the two checkers above so it can
never regress silently:
- gate `slot_migration.clj`'s `:orphaned-keys` on **all** keys, not just txn keys, once Bug X
  is fixed (keeping a short post-finalization settle window for FM-CLUSTER-037);
- add a durability rule that fails when a final read returns a value strictly older than an
  acked write that has no later acked write superseding it on the same key.

### Suggested next steps (revised)

- [x] Determine the exact anomaly for `slot-migration` / `slot-migration-partition` — it is
      `checker/stats`, not a routing bug; queued-EXEC MOVED is Redis-correct and already
      spec'd (FM-CLUSTER-029 / `route_queued_batch`).
- [x] Determine the exact anomaly for `raft-chaos` — cross-test keyspace leakage plus an
      order-blind value rule; no routing corruption in the history.
- [ ] **(P1)** File and fix Bug X: single-key write on a MIGRATING source must `ASK`, not
      serve locally. Rewrite FM-CLUSTER-028 to match.
- [ ] **(P2)** Harness: `FLUSHALL` between batch tests (`cluster_db.clj` `setup!`) + `DBSIZE`
      assertion.
- [ ] **(P2)** Harness: order-aware value rule + admit `:info` writes (`key_routing.clj:437-445`).
- [ ] **(P3)** Harness: stop gating on bare `checker/stats` for `:f`s with legal all-fail
      outcomes (`core.clj:210`).
- [ ] **(P3)** Tighten `slot_migration.clj` orphan/durability gating (see above) so Bug X
      cannot regress silently.
- [ ] Unchanged from the original filing: consider tightening `check-hash-history` (issue 26).

### Reproduction notes

The `Upload jepsen-store` step of run 30828159222 **failed**, so `results.edn` / the op
histories were never archived — this analysis is reconstructed entirely from the job log.
Fixing that upload step is a prerequisite for any future jepsen forensics; without it the
only record is a 755k-line log that expires.
