# 31 — Raft-suite real failures: queued-txn MOVED redirect during slot migration, and
key-routing wrong-value reads under raft-chaos

Status: needs-triage
Type: bug (correctness — cluster/raft, replication routing)
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
