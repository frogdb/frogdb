# A failed spill silently becomes a real delete — and replicates the `DEL`

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/01 F2 · MASTER.md §3 · MASTER.md §7 (blocked on a semantics call)
Score: severity 5 · likelihood 3 · effort 2 · priority 19
Area: frogdb-core / shard eviction + tiered storage

## Context

`spill_for_eviction`'s `Err(_)` arm falls through to `delete_for_eviction`, which routes a *real*
removal through `run_internal_removal_effects`: WAL delete, replicated `DEL`, `evicted`
notification. A transient RocksDB write failure therefore destroys the value permanently, on the
primary *and* on every replica, while the client's write succeeds. Tiered storage is by definition
disk-heavy, so ENOSPC / EIO / a RocksDB write stall on the warm CF is an ordinary ops event for
exactly the deployments that enable it.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix. `MASTER.md` §7 lists failed-spill behaviour as one of the ten items requiring a
semantics call before its test can assert anything — hence `needs-triage`.

## Evidence

- `core/src/shard/eviction.rs:269-277` — `line_counts` 0 for the
  `Err(e) => { … self.delete_for_eviction(key).await }` arm.
- The doc comment above it (`eviction.rs:240-254`) explicitly says "Only its fallback-to-delete
  path is a real removal", i.e. the design is aware the fallback is destructive; nothing tests it.
- `core/src/store/hashmap.rs:771-772` (the `SpillError::Rocks` return) is likewise 0-covered.
- **Why the existing test passes anyway**: `core/tests/tiered_storage.rs:265`
  (`test_spill_errors`) already constructs the error cases at level 2, but asserts only that
  `spill_key` returned `Err` — never the *policy* consequence (delete + replicate + notify).

## Options

This finding contains a product decision, not only a test decision. Reproduced verbatim from the
proposal's `OPTIONS on F2 (failed spill → delete)`:

1. **Pin today's behaviour** — a failed spill degrades to a real eviction (delete + replicate +
   `evicted` notification). *Trade-off*: cheapest, honest about the current contract, but it
   codifies silent data loss on a transient disk error, which is a poor default for a database.
2. **Fail the write with OOM and keep the key** — treat a spill failure like "could not free
   memory". *Trade-off*: no data loss, and consistent with `check_memory_for_write`'s existing OOM
   contract; costs availability under warm-tier failure (writes start erroring) and needs a
   production-code change before the test can be written.
3. **Retry-then-degrade** — retry the spill against the next candidate, and only delete after the
   pool is exhausted. *Trade-off*: best behaviour, most code, and hardest to test deterministically.

**Recommendation: option 2**, with the test asserting OOM + key survival. A database should not
delete data because a disk write failed. Option 1 is acceptable only as a temporary pin if the
change is deferred — in which case the test should carry an explicit comment naming the decision.

## Acceptance criteria

- [ ] The chosen option above is recorded on this issue before implementation starts.
- [ ] A `shard_driver` test constructs a worker with `with_eviction(EvictionConfig::new(limit,
      TieredLru))` and a warm store whose `try_put` fails, drives writes until eviction triggers,
      and asserts the **observable consequence** — not that `spill_key` returned `Err`.
- [ ] Under option 2 (recommended) the test asserts the write is rejected with OOM and the key
      survives, and **fails against today's code**. Under option 1 it asserts the key is deleted
      and a `key-evicted` notification plus a replicated `DEL` are observed, and carries a comment
      naming the decision.
- [ ] `core/tests/tiered_storage.rs:265`'s `test_spill_errors` is extended or superseded so no test
      remains that asserts only the `Err` return.

## Test boundary

**3** — `shard_driver` with a warm store, because the assertion is about the *replication +
notification + WAL* effects of the fallback, which only the worker's effect pipeline produces. A
level-2 store test cannot see them; a level-4 server test would add a socket without adding signal.

## Depends on

- Infrastructure I1 (`shard_driver` harness extension: `with_eviction`, optional warm/persistent
  store) — issue 01, `.scratch/testing-improvements-round2/issues/`. Every builder option needed
  already exists and is simply not forwarded.
- Theme T2 (failure of a derived structure reported as success) — issue 20,
  `.scratch/testing-improvements-round2/issues/`.
