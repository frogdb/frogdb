# A failed spill silently becomes a real delete — and replicates the `DEL`

Status: done
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

## Decision

**Option 2 — fail the write with OOM and keep the key — chosen, narrowed to the I/O case.**

Recorded before implementation, per the first acceptance criterion. The narrowing matters and is
not in the issue's option list: option 2 as written would apply to *every* `SpillError`, which
would turn a `NoWarmStore` (a tiered policy configured without a warm tier) into an unwritable
shard. Only [`SpillError::Rocks`] means the tier *failed*; `KeyNotFound`, `AlreadyWarm` and
`NoWarmStore` are structural and keep today's degrade-to-eviction behaviour, which is exactly what
plain LRU/LFU — the policy `TieredLru` is a superset of — would have done anyway.

Research backing the call (the brief asked for Redis/Valkey/Dragonfly precedent before choosing
semantics): no system in this family converts a failed offload into a delete.

- **Dragonfly** SSD tiering: a failed stash runs `ClearStashPending`, which leaves the value in
  RAM and bumps a cancelled-stash counter. The value is never dropped.
- **Redis Enterprise Flex** (Redis-on-flash): values that cannot be moved to flash stay in RAM,
  and the shard reports OOM when RAM is exhausted.
- **Redis OSS**: the closest analogue is `writeCommandsDeniedByDiskError`, which refuses writes
  with `-MISCONF` when the last background save failed. Redis's answer to "a disk write failed" is
  to stop accepting writes, never to discard data.

Losing availability under a broken warm tier is recoverable; losing the data is not.

## Resolution

**Confirmed live.** Reproduced at level 3 before the fix: `SET k2` returned `+OK`, `k1` was gone,
a `DEL k1` was broadcast to replicas and an `evicted` keyevent fired — all because a warm-tier
write failed.

### Fix

`core/src/shard/eviction.rs`, `spill_for_eviction`: the single `Err(e) => delete_for_eviction(...)`
arm is split. `Err(SpillError::Rocks(_))` logs at `error!` (naming the consequence: writes on this
shard will be OOM-rejected until the tier recovers) and returns `false`, which
`check_memory_for_write` already turns into `CommandError::OutOfMemory` through its existing
"nothing left to evict" path — no plumbing change, because eviction is synchronous with the write
check. Every other variant keeps the delete fallback. The rationale, including the
Redis/Valkey/Dragonfly precedent, is recorded in the doc comment above the method rather than only
here.

### Tests

Level 3 — **new** `crates/shard-harness/tests/eviction_spill_failure.rs`:

- `a_warm_tier_write_failure_refuses_the_write_instead_of_deleting_the_key` — the defect. Asserts
  the *observable consequence*, per the second acceptance criterion: `-OOM` reply, `k1` still
  present, **no `DEL` frame on the recording broadcaster**, **no `evicted` keyevent**.
  **RED proof**: with the new match arm disabled, it fails with
  `expected the write to be rejected, got Simple(b"OK")`.
- `a_missing_warm_tier_still_degrades_to_a_real_eviction` — pins the boundary of the narrowing:
  exactly one `DEL k1` and one `evicted` keyevent, write succeeds.
- `a_successful_spill_is_invisible_to_replicas_and_subscribers` — pins the happy path, so the
  other two cannot both pass on a build that never spills.

Level 2 — `core/tests/tiered_storage.rs`: `test_spill_errors` and `test_spill_no_warm_store` now
assert *which* `SpillError` variant each situation produces instead of only `is_err()` (fourth
acceptance criterion — an `is_err()` assertion cannot distinguish the two policies and so could
never have caught this), plus a new `test_spill_reports_warm_tier_io_failure_as_rocks`.

### New harness surface

- `crates/shard-harness/src/recording_broadcaster.rs` — `RecordingBroadcaster`, a
  `ReplicationBroadcaster` that records `(shard_id, command, args)` and reports `is_active() ==
  true`. The harness previously had only `NoopBroadcaster`, which reports *inactive* and therefore
  closes the shard's propagation gate entirely — so no test could assert what reached a replica.
  This is a generally useful seam, not specific to this issue.
- `tempfile` as a shard-harness dev-dependency (RocksDB was already in its graph via
  `frogdb-core`).

### How the fault is injected, and why no production seam was added

`WarmTier` holds a concrete `Arc<RocksStore>`, so there is no `try_put` to stub, and adding a
trait object on the hot path for a test would be the wrong trade. Instead the fixture opens the
store with `warm_enabled = false` and hands that handle to `set_warm_store`: the tier is
*configured* (so `spill_key` passes its `NoWarmStore` guard and commits to spilling) but the warm
column families do not exist, so `put_warm` returns a real `RocksError::ColumnFamilyNotFound`,
which `spill_key` maps to `SpillError::Rocks` — the same variant an ENOSPC or EIO produces,
through the same code path, with **zero production-code change**.

### Divergences and follow-ups

1. **Option 2 was narrowed to `SpillError::Rocks`** (see Decision). Applying it to every variant,
   as the issue's option text implies, would break `NoWarmStore` deployments.
2. **No new metric.** An operator seeing OOM cannot currently distinguish "genuinely full" from
   "warm tier broken" except from the log line. A `frogdb_tiered_spill_failures_total{shard,
   reason}` counter is the right fix, but adding a metric regenerates `website/src/data/metrics.json`
   through `ops/docs-gen`, and the website is out of scope for this task. **Filed as a follow-up
   rather than done half-way.**
3. **The retry-then-degrade option (3) was not taken.** A warm-tier I/O failure is almost never
   per-key — ENOSPC and EIO affect every candidate — so retrying against the next candidate mostly
   burns the attempt budget before OOMing anyway, at the cost of a harder-to-test loop. The
   structural variants, which *are* per-key, already continue to the next candidate via the
   existing sample loop.
4. `sample_keys` deliberately excludes warm keys (`test_warm_keys_not_in_eviction_sample`), so an
   already-spilled key is not re-offered as a candidate. That is why the fixture's memory limit is
   sized above a spilled key's metadata-only footprint; a tighter limit would make even a working
   warm tier unable to satisfy the check, which is a different scenario.
