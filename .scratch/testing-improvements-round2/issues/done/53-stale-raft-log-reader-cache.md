# A cloned `RaftLogReader` keeps a stale log cache and serves overwritten-term entries

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/11 F3 · MASTER.md §3
Score: severity 5 · likelihood 2 · effort 2 · priority 17
Area: frogdb-cluster / Raft storage

## Context

`get_log_reader` returns a *new* `ClusterStorage` holding a **copy** of the log cache. That copy
never receives `invalidate_cache_range` from the owning handle, and openraft creates the log reader
**once** at startup and holds it for the node's lifetime. After a leadership flap — cache entries
as leader, step down, truncate a conflicting suffix, re-append different content at the same
indexes, become leader again — the reader serves entries from an overwritten term at indexes that
were truncated and re-appended. That is Raft log divergence: the leader ships stale entries to
followers, or membership recovery reads the wrong entry.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `crates/cluster/src/storage.rs:280-288` — `get_log_reader` returns a *new* `ClusterStorage` with
  `log_cache: RwLock::new(self.log_cache.read().clone())`: a detached copy that never receives
  `invalidate_cache_range` from the owning handle.
- `storage.rs:227-228` — `try_get_log_entries` iterates RocksDB for which indexes exist but returns
  the **cached value** when present.
- **Why nothing catches it**: `truncate` (`:342-367`) and `invalidate_cache_range` (`:176-186`) are
  `monoculture` (5 tests, `integration_cluster` only) and only ever invalidate the caller's own
  cache.

## What to fix

1. Share one `Arc<RwLock<..>>` cache between the owning handle and every reader clone, so
   `invalidate_cache_range` reaches all of them — or
2. drop the cache from the reader clone entirely and always read through to RocksDB, if the cache's
   measured benefit does not justify the coherence machinery.
3. Add the deterministic regression test below; the repro needs no cluster.

## Acceptance criteria

- [ ] New crate-level test: open storage, append entry E1@idx10, read `10..11` through a reader
      obtained from `get_log_reader()` (populating the reader's cache), then via the owning handle
      `truncate(9)` and append a *different* E2@idx10; re-read `10..11` through the **same** reader
      and assert the payload/term is E2's. **Fails today.**
- [ ] A second case covers `invalidate_cache_range` on a range the reader has cached, asserting the
      reader does not serve the pre-invalidation value.
- [ ] `truncate` and `invalidate_cache_range` are no longer `monoculture` — they have coverage
      outside `integration_cluster`.

## Test boundary

**2** — a pure storage-layer invariant. Not level 5: a cluster test could never attribute the
resulting divergence to this cache, and the repro is fully deterministic at the crate level.

## Depends on

Nothing.

## Resolution

Fixed 2026-08-08 under **FM-CLUSTER-099** (`specs/cluster.md`) —
option 1 of "What to fix": one `Arc<RwLock<BTreeMap<u64, Entry>>>` is now shared by the writing
handle and every reader `get_log_reader` hands out, so `invalidate_cache_range` (the only
invalidation, reached from `truncate` and `purge`) reaches every reader by construction. Option 2
(drop the reader's cache) was not taken: sharing keeps the read path's benefit and the coherence
machinery is a single `Arc`, since there is now exactly one cache rather than two that must agree.

The suspicion was confirmed, not merely fixed around: both new tests were run against the previous
`log_cache: RwLock::new(self.log_cache.read().clone())` and both fail there.

Acceptance criteria:

- [x] `a_log_reader_never_serves_an_entry_the_owner_truncated` — append E1@10, read `10..=10`
      through a reader, `truncate(9)` and append a different E2@10 (later term, different payload)
      on the owner, re-read through the **same** reader and assert E2. Failed before the fix.
- [x] `cache_invalidation_reaches_a_reader` — a range the reader has cached is invalidated on the
      owner; the reader no longer serves the pre-invalidation value, neighbouring indexes survive,
      and a reader's own fill is visible to the owner (one cache, both directions).
- [x] `truncate` and `invalidate_cache_range` now have coverage outside `integration_cluster`:
      these two crate-level tests plus the pre-existing
      `truncate_drops_only_the_tail_after_the_kept_index` all live in `frogdb-cluster`, so they
      also count toward that crate's own mutation score.

## Re-triage 2026-08-06

**Verdict: still-valid**

Confirmed live; the Phase-4 cluster lock did not close it. The code did not move — it is still
`frogdb-server/crates/cluster/src/storage.rs`, only the line numbers shifted (the file's only edit
since is the repo-restructure commit `7ba151f0`). `get_log_reader` is now `storage.rs:472-481`
(issue cited `:280-288`) and still hands back a detached `ClusterStorage` with
`log_cache: RwLock::new(self.log_cache.read().clone())` at `:477` while sharing only `db` and
`snapshot_save_lock` by `Arc`. `try_get_log_entries` still short-circuits on the clone's own cache —
`storage.rs:415` `if let Some(entry) = self.get_cached(index)` inside the RocksDB iteration (issue
cited `:227-228`). `invalidate_cache_range` is now `:364-374` (issue cited `:176-186`) and is
reached only from `truncate` (`:567`) and `purge` (`:600`) on the *owning* handle, so a reader clone
is never invalidated. Nothing anywhere in `crates/cluster` or `crates/cluster-runtime` calls
`get_log_reader` outside its own definition — no test constructs a reader, so the clone's staleness
is unforced. `cluster-failure-modes.md` names `cache_evicts_the_oldest_only_once_over_the_bound`,
`cache_invalidation_respects_both_range_ends` and `truncate_drops_only_the_tail_after_the_kept_index`
(spec line 295), all of which operate on a single handle; **no FM row covers reader-clone cache
coherence**.
