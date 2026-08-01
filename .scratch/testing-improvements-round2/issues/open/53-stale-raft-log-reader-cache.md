# A cloned `RaftLogReader` keeps a stale log cache and serves overwritten-term entries

Status: ready-for-agent
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
