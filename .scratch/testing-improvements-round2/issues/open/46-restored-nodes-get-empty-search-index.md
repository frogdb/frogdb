# Restored / attached nodes get a permanently empty search index, reported as success

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/10 F3 + proposals/10 F4 · MASTER.md §3
Score: severity 5 · likelihood 4 · effort 4 · priority 19 (both F3 and F4)
Area: frogdb-search + frogdb-persistence / snapshot & full sync

## Context

Neither snapshots nor replication full sync ship `<data_dir>/search`, but the RocksDB checkpoint
*does* carry the `search_meta` CF. Recovery therefore finds the index *definition*, calls
`Index::open_or_create` against the replica's empty search dir, builds a **fresh empty index**, and
records `RecoveryOutcome::Recovered { num_docs: 0 }` — the success variant. The replica answers
every `FT.SEARCH` with zero hits, forever, with no error and no log after startup. The companion
defect (10/F4) is the crash case: the tantivy commit point and the WAL recovery point are
independent, so after a `SIGKILL` the index can be missing documents that exist or holding
documents that were rolled back — neither detected, neither self-healing, both persisting across
every subsequent restart.

**This is a suspected live defect found by reading, not by test failure — the proposed tests fail
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `frogdb-server/crates/persistence/src/snapshot/stager.rs:9-11` — *"A snapshot deliberately does
  **not** include the search-index sidecar (`<data_dir>/search`)"*; `:100-101` — *"Replication full
  sync ships its own flat RocksDB checkpoint and never touches `search/` either."*
- The RocksDB checkpoint **does** carry the `search_meta` CF, so `IndexLifecycleManager::recover`
  (`core/src/shard/search/lifecycle.rs:357-431`) finds the index *definition*, calls
  `ShardSearchIndex::open` (`search/src/index.rs:252`), which does `Index::open_or_create`
  (`index.rs:258`) against the replica's own empty `<data_dir>/search` and **creates a fresh empty
  index**. `recover` then records `RecoveryOutcome::Recovered { num_docs: 0 }`
  (`lifecycle.rs:381-383`) — the *success* variant. Nothing compares `num_docs` to the shard's key
  count.
- **Why the existing tests pass anyway**: the 11 `lifecycle.rs` tests all exercise `recover` in
  isolation with no dataset, so none can catch this; `rg` for a test that restarts a server and
  then issues `FT.SEARCH` returns nothing.
- Crash half (10/F4): `core/src/shard/event_loop.rs:31` —
  `search_commit_interval = interval(Duration::from_secs(1))`, fired at `:88-95`, entirely
  independent of `WriteEffectKind::WalPersistence` (`core/src/shard/post_execution.rs:385-405`).
  The only synchronising hook is the *pre-snapshot* one (`server/src/server/init.rs:353-366`),
  which orders the search flush before the WAL drain for BGSAVE only — nothing equivalent exists on
  the crash path. `recover` (`lifecycle.rs:357-431`) reopens the tantivy dir and never reconciles
  it against the store. `search_hook.rs` has no inline tests. Round 1's issues 12
  (`durability-fsync-boundary`) and 14 (`wal-recovery-mode-pin`) pinned WAL durability but never
  touched the search index — this is exactly the residue they left.

## Options

The proposal raised an explicit `OPTIONS` block on F3; the boundary decision changes what gets
built, so it must be settled first.

- **(a) Multi-node replica test** (`test-harness` primary+replica, level 5). Highest fidelity,
  proves the real operator scenario end to end. Slow, and needs the restart step because of the
  known runtime-resync-not-installed behaviour (round-1 issue 61).
- **(b) Single-node "restore into a fresh data dir" test** (level 3): create the index, BGSAVE,
  copy *only* the RocksDB checkpoint into a new data dir (mimicking what full sync ships), start a
  server on it, assert `FT.INFO num_docs`. Much faster and deterministic, and it isolates the same
  defect — but it asserts against a hand-simulated transfer rather than the real one, so a change
  to what full sync ships would not break it.
- **(c) Pure unit test on `recover`** (level 1): populate `search_meta` with a definition, point
  `data_dir` at an empty dir, assert the outcome is *not* `Recovered`. Cheapest, but it pins the
  desired behaviour of a function that does not yet have it, and proves nothing about the transfer.
- **Recommendation**: **(b) now, (a) as a follow-up.** (b) buys the detection at level-3 cost and
  will fail today; (a) is worth building once but should not gate the fix. (c) is worth adding
  *with* the fix, not instead of it.

## Acceptance criteria

- [ ] The chosen option above is recorded on this issue before implementation starts.
- [ ] Primary holds `idx` over 100 `user:*` hashes and `FT.SEARCH` returns 100; after the transfer
      (real or simulated per the chosen option) the restored node's `FT.INFO idx` reports
      `num_docs == 100` **and** `FT.SEARCH idx *` returns 100. **Fails today** (0).
- [ ] A reusable helper asserts `FT.INFO num_docs == count of prefix-matching, type-matching,
      unexpired keys`, shared with issue 45.
- [ ] Crash half: with active expiry off, write N hashes, let the index commit, write M more,
      hard-kill within the commit interval, restart, and assert `FT.INFO num_docs == N + M` — or,
      if the design decision is that the index may lag, assert it converges within a bounded time
      and that the *divergence is detected and logged* rather than silently accepted.

## Test boundary

**5** as proposed for F3 (the behaviour *is* the full-sync transfer) and for F4 (needs fault
injection), with **3** available for the simulated-restore variant. Level 4 is not enough for the
real-transfer assertion because a single server never performs the transfer; level 3 is acceptable
only under option (b), which trades transfer fidelity for speed.

## Depends on

- Infrastructure I4 (conservation checker for derived structures in `crates/testing/`) — issue 04,
  `.scratch/testing-improvements-round2/issues/`. The proposal is explicit that the crash half
  should be built as a checker (`index ≡ dataset ∩ prefix`), not a one-off example test, because
  the same invariant catches issues 45 and 46 together.
- Infrastructure I2 (subprocess-SIGKILL crash primitive) — issue 02,
  `.scratch/testing-improvements-round2/issues/`; note `ClusterNode::kill()` is a *graceful*
  shutdown despite its name.
- Theme T2 (failure of a derived structure reported as success) — issue 20,
  `.scratch/testing-improvements-round2/issues/`.

## Re-triage 2026-08-06

**Verdict: still-valid**

The exclusion is not just intact, it is now *documented policy*:
`persistence/src/snapshot/stager.rs:9-12` still says a snapshot deliberately does not include
`<data_dir>/search`, `:97-110` carries the proposal-23 DELETE note (*"Replication full sync ships
its own flat RocksDB checkpoint and never touches `search/` either"*), and Phase 2 pinned the
exclusion with a **test that asserts it** — `test_stager_excludes_search_sidecar`, listed under
FM-PERSISTENCE-018. Nothing was added on the restore side: `IndexLifecycleManager::recover`
(`core/src/shard/search/lifecycle.rs:357-395`) still reads the definition out of `search_meta`,
calls `ShardSearchIndex::open` against the node's own (empty) search dir, and pushes
`RecoveryOutcome::Recovered { num_docs }` at `:381-383` with no comparison against the shard's key
count. `frogdb-search` and the core search module were out of campaign scope, and no
`FM-PERSISTENCE-*` row covers the restored-index case — note FM-PERSISTENCE-019's Observable cell
("The restored checkpoint contains every write … *including search-index state*") reads as a
guarantee this issue shows is false for a restore into a fresh data dir, and should be corrected
with the fix. Crash half also intact, with line drift: `event_loop.rs:31` → `:32`
(`search_commit_interval = interval(Duration::from_secs(1))`) fired at old `:88-95` → `:104`, still
independent of `WriteEffectKind::WalPersistence` (`post_execution.rs:385`). The only restart test,
`server/tests/search.rs:1535 test_ft_survives_restart`, reuses the **same** data dir so the sidecar
is simply still on disk — it cannot see this; `test_ft_search_bgsave_flushes_search` (`:1616`) never
restarts. The option decision (a/b/c) is still unrecorded, so this stays `needs-triage`.
