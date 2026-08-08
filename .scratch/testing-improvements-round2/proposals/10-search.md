# frogdb-search — testing gap audit (round 2)

## Scope

| path | LOC (src) | line cov | notes |
|---|---|---|---|
| `frogdb-server/crates/search/src/` | 10 031 | 6 022 / 7 007 = **85.9 %** | 11 inline `#[cfg(test)]` mods, **no `tests/` dir** |
| `frogdb-server/crates/core/src/shard/search/` | 2 337 | — | 1 inline test mod (`lifecycle.rs:463`) |
| `frogdb-server/crates/core/src/shard/search_hook.rs` | 193 | 83 / 90 = 92 % | write-path index hook, no inline tests |
| `frogdb-server/crates/server/src/connection/search/` | 2 258 | — | coordinator; `merge.rs` has 20 good unit tests |

Per-file low points: `search/src/schema.rs` **73.9 %**, `persistence/src/serialization/search.rs` 72.2 %
(unrelated — JSON/vectorset, not FT), `server/src/commands/search.rs` **53.4 %**,
`core/src/shard/search/query.rs` 435/701 = **62 %**.

Depth classes over the 1 235 functions in `search` + `core/src/shard/search`:

| class | count |
|---|---|
| untested | 406 (33 %) |
| single-test | 337 (27 %) |
| monoculture | 229 (19 %) |
| well-covered | 210 (17 %) |
| covered | 53 (4 %) |

**Correction to the dispatch brief**: `execute_ft_hybrid` (`core/src/shard/search/query.rs:255`) is
*not* zero-execution. The `untested rc=0/576 exec=0` row is a **generic-instantiation artifact** —
the `::<_>` shape emitted alongside the monomorphised copies. The deduplicated reading is
`monoculture, 337/576 regions, 8 tests, 1 suite`. Twenty-two of its inner closures *are*
genuinely `untested rc=0` — every one of them an argument-parsing branch (`RANGE`, `EPSILON`,
`EF_RUNTIME`, `SLOP`, `INFIELDS`, `GEOFILTER`, `FILTER`, `RETURN`). That is the real gap and it
is what F11 covers. I chased it down first, then looked for siblings; the siblings turned out to
be much bigger than the seed.

## Summary

Search is the only major subsystem in FrogDB whose primary data structure is **derived, held
outside RocksDB, and never reconciled against the dataset**. Tantivy lives in a plain filesystem
directory under `<data_dir>/search`, commits on an *independent 1-second timer*
(`event_loop.rs:31`), is deliberately excluded from snapshots (`snapshot/stager.rs:9-11`) and from
replication full-sync (`stager.rs:100-101`), and `IndexLifecycleManager::recover` reopens whatever
happens to be on the local disk with `Index::open_or_create` (`search/src/index.rs:258`) — which
**silently creates an empty index** when the directory is absent. Nothing anywhere compares index
doc-count against dataset key-count. The consequence is a whole class of bugs that produce *no
error, ever*: a replica that full-syncs an index and answers `FT.SEARCH` with zero hits; a
crash-restart where the index sits at a different commit point from the WAL-recovered dataset;
`FT.ALTER` on a JSON index which wipes the directory and rebuilds it from a rescan closure that
only knows how to read hashes.

Alongside that, two client-reachable crashes are live in the tree today. `FT.SEARCH … LIMIT 0 0`
— the canonical RediSearch "count only" idiom — reaches `TopDocs::with_limit(0)`, which
`assert_ne!`s (F1). And the KNN/hybrid path passes a client-supplied `PARAMS` blob straight into
usearch with no dimension check, so a wrong-length query vector is an out-of-bounds read in C++
(F2). Neither has a test, and grepping the whole repo finds **zero** occurrences of `LIMIT 0 0`.

Testing shape is the third problem. All 150 FT.* tests live in one 7 912-line socket file
(`server/tests/search.rs`) containing **135 `tokio::time::sleep` calls** — because the 1 s commit
timer is the only way a write becomes visible and there is no seam to force it. There are **zero**
FT.* tests at the `shard_driver` level, and the `search` crate has no `tests/` dir at all even
though `ShardSearchIndex::open_in_ram` exists precisely for cheap in-process testing (and is
itself `untested`, 0/41 regions). The counter-example is right there in the tree:
`server/src/connection/search/merge.rs:659` unit-tests the cross-shard merge from hand-built
partials with no server at all, and it is the best-tested search code in the repo.

## Existing test inventory

| surface | covers | strengths | blind spots |
|---|---|---|---|
| `search/src/*` inline (11 mods) | schema build, index/search/delete round-trips, geo, synonyms, aggregate reducers + merge, vector sidecar load/save, hybrid fusion | vector sidecar consistency is genuinely excellent (`vector.rs:598-640` white-box "lying field" tests); aggregate merge tested with real 2-partial merges | all in-process, single index, tiny corpora; no resource bounds; no `open_in_ram`; no adversarial numeric args |
| `core/src/shard/search/lifecycle.rs:463` (11 tests) | create/drop/alter persist ordering, rollback, `recover` idempotence, corrupt-index quarantine, undeserializable metadata, fatal CF failure | best restart-recovery coverage in the area; explicit failure-mode tests | every test is `single-test` class; **all assert on `recover` alone — none pairs it with a dataset** |
| `server/tests/search.rs` (150 tests, 7 912 LOC, 424 asserts) | every FT.* command over RESP; 135 FT.SEARCH, 126 FT.CREATE, 20 FT.AGGREGATE, 8 FT.HYBRID | broad command surface; some genuinely sharp expiry tests (`:509`, `:1222` drive `DEBUG SET-ACTIVE-EXPIRE`) | **135 `sleep()` calls**; 5 FT.ALTER tests, all HASH; 1 FT.DROPINDEX test; no restart; no replica; no `LIMIT 0 0`; no deep offset |
| `redis-regression/tests/search_regression.rs` (1 387 LOC) | parity for 24 FT.* verbs, heaviest on FT.SEARCH (29) / FT.CREATE (16) / FT.SUGADD (11) | real-Redis parity for reply shapes | 8 FT.AGGREGATE, 3 FT.ALTER, no FT.HYBRID, no restart/replica |
| `server/src/connection/search/merge.rs:659` (20 tests) | cross-shard merge: BM25 order, numeric/string SORTBY, global offset+limit, KNN ascending, shard-error-wins, typed aggregate partials | **model boundary** — constructed partials, no server, fast, precise | only merge; never paired with a real multi-shard shard-level result |
| `testing/fuzz/` | `search_query_parse`, `search_expr_parse`, `search_expr_eval`, `ft_create_parse`, `aggregate_parse`, `aggregate_pipeline_eval` | expression eval + pipeline eval are fuzzed with structured `Arbitrary` inputs | `search_query_parse` fuzzes standalone `parse_query` only — **not** the schema-bound `QueryParser::parse_with_geo_filters` the real search path uses |
| `core/tests/shard_driver/` | — | — | **no FT.* coverage whatsoever** |

---

## Findings

### F1: `FT.SEARCH … LIMIT 0 0` panics the shard worker

- **Severity** 4 — a panic inside the `ShardWorker` event-loop task kills the shard; every key on
  it becomes unreachable for the process lifetime. Unavailability, not data loss.
- **Likelihood** 5 — `LIMIT 0 0` is the documented RediSearch idiom for "give me the match count
  without documents", emitted by redis-py / node-redis / redisearch-go helpers on default config.
- **Effort** 1 — pure unit test on `ShardSearchIndex::search`.
- **Priority** **21**
- **Evidence**: `frogdb-server/crates/search/src/wire.rs:121-122` parses `LIMIT 0 0` to
  `offset=0, limit=0`; `wire.rs:337-340` `index_options()` yields `SearchOptions { offset: 0,
  limit: self.offset + self.limit }` = `limit: 0`; `frogdb-server/crates/search/src/index.rs:571`
  computes `fetch_limit = offset + limit = 0`; `index.rs:699` calls `TopDocs::with_limit(0)`, and
  tantivy 0.26.1 `src/collector/top_score_collector.rs:94` is
  `assert_ne!(limit, 0, "Limit must be greater than 0")`. No clamp exists in
  `server/src/commands/search.rs` or `connection/search/query.rs`. No `catch_unwind` guards the
  shard event loop (the only two in the workspace are in a test and in `scripting/gate.rs:374`).
  Second reachable path: `FT.HYBRID … COMBINE RRF 0` → `core/src/shard/search/query.rs:437`
  `combine_count = 0` → `:701` `count = 0` → `index.rs:1098` `fetch_count = window * 0 = 0` →
  same assert. Third: `VSIM … KNN <n> 0`. `rg '"0", "0"'` over both search test files returns
  nothing.
- **Proposed test**: table-driven over `ShardSearchIndex::search` with `(offset, limit)` ∈
  `{(0,0), (0,1), (5,0), (0,usize::MAX)}` asserting each returns `Ok`/`Err` and **never
  panics**; `(0,0)` must return `total == <match count>` with an empty `hits` vec. Plus one
  crate-level `hybrid_search` case with `count = 0`. Plus one RESP-level regression pin that
  `FT.SEARCH idx * LIMIT 0 0` replies `[N]` and the server is still alive on the next command.
- **Boundary**: **1** (pure unit) for the matrix — it is an arithmetic edge in one function — plus
  a single level-4 liveness pin, because "the shard survives" is only observable over the socket.

### F2: KNN/hybrid query vectors are never dimension-checked before entering usearch

- **Severity** 4 — `usearch::Index::search` forwards the `&[f32]` through cxx to C++, which reads
  `dimensions` floats from the pointer regardless of slice length. A short query vector is an
  out-of-bounds heap read: segfault, or adjacent heap bytes silently folded into distance scores.
- **Likelihood** 4 — dimension mismatch is the single most common vector-database client error
  (wrong embedding model, f64 instead of f32, truncated blob). Ordinary application bug.
- **Effort** 1 — plain unit test on `VectorFieldManager`.
- **Priority** **19**
- **Evidence**: `frogdb-server/crates/search/src/vector.rs:354-358` — `fn knn` passes `query`
  straight to `self.index.search(query, k)` with no `query.len() == self.dim` check, while the
  *write* path `vector.rs:298-306` **does** validate (`"vector blob size mismatch: expected {}
  bytes, got {}"`). The asymmetry is the bug. Caller
  `core/src/shard/search/query.rs:98-101` builds the vector with
  `blob.chunks_exact(4).map(f32::from_le_bytes)` — `chunks_exact` **silently discards** a trailing
  partial chunk, so a 130-byte blob against a 32-dim index yields 32 floats and looks fine, while
  a 4-byte blob yields 1. Same construction at `query.rs:695-698` for hybrid. usearch 2.24.0's
  Rust binding (`rust/lib.rs:361`) declares `search_f32(query: &[f32], count: usize)` with no
  length assertion. `execute_ft_knn_search` is `monoculture` (4 tests, one suite).
- **Proposed test**: build a `dim=4` `VectorFieldManager`, index one valid vector, then
  `knn(&[1.0, 2.0], 1)` and `knn(&[], 1)` — assert `Err(SchemaError)` mentioning the expected
  dimension, and that the call does not read past the slice (run the suite under
  `RUSTFLAGS=-Zsanitizer=address` in the nightly fuzz job to make the OOB observable). Add a
  crate-level case for an odd-length blob asserting `chunks_exact` truncation is rejected rather
  than silently accepted.
- **Boundary**: **1** — the missing check belongs on `VectorField::knn` next to the one already
  on `VectorField::index`; testing it anywhere higher would only obscure which layer is at fault.

### F3: an index survives a full-sync/snapshot restore as an *empty* index, silently

- **Severity** 5 — `FT.SEARCH` on the replica returns zero hits, forever, with no error and no
  log after startup. A read-scaling deployment silently serves empty search results. Consistency
  violation between two nodes that both claim to hold the index.
- **Likelihood** 4 — attaching a new replica to a primary that already has an index is ordinary
  operation, as is restoring a node from a snapshot.
- **Effort** 4 — needs the primary/replica harness plus a restart plus index assertions.
- **Priority** **19**
- **Evidence**: `frogdb-server/crates/persistence/src/snapshot/stager.rs:9-11` — *"A snapshot
  deliberately does **not** include the search-index sidecar (`<data_dir>/search`)"*; `:100-101`
  — *"Replication full sync ships its own flat RocksDB checkpoint and never touches `search/`
  either."* The RocksDB checkpoint **does** carry the `search_meta` CF, so
  `IndexLifecycleManager::recover` (`core/src/shard/search/lifecycle.rs:357-431`) finds the
  index *definition*, calls `ShardSearchIndex::open` (`search/src/index.rs:252`), which does
  `Index::open_or_create` (`index.rs:258`) against the replica's own empty `<data_dir>/search`
  and **creates a fresh empty index**. `recover` then records
  `RecoveryOutcome::Recovered { num_docs: 0 }` (`lifecycle.rs:381-383`) — the *success* variant.
  Nothing compares `num_docs` to the shard's key count. The 11 `lifecycle.rs` tests all exercise
  `recover` in isolation with no dataset, so none can catch this. `rg` for a test that restarts a
  server and then issues `FT.SEARCH` returns nothing.
- **Proposed test**: primary with `idx` over 100 `user:*` hashes; assert `FT.SEARCH` returns 100.
  Attach a replica, wait for full sync, restart the replica so the staged checkpoint installs,
  then on the replica assert `FT.INFO idx` reports `num_docs == 100` **and** `FT.SEARCH idx *`
  returns 100. Companion assertion: `FT.INFO`'s `num_docs` must equal the number of prefix-matching
  keys — that single invariant is the cheap detector for this whole bug class and should be a
  helper reused by F4/F5/F6.
- **Boundary**: **5** (multi-node harness) — the behaviour *is* the full-sync transfer; no lower
  level can observe it.
- **OPTIONS**:
  - **(a) Multi-node replica test** (`test-harness` primary+replica, level 5). Highest fidelity,
    proves the real operator scenario end to end. Slow, and needs the restart step because of the
    known runtime-resync-not-installed behaviour (round-1 issue 61).
  - **(b) Single-node "restore into a fresh data dir" test** (level 3): create the index, BGSAVE,
    copy *only* the RocksDB checkpoint into a new data dir (mimicking what full sync ships),
    start a server on it, assert `FT.INFO num_docs`. Much faster and deterministic, and it
    isolates the same defect — but it asserts against a hand-simulated transfer rather than the
    real one, so a change to what full sync ships would not break it.
  - **(c) Pure unit test on `recover`** (level 1): populate `search_meta` with a definition,
    point `data_dir` at an empty dir, assert the outcome is *not* `Recovered`. Cheapest, but it
    pins the desired behaviour of a function that does not yet have it, and proves nothing about
    the transfer.
  - **Recommendation**: **(b) now, (a) as a follow-up.** (b) buys the detection at level-3 cost
    and will fail today; (a) is worth building once but should not gate the fix. (c) is
    worth adding *with* the fix, not instead of it.

### F4: index and dataset recover to different points after an unclean shutdown, permanently

- **Severity** 5 — the tantivy commit point and the WAL recovery point are independent. After a
  crash the index can be missing documents that exist (search returns fewer hits than the data
  supports) or holding documents that were rolled back (search returns keys that do not exist).
  Neither is detected, neither self-heals, and both persist across every subsequent restart.
- **Likelihood** 4 — any `SIGKILL`, OOM-kill, or host failure. The window is up to one full
  commit interval of writes.
- **Effort** 4 — needs a crash-restart harness with index assertions.
- **Priority** **19**
- **Evidence**: `core/src/shard/event_loop.rs:31` —
  `search_commit_interval = interval(Duration::from_secs(1))`, fired at `:88-95`, entirely
  independent of `WriteEffectKind::WalPersistence`
  (`core/src/shard/post_execution.rs:385-405`). The only synchronising hook is the *pre-snapshot*
  one (`server/src/server/init.rs:353-366`), which orders the search flush before the WAL drain
  for BGSAVE only — nothing equivalent exists on the crash path. `recover`
  (`lifecycle.rs:357-431`) reopens the tantivy dir and never reconciles it against the store.
  `search_hook.rs` has no inline tests. Round 1's issue 12 (`durability-fsync-boundary`) and 14
  (`wal-recovery-mode-pin`) pinned WAL durability but never touched the search index — this is
  exactly the residue they left.
- **Proposed test**: with active expiry off, write N hashes, let the index commit, write M more,
  hard-kill within the commit interval, restart, and assert `FT.INFO num_docs == N + M` (or, if
  the design decision is that the index is allowed to lag, assert it converges within a bounded
  time and that the *divergence is detected and logged* rather than silently accepted). A
  property/model formulation is the stronger version: a `testing/` workload that interleaves
  writes, deletes and crashes, with a conservation checker asserting
  `index_docs == store_keys_matching_prefix` at every quiescent point. That is the same shape as
  the existing conservation checkers in `crates/testing/` and should reuse them.
- **Boundary**: **5** — needs fault injection. Recommend it be built as a checker in
  `crates/testing/` rather than a one-off example test, because the invariant
  (`index ≡ dataset ∩ prefix`) is the same one F3, F5 and F6 all violate, and one checker catches
  all four.

### F5: `FT.ALTER` on a JSON-source index destroys every document

- **Severity** 4 — `reopen_with_def` deletes the whole tantivy directory, and the rescan closure
  that refills it only reads hashes. On an `ON JSON` index every document is silently dropped;
  `FT.ALTER` returns `+OK` and every subsequent `FT.SEARCH` returns zero hits, permanently.
- **Likelihood** 3 — adding a field to an existing JSON index is a routine schema evolution step.
- **Effort** 2 — crate/shard-level test.
- **Priority** **16**
- **Evidence**: `core/src/shard/search/index_mgmt.rs:128-141` — the `alter` scan closure is
  `if let Some(value) = store.get(&key) && let Some(hash) = value.as_hash() { idx.index_hash(...) }`
  with **no `IndexSource::Json` branch at all**. Compare the *sibling* `execute_ft_create`
  closure at `core/src/shard/search/create.rs:36-52`, which correctly does
  `let is_json = def.source == IndexSource::Json;` and dispatches to `index_json` /`index_hash`.
  The destructive half is `search/src/index.rs:1010-1015` — `std::fs::remove_dir_all(path)` then
  `Index::open_or_create` — so this is not a stale-doc bug, it is a full wipe. All five
  `FT.ALTER` tests in `server/tests/search.rs` (`:147`, `:187`, `:206`, `:251`, `:2949`) use HASH
  indexes; the file contains 8 `"JSON"` occurrences and none of them is an ALTER. The regression
  suite has 3 `FT.ALTER` uses, also HASH. `execute_ft_alter::{closure#0}::{closure#0}` is
  `covered`, 4 tests, 25/27 regions — the two uncovered regions are the JSON path that does not
  exist.
- **Proposed test**: create `ON JSON` index over `doc:*` with 3 JSON documents, assert
  `FT.SEARCH` finds 3; `FT.ALTER … SCHEMA ADD newfield TEXT`; assert `FT.SEARCH` **still** finds 3
  and that a query on `newfield` matches. Mirror it for HASH so the two sources are pinned
  together. Also assert `FT.ALTER` preserves VECTOR-field contents (the sidecar is rebuilt from
  scratch at `index.rs:1034`).
- **Boundary**: **3** (`shard_driver`) — this is pure command semantics against a real
  `ShardWorker` and store; it needs no socket, no routing, and no RESP. Today it *cannot* be
  written there because `shard_driver` has zero FT.* support, which is the enabling work in F15.
  Until then, level 4.

### F6: unbounded `LIMIT` / deep offset allocates eagerly and OOM-aborts the process

- **Severity** 4 — tantivy's `TopNComputer` allocates `Vec::with_capacity(top_n * 2)` **per
  segment, up front**, before collecting anything. `LIMIT 1000000000 10` requests a 2 × 10⁹-entry
  vector per segment: an immediate multi-gigabyte allocation and an abort. Process-level
  unavailability from one command.
- **Likelihood** 3 — an application paging bug (offset computed from a user-controlled page
  number) or a trivially adversarial client. Requires no special config.
- **Effort** 1 — pure unit test.
- **Priority** **17**
- **Evidence**: `search/src/index.rs:571` `(fetch_offset, fetch_limit) = (offset, offset + limit)`
  — the offset is *added to* the limit, so deep offsets scale the allocation linearly;
  `index.rs:594 / 634 / 699` all pass `fetch_limit` to `TopDocs::with_limit`; tantivy 0.26.1
  `src/collector/top_score_collector.rs:609-611` —
  `let vec_cap = top_n.max(1) * 2; buffer: Vec::with_capacity(vec_cap)`. No upper clamp exists in
  `wire.rs` (`:121-122` parses any `usize`), in `commands/search.rs`, or in `index.rs`. Related
  second allocation: the geo path at `index.rs:568-569` uses `(0, raw_total.max(1))` — i.e. it
  fetches **every matching document** whenever a `GEOFILTER` is present, so a geo query matching
  10 M docs allocates 20 M entries regardless of the client's `LIMIT`. `rg` finds no test using a
  `LIMIT` above 1 000.
- **Proposed test**: assert `FT.SEARCH` with `LIMIT 0 100000000` returns an error (or is clamped
  to a documented `MAXSEARCHRESULTS`-style bound) rather than allocating; assert a GEOFILTER query
  over a corpus larger than the requested limit does not allocate proportional to corpus size.
  Redis's own `MAXSEARCHRESULTS` / `MAXAGGREGATERESULTS` `FT.CONFIG` knobs are the parity model
  here and are the natural place to put the clamp.
- **Boundary**: **1** for the clamp behaviour (it is one arithmetic guard in `search()`), plus
  a level-3 pin that the `FT.CONFIG` knob is actually consulted, if the fix routes through config.

### F7: writes are invisible to `FT.SEARCH` for up to 1 s and there is no seam to force a commit

- **Severity** 3 — a client's own `HSET` is not visible to its next `FT.SEARCH` on the same
  connection. Wrong answer on a data path a user notices immediately. (Eventual-consistency for
  search is a defensible *design*; the finding is that it is undocumented, untested, unbounded on
  the write path, and has no escape hatch.)
- **Likelihood** 5 — every write-then-search sequence on default config.
- **Effort** 2 — the mechanism (`SearchMsg::FlushSearchIndexes`) already exists.
- **Priority** **17**
- **Evidence**: `core/src/shard/post_execution.rs:406-411` — `WriteEffectKind::SearchIndex`
  calls `apply_reindex`, which marks the writer dirty (`search/src/index.rs:449`) but never
  commits. The only commits are the 1 s timer (`event_loop.rs:88-95`), shutdown
  (`event_loop.rs:106-118`), the pre-snapshot hook (`dispatch_search.rs:8-18`), and `create`/
  `alter` (`lifecycle.rs:233, 329`). The query path
  (`core/src/shard/execution.rs:834-871`) commits nothing. The test suite's own comment admits
  it: `server/tests/search.rs:547-551` — *"visibility to FT.SEARCH follows on the shard's periodic
  search-commit tick (1s cadence)"*. The cost is `rg -c 'sleep' server/tests/search.rs` = **135**.
  Every one of those is a wall-clock race and a guaranteed source of instrumented-build flakes
  (the retired coverage-depth doc recorded timing-sensitive tests failing under
  instrumentation; see git history).
- **Proposed test**: two parts. **(i)** Behaviour pin: document and assert the contract —
  either `HSET` then `FT.SEARCH` on one connection is immediately consistent (then the write path
  must commit, and the test asserts it with no sleep), or it is explicitly eventual (then the test
  asserts convergence within a stated bound and the divergence is documented). This is a design
  decision that should be surfaced, not a test to write blind. **(ii)** Test-infrastructure:
  expose `SearchMsg::FlushSearchIndexes` as `DEBUG FLUSH-SEARCH-INDEX` (it is already plumbed to
  every shard and already awaited by the snapshot hook), then mechanically replace all 135
  `sleep()` calls. This deletes ~70 s of wall-clock from the suite and removes the flake class.
- **Boundary**: **4** for (i) — read-your-writes across a connection is genuinely a
  connection-level property. (ii) is infrastructure, not a test.
- **OPTIONS**:
  - **(a) `DEBUG FLUSH-SEARCH-INDEX` subcommand.** Reuses the existing message and the existing
    `DEBUG SET-ACTIVE-EXPIRE` precedent in this very file. Ships a test-only verb in the
    production binary (as `DEBUG` already does throughout).
  - **(b) Make the commit interval configurable** (e.g. `search-commit-interval`) and set it to
    something tiny in `TestServerConfig`. No new verb, and the knob is arguably useful to
    operators tuning the visibility/throughput trade-off. But it makes tests fast-polling rather
    than deterministic — sleeps become shorter, not absent.
  - **(c) Commit synchronously on the write path when any index is dirty.** Removes the class
    entirely and makes search read-your-writes consistent. Tantivy commits are expensive
    (segment flush + `fsync`), so this would be a serious write-throughput regression — almost
    certainly wrong as a default, possibly right behind a `FT.CONFIG` durability knob.
  - **Recommendation**: **(a)**, and treat the underlying consistency contract as a separate
    product decision to be written down in the domain docs. (c) is what RediSearch does *not* do
    either — it is also eventually consistent — so the honest fix is documentation plus a
    deterministic test seam.

### F8: `FT.AGGREGATE` silently truncates at 100 000 rows per shard

- **Severity** 3 — every aggregation over an index with more than 100 000 matching documents
  returns a *plausible but wrong* number, with no error, no warning, and no truncation flag.
  `COUNT` under-reports, `SUM`/`AVG` are computed over an arbitrary BM25-ordered prefix.
- **Likelihood** 4 — 100 k documents is a small index; this is normal operation at any real scale.
- **Effort** 2 — crate-level test.
- **Priority** **15**
- **Evidence**: `core/src/shard/search/query.rs:178-180` —
  `idx.search(&request.query, &SearchOptions::page(0, 100_000))` with the comment *"Execute search
  to get ALL matching rows (no limit, offset 0)"*, which the constant contradicts. The cap is
  per-shard, so the effective ceiling scales with shard count in a way no client can predict. The
  20 `FT.AGGREGATE` tests in `server/tests/search.rs` and the 8 in the regression suite all use
  corpora of a handful of documents. `execute_ft_aggregate` is `well-covered` by count (24 tests)
  but 74/103 regions — a textbook monoculture: many tests, one tiny-corpus angle.
- **Proposed test**: index 100 001 documents, run
  `FT.AGGREGATE idx * GROUPBY 0 REDUCE COUNT 0 AS n` and assert `n == 100001` — or, if the cap is
  intentional, assert the command *errors* rather than silently truncating. Also assert the same
  for `SUM` over a field whose true total is known. Cheapest honest version: make the cap
  injectable and test at 10 + 11 documents rather than materialising 100 k.
- **Boundary**: **2** (crate-level, `ShardSearchIndex` + `execute_shard_local`) — the truncation
  is in the search-options construction and needs no shard, no store and no socket. Building
  100 001 documents through a socket would take minutes; in-RAM via `open_in_ram` it is
  sub-second, which is exactly the argument for F15.

### F9: logically-expired keys are returned by `FT.SEARCH` with stale content

- **Severity** 3 — `FT.SEARCH` serves document content out of tantivy's stored fields, never out
  of the store, so a key whose TTL has passed but which has not yet been reaped by lazy read or
  the active sweep is returned as a live hit with its full (stale) field values. `GET` on the same
  key returns nil in the same instant.
- **Likelihood** 4 — the window is the active-expiry sweep interval on every TTL'd key; with
  `DEBUG SET-ACTIVE-EXPIRE 0`, or on a key never read again, it is unbounded.
- **Effort** 2.
- **Priority** **15**
- **Evidence**: `search/src/index.rs:709` — `let (key, fields) = self.extract_hit_fields(&doc);`
  reads from the tantivy document, and `search()` never consults the store. De-indexing happens
  only on the removal paths: `core/src/shard/worker.rs:736` (lazy purge) and `:761` (hash
  emptied), and via `run_internal_removal_effects` for the active sweep and for eviction. Every
  one of those is *after* the key is actually removed, so the gap between logical expiry and
  physical removal is a window where the index disagrees with the store. Existing tests
  (`server/tests/search.rs:509-560`, `:1219-…`) test the *post-reap* state correctly — they
  disable active expiry, force a lazy read, sleep 1 500 ms and assert the key is gone — but none
  asserts the state *between* expiry and reap. Note this window is genuinely different from the
  index-lag window in F7: here the index is not lagging a write, it is serving data that the
  dataset no longer contains.
- **Proposed test**: `DEBUG SET-ACTIVE-EXPIRE 0`; index `user:1`; `PEXPIRE` and
  `DEBUG EXPIRE-BACKDATE` it into the past; *without* reading the key, force a search-index
  commit and assert `FT.SEARCH idx *` does **not** return `user:1` — or, if serving it is the
  accepted divergence, pin that explicitly and document it, so a future change cannot flip the
  behaviour silently. Both DEBUG verbs already exist and are used in this file.
- **Boundary**: **4** — needs `DEBUG SET-ACTIVE-EXPIRE` and `DEBUG EXPIRE-BACKDATE`, which are
  connection-level verbs. Would drop to **3** if `shard_driver` gained FT.* support (F15), since
  it can already drive ticks and expiry directly.

### F10: cross-shard aggregate merge ships unbounded per-shard state to the coordinator

- **Severity** 3 — `COUNT_DISTINCT` merges by unioning the **full** value set from every shard and
  `QUANTILE` by concatenating **every** observed f64. A high-cardinality `GROUPBY` materialises the
  entire distinct-value set of the dataset in the coordinator connection's memory, per group.
- **Likelihood** 3 — `GROUPBY @user_id REDUCE COUNT_DISTINCT 1 @session` over a large index is an
  ordinary analytics query.
- **Effort** 2.
- **Priority** **13**
- **Evidence**: `search/src/aggregate.rs:1011-1018` — `CountDistinct` merge is
  `for val in sset { dset.insert(val.clone()) }` over a `HashSet<String>`;
  `aggregate.rs:1080-1085` — `Quantile` merge is `dvals.extend(svals)` over a `Vec<f64>` which
  `finalize_state` then clones and sorts (`:1165-1169`). `CountDistinctish` is correctly bounded
  (fixed HLL register array, element-wise max at `:1019-1029`) — the exact/approximate pair is
  present and only the approximate one is safe. `test_count_distinct_reducer`
  (`aggregate.rs:1514`) and `test_quantile_reducer` (`:1614`) are both `single-test` and both use
  a single partial with a handful of values; only `test_merge_partials_sum`/`_avg` and
  `test_countdistinctish_merge` exercise a real two-partial merge.
- **Proposed test**: merge two partials each holding a large distinct set and assert either a
  bounded memory profile or an explicit error; and assert `QUANTILE` merge is correct across two
  partials at all (currently untested at >1 partial — worth pinning independently of the bound,
  since concatenate-then-sort *is* exact and should be locked in before anyone replaces it with a
  t-digest).
- **Boundary**: **1** — `merge_states` is a pure function over constructed partials. This is the
  boundary `merge.rs:659` already uses correctly and it is the cheapest test in the whole audit.

### F11: `FT.HYBRID` silently ignores `RANGE`/`RADIUS` and `EF_RUNTIME`, and silently defaults malformed options

- **Severity** 3 — `FT.HYBRID … VSIM @v $b RANGE 2 RADIUS 0.3` returns KNN results ignoring the
  radius entirely: a *different result set* than asked for, reported as success. Malformed numeric
  options fall back to hard-coded defaults rather than erroring.
- **Likelihood** 3 — `RANGE` is part of the documented hybrid grammar the parser accepts.
- **Effort** 2.
- **Priority** **13**
- **Evidence**: `core/src/shard/search/query.rs:748` — `let _ = range_radius; // RANGE mode: for
  future use`, after `:372-391` has fully parsed it. `_ef_runtime` (`:279`, parsed at `:365` and
  `:396`) is likewise never read. The silent-default pattern runs through the whole parser:
  `:447-450` `CONSTANT` → `.unwrap_or(60.0)`, `:457-460` `ALPHA` → `0.5`, `:467-470` `BETA` →
  `0.5`, `:477-480` `WINDOW` → `3`. So `COMBINE LINEAR 10 ALPHA banana` scores with α = 0.5 and
  reports success. Coverage: `execute_ft_hybrid` is `monoculture` (8 tests, 1 suite, 337/576
  regions) and **22 of its inner closures are `untested` with 0 regions covered** —
  `query.rs:355, 367, 383, 398` (the RANGE/EPSILON/EF_RUNTIME parse arms), `:449, 479` (CONSTANT,
  BETA), `:526, 550, 570, 583, 595` (RETURN, INFIELDS, SLOP, FILTER bounds), `:618, 622, 626`
  (GEOFILTER lon/lat/radius). Only 8 `FT.HYBRID` invocations exist in the entire test suite and
  the regression suite has none.
- **Proposed test**: a table over the hybrid grammar asserting each option is either **honoured**
  (observable in the result set or in `YIELD_SCORE_AS` output) or **rejected**; specifically that
  `RANGE … RADIUS r` filters by radius, and that `ALPHA banana` is an error rather than 0.5.
- **Boundary**: **3** (`shard_driver`) — `execute_ft_hybrid` is a `ShardWorker` method taking
  `&[Bytes]`; a driver test can call it directly with constructed args and assert the parse
  outcome, which is far sharper than inferring it from merged RESP output. Blocked on F15;
  level 4 until then.

### F12: `STDDEV` returns 0 for genuinely varying data (catastrophic cancellation)

- **Severity** 3 — wrong answer on a documented reducer, silently.
- **Likelihood** 2 — needs values whose magnitude is large relative to their spread (timestamps,
  prices in cents, IDs) — common in exactly the analytics workloads that use STDDEV, but not
  universal.
- **Effort** 1.
- **Priority** **12**
- **Evidence**: `search/src/aggregate.rs:1152-1159` — `variance = (*sum_sq / n) - (*sum / n)
  .powi(2)` then `variance.max(0.0).sqrt()`. This is the textbook naive formula. For values around
  1e8 with a spread of ~1, `sum_sq/n` and `(sum/n)²` agree to more digits than f64 carries, so the
  difference is noise — frequently negative, at which point `.max(0.0)` clamps it and STDDEV
  reports exactly `0`. `test_stddev_reducer` (`aggregate.rs:1587`) is `single-test` and uses small
  integers where the formula is fine. The merge itself (`:1064-1079`, summing `sum`/`sum_sq`/
  `count`) is algebraically correct — the defect is purely in finalisation, so the fix is Welford
  or a shifted-mean accumulator and does not disturb the merge contract.
- **Proposed test**: `STDDEV` over `[100000000.0, 100000001.0, 100000002.0]`, assert the result is
  within 1e-6 of 1.0 (today it returns 0). Add the same values split across two partials to pin
  that the merge stays correct under whatever accumulator replaces it.
- **Boundary**: **1** — pure function on constructed state.

### F13: tiered-spilled documents return null content from `FT.HYBRID`/KNN but full content from `FT.SEARCH`

- **Severity** 3 — the same document, matched by two different query verbs on the same index,
  returns its fields from one and `nil` from the other.
- **Likelihood** 2 — requires a tiered-storage eviction policy plus vector/hybrid queries.
- **Effort** 2.
- **Priority** **11**
- **Evidence**: `FT.SEARCH` reads content from tantivy (`search/src/index.rs:709`), but the KNN
  path reads from the store — `core/src/shard/search/query.rs:115` `store.get(&Bytes::from(hit
  .key.clone())).map(|value| …)`, so a `None` yields `fields: None`. Same construction for hybrid
  at `query.rs:756`. A spill deliberately leaves the search index untouched
  (`core/src/shard/eviction.rs:286-292` documents that a spill "is TIERING, **not** a removal"
  and must stay invisible to clients) — which is right for `FT.SEARCH` and wrong for KNN/hybrid,
  because those two resolve content through the store rather than the index. Whether `Store::get`
  transparently unspills on this path is the open question the test would settle.
- **Proposed test**: with a tiered policy, index documents with vectors, force a spill of a
  matching key, then assert `FT.SEARCH` and `FT.HYBRID`/KNN return **identical** field content for
  that key.
- **Boundary**: **3** (`shard_driver`) — tiered storage is already driven at that level
  (`core/tests/tiered_storage.rs`), so this is the natural home once FT.* is reachable there.

### F14: numeric arguments across the FT wire parser silently default instead of erroring

- **Severity** 2 — `LIMIT 0 -1` becomes `limit = 10`; a garbage `SLOP`, `DIALECT` or `TIMEOUT`
  is ignored. Wrong-but-plausible results and a divergence from RediSearch, which errors.
- **Likelihood** 3 — `-1` for "unlimited" is a near-universal client assumption carried over from
  `LRANGE`/`ZRANGE`.
- **Effort** 1.
- **Priority** **11**
- **Evidence**: `search/src/wire.rs:508-510` — `parse_num` is
  `from_utf8(arg).ok().and_then(|s| s.parse().ok())`, returning `Option`, and every call site
  discards the `None`: `:121-122` `LIMIT` → `unwrap_or(0)` / `unwrap_or(10)`. Same pattern
  throughout `core/src/shard/search/query.rs` (`:499-502` `PARAMS` count → `unwrap_or(0)`,
  `:524-527` `RETURN` count, `:548-551` `INFIELDS` count). Negative-path coverage is the gap the
  brief calls out as "usually cheap and high-severity"; here it is cheap and medium-severity, but
  it is a handful of table rows.
- **Proposed test**: table over `wire::parse_ft_search_request` asserting that each malformed
  numeric argument produces `Err` (or the RediSearch-parity error string) rather than a default.
  Pair with a `redis-regression` case for the exact error text.
- **Boundary**: **1** for the parse table (`wire.rs` is a pure parser, already unit-tested at
  `:537`), plus a level-4 parity pin only for the error string.

### F15: no FT.* coverage exists at any boundary below the socket

- **Severity** 3 — this is a meta-finding, but it is the reason five of the findings above carry
  effort 3–4 instead of 2. It is also why F5 and F8 went undetected: both are cheap to catch at
  level 2–3 and prohibitively expensive at level 4.
- **Likelihood** 5 — it shapes every future search test written.
- **Effort** 3 — add FT.* `ScatterOp` seams to the `shard_driver` harness; add a `tests/` dir to
  the `search` crate using `open_in_ram`.
- **Priority** **11** (3·3 + 2·5 − 3 = 16 by formula; deliberately reported lower because it is
  enabling work rather than a bug — see note)
- **Evidence**: `rg -l 'FT\.' core/tests/` returns **nothing** — the `shard_driver` harness has
  zero FT.* coverage despite `execute_ft_*` being ordinary `ShardWorker` methods reached through
  `ScatterOp` (`core/src/shard/execution.rs:834-889`). The `search` crate has **no `tests/`
  dir**, and `ShardSearchIndex::open_in_ram` (`search/src/index.rs:287`), which exists explicitly
  "for testing", is `untested` at 0/41 regions — the crate's own inline tests all use
  `TempDir` + mmap instead. Meanwhile 150 tests and 7 912 lines sit in `server/tests/search.rs`
  with 135 sleeps. The anti-pattern the brief names is present at scale here: `FT.TAGVALS`,
  `FT.DICTADD`/`DICTDEL`/`DICTDUMP`, `FT.EXPLAIN`, `FT.SYNDUMP` and `FT.CONFIG GET` are all
  single-shard, non-blocking, non-RESP-specific operations tested exclusively through a full
  client + connection + scatter/merge flow.
- **Proposed work**: (i) add `tests/` to `frogdb-search` built on `open_in_ram` — this is where
  F1, F2, F6, F8, F10, F12 and F14 belong and it makes 100 k-document corpora sub-second;
  (ii) add FT.* drive seams to `shard_driver` — this is where F5, F11 and F13 belong;
  (iii) migrate the pure-semantics socket tests down. The migration is not urgent; the *new*
  tests landing at the right level is.
- **Boundary**: infrastructure. **Recommendation**: do (i) first — it is a new directory with no
  existing-test churn and it unblocks the six cheapest findings immediately. (ii) is a larger
  change to a shared harness and should be sequenced with whoever else is proposing
  `shard_driver` extensions this round.

---

## Deprioritised

- **Schema-bound query-parser fuzzing.** `testing/fuzz/fuzz_targets/search_query_parse.rs` fuzzes
  standalone `parse_query` only, not `QueryParser::parse_with_geo_filters` — the path the real
  search actually takes, with field resolution and geo-companion lookup. Worth extending the
  existing target (~10 lines) but not worth a finding: round 1's issue 40 already established
  continuous fuzzing, and the parse surface is the *best*-defended part of this crate.
- **`FT.CONFIG SET` persistence** (`lifecycle.rs:155` `persist_search_config`, `single-test`,
  1 execution). Wrong config survives restart — severity 2, and the surrounding
  alias/dictionary persistence is well covered by `recover_restores_aliases_and_dictionaries`.
- **`RANDOM_SAMPLE` reservoir merge bias** (`aggregate.rs:1097-1113`). The cross-partial
  reservoir combination is not a uniform sample. Statistically wrong, but the reducer is explicitly
  a *sample*; no user contract is violated. (I did check the `*dseen = *dseen + sseen -
  sres.len()` subtraction for `usize` underflow — it is safe, since the loop above increments
  `dseen` once per element of `sres`.)
- **`FIRST_VALUE` without a `BY` clause is shard-order-dependent** (`aggregate.rs:1060-1063`:
  a `sort_key: None` source is a no-op merge, so the winner is whichever partial arrived first).
  RediSearch leaves this unspecified too. Non-deterministic but not wrong.
- **`MIN`/`MAX` over an empty group return the string `"0"`** (`aggregate.rs:1124-1137`), which
  collides with a legitimate zero. Cosmetic-to-severity-2; would be a one-line parity test if
  someone is already in the file.
- **`glob_match_simple`** (`core/src/shard/search/mod.rs:23`, `covered`, 16/25 regions) — handles
  only `*`-prefix/suffix/both, so `FT.CONFIG GET FOO*BAR` misbehaves. Severity 1; `FT.CONFIG GET`
  patterns are an introspection nicety.
- **`format_strftime`** (`search/src/expression.rs:886`, `untested`, 0/184 regions) — the single
  largest untested function in the crate by region count. It is reached only via
  `APPLY timefmt(...)`, is already covered by the `search_expr_eval` fuzz target for panic-safety,
  and produces a formatted string rather than a decision. High region count, low consequence — a
  good example of why region count alone is a bad ranking signal.
- **`FT.DROPINDEX` has exactly one test** in `server/tests/search.rs`. The lifecycle-level
  behaviour (metadata-first ordering, failure leaves the index intact) *is* well tested at
  `lifecycle.rs:492` and `:532`, so the residue is only the RESP wrapper.

## Cross-area notes

- **Shared invariant, shared checker.** F3, F4, F5 and F9 are four faces of one missing
  invariant: `index_docs ≡ {store keys matching prefix, of matching type, not expired}`. It should
  be implemented **once**, as a conservation checker in `crates/testing/` alongside the existing
  ones, and asserted at every quiescent point of the existing fault-injection and restart
  workloads — not as four example tests. This is the single highest-leverage item in the audit and
  it needs coordination with whoever owns `crates/testing/`.
- **Persistence / replication agents**: `snapshot/stager.rs:9-11` and `:100-101` document that
  the search sidecar is excluded from both snapshots and full sync *by decision* (proposal 23,
  on the grounds that no restore-path reader existed). F3 argues that decision produces a silent
  empty index on every restored or newly-attached node. Whether the fix is "ship the sidecar" or
  "rebuild the index from the dataset after a restore" is a design question that belongs to those
  areas, not to search. Flagging it rather than deciding it.
- **Eviction agent**: `core/src/shard/eviction.rs:286-292` deliberately keeps spills out of the
  search-index removal path. That is correct for `FT.SEARCH` and produces F13 for KNN/hybrid,
  because those two resolve content through the store. Worth a joint look.
- **`DEBUG` verb owner**: F7 proposes `DEBUG FLUSH-SEARCH-INDEX`. The message
  (`SearchMsg::FlushSearchIndexes`) is already plumbed to every shard and already awaited by the
  BGSAVE hook, so this is a dispatch entry, not new machinery — but it is a new `DEBUG`
  subcommand and should go through whoever owns that surface.
- **Round-1 overlap**: none. No issue in `.scratch/testing-improvements/issues/` (01–66) touches
  search. F4 is the search-shaped residue of issues 12 (`durability-fsync-boundary`) and 14
  (`wal-recovery-mode-pin`), which pinned WAL durability without ever asking what the derived
  index was doing at the same moment. Issue 66 (`mutation-testing`) would independently catch the
  assertion-weak tests noted in the inventory.

## Findings by priority

| id | title | S | L | E | priority | boundary |
|---|---|---|---|---|---|---|
| F1 | `LIMIT 0 0` panics the shard worker | 4 | 5 | 1 | **21** | 1 (+4 pin) |
| F2 | KNN query vector dimension unchecked → OOB read | 4 | 4 | 1 | **19** | 1 |
| F3 | Full-sync/restore yields a silently empty index | 5 | 4 | 4 | **19** | 5 (OPTIONS) |
| F4 | Index/dataset diverge permanently after a crash | 5 | 4 | 4 | **19** | 5 |
| F6 | Unbounded `LIMIT` allocates eagerly → OOM | 4 | 3 | 1 | **17** | 1 |
| F7 | 1 s write-visibility lag, no forced-commit seam | 3 | 5 | 2 | **17** | 4 (OPTIONS) |
| F5 | `FT.ALTER` on a JSON index destroys every document | 4 | 3 | 2 | **16** | 3 |
| F8 | `FT.AGGREGATE` silently truncates at 100 k rows | 3 | 4 | 2 | **15** | 2 |
| F9 | Expired-unreaped keys returned with stale content | 3 | 4 | 2 | **15** | 4 |
| F10 | Unbounded `COUNT_DISTINCT`/`QUANTILE` merge state | 3 | 3 | 2 | **13** | 1 |
| F11 | `FT.HYBRID` silently ignores `RANGE`/`EF_RUNTIME` | 3 | 3 | 2 | **13** | 3 |
| F12 | `STDDEV` returns 0 via catastrophic cancellation | 3 | 2 | 1 | **12** | 1 |
| F13 | Spilled docs: null content from KNN, full from SEARCH | 3 | 2 | 2 | **11** | 3 |
| F14 | Numeric args silently default instead of erroring | 2 | 3 | 1 | **11** | 1 |
| F15 | No FT.* coverage below the socket (enabling work) | 3 | 5 | 3 | **11**\* | infra |

\* F15 scores 16 by formula; reported at 11 because it is enabling work, not a bug. Sequence it
first anyway — it drops the effort on F5, F8, F11 and F13.
