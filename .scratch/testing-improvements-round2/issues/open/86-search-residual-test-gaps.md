# search — residual test gaps (8 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/10 — residual findings after promotion to issues 19–76
Score: 8 findings, priority range 11–19
Area: `frogdb-server/crates/search/src/`, `core/src/shard/search/`, `core/src/shard/search_hook.rs`, `server/src/connection/search/`

## Context

This area is the full-text/vector search subsystem: the `search` crate (10,031 src LOC,
**6,022 / 7,007 = 85.9 % lines**, 11 inline `#[cfg(test)]` mods and **no `tests/` dir**),
`core/src/shard/search/` (2,337 LOC, 1 inline test mod), `core/src/shard/search_hook.rs`
(193 LOC, 83/90 = 92 %) and the cross-shard coordinator under
`server/src/connection/search/`. Per-file low points are `search/src/schema.rs` **73.9 %**,
`server/src/commands/search.rs` **53.4 %** and `core/src/shard/search/query.rs`
435/701 = **62 %**; depth classes over the 1,235 functions in `search` +
`core/src/shard/search` are `untested` 406 (33 %), `single-test` 337 (27 %), `monoculture`
229 (19 %), `well-covered` 210 (17 %), `covered` 53 (4 %). The proposal's verdict on the
*shape* of that coverage: search is "the only major subsystem in FrogDB whose primary data
structure is **derived, held outside RocksDB, and never reconciled against the dataset**",
and its testing shape is the third problem — all 150 FT.\* tests live in one 7,912-line
socket file with **135 `tokio::time::sleep` calls**, there are **zero** FT.\* tests at the
`shard_driver` level, and the `search` crate has no `tests/` dir at all even though
`ShardSearchIndex::open_in_ram` exists precisely for cheap in-process testing.

## Promoted elsewhere

- F1 → issue 63, `.scratch/testing-improvements-round2/issues/` (`FT.SEARCH … LIMIT 0 0` panics the shard worker)
- F3 → issue 46, `.scratch/testing-improvements-round2/issues/` (restored/attached nodes get a permanently empty search index) **and** issue 20, `.scratch/testing-improvements-round2/issues/` (theme T2 — failure of a derived structure reported as success)
- F4 → issue 46, `.scratch/testing-improvements-round2/issues/` (same defect, crash-restart half — index and dataset recover to different points) **and** issue 20, `.scratch/testing-improvements-round2/issues/` (theme T2)
- F5 → issue 45, `.scratch/testing-improvements-round2/issues/` (`FT.ALTER` on an `ON JSON` index destroys every document)
- F6 → issue 70, `.scratch/testing-improvements-round2/issues/` (unbounded allocations — FT deep-offset `2×(offset+limit)`)
- F9 → issue 22, `.scratch/testing-improvements-round2/issues/` (theme T4 — search returns expired-unreaped keys with stale content)
- F15 → issue 94, `.scratch/testing-improvements-round2/issues/` (no FT.\* coverage below the socket — the two missing search test seams). This one sits outside the 19–76 range: F15 is enabling infrastructure that was dropped from `INFRASTRUCTURE.md`'s I1–I18 consolidation and was filed separately as issue 94, so it is **not** re-filed here. F11 and F13 below are gated on it.

## Residual findings

### F2 — KNN/hybrid query vectors are never dimension-checked before entering usearch

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

### F7 — writes are invisible to `FT.SEARCH` for up to 1 s and there is no seam to force a commit

**BLOCKED on an unmade semantics decision** — `MASTER.md` §7 lists "search write-visibility
seam *(10/F7)*" among the decisions that must be made before the test can assert anything:
either write-then-search on one connection is immediately consistent, or it is explicitly
eventual. `INFRASTRUCTURE.md`'s "Explicitly *not* infrastructure" section settles the
mechanism half — `DEBUG FLUSH-SEARCH-INDEX` is a dispatch entry, not machinery, since
`SearchMsg::FlushSearchIndexes` is already plumbed to every shard and already awaited by the
BGSAVE hook — so all that is outstanding is sign-off from whoever owns the `DEBUG` surface,
plus the contract call for part (i).

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
  (`docs/agents/coverage-depth.md` already documents timing-sensitive tests failing under
  instrumentation).
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

### F8 — `FT.AGGREGATE` silently truncates at 100 000 rows per shard

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

### F10 — cross-shard aggregate merge ships unbounded per-shard state to the coordinator

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

### F11 — `FT.HYBRID` silently ignores `RANGE`/`RADIUS` and `EF_RUNTIME`, and silently defaults malformed options

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
- **Sequencing note**: F15's `shard_driver` FT.\* seams are issue 94,
  `.scratch/testing-improvements-round2/issues/`. Until they land this is a level-4 test.

### F12 — `STDDEV` returns 0 for genuinely varying data (catastrophic cancellation)

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

### F13 — tiered-spilled documents return null content from `FT.HYBRID`/KNN but full content from `FT.SEARCH`

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
  (`core/tests/tiered_storage.rs`), so this is the natural home once FT.\* is reachable there.
- **Sequencing note**: "once FT.\* is reachable there" is F15, filed as issue 94,
  `.scratch/testing-improvements-round2/issues/`. The proposal also flags this for a joint look
  with the eviction agent, since `eviction.rs:286-292` keeps spills out of the search-index
  removal path deliberately.

### F14 — numeric arguments across the FT wire parser silently default instead of erroring

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

## Acceptance criteria

- [ ] F2: a test asserts `VectorFieldManager::knn` on a `dim=4` index returns `Err(SchemaError)` naming the expected dimension for both `knn(&[1.0, 2.0], 1)` and `knn(&[], 1)`, and that an odd-length query blob is rejected rather than silently truncated by `chunks_exact`.
- [ ] F7: the write-visibility contract is written down (immediately consistent, or explicitly eventual with a stated bound), a test asserts it with no `sleep`, and `DEBUG FLUSH-SEARCH-INDEX` (or the chosen alternative) exists such that `rg -c 'sleep' server/tests/search.rs` is 0.
- [ ] F8: a test asserts `FT.AGGREGATE idx * GROUPBY 0 REDUCE COUNT 0 AS n` over 100 001 matching documents returns `n == 100001` (or errors rather than truncating), and the same for a `SUM` whose true total is known — with the 100 000 cap made injectable so the test runs at 10 + 11 documents.
- [ ] F10: a test asserts `COUNT_DISTINCT` and `QUANTILE` merges of two large partials are either bounded in memory or return an explicit error, and that a two-partial `QUANTILE` merge is numerically correct.
- [ ] F11: a table test asserts every `FT.HYBRID` option is either honoured or rejected — specifically that `RANGE … RADIUS r` filters by radius and that `COMBINE LINEAR 10 ALPHA banana` is an error rather than α = 0.5.
- [ ] F12: a test asserts `STDDEV` over `[100000000.0, 100000001.0, 100000002.0]` is within 1e-6 of 1.0, and that the same values split across two partials still merge correctly.
- [ ] F13: a test asserts `FT.SEARCH` and `FT.HYBRID`/KNN return identical field content for a document whose key has been spilled by a tiered eviction policy.
- [ ] F14: a table over `wire::parse_ft_search_request` asserts each malformed numeric argument (`LIMIT 0 -1`, garbage `SLOP`/`DIALECT`/`TIMEOUT`, and the `PARAMS`/`RETURN`/`INFIELDS` counts in `query.rs`) yields `Err` rather than a default, with a `redis-regression` case pinning the error text.

## Depends on

Nothing.

Two clarifications, since this area did request shared infrastructure:

- **I4 (issue 04, `.scratch/testing-improvements-round2/issues/` — the conservation checker)** was
  requested by this area and its author called it "the single highest-leverage item in the audit",
  but the proposal's own cross-area note scopes it to F3, F4, F5 and F9 — *all four promoted
  elsewhere* (issues 46, 20, 45, 22). No residual finding here depends on it, so it is not listed
  as a dependency of this issue; it remains a dependency of those.
- **F15 is issue 94**, `.scratch/testing-improvements-round2/issues/`, not one of I1–I18. F11 and
  F13 both drop from level 4 to level 3 once it lands, and F8 becomes sub-second rather than
  minutes. Issue 01 (I1 — `shard_driver` harness extension) is a *different* item and its scope
  does not include FT.\* drive seams, though issue 94 asks to be sequenced with it.

## Re-triage 2026-08-06

**Verdict: still-valid** — 0/8 findings discharged; every cited site reproduces verbatim.

| F | verdict | evidence (verified today) |
|---|---|---|
| F2 | still-valid | `search/src/vector.rs:354-358` — `VectorField::knn` still forwards `query` to `self.index.search(query, k)` with no `query.len() == self.dim` check, while the write path still validates. |
| F7 | still-valid | `rg -c 'sleep' server/tests/search.rs` = **135**, unchanged. `SearchMsg::FlushSearchIndexes` is still reachable only from the checkpoint/quiesce hook (`server/src/server/checkpoint_quiesce.rs:93,162`) and `dispatch_search.rs:8`; no `DEBUG FLUSH-SEARCH-INDEX` verb exists. The semantics decision is still unmade. |
| F8 | still-valid | `core/src/shard/search/query.rs:179` still `SearchOptions::page(0, 100_000)` under the "no limit" comment. |
| F10 | still-valid | `search/src/aggregate.rs:1012-1013` `CountDistinct` merge is still a full `HashSet<String>` union; `:1081-1082` `Quantile` merge still `extend`s a `Vec<f64>`, finalized by clone+sort at `:1161`. |
| F11 | still-valid | `query.rs:748` still `let _ = range_radius; // RANGE mode: for future use`; `_ef_runtime` (`:279`) still parsed at `:365`/`:396` and never read; the silent defaults survive at `:450` (60.0), `:460`/`:470` (0.5). |
| F12 | still-valid | `aggregate.rs:1157` still `(*sum_sq / n) - (*sum / n).powi(2)` clamped by `.max(0.0)` — the naive catastrophic-cancellation formula. |
| F13 | still-valid | KNN/hybrid content still resolves through `store.get(...)` in `query.rs`, so a tiered-spilled key yields `fields: None` while `FT.SEARCH` reads from tantivy. |
| F14 | still-valid | `search/src/wire.rs:508-510` `parse_num` still returns `Option` and every call site still `unwrap_or`s a default. |

Expected: `crates/search` sits behind a non-default cargo feature and was explicitly out of
hardening-campaign scope, so no FM spec covers it and none of the eight sites was touched. Issue
94 (the missing sub-socket FT.\* seams) still gates F11 and F13 at level 4 and still makes F8's
100 001-document criterion impractical. Nothing in the body needs a file:line correction — the
area did not move during the extractions. F2 remains the one that is a **latent memory-safety
bug rather than a test gap** (a short `&[f32]` reaches usearch's C++ side, which reads `dim`
floats regardless of slice length); it is unchanged since filing, not newly introduced.
