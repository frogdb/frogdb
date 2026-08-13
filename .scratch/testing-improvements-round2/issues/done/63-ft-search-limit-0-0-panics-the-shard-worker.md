# `FT.SEARCH … LIMIT 0 0` panics the shard worker and takes the shard down for the process lifetime

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/10 F1 · MASTER.md §3 (availability / resource)
Score: severity 4 · likelihood 5 · effort 1 · priority 21
Area: frogdb-search / query execution

## Context

`LIMIT 0 0` is *the* documented RediSearch idiom for "give me the match count without the
documents", and redis-py / node-redis / redisearch-go helpers emit it on default config. In
FrogDB it arrives at tantivy's `TopDocs::with_limit(0)`, which asserts. The panic is inside the
`ShardWorker` event-loop task, so the shard dies and every key on it becomes unreachable for the
process lifetime — no `catch_unwind` guards the loop. Two further reachable paths produce the
same zero: `FT.HYBRID … COMBINE RRF 0` and `VSIM … KNN <n> 0`.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`frogdb-server/crates/search/src/wire.rs:121-122` parses `LIMIT 0 0` to
`offset=0, limit=0`; `wire.rs:337-340` `index_options()` yields `SearchOptions { offset: 0,
limit: self.offset + self.limit }` = `limit: 0`; `frogdb-server/crates/search/src/index.rs:571`
computes `fetch_limit = offset + limit = 0`; `index.rs:699` calls `TopDocs::with_limit(0)`, and
tantivy 0.26.1 `src/collector/top_score_collector.rs:94` is
`assert_ne!(limit, 0, "Limit must be greater than 0")`. No clamp exists in
`server/src/commands/search.rs` or `connection/search/query.rs`. No `catch_unwind` guards the
shard event loop (the only two in the workspace are in a test and in `scripting/gate.rs:374`).
Second reachable path: `FT.HYBRID … COMBINE RRF 0` → `core/src/shard/search/query.rs:437`
`combine_count = 0` → `:701` `count = 0` → `index.rs:1098` `fetch_count = window * 0 = 0` →
same assert. Third: `VSIM … KNN <n> 0`.

Why nothing catches it: `rg '"0", "0"'` over both search test files returns nothing — the idiom
is never exercised anywhere in the repo.

## What to fix

1. Handle `limit == 0` in `ShardSearchIndex::search` (`index.rs:571`) as "count only": return
   the total match count with an empty hits vec, without constructing a `TopDocs` collector.
2. Apply the same guard to the hybrid path (`index.rs:1098`, `core/src/shard/search/query.rs:437`,
   `:701`) and to the `VSIM … KNN <n> 0` path.
3. Decide separately whether the shard event loop should carry a `catch_unwind` so a future
   arithmetic edge degrades one query instead of one shard — file separately if it grows.

## Acceptance criteria

- [ ] A table-driven unit test over `ShardSearchIndex::search` with `(offset, limit)` ∈
      `{(0,0), (0,1), (5,0), (0,usize::MAX)}` asserts each returns `Ok`/`Err` and **never
      panics**. Fails today on `(0,0)`.
- [ ] The `(0,0)` case asserts `total == <match count>` with an empty `hits` vec.
- [ ] One crate-level `hybrid_search` case with `count = 0` asserts no panic.
- [ ] A RESP-level regression pin asserts `FT.SEARCH idx * LIMIT 0 0` replies `[N]` and that the
      server answers the *next* command on the same shard (liveness).

## Test boundary

Level 1 (pure unit) for the matrix — it is an arithmetic edge in one function — plus a single
level-4 liveness pin, because "the shard survives the panic" is only observable over the socket.
The level-1 matrix is not sufficient alone precisely because it cannot observe shard death.

## Depends on

nothing

## Re-triage 2026-08-06

**Verdict: still-valid — confirmed live panic**

Nothing changed. `git log -S"with_limit" -- frogdb-server/crates/search/src/index.rs` returns only
the two original feature commits (`fae0204b`, `f360a69c`); the hardening campaign never touched
`crates/search` and no FM row mentions it. Today's chain, statically verified end to end:
`wire.rs:119-122` parses `LIMIT 0 0` to `offset=0, limit=0`; `wire.rs:337-341 index_options()`
yields `SearchOptions { offset: 0, limit: self.offset + self.limit }` = `limit: 0`;
`index.rs:568-571` computes `(fetch_offset, fetch_limit) = (offset, offset + limit)` = `(0, 0)`
with **no** `limit == 0` guard anywhere in `search()` (`index.rs:493` onward); `fetch_limit` then
reaches `TopDocs::with_limit(fetch_limit)` at **`index.rs:594`, `:634` and `:699`** (the issue's
single `:699` is now three call sites — numeric-sort, string-sort and default-BM25). The workspace
pins **tantivy 0.26.0** (`Cargo.lock:4951-4952`), whose
`src/collector/top_score_collector.rs:94` is `assert_ne!(limit, 0, "Limit must be greater than 0")`
— an unconditional `assert_ne!`, live in release. The hybrid vector is also intact:
`core/src/shard/search/query.rs:432-438` accepts any parsed `usize` as `combine_count` including
`0`, `:700-701` folds it to `count`, and `index.rs:1094-1106 hybrid_search` computes
`fetch_count = window * count` = 0 and feeds it to `search` as `limit`. Only three `catch_unwind`
sites exist in the workspace (`server/src/connection/transaction_conn_command.rs`,
`core/src/scripting/gate.rs`, one test) — none on the shard event loop. `rg` for a zero-limit test
or clamp across `crates/search/src`, `crates/core/src/shard/search` and
`server/src/commands/search.rs` returns nothing.

Caveat per the re-triage brief: verified statically only (no `cargo` runs permitted); the crash
itself is unverified-by-execution, but the assert is unconditional and the argument path is direct.

## Resolution

Fixed 2026-08-07 in `563bdcd1` (arithmetic) and backstopped by `b7df2845`
([campaign-2 issue 07](../../../hardening-2/issues/done/07-no-panic-isolation-at-the-shard-boundary.md)).

**The panic was real and reproduced by execution**, closing the re-triage's
verified-statically-only caveat. Reverting just the `limit == 0` guard and re-running the
matrix test gives:

```
thread 'index::tests::search_paging_window_edges_return_instead_of_panicking' panicked at
  tantivy-0.26.0/src/collector/top_score_collector.rs:94:9:
assertion `left != right` failed: Limit must be greater than 0
  left: 0
 right: 0
```

**Fix** — `ShardSearchIndex::search` (`frogdb-server/crates/search/src/index.rs`) now
answers a zero window straight from the `Count` collector: `total = raw_total`, empty
`hits`, no `TopDocs` constructed. The geo branch deliberately falls through, because a
geo-filtered total is only knowable by enumerating and post-filtering, and its fetch
window (`raw_total.max(1)`) is non-zero by construction — its trailing `.take(limit)`
already yields the count-only shape. `knn_search` returns early on `k == 0` (never
entering usearch) and `hybrid_search` on `count == 0` (never running either leg), which
covers `FT.HYBRID … COMBINE RRF 0`, `VSIM … KNN <n> 0` and `*=>[KNN 0 @f $b]`.

**Second latent bug found by the same matrix**: `LIMIT 0 usize::MAX` overflowed
tantivy's `TopNComputer`, which allocates `2 * limit` slots up front. The non-geo fetch
window is now `offset.saturating_add(limit).min(raw_total.max(1))` — clamping to the
match count cannot change the result set, since the query has exactly `raw_total`
matches.

**RediSearch parity**: `[N]` — a one-element array holding the total, which is what
RediSearch replies for `LIMIT 0 0`. No coordinator change was needed:
`FtSearchMerge::finish` (`server/src/connection/search/merge.rs`) already pushes
`Response::Integer(total)` first and then `.skip(offset).take(limit)` over the hits, so
a zero limit emits the count and nothing else.

**Item 3 of "What to fix"** (should the event loop carry a `catch_unwind`) was already
filed as campaign-2 issue 07 and is now implemented — see `shard/panic_guard.rs`. The
isolation is a backstop, not a substitute: this arithmetic is fixed on its own.

**Tests** — `frogdb-search` 159/159 pass: the table-driven
`search_paging_window_edges_return_instead_of_panicking` over `(offset, limit)` ∈
`{(0,0), (0,1), (5,0), (0,usize::MAX)}` (every row asserting the same `total`, the
zero-window rows asserting empty hits), `search_zero_limit_counts_without_fetching_documents`
(count-only total agrees with the windowed query's, and the zero-match case still counts),
and `hybrid_and_knn_zero_count_return_no_hits`. RESP-level pin in
`frogdb-server/crates/server/tests/search.rs`:
`regression_ft_search_limit_zero_zero_is_count_only_and_shard_survives` asserts the `[N]`
reply and that the same connection is answered afterwards (liveness).

**No failure-mode row.** The search area has no locked spec under
`specs/` (the campaign covered txn/vll/persistence/replication/cluster
only), and inventing a new spec area was out of scope for this fix. The shard-survival
half of the invariant is rowed as
[FM-VLL-005](../../../../specs/vll.md#fm-vll-005--a-granted-op-panics-while-executing),
which cites this issue as the motivating panic.
