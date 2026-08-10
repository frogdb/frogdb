# Proposal 71 — Search query plan: one collection loop, one FT.HYBRID grammar, one hit merge

Lane candidates: **SV11 + SV12**.

**Verified at HEAD `159cb7a2`.** The lane brief was written against `08c143d6`; every citation
below was re-derived from the current tree. **Four of the brief's claims are wrong and are
corrected here** — two line ranges, one duplication that does not exist, and the
`Latent` classification. The real duplication SV11 is reaching for lives one layer *up*
from where the brief points, in `frogdb-core`, and half of it is **live**.

## Summary

Three parts, one through-line: **a query is a value, and nobody in the FT path is holding
it.** Each of the three sites re-derives the query — its options, its collection order, its
merge policy — from raw material, in its own way, and the ways have drifted.

- **SV11a — `ShardSearchIndex::search` (`search/src/index.rs:493-761`, 269 lines) has one
  interface and four bodies.** Composition (parser + INKEYS + numeric FILTER + geo merge),
  window math (count-only guard, geo over-fetch, collector clamp), collection, per-doc
  materialization and pagination are interleaved into a three-way `if/else` where the
  materialization block is **byte-identical in all three arms** (624-641 / 669-690 /
  729-746, verified by diff — the only difference is indentation and which of
  `score`/`sort_value` is filled) and the geo re-page tail is byte-identical in all three
  (643-650 / 707-714 / 748-755). Split it: a pure `plan(query_str, opts) -> QueryPlan`
  computing the composed query, the geo filters, the fetch window and the collection order;
  an `execute(plan, searcher)` with **one** collect-materialize-paginate loop. **Latent**,
  with one verified behavior deviation surfaced by the split (string SORTBY, below) that
  this proposal deliberately does **not** fix.
- **SV11b — FT.HYBRID is the one FT command that parses its grammar twice, and both
  parsers are wrong about the other.** `frogdb_search::wire`'s module doc states the
  invariant in so many words (`wire.rs:3-9`): *"The FT.SEARCH and FT.AGGREGATE grammars are
  parsed exactly once, at the coordinator … Both ends of the shard boundary consume this one
  module, so the option grammar and the hit layout cannot drift apart."* FT.HYBRID is not in
  that sentence, and not by accident: `ScatterOp::FtHybrid` carries **raw
  `query_args: Vec<Bytes>`** (`core/src/shard/message.rs:1279-1283`), the connection layer
  scans it for four options (`connection/search/hybrid.rs:22-92`), and every internal shard
  re-parses the whole grammar in a **560-line** `execute_ft_hybrid`
  (`core/src/shard/search/query.rs:255-814`). **Two live defects fall directly out of the
  two-parser structure**, both traced end-to-end below; one is a clean one-line hotfix.
- **SV12 — `FtSearchMerge` and `FtHybridMerge` are the same merge, and the copy is the one
  missing a behavior** (`connection/search/merge.rs:339` and `:464`). Diffed: 75 lines vs 63
  with exactly four substantive differences, and one of them —
  numeric-sort detection, present at `:395-398`, absent from the hybrid twin — is **live**.
  Collapse both onto a `HitMerge` core parameterized by ordering policy and `withscores`.

**Corrections to the lane brief, stated plainly.**

| Brief claim | Verified |
|---|---|
| `search` is "~420 lines, index.rs:493-914" | `search` is `493-761`, **269 lines**. `764-913` are five *already-extracted* private helpers (`build_snippet_generators` 764, `apply_highlights` 788, `apply_summarize` 822, `doc_passes_geo_filters` 861, `extract_hit_fields` 882). The brief counted the extraction that already happened |
| `hybrid_search` is "~300, :1121-1428" | `hybrid_search` is `1121-1183`, **63 lines**. `1189-1437` are free functions (`parse_language`, `build_tantivy_schema`, `parse_geo_value`, `haversine_distance`) |
| "composition+hydration duplicated" between `search` and `hybrid_search` | **False.** `hybrid_search` already delegates: `self.search(...)` (index.rs:1143) + `self.knn_search(...)` (:1158) + `hybrid::hybrid_fuse` (:1176). There is nothing to dedupe there. The duplication exists in `frogdb-core`: `execute_ft_hybrid`'s hydration loop (`shard/search/query.rs:752-803`) versus `execute_ft_knn_search`'s (`:110-149`) |
| SV11 "Latent" | The `frogdb-search` half is latent. The FT.HYBRID half is **live** — two defects, traced |
| SV12 "merge.rs:339/:464, ~120-line merge body copied" | Line numbers correct. Bodies are 75 and 63 lines with four real differences, one of which is a live defect |

**Nothing here is in a locked area** — see *Spec / LOCKED impact*.

## Files involved

| Path | Lines | Part | Role |
|---|---|---|---|
| `frogdb-server/crates/search/src/index.rs` | 2197 | SV11a | **The change.** `search` (493-761) splits into `plan` + `execute`; `SearchOptions` (74-97) is the plan input; `hybrid_search` (1121-1183) reuses the plan. Helpers 764-913 keep their homes |
| `frogdb-server/crates/search/src/wire.rs` | 755 | SV11b | **The change (new type).** `FtHybridRequest` joins `FtSearchRequest` (28) and `FtAggregateRequest` (366). `index_options` (337) is the adapter to copy; `ShardSearchHit` (463) / `sort_key` (479) / `ShardSearchReply` (490) / `FtShardReply` (500) are the shared reply wire, unchanged |
| `frogdb-server/crates/core/src/shard/search/query.rs` | 858 | SV11b | **The change (largest).** `execute_ft_hybrid` (255-814, **560 lines**) → parse moves out, hydration is shared. `execute_ft_knn_search` (90-162) donates the shared hydration. `to_shard_reply` (11-49) and `execute_ft_search` (52-87) are the shape to match. `execute_ft_explain` (816-857) is **read-only evidence** |
| `frogdb-server/crates/core/src/shard/message.rs` | 1446 | SV11b | One variant edited: `ScatterOp::FtHybrid { index_name, query_args }` (1279-1283) → `{ index_name, request: Box<FtHybridRequest> }`, matching `FtAggregate` (1275-1278) |
| `frogdb-server/crates/core/src/shard/execution.rs` | 2132 | SV11b | One arm edited: the `ScatterOp::FtHybrid` dispatch (888-895) forwards the typed request instead of raw args |
| `frogdb-server/crates/server/src/connection/search/hybrid.rs` | 125 | SV11b | **Rewritten to match `query.rs`.** The 71-line hand-rolled option scan (22-92) is replaced by `FtHybridRequest::parse` |
| `frogdb-server/crates/server/src/connection/search/merge.rs` | 1208 | SV12 | **The change.** `FtSearchMerge` (339-457) + `FtHybridMerge` (464-547) → `HitMerge`. `sort_by_key` (320), `fields_to_response` (309) and `shard_conflict_or_bug` (36) already exist and are reused. Tests at 660-1210 |
| `frogdb-server/crates/search/src/hybrid.rs` | 350 | — | **Read-only.** `hybrid_fuse` (61) / `HybridHit` (22) / `FusionStrategy` (13) are already a clean seam with six unit tests (211-349). Untouched |
| `frogdb-server/crates/server/src/connection/search/query.rs` | 47 | — | **Read-only.** `handle_ft_search` — the shape SV11b copies, including its doc comment (13-16) |
| `frogdb-server/crates/server/src/connection/search/aggregate.rs` | 58 | — | **Read-only.** `handle_ft_aggregate` — the second instance of that shape (11-14) |
| `frogdb-server/crates/core/src/shard/dispatch_core.rs` | 535 | — | **Read-only.** `scatter_error_reply`'s `FtSearch | FtHybrid` arm (206-208) — proof the typed error path already treats the two identically |
| `frogdb-server/crates/server/src/connection/dispatch.rs` | 1177 | — | **Read-only.** `ServerWideOp::FtHybrid => self.handle_ft_hybrid(args)` (263). Owned by sibling **68** |
| `frogdb-server/crates/server/tests/search.rs` | 8011 | all | Eight FT.HYBRID integration tests (6773-7115) and the typed-wire regression `test_ft_search_full_grammar_multi_shard` (7126) — the precedent this proposal extends |

`frogdb-search`, `frogdb-core` and `frogdb-server` are all built at default features; the FT
commands are registered unconditionally (`server/register.rs:201-215` has no `#[cfg(feature)]`,
unlike the fifteen `cmd-*` gates elsewhere in that file). No feature-flag alternation is
needed in the iteration loop.

## Problem

### SV11a — one query, one interface, four bodies

`ShardSearchIndex::search` does five separable jobs in one 269-line function:

| Job | Lines | Shape |
|---|---|---|
| Compose | 512-560 | `QueryParser::new` + `with_infields`/`with_slop`/`with_verbatim` (518-526), `parse_with_geo_filters` (527), INKEYS `TermSetQuery` conjunction (530-539), per-`FILTER` `RangeQuery` conjunction (542-555), geo-filter merge (558) |
| Window | 563-597 | `Count` collector (567), the `LIMIT 0 0` count-only early return (580-585), the geo over-fetch / collector-clamp branch (587-597) |
| Collect | 617-622, 657-662, 722-727 | Three collectors: numeric fast field, score-with-string-post-sort, score |
| Materialize | 624-641, 669-690, 729-746 | **Byte-identical three times** |
| Paginate | 643-654, 707-718, 748-759 | Geo re-page + `Ok(SearchResult)` — **byte-identical three times** |

The materialization claim is not an eyeball estimate. Diffing 624-641 against 729-746 yields
exactly two token differences (`for (sort_val, …)` vs `for (score, …)`, and the
`score`/`sort_value` fields of the pushed `SearchHit`); every other line, including the geo
`continue`, the `extract_hit_fields` call, the `apply_highlights` call and the `summarize`
`if let`, matches character for character modulo indentation. Diffing the geo tails 643-650
against 707-714 and against 748-755 yields **no differences at all** beyond indentation.

**The cost is not the 269 lines — it is that the subtlest facts in the function have no
home.** Two of them are currently carried by comment blocks longer than the code they guard:

- The `LIMIT 0 0` count-only contract (569-579, **eleven lines of comment**) explains that
  `TopDocs::with_limit` asserts `limit != 0` *in release*, and that the geo case must
  deliberately fall through. That is a property of the *window*, but it is written as an
  early `return` in the middle of a function whose three later branches each have to be
  re-read to confirm they honor it.
- The collector-clamp reasoning (590-595, six lines) — an unclamped
  `LIMIT 0 18446744073709551615` overflows the collector's `2 * limit` allocation. Same
  shape: a window fact, stored as a branch.

Both are exactly the kind of fact that a value can carry and a control-flow arm cannot. And
the evidence that the arrangement does not hold is already in the file: **the string-sort
branch selects the wrong candidates.** At 657-662 it collects by
`TopDocs::with_limit(fetch_limit).and_offset(fetch_offset).order_by_score()` and then sorts
the retrieved page lexically in Rust (693-706). So `FT.SEARCH … SORTBY @name` returns *the
top-N documents by BM25, re-ordered lexically* — not the N lexically-first matching
documents, which is what RediSearch returns. The numeric branch has no such problem: it uses
`order_by_fast_field` (621), so the collector selects by the sort field.

This is a **live deviation** and this proposal does **not** fix it (see *Risks*). It is cited
here as the diagnostic: with collection order smeared across three arms, "which field decides
selection" is not a question the code can be asked, so the two arms answered it differently
and nothing noticed. There is no unit test in `frogdb-search` that sets `sort_by` at all —
`grep -n 'sort_by' index.rs` returns the struct field (80), the destructure (501) and the
match (606), and nothing in the 757-line test module (1440-2197). Every SORTBY test in the
repo is a server integration test on a **NUMERIC** field (`tests/search.rs:1840`, `:1892`,
`:7175`; `merge.rs:746`), i.e. on the branch that is right.

Secondary, same theme: `HybridTextOptions` (index.rs:112-123) is a five-field struct whose
fields (`infields`, `slop`, `verbatim`, `extra_filters`, `extra_geo_filters`) are a strict
subset of `SearchOptions` (74-97), used at exactly one call site
(`core/shard/search/query.rs:727`). It exists because `hybrid_search` takes seven positional
parameters and could not take a `SearchOptions` without implying it honors
HIGHLIGHT/SUMMARIZE/SORTBY, which it does not. That is a real constraint expressed as a
duplicate type.

### SV11b — FT.HYBRID: one grammar, two parsers, two live defects

FT.SEARCH and FT.AGGREGATE each state their contract at the handler:

> The grammar is parsed exactly once, here, into an [`FtSearchRequest`]; every shard receives
> the parsed request through the scatter op and the merge reads its knobs from the same
> struct — **the coordinator and the shards cannot disagree about what an option means.**
> — `connection/search/query.rs:13-16`

> The pipeline is parsed exactly once, here, into typed steps carried by
> [`FtAggregateRequest`]. — `connection/search/aggregate.rs:12-14`

FT.HYBRID has no such sentence because it cannot make the claim. Its scatter op carries raw
bytes:

```rust
/// FT.HYBRID - hybrid search combining BM25 and vector search on this shard.
FtHybrid {
    index_name: Bytes,
    query_args: Vec<Bytes>,
},
```
— `core/src/shard/message.rs:1279-1283`, directly below `FtAggregate`, whose doc says *"The
request carries the pipeline parsed once at the coordinator"* (1273-1274).

So the grammar is parsed **twice, by two different parsers, in two crates**:

| Parser | Where | Understands | Ignores |
|---|---|---|---|
| Connection-side scan | `connection/search/hybrid.rs:31-92` (71 lines, flat token walk from `i = 1`) | `LIMIT`, `SORTBY`, `NOSORT`, `TIMEOUT` | everything else, incl. all nesting |
| Shard-side state machine | `core/shard/search/query.rs:297-680` (383 lines, 22 mutable locals, nested `SEARCH`/`VSIM`/`COMBINE` sub-loops) | `SEARCH`, `VSIM`, `COMBINE`, `PARAMS`, `NOCONTENT`, `RETURN`, `VERBATIM`, `INFIELDS`, `SLOP`, `FILTER`, `GEOFILTER` | `LIMIT`/`SORTBY`/`NOSORT`/`TIMEOUT`/`DIALECT`/`LOAD`/`GROUPBY`/`APPLY` — skipped by a heuristic |

**Live defect 1 — the shard's skip heuristic swallows four options, order-dependently.**

The shard's "coordinator-level options" arm (`query.rs:646-675`) skips forward until it sees
a token it recognizes as top-level. Its resume set (653-670) is:

```
SEARCH | VSIM | COMBINE | PARAMS | LIMIT | SORTBY | NOSORT | LOAD | GROUPBY | APPLY
      | FILTER | NOCONTENT | RETURN | TIMEOUT | DIALECT
```

**`VERBATIM` (542), `INFIELDS` (546), `SLOP` (566) and `GEOFILTER` (611) are parsed at top
level but are missing from the resume set.** Any of the four placed *after* any of the eight
skipped options is silently eaten. End-to-end trace, `FT.HYBRID idx SEARCH "runing" VSIM @v
$b COMBINE RRF 10 LIMIT 0 5 VERBATIM PARAMS 2 b <blob>`:

1. `handle_ft_hybrid` (`connection/search/hybrid.rs:14`) reads `LIMIT 0 5` → `global_offset=0`,
   `global_limit=5`; ships **all** of `query_args` untouched (116-119).
2. Shard `execute_ft_hybrid` walks: `SEARCH`→query, `VSIM`→field/param, `COMBINE`→RRF/10,
   then hits `LIMIT` at 646. The skip loop consumes `"0"`, `"5"`, then **`"VERBATIM"`**
   (not in the resume set), and stops at `"PARAMS"`.
3. `verbatim` stays `false` → `HybridTextOptions { verbatim: false, … }` (727-733) →
   `hybrid_search` (734) → `self.search(…, SearchOptions { verbatim: false, … })` (1143-1155)
   → `index.rs:524` never calls `parser.with_verbatim(true)`.
4. The client asked for no stemming and got stemming. **Silently.**

Move `VERBATIM` before `LIMIT` and it works. The option's meaning depends on its position —
in a grammar that is otherwise order-free. `INFIELDS`, `SLOP` and `GEOFILTER` have the same
shape; `GEOFILTER` is the worst of the four, because a dropped geo filter returns *more*
documents, which reads as success.

This is a one-line fix and a genuine hotfix (below). It is also, structurally, a bug that
only a second parser can have: the shard skips `LIMIT` *by guessing where it ends* precisely
because it was handed bytes instead of a request.

**Live defect 2 — `FT.HYBRID … SORTBY` is a silent no-op that also discards the default
ordering.** Traced:

1. Connection sets `sortby_active = true` on seeing the token (`hybrid.rs:50-52`) and passes
   it into `FtHybridMerge` (101-111).
2. Shard skips `SORTBY` (646) — it never learns which field to sort by — and unconditionally
   emits `sort_value: None` for every hybrid hit (`query.rs:808`).
3. `ShardSearchHit::sort_key()` returns `String::new()` when `sort_value` is `None`
   (`wire.rs:479-485`), so every `(sort_key, hit)` pair in the merge carries `""`
   (`merge.rs:492-497`).
4. `finish` takes the `sortby_active` branch (`:516-517`) → `sort_by_key` compares `""`
   against `""` for every pair → all `Ordering::Equal` → Rust's stable sort leaves
   shard-arrival order.
5. The default fused-score sort (`:520-524`) is skipped because `sortby_active` won the
   branch.

So the client gets **neither** the requested sort **nor** relevance order. Adding `SORTBY` to
an FT.HYBRID query makes the result strictly worse than omitting it. A third, smaller defect
rides along: `hybrid.rs:27` is `let sortby_numeric = false;` — not `let mut`, never assigned —
and `FtHybridMerge::absorb` has no numeric-detection arm, so even a correct `sort_value` would
sort `"100"` before `"9"`. That third one is SV12's, and SV12 fixes it by construction.

**Parsed and discarded.** Two more knobs are read by the shard parser and thrown away:
`range_radius` (`query.rs:278`, parsed at 372-385, retired at 748 by `let _ = range_radius;`)
and `_ef_runtime` (279, parsed at 359-370 and 393-401, never read). `VSIM … RANGE <n> RADIUS
<r>` therefore silently degrades to a top-`combine_count` KNN fusion — a wrong answer, not a
refusal. These are capability gaps rather than regressions, and this proposal's job is to make
them *visible* (an `FtHybridRequest` field with no consumer is a compile-time-greppable hole,
where `let _ = range_radius;` is not); deciding them is out of scope.

**The hydration duplication the brief was reaching for.** `execute_ft_knn_search`
(`query.rs:110-149`) and the FT.HYBRID hit loop (`:753-803`) are the same routine: look the
key up in the store, branch on `IndexSource::Json` vs hash, walk the fields applying the
`return_fields` filter, then append synthetic score fields — `__vec_score` in the first,
`YIELD_SCORE_AS` names in the second. ~40 lines each, one shared idea, two homes. A third
variant, `to_shard_reply` (`:11-49`), applies the same `return_fields` + `nocontent` policy to
hits whose fields came from the *index* rather than the store. Three answers to "what fields
does a hit carry".

**Leverage, stated for later rather than claimed now.** `execute_ft_explain`
(`query.rs:816-857`) builds a **second** `QueryParser` (836-841) with none of
`with_infields` / `with_slop` / `with_verbatim`, and never sees INKEYS, FILTER or GEOFILTER —
so FT.EXPLAIN describes a query the engine would not run. With a `QueryPlan` value, EXPLAIN
becomes `plan.describe()` and the two cannot differ. **That change is not in this proposal's
scope** (it is a wire-visible behavior change to FT.EXPLAIN's output); it is named because it
is the strongest argument that the plan is worth having, and because it is the payoff a
follow-up gets for free.

### SV12 — the merge twins, and the one that lost a behavior

`FtSearchMerge` (`merge.rs:339-457`) and `FtHybridMerge` (`:464-547`) implement the same
`MergeStrategy`. Diffing the two `absorb`+`finish` bodies (382-456 against 484-546) yields
**exactly four** substantive hunks:

| # | `FtSearchMerge` | `FtHybridMerge` |
|---|---|---|
| 1 | numeric-sort detection, `:395-398` | **absent** — `sortby_numeric` is dead state |
| 2 | `shard_conflict_or_bug(other, "FT.SEARCH")` | `…, "FT.HYBRID")` |
| 3 | order: `sortby` → KNN-ascending → score-descending | order: `nosort` → `sortby` → score-descending |
| 4 | `withscores` renders the score, `:447-449` | **absent** |

Everything else — the `PartialResult::Ft(FtShardReply::Search(Ok(_)))` arm, the `total`
accumulation, the sort-key precompute, the `Err(msg)` arm, the `other =>` conflict arm, the
`skip(offset).take(limit)` slice, and the flat `[total, key, [fields…], …]` rendering — is
character-identical.

Difference 1 is difference 3's victim: `sortby_numeric` is a *field on the struct*
(`:467`) that is initialized `false` at the one construction site (`hybrid.rs:104`), never
written, and read at `:517`. Rust cannot warn — it is written once and read once. This is
the "one fact, two textual homes" failure in its purest form: the fact is *how a SORTBY key
is compared*, `sort_by_key` (`:320-331`) already owns the comparison, and the **detection**
of which comparison to use got left behind in one of the two copies.

The test surface shows the same asymmetry: `FtSearchMerge` has ten unit tests (`:715-1032`),
`FtHybridMerge` has two (`:933`, `:1058`) — a continuation-lock error test and a
fused-score-descending test. There is no `FtHybridMerge` SORTBY test at all, which is why
neither this nor defect 2 above was caught.

## Proposed change

Vocabulary note: this proposal says **internal shard** for the `ShardWorker`-owned partition
and **connection layer** for the fan-out side, per `frogdb-server/CONTEXT.md` (16-18,
107-112). Where existing doc comments say "coordinator", they are quoted, not adopted.

### SV11a — `QueryPlan`: the query becomes a value

Introduce a plan type in `frogdb-search` holding everything the shard-local execution of one
FT query is determined by, and split the god-function around it:

```rust
/// Everything one index-level query is determined by, computed once from the
/// query string, the options and the index definition — before any searcher is
/// touched. The window rules (`LIMIT 0 0` count-only; geo over-fetch; the
/// collector's `2 * limit` clamp) are properties of `window`, not of a branch.
pub struct QueryPlan {
    query: Box<dyn tantivy::query::Query>,
    geo_filters: Vec<GeoFilter>,
    window: FetchWindow,      // CountOnly | Fetch { offset, limit }
    order: CollectOrder,      // Score | NumericFastField { field, Order } | StringField { field, Order }
    materialize: Materialize, // highlight recipe + summarize options
    page: (usize, usize),     // the caller's offset/limit, for the geo re-page
}

impl ShardSearchIndex {
    /// Compose + window: pure with respect to the reader.
    pub fn plan(&self, query_str: &str, opts: &SearchOptions) -> Result<QueryPlan, SearchError>;
    /// Collect + materialize + paginate: one loop, parameterized by `plan.order`.
    pub fn execute(&self, plan: &QueryPlan) -> Result<SearchResult, SearchError>;
}
```

`search` becomes `self.execute(&self.plan(query_str, opts)?)` and keeps its signature, so no
caller changes. `hybrid_search` becomes `self.execute(&plan.with_window(fetch_count))?` —
the text leg of a hybrid query is *the same plan with a different window*, which is precisely
the relationship the brief was describing and which the current code expresses by passing a
second options struct.

Consequences that follow rather than being bolted on:

- **`HybridTextOptions` is deleted.** With the window a plan field rather than an options
  field, `hybrid_search` can take `&SearchOptions` and simply *not read* the fields it does
  not honor — and the plan makes "does not honor" checkable: `plan()` can reject (or the
  caller can assert) a `sort_by`/`highlight` on a hybrid text leg instead of silently
  dropping it as today.
- **The three materialize copies become one** `fn materialize(&self, doc, order, &Materialize)
  -> SearchHit`, and the three geo tails become one `fn paginate(hits, raw_total, has_geo,
  page) -> SearchResult`.
- **The count-only and clamp comments move to `FetchWindow`'s constructor**, where they
  describe a value rather than a jump.

**Deletion test.** Delete `QueryPlan`: the composition (parser configuration, INKEYS
conjunction, per-FILTER `RangeQuery` conjunction, geo merge — 49 lines) regrows inside
`search`; the window rules regrow as two early branches; the materialize block regrows
three times and the geo tail regrows three times. Delete `FetchWindow` alone and the
eleven-line count-only comment has nowhere to live but a branch. It earns its keep.

**Not proposed here:** changing `CollectOrder::StringField` to select by the sort field. That
is the live deviation named in *Problem*; making the plan express collection order is what
lets a follow-up fix it in one place, and the fix is a behavior change owing a compatibility
ruling. See *Risks*.

### SV11b — `FtHybridRequest`: parse once, at the connection layer

Do for FT.HYBRID exactly what `FtSearchRequest` and `FtAggregateRequest` already do,
including the doc sentence:

```rust
// frogdb-server/crates/search/src/wire.rs — beside FtSearchRequest / FtAggregateRequest
pub struct FtHybridRequest {
    // SEARCH leg
    pub search_query: String,
    pub search_yield_as: Option<String>,
    // VSIM leg
    pub vsim_field: String,
    pub vsim_param: String,
    pub vsim_mode: VsimMode,            // Knn { k } | Range { radius, epsilon }
    pub ef_runtime: Option<usize>,
    pub vsim_yield_as: Option<String>,
    // COMBINE leg
    pub strategy: FusionStrategy,
    pub count: usize,
    pub window: usize,
    pub combine_yield_as: Option<String>,
    // shared text options + reply shape
    pub params: HashMap<String, Bytes>,
    pub nocontent: bool,
    pub return_fields: Option<Vec<String>>,
    pub verbatim: bool,
    pub infields: Option<Vec<String>>,
    pub slop: Option<u32>,
    pub filters: Vec<(String, f64, f64)>,
    pub geofilters: Vec<GeoFilter>,
    // connection-level, ignored by shards (mirrors FtSearchRequest::timeout)
    pub offset: usize,
    pub limit: usize,
    pub sortby: Option<(String, SortOrder)>,
    pub nosort: bool,
    pub timeout: Option<Duration>,
}

impl FtHybridRequest {
    pub fn parse(args: &[Bytes]) -> Result<Self, String>;
    /// The text leg's index-level options — the twin of
    /// `FtSearchRequest::index_options` (wire.rs:337).
    pub fn text_options(&self) -> SearchOptions;
}
```

Then:

1. `ScatterOp::FtHybrid { index_name, request: Box<FtHybridRequest> }`
   (`message.rs:1279-1283`), matching `FtAggregate`'s `Box<FtAggregateRequest>` one variant
   above. `Box` for the same reason: the op is cloned per internal shard.
2. `handle_ft_hybrid` (`connection/search/hybrid.rs`) collapses to the fifteen-line shape of
   `handle_ft_search`: parse, derive `effective_timeout`, build the merge **from the
   request**, fan out. Its 71-line scan (22-92) is deleted.
3. `execute_ft_hybrid` (`query.rs:255-814`) loses its 383-line parser and its 22 locals; what
   remains is ~90 lines: resolve the index, substitute `$params`, `idx.hybrid_search(…)`,
   hydrate. The `Err("ERR SEARCH clause is required")` / `Err("ERR VSIM clause …")`
   validations (683-688) move into `parse`, where they are returned to the client **once**
   instead of once per internal shard.
4. One shared hydrator replaces the two copies:
   ```rust
   /// The reply fields for a hit whose content comes from the store (KNN and
   /// hybrid hits), applying RETURN and appending the op's synthetic scores.
   fn hydrate_from_store(
       store: &mut dyn Store,
       idx: &ShardSearchIndex,
       key: &str,
       nocontent: bool,
       return_fields: Option<&[String]>,
       extra: &[(String, String)],
   ) -> Option<Vec<(String, String)>>
   ```
   called from `execute_ft_knn_search` with `[("__vec_score", …)]` and from the hybrid loop
   with the zero-to-three `YIELD_SCORE_AS` pairs.
5. **Live defect 1 disappears structurally**: with no raw args on the wire there is no skip
   heuristic to get wrong. (It is fixed *ahead* of this work by the hotfix, so the two are
   independent — see *Effort*.)
6. **Live defect 2 becomes fixable in one place**: the internal shard now knows the SORTBY
   field, so the hybrid hit loop can fill `sort_value` the way `to_shard_reply` does for
   FT.SEARCH (`query.rs:35-39`). Whether FT.HYBRID *should* honor SORTBY, or refuse it, is a
   compatibility ruling — but the choice becomes expressible. Today it is not: the internal
   shard cannot honor an option it was never told about, and the connection layer cannot
   refuse one it half-parsed.

**Deletion test.** Delete `FtHybridRequest`: the grammar regrows in two places — 71 lines at
the connection layer and 383 in the shard — and the second copy has to re-invent a rule for
where the first copy's options end. That rule is defect 1. The type is not a wrapper; it is
the thing that makes the second parser unnecessary.

### SV12 — `HitMerge`: one merge, two policies

```rust
/// Cross-shard merge for the hit-shaped FT fan-outs (FT.SEARCH, FT.HYBRID).
/// The ordering policy is the only thing the two commands disagree about.
pub(crate) enum HitOrder {
    SortByKey { desc: bool },   // numeric-vs-lexical decided during absorb
    ScoreAscending,             // KNN: lower distance = better
    ScoreDescending,            // BM25 relevance, or hybrid fused score
    AsReceived,                 // FT.HYBRID NOSORT
}

pub(crate) struct HitMerge {
    cmd: &'static str,
    order: HitOrder,
    withscores: bool,
    global_offset: usize,
    global_limit: usize,
    // gathered state
    error: Option<Response>,
    all_hits: Vec<(String, ShardSearchHit)>,
    total: usize,
    sortby_numeric: bool,
}

impl HitMerge {
    pub(crate) fn for_search(request: &FtSearchRequest) -> Self;
    pub(crate) fn for_hybrid(request: &FtHybridRequest) -> Self;
}
```

`absorb` is the current `FtSearchMerge::absorb` verbatim, with `cmd` substituted into
`shard_conflict_or_bug` and the numeric detection unconditional (it is a no-op unless
`order` is `SortByKey`). `finish` is one `match self.order` followed by the shared
slice-and-render, with the `withscores` line guarded as today. `FtSearchMerge` and
`FtHybridMerge` cease to exist as types; `sort_by_key`, `fields_to_response` and
`shard_conflict_or_bug` are untouched.

**Difference 1 is fixed by construction** — there is one `absorb`, so there is one numeric
detection. Note precisely what that does and does not do: it makes FT.HYBRID's SORTBY compare
numerically *once the internal shard supplies a sort value*. On its own (SV12 landing before
SV11b) it changes nothing observable, because every hybrid sort key is still `""`. That is a
feature of the landing order, not a limitation: SV12 is a pure refactor at the moment it
lands, and stops being a latent trap the moment SV11b lands.

**Deletion test.** Delete `HitMerge`: the typed-reply absorb, the total accumulation, the
sort-key precompute, the numeric detection, the error arms, the offset/limit slice and the
RediSearch flat rendering regrow twice — and, on the evidence of the current tree, the second
copy will again be the one missing a piece.

## Testability improvement

*The interface is the test surface* — each part converts an untestable interior into a
callable one.

- **SV11a.** `plan()` is pure with respect to the searcher: it needs the schema, the field
  map and the index definition, and returns a value. That makes the three facts currently
  provable only through a full search assertable directly — **(i)** `LIMIT 0 0` yields
  `FetchWindow::CountOnly` and never reaches a collector (today pinned only indirectly, by
  `search_zero_limit_counts_without_fetching_documents`, index.rs:2112); **(ii)** the clamp
  holds for `limit == usize::MAX` (today pinned by
  `search_paging_window_edges_return_instead_of_panicking`, :2071, which can only observe
  that no panic occurred); **(iii)** `sort_by` on a NUMERIC field selects
  `CollectOrder::NumericFastField` and on a TEXT field selects `StringField` — the
  distinction that the string branch currently gets wrong, and which **no test in
  `frogdb-search` exercises at all today**. `execute()` becomes table-testable across the
  four `CollectOrder` values against one fixture index, replacing three hand-written arms
  that share no assertions.
- **SV11b.** `FtHybridRequest::parse` is a pure `&[Bytes] -> Result<_, String>`, so the
  entire FT.HYBRID grammar becomes unit-testable in `frogdb-search` beside
  `FtSearchRequest`'s tests — where today it is reachable only by standing up a
  multi-shard server (the eight tests at `tests/search.rs:6773-7115`, none of which
  covers option ordering). The first tests to write are the two live defects as
  regressions: **VERBATIM/INFIELDS/SLOP/GEOFILTER after LIMIT survive parsing**, and
  **SORTBY reaches the request**. Both are one-line assertions on a struct; both are
  currently only observable as a wrong query result three crates away.
- **SV12.** Ten `FtSearchMerge` tests and two `FtHybridMerge` tests become twelve `HitMerge`
  tests over four `HitOrder` values, so `HitOrder::SortByKey`'s numeric detection is covered
  once for both commands instead of once for one of them. The specific gap this closes is
  the reason difference 1 exists.

## Spec / LOCKED impact

**None owed.**

- **No locked crate is touched.** The four locked pairs are txn (`frogdb-txn` +
  `frogdb-vll`), persistence (`frogdb-persistence` + `frogdb-recovery`), replication
  (`frogdb-replication` + `frogdb-replication-runtime`) and cluster (`frogdb-cluster` +
  `frogdb-cluster-runtime`), with boundary ADRs `adr/0002`–`0004`. This proposal touches
  `frogdb-search`, `frogdb-core` and `frogdb-server` — none of which is in that set. **No
  `just mutants-gate` obligation and no `just mutants-diff` push discipline is owed.**
  Search is unlocked; verified, and stated as the brief asked.
- **No FM-tagged test is touched.** `grep -rn 'FM-'` over exactly the seven files in the
  *Files involved* table that are edited returns **no matches**. `grep` for
  `shard/search`, `frogdb-search`, `connection/search` and `index.rs` across
  `.scratch/hardening/specs/*.md` likewise returns nothing — no failure-mode row cites any
  of these paths. `just lint-failure-modes` needs no spec edit; run it anyway, since it is
  part of `just lint`.
- **Seam gates.** The one worth checking by name is `lint-continuation-lock`
  (`scripts/continuation-lock-gate.py`), because SV11b edits `core/src/shard/message.rs`.
  It pins **arm counts and enum parity for eleven `*Msg` enums** (script lines 81-91:
  `CoreMsg` 4, `PubSubMsg` 11, …) and pins `CoreMsg::ScatterRequest` as a GATE arm (98).
  `ScatterOp` is **not** one of the eleven — it is the payload of `ScatterRequest`, not a
  dispatch enum — and SV11b changes one `ScatterOp` variant's *fields* while adding and
  removing no `*Msg` arm and touching no `can_execute_during_lock` call. The gate is
  unaffected. `lint-format-float` pins `format_float` to `protocol/src/format.rs`; the
  hydration code uses plain `f32::to_string`, unchanged by this proposal, so it stays out of
  the gate's way — do not "tidy" it into a formatter while here. The remaining twelve gates
  (info seam, redirect seam, pub/sub confirmation, failover atomicity, metrics chokepoint,
  clock seam, durable-ack, nested config, error-sanitize, no-typed-unwrap,
  keyspace-notify-routing, script gate) cover none of these files. Run `just lint-gates`
  regardless — it is compile-free and lefthook runs it on every commit.
- **Wire-visible behavior.** SV11a and SV12 change no reply. SV11b changes an
  **internal** message payload only (`ScatterOp` never leaves the process). The two live
  defects are fixed, which *is* a wire-visible change — that is the point, and each is called
  out below with its scope.

## Risks / scope boundaries

### Boundaries vs sibling proposals

Proposals 63–70 were read on disk and their file tables compared against this one.

| Sibling | Owns | Edge with 71 | Resolution |
|---|---|---|---|
| **67** server small dedups (SV5/6/7) | `connection/{builder,deps}.rs`, `commands/search.rs`, `commands/timeseries.rs`, `connection/search/{helpers,index_mgmt,explain,synonyms}.rs`, `core/src/command.rs` | **Adjacent, zero file overlap — confirmed by reading 67.** 67 states explicitly that SV7 "edits four files in `connection/search/` but **not** `merge.rs`" (67:494). 71 edits `connection/search/{hybrid,merge}.rs` and neither of 67's four. SV6's 22 sites are in `server/src/commands/search.rs` (command *specs*), which 71 does not touch — including `FtHybridCommand` (`:329-346`), whose `ServerWide(ServerWideOp::FtHybrid)` strategy is unchanged | No coordination needed. Both may land in either order. Cite full paths in commits — there are three files named `search.rs` and two named `query.rs` in play |
| **67 follow-up** `debug_handler.rs:173` hard-coded 5s timeout | — | **Not claimed here.** 67 named it an out-of-scope follow-up (67:522-529); it is a `SearchMsg` shard-0 read, unrelated to the FT scatter path | Leave it to 67's follow-up |
| **66** ShardWorker builder | `core/src/shard/builder.rs`, `shards.rs`; reads `shard/search/lifecycle.rs` (66:80) and `execution.rs:1365,:1380` | **Soft edge, one shared file.** 66's `execution.rs` citations are in that file's test module; 71 edits `execution.rs:888` (the `ScatterOp::FtHybrid` arm). Different regions, same file | Whichever lands second rebases an eight-line hunk. 66 does not touch `shard/search/query.rs` |
| **70** ACL registry consult | acl crate, `command_spec.rs`, and `core/src/shard/search/config.rs` (70:84) | **Same directory, different file.** 70 edits FT.CONFIG's shard handler; 71 edits `query.rs` in the same directory | None |
| **68** EXEC framing datum | `connection/{transaction,dispatch,pubsub_conn_command}.rs` | **Read-only.** 71 cites `dispatch.rs:263` (`ServerWideOp::FtHybrid => handle_ft_hybrid`) as evidence and edits nothing in that file. 68 lists `connection/search/{helpers,…}` in its non-intersection note (68:444) and does not name `hybrid.rs` or `merge.rs` | None. If 68 reshapes `DispatchStage::ServerWide`, 71's handler signature is unaffected |
| **63 / 64 / 65 / 69** | `server/{mod,init,subsystems,cluster_init}.rs`, `runtime_config.rs` | None | — |

### Other risks

- **SV11b is a breaking internal wire change.** `ScatterOp` is an in-process enum, not a
  network format, so there is no compatibility window to manage — but a mixed build is
  impossible, meaning the `message.rs` / `execution.rs` / `hybrid.rs` /
  `shard/search/query.rs` edits must land as **one** commit. Do not try to split SV11b by
  file.
- **SV11b changes where FT.HYBRID's argument errors are produced.** Today
  `"ERR SEARCH clause is required"` is generated by every internal shard and the merge
  surfaces the first one (`merge.rs:500-502`); afterwards it is generated once, before the
  fan-out. The client-visible string is identical and the fan-out is skipped — a strict
  improvement, but it is a change in *which* component replies, and `tests/search.rs:7013`
  (`test_ft_hybrid_missing_param`) asserts on the message. Check that test rather than
  assuming; note that `"ERR No such parameter"` (`query.rs:691-694`) is resolved against
  `PARAMS`, so it can also move to parse time.
- **SV11b's fix for live defect 2 is a compatibility ruling, not a refactor.** Once the
  internal shard knows the SORTBY field, three answers are defensible: honor it (fill
  `sort_value` as FT.SEARCH does), refuse it (`-ERR SORTBY is not supported by FT.HYBRID`),
  or keep ignoring it but stop letting it displace fused-score order. Research what
  RediSearch 8 does before choosing; do not let the refactor pick by accident. **Landing
  SV11b without choosing is acceptable** provided the ignore-path is made explicit and the
  displacement is removed.
- **SV11a deliberately preserves the string-SORTBY selection deviation.** `CollectOrder::
  StringField` must, in the first commit, reproduce today's "collect by score, sort lexically
  in Rust" behavior exactly. Fixing it means either over-fetching all matches (a memory
  regression on large indexes) or requiring a `SORTABLE` fast field, which is what RediSearch
  itself requires — a real decision with a real compat surface. Pin the current behavior with
  a test first, so that the follow-up that changes it is visibly a behavior change and not a
  refactor side effect.
- **The hotfix's resume-set widening has a contrived false positive.** Adding `VERBATIM`,
  `INFIELDS`, `SLOP`, `GEOFILTER` to the skip loop's resume set means a `SORTBY`ed field
  literally named `slop` would terminate the skip early and be read as an option. That
  hazard already exists for the fifteen names in the set today (`FILTER`, `RETURN`,
  `LIMIT`, …) and the four additions are strictly less likely than the existing ones. It is
  the correct fix *for the current structure*; SV11b removes the structure and with it the
  hazard.
- **`Box<FtHybridRequest>` size.** `FtSearchRequest` is already boxed into the scatter op
  (`connection/search/query.rs:33`, `message.rs:1268-ish`) and `FtAggregateRequest` likewise
  (`message.rs:1275-1278`). `FtHybridRequest` is larger than both (three legs plus the shared
  text options), and the op is cloned once per internal shard. Box it for the same reason the
  other two are boxed; do not introduce an `Arc` for this — the existing pattern is `Box` +
  `Clone`, and consistency here is worth more than one allocation per shard per query.
- **Do not fold `to_shard_reply` into the shared hydrator.** It applies the same RETURN /
  NOCONTENT policy but to fields that came from the *index*, not the store, and it is the
  only one of the three that propagates `sort_value`. Two adapters would be a real seam;
  merging all three would produce a function with a `source: Index | Store` flag, which is
  the branch it was supposed to remove.
- **`RANGE` and `EF_RUNTIME` stay unimplemented.** SV11b gives them typed homes
  (`VsimMode::Range`, `ef_runtime`) so that "parsed and discarded" becomes greppable, and
  should keep the behavior byte-identical. Implementing vector range queries is a feature,
  not this proposal.

## Effort estimate

**L overall**, four independently landable commits:

| Item | Effort | Notes |
|---|---|---|
| **Hotfix H-A** (resume set) | **XS** | Four names added to one `matches!` in `core/src/shard/search/query.rs:653-670`, plus a regression test. Lands on `main` alone, ahead of everything else |
| **SV12** `HitMerge` | **S** | One crate, one file, no wire change. Two structs → one; twelve tests re-homed. Behavior-preserving at the moment it lands |
| **SV11a** `QueryPlan` | **M** | One crate (`frogdb-search`), one file, no cross-crate signature change (`search` keeps its shape). The care is in preserving the window math and the string-SORTBY behavior exactly. Well-covered by the crate's existing 757-line test module, plus the new plan tests |
| **SV11b** `FtHybridRequest` | **M–L** | Four crates' files in one commit (wire, message, execution, two handlers). ~450 lines out of `shard/search/query.rs`, ~200 into `wire.rs`, ~40 saved by the shared hydrator. The largest single diff and the only one with a wire change |
| Mutation re-gate | **none** | No locked crate touched |

**Recommended landing order: H-A → SV12 → SV11a → SV11b.** H-A first because it is a live
silent-wrong-answer fix that should not wait behind an L-sized refactor. SV12 next because it
is the smallest and shares no file with SV11a. SV11b last because it is the only commit that
crosses crates and because `HitMerge::for_hybrid(&FtHybridRequest)` is cleanest to write once
`HitMerge` exists.

### Independently-landable hotfix candidates

**H-A — the shard's skip heuristic swallows VERBATIM / INFIELDS / SLOP / GEOFILTER.**
**Confirmed LIVE**, traced end-to-end in *Problem* (client → `handle_ft_hybrid`
(`connection/search/hybrid.rs:14`) → raw `query_args` on `ScatterOp::FtHybrid` →
`execute_ft_hybrid`'s skip arm (`core/src/shard/search/query.rs:646-675`) → `verbatim` stays
`false` → `HybridTextOptions` (`:727-733`) → `hybrid_search` (`index.rs:1143-1155`) →
`parser.with_verbatim` never called (`index.rs:524`) → stemmed matches returned for a
`VERBATIM` query). The fix is to add the four names to the resume `matches!` at
`query.rs:653-670`. Wire-visible, silent-wrong-answer today, order-dependent, and untested:
none of the eight FT.HYBRID integration tests (`tests/search.rs:6773-7115`) passes any of the
four options, let alone after `LIMIT`. Land it with a regression test that runs the same
query with the option before and after `LIMIT` and asserts the two agree.

**H-B — `FT.HYBRID … SORTBY` is a silent no-op that also discards fused-score order.**
**Confirmed LIVE**, traced in *Problem* (`hybrid.rs:50-52` sets `sortby_active` → shard skips
`SORTBY` at `query.rs:646` and emits `sort_value: None` at `:808` → `sort_key()` returns `""`
(`wire.rs:479-485`) → `merge.rs:516-517` sorts all-equal keys with a stable sort → arrival
order, and `:520-524`'s fused-score sort is skipped). **Not offered as a clean hotfix**,
because the minimal correct change is a ruling, not an edit. The conservative variant —
in `FtHybridMerge::finish`, fall back to fused-score ordering when every sort key is empty —
is one condition and is strictly better than today's behavior, but it papers over an
unimplemented option. Recommendation: file it, take the ruling with SV11b, and if the ruling
slips, land the conservative variant as an explicitly-labelled stopgap. It is listed here
because the round's hotfix ledger should carry it either way.

**Not hotfixes, named so they are not mistaken for one:** the string-SORTBY selection
deviation (`index.rs:657-662`), and the parsed-then-discarded `RANGE` / `EF_RUNTIME`
(`query.rs:278-279`, `:748`). Both are live-visible, both need a compatibility decision, and
neither is a one-line change.
