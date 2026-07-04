# Proposal: FT.* Request + Shard-Hit Wire Types

Status: implemented
Date: 2026-07-04

## Problem

The FT.SEARCH / FT.AGGREGATE fan-out had no owned wire contract. The *request* grammar and the
*reply* layout were both emergent behaviors: two positional scanners on the request side and a
sentinel-keyed positional protocol on the reply side, split across two crates and agreeing only by
convention. The interface lived nowhere — there was no type a shard and the coordinator both
pointed at.

**Request side — one grammar, two parsers.** FT.SEARCH was parsed twice per query: the coordinator
(`server/connection/handlers/search/query.rs:34-98`) scanned a *subset* of the grammar
(LIMIT / NOCONTENT / WITHSCORES / SORTBY / TIMEOUT) to configure the merge, then forwarded the raw
args; each shard re-parsed the *full* grammar (`core/shard/search/query.rs:28-410`,
`parse_ft_search_options`: INFIELDS / INKEYS / FILTER / GEOFILTER / HIGHLIGHT / SUMMARIZE / SLOP /
VERBATIM / PARAMS on top of the coordinator's subset). Two hand-rolled positional scanners over the
same token stream, in different crates, with nothing enforcing agreement. They had already
diverged: the coordinator's subset scanner was not PARAMS-aware, so a PARAMS *value* spelled
`NOCONTENT` or `LIMIT` was misread as an option at the coordinator while the shard (correctly)
treated it as a parameter value (see [Correctness flags](#correctness-flags)).

FT.AGGREGATE was worse — parsed **1+N** times: the coordinator scanned coordinator-only options
(TIMEOUT / DIALECT / WITHCURSOR, `aggregate.rs:30-85`) *and* parsed the whole pipeline via
`parse_aggregate_pipeline` (`aggregate.rs:98`) for the merge steps; then every one of N shards
parsed the same pipeline again (`core/shard/search/query.rs:655-667`).

**Reply side — a sentinel protocol with no owner.** Results crossed shard→coordinator as a
positional convention inside the generic `Vec<(Bytes, Response)>`:
`format_ft_search_results` (`core/shard/search/query.rs:412-465`) emitted a first element keyed
`__ft_total__` carrying the shard count, then per-hit arrays whose layout was
`[score, sort_value-iff-SORTBY, score-again-iff-WITHSCORES, fields-iff-not-NOCONTENT]`. The
decoder (`server/.../search/merge.rs`) rebuilt that layout by convention: `key == b"__ft_total__"`
(`:298`), sort value at `items[1]` (`:312`), score re-parsed from a formatted string (`:306-308`),
and a stateful index walk in `finish` (`:373-388`) that had to *re-derive* which elements the
shard had chosen to emit. One layout invariant, two crates, zero shared types. FT.AGGREGATE's
partial-aggregate reply was the same disease at scale: ~120 lines serializing every
`PartialReducerState` variant to tagged `Response` arrays (`query.rs:748-866`) mirrored by a
226-line positional decoder (`merge.rs:643-868`, `parse_partial_reducer_states`) — a hand-written
codec pair for data that never leaves the process.

**The index seam was 13 positional arguments.** `search_inner`
(`search/src/index.rs:477-493`) took 13 positional parameters under
`#[allow(clippy::too_many_arguments)]`; `search` and `search_sorted` were pure pass-throughs
padding it with `None`/`Vec::new()` filler, and `search_with_options` existed to re-split a tuple.
The wrappers failed the deletion test — they added no behavior, only argument spelling
(`search_sorted` had zero callers).

Deletion test, applied to the whole seam: there was nothing you could delete to remove "the
FT.SEARCH wire contract." Delete the coordinator scanner and the merge misconfigures silently;
delete the shard's `__ft_total__` element and the decoder miscounts totals without erroring;
reorder one entry element on either side and hits silently lose their scores. Every one of those
is a *convention* failure the compiler could not see.

## Current state (before)

### Two scanners, one grammar (coordinator subset)

```rust
// server/.../search/query.rs:34-98 — scans LIMIT/NOCONTENT/WITHSCORES/SORTBY/TIMEOUT
// by walking the raw args; not PARAMS-aware, so a PARAMS value that spells an
// option keyword is misread. The full grammar lives in a *second* scanner in
// core/shard/search/query.rs:28-410 that the coordinator never consults.
while i < query_args.len() {
    let arg_upper = query_args[i].to_ascii_uppercase();
    match arg_upper.as_slice() {
        b"LIMIT" => { ... }
        b"NOCONTENT" => { nocontent = true; ... }
        ...
```

### The sentinel reply protocol

```rust
// core/shard/search/query.rs:417-421 (encoder)
results.push((
    Bytes::from_static(b"__ft_total__"),
    Response::Integer(search_result.total as i64),
));
// server/.../search/merge.rs:298-312 (decoder, other crate)
if key.as_ref() == b"__ft_total__" { ... }
let sort_val = if self.sortby_active && items.len() > 1 { /* items[1] */ }
```

## Implemented design

One module owns both directions of the shard boundary:
`frogdb-server/crates/search/src/wire.rs`. It lives in `frogdb-search` because that is the one
crate both ends already depend on (`core` executes queries with it, `server` merges with it) —
the types sit *below* the boundary they describe, so neither side can drift from the other.

> **The grammar is parsed exactly once, at the coordinator.** Shards receive the parsed request;
> the merge is configured from the same struct the shards execute. Option agreement is by
> construction, not by parallel maintenance.

### The request types

```rust
// search/src/wire.rs
pub struct FtSearchRequest {
    pub query: String,
    pub offset: usize,          // global paging window — applied at the coordinator
    pub limit: usize,
    pub nocontent: bool,
    pub withscores: bool,
    pub verbatim: bool,
    pub return_fields: Option<Vec<String>>,
    pub sortby: Option<(String, SortOrder)>,
    pub infields: Option<Vec<String>>,
    pub inkeys: Option<Vec<String>>,
    pub filters: Vec<(String, f64, f64)>,
    pub geofilters: Vec<GeoFilter>,
    pub highlight: Option<HighlightOptions>,
    pub slop: Option<u32>,
    pub summarize: Option<SummarizeOptions>,
    pub params: HashMap<String, Bytes>,
    pub timeout: Option<Duration>, // coordinator-only; shards ignore it
}

impl FtSearchRequest {
    pub fn parse(args: &[Bytes]) -> Self;          // the ONE grammar owner
    pub fn index_options(&self) -> SearchOptions;  // shard overfetch window 0..offset+limit
    pub fn is_knn(&self) -> bool;
}

pub struct FtAggregateRequest {
    pub query: String,
    pub steps: Vec<AggregateStep>, // pipeline parsed ONCE; shards run the local
                                   // prefix, the merge runs the merge steps
    pub timeout: Option<Duration>,
    pub withcursor: bool,
    pub cursor_count: usize,
    pub cursor_maxidle_ms: u64,
}
```

`ScatterOp::FtSearch` / `ScatterOp::FtAggregate` (`core/shard/message.rs`) now carry
`Box<FtSearchRequest>` / `Box<FtAggregateRequest>` instead of `query_args: Vec<Bytes>`. The
scatter path is in-process (tokio channels), so the typed request crosses as a plain Rust value —
no serialization layer, and `Box` keeps the `ScatterOp` enum small.

### The reply types

```rust
// search/src/wire.rs
pub struct ShardSearchHit {
    pub key: String,
    pub score: f32,                            // BM25 / KNN distance / fused score
    pub sort_value: Option<SortValue>,         // SORTBY merge key, typed
    pub fields: Option<Vec<(String, String)>>, // None = no content (NOCONTENT, or
                                               // KNN/hybrid doc missing from store)
}

pub struct ShardSearchReply { pub total: usize, pub hits: Vec<ShardSearchHit> }

pub enum FtShardReply {
    Search(Result<ShardSearchReply, String>),   // FT.SEARCH + FT.HYBRID
    Aggregate(Result<PartialAggregate, String>), // the typed partial, as-is
}
```

`PartialResult` (`core/shard/types.rs`) gains `ft: Option<FtShardReply>` alongside the existing
keyed results — additive, so the other ~25 scatter ops are untouched. The shard's
`execute_ft_search` / `execute_ft_aggregate` / `execute_ft_hybrid` now return
`Result<_, String>` and the dispatch (`core/shard/execution.rs`) wraps them with
`PartialResult::from_ft`. The `Err` string is the exact client-facing message, so error text is
byte-identical to before.

What this deleted outright:

| Deleted | Was | Lines |
|---|---|---|
| `__ft_total__` / `__ft_error__` / `__ft_search__` / `__ft_hybrid__` sentinels | both crates | all uses |
| `format_ft_search_results` + KNN/hybrid positional entry builders | `core/shard/search/query.rs` | ~110 |
| `PartialReducerState` → `Response` serializer | `core/shard/search/query.rs:748-866` | ~120 |
| `parse_partial_reducer_states` decoder | `server/.../search/merge.rs:643-868` | ~226 |
| Coordinator subset scanners (FT.SEARCH + FT.AGGREGATE handlers) | `server/.../search/{query,aggregate}.rs` | ~130 |
| Shard full-grammar scanner `parse_ft_search_options` + `FtSearchOptions` | `core/shard/search/query.rs:8-410` | ~400 |
| Per-shard pipeline re-parse (`parse_aggregate_pipeline` on N shards) | `core/shard/search/query.rs:655-667` | N-1 parses |

The merges (`FtSearchMerge`, `FtHybridMerge`, `FtAggregateMerge`) now operate on typed hits:
`FtSearchMerge::from_request(&FtSearchRequest)` derives every merge knob (SORTBY direction, KNN
ascending, WITHSCORES, global window) from the same struct the shards execute. The stateful
element-index walk in `finish` is gone — WITHSCORES emits `hit.score`, content emits
`hit.fields`, and a hit with `fields: None` emits nothing, which is exactly the old positional
behavior expressed as data instead of index arithmetic.

### The index seam

`search_inner`'s 13 positional arguments became one parameter object:

```rust
// search/src/index.rs
pub struct SearchOptions { offset, limit, sort_by, infields, highlight, slop,
                           summarize, verbatim, inkeys, filters, geofilters }

pub fn search(&self, query_str: &str, opts: &SearchOptions) -> Result<SearchResult, SearchError>
```

The pass-through wrappers are deleted (deletion test: they added no behavior): `search(q, o, l)`
callers use `SearchOptions::page(offset, limit)`, `search_sorted` had no callers, and
`search_with_options` *was* `search_inner` modulo a tuple re-split. `FtSearchRequest::
index_options()` is the single place the request's global window becomes the shard's overfetch
window (`0..offset+limit`).

### Why this is the right depth

- **Locality.** "What does FT.SEARCH accept and what does a shard send back?" has one answer:
  `wire.rs`. Adding a grammar option is one parser arm + one struct field, and the compiler walks
  you to the shard executor and the merge — previously it was two scanners, an encoder, and a
  decoder, all found by grep.
- **Leverage.** The merge logic shrank to actual policy (sort order, pagination, reply shape);
  everything that was transport re-derivation is gone. FT.AGGREGATE's partial state now crosses
  the boundary as the `PartialAggregate` it already was — the 350-line codec pair existed only
  because the boundary was stringly-typed.
- **Deletion test.** Delete `wire.rs` and you lose exactly the FT.* wire contract — every use
  site is a compile error, not a silent decode drift. That artifact did not exist before.

## Migration plan (as landed)

1. **Index seam.** `SearchOptions` + single `search()` in `frogdb-search`; delete
   `search_sorted` / `search_with_options` / `search_inner` wrappers; update callers
   (`hybrid_search`, shard aggregate, crate tests).
2. **Typed wire.** Add `wire.rs` (requests + replies + `FtShardReply`); `ScatterOp` carries the
   boxed requests; `PartialResult.ft` carries the typed reply; rewrite the shard executors and the
   three merges; delete both scanners, the sentinel encoders, and the reducer-state codec pair.
3. **Tests + proposal doc** (below).

## Testing impact

- **One grammar, unit-tested where it lives.** `wire.rs` tests cover the full-grammar parse
  (every option asserted), defaults, TIMEOUT-zero, KNN detection, the overfetch-window mapping,
  and the PARAMS/keyword-collision case the dual scanner got wrong. The aggregate parse pins
  coordinator-option extraction (WITHCURSOR COUNT/MAXIDLE, TIMEOUT, DIALECT) and that
  coordinator-only options never reach the pipeline parser.
- **Merges tested against typed hits** (`merge.rs` unit tests): BM25-descending across shards,
  SORTBY numeric detection across shards (`"10"` vs `"2"`), SORTBY string DESC, WITHSCORES +
  NOCONTENT reply shape, global OFFSET/LIMIT after merge, KNN ascending, shard-error
  short-circuit, hybrid fused-score ordering with content-less hits, and FT.AGGREGATE merging two
  real `execute_shard_local` partials (COUNT summed per group) plus its error path. None of these
  were unit-testable before — the merge could only be fed hand-built sentinel arrays.
- **Full-grammar regression, end to end.** `test_ft_search_full_grammar_multi_shard`
  (server integration suite): HIGHLIGHT + SORTBY DESC + LIMIT on a 4-shard server, asserting
  pre-pagination total, numeric (not lexical) cross-shard order, and highlighted field values.
  Coordinator-vs-shard option agreement is by construction now; this pins the observable behavior.
- **Wire compat pinned by the existing suites.** Client-facing FT.SEARCH / FT.AGGREGATE /
  FT.HYBRID reply shapes are unchanged; the search crate, server search integration, and
  redis-regression search suites run unmodified.

## Risks / open questions

- **FT.HYBRID request grammar is *not* unified.** Only its reply is typed (it shares
  `ShardSearchHit` / `FtShardReply::Search`); the hybrid grammar is still parsed by a
  coordinator subset scanner (`hybrid.rs`) and a shard full scanner (`execute_ft_hybrid`) —
  the same disease this proposal cured for FT.SEARCH. A typed `FtHybridRequest` is the natural
  follow-up; it was left out to keep this change reviewable.
- **In-process assumption.** `FtShardReply` crosses a tokio channel, not a network. If shards
  ever become remote processes, `wire.rs` is exactly the module that grows
  `Serialize`/`Deserialize` — the seam is where the encoding decision will live, which is the
  point.
- **Lenient parsing preserved, now in one place.** The grammar remains forgiving (malformed
  counts fall back to defaults, unknown tokens are skipped) to match the pre-existing behavior
  the regression suites pin. Tightening it (RediSearch errors on bad syntax) is now a one-file
  change but is deliberately not part of this extraction.
- **Merge sort-key semantics kept string-shaped.** `ShardSearchHit::sort_key()` reproduces the
  old wire's string form (`F64::to_string`, missing = `""`) and numeric detection still means
  "every observed key parses as f64", so cross-shard ordering is bit-identical to before. A
  future change could compare `SortValue` natively; that is a behavior change and was kept out.

## Correctness flags

- **Coordinator misparsed PARAMS values as options (fixed by construction).** The old
  coordinator scanner (`server/.../search/query.rs:34-98`) walked raw args without PARAMS
  awareness: `FT.SEARCH idx "*" PARAMS 2 p NOCONTENT` set the coordinator's `nocontent` flag
  (changing the merged reply shape) while the shard correctly treated `NOCONTENT` as the value of
  parameter `p`. With one PARAMS-aware parser feeding both sides, the collision is impossible;
  pinned by `test_search_request_params_not_misread_as_options`.
- **The drift class itself.** Every FT.SEARCH option existed in two scanners that nothing kept in
  sync, and every reply element existed in an encoder/decoder pair that nothing type-checked
  (e.g. the `finish` index walk had to *guess* whether `items[idx]` was a score or a fields array
  from replicated flags). These were latent, not hypothetical — the PARAMS case above is the
  instance that had already happened. The typed wire retires the class, not the instance.
