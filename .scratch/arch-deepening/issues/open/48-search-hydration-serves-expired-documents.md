# 48 — LIVE-suspect: `FT.*` and `TS.*` hydrate result documents through raw `Store::get`, serving expired-unreaped content

Status: needs-triage

## What to build

Five sites in `frogdb-core` materialise multi-key result sets by reading each matched key through
raw `Store::get`, which does **no expiry check** (`core/src/store/hashmap.rs:934-943`; the crate's
own contract test at `:2104-2140` pins the bypass as intended and says callers needing "expired
reads as absent" must use `get_with_expiry_check`). Unlike `search_hook.rs`, these read keys they
did **not** just write, so a document past its deadline that the 100 ms index-driven sweep has not
yet reaped can be hydrated into a reply that `EXISTS` denies in the same connection.

**Search — `core/src/shard/search/query.rs` (all three verified at HEAD).** `:115` sits in
`execute_ft_knn_search` (fn at `:90`, reached from `execute_ft_search` at `:52` for vector
queries); `:208` sits in `execute_ft_aggregate` (`:165`); `:756` sits in `execute_ft_hybrid`
(`:255`). Each is the same shape — take the hit key from tantivy, `store.get(&Bytes::from(hit.key))`
to populate `fields`, with a comment at `:111-112` saying only *"a hit whose document is missing
from the store carries no content"*. **This is specifically the KNN / `FT.AGGREGATE` / `FT.HYBRID`
hydration path, not plain lexical `FT.SEARCH`**, which reads its content straight off the tantivy
document (`frogdb-search/src/index.rs`) and never consults the store at all.

**Time series — `core/src/shard/timeseries_execution.rs:32` (`TS.MGET`) and `:63` (`TS.MRANGE`).**
Both query a label index (`label_index.query(&filters)`) and then hydrate every matching key with
`self.store.get(&key)` inside the loop. The proposal's per-site table rated these
"**LATENT**-suspect — reached from handlers that have already probed the key"; **reading the code
does not support that**: these are label-index scatter reads over keys the handler never named, so
the shape is identical to the `FT.*` case and the "already probed" defence does not apply. Re-rate
during triage.

**Rating: LIVE-suspect, not confirmed.** The adversarial review deliberately stopped short of
proving reachability, because it needs a live `FT.CREATE` fixture and, more importantly, because
whether such a document reaches the hydration step at all depends on the **index maintenance path**
— if lazy/active expiry already de-indexes the doc before the query runs, the hit never appears and
these `get` calls are harmless. That question is owned by `frogdb-core`'s search hook and was
explicitly declared out of proposal 97's scope. Step one of this issue is therefore to confirm or
refute reachability with a test, not to change code.

**Fix direction if confirmed**: route the five hydration reads through `get_with_expiry_check` so an
expired doc yields `None` and the hit carries no content (search), or is skipped entirely
(time series). Note that for `FT.*` that only closes the *content* leak — the hit key itself still
comes from tantivy — so the complete fix likely lives on the de-indexing side.

**Related open issues — coordinate, do not duplicate.**
`.scratch/testing-improvements-round2/issues/open/22-expiry-not-checked-before-reads.md` (F9, still
valid per its 2026-08-06 re-triage) covers the **index side** of the same symptom for plain
`FT.SEARCH`: `search()` never consults the store, so a logically-expired doc is returned from
tantivy alone. This issue is its **hydration-side complement** in a different crate, a different
call path, and a different set of commands. Issue 86 (`86-search-residual-test-gaps.md`, F13)
independently confirms the split — *"KNN/hybrid content still resolves through `store.get(...)` in
`query.rs`, so a tiered-spilled key yields `fields: None` while `FT.SEARCH` reads from tantivy"* —
which is the same code path viewed through the tiered-spill lens. Whoever picks any of the three
should read all three and land one coherent fix.

## Acceptance criteria

- [ ] **Step one — reachability confirmed or refuted by a test.** With `DEBUG SET-ACTIVE-EXPIRE 0`,
      an indexed hash/JSON doc past its deadline, and no intervening read: assert whether
      `FT.SEARCH` (KNN), `FT.AGGREGATE`, `FT.HYBRID` and `TS.MGET`/`TS.MRANGE` return it. The test
      lands either way — as a fix's regression test, or as a named pin whose comment cites this
      issue and records that the index path already excludes such docs
- [ ] If reachable: no `FT.*` reply carries field content for a key that `EXISTS` reports as `0` in
      the same connection
- [ ] If reachable: `TS.MGET` / `TS.MRANGE` omit samples from a past-deadline, unreaped series
- [ ] The five raw `Store::get` call sites are each ruled in a comment at the code — fixed, or
      documented as unreachable with the reason — so a future `get` → `get_unchecked` rename does
      not have to re-derive the analysis
- [ ] `just test frogdb-server search` green; `just test frogdb-server timeseries` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 97 (`.scratch/arch-deepening/proposals/97-typed-store-access.md`),
defect F-4 / §Problem 7 per-site ruling table — `shard/search/query.rs:115`/`:208`/`:756` rated
"LIVE-suspect, out of scope, file as a follow-up issue", plus `shard/timeseries_execution.rs:32`/`:63`
rated "LATENT-suspect, needs a read".

## Comments
