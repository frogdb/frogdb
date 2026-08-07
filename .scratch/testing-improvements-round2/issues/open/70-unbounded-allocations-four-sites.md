# Unbounded allocations at four client-reachable sites take the process down

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/06 F9 · 10 F6 · 07 F14 · 09 F12 · MASTER.md §3
Score: severity 4 · likelihood 3 · effort 1 · priority 17
Area: cross-crate — commands (string, bloom/cuckoo), search, scripting

## Context

Four independent sites size an allocation from client-supplied numbers with no ceiling. Each
turns one request into a multi-gigabyte allocation or an integer wrap — a process-level abort
taking every other connection with it. Filed together because the fix shape is identical and
`INFRASTRUCTURE.md` I11 names three of them as instances one harness catches.

A fifth site from the same audit row — unbounded RESP nesting depth (08/F4) — was split out to
issue 95, `.scratch/testing-improvements-round2/issues/`. It is the only one of the five carrying
an unresolved `OPTIONS:` block, and keeping it here would have held four fully-specified fixes
behind one open decision.

**These are suspected live defects found by reading, not by test failure — the proposed tests fail
against today's code.** The evidence is the auditing agents' and needs confirmation before or during
the fix; none is the row the coordinator verified directly.

## Evidence

### Site 1 — `LCS` DP matrix (06/F9; severity 4 · likelihood 2 · effort 1 · priority 15)
`string.rs:1054` — the DP matrix is allocated from the two input lengths with no ceiling (two 100KB
strings is ~80GB). `string.rs:1022-1031` uses `.unwrap_or_default()` on the key fetch, so a list or
hash at either key is silently an empty string. `string.rs:1069` returns the bare `LEN` integer
before reaching `:1074`, so `LCS k1 k2 LEN IDX` returns an integer instead of erroring; `:1015-1019`
rejects `WITHMATCHLEN` without `IDX`, which Redis accepts. Same family: `APPEND` has no 512MB
ceiling at all (`string.rs:196-213`) whereas `SETRANGE` has one, and that one is a hardcoded
`MAX_STRING_LEN` const (`string.rs:338`) rather than live-mutable `proto-max-bulk-len`.

### Site 2 — FT deep offset `2×(offset+limit)` (10/F6; severity 4 · likelihood 3 · effort 1 · priority 17)
`search/src/index.rs:571` `(fetch_offset, fetch_limit) = (offset, offset + limit)` — the offset is
*added to* the limit, so deep offsets scale the allocation linearly; `index.rs:594 / 634 / 699` all
pass `fetch_limit` to `TopDocs::with_limit`; tantivy 0.26.1
`src/collector/top_score_collector.rs:609-611` — `let vec_cap = top_n.max(1) * 2; buffer:
Vec::with_capacity(vec_cap)`. No clamp in `wire.rs` (`:121-122` parses any `usize`), in
`commands/search.rs`, or in `index.rs`. The geo path at `index.rs:568-569` uses `(0, raw_total.max(1))`
— every matching doc when a `GEOFILTER` is present. `rg` finds no test using a `LIMIT` above 1 000.

### Site 3 — `BF/CF.LOADCHUNK` `usize` wrap (07/F14; severity 4 · likelihood 2 · effort 2 · priority 14)
`commands/src/bloom.rs:495-668` and `commands/src/cuckoo.rs:559-748`. `CF.LOADCHUNK`'s `let fp_bytes
= num_buckets * layer_bucket_size as usize * 2;` can wrap on the `usize` multiply, so the subsequent
`offset + fp_bytes > data.len()` check passes and per-bucket indexing panics — a crash-loop, since
the filter is `WalStrategy::PersistFirstKey` and survives restart. `Vec::with_capacity(num_buckets)`
with `num_buckets ≈ 2^63` is an OOM abort. No semantic check on `k == 0`, `capacity == 0`, `count >
capacity`, or NaN/negative `error_rate`. `CfLoadchunk::execute` (131 regions) and `BfLoadchunk::execute`
(112) are both `single-test`, reached only by `bloom_regression::{cf,bf}_scandump_loadchunk_roundtrip`.

### Site 4 — FUNCTION LOAD capture VM has no memory limit (09/F12; severity 4 · likelihood 2 · effort 2 · priority 14)
`scripting/src/loader.rs:57` sets `memory_limit_bytes: 0` in the `SandboxOptions` for the Load VM, and
`scripting/src/sandbox.rs:145-148` only calls `set_memory_limit` when the value is non-zero (`0` is
documented as "unlimited" at `sandbox.rs:121`). The execution VM does get the cap
(`core/src/scripting/lua_vm.rs:96`, default 256 MB). None of `loader.rs`'s 17 tests supplies an
allocating library, and `sandbox.rs`'s own test helper also passes `memory_limit_bytes: 0`
(`sandbox.rs:941`), so the limit is never exercised in either VM.

## What to fix

1. Clamp the `LCS` DP allocation; fix `unwrap_or_default()` type-blindness, the `LEN IDX` / `WITHMATCHLEN` matrix, and give `APPEND` a live `proto-max-bulk-len` ceiling.
2. Clamp `fetch_limit` in `search()` (`index.rs:571`) — an `FT.CONFIG MAXSEARCHRESULTS`-style knob is the parity-correct home — and bound the `GEOFILTER` full-fetch path.
3. Use checked arithmetic for `fp_bytes`/`num_buckets`, add the LOADCHUNK semantic checks, and extract the header parse into a testable pure function.
4. Give the FUNCTION LOAD capture VM the same memory cap the execution VM gets.

## Acceptance criteria

- [ ] Site 1: `LCS` over strings whose product exceeds the budget errors; `LCS` on a list key is WRONGTYPE; `LEN IDX` errors; `APPEND` past the ceiling errors. Fails today.
- [ ] Site 2: `FT.SEARCH … LIMIT 0 100000000` errors or is clamped, and a GEOFILTER query does not allocate proportional to corpus size. Fails today.
- [ ] Site 3: malformed chunks (truncated header, `num_layers = u32::MAX`, wrapping `num_buckets`, `k = 0`, `count > capacity`, `error_rate = NaN`) each return a clean `CommandError`, no panic, key unchanged. Fails today.
- [ ] Site 4: `load_library` with a top-level allocating loop errors under a low cap; an `EVAL` past the 256 MB cap errors and the *next* EVAL on that shard still works. Fails today.

## Test boundary

Level 1 for all four guards — each is one arithmetic guard over pure input, so nothing above
level 1 adds signal. Two level-3 companions are needed because level 1 cannot see them: site 1's
type/option matrix, and site 2's `FT.CONFIG` knob.

## Depends on

issue 11 (I11 — registry-wide argument-fuzz harness; names 06/F9, 07/F14, 10/F6 as instances),
`.scratch/testing-improvements-round2/issues/`. Sibling: issue 95, same directory.

## Re-triage 2026-08-06

**Verdict: still-valid** (all four sites; per-site below)

None of the four sites is inside a locked area, and none was touched. Paths are now
`frogdb-server/crates/<crate>/src/...`.

- **Site 1 — LCS DP matrix: still-valid.** `commands/src/string.rs:1055` (was `:1054`) is still
  `let mut dp = vec![vec![0usize; n + 1]; m + 1];` with no ceiling. `.unwrap_or_default()` on both
  key fetches at `:1023-1032` (was `1022-1031`); the bare `LEN` return at `:1070-1072` (was
  `:1069`) still short-circuits before the `IDX` branch at `:1075`; `WITHMATCHLEN`-without-`IDX` is
  still rejected at `:1015-1020`. `APPEND` (`:197-214`, was `196-213`) still has no ceiling, and
  `SETRANGE`'s is still the hardcoded `const MAX_STRING_LEN` at `:339` (was `:338`).
- **Site 2 — FT deep offset: still-valid.** `search/src/index.rs:568-572` (was `:571` / `:568-569`)
  still computes `(offset, offset + limit)`, or `(0, raw_total.max(1))` when a `GEOFILTER` is
  present, and feeds `fetch_limit` to `TopDocs::with_limit` at `:594`, `:634`, `:699`. No clamp in
  `search/src/wire.rs:121-122` (still `parse_num(...).unwrap_or(0/10)` into `usize`), and
  `wire.rs:340` re-amplifies with `limit: self.offset + self.limit`. `rg` finds no
  `MAXSEARCHRESULTS` / `max_search_results` knob anywhere.
- **Site 3 — BF/CF.LOADCHUNK: still-valid.** `commands/src/cuckoo.rs:710` is still
  `let fp_bytes = num_buckets * layer_bucket_size as usize * 2;` — unchecked `usize` multiply
  guarding the `:711` bounds check and the per-bucket indexing at `:721`; `Vec::with_capacity` at
  `:690` (`num_layers` from a raw `u32`) and `:717` (`num_buckets` from a raw `u64`).
  `commands/src/bloom.rs:625` has the same `Vec::with_capacity(num_layers)` from `:622`. No
  semantic check on `k == 0`, `capacity == 0`, `count > capacity`, or NaN/negative `error_rate`
  (`bloom.rs:619-660`, `cuckoo.rs:679-735`). Old cites `bloom.rs:495-668` → **`567-668`**,
  `cuckoo.rs:559-748` → **`641-745`**.
- **Site 4 — FUNCTION LOAD capture VM: still-valid.** `scripting/src/loader.rs:57` still sets
  `memory_limit_bytes: 0`, and `scripting/src/sandbox.rs:146-147` still only calls
  `set_memory_limit` when the value is non-zero. The execution VM still gets the cap at
  `core/src/scripting/lua_vm.rs:103` (was `:96`). The sandbox test helper still passes `0`
  (`sandbox.rs:984`, was `:941`).
