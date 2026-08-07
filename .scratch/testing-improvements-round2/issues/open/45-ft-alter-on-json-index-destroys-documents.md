# `FT.ALTER` on an `ON JSON` index destroys every document

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/10 F5 · MASTER.md §3
Score: severity 4 · likelihood 3 · effort 2 · priority 16
Area: frogdb-core / shard search index management

## Context

`FT.ALTER` reopens the index by deleting the whole tantivy directory and rebuilding it from a
rescan closure — and that closure has no `IndexSource::Json` branch at all, only a hash branch. On
an `ON JSON` index every document is therefore silently dropped: `FT.ALTER` returns `+OK` and every
subsequent `FT.SEARCH` returns zero hits, permanently. The sibling `FT.CREATE` closure gets this
right, which is what makes the omission invisible on review. Adding a field to an existing JSON
index is a routine schema-evolution step.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `core/src/shard/search/index_mgmt.rs:128-141` — the `alter` scan closure is
  `if let Some(value) = store.get(&key) && let Some(hash) = value.as_hash() { idx.index_hash(...) }`
  with **no `IndexSource::Json` branch at all**.
- Compare the *sibling* `execute_ft_create` closure at `core/src/shard/search/create.rs:36-52`,
  which correctly does `let is_json = def.source == IndexSource::Json;` and dispatches to
  `index_json`/`index_hash`.
- The destructive half is `search/src/index.rs:1010-1015` — `std::fs::remove_dir_all(path)` then
  `Index::open_or_create` — so this is not a stale-doc bug, it is a full wipe.
- **Why the existing tests pass anyway**: all five `FT.ALTER` tests in `server/tests/search.rs`
  (`:147`, `:187`, `:206`, `:251`, `:2949`) use HASH indexes; the file contains 8 `"JSON"`
  occurrences and none of them is an ALTER. The regression suite has 3 `FT.ALTER` uses, also HASH.
  `execute_ft_alter::{closure#0}::{closure#0}` is `covered`, 4 tests, 25/27 regions — the two
  uncovered regions are the JSON path that does not exist.

## What to fix

1. Give the `alter` rescan closure the same `IndexSource` dispatch as `create.rs:36-52` — ideally
   by extracting one shared closure so the two can never diverge again.
2. Check the VECTOR sidecar rebuild at `index.rs:1034`, which is also rebuilt from scratch and
   depends on the same closure.
3. Consider making the rebuild non-destructive (build into a temp dir, then swap) so a failure
   mid-rescan cannot leave an empty index either.

## Acceptance criteria

- [ ] New test creates an `ON JSON` index over `doc:*` with 3 JSON documents, asserts `FT.SEARCH`
      finds 3, runs `FT.ALTER … SCHEMA ADD newfield TEXT`, then asserts `FT.SEARCH` **still** finds
      3 and that a query on `newfield` matches. **Fails today** (returns 0).
- [ ] The HASH mirror of the same test exists so the two sources are pinned together.
- [ ] A case asserts `FT.ALTER` preserves VECTOR-field contents.
- [ ] The rescan closure has a single `IndexSource` dispatch shared with `execute_ft_create`.

## Test boundary

**3** (`shard_driver`) — this is pure command semantics against a real `ShardWorker` and store; it
needs no socket, no routing, and no RESP. Today it *cannot* be written there because `shard_driver`
has zero FT.\* support (proposal 10/F15 is the enabling work), so land it at level 4 for now and
move it down when that support exists.

## Depends on

- Infrastructure I4 (conservation checker for derived structures) — issue 04,
  `.scratch/testing-improvements-round2/issues/`. The invariant `FT.INFO num_docs == number of
  prefix-matching keys` is the cheap detector for this whole bug class and is shared with issue 46.
- `shard_driver` FT.\* support (proposal 10/F15, "no FT.\* coverage exists at any boundary below
  the socket") is **not** currently one of the I1–I18 infrastructure items, so there is no issue
  number to cite for it. Until it exists, this test lands at level 4.

## Re-triage 2026-08-06

**Verdict: still-valid**

All three cited sites are unchanged, and the line numbers still hold.
`core/src/shard/search/index_mgmt.rs:128-141` — the `self.search.alter(name, new_fields, |idx| …)`
rescan closure — is still hash-only (`if let Some(value) = store.get(&key) && let Some(hash) =
value.as_hash() { idx.index_hash(...) }`) with **no `IndexSource::Json` branch**, while the sibling
`core/src/shard/search/create.rs:38-52` still does `let is_json = def.source == IndexSource::Json;`
and dispatches to `index_json`/`index_hash`. The destructive reopen is still
`std::fs::remove_dir_all(path)?` → `Index::open_or_create(dir, …)` at
`search/src/index.rs:1010-1015` (a second copy at `:897`), and `VectorFieldManager::new` is still
rebuilt from scratch at `:1034`. `frogdb-search` / `frogdb-core`'s search module were never part of
the hardening campaign (no locked crate, no FM row mentions `FT.ALTER`), and the test picture is
unchanged: all six `FT.ALTER` uses in `server/tests/search.rs` (`:147`, `:187`, `:206`, `:251`,
`:2950`) are HASH indexes, and none of the `ON JSON` tests (`:5801`+, `:7420`+) alters. Confirmed
live data-loss defect.
