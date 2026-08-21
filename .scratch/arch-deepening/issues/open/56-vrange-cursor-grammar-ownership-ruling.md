# 56 — Decide who owns `VRANGE`'s cursor + `COUNT` grammar before 98 and 99 both land

Status: needs-triage

## What to build

`VRANGE` is a **fourth** cursor-plus-`COUNT` wire grammar in the repo, and proposals 98 and 99
each move a different piece of the surrounding machinery without either one claiming it. The
round must not end with two cursor-grammar owners by accident. This issue is the ruling, and it
has to be made before either proposal's implementation commit lands.

**The code.** `frogdb-server/crates/commands/src/vectorset/vrange.rs` — on `origin/main` the
`execute` body is `:40-83`; the proposal cites `:36-62` and the sentinel at `:68-72`, which is the
pre-`d48e1b44` numbering (a `+6` doc-block insertion landed in every vectorset file, so add 6 to
read the proposal's cites). The grammar is `VRANGE key cursor [COUNT count]` under
`Arity::Range { min: 3, max: 4 }` (`vrange.rs:24`), and it is parsed by an `if/else` that writes
the **same count ladder twice** — `vrange.rs:47-55` for the `COUNT <n>` form and `:56-65` for the
bare positional form, both spelling `"Invalid count"` twice inside a single expression. The cursor
itself is a lexicographic-position sentinel, not a keyspace cursor: `vrange.rs:74-78` maps `"0"`
or the empty string to `b""` and otherwise passes the bytes straight to `vs.range(cursor_bytes,
count)` (`types/src/vectorset.rs:514-526`, a `BTreeMap` range with `.take(count)`).

**The two claims that must be reconciled.** Proposal 98 promotes `ScanRequest`, `ScanRequest::parse`,
`empty_scan_reply` and `scan_reply` out of `commands::utils` into `commands::scan` and widens the
struct with `key_type`/`novalues`, making `commands::scan` the single owner of *the SCAN family's*
grammar — but it deliberately scopes itself to SCAN/HSCAN/SSCAN/ZSCAN and states (98's sibling
table, and again in its non-goals) that it does **not** touch `VRANGE`. Proposal 99 unifies
`VRANGE`'s two ladders — along with `vlinks.rs`, `vrandmember.rs`, and the byte-identical
`parse_usize`/`parse_f32` copies in `vadd.rs`/`vsim.rs` — onto `utils::parse_usize`
(`commands/src/utils.rs:19` re-exporting `types/src/args.rs:316-319`), as its **H2**, which 99
explicitly flags as a **wire change** rather than a free fix: today's `"Invalid count"` becomes
`"ERR value is not an integer or out of range"`, and zero tests observe the current strings, so it
would land silently.

**The question to decide, stated plainly:** does `VRANGE` join `ScanRequest` (one cursor-grammar
owner, 98's module) or `utils::parse_usize` (grammar stays bespoke, only the integer parse is
canonicalised, 99's H2)? 99's own analysis argues for the latter and 98 agrees it is the right
call — `VRANGE`'s cursor is a vector-index position, not a keyspace cursor, and `ScanCaps` has
nothing to say about it — but that leaves the repo with two cursor-grammar owners, which is a
defensible outcome only if it is a **decided** one and written down where the next round will find
it. The ordering consequence follows from the answer: if `VRANGE` stays with `parse_usize`, the
two proposals are order-independent; if it joins `ScanRequest`, 99 must land before 98 so 98 has
a single subject to absorb.

Two facts the ruling should carry regardless of which way it goes. First, H2 is a wire change and
needs assertions on the *current* strings landed **before** it, or the change is invisible; the
family's four error literals appear nowhere in either vectorset regression file today. Second,
`vrange.rs` is behind `#[cfg(feature = "vectorset")]` (`commands/src/lib.rs:61` on `origin/main`;
`:59` in the round's worktree), so it is **invisible to a default `just check`** — unlike 98's
files, which are all `core-profile`. Whoever ends up owning it must build with
`--features vectorset` to see their own change compile.

## Acceptance criteria

- [ ] A written ruling in this issue names the owner of `VRANGE`'s cursor + `COUNT` grammar —
      98's `ScanRequest`/`commands::scan`, or 99's `utils::parse_usize` with the grammar left
      bespoke — and states the resulting 98/99 ordering constraint
- [ ] `CONTEXT.md` (or the SCAN-family doc the ruling designates) records how many cursor-grammar
      owners the repo has and why, so the next round does not re-litigate it
- [ ] `vrange.rs`'s duplicated count ladder (`:47-55` and `:56-65`) is reduced to one parse under
      whichever owner is chosen
- [ ] Before any error-string change ships, a regression test in
      `frogdb-server/crates/redis-regression/tests/vectorset_regression.rs` asserts the **current**
      `"Invalid count"` replies for `VRANGE k 0 COUNT abc` and `VRANGE k 0 abc`, so the H2 wire
      change is visible as a deliberate diff rather than a silent one
- [ ] `just test frogdb-redis-regression vrange` (run with `--features vectorset`)

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 98 (`.scratch/arch-deepening/proposals/98-scan-grammar-unify.md`),
defect I1; and proposal 99 (`.scratch/arch-deepening/proposals/99-vectorset-file-collapse.md`),
ruling request (b) — a single ruling covers both.

## Comments
