# LOCKED is prose in three places — make the spec header the one machine-read manifest

Status: done
Type: mechanism (single source of truth)
Severity: likelihood 3/3 (already drifted once: 15 rows carry a locked area's badge without a
mutation run; `mutants-gate` threshold is typed by hand), consequence 2/3 (a wrong or stale gate
is silently accepted; a crate can leave the perimeter without anything noticing) — score 6
Area: campaign mechanism / spec-lint

## Problem

"Locked" is the campaign's central claim — *behavior in this area cannot change without a test
failing* — and no script reads it. The facts live as prose in three places and are consumed by
none of the tools that act on them:

| fact | where it lives | who reads it |
|---|---|---|
| area → gated crates | `CLAUDE.md` "Locked core areas" | nobody (`spec-lint.py`'s `NEXTEST_CRATES` is a flat *witness* list, not the perimeter) |
| gate threshold | `CLAUDE.md` prose; free text on line 3 of each spec; `just mutants-gate <crate> <threshold>` typed by hand | nobody |
| LOCKED vs draft | `Status: LOCKED (date) — <prose>` in five specs; `blocking.md` has no `Status:` line at all | nobody (`spec-lint` ignores the header) |
| `mutants-diff` before push | `CLAUDE.md` convention | nobody |

So `just mutants-gate frogdb-cluster 0.90` is accepted without complaint, a crate can be
extracted out of (or into) the perimeter with no lint noticing, and the README table in
`.scratch/hardening/` is the only cross-check — a hand-maintained one.

Ruled 2026-09-02 (grill session): the spec header becomes the manifest. The spec *is* the
contract, so the contract carries its own terms; `spec-lint.py` already parses every
`specs/*.md`; the issue tracker's `Status:` line + `just scratch-check` is the existing precedent
for a lint-enforced header key.

## Design (ruled)

**Header key block** at the top of every `specs/*.md`, immediately after the H1, one key per line,
parsed until the first blank line:

```
# Persistence — failure modes

Status: LOCKED (2026-08-02)
Gate: 0.85
Crates: frogdb-persistence, frogdb-recovery
```

- `Status:` is **required** on every spec. Values: `LOCKED` | `DRAFT`, nothing else. A
  parenthesised date may follow `LOCKED`; it is parsed and otherwise informational.
  `blocking.md` gets `Status: DRAFT`.
- `LOCKED` ⇒ `Gate:` and `Crates:` are **required**. `DRAFT` ⇒ both are **forbidden** (a draft
  with a gate is a lock that forgot to say so). No third state: a crate with a mutation gate has
  a contract, and that is what LOCKED means.
- `Gate:` is a decimal in `(0, 1]`, one per spec, applying to every crate the spec lists.
- `Crates:` is the **mutation perimeter** — the crates `cargo mutants -p` runs on — not the
  witness search space. A crate appears in **exactly one** spec's `Crates:` (lint error
  otherwise) and must be a workspace member. The parser accepts an entry of the form
  `crate` only; the `crate/path` form (a sub-tree gate, which campaign-2 W5 will need for
  `frogdb-core`'s `shard/` and `persistence/` subtrees at 0.70) is reserved and rejected until a
  spec uses it, so the extension is one line, not a redesign.
- The prose that follows today's `Status:` line (score at lock, survivor count) moves below the
  key block as ordinary text; the lint ignores it. `NEXTEST_CRATES` in `spec-lint.py` is
  untouched — it is a spec-lint implementation detail, not a contract term.

**One parser**, a small importable module (`scripts/locked_areas.py` or similar) used by
`spec-lint.py`, the new gate, `mutants-gate.py`, and `workflow_gen` (issues 15/16 generate the
CI paths filter from it). No second regex anywhere.

**Consumers:**

1. `just lint-locked-areas` — new **compile-free** gate in the `lint-gates` family
   (`agents/seam-lints.md` entry: *every locked crate is declared in exactly one spec header*).
   Checks: `Status:` present and legal; LOCKED ⇒ Gate + Crates present, DRAFT ⇒ absent; gate in
   range; every crate resolves to a workspace member; no crate in two specs. Runs on every commit
   via lefthook (no `CLAUDECODE` skip) and in CI's `seam-gates` job. Row linting stays in
   `lint-spec`, which only needs the header for its area list.
2. `just mutants-gate <crate>` — the threshold argument is dropped; the script looks the crate up
   in the manifest. A crate in no LOCKED spec is an error. `--min-score` stays as an explicit
   override for experiments.
3. `just mutants-diff <crate>` — same lookup; refuses a crate outside the perimeter. Verify it
   propagates cargo-mutants' non-zero exit on a missed mutant (issue 15's CI job relies on the
   same verdict reading identically locally).
4. `just locked-areas` — prints the table (area, status, gate, crates) for humans and agents.
5. `CLAUDE.md` "Locked core areas" — drop the crate lists and gate numbers; keep the rules
   (spec-first, forcing test in the owning crate, documented equivalents at the code) and point
   at `just locked-areas`. The "run `just mutants-diff` before pushing" imperative is
   **downgraded to advice** once issue 15 lands: "CI runs `mutants-diff` on locked-crate changes
   and fails on any missed mutant; run it locally to iterate faster."
6. `website/scripts/spec-gen.py` — verify it renders the key block sensibly (it publishes
   `specs/*.md`); adjust if the block needs a table or a strip.

## Not in scope

- Running `lint-spec` in CI (issue 14), the diff-mutation CI job (issue 15), the scheduled full
  gate (issue 16).
- Row-level mutation provenance ("was this row ever in a mutation run") — campaign-2 W3 re-runs
  the gates over the 15 post-lock rows; per-row metadata was considered and deferred.

## Forcing test

`scripts/tests/test_spec_lint.py`-style fixture cases for the new gate: a spec with no
`Status:`; `DRAFT` with a `Gate:`; `LOCKED` missing `Crates:`; a crate named by two specs; a
crate that is not a workspace member; a `crate/path` entry (rejected as reserved). Each must
fail with a message naming the spec and the key. `just mutants-gate frogdb-cluster` must print
`gate: 80.0%` without an argument; `just mutants-gate frogdb-server` must refuse.

## Resolution

Landed as `feat(spec): spec headers are the locked-areas manifest`. `scripts/locked_areas.py`
owns the header parser; `just lint-locked-areas` (in `lint-gates`), `just mutants-gate <crate>`,
`just mutants-diff <crate>`, `just locked-areas`, and `website/scripts/spec-gen.py` read it.
`blocking.md` and `memory.md` carry `Status: DRAFT`; `CLAUDE.md` no longer restates crates or
gates. Deferred: `spec-lint.py` still derives the area from the filename (it needs nothing from
the header); the `mutants-diff` downgrade to advice in `CLAUDE.md` waits on issue 15.
