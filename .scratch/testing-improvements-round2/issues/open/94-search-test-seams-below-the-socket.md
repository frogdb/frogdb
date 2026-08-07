# No FT.* coverage exists at any boundary below the socket — build the two missing search test seams

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/10 F15 — enabling infrastructure omitted from `INFRASTRUCTURE.md`'s I1–I18
Tier: A
LOE: (i) ~1 day, (ii) sequence with issue 01 — *estimated*
Area: frogdb-search / `core/tests/shard_driver/`

## Context

Every FT.* test in the workspace runs over a socket. There is no level-1, level-2 or level-3
search test anywhere, which is why five findings in proposal 10 carry effort 3–4 instead of 2,
and why two live defects went undetected: `FT.ALTER` destroying JSON documents (issue 45,
`.scratch/testing-improvements-round2/issues/`) and 10/F8 are both cheap to catch at level 2–3
and prohibitively expensive at level 4.

This item was requested by proposal 10 as infrastructure and **was dropped from
`INFRASTRUCTURE.md`'s I1–I18 consolidation**, so no issue in the 01–18 range owns it. Issues 45
and 46 both name it as their enabling work and both stay pinned at level 4 until it exists.

Proposal 10 deliberately scored it **priority 11** rather than the formula's 16, on the grounds
that it is enabling work rather than a bug. That discount is the author's and is preserved here;
do not re-rank it upward without saying so.

## Evidence

- `rg -l 'FT\.' core/tests/` returns **nothing** — the `shard_driver` harness has zero FT.*
  coverage, despite `execute_ft_*` being ordinary `ShardWorker` methods reached through
  `ScatterOp` (`core/src/shard/execution.rs:834-889`).
- The `search` crate has **no `tests/` dir**.
- `ShardSearchIndex::open_in_ram` (`search/src/index.rs:287`), which exists explicitly "for
  testing", is `untested` at **0/41 regions** — the crate's own inline tests all use `TempDir` +
  mmap instead.
- Meanwhile 150 tests and 7,912 lines sit in `server/tests/search.rs`, with **135 sleeps**.
- The anti-pattern the brief names is present at scale: `FT.TAGVALS`, `FT.DICTADD`/`DICTDEL`/
  `DICTDUMP`, `FT.EXPLAIN`, `FT.SYNDUMP` and `FT.CONFIG GET` are all single-shard, non-blocking,
  non-RESP-specific operations tested exclusively through a full client + connection +
  scatter/merge flow.

## What to build

Two independent pieces. **(i) first** — proposal 10's explicit recommendation, because it is a
new directory with no existing-test churn and it unblocks the six cheapest findings immediately.

1. **`tests/` for `frogdb-search`, built on `open_in_ram`.** Home for findings 10/F1, F2, F6,
   F8, F10, F12, F14. Makes 100k-document corpora sub-second, and gives `open_in_ram` its first
   coverage.
2. **FT.\* drive seams on `shard_driver`.** Home for 10/F5, F11, F13. This is a change to a
   shared harness and must be sequenced with the other `shard_driver` extension work in issue
   01, `.scratch/testing-improvements-round2/issues/` — not landed independently.
3. Migrating the existing pure-semantics socket tests down is **explicitly not urgent**. What
   matters is that *new* tests land at the right level. Do not fold a 7,912-line migration into
   this issue.

## Acceptance criteria

- [ ] `frogdb-search/tests/` exists and at least one test there builds an index via
      `ShardSearchIndex::open_in_ram` with no `TempDir` and no mmap.
- [ ] `open_in_ram` (`search/src/index.rs:287`) is no longer at 0/41 regions.
- [ ] At least one FT.* command is driven through `core/tests/shard_driver/` without a socket,
      via the `ScatterOp` path at `core/src/shard/execution.rs:834-889`.
- [ ] The seam added in (2) is reviewed against issue 01's harness changes before landing, and
      this issue records which of the two landed first.
- [ ] Issues 45 and 46 are re-scored against the new boundary once (i) and (ii) exist — both
      currently carry a level-4 estimate that assumes this work does not exist.

## Test boundary

This *is* the boundary work: it creates levels 2 and 3 for a subsystem that currently only has
level 4. Per proposal 10, (i) is level 2 (crate API over an in-RAM index) and (ii) is level 3
(real `ShardWorker`, real scatter, no socket).

## Depends on

Issue 01, `.scratch/testing-improvements-round2/issues/` — for part (2) only, which shares the
`shard_driver` harness. Part (1) depends on nothing and can start immediately.

## Re-triage 2026-08-06

**Verdict: still-valid**

The gap is intact — with one evidence bullet retracted. `frogdb-search` still has **no `tests/`
dir** (`crates/search/` is `Cargo.toml` + `src/` only), and `rg -l 'FT\.'` over both
`crates/core/tests/` and the relocated harness tree `crates/shard-harness/tests/` returns nothing,
so there is still no FT.* coverage at level 1-3. `server/tests/search.rs` has grown to **7,933
lines with 135 sleeps**. The crate saw no commits at all since 2026-07-21 (`5716945b`,
`72d07e47`), and the hardening campaign never touched it. **Retract the `open_in_ram` bullet:**
`ShardSearchIndex::open_in_ram` (`search/src/index.rs:287`) is *not* at 0/41 regions — it has ~20
in-crate `#[cfg(test)]` callers today (`index.rs:1447-1949`, `spellcheck.rs:161-275`) and already
had 15 at audit-filing commit `a0e85aac`. That figure is a false negative from the broken lcov
pipeline (issue 27 / decision D3, issue 31). Acceptance criterion 2 is therefore already met and
should be struck; criterion 1 (a real `tests/` dir) still stands on its own merits. Correction to
the re-triage brief's premise: `frogdb-search` is **not** behind a cargo feature — it is a plain
workspace member (`Cargo.toml:34`) and a non-optional dependency of both `frogdb-core` and
`frogdb-server`; the feature-gated things are the FT.* command *families*. Stale ref: part (2)'s
home is now `frogdb-server/crates/shard-harness/tests/`, not `core/tests/shard_driver/`.
