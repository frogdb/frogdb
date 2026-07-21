# Spec: compatibility/test-suite.mdx
Status: update (generated, exists)
Audiences: A2, A5

Goal: The reader sees which upstream Redis regression suites FrogDB has ported,
how many tests run, and every intentional exclusion with its documented reason
and category — all generated directly from the regression-test source so it is
always in sync. A skeptic (A2) uses this page as the evidence backing the
"supported" claims on the Command matrix; a contributor (A5) uses it to see
what is and isn't covered. The page already exists and is data-driven; this
spec is mostly "keep as-is" plus required fixes to the generator that feeds it.

Not in scope:
- Command supported/unsupported status — that is the [Command
  matrix](/compatibility/command-matrix/). This page is about *test coverage*,
  not the command surface. Cross-link, don't merge.
- The broader testing strategy (Shuttle, Turmoil, Jepsen, fuzz) — that is
  [Testing methodology](/compatibility/testing-methodology/). Link to it.
- Behavioral deltas prose — that is [Overview & differences](/compatibility/overview/).

Sources of truth:
- `website/src/content/docs/compatibility/redis-test-suite.mdx` — the current
  page (rename to slug `compatibility/test-suite`). Its body is already just
  four `<Compat*/>` components + intro/section prose.
- `website/scripts/compat-gen.py` — the generator. **This is where the fixes
  are.** Read it in full.
- `website/src/data/compat-exclusions.json` — the generated output it consumes.
- `frogdb-server/crates/redis-regression/tests/*.rs` — the ground truth the
  generator parses (98 `*_tcl.rs` / `*_regression.rs` files at time of writing;
  each ports one upstream `.tcl`). Do not hardcode this file count anywhere.
- `frogdb-server/crates/redis-regression/src/lib.rs` — crate-level doc
  recording *file-level* out-of-scope upstream `.tcl` files (whole suites that
  will never be ported: AOF, RDB, PSYNC replication, server lifecycle, tool
  integrations). This file-level exclusion list is NOT currently surfaced on
  the page (see "Enhancement" below).
- `website/src/components/CompatSummary.astro`, `CompatCategories.astro`,
  `CompatCommands.astro`, `CompatSuiteDetail.astro` — the four components; keep
  using them.

Required generator fixes (compat-gen.py — these are the substance of the
"update"; each is a real drift risk found in the source):
1. **Hardcoded `upstream_version`.** `compat-gen.py` sets
   `summary.upstream_version = "Redis 8.6.0"` as a string literal (in
   `generate()`), and the crate doc/`redis-test-suite.mdx` frontmatter also say
   "Redis 8.6.0". This must come from S6/`versions.json` (the single declared
   Redis-target value), not a literal in the script. Fix: have compat-gen.py
   read the version from `versions.json` (or accept it as an arg passed by the
   `docs-build` pipeline) and fail loudly if absent. Remove the literal.
   - Also remove the hardcoded "Redis 8.6.0" from the page frontmatter
     `description` and the intro sentence — render it from the JSON summary
     (`upstream_version`), which the components already read.
2. **Manual `FILE_TO_COMMANDS` map.** `compat-gen.py` hardcodes a
   `dict[str, list[str]]` mapping ~60 test files to command names. New/renamed
   test files silently get `[]` (no command impact), so the "Command Impact"
   table drifts as the suite grows. Required fix: either (a) derive the
   command association from a machine-readable source (e.g. a
   `//! commands: XADD, XLEN` doc-comment tag added to each port file, parsed
   the same way the exclusions are), or (b) add a `--check`-enforced assertion
   that every `*.rs` test file present under `tests/` has an entry in
   `FILE_TO_COMMANDS`, so a new unmapped file fails CI instead of silently
   dropping coverage. Prefer (a); (b) is the minimum. Document the chosen
   mechanism in the generator docstring.
3. **`CATEGORY_META` completeness.** The category → (label, description) map is
   also hardcoded. Lower priority, but the generator should fall back visibly
   (it already title-cases unknown codes) — keep, and add a `--check` warning
   when an exclusion uses a category not in `CATEGORY_META` so new categories
   get proper labels.

Existing content (keep, with edits): the page is
intro → `## Summary` (`<CompatSummary/>`) → `## Exclusion Categories`
(`<CompatCategories/>`) → `## Command Impact` (`<CompatCommands/>`) →
`## Suite Details` (`<CompatSuiteDetail/>`) → `## See Also`. Keep this
structure. Edits:
- Frontmatter `title` stays "Redis Test Suite" (or "Test suite results" to
  match PLAN §5 wording — pick one; sidebar label should read "Test suite
  results"). Remove "8.6.0" from `description`; make it version-agnostic.
- Intro: remove the hardcoded "Redis 8.6.0" literal; state the version comes
  from the generated summary. Keep the explanation that ported tests are Rust
  `#[tokio::test]` functions mirroring upstream Redis TCL tests, that every
  ported test runs on every commit, and that exclusions are documented in
  source and generated from it. **Do NOT claim tests run as live/real TCL** —
  the upstream TCL runner (`testing/redis-compat/`) was removed; there are zero
  `.tcl` files in the repo and these are Rust-native ports (confirmed in
  `redis-regression/src/lib.rs`).
- `## See Also`: remove the card to the cut Migration Guide; repoint the
  "Commands" card at `/compatibility/command-matrix/`; add cards for
  Overview & differences and Testing methodology.

Enhancement (optional, if cheap): surface the crate-level *file-level*
exclusions (whole upstream suites never ported — AOF, RDB, PSYNC replication,
server lifecycle, tool integrations) from `redis-regression/src/lib.rs`. These
explain gaps a skeptic will notice ("where are the AOF tests?"). Would require
compat-gen.py to also parse the crate-level `//!` doc for the out-of-scope
sections and emit a `file_exclusions` array, plus a small component or a
`## Out-of-scope suites` section. Mark as a follow-up if it expands scope.

Generated data: `website/src/data/compat-exclusions.json` via
`website/scripts/compat-gen.py` (existing; recipes `compat-gen` /
`compat-gen-check`). New dependency: **S6 `versions.json`** for
`upstream_version`. S8 must ensure `compat-gen-check` runs on `website/**`
changes and that `docs-build` regenerates the JSON (it already runs
`compat-gen`).

Drift guards:
- `compat-gen --check` in CI (exists) fails when the committed JSON differs
  from regenerated output.
- After fix #1, the upstream version is single-sourced through S6 → no
  hardcoded "8.6.0" anywhere in script, JSON, or page prose.
- After fix #2, a new test file cannot silently drop out of the Command Impact
  table (CI fails on an unmapped file, or the mapping is derived from the file
  itself).
- The page carries zero hardcoded counts; every number (ported tests,
  exclusions, broken tests, version) renders from `compat-exclusions.json`.
