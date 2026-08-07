# Decision D3 — when to fix the coverage tooling, relative to the testing work

Status: ready-for-human
Type: decision
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §7 D3 · MASTER.md §1 "Data-quality caveat"
Area: tooling / CI

## Context

Two defects in the coverage pipeline itself were found because agents distrusted their inputs, and
both were confirmed by direct measurement:

| artifact | defect | measured |
|---|---|---|
| `target/llvm-cov/lcov.info` | contains essentially no test-execution data; only `config-derive`, a build-time proc macro, has nonzero counts | FNDA 29 nonzero / 34 644 (0.1%); DA 323 / 128 130 (0.3%) |
| `target/llvm-cov/depth/depth.json` | `class_counts` computed over duplicate function records (monomorphisations + `::<_>` generic placeholders, one copy zeroed) | `untested` 14 849 raw → 7 008 name-deduped → **2 163 span-deduped** |

`coverage-nightly.yml` consumes `just coverage-lcov`, so the CI coverage number is meaningless; and
the depth report's claim that its de-duplicated figure "matches `llvm-cov export --format=lcov`
exactly" is false. Both are filed as their own work items — issues 27 and 28,
`.scratch/testing-improvements-round2/issues/`. What is *not* settled is when they get done
relative to the ~249 findings this audit produced.

The audit's own position on the data: the per-file `line_counts` in `depth.json` are sound; the
strongest findings are anchored on those or on read source, not on depth classes; and every finding
whose sole evidence was a depth class was re-checked against span-deduped data before being
reported, all of which survived. So the findings do not depend on this being fixed first.

## Options

**(a) Fix the tooling before starting the testing work.** *Consequence:* every subsequent
prioritisation, per-crate targeting and "did this help?" measurement runs on real data, and the
nightly job stops publishing a fabricated number immediately. Cost is a delay of unknown size at
the front — issue 27 requires root-causing why `cargo llvm-cov nextest --all --lcov` emits zero
counts, which has not been diagnosed yet. It also puts tooling work ahead of ~40 suspected
data-loss defects.

**(b) Fix it after the testing work.** *Consequence:* the defect and gap work starts immediately
and is unaffected, since the audit already re-verified its evidence. But the nightly job keeps
posting a meaningless percentage against a baseline for the whole period, nobody can measure
whether the testing work moved coverage at all, and the next person to quote a coverage number from
this repo will quote a wrong one.

**(c) Fix it in parallel, as an independent track.** *Consequence:* neither blocks the other, and
it matches how the audit filed them ("independent of this audit"). Costs a second concurrent
workstream and an owner who is not also on the findings.

**(d) Disable the misleading output now, fix properly later.** *Consequence:* the cheapest way to
stop the harm — remove or gate the coverage-summary step in `coverage-nightly.yml` so no number is
published — while leaving the root cause for later. Cost: the repo has no coverage signal at all in
the interim, and a disabled check tends to stay disabled.

## Recommendation

None. MASTER.md §7 records no ordering recommendation for D3. It states two things that any choice
must respect:

- *"Both should be filed as their own issues, independent of this audit."* — done, as issues 27 and
  28, `.scratch/testing-improvements-round2/issues/`.
- *"Until fixed, no coverage number from this repo should be quoted."* — this holds under every
  option above, and under (b) or (c) it needs to be communicated, not just recorded here.

## Depends on

Nothing blocks this decision. It sequences issues 27 and 28,
`.scratch/testing-improvements-round2/issues/`, against the rest of the round-2 backlog. Related:
issue 32, same directory, is the equivalent sequencing decision for the shared test infrastructure
(issues 01–18, same directory).

## Re-triage 2026-08-06

**Verdict: still-valid**

Nothing was decided and nothing was fixed. Issues 27 and 28 are both still in `issues/open/`; `git
log --since=2026-07-27 -- scripts/coverage-depth.py .github/workflows/coverage-nightly.yml` shows
no fix commit, and `just coverage-lcov` (`Justfile:85`) is unchanged.
`.github/workflows/coverage-nightly.yml` still runs `just coverage-lcov` (`:79`) and still writes a
"Coverage summary" against the **84.0%** baseline (`:80-101`) — and its `schedule:` cron was
*re-armed* at campaign exit (`45591265`, after `dd2f6704` had made every workflow manual-only), so
the fabricated number is being published again nightly. Events narrowed the question rather than
answering it: the hardening campaign ran to completion on mutation scores, never on line coverage,
so option (a) ("fix the tooling before starting the testing work") is now moot — the live choice is
(b)/(c)/(d). One new datum for whoever takes the call: issue 94's headline evidence
(`ShardSearchIndex::open_in_ram` "untested at 0/41 regions") is a **false negative produced by this
very defect** — the function had 15 in-crate callers at audit-filing commit `a0e85aac` and has ~20
today (`search/src/index.rs:1447+`, `spellcheck.rs:161+`). Stays `ready-for-human`.
