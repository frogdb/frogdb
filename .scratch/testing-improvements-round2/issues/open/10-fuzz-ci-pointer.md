# Fuzz CI — tracked at issue 40 in `.scratch/testing-improvements/issues/`, not here

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I10
LOE: 2–4 days (estimated)
Tier: C
Area: CI / fuzzing (`testing/fuzz`, workflow-gen)
Asked by: 08 (item 3), 13

## Context

This is a pointer, not a work item. Round 2 rediscovered that fuzzing is not running and
listed it as infrastructure item I10, but the work is already tracked — and already
reopened with round-2 evidence — as **issue 40,
`.scratch/testing-improvements/issues/`**. That issue carries the full acceptance criteria,
including the three reopen criteria added on 2026-07-28. Do the work there; this file exists
so an I-number reader is not left believing I10 is unfiled.

Round-2 residue: the item was raised independently by two areas (08 and 13) whose own
findings depend on fuzzing actually executing, so it appears in `INFRASTRUCTURE.md` as tier C
alongside the genuinely new items. `INFRASTRUCTURE.md` I10 and
`.scratch/testing-improvements-round2/README.md` both say the same thing: do not file a
duplicate.

## Evidence

- **Current state**: **fuzzing is not running.** `fuzz.py` shows the nightly cron was
  deliberately removed, and the PR `corpus-replay` gate is `-runs=0` restore-only, so it
  silently no-ops on a cold cache. This affects all **34** targets.
- **Highest-value targets** (13): `deserialize`, each per-type decoder, `RESTORE` payloads.

## Options

> **Decision needed** (08's framing): a weekly campaign, a per-PR time-boxed run for a
> security-critical subset, or accept manual dispatch **and remove the "continuous" framing
> from the docs**. The third option is legitimate; the current state — docs claiming
> continuous fuzzing that does not run — is not.

## What to build

Nothing here. Record 08's framing above and 13's target ranking on issue 40,
`.scratch/testing-improvements/issues/`, and close this file when that issue closes.

## Acceptance criteria

- [ ] 08's decision framing and 13's highest-value target list are recorded on issue 40,
      `.scratch/testing-improvements/issues/`.
- [ ] No duplicate fuzz-CI issue exists in this directory.
- [ ] This file is moved to `done/` when issue 40 closes.

## Test boundary

Level 1 — the fuzz targets are in-process harnesses over parsers and decoders; the gap is CI
scheduling and corpus persistence, not test level.

## Depends on

Issue 40, `.scratch/testing-improvements/issues/` — the tracking issue for this work.

## Re-triage 2026-08-06

**Verdict: still-valid**

The pointer stays open because its target stays open: issue 40 is still the only file in
`.scratch/testing-improvements/issues/open/`. Campaign-exit CI restoration (`0a0881a1`,
`45591265`) discharged the **first** of issue 40's three reopen criteria — the nightly cron is
back in the DSL (`workflow_gen/src/workflow_gen/workflows/fuzz.py:76` `NIGHTLY_CRON = "41 2 * *
*"`, plus a `pull_request` trigger on `corpus-replay`) and regenerated into
`.github/workflows/fuzz.yml:12-22`, so `fuzz-campaign` writes the `fuzz-corpus-` cache again and
the PR replay gate has something to restore. The other two are **not** done: `corpus-replay`
still degrades silently on a cold corpus (`echo "$target: no persisted corpus, skipping"`,
`fuzz.py:_replay_run_step`, no warning annotation and no all-empty failure), and there is no
workflow-gen test suite at all (`.github/workflows/workflow_gen/` contains only `src/` — nothing
asserts the expected trigger set per workflow, so the next hosted-runner sweep can drop a cron
again). This file's own criterion 1 is also unmet: neither 08's decision framing nor 13's
highest-value target ranking has been recorded on issue 40.
