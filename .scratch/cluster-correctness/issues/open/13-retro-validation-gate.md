# 13 — Retro-validation gate: the machine catches what the audit caught

Status: needs-triage

## Parent

[PRD](../../PRD.md) §5 sequencing gate + §6 exit criterion 8.

## What to build

In a scratch branch, revert each of the five 2026-08-08 audit fixes one at a time
(FM-CLUSTER-098 vote durability, 099 log-reader cache, 100 handoff_seq restore, 101
voter removal, 102 detector clamp) and run the layers. Record, per defect, which layers
flag it (L1 hooks / P1–P4 / stateright / seeded schedules / Jepsen checker). Every one
of the five must be caught by at least one layer — this is the PRD's falsifiable exit
claim. A defect no layer catches is a gap issue filed against the responsible
workstream, and the PRD does not exit until it closes.

Deliverable: a results table appended to the PRD (§6) with the evidence.

## Acceptance criteria

- [ ] All five reverts attempted, each against the full layer stack
- [ ] 5/5 caught by ≥1 layer, with the catching layer(s) named
- [ ] Any miss filed as a gap issue and closed before PRD exit
- [ ] Results table in the PRD

## Blocked by

- Issues 02, 03, 09, 10 (`.scratch/cluster-correctness/issues/`) — needs the layers it
  measures (04, 06, 07 strengthen the count but the gate can run once these four exist).
