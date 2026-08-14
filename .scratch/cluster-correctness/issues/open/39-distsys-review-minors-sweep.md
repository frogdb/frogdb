# 39: Distsys-review minors sweep (cluster)

Status: ready-for-agent

## Origin

Minor findings from the independent distsys review
(`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`), ruled one at a
time by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).
One sweep issue per area tracker; each checklist entry is a ruled minor with its
resolution pinned. Cluster crates locked, gate 0.80 — spec-row edits run
`just lint-spec`; any code change gets `just mutants-diff` triage.

## Ruled minors

### MIN-1 — FM-CLUSTER-037's "bounded by Raft apply latency" is unsupported

Ruling: **weaken phrasing**. Neither cited test measures the interval, and VLL
queueing, blocking-command parking, and pause delays sit between the snapshot read
and command execution. Restate the row honestly: staleness is apply latency *plus*
execution-pipeline queueing (VLL queue, parked blocking commands, pause holds) —
eventual, not bounded; no load-measuring timing test (flake risk, ruled out).
Spec-only edit.

- [ ] FM-CLUSTER-037 restated; `just lint-spec` green

## Acceptance criteria

- [ ] Every checklist entry above resolved as ruled
- [ ] `just lint-spec` green after all spec edits
- [ ] `just mutants-diff` triaged for any code-touching entry

## Blocked by

None — can start immediately.
