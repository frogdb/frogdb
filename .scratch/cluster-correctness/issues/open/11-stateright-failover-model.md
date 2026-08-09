# 11 — Stateright model 2: failover composite

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W3.

## What to build

Second stateright model on the issue-10 infrastructure: detector verdicts +
`Failover { force }` racing `MarkNodeFailed`/`MarkNodeRecovered` and concurrent proposals
from two would-be leaders.

Safety: at most one Primary per slot after quiesce; epoch strictly grows across every
promotion. Same smoke/nightly split and budget-recording discipline as issue 10.

## Acceptance criteria

- [ ] Both safety properties checked; state-space size recorded in the model header
- [ ] Smoke config in default suite; full budget nightly
- [ ] Counterexamples checked in as regression scenarios
- [ ] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

- Issue 10 (`.scratch/cluster-correctness/issues/`) — shares the model infrastructure.
