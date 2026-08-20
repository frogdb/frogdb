# 04: Retrofit the cluster models onto the shared Quint library

Status: needs-triage

## What to build

Issue 01 landed `specs/quint/lib_option.qnt` + `specs/quint/lib_monotone.qnt` additively —
no existing model was edited, because `cluster_admission*.qnt` and
`cluster_migration_failover*.qnt` were under concurrent rework (the issue-31 campaign) at
the time. The library's helpers therefore have zero *existing* callers: the duplication
they were extracted from is still written out longhand in both models.

Retrofit both models onto the library:

- `cluster_migration_failover_logic.qnt` / `_machine.qnt` — the ~83 `match o { Some(v) =>
  P(v) | None => false }` sites become `optExists(o, v => P(v))`, the ~16 `| None => true`
  sites become `optForall` (`identityOrderOk` is the archetype), `max2` moves out of
  `_logic` and is imported from `lib_monotone`, `identityOrderOk`'s ordering conjunct
  becomes `lexGt((inc, seq), (st.inc, st.seq))`, and the `defects`/`coverage` ghost
  updates go through `latch`.
- `cluster_admission*.qnt` — no `match` sites (it compares against `None` directly), so
  its share is small; check whether it has anything to move at all before editing it, and
  say so in the report if it does not.

Do it as one model at a time, after the issue-31 rework settles — never against a model
another agent is mid-edit on.

## Why it matters

Two consumers is what keeps a helper honest. Until the models call them, `lib_option`'s
and `lib_monotone`'s definitions are pinned only by `lib_selftest.qnt`, and the longhand
in the models is free to drift from the helper it was extracted from (e.g. an `| None =>`
arm flipped in one site out of 83).

## Acceptance criteria

- Both cluster models import the library; the duplicated longhand is gone
- Behavior-preserving, proven per model: `quint test`, the model's `quint run --invariants`
  sweep, and its mutation regression set from the phase-2 task reports all reproduce their
  pre-retrofit results
- `just quint-check`, `just quint-run` and `just lint-spec` green

## Blocked by

The issue-31 Quint rework settling on `specs/quint/cluster_migration_failover*.qnt` and
`cluster_admission*.qnt` — this issue edits exactly those files.
