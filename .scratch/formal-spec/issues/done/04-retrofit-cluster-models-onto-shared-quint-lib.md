# 04: Retrofit the cluster models onto the shared Quint library

Status: done

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

- Both cluster models import the library; the duplicated longhand is gone — **met**.
  Migration family (`7c0b13e8`): 91 `optExists`/`optForall` sites, 52 `latch` sites,
  `max2` deleted from `_logic` in favour of `lib_monotone`'s, `identityOrderOk`'s
  ordering conjunct now `lexGt((inc, seq), (st.inc, st.seq))`. Admission family
  (`3cee0668`): 1 `optForall` + 2 `latch`.
- Behavior-preserving, proven per model — **met**. Per model, before and after at the
  same seeds: identical `quint test` pass set (migration 87/87), identical invariant
  verdicts at 200x20 and 4000x40 for seeds 0x1/0x2/0x3 (every one `[ok] No violation
  found`), and **byte-identical** witness counts at the gate tier (2000x40, seeds
  0x1/0x2). The sampled walk was first confirmed deterministic per seed by replaying
  the baseline lane, so identity — not "within noise" — is the bar that was met.
  Mutation regression set: migration rows R1-2, R1-3, R8-1, R9b-1, R10-1 all still
  CAUGHT; admission rows A04, A08, A26 (the three forcing `inv_configured_id_stable`)
  still die both by test and by the invariant on the sampled walk. Every mutation was
  applied to the real file and the restore verified byte-identical.
- `just quint-check`, `just quint-run` and `just lint-spec` green — **met**, plus
  `just quint-run-steered` at 0 red / 160 cells (the steered lane exercises
  `stepSteered` in the migration family, so it is the sharper of the two walks here).

## Sites deliberately left longhand

Not every `match` is one of the two helpers, and rewriting those would change meaning:

- **`identityOrderOk`'s own `match`.** Since ruling R2 its absent-operand arm is
  *kind-dependent* (`kind != Demotion`) — neither `optExists`' conservative `false` nor
  `optForall`' vacuous `true`. Only its `Some` arm moved (to `lexGt`). The reason is
  recorded at the code.
- **Accumulator-valued folds** (`| None => acc`): `prunedMigrations`,
  `cancelledBySource`, `migrationSourcesOf`, `fencedOwners`, `markResidueOnDeparture`,
  `retargetResidueOnDemotion`.
- **Non-`bool` results**: `shardPrimary` (Option), `reconcileKindOf` (TransitionKind),
  `bootMintOf`, `applyPrepareHandoff`, `applyConfirmDrained`, `residueFollowsDemotion`
  (record/map), and the machine's `postNodes` / `postResidue` / `refusals'` / `disp`.
- **Both arms distinct expressions**: `identityFactsDiffer`.
- **The 22 action-valued `| None => nope` arms** in the machine — an action is not a
  `pure def` argument, which `lib_option`'s header already anticipated.
- **`configuredIdFor` / `idForBoot`** in `cluster_admission_logic.qnt`: `optMap` /
  `optGetOr` shapes, which `lib_option` deliberately does not host.
