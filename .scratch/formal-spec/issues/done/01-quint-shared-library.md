# 01: Extract reusable constructs into a shared Quint library

Status: done

## What to build

Extract the constructs duplicated across `specs/quint/*.qnt` models (local `Option` type +
helpers, spread-preserving ghost-field idiom, node/epoch primitives) into a shared library
that models import, so phase-3 replication models don't triple the duplication.

Layout decision to make first (either is cheap):

- **`specs/quint/lib/` subdirectory** — the spec linter's non-recursive `*.qnt` glob already
  ignores subdirectories, so lib files are exempt from the header-citation requirement with
  zero linter change. Models import via `import cluster_lib.* from "./lib/cluster_lib"`.
- **Flat `lib_*.qnt` naming convention** — keeps the phase-2 plan's flat-file constraint but
  requires a linter edit (skip `lib_*` from the model count + citation requirement) and a
  linter test.

Either way:

- `just quint-check` typechecks each model individually; imported libs are typechecked
  transitively. Add an explicit loop (or a smoke import) so a lib nothing imports still gets
  typechecked.
- Do NOT vendor basicSpells wholesale — extract only what the models actually use.

## Resolution

Shipped as three new flat files under `specs/quint/`, additive — no existing model was
edited (the migration model was under concurrent rework and the admission model was
queued; see the deferred item below).

- `lib_option.qnt` — the `Option`-shaped *reads* the models repeat: `optExists`
  (83 `| None => false` sites in the migration model) and `optForall` (16 `| None => true`
  sites, incl. `identityOrderOk`'s vacuous arm). It hosts helpers, **not the type**:
  `Option[a]` stays declared exactly once in `cluster_common_types.qnt` and is imported,
  because Quint sum-type constructors are nominal and a second declaration would be a
  distinct type whose `Some`/`None` clash on import.
- `lib_monotone.qnt` — the "only ever moves one way" primitives at three types: `max2`
  (watermark advance), `lexGt` (strict lexicographic domination on a
  `(generation, position)` pair — `identityOrderOk`'s ordering conjunct, and the
  replid/offset pairing shape), `latch` (the value-level half of the `defects`/`coverage`
  ghost convention).
- `lib_selftest.qnt` — the executable self-test. The helper modules are `pure def`s with
  no state, so `quint-check` typechecks them but nothing *executes* them, and a `run` test
  in a file no runner opens cannot fail. `scripts/quint-models.sh` lists a file iff it
  declares a `val inv_*`, so the helper laws are stated as the invariants of the smallest
  state machine with the consumers' shape (monotone watermark + `Option`-shaped stored
  pair + refusal ghost). `just quint-run` therefore executes 4 `run` tests and samples
  3 laws on every PR. Four helper mutations were run against it and all four are caught
  (recorded in the file header).

**Layout ruling: flat `lib_*.qnt`, and no linter edit.** The `lib/` subdirectory option
was rejected — `specs/quint/` files stay flat (phase-2 plan tripwire 2: the lint and
`quint-check` globs are non-recursive). The flat option's assumed linter edit turned out
to be unnecessary: tripwire 3's resolution is that helper modules cite the rows they
*support*, and both libs do (via their consumers — TR-CLUSTER-010/018/019,
TR-REPLICATION-013/015/018/019/020/033), so `check_quint_citations` passes unmodified.
The lint's "models" number was already a `.qnt` file count rather than a runnable-model
count (`cluster_common_types.qnt` and the `_logic`/`_machine`/`_types` satellites have
always been in it), so nothing about it changed meaning.

The helper modules declare no `val inv_*`, so `scripts/quint-models.sh` does not list
them and `just quint-run`/`just quint-verify-*` never try to run a module with no
`init`/`step`. `just quint-check` globs every `.qnt`, so both are typechecked even though
nothing imports them yet — the "lib nothing imports still gets typechecked" ask needed no
recipe change.

The bar for hosting a helper was **two real consumers, existing or imminent**. Left out
deliberately, each recorded at the point it would have gone: `optGetOr` (one existing
consumer), `optMap`/`optFilter`/`optIsSome`/`optIsNone` (none — models compare against
`None` directly), `min2`/`lexGe`/`clamp`/`abs` (none), a record-level ghost-update helper
(Quint cannot parameterize over a field name, so `latch` is the whole extractable half),
and the `| None => nope` arms (they dispatch to a *disabled action*, which cannot be a
`pure def` at all).

**Deferred, tracked as issue 04:** retrofitting `cluster_admission.qnt` and
`cluster_migration_failover.qnt` onto the library (this issue's "duplicated code deleted"
+ "re-run both models' full evidence set" criteria). Both models were being edited
concurrently by the issue-31 Quint rework when this landed, so the retrofit was carved
out rather than raced. Phase-3 replication models consume the library first.
