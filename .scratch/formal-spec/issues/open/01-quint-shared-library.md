# 01: Extract reusable constructs into a shared Quint library

Status: ready-for-agent

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
- Migrate `cluster_admission.qnt` and `cluster_migration_failover.qnt` onto the library;
  re-run their full evidence set (quint test, quint run, mutation regression set from the
  task reports, `just quint-check`, `just lint-spec`) to prove the refactor is
  behavior-preserving.
- Do NOT vendor basicSpells wholesale — extract only what the models actually use.

## Acceptance criteria

- [ ] Shared library exists; both cluster models import it; duplicated `Option`/helper code deleted
- [ ] `just quint-check` covers the library file(s) even when unimported
- [ ] `just lint-spec` green with model count still reflecting real models only
- [ ] Both models' quint tests + invariant runs + mutation regression sets still pass, evidence recorded

## Blocked by

None - can start immediately. (Do after phase-2 Tasks 2–5 land to avoid churn under an
in-flight SDD loop; before phase-3 replication models are written.)
