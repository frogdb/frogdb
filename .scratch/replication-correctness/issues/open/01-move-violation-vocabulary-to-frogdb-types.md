# 01 — Move `Violation`/`Citation`/`Tier` to `frogdb-types`

Status: needs-triage

## Parent

[PRD](../../PRD.md) §8 D6.

## What to build

A mechanical move of the three catalog vocabulary types out of
`frogdb-server/crates/cluster/src/invariants.rs:80-148` and into `frogdb-types`
(`frogdb-server/crates/types`), which both `frogdb-cluster` and `frogdb-replication` already
depend on. Re-export them from the cluster catalog and keep `frogdb_core::Violation`
(`frogdb-core/src/lib.rs:63`) resolving at its current path, so `DEBUG CLUSTER CHECK`, its
response formatter, the Jepsen path and the scheduler's violation rendering do not move at all.

The point of the move is stated in D6: `frogdb-replication` does **not** depend on
`frogdb-cluster` and must not start, and the rejected alternative — a duplicate three-line
`Violation` in `frogdb-replication` — would force the Jepsen checker, the DEBUG formatter and
the scheduler's rendering to carry two code paths for one concept. This is not the shared
*framework* the cluster campaign ruled out; it is one 4-field struct and a `const fn` assert,
and keeping it single is what makes the per-area catalogs report in one voice.

The compile-time discipline has to survive the move intact: `Tier::DocumentedException(Citation)`
keeps the citation as a **variant field**, and `Citation::failure_mode` / `Citation::issue` stay
`const fn`s that `assert!` a non-empty reference, so inside a `static CATALOG` a citation-less or
blank-citation exception remains a build error rather than a review comment. Two tiers, no third.

`frogdb-cluster` is a LOCKED area (gate 0.80): no behavior changes here, but run
`just mutants-diff frogdb-cluster` and `just lint-failure-modes` before pushing.

## Acceptance criteria

- [ ] `Violation`, `Citation` and `Tier` are defined exactly once, in `frogdb-types`; no copy
      exists in any other crate
- [ ] `frogdb_core::Violation` still resolves at its current path; `DEBUG CLUSTER CHECK`, its
      formatter, the Jepsen checker and the scheduler's violation rendering are unchanged
- [ ] `frogdb-replication` can name all three types without gaining a dependency on
      `frogdb-cluster` (verified against the dependency graph, not by inspection)
- [ ] The citation asserts still fire at compile time — a blank `Citation` inside a `static`
      fails the build, demonstrated by a compile-fail test or a recorded manual check
- [ ] `just check` (workspace), `just test frogdb-cluster` and `just lint-failure-modes` green
      (catalog id vocabulary unchanged)
- [ ] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

None — can start immediately.
