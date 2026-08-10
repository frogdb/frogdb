# 01 — Move `Violation`/`Citation`/`Tier` to `frogdb-types`

Status: done

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

- [x] `Violation`, `Citation` and `Tier` are defined exactly once, in `frogdb-types`; no copy
      exists in any other crate
- [x] `frogdb_core::Violation` still resolves at its current path; `DEBUG CLUSTER CHECK`, its
      formatter, the Jepsen checker and the scheduler's violation rendering are unchanged
- [x] `frogdb-replication` can name all three types without gaining a dependency on
      `frogdb-cluster` (verified against the dependency graph, not by inspection)
- [x] The citation asserts still fire at compile time — a blank `Citation` inside a `static`
      fails the build, demonstrated by a compile-fail test or a recorded manual check
- [x] `just check` (workspace), `just test frogdb-cluster` and `just lint-failure-modes` green
      (catalog id vocabulary unchanged)
- [x] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

None — can start immediately.

## Resolution (2026-08-10)

Moved verbatim. New file `frogdb-server/crates/types/src/catalog.rs` holds `Violation`,
`Citation` and `Tier`, unchanged field-for-field and behavior-for-behavior — including
`Citation::failure_mode`/`Citation::issue` staying `const fn`s that `assert!` a non-empty
reference, and `Tier::DocumentedException(Citation)` keeping the citation as a variant field.
`Violation::new` was widened from private to `pub` (mechanically required: the constructor now
lives in a different crate from the `check_*` functions that call it in `frogdb-cluster`); it
was already effectively unconstrained since `id`/`detail` are both public fields, so this is not
a new capability, just a formality.

**Re-export chain.** `frogdb-types/src/lib.rs` adds `pub mod catalog;` and
`pub use catalog::{Citation, Tier, Violation};`. `frogdb-cluster/src/invariants.rs` replaces the
struct/impl/enum definitions with `pub use frogdb_types::{Citation, Tier, Violation};`, so
`frogdb-cluster/src/lib.rs`'s existing `pub use invariants::{Citation, Invariant, Tier,
Violation};` and `frogdb-core/src/lib.rs`'s existing `pub use cluster::{... Violation ...}`
resolve unchanged — neither file needed an edit. `frogdb_core::Violation` is now reachable via
two paths that name the identical type (also through `frogdb-core`'s existing
`pub use frogdb_types::*;`), which is not a conflict since both resolve to
`frogdb_types::catalog::Violation`.

**New dependency.** `frogdb-cluster/Cargo.toml` gained `frogdb-types.workspace = true` (it had no
prior dependency on the types crate). `Cargo.lock` updated accordingly.

**Dependency-graph verification** (not by inspection, per the acceptance criterion):
`cargo tree -p frogdb-replication -i frogdb-cluster` reports `error: package ID specification
'frogdb-cluster' did not match any packages` — `frogdb-cluster` does not appear anywhere in
`frogdb-replication`'s dependency graph, direct or transitive. `cargo tree -p frogdb-replication
-i frogdb-types` confirms `frogdb-replication` already reaches `frogdb-types` both directly and
via `frogdb-persistence`, so it can name `frogdb_types::{Violation, Citation, Tier}` today with no
manifest change.

**Compile-time discipline, manually verified.** Temporarily added
`const _PROBE: Citation = Citation::issue("");` to `catalog.rs` and ran `just check frogdb-types`:

```
error[E0080]: evaluation panicked: a DOCUMENTED-EXCEPTION must cite an issue
   --> frogdb-server/crates/types/src/catalog.rs:146:51
```

confirming the `const fn` assert still fires at compile time from its new home. The probe was
reverted; the module's test section records the check inline as a comment (the crate has no
trybuild harness for a permanent compile-fail test).

**Verification results:**
- `just check frogdb-types`, `just check frogdb-cluster`, `just check frogdb-core` — all green
  during iteration.
- `just check` (full workspace) — green, `Finished ... in 56.79s`.
- `just test frogdb-cluster` — 294/294 passed, 5 skipped (unchanged from before the move).
- `just test frogdb-types` — 379/379 passed (376 pre-existing + 3 new unit tests added for the
  moved `Citation`/`Violation`/`Tier` behavior, since that coverage now belongs with the type's
  defining crate).
- `just lint-failure-modes` — `OK: 279 failure modes (BLOCKING, CLUSTER, PERSISTENCE,
  REPLICATION, TXN, VLL), 1401 test references, 1401 tags, 41 invariant citations over 11
  catalog entries` — catalog id vocabulary unchanged.
- `just mutants-diff frogdb-cluster` — `INFO No mutants to filter`: the crate's diff is a
  deletion plus one `use` re-export line, so there is no new mutatable logic in `frogdb-cluster`
  to test. Trivially green, as expected for a pure move.
- `cargo clippy -p frogdb-cluster [--tests] -- -D warnings` and
  `cargo clippy -p frogdb-types [--tests] -- -D warnings` — both clean. The scoped `just lint
  frogdb-cluster` recipe itself fails, but on a pre-existing, unrelated warning
  (`redundant_closure` in `frogdb-server/crates/persistence/src/snapshot/mod.rs:140`, from commit
  `c62da703` already on the branch's base) reached because the recipe's `lint-turmoil`
  prerequisite always clippies all of `frogdb-server` with the `turmoil` feature regardless of
  the `crate` argument — it is not scoped the way the final `cargo clippy -p <crate>` step is.
  Confirmed pre-existing via `git merge-base --is-ancestor c62da703... HEAD`. Not touched here;
  out of scope for this move.

Branch: `worktree-agent-a3f93adcb5a2b2b3a`.
