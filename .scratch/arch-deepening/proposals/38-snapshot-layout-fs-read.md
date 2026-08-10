# Proposal 38 — `SnapshotLayout` owner type, and the `SnapshotFs` seam applied to the read half

## Summary

The on-disk snapshot layout — `snapshot_NNNNN/`, `.snapshot_NNNNN.tmp`, `latest`,
`metadata.json`, `checkpoint/` — has no owner. Its names are re-authored as string
literals at four production construction sites and two production parse sites, plus
twenty-one more in `frogdb-persistence`'s own tests and six across `frogdb-core` and
`frogdb-server`. The knowledge "epoch 7 is the directory `snapshot_00007`, staged at
`.snapshot_00007.tmp`, pointed at by `latest`" lives in every caller's head rather than
in a module.

ADR-0003 already made the *write* half of that layout addressable: publication goes
through the `SnapshotFs` seam so the fsync/rename ordering is a testable sequence rather
than a claim about syscalls. The **read** half was never routed. `SnapshotFs` has five
methods and every one of them mutates (`write`, `rename`, `symlink`, `sync_file`,
`sync_dir`); the boot-time reader calls `std::fs::read_link`, `std::fs::read_to_string`,
`std::fs::read_dir` and `Path::exists` directly
(`rocks_coordinator.rs:113,133,136,142,145`), as does retention
(`stager.rs:224`). The ADR's own stated cost — "a new publication path must be routed
through the trait or it is untested by construction" — has a mirror image the ADR does
not name: the read path is untested by construction for every failure that is not
"absent" or "garbage".

This proposal introduces **`SnapshotLayout { root }`** as the single owner of the layout
(the same shape `StagedCheckpoint` already has for the `checkpoint_ready` install
protocol one directory over), and extends `SnapshotFs` with read operations so the
readers cross the seam the writers already cross. No behavior changes; no failure-mode
row changes.

## Files involved

| Path | Approx. lines involved | Role |
|------|------|------|
| `frogdb-server/crates/persistence/src/snapshot/rocks_coordinator.rs` (348) | 107–152, 250–266, 283 | The whole read half (`highest_snapshot_epoch`, `load_latest_metadata`) + two of the four name-construction sites |
| `frogdb-server/crates/persistence/src/snapshot/stager.rs` (347) | 61–84, 133–138, 148–156, 170–185, 193–197, 216–248, 261–283 | Four layout-derived fields on the stager; `checkpoint`/`metadata.json`/`latest` literals; the retention parser |
| `frogdb-server/crates/persistence/src/fs_seam.rs` (238) | 32–45, 50–93, 103–187 | `SnapshotFs` trait, `RealFs`, `RecordingFs` — gains read operations |
| `frogdb-server/crates/persistence/src/snapshot/mod.rs` (250) | 1–16 | Module declarations + re-exports; new `layout` module |
| `frogdb-server/crates/persistence/src/snapshot/tests.rs` (1456) | 183–210, 231–266, 280–330, 480–605, 935–1140 | 21 hand-authored layout sites, incl. the duplicated stager construction and 10 `#[cfg(unix)]` gates |
| `frogdb-server/crates/persistence/src/rocks/staged.rs` (unchanged) | 1–110, 136–162 | **Precedent only** — the same extraction, already done for `checkpoint_ready` |
| `frogdb-server/crates/core/src/persistence/test_harness.rs` | 340–380 | `create_test_snapshot_dir` / `write_snapshot_metadata` / `create_latest_symlink` re-author the layout |
| `frogdb-server/crates/core/src/persistence/crash_recovery_tests.rs` | 934, 975, 1321, 1408 | `create_latest_symlink(&snapshot_dir, "snapshot_00001")` — the name as a bare literal |
| `frogdb-server/crates/server/src/connection/persistence_conn_command.rs` | 541–544 (`#[cfg(test)]`) | Sabotage test plants a file at a hand-built `.snapshot_{epoch:05}.tmp` |
| `frogdb-server/crates/server/tests/integration_persistence.rs` | 1602–1625 | `wait_for_snapshot_checkpoint` re-implements the epoch scan, the `.tmp` exclusion, and the completeness check |
| `website/src/content/docs/operations/backup-restore.md` | 22–29, 77 | The layout as an operator-facing contract (`snapshot_NNNNN/`, `latest`, `snapshot_00042`) |

## Problem

### 1. Four production sites author the same name; one of them is a pure re-derivation

`RocksSnapshotCoordinator::execute` builds the stager's path trio inline
(`rocks_coordinator.rs:255–266`):

```rust
SnapshotStager {
    tmp: snapshot_dir.join(format!(".snapshot_{epoch:05}.tmp")),
    final_dir: snapshot_dir.join(format!("snapshot_{epoch:05}")),
    name: format!("snapshot_{epoch:05}"),
    snapshot_dir,
    ...
}
```

and then, 17 lines later, `record` re-derives the same directory a second time purely to
log it (`rocks_coordinator.rs:283`):

```rust
let path = self.snapshot_dir.join(format!("snapshot_{epoch:05}"));
```

That second derivation is correct today only because both sites happen to spell the
format string identically. It is not read back from the stager, which is the one
component that actually knows where it installed the artifact.

The reverse direction is authored twice more, in two crates' worth of scanners that must
agree with the constructors and with each other:

- `rocks_coordinator.rs:119–124` — boot epoch seed: `strip_prefix("snapshot_")` + `parse::<u64>()`.
- `stager.rs:228–231` — retention: `starts_with("snapshot_")` + `strip_prefix` + `parse::<u64>()`.

Both silently depend on the leading dot in `.snapshot_NNNNN.tmp` to exclude staging
directories, and on `file_type().is_dir()` (which does not follow symlinks) to exclude
`latest`. Neither of those exclusions is stated anywhere; each scanner re-discovers them.

### 2. The test helper is a verbatim second author

`snapshot/tests.rs:249–266` reproduces the coordinator's construction exactly:

```rust
SnapshotStager {
    tmp: snapshot_dir.join(format!(".snapshot_{epoch:05}.tmp")),
    final_dir: snapshot_dir.join(format!("snapshot_{epoch:05}")),
    name: format!("snapshot_{epoch:05}"),
    ...
}
```

Its doc comment — "Build a stager with the standard `<snapshot_dir>` path layout" —
states the problem plainly: there is a standard layout, and the test has to know it.
Every stager test therefore validates the *test helper's* idea of the layout, not the
coordinator's. A change to the production format string does not fail a single test in
`stager.rs`'s own suite.

### 3. `SnapshotStager`'s interface leaks four derivable fields

Of the stager's nine fields (`stager.rs:61–84`), four — `snapshot_dir`, `tmp`,
`final_dir`, `name` — are functions of `(root, epoch)`, and `epoch` is a fifth field
sitting right next to them. The interface obliges every constructor to compute the four
consistently; nothing checks that it did. A caller that passed a `tmp` and a `final_dir`
from different epochs would compile, run, and publish one epoch's staging directory under
another epoch's name.

### 4. The read half never crosses the seam ADR-0003 built

`SnapshotFs` (`fs_seam.rs:32–45`) is mutation-only. Consequently the reader is reachable
only through the real filesystem:

| Read | Site | Reached via |
|------|------|-------------|
| Does `latest` exist? | `rocks_coordinator.rs:133` | `Path::exists` |
| What does `latest` point at? | `rocks_coordinator.rs:136` | `std::fs::read_link` |
| Slurp `metadata.json` | `rocks_coordinator.rs:142,145` | `Path::exists` + `read_to_string` |
| Enumerate `snapshot_NNNNN` for the boot seed | `rocks_coordinator.rs:113` | `std::fs::read_dir` |
| Enumerate `snapshot_NNNNN` for retention | `stager.rs:224` | `std::fs::read_dir` |

Three consequences, all verified against the current tree:

- **Only two of the three unreadable shapes are testable.**
  `coordinator_boot_keeps_the_epoch_when_latest_metadata_is_unreadable`
  (`tests.rs:1013`) loops over exactly `("garbage", Some("{not json"))` and
  `("absent", None)`. FM-PERSISTENCE-017's invariant claims the seed survives an
  `latest`/`metadata.json` that is "absent, zero-length, garbage, whatever a power loss
  left behind" — but an `EIO` from `read_link`, or a `read_to_string` that fails
  mid-file, cannot be produced without syscall interposition. The one shape a *disk*
  failure actually produces is the one the suite cannot force.
- **Six boot tests open a real RocksDB to exercise code that touches no database.**
  `load_latest_metadata` is a private associated function and `highest_snapshot_epoch` is
  `pub(crate)` but has no direct caller in tests; both are reached only through
  `RocksSnapshotCoordinator::new`, which requires an `Arc<RocksStore>`. Every boot-seed
  test therefore calls `make_store` (`tests.rs:225–229`: a real `RocksStore::open` plus a
  `put`) purely to get a coordinator. The interface is the test surface, and here the
  test surface is a database.
- **Ten `#[cfg(unix)]` gates in `snapshot/tests.rs`** (lines 301, 416, 598, 960, 1011,
  1045, 1077, 1127, 1190, 1386) exist because the tests must create and read real
  symlinks. The `#[cfg(not(unix))]` arms in `RealFs::symlink` (`fs_seam.rs:70`) and
  `update_latest_symlink` (`stager.rs:276`) are unreachable by every test on every
  platform the project builds for (`Justfile:731–736` targets
  `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu` only).

### 5. Writer and reader already disagree about `latest`, in the arm nobody can test

On non-unix, `update_latest_symlink` writes `latest` as a **plain file containing the
target name** (`stager.rs:276–280`), matching `RealFs::symlink`'s documented fallback.
`load_latest_metadata` reads it with an unconditional `std::fs::read_link`
(`rocks_coordinator.rs:136`) and has no plain-file fallback — so on that platform every
boot would take the error arm, log "Failed to read the latest snapshot metadata", and
seed `last_save_time` as `None` forever.

**This is not a live bug**: no shipped target is non-unix, so the code is unreachable. It
is offered as evidence of the failure mode an unowned layout produces — the two halves of
one on-disk contract, written 130 lines apart, already disagree, and no test on any
supported platform can say so.

### 6. The layout is a cross-crate and operator-facing contract with no single statement of it

`frogdb-core`'s crash-recovery harness authors it (`test_harness.rs:342,363,370` and four
`create_latest_symlink(&snapshot_dir, "snapshot_00001")` call sites in
`crash_recovery_tests.rs`); `frogdb-server`'s integration suite re-implements the whole
scan, the `.tmp` exclusion and the completeness predicate by hand
(`integration_persistence.rs:1602–1625`); and the website documents the names as a
supported backup unit (`backup-restore.md:22–29,77`). A change to the zero-pad width or
the prefix would orphan every snapshot on an existing installation — retention would stop
reaping them and the boot seed would restart at 1 and collide with a live directory —
and nothing in the repository would fail.

## Why this shape, in the vocabulary

- **The module is missing entirely.** There is no module with the layout as its
  implementation; the layout is spread across its callers as literals. Both the
  constructor direction and the parse direction are re-implemented per caller, which is
  the definition of zero **leverage**: nothing is paid back across the N call sites.
- **The seam is half-built.** ADR-0003 put a seam at the write side of the same on-disk
  protocol. A seam is where an interface lives, and here half the protocol's interface —
  every question the reader asks the filesystem — lives outside it. One adapter
  (`RealFs`) plus one recording adapter (`RecordingFs`) already exist, so this is a real
  seam, not a hypothetical one; the read operations simply were not routed to it.
- **Locality.** The exclusion rules (`.tmp` is skipped because of the dot, `latest` is
  skipped because `file_type()` does not follow symlinks) are re-derived at three
  scanners in two crates. Under an owner they are stated once and verified once.
- **Depth.** `SnapshotStager`'s interface currently obliges the caller to supply four
  mutually-consistent derived paths. Replacing them with `layout` shrinks the interface
  from nine fields to six while the implementation is unchanged — more behavior behind
  less interface.
- **Precedent in-crate.** `rocks/staged.rs` is this proposal, already executed one
  directory over: "Before this module the dir/file names were string literals duplicated
  across the three crates; `StagedCheckpoint` is the single owner"
  (`staged.rs:20–21`). Its test
  `staged_paths_are_the_cross_crate_contract` (`staged.rs:143–162`) is the template for
  the pinning test proposed below.

## Proposed change

**Stage 1 — `SnapshotLayout` (pure, no filesystem).**

A new `snapshot/layout.rs` owning every name and both directions of the mapping:

- Constructors from a snapshot root; accessors for `latest`, and for one epoch's
  `dir` / `staging_dir` / `name` / `metadata_path` / `checkpoint_dir` /
  `staging_metadata_path`.
- The parse direction — "is this directory entry a published snapshot, and at what
  epoch?" — as one predicate, stating the `.tmp` and `latest` exclusions once.
- Private `const`s for the five literals, mirroring `staged.rs`'s
  `STAGED_CHECKPOINT_DIR` / `STAGED_REPLICATION_METADATA_FILE` /
  `ROCKSDB_CURRENT_MANIFEST`.

`SnapshotStager` drops `snapshot_dir`, `tmp`, `final_dir` and `name` in favour of
`layout` (keeping `epoch`), so the coordinator can no longer hand it an inconsistent
trio. `record`'s logged path comes from the layout rather than a second `format!`.
The scanners in `rocks_coordinator.rs` and `stager.rs`, the test helper at
`tests.rs:249–266`, core's harness, and the server integration scan all route through it.

**Stage 2 — read operations on `SnapshotFs`.**

Add the four reads the layout needs (`read`, `read_dir` returning names, `read_link`,
`exists`) to the trait, implement them on `RealFs` and `RecordingFs`, and move the boot
reader off `std::fs`: `load_latest_metadata` and `highest_snapshot_epoch` become
`SnapshotLayout` methods taking `&dyn SnapshotFs`, with `RealFs`-defaulting public
wrappers — exactly the `load_staged_checkpoint` / `load_staged_checkpoint_with` pairing
that `rocks/checkpoint.rs:39–49` already uses for the install side. `latest`
resolution moves into the layout too, so the symlink-versus-plain-file question is
answered in one place for both the writer and the reader (closing §5).

Deliberately **not** added: `remove_file` / `remove_dir_all`. Retention deletion has no
durability ordering rule to enforce, so putting it behind the seam would buy fault
injection at the cost of widening a locked crate's most-asserted interface. It is a
separate decision, not a prerequisite.

## Testability improvement

1. **A pinning test with no filesystem and no database.** Assert the literal strings
   (`snapshot_00007`, `.snapshot_00007.tmp`, `latest`, `metadata.json`, `checkpoint`),
   the five-digit zero-pad, the `epoch → name → parse → epoch` round trip, and that
   `.snapshot_00007.tmp` / `latest` / `snapshot_x` are all rejected by the epoch
   predicate. This is the test that currently cannot exist: today a changed format string
   fails nothing, because every test re-derives the name the same way the code does.
2. **The third unreadable shape becomes reachable.** A fake `SnapshotFs` returning `EIO`
   from `read_link` or a short read from `read` lets
   `coordinator_boot_keeps_the_epoch_when_latest_metadata_is_unreadable` grow the case
   FM-PERSISTENCE-017's invariant text already promises — a *failing* pointer, not merely
   an absent or garbage one — without syscall interposition.
3. **The non-unix `latest` arms become reachable on Linux.** With resolution behind the
   seam, a fake can present `latest` as a plain file and the reader's fallback is
   exercisable on a supported platform, retiring several of the ten `#[cfg(unix)]` gates
   and turning §5's asymmetry into an assertion.
4. **Six boot tests stop opening RocksDB.** `SnapshotLayout::latest_metadata(fs)` is
   callable with a `TempDir` and no store, deleting the `make_store` dependency from the
   boot-seed tests (`tests.rs:1013`, `1077`, `1127`, and the three cases inside them) —
   the interface becomes the test surface.
5. **Deletion test.** Delete `SnapshotLayout` and the complexity reappears in nine
   places across three crates (four constructors, three scanners, one test helper, one
   integration scan). It is not a pass-through.

## Risks / scope boundaries

**`frogdb-persistence` is a LOCKED crate (ADR-0003, gate 0.85).**

- Landing requires `just mutants-diff frogdb-persistence` before push; a full
  `just mutants frogdb-persistence` + `just mutants-gate frogdb-persistence 0.85` if the
  diff run shows movement. The pure `SnapshotLayout` functions are cheap to gate (string
  formatting; the pinning test kills the mutants directly). Each new `RealFs` read method
  adds a mutant only a real-filesystem test can kill — follow the existing pattern,
  `real_fs_syncs_fail_on_a_path_that_is_not_there` (`fs_seam.rs:199–217`), which exists
  for exactly this reason on the sync methods.
- **No failure-mode row changes.** This is a pure structural move: no observable, no
  NOT-observable, no outcome variant is touched. FM-PERSISTENCE-017 (partial snapshot
  never visible / boot seed), -018 (post-commit steps cannot fail a durable snapshot /
  retention), -022, -023 (fsync ordering), -042 govern this code, and every one of them
  stays as written.
- **But three spec rows name these functions in prose**, and `scripts/failure-modes.py`
  only enforces row↔`Forced by` test-name agreement — it does not check prose. So
  renaming `load_latest_metadata` / `highest_snapshot_epoch` / `cleanup_old_snapshots`
  into `SnapshotLayout` passes `just lint-failure-modes` silently while leaving the
  spec's invariant text stale (FM-PERSISTENCE-017 names `load_latest_metadata`,
  FM-PERSISTENCE-018 names `cleanup_old_snapshots`, FM-PERSISTENCE-023 names the
  `SnapshotFs` method list). Update the prose in the same commit; it is a documentation
  edit, not a spec-first behavior change.
- **Do not rename the forced tests.** FM-017 forces
  `coordinator_boot_keeps_the_epoch_when_latest_metadata_is_unreadable`,
  `test_incomplete_snapshot_skipped`, `test_corrupted_snapshot_metadata`,
  `test_truncated_metadata_recovery`, `test_missing_completion_marker`, and the
  `test_stager_*` family; FM-018 forces `test_cleanup_old_snapshots`,
  `test_cleanup_unlimited`, `test_stager_retention_across_epochs`; FM-023 forces
  `stager_fsyncs_metadata_and_staging_dir_before_install`,
  `stager_fsyncs_snapshot_dir_after_install_and_after_latest_repoint`,
  `staged_checkpoint_install_fsyncs_the_data_dir_parent`. Renaming any of them breaks
  the lint in both directions unless the spec moves with it.

**`RecordingFs` trace compatibility — the concrete hazard.** FM-023's forced tests assert
exact ordered traces via `RecordingFs::trace` (`fs_seam.rs:133–153`), and two of them live
outside the snapshot module (`rocks/tests.rs:482–518`, `data_dir.rs:277`). If read
operations are appended to the same `ops` vector, those traces gain entries and
spec-forced tests fail on a change that alters no behavior — the worst possible signal.
Mitigation: record reads separately, or keep `trace()` mutation-only and add an
`all_ops()` for read-aware assertions. Decide this before writing any code.

**On-disk compatibility.** The names are a live operator contract
(`backup-restore.md:22–29,77`). A changed pad width or prefix orphans every existing
snapshot: retention would stop reaping them and the boot seed would restart at 1 onto a
live directory. The pinning test must assert literal strings, never re-derive them with
the same `format!` the code uses.

**Scope boundaries versus siblings (file-level, no overlap):**

| Proposal | Owns | This proposal must not touch |
|---|---|---|
| 38 (this) | `snapshot/layout.rs` (new), `snapshot/rocks_coordinator.rs`, `snapshot/stager.rs`, `fs_seam.rs` (**additive only** — no existing method signature changes) | — |
| 40 — FrameHeader codec unify | `serialization/mod.rs`, `serialization/probabilistic.rs`, `serialization/dataset.rs` | all of `serialization/` |
| 42 — RocksSink `CommitEffects` | `rocks/flush.rs` | `rocks/flush.rs` |
| 44 — RocksStore open shims | `rocks/mod.rs`, `rocks/columns.rs`, the `test-support` feature | `rocks/mod.rs`, `rocks/columns.rs` |

Because 38's `fs_seam.rs` change is purely additive, 40/42/44 can land in any order
relative to it. `rocks/staged.rs` and `rocks/checkpoint.rs` stay **unchanged** — the
`checkpoint_ready` layout already has its owner, and rerouting its `exists()` /
`is_complete_db()` predicates through the seam would perturb FM-023's install-side trace
assertions for no gain here. That is a separate decision, out of scope.

**Cross-crate reach.** `frogdb-core` depends on `frogdb-persistence` (and re-exports it
wholesale via `pub use frogdb_persistence::*`), so a `pub` `SnapshotLayout` is directly
usable from core's test harness and the server integration suite with no new dependency
edge. `SnapshotStager` and `SnapshotFs` stay `pub(crate)`.

## Effort

**M**, and it splits cleanly:

- **Stage 1 (S) — `SnapshotLayout` alone, no `SnapshotFs` change.** Independently
  landable and independently valuable: it kills all four production name constructions,
  both production parsers, the duplicated test helper, and the four leaked stager fields,
  and it adds the pinning test that makes the layout a checked contract rather than a
  convention. Zero trait changes means zero `RecordingFs` trace risk, so the mutation
  re-gate is a narrow diff.
- **Stage 2 (M) — read operations on the seam.** Larger blast radius: the trait grows,
  `RealFs`/`RecordingFs` grow, the boot reader moves, and the `RecordingFs` trace
  decision above has to be made and validated against three FM-023-forced tests in two
  modules.

**Independently-landable hotfix: none — no live defect was found.** The reader/writer
`latest` disagreement (§5) is confined to `#[cfg(not(unix))]` code that no shipped target
compiles, and the four name constructions agree today. Stage 1 is the smallest slice
worth landing on its own.
