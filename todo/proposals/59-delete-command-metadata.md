# Proposal: Delete the `CommandMetadata` Second Metadata Interface

Status: proposed
Date: 2026-07-16

## Problem

The registry carries **two** hand-maintained metadata interfaces for the same facts. `CommandSpec`
(`command_spec.rs:311`) is the single source of truth this branch
(`refactor/command-spec-single-source`) exists to establish — and proposal
[01](01-declarative-command-spec.md) established it. Alongside it, a parallel `CommandMetadata`
trait re-declares the exact same subset (name / arity / flags / execution_strategy / keys /
keys_with_flags), reached through a third `CommandImpl` variant, `MetadataOnly`, that no production
code registers. Its own docs describe it as transitional; the transition finished when every
connection group migrated to `ConnectionCommand`.

| Symptom | Where (file:line) |
|---------|-------------------|
| `CommandMetadata` trait — a duplicate of the spec's metadata subset | `core/src/command.rs:651-682` |
| `CommandImpl::MetadataOnly` variant, doc'd "transitional … collapses into `Connection` as each connection group is migrated" | `core/src/registry.rs:29-31,39-42` |
| 6 `MetadataOnly` metadata-forwarding match arms + 3 combined `Shard(_) \| MetadataOnly(_)` arms | `core/src/registry.rs:58,67,76,85,98,113` and `:126,134,159` |
| `register_metadata` — the only constructor of `MetadataOnly`; **zero production callers** | `core/src/registry.rs:225-229` |
| `define_metadata_command!` codegen macro (and its private `define_command!` helper arms), used only by its own test | `core/src/command_macro.rs` (entire 209-line file) |
| `server/src/commands/metadata.rs` — 10 `metadata_command!` structs (`MultiMetadata` … `LastsaveMetadata`) defined and **never registered** | `server/src/commands/metadata.rs` (entire 157-line file) + `commands/mod.rs:13` |
| Test-only impls of the dead trait | `core/src/registry.rs:316-338,370-387,471`; `command_macro.rs:166-209` |

Two consequences:

1. **A second metadata home that can silently diverge.** Nothing forces a `metadata_command!`
   entry to agree with the live spec of the same command. Proposal [40](40-deletions-small-seams.md)
   §7 already flagged and claimed to delete the "never-registered metadata-only duplicates" in
   `metadata.rs`; it removed only the *pub/sub* ones (migrated behind `ConnCtx`). The ten
   transaction / connection-state / auth / persistence structs survived and are dead today —
   defined-but-never-registered, referenced only by their own definitions. `MULTI`, `EXEC`, etc.
   are live commands registered elsewhere; these structs are stale shadow metadata.

2. **An all-interface, no-module variant.** `MetadataOnly` exists so a command can be listed for
   `COMMAND INFO` without an executor. But `Connection` already does exactly that — a
   `ConnectionCommand` lands in `entries` (visible to `names()` / `get_entry()`) but not in
   `commands` (invisible to `get()` / `command_names()`), which is the entire behavioral contract
   `MetadataOnly` was carrying. The variant survives one hop and is pattern-matched; the deletion
   test fails in the telling direction: removing it deletes only plumbing, no behavior.

## Design

`CommandImpl` becomes a two-way union — `Shard | Connection` — and every match over it loses an arm.
Delete, top to bottom:

- **The trait.** `CommandMetadata` (`command.rs:651-682`) and its re-export (`lib.rs:70`).
- **The variant + machinery** in `registry.rs`: the `MetadataOnly` variant and its type/method docs
  (`:29-31,39-42,48-49,144`); the import (`:6`); the six metadata-forwarding arms
  (`:58,67,76,85,98,113`); the three combined arms collapse `Shard(_) | MetadataOnly(_)` → `Shard(_)`
  (`:126,134,159`); `register_metadata` (`:225-229`). The `names()`/`command_names()`/`len()` and
  `get_entry()` doc comments that say "includes metadata-only" (`:168,237,242,247,252`) are reworded
  to describe the `Connection` split, which now carries that role alone.
- **The codegen.** `command_macro.rs` in full: `define_metadata_command!`, its private
  `define_command!` `@arity`/`@flags`/`@keys` helper arms (zero callers outside this file — verified),
  and the file's own test module — plus its `pub mod command_macro;` line (`lib.rs:16`). Full commands
  already declare a `CommandSpec` and implement `execute()` by hand; this macro only ever fed the
  dead trait.
- **The dead struct file.** `server/src/commands/metadata.rs` in full and its `pub mod metadata;`
  (`commands/mod.rs:13`).

**Test-only impls — convert, don't just drop.** Two registry tests pin a contract worth keeping:
`test_metadata_only_registration` (`:370-387`) and the `register_metadata` half of
`test_mixed_registration` (`:471`) assert an entry that is present in `names()` / `get_entry()` but
absent from `get()` / `command_names()`. That invariant survives the deletion — it is exactly what
the `Connection` variant provides — so retarget both onto `register_connection` using the
`TestConnCommand` / `conn_spec` fixtures already present in the same test module (`:389-422`), and
delete the `TestMetadataOnly` struct (`:316-338`). `test_metadata_only_registration` then overlaps
`connection_registration_dispatches_through_union` (`:424-445`); fold the unique assertions in and
drop the redundant one, or keep both — either way no coverage is lost.

## Why this is the right depth

- **Deletion test.** This is the deletion, not a refactor: a trait, an enum variant, ~9 match arms,
  a constructor, a whole codegen file, and a whole dead-struct file leave — and nothing that ran in
  production stops running, because nothing in production constructed a `MetadataOnly`. The only
  edits that are *not* deletions are three tests re-pointed at an equivalent, already-tested seam.
- **Leverage / single source.** Proposal 01's step 4 explicitly scheduled "fold `CommandMetadata`
  onto `CommandSpec` … delete legacy trait methods and `define_command!`" as "the deletion-test
  payoff" (`01-declarative-command-spec.md:411`). This proposal *is* that final cleanup: after it,
  a command's metadata has exactly one home (`CommandSpec`), reachable through exactly two executor
  shapes (`Shard`, `Connection`), and the "which is authoritative" question cannot be asked.
- **Locality.** The `COMMAND INFO` introspection surface reads `get_entry(...).flags()`/`.arity()`
  through the union; collapsing the union to two variants keeps that surface identical while removing
  the third code path it had to fan out to.

## Testing impact

- The retargeted `test_metadata_only_registration` and `test_mixed_registration` assert the same
  in-`names()`/not-in-`get()` invariant through `register_connection`; `connection_registration_*`
  and the `validate_strategy_*` tests are untouched (they already exercise the `Connection` arm).
- `command_macro.rs`'s test module dies with the file; it tested only the deleted macro.
- No integration or TCL suite references `MetadataOnly`, `register_metadata`, `CommandMetadata`, or
  the `metadata.rs` structs (workspace grep: matches confined to the files being deleted). `COMMAND
  INFO` / `COMMAND GETKEYS` regressions run through `get_entry` and are strategy-driven, so they stay
  green — the same reasoning proposal 40 §7 relied on.
- `just fmt` / `just check` / `just lint` on `frogdb-core` and `frogdb-server`, plus their crate
  tests.

## Risks / open questions

- **Historical proposals reference the removed names.** Proposal 40 §7 (`:149-150,158-161`) and
  proposal 01 §step-4 (`:411`) name `CommandMetadata` / `register_metadata` / `define_command!`.
  Both are `implemented`/`proposed` historical records describing the state at their time; this
  proposal is the scheduled follow-through, so they are left as-is (no back-editing of accepted
  proposals). `INDEX.md` is updated separately, not here.
- **Proposal 40 §7 over-claimed.** It states the never-registered `metadata.rs` duplicates were
  deleted, but only the pub/sub subset was; ten structs remain. This proposal completes that
  deletion. Worth a one-line note in the commit so the discrepancy is on the record.
- **Re-export removal is a public-crate API change.** `CommandMetadata` leaves `frogdb_core`'s
  `pub use`. Pre-production, breaking changes are acceptable, and the grep confirms no external
  consumer inside the workspace; nothing to preserve.
