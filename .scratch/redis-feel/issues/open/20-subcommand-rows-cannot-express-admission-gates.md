# Subcommand rows cannot express `noscript` / `stale`, so nine parity exemptions are structural

Status: needs-triage

## Origin

Found while working [issue 17](../done/17-unimplemented-admission-gates.md).
Removing `noscript` and `stale` from `UNCOMPARED_FLAGS` turned the flag-parity
gate on for both admission flags and surfaced 53 divergences. 46 were fixed by
correcting the spec; the residue is not a set of judgment calls but one modelling
limitation, repeated.

## What is wrong

`SubcommandSpec::flags_over` (`frogdb-server/crates/core/src/command_spec.rs`)
lets a row refine only `BEHAVIORAL_FLAGS` — `WRITE | READONLY | DENYOOM` — and
`SubcommandFlagsNotBehavioral` rejects a row that declares `NOSCRIPT`, `STALE`,
`ADMIN` or `LOADING`. Those four are container-level facts in this codebase: a
subcommand inherits its container's gate and cannot clear or add one.

Upstream declares them per subcommand, and does so non-uniformly — most visibly,
every container's `HELP` row drops `noscript` and gains `stale`. So a container
whose gates are otherwise correct is wrong on exactly one row, and widening or
narrowing the container to fix that row breaks all its siblings (a see-saw).

Nine `SUBCOMMAND_FLAG_EXEMPTIONS` entries exist purely because of this — see
`frogdb-server/crates/server/src/server/upstream_metadata_tests.rs`:

- `ACL|HELP`, `CLIENT|HELP`, `CONFIG|HELP`, `FUNCTION|HELP`, `SCRIPT|HELP`
  (shared reason `HELP_INHERITS_CONTAINER_GATES`)
- `MEMORY|HELP`, `OBJECT|HELP` (shared reason
  `HELP_UNDER_STALE_LESS_CONTAINER`)
- `CLUSTER|RESET` — upstream's only `noscript` CLUSTER row
- `SCRIPT|LOAD` — upstream's only `stale` non-HELP SCRIPT row

Plus two whole-command entries of the same family (`ACL`, `CLIENT`), where
upstream's own container row carries `SENTINEL` alone while FrogDB's carries the
union its subcommands need.

## Why it matters

The exemptions are honest but they are also load-bearing: while they stand, the
parity gate cannot tell a deliberate divergence from a new drift on those rows —
which is precisely how the 53 divergences accumulated unnoticed behind the old
`UNCOMPARED_FLAGS` entry.

There is a small behavior gap too. `HELP` is refused from a script and on a
link-down replica wherever the container is, where upstream answers it. Static
help text discloses nothing and needs no fresh data, so upstream's choice is the
better one.

## Sketch

Let `SubcommandSpec` refine `NOSCRIPT` and `STALE` in addition to the behavioral
flags, and have the gates read the resolved row flags rather than the container's
— `Command::flags_for` / `CommandImpl::flags_for` already return the resolved set
and the admission chokepoint already consults it (issue 17), so the gate side may
need no change at all. Then delete the nine subcommand entries and revisit the
two container entries.

Cost to watch: `flags_over`'s validation and the `ADMIN` split (`SPLIT_ADMIN_SURFACES`)
are the other half of this model; changing what a row may declare should not
quietly give rows a second way to open an admin surface.
