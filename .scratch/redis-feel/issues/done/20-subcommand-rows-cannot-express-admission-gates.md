# Subcommand rows cannot express `noscript` / `stale`, so nine parity exemptions are structural

Status: done

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

## Ruling (2026-08-28)

General mechanism (option a):

- Add an optional admission-override to `SubcommandSpec` — `None` = inherit the
  container's admission flags (today's behavior for all existing rows);
  `Some(...)` = the row's NOSCRIPT/STALE/LOADING verbatim. Naive widening of
  `BEHAVIORAL_FLAGS` is ruled out: `flags_over` replaces that half wholesale and
  would silently clear noscript/stale on all existing rows.
- Plumb subcommand-effective flags into both gates: the noscript branch in
  `command_admission` and the stale (MASTERDOWN) gate in `run_pre_checks`. Both
  already see argv; subcommand row resolution already exists.
- HELP rows declare `stale + loading` (upstream marks every container HELP
  `loading stale`) — behavior change: HELP is served on a link-down replica,
  Redis parity.
- `CLUSTER|RESET` declares noscript via the override; the
  `is_forbidden_subcommand` side-table entry for it dies (single source of
  truth). Verify CLUSTER FLUSHSLOTS handling before removing anything else.
- `SCRIPT|LOAD` stays a deliberate, documented deviation (refusing stale
  SCRIPT LOAD is the ruled behavior) — its exemption entry remains by choice,
  reworded to say so.
- The 9 structural HELP exemptions in `SUBCOMMAND_FLAG_EXEMPTIONS` come off
  (shrink-only list, removal is the enforcement).
- ADMIN stays out of the override — `SPLIT_ADMIN_SURFACES` is already the
  single source of truth for per-subcommand admin.

## Resolution (2026-08-28)

Implemented (`d74f86c2`). `SubcommandSpec` gained `admission:
Option<CommandFlags>` set via `const fn with_admission`; `None` inherits the
container's admission subset, `Some` replaces it wholesale (a row can clear
noscript, not only add). New `ADMISSION_FLAGS = NOSCRIPT | STALE | LOADING`;
ADMIN stays out (`SPLIT_ADMIN_SURFACES` remains the sole per-subcommand admin
authority). `SpecError::SubcommandAdmissionNotAdmission` rejects non-admission
bits at validation.

Key finding: no gate call-site edits were needed. All three `admit_command`
sites, the MASTERDOWN gate in `run_pre_checks`, the script gate, and the
COMMAND INFO/DOCS nested-row emitter already read per-invocation flags through
`CommandImpl::flags_for` -> `flags_over` — widening `flags_over` was the single
seam.

Data: all 15 container HELP rows declare `STALE | LOADING` via one shared
`help()` constructor (behavior change: HELP served on a link-down replica,
Redis parity). `CLUSTER|RESET` carries `NOSCRIPT | STALE` via the override and
its `is_forbidden_subcommand` arm is deleted (FLUSHSLOTS stays in the
side-table — FrogDB dispatches no FLUSHSLOTS row to hang a flag on,
documented). `SCRIPT|LOAD` stays a deliberate deviation, exemption reworded to
"expressible but retained by choice".

Exemptions: 8 structural entries removed (7 HELP rows + CLUSTER|RESET); dead
reason constants deleted; survivors all diverge on non-admission bits only.
No new divergences surfaced by the widened comparison.

Forcing tests: override math + upstream-resolution units in
`command_spec.rs`; integration
`a_link_down_replica_serves_container_help_but_not_container_reads`
(MEMORY HELP ok, MEMORY USAGE -> MASTERDOWN, `replica-serve-stale-data no`);
`tcl_eval_cluster_reset_not_allowed_from_script` strengthened to pin the exact
flag-path wording.
