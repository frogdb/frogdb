# `SENTINEL` is reported as an unimplemented command with no recorded reason

Status: needs-triage
Type: documentation / ruling needed
Area: compat surface

## Problem

The compat matrix lists `SENTINEL` (Redis 2.8.4 container command, flags `ADMIN SENTINEL
ONLY_SENTINEL`) as `unsupported` with the generic note "Present in Redis 8.6.1; not implemented in
FrogDB" — indistinguishable from gaps that *are* bugs, like
[issue 30](../../../redis-feel/issues/) (`FAILOVER`) and
[issue 31](../../../redis-feel/issues/) (`RESTORE-ASKING`).

This one is almost certainly a deliberate architectural deviation, not a gap. `SENTINEL` is
`ONLY_SENTINEL` — it exists only on a server running in Sentinel mode. FrogDB's HA story is Raft
(`frogdb-cluster` + `frogdb-cluster-runtime`, ADR-0004), which fills the same role by a different
design; FrogDB would have to *become* a Sentinel to implement it.

But nothing in the repo says so. Per ADR-0005's own consequence — claiming the compat target makes
every unadvertised gap a bug by definition — the fix is to advertise the deviation, not to
implement the command.

## Ruling needed

Confirm `wontfix`, then record it in the two places that make it truthful:

1. The matrix generator's exclusion input (`website/src/data/compat-exclusions.json`) so the matrix
   renders a reason instead of "not implemented".
2. The compatibility/differences docs, stating that FrogDB has no Sentinel mode and that
   Raft-based cluster HA replaces it.

Consider covering the whole Sentinel-mode surface at once, so a future `SENTINEL` subcommand
appearing upstream does not reopen this.

## Acceptance criteria

- [ ] Ruling confirmed; `Status:` flipped to `wontfix` and the file moved to `done/`
- [ ] The compat matrix shows a reason for `SENTINEL`, not a bare "not implemented in FrogDB"
- [ ] The compatibility docs state "no Sentinel mode; Raft-based HA instead"

Size: XS
