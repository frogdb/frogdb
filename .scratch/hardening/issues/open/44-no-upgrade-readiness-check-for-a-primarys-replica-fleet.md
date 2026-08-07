# No upgrade-readiness check reports a standalone primary's replica fleet versions

Status: needs-triage
Type: gap (missing operator surface)
Severity: likelihood 2/3 (any rolling upgrade of a non-cluster primary-replica pair), consequence
2/3 (an operator finalizes an upgrade blind, rather than being told "no") — score 4
Area: replication / cluster upgrade

## Problem

Split out of issue 27, which asked two questions and got one answered. Issue 27's gate shipped
(FM-REPLICATION-064): a primary now **refuses** a `PSYNC` from a replica on a different major
version and **warns** on a minor skew. That is a *connection* gate — it stops an incompatible
replica from attaching, and it is the right shape for that job.

It is not an *upgrade-readiness* surface. An operator part-way through a rolling upgrade wants to
ask "is every replica attached to this primary already at the target version?" before finalizing,
and there is still nothing that answers it for a non-cluster deployment:

- `ClusterCommand::FinalizeUpgrade` (`frogdb-server/crates/cluster/src/commands.rs`) validates the
  **raft topology's** node versions. A standalone primary with two replicas has no raft topology,
  so it validates nothing there.
- `version_gate::is_gate_active` and the `frogdb_version_gate_active` gauge key on the cluster's
  finalized `active_version` and never read a replica announcement.
- `INFO replication`'s `slaveN:` lines render `ip/port/state/offset/lag` — not the announced
  version, which `ReplicaInfo::replica_version` holds and no wire surface renders
  (FM-REPLICATION-049).

So the version FM-REPLICATION-064 refuses on is invisible to the operator until it causes a
refusal. A minor skew — the case that most often means "the rollout is half-done" — is visible only
in the primary's log.

## What would close it

A readiness check an upgrade tool can poll, for a primary that is not a raft member. Two shapes,
not equivalent:

1. **A rendered field**: add the announced version to the `slaveN:` line (or a parallel
   `slaveN_version:` line, since `slaveN:` is a Redis-compatible format that tooling parses
   positionally). Cheapest, and makes the fleet's versions visible in the surface operators already
   read. Does not itself refuse anything.
2. **A command** (`FROGDB.UPGRADE STATUS`-shaped) reporting per-replica announced version, the
   target, and a single ready/not-ready verdict. More work, but it is the thing a tool actually
   wants to gate on, and it can fail closed.

**Unknown must block here**, which is the deliberate opposite of the handshake gate: refusing to
*finalize* is reversible and costs an operator a retry, so a vacuously-true "every known replica is
at the target" over an empty or unknown-laden set is the failure this issue exists to prevent
(FM-REPLICATION-049's stance, and the reason issue 22 escalated). That reopens the third decision
issue 27 raised and left open: **is there an operator override for a permanently-unknown peer?** A
pre-option or non-FrogDB replica would otherwise make a deployment un-finalizable forever.

## Tests that should exist

- `finalization_is_refused_while_a_replica_reports_an_older_version`
- `finalization_is_refused_while_any_replica_version_is_unknown` — the vacuous-truth case, and the
  whole reason this is not a cosmetic issue.
- `finalization_succeeds_once_every_attached_replica_is_at_the_target`

## Spec impact

A new FM-REPLICATION row for the readiness surface, whose NOT-observable half must name the
vacuous-truth failure and must distinguish itself from FM-REPLICATION-064 (which admits unknown on
purpose). FM-REPLICATION-049's `Outcome variant` note — "`replica_version` is internal, no surface
renders it" — needs amending if shape 1 is chosen.
