# Nothing reads a replica's announced version — the rolling-upgrade gate does not exist

Status: needs-triage
Type: gap (missing consumer / gate believed to exist)
Severity: likelihood 2/3 (any rolling upgrade of a primary-replica pair), consequence 3/3 (a version
gate that is believed to be enforced and is not) — score 6
Area: replication / cluster upgrade

## Problem

Filed by issue 22, whose remedy step 4 says: "find the finalization check the comment refers to and
confirm it actually reads the field — if no such check exists, that is a third issue, not a closed
one."

No such check exists.

Issue 22 fixed the **writer**: `REPLCONF frogdb-version` is now folded into `ReplicaAnnouncement`
and seeded into `ReplicaSession`, so `ReplicaInfo::replica_version` carries what the replica said
(`None` = unknown). The **reader** the deleted comment named — "so the primary can check all replica
versions during finalization" — was never written:

- `ClusterCommand::FinalizeUpgrade` (`frogdb-server/crates/cluster/src/commands.rs:444-475`)
  iterates `inner.nodes`, the **raft topology**, and validates `node.version`. It fails closed on an
  empty version, which is right, but those are cluster members — not the replicas attached to this
  primary over `PSYNC`. A standalone primary with two replicas has no raft topology at all, so
  finalization there validates nothing.
- `version_gate::is_gate_active` / `pending_gates`
  (`frogdb-server/crates/cluster/src/version_gate.rs`) key on the cluster's finalized
  `active_version`. Neither reads a replica announcement.
- `frogdb_version_gate_active` (`types/src/metrics/definitions.rs:501`) is exported from
  `subsystems.rs` off the same `active_version`, so the gauge an operator watches during an upgrade
  says nothing about the attached replicas either.

Consequence: an operator running a rolling upgrade of a replicated (non-cluster) deployment has no
mechanism that refuses to finalize while an old replica is still streaming, and the surfaces that
look like one (`FinalizeUpgrade`, the gate gauge) are about a different set of nodes.

## Open question — this needs a product decision, not just code

Two shapes, and they are not equivalent:

1. **Extend `FinalizeUpgrade` to also consult the primary's attached replicas.** Fails closed on
   `replica_version == None` (see FM-REPLICATION-049: unknown must block, or the gate fails open).
   Cheap, but couples a raft command to a per-node replication registry, and only helps a node that
   is *both* a raft member and a `PSYNC` primary.
2. **A separate replication-level readiness check** that an upgrade tool queries (`FROGDB.UPGRADE
   STATUS`-shaped, or a field in `INFO replication`), reporting the announced version of every
   attached replica and whether any is unknown or below target. Works for standalone deployments,
   which is where the gap actually bites; needs a wire surface designed and a `slaveN:`-adjacent
   field or a new command.

Either way a third decision is needed: **does an unknown version block finalization outright, or is
there an operator override?** Failing closed with no override makes any non-FrogDB or pre-option
peer permanently un-finalizable.

## Tests that should exist

Whatever shape is chosen:

- `finalization_is_refused_while_a_replica_reports_an_older_version`
- `finalization_is_refused_while_any_replica_version_is_unknown` — the vacuous-truth case, which is
  the whole reason this is severity 3.
- `finalization_succeeds_once_every_attached_replica_is_at_the_target`

## Spec impact

FM-REPLICATION-049 records the announcement and explicitly stops at the writer ("the primary records
what the replica said, and unknown stays unknown"). Closing this issue adds a row for the gate
itself — the consumer side — and that row's NOT-observable half must name the vacuous-truth failure.
