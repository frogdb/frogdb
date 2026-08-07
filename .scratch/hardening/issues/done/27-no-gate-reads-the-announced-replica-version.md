# Nothing reads a replica's announced version — the rolling-upgrade gate does not exist

Status: done
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

## Resolution

The product decision (user, final) picked neither of the two shapes above: the consumer is a
**compatibility gate at the replication handshake**, not a finalization check. The primary
**refuses** a `PSYNC` from a replica announcing an incompatible *major* version, and **warns** once
per replica session on a *minor* mismatch. Spec: **FM-REPLICATION-064**, plus a matching Redis
deviation row (Redis has no such gate — its incompatibility surfaces later, as an RDB the replica
cannot load).

**Where the announced version reaches the primary.** `REPLCONF frogdb-version <v>` →
`AnnouncedOption::parse` → folded into the per-connection `ReplicaAnnouncement` by
`DispatchStage::ReplicationHandshake` → passed by value into `handle_psync` → seeded into the
`ReplicaSession` by `register_announced_replica` (FM-REPLICATION-049). There is no session before
the `PSYNC`, so the announcement is per-connection state up to that point.

**Where the gate went, and why.** `PrimaryReplicationHandler::handle_psync`
(`frogdb-server/crates/replication/src/primary/mod.rs`), immediately after the shutdown-drain check
and *before* `record_sync_outcome` / `register_announced_replica`. Not at the `REPLCONF`: that step
is the *record*, and a peer is free to log an error there and send `PSYNC` anyway (Redis's replica
does exactly that for options its primary rejects), so a gate living only there fails open against
the peer it exists to stop. `PSYNC` is where the primary would commit a registry entry, a resync
counter and possibly a checkpoint — so refusing there is both the last enforceable point and the
one that leaves no trace behind.

**"Incompatible" is defined once**, in the new `frogdb-replication/src/version_compat.rs`, whose
module doc is the rule's documentation at the gate:

- versions are compared on their numeric core (the string is cut at the first `-` or `+`, so
  `2.0.0-rc1` compares as `2.0`);
- **different major = incompatible → refused**;
- same major, different minor = **served + one `WARN` per session**;
- same major and minor (patch-only difference or exact match) = served, silent;
- either side unreadable or unannounced = **served + one `WARN`** naming the raw string (or
  `<unannounced>`). Unknown is *unknown*, not old — refusing it would drop every pre-option and
  non-FrogDB peer on a suspicion. This is the deliberate opposite of FM-REPLICATION-049's
  "unknown must block" stance, which belongs to the *finalization* gate: refusing to finalize is
  reversible and costs a retry, refusing to replicate costs availability and durability.

**The error text is actionable** — one line naming both versions, both majors, the rule, and both
remedies:

```
-ERR PSYNC refused - replica announced FrogDB version 999.0.0 (major 999) but this primary is
FrogDB version 0.1.0 (major 0); replication requires both ends on the same major version. Run the
replica on a 0.x build, or move this primary to 999.x, then let it reconnect.
```

A FrogDB replica re-surfaces that same text in its own log (`ReplicaConnection::psync` maps a
leading `-` line to `PSYNC error: <rest>`), so either operator can act from the node they are
watching. `MajorMismatch::wire_error()` is the single spelling, shared by the wire, the log and the
tests.

**Not done, filed instead.** The finalization-side readiness check the original issue asked about —
"is this standalone primary's replica fleet at the target version yet", the thing an upgrade tool
polls, where unknown *does* block — remains unbuilt and is now
`.scratch/hardening/issues/open/44-no-upgrade-readiness-check-for-a-primarys-replica-fleet.md`.
The three tests this issue proposed (`finalization_is_refused_while_...`) belong to that issue, not
to this one, because the gate that shipped refuses a *connection*, not a finalization.

**Tests** (all tagged `// FM-REPLICATION-064`, all in the locked crate except the last):
`version_compat::tests` — `an_identical_version_is_compatible`,
`a_patch_only_difference_is_compatible_and_silent`, `a_minor_difference_is_admitted_and_reported`,
`a_different_major_is_refused_in_either_direction`, `the_refusal_error_names_both_versions`,
`an_unannounced_version_is_unproven_not_incompatible`,
`an_unreadable_version_is_unproven_and_keeps_the_raw_string`,
`an_unreadable_primary_version_refuses_nobody`,
`a_pre_release_or_build_suffix_compares_on_its_numeric_core`,
`a_version_without_a_readable_minor_compares_on_its_major`,
`a_peer_built_from_this_tree_is_compatible_with_this_primary`; `replica_session::tests` —
`psync_from_an_incompatible_major_is_refused_before_anything_is_registered` (asserts the registry
and the sync counters are untouched), `psync_from_a_minor_skewed_replica_is_served`,
`psync_from_a_replica_with_an_unreadable_version_is_served`; end-to-end —
`test_psync_is_refused_for_an_incompatible_replica_version` (`frogdb-server`).
