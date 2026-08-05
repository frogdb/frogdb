# Cluster — failure modes

Status: LOCKED (2026-08-05) — Phase 4 mutation gate passed (`frogdb-cluster` 99.6% on 496
mutants, `frogdb-cluster-runtime` 99.0% on 224, vs an 0.80 gate; the four surviving mutants are
all documented equivalents at the code). Behavior changes to this area are spec-first: edit the
row, update the forcing test, then the code. Before pushing changes that touch these crates, run
`just mutants-diff <crate>`. See `docs/agents/hardening-campaign.md`.

Every way FrogDB's cluster layer can refuse, redirect, reassign, or succeed, one table per mode.
This is the reference the mutation run is measured against: a mutant that survives is a row nothing
forces.

Scope. The cluster area is one replicated state machine plus the seams that read it:

* **Topology and membership (FM-CLUSTER-001..009, 078)** — the `ClusterCommand` arms that add,
  remove, re-role, and reset nodes, the bootstrap slot split, the rolling-upgrade gate, and how the
  applied topology is published to readers
  (`frogdb-server/crates/cluster/src/{commands,state,types,version_gate}.rs`).
* **Config epoch (010..017, 076)** — the two epochs (the replicated counter and the per-node value),
  who may raise them, the collision rule, and what survives a restart
  (`cluster/src/{state,commands,storage}.rs`, `server/src/commands/cluster/mod.rs`).
* **Slot ownership and routing (018..030)** — keyslot mapping, the `RouteDecision` verdicts and
  their wire renderings, CROSSSLOT, READONLY, ASKING, and `WATCH` registration
  (`server/src/slot_migration/{routing,validator}.rs`, `core/src/shard/partition.rs`).
* **Slot migration (031..038)** — the `SETSLOT` state machine as it exists today: a two-point
  ownership swap with no intermediate migration state, its idempotency, its cancel, and the
  documented residual window between commit and apply (`cluster/src/commands.rs:361-436`,
  `server/src/slot_migration/`).
* **Failover, promotion, and the propose path (039..051)** — the atomic `Failover` entry, the role
  events it emits, the reconciler that re-drives a data path that disagrees, and every error
  surface of `ClusterWriter::propose` (`cluster/src/{commands,state,writer,network}.rs`).
* **Failure detection (052..060)** — the leader-only, level-triggered detector: latching
  hysteresis, staleness, quorum arithmetic, promotion scoring, and the live runtime flags
  (`cluster-runtime/src/{failure_detector,flags}.rs`).
* **Admin gating (061..064)** — the per-subcommand admin surface for `CLUSTER`, including both
  fail-closed defaults (`core/src/command_spec.rs`, `server/src/connection/guards.rs`).
* **Bus, pub/sub, and reporting (065..075, 077)** — the cluster bus transport decision, the pub/sub
  RPC outcome mapping, the bus packet counters, and what `CLUSTER NODES`/`SLOTS`/`SHARDS`/`INFO`
  render
  (`cluster-runtime/src/{bus,pubsub}.rs`, `cluster/src/wire.rs`,
  `server/src/commands/cluster/mod.rs`).

Out of scope, deliberately: the pause-barrier design (no `FM-CLUSTER-BARRIER` rows — the design is
pending a decision), the operator and `frogctl`, and the frozen redis-regression suite. `WAIT` has
no cluster-mode special case at all — it is keyless, counts *this node's* replicas, and never
redirects — so it is specced once in
[Replication — failure modes](replication-failure-modes.md) (FM-REPLICATION-037..043) and not
duplicated here.

## How to read a row

| Field | Meaning |
|---|---|
| Trigger | The concrete precondition or interleaving that puts the cluster in this mode. |
| Observable | What a client of some node sees: the reply, the redirect, `CLUSTER INFO`/`NODES`, the slot map. |
| NOT observable | What must never appear in this mode. This is the half mutation testing attacks. |
| Invariant | The internal guarantee, named at the mechanism that provides it. |
| Outcome variant | The enum variant / error string the mode reports through, or `n/a`. |
| Forced by | The test(s) that fail if the behavior changes. Every one carries a `// FM-CLUSTER-NNN` tag at its definition site; `just lint-failure-modes` enforces both directions. |
| Bug refs | Known issues that touch this mode. |

Test names are bare function names, resolved against the crate list in `scripts/failure-modes.py`
(`NEXTEST_CRATES`).

---

## FM-CLUSTER-001 — `AddNode` is an upsert that can never demote a node by restarting it

| Field | Value |
|---|---|
| Trigger | A node re-registers itself through `CLUSTER MEET` or on its own boot. It sends the only role it can know unaided — `Primary` — even when the cluster has since made it a replica. |
| Observable | The node's `addr` and `cluster_addr` are refreshed to the values in the incoming record; its `role` and `primary_id` in `CLUSTER NODES` are exactly what they were before. A node that has never been seen is added verbatim. The command always answers `ClusterResponse::Ok`. |
| NOT observable | A restart flipping a replica back to `master` in `CLUSTER NODES` — which would point its replication stream at itself and orphan the primary's write path. Nor `NodeAlreadyExists`: that variant exists in `ClusterError` but `AddNode` never constructs it, so a re-register is never an error. Nor a role/promotion event: an `AddNode` that changes nothing emits nothing. |
| Invariant | `apply_command`'s `AddNode` arm overwrites the incoming `role`/`primary_id` from the existing record before inserting (`commands.rs:43-54`), so role transitions have exactly one source — `SetRole` and `Failover` — and re-registration is not one of them. A binary-version mismatch against the highest version any *other* versioned peer reports warns and never rejects, so a mixed-version cluster still converges on membership; the warning carries that value under `max_peer_version`, not a name like `cluster_version` that would misread it as the cluster's consensus version. |
| Outcome variant | `ClusterResponse::Ok` |
| Forced by | `test_add_node`, `test_add_duplicate_node`, `test_add_node_reregistration_keeps_recorded_role`, `test_add_node_mixed_version_succeeds`, `test_apply_local_shares_validated_path`, `a_disagreeing_re_registration_is_reported_on_either_half`, `test_node_flags_handshaking_sets_only_the_handshake_bit`, `mixed_version_warning_compares_against_the_other_versioned_nodes` |
| Bug refs | — |

`MEET` and `FORGET` have no `ClusterCommand` of their own: they are server-layer verbs that lower to
`AddNode` / `RemoveNode`, so their replicated semantics are exactly rows 001 and 002.

## FM-CLUSTER-002 — `RemoveNode` orphans the departing node's slots rather than transferring them

| Field | Value |
|---|---|
| Trigger | `CLUSTER FORGET <node>` against a node that owns slots, has open migrations, or has replicas parented to it. |
| Observable | The node disappears from `CLUSTER NODES`. Every slot it owned becomes *unassigned* — a client keyed into one of those slots gets `CLUSTERDOWN`, not a `MOVED` to some successor. Forgetting an unknown node is `NodeNotFound`. |
| NOT observable | Slots silently retargeted at another node: `FORGET` is not a failover, and inventing a successor here would hand a keyspace to a node that never received its data. Nor an epoch bump — removing a node changes no node's authority. |
| Invariant | The arm is `slot_assignment.retain(owner != node_id)` followed by `nodes.remove` (`commands.rs:110-113`); transfer lives only in `Failover`, which validates a named successor first. |
| Outcome variant | `ClusterResponse::Ok` / `ClusterError::NodeNotFound` |
| Forced by | `test_remove_node_clears_slots`, `test_remove_node_nonexistent`, `remove_node_leaves_migrations_and_replicas_dangling` |
| Bug refs | — |

Accepted non-guarantee, pinned by `remove_node_leaves_migrations_and_replicas_dangling`: `RemoveNode`
cleans up neither migrations that name the removed node nor the `primary_id` of its replicas. Only a
*force* `Failover` does that (FM-CLUSTER-036). A `FORGET` of a migration endpoint therefore leaves a
migration that can never complete; the operator's recovery is `SETSLOT … STABLE` (FM-CLUSTER-035).

## FM-CLUSTER-003 — slot assignment is validate-all-then-apply

| Field | Value |
|---|---|
| Trigger | `CLUSTER ADDSLOTS` naming a range in which one slot is already owned by a *different* node. |
| Observable | The whole command is refused with `SlotAlreadyAssigned(slot, owner)` and **no** slot in the batch changes hands. Re-assigning a slot to the node that already owns it is idempotent, not an error. Assigning to an unknown node is `NodeNotFound`. |
| NOT observable | A partially applied batch — the prefix of the range moved and the suffix refused. That would leave the caller with an error and the cluster with a slot map neither side believes in. |
| Invariant | The arm walks every slot of every range in a validation pass before it mutates anything (`commands.rs:118-140`), and `apply_command` holds one write lock for the whole command, so the transition is a single linearization point. |
| Outcome variant | `ClusterError::SlotAlreadyAssigned` |
| Forced by | `test_assign_slots`, `test_assign_slots_node_not_found`, `test_assign_slots_already_assigned`, `test_assign_slots_idempotent`, `assign_slots_rejects_the_whole_batch_on_one_conflict` |
| Bug refs | — |

## FM-CLUSTER-004 — `RemoveSlots` unassigns only slots the named node owns

| Field | Value |
|---|---|
| Trigger | `CLUSTER DELSLOTS` naming slots that are assigned to a node other than the one in the command. |
| Observable | The whole batch is refused with `InvalidOperation("slot N is owned by X, not Y")` and no slot changes hands. The owner removing its own slots succeeds. Naming a slot nobody owns refuses the whole batch with `SlotNotAssigned(slot)`. |
| NOT observable | A partially applied batch — the same validate-all-then-apply shape as FM-CLUSTER-003. Nor a node's slots being unassigned out from under it by a command naming a different node: every key in them would answer `CLUSTERDOWN` until someone re-assigned them. |
| Invariant | The validation pass asserts both that each slot has an owner and that the owner is `node_id` (`commands.rs:142-170`), matching the strictness of every sibling slot-mutating arm (`AssignSlots`, `BeginSlotMigration`, `CompleteSlotMigration`). Redis' `CLUSTER DELSLOTS` is a *local* unassignment with no node argument, so ownership is structural there; FrogDB's is replicated and carries an explicit id, so the assertion has to be made rather than assumed. The server-layer handler only ever fills `node_id` from `ctx.node_id`, so the check is invisible to a well-formed `CLUSTER DELSLOTS`. |
| Outcome variant | `ClusterError::{SlotNotAssigned, InvalidOperation}` |
| Forced by | `test_remove_slots_success`, `test_remove_slots_not_assigned`, `remove_slots_rejects_a_slot_owned_by_another_node`, `remove_slots_rejects_the_whole_batch_when_one_slot_is_foreign` |
| Bug refs | fixed: [33-remove-slots-is-owner-blind.md](../issues/done/33-remove-slots-is-owner-blind.md) |

## FM-CLUSTER-005 — a replica role always names a primary that exists

| Field | Value |
|---|---|
| Trigger | `CLUSTER REPLICATE <node>`, or any `SetRole` proposal. |
| Observable | `SetRole{Replica}` with no `primary_id` is `InvalidOperation("replica requires a primary_id")`; with a `primary_id` naming a node that is not a member it is `NodeNotFound(pid)`. Only a fully resolvable pairing is recorded. |
| NOT observable | A replica recorded against a primary the cluster has never heard of — its data path would have nothing to attach to and `CLUSTER NODES` would render a dangling master id. |
| Invariant | Both checks run before any mutation (`commands.rs:172-182`), so a rejected `SetRole` leaves role, parent, and events untouched. |
| Outcome variant | `ClusterError::{InvalidOperation, NodeNotFound}` |
| Forced by | `test_set_role`, `test_set_role_node_not_found`, `test_set_role_replica_without_primary`, `test_set_role_replica_primary_not_found` |
| Bug refs | — |

## FM-CLUSTER-006 — `CLUSTER RESET` SOFT keeps this node's identity, HARD mints a new one

| Field | Value |
|---|---|
| Trigger | `CLUSTER RESET [SOFT|HARD]` on a node with an empty keyspace (a non-empty one is refused at the command layer before it reaches Raft). |
| Observable | Both forms clear the slot map, clear all migrations, forget every peer, and force this node to `Primary` with no parent. SOFT keeps the node id and both epochs. HARD re-keys the node under a freshly minted id and zeroes both the cluster counter and the node's own epoch. A reset naming a node that is not a member still clears everything and answers `Ok`. On a follower the reset is forwarded to the leader, or answered `REDIRECT`/`CLUSTERDOWN` if it cannot be — reset proposes through `ClusterWriter` like every other admin command (FM-CLUSTER-047..050). |
| NOT observable | A reset that fails: the arm is infallible by construction, because a half-reset node is worse than a fully reset one. Nor a raw `ERR Raft error: APIError(ForwardToLeader(..))` on a follower — the `Debug` rendering of an openraft internal is not a redirect any client can follow. Nor a node that re-keyed itself locally after a reset that never landed. |
| Invariant | `commands.rs:478-518` clears the slot map and migrations unconditionally before it branches, so no slot or migration can survive either form. `ClusterWriter::propose_reset` snapshots the peer list *before* proposing (a committed reset empties the topology), routes through `propose`, and applies the local identity update only on `Applied`. |
| Outcome variant | `ClusterResponse::Ok` / `ResetProposed::{Applied, Rejected}` / `ProposeError::{Redirect, Raft}` |
| Forced by | `test_reset_cluster_soft`, `test_reset_cluster_hard`, `test_reset_cluster_nonexistent_node`, `reset_on_a_follower_yields_a_redirect_not_a_raft_error`, `reset_forwarded_to_the_leader_reports_forwarded`, `reset_committed_on_the_leader_keeps_a_soft_reset_identity`, `reset_rejected_by_the_state_machine_changes_nothing_local`, `self_node_id_treats_zero_as_unset`, `from_snapshot_restores_the_table_and_keeps_the_local_identity` |
| Bug refs | fixed: [39-cluster-reset-bypasses-the-cluster-writer.md](../issues/done/39-cluster-reset-bypasses-the-cluster-writer.md) |

`CLUSTER RESET HARD` is why the config epoch is **not** globally monotonic — see FM-CLUSTER-010's
non-guarantee. It also changes the node's identity inside replicated state; the local
`self_node_id` atomic is updated by `propose_reset` on the `Applied` path — including the forwarded
one, because the entry that re-keyed this node was applied on the leader and a follower still
announcing its old id would contradict the topology it is about to receive. The connection layer
keeps only the transport cleanup (`network_factory.remove_node`), which is the piece the cluster
crate cannot reach.

## FM-CLUSTER-007 — one function owns the bootstrap slot split

| Field | Value |
|---|---|
| Trigger | Bootstrapping a cluster, through either the local-seed path or the Raft-replicated path. |
| Observable | With N nodes each gets a contiguous `16384/N`-slot range in the order the ids were given; the last node absorbs the remainder, so every one of the 16384 slots is owned exactly once. Position 0 always owns the range starting at slot 0, whatever the numeric ids are. Zero nodes yields no ranges. |
| NOT observable | A gap or an overlap in the split — either one is a keyspace a client can address and no node will serve. Nor a different split depending on which bootstrap path ran. |
| Invariant | `even_slot_ranges` is the single source both paths call, and `test_even_slot_ranges_cross_path_equivalence` compares the two command sequences directly rather than trusting the call sites. |
| Outcome variant | n/a |
| Forced by | `test_even_slot_ranges_partition_correctness`, `test_even_slot_ranges_order_preservation`, `test_even_slot_ranges_exact_boundaries`, `test_even_slot_ranges_empty`, `test_even_slot_ranges_cross_path_equivalence` |
| Bug refs | — |

## FM-CLUSTER-008 — a rolling upgrade cannot be finalized while a straggler is behind

| Field | Value |
|---|---|
| Trigger | `FinalizeUpgrade{version}` while some member is still running an older binary, a pre-versioning binary (empty version string), or an unparseable one. |
| Observable | `InvalidOperation` naming the offending node and its version; `active_version` is unchanged, so every version-gated feature stays off. An unparseable *target* is `InvalidOperation("invalid target version …")` whether or not the cluster has members. With every node at or above the target it succeeds, and re-finalizing the same version succeeds again. A cluster with no members at all accepts a valid target (bootstrap). |
| NOT observable | A finalization that half-applies — the check loop runs to completion before `active_version` is written. Nor a rejection of a node that is *ahead* of the target: a mixed cluster mid-upgrade is the normal state. Nor a target version that no gate can parse being accepted: `is_gate_active_in` reads an unparseable active version as *inactive* (FM-CLUSTER-009), so storing one silently disables every gate, and the operation is documented irreversible. |
| Invariant | The target is semver-parsed **before** the per-node loop it is invariant over (`commands.rs:456-470`), so the zero-member case — where the loop body never runs — is validated by the same parse as every other case. Every node is then parsed and compared before `active_version` is set. |
| Outcome variant | `ClusterError::InvalidOperation` |
| Forced by | `test_finalize_upgrade_succeeds_when_all_nodes_at_target`, `test_finalize_upgrade_rejects_when_node_behind`, `test_finalize_upgrade_rejects_empty_version_node`, `test_finalize_upgrade_allows_nodes_ahead_of_target`, `test_finalize_upgrade_invalid_target_version`, `test_finalize_upgrade_idempotent`, `finalize_upgrade_rejects_an_unparseable_target_with_no_nodes`, `finalize_upgrade_with_no_nodes_accepts_a_valid_target` |
| Bug refs | fixed: [34-finalize-upgrade-accepts-garbage-with-no-nodes.md](../issues/done/34-finalize-upgrade-accepts-garbage-with-no-nodes.md) |

## FM-CLUSTER-009 — a version gate refuses until finalization raises the active version

| Field | Value |
|---|---|
| Trigger | Asking whether a gated feature may run, in any state of the rolling upgrade. |
| Observable | The gate is active only when `active_version` parses and is at or above the gate's minimum. No active version, an unparseable active version, and an unknown gate name all report inactive. `pending_gates` lists what a given target would unlock, excluding what is already active, and is empty for a patch bump that unlocks nothing. |
| NOT observable | A gate defaulting to *on* in any ambiguous state — a feature that half the cluster cannot parse is the failure this exists to prevent. An unparseable target version yields an empty pending list rather than an error, so the caller never treats a parse failure as "everything is unlocked". |
| Invariant | `is_gate_active_in` is a pure lookup with a single `>=` comparison and `find(..).is_some_and(..)`, so every refusal path collapses to `false` (`version_gate.rs:47-75`). |
| Outcome variant | `bool` / `Vec<&VersionGateEntry>` |
| Forced by | `gate_active_when_version_meets_minimum`, `gate_inactive_when_version_below_minimum`, `gate_inactive_when_no_active_version`, `none_active_version_means_inactive`, `invalid_active_version_means_inactive`, `no_gates_means_nothing_active`, `static_gate_active_when_version_sufficient`, `pending_gates_static_registry`, `pending_gates_returns_unlockable_gates`, `pending_gates_excludes_already_active`, `pending_gates_empty_for_patch_upgrade`, `gate_suppression_is_reported_once_and_only_when_suppressed` |
| Bug refs | — |

---

## FM-CLUSTER-010 — the cluster counter never sits below any node's epoch

| Field | Value |
|---|---|
| Trigger | Any sequence of `AddNode` / `IncrementEpoch` / `Failover` / `MarkNodeFailed` / reset. |
| Observable | `cluster_current_epoch` in `CLUSTER INFO` is always at least the largest `config_epoch` in `CLUSTER NODES`, and no two *primaries* share a nonzero epoch. A collision mint always lands strictly above the counter's previous value. |
| NOT observable | Two primaries claiming the same nonzero epoch — the state in which neither can be said to have superseded the other. Nor a mint that produces a value some node already holds. |
| Invariant | `mint_config_epoch` is `counter.max(max_node_epoch()) + 1` (`state.rs:261-264`), so the mint reads the node epochs rather than trusting the counter, and the uncontested branch of `reconcile_incoming_epoch` ratchets the counter up to any accepted claim. |
| Outcome variant | `EpochReconciliation::{Accepted, Reassigned}` |
| Forced by | `test_config_epoch_counter_dominates_every_node_epoch_across_command_sequence`, `test_add_node_collision_mints_above_cluster_counter`, `test_add_node_uncontested_epoch_raises_cluster_counter`, `max_node_epoch_tracks_the_highest_claim` |
| Bug refs | — |

Deliberate non-guarantee: the epoch is **not** globally monotonic across `CLUSTER RESET HARD`, which
zeroes the counter (FM-CLUSTER-006). Assertions of the form `cluster_current_epoch` only ever
increases are wrong; the invariant is the dominance relation above, within a cluster's lifetime.

## FM-CLUSTER-011 — an epoch collision is resolved against the node that arrives

| Field | Value |
|---|---|
| Trigger | An `AddNode` for a primary whose claimed nonzero `config_epoch` is already held by a *different* primary. |
| Observable | The incumbent keeps the contested epoch; the arriving node is stamped with a freshly minted one and a warning is logged. A node re-claiming *its own* recorded epoch is an ordinary upsert, not a collision. A *replica* claiming a primary's epoch is not a collision either. |
| NOT observable | The incumbent being renumbered — the node that is already serving the slot map at that epoch is the one whose authority must not move. Nor a rejection: `AddNode` never refuses on epoch grounds, because refusing membership over a numbering conflict partitions the cluster instead of healing it. |
| Invariant | The collision test requires `node.is_primary()` and a *different* id at the same epoch (`state.rs:296-300`), matching Redis' `clusterHandleConfigEpochCollision`, which likewise renumbers only one side and exempts replicas. |
| Outcome variant | `EpochReconciliation::Reassigned{claimed, assigned}` |
| Forced by | `test_add_node_epoch_collision_reassigns_incoming_node`, `test_add_node_self_epoch_is_not_a_collision`, `test_add_node_replica_sharing_primary_epoch_is_not_a_collision` |
| Bug refs | — |

## FM-CLUSTER-012 — epoch 0 means "unassigned" and is preserved, never contested

| Field | Value |
|---|---|
| Trigger | A node registering with `config_epoch: 0` — the value `NodeInfo::new_primary` produces, i.e. every fresh boot. |
| Observable | If the cluster already holds a nonzero epoch for that node id, the recorded value is preserved and the incoming 0 is discarded (the address is still refreshed). If it does not, 0 is accepted and the cluster counter is left alone. Two nodes may both sit at 0. |
| NOT observable | A restart resetting a node's epoch to 0 and thereby forfeiting its authority. Nor two nodes at 0 being treated as a collision and burning a mint on every boot. |
| Invariant | The zero branch of `reconcile_incoming_epoch` runs *before* the collision test and returns early (`state.rs:286-292`), so 0 can neither collide nor move the counter. |
| Outcome variant | `EpochReconciliation::{Preserved, Accepted}` |
| Forced by | `test_add_node_zero_epoch_is_not_a_collision`, `test_add_node_zero_epoch_preserves_recorded_epoch` |
| Bug refs | — |

## FM-CLUSTER-013 — flagging a node FAIL and bumping the epoch is one transition

| Field | Value |
|---|---|
| Trigger | The leader's reconciliation writes `MarkNodeFailed{node}`. |
| Observable | `CLUSTER NODES` shows the `fail` flag and `CLUSTER INFO` shows a raised `cluster_current_epoch`, together, in the entry that carried them. Marking an unknown node is `NodeNotFound` and moves no epoch. The response is `Ok`, not `Epoch`. |
| NOT observable | A FAIL flag visible at a stale epoch — a peer that read the flag but not the bump would treat the failed node's slot claims as still current. That is exactly what a separate `IncrementEpoch` entry would allow if the leader crashed between the two. |
| Invariant | The arm sets `flags.fail` and increments `config_epoch` inside one `apply_command` write lock (`commands.rs:335-348`), so no reader can observe one without the other. |
| Outcome variant | `ClusterResponse::Ok` / `ClusterError::NodeNotFound` |
| Forced by | `test_mark_node_failed`, `test_mark_node_failed_nonexistent`, `test_mark_node_failed_bumps_epoch_atomically`, `test_mark_node_failed_missing_node_does_not_bump_epoch` |
| Bug refs | — |

## FM-CLUSTER-014 — recovery clears both failure flags and moves no epoch

| Field | Value |
|---|---|
| Trigger | The leader's reconciliation writes `MarkNodeRecovered{node}` after the peer answers probes again. |
| Observable | Both `fail` and `pfail` are cleared. `cluster_current_epoch` is unchanged. An unknown node is `NodeNotFound`. |
| NOT observable | An epoch bump on recovery. The asymmetry with FM-CLUSTER-013 is deliberate: marking a node down changes who may claim its slots, so it needs a new epoch; welcoming it back changes nothing about authority, and bumping there would let a flapping peer churn the epoch once per round. |
| Invariant | The arm clears both flags and touches nothing else (`commands.rs:350-359`); the anti-churn guarantee upstream of it is the detector's hysteresis (FM-CLUSTER-052) plus level-triggered reconciliation, which performs zero writes on a converged cluster. |
| Outcome variant | `ClusterResponse::Ok` / `ClusterError::NodeNotFound` |
| Forced by | `test_mark_node_recovered`, `test_mark_node_recovered_nonexistent` |
| Bug refs | — |

`pfail` is structurally supported everywhere (flags, health counting, `CLUSTER NODES` rendering) but
is **never produced** — FrogDB has no gossip suspicion phase, so nothing sets it and only
`MarkNodeRecovered` clears it. Rows 073 and 071 pin the rendering anyway, so a future suspicion
phase inherits a specced surface.

## FM-CLUSTER-015 — a proposal's result carries its type, not a string

| Field | Value |
|---|---|
| Trigger | Any `ClusterCommand` applied through Raft. |
| Observable | `IncrementEpoch` answers `ClusterResponse::Epoch(n)` with the new value; a rejected command answers `ClusterResponse::Error(e)` carrying the typed `ClusterError`; everything else answers `Ok`. All three survive a JSON round trip across the apply boundary. |
| NOT observable | An error flattened to a string before it crosses the state-machine boundary — the caller could no longer distinguish `SlotAlreadyAssigned` from `NodeNotFound` without parsing prose. |
| Invariant | `ClusterResponse` is openraft's `R` type, so the typed value is what `client_write` returns; `as_error()` is the only accessor and it matches a single variant. |
| Outcome variant | `ClusterResponse::{Ok, Epoch, Error}` |
| Forced by | `test_increment_epoch`, `increment_epoch_returns_typed_epoch`, `test_cluster_response_as_error`, `test_cluster_response_serde_round_trip`, `non_event_command_returns_no_events` |
| Bug refs | — |

## FM-CLUSTER-016 — `CLUSTER INFO` reports the replicated counter verbatim and the Raft term separately

| Field | Value |
|---|---|
| Trigger | `CLUSTER INFO` on any node. |
| Observable | `cluster_current_epoch` is the replicated counter exactly as stored — a bump is visible even while the Raft term is higher. `cluster_my_epoch` is *this node's* `NodeInfo::config_epoch`, which is generally not the counter. `cluster_raft_term` is a FrogDB extension reported as its own field; on a standalone node the line is omitted entirely. |
| NOT observable | The historical `max(counter, raft_term)` fold, which made an election look like an epoch bump and hid a real bump under a higher term. Nor `cluster_raft_term:0` on a node with no Raft — a field with no meaning reported as a number is worse than an absent field. |
| Invariant | `ClusterInfoReport` takes the counter and the term as separate inputs and renders them as separate lines, with the term as an `Option` so absence is representable. |
| Outcome variant | `CLUSTER INFO` fields |
| Forced by | `test_cluster_info_reports_config_epoch_counter_verbatim`, `test_cluster_info_reports_raft_term_as_its_own_field`, `test_cluster_info_epoch_bump_is_visible_under_a_higher_term`, `test_cluster_info_my_epoch_is_the_per_node_value_not_the_counter`, `test_cluster_info_standalone_omits_the_raft_term_line` |
| Bug refs | — |

## FM-CLUSTER-017 — cluster metadata survives a restart through the log and through a snapshot

| Field | Value |
|---|---|
| Trigger | Restarting a node, with and without a durable state-machine snapshot, and with the log purged behind the snapshot. |
| Observable | Replaying the log into a fresh state machine reproduces the config epoch with no election involved. With a snapshot on disk and an empty log, the epoch and `last_applied` are still correct and the snapshot is what `get_current_snapshot` advertises. A store with nothing persisted advertises nothing rather than synthesizing from live state. A save whose `last_log_id` is at or below the stored one is a silent no-op. |
| NOT observable | A snapshot going backwards, or a snapshot openraft believes exists that is not on disk — openraft purges log entries on the strength of an advertised snapshot, so a synthesized one would discard entries nothing can reproduce. |
| Invariant | `ClusterSnapshotStore::save` compares `last_log_id.index` under a mutex and writes meta plus payload in one `WriteBatch` with `set_sync(true)`; `get_current_snapshot` reads the store rather than the live state whenever a store is attached (`storage.rs:111-147`, `state.rs:808-837`). |
| Outcome variant | `StorageError` / `Option<Snapshot>` |
| Forced by | `test_config_epoch_round_trips_through_storage_restart`, `test_state_machine_snapshot_survives_restart_without_log_replay`, `test_snapshot_save_never_moves_backwards`, `test_snapshot_store_is_opt_in`, `test_storage_open_and_close`, `test_storage_vote`, `test_storage_metadata`, `test_storage_committed`, `membership_entries_are_recorded_for_applied_state`, `snapshot_builder_shares_the_live_state`, `begin_receiving_snapshot_hands_back_an_empty_buffer`, `log_keys_round_trip_and_sort_in_index_order`, `save_committed_writes_then_deletes_the_persisted_key`, `cache_evicts_the_oldest_only_once_over_the_bound`, `cache_invalidation_respects_both_range_ends`, `excluded_start_bound_skips_that_index`, `append_persists_entries_and_log_state_reports_the_tail`, `truncate_drops_only_the_tail_after_the_kept_index` |
| Bug refs | — |

## FM-CLUSTER-076 — `SET-CONFIG-EPOCH` assigns the exact value, and only at bootstrap

Numbered out of sequence because it was added after the first pass; it belongs to the config-epoch
group (010..017) and sits here rather than at the end of the file.

| Field | Value |
|---|---|
| Trigger | `CLUSTER SET-CONFIG-EPOCH <n>` on any node, at any point in its life. |
| Observable | On a node that knows no other member and whose own `config_epoch` is still 0, the node's epoch becomes exactly `n` and `cluster_current_epoch` is ratcheted up to `n` if it was lower. Otherwise the command is refused: `InvalidOperation` naming which guard fired, or `NodeNotFound` if the receiving node is not in the topology. A refusal moves no epoch. |
| NOT observable | Success reported for an assignment that did not happen — the previous behavior parsed `n`, discarded it, and issued a plain `IncrementEpoch`, so a bootstrap script asking for epoch 100 got epoch 1 and a `+OK`. Nor a live cluster renumbering a node by hand: FrogDB resolves collisions through FM-CLUSTER-011, so a manual assignment mid-life could only manufacture one. Nor the counter following an assignment *downwards* (FM-CLUSTER-010). |
| Invariant | The `SetConfigEpoch` arm applies Redis' two `clusterCommand` guards (`dictSize(server.cluster->nodes) > 1`, `myself->configEpoch != 0`) before mutating, then sets the field and takes `config_epoch.max(epoch)` on the counter (`commands.rs:238-272`). The server handler is a pure adapter: it parses the argument and emits `RaftClusterOp::SetConfigEpoch { node_id: ctx.node_id, epoch }`, so the value can no longer be dropped between parse and apply. |
| Outcome variant | `ClusterResponse::Epoch` / `ClusterError::{InvalidOperation, NodeNotFound}` |
| Forced by | `set_config_epoch_assigns_the_exact_value_requested`, `set_config_epoch_never_lowers_the_cluster_counter`, `set_config_epoch_refused_once_the_node_knows_another_node`, `set_config_epoch_refused_once_the_node_holds_an_epoch`, `set_config_epoch_on_an_unknown_node_is_not_found`, `test_cluster_set_config_epoch_assigns_the_exact_value_on_a_lone_node`, `test_cluster_set_config_epoch_refused_once_the_node_knows_a_peer` |
| Bug refs | fixed: [38-set-config-epoch-discards-its-argument.md](../issues/done/38-set-config-epoch-discards-its-argument.md) |

---

## FM-CLUSTER-018 — a key's slot is its hash tag's slot, and a slot never spans shards

| Field | Value |
|---|---|
| Trigger | Any keyed command in cluster mode. |
| Observable | `CLUSTER KEYSLOT` and the routing layer agree: the slot is CRC16-XMODEM of the hash tag when one is present and of the whole key otherwise, mod 16384. Keys sharing a hash tag share a slot, and keys sharing a slot always share a local shard. |
| NOT observable | Two keys in the same slot landing on different shards — every cross-key guarantee in the codebase (`MULTI`, scripts, multi-key commands) is built on slot co-location implying shard co-location. |
| Invariant | Shard selection is `slot_for_key(key) % num_shards`, derived from the same slot, so the implication holds by construction rather than by agreement between two hashes; `same_slot_implies_same_shard_over_many_keys` sweeps it rather than spot-checking. |
| Outcome variant | n/a |
| Forced by | `same_shard_single_key_co_locates`, `same_slot_single_key_co_locates`, `hash_tagged_keys_co_locate`, `same_slot_implies_same_shard_over_many_keys`, `accepts_borrowed_slice_keys` |
| Bug refs | — |

## FM-CLUSTER-019 — a keyless command is never routed

| Field | Value |
|---|---|
| Trigger | A command with no keys — `PING`, `INFO`, a `MULTI` block containing only keyless commands. |
| Observable | It is served locally on whichever node received it. Slot validation returns "no verdict" rather than a slot. |
| NOT observable | `CLUSTERDOWN` or `MOVED` for a command that names no key: there is no slot to be wrong about, and refusing keyless traffic would make an unassigned slot map take the whole node offline for administration. |
| Invariant | Both validators return `Ok(None)` on an empty key set before computing anything (`validator.rs`), and `route_queued_batch` serves locally when the batch contributed no keys. |
| Outcome variant | `Option<Response>` = `None` / `RouteOutcome::ServeLocal` |
| Forced by | `same_shard_empty_is_none`, `same_slot_empty_is_none`, `batch_with_no_keyed_command_serves_locally` |
| Bug refs | — |

## FM-CLUSTER-020 — keys in two slots are refused, never partially served

| Field | Value |
|---|---|
| Trigger | A multi-key command, a `MULTI` batch, or a `WATCH` set spanning two slots. |
| Observable | `CROSSSLOT Keys in request don't hash to the same slot`, with the identical wire string from all three entry points. |
| NOT observable | The first slot's keys being served and the rest redirected — a partially executed multi-key command has no correct client recovery. |
| Invariant | All three paths funnel through the same seam string; the batch and watch routers compute the slot set first and refuse before any per-slot verdict is taken. |
| Outcome variant | `CROSSSLOT` error |
| Forced by | `cross_slot_pair_rejects_with_seam_string`, `batch_spanning_two_slots_is_crossslot`, `watch_across_two_slots_is_crossslot` |
| Bug refs | — |

## FM-CLUSTER-021 — an owned, stable slot is served locally

| Field | Value |
|---|---|
| Trigger | A key whose slot this node owns, with no migration open on it. |
| Observable | The command executes here. No redirect of any kind is emitted, with or without `ASKING`, with or without `READONLY`. |
| NOT observable | A redirect on the happy path — the cost of a spurious `MOVED` is a client-side topology refresh on every request. |
| Invariant | `route_with_snapshot` is a pure function of `(snapshot, self id, slot, asking, is_restore)`; the local-serve arm is taken before any migration or ownership branch (`routing.rs:133-168`). |
| Outcome variant | `RouteDecision::LocalServe` → `RouteOutcome::ServeLocal` |
| Forced by | `route_local_serve_when_self_owns_no_migration`, `to_response_local_arms_serve_locally`, `batch_on_owned_stable_slot_serves_locally` |
| Bug refs | — |

## FM-CLUSTER-022 — `MOVED` names the owner's client address

| Field | Value |
|---|---|
| Trigger | A key whose slot another node owns. |
| Observable | `MOVED <slot> <host>:<port>`, where the address is the owner's *client* address, not its bus address. An IPv6 owner is rendered bracketed so the client's `host:port` split is unambiguous. The same verdict is reached for a single command and for a queued `MULTI` batch. |
| NOT observable | A `MOVED` naming the cluster bus port — the client would connect to a port that speaks postcard-framed RPC, not RESP. |
| Invariant | The address comes from `NodeInfo::addr` (the client address) and never from `cluster_addr`; rendering goes through one `to_response` so single and batch paths cannot diverge. |
| Outcome variant | `RouteDecision::Moved` → `MOVED` |
| Forced by | `route_moved_when_other_node_owns_no_asking`, `to_response_moved_with_addr_emits_moved`, `to_response_moved_ipv6_is_bracketed`, `batch_on_foreign_slot_is_moved_to_the_owner` |
| Bug refs | `.scratch/hardening/issues/open/31-raft-suite-queued-txn-moved-redirect-and-key-routing-wrong-value-reads.md` |

## FM-CLUSTER-023 — an owner this node cannot address degrades to `CLUSTERDOWN`

| Field | Value |
|---|---|
| Trigger | The slot has an owner id, but that id is not in this node's view of cluster state — a topology entry the local snapshot has not caught up with. |
| Observable | `CLUSTERDOWN`, not a `MOVED`. |
| NOT observable | A `MOVED` with an empty or fabricated address. A client that follows a malformed redirect fails in the client library rather than at the protocol level, which is far harder to diagnose than an explicit `CLUSTERDOWN`. |
| Invariant | `RouteDecision::Moved` carries an `Option<SocketAddr>`, so "known owner, unknown address" is representable and `to_response` maps the `None` arm to `CLUSTERDOWN` rather than formatting a hole. |
| Outcome variant | `RouteDecision::Moved(None)` → `CLUSTERDOWN` |
| Forced by | `route_moved_with_no_addr_when_owner_node_unknown`, `to_response_moved_without_addr_emits_clusterdown` |
| Bug refs | `.scratch/hardening/issues/done/30-self-fence-flake-and-misleading-clusterdown.md` |

## FM-CLUSTER-024 — an unassigned slot is `CLUSTERDOWN`

| Field | Value |
|---|---|
| Trigger | A key whose slot nobody owns — mid-bootstrap, after a `FORGET` (FM-CLUSTER-002), or during a reset. |
| Observable | `CLUSTERDOWN` from the single-command path, the queued-batch path, and `WATCH` registration alike. |
| NOT observable | The receiving node serving the key on the grounds that nobody else claims it — that is how two nodes end up owning the same keyspace. |
| Invariant | `Unassigned` is a distinct `RouteDecision` variant rather than a `Moved` with no target, so it cannot be rescued by the `READONLY` branch (FM-CLUSTER-025). |
| Outcome variant | `RouteDecision::Unassigned` → `CLUSTERDOWN` |
| Forced by | `route_unassigned_when_no_owner_no_migration`, `to_response_unassigned_emits_clusterdown`, `batch_on_unassigned_slot_is_clusterdown`, `watch_on_an_unassigned_slot_is_clusterdown` |
| Bug refs | — |

## FM-CLUSTER-025 — `READONLY` rescues a `MOVED`, never an unassigned slot

| Field | Value |
|---|---|
| Trigger | A `READONLY` connection on a replica reading a slot owned elsewhere, and the same connection issuing a batch that contains a write. |
| Observable | A read-only batch on a foreign slot is served locally. A batch containing any write is not — it takes the ordinary `MOVED`. A `READONLY` connection addressing an *unassigned* slot still gets `CLUSTERDOWN`. |
| NOT observable | A write accepted on a node that does not own the slot because the connection happened to be in `READONLY` mode — that is an acked write on the wrong node, the same class as FM-CLUSTER-030. Nor `READONLY` manufacturing an owner for a slot that has none. |
| Invariant | `to_response(readonly_eligible)` applies the override to the `Moved` arm only, and eligibility is computed from the batch's own write flags rather than from the connection state alone (`routing.rs:83-103`). |
| Outcome variant | `RouteDecision::Moved` + `readonly_eligible` → `ServeLocal` |
| Forced by | `to_response_moved_readonly_eligible_serves_locally`, `to_response_unassigned_ignores_readonly_override`, `batch_readonly_eligible_serves_a_foreign_slot_locally`, `batch_readonly_ineligible_when_it_contains_a_write`, `batch_readonly_never_rescues_an_unassigned_slot` |
| Bug refs | — |

## FM-CLUSTER-026 — `ASKING` is the only key to an importing slot

| Field | Value |
|---|---|
| Trigger | A client addressing the *target* of an open migration, with and without a preceding `ASKING`. |
| Observable | With `ASKING`, the target accepts the command. Without it the client is sent back to the current owner with `MOVED` — or `CLUSTERDOWN` when the slot has no owner yet. The same holds for a queued `MULTI` batch, where `ASKING` is sticky for the whole block. |
| NOT observable | An importing node serving un-`ASKING`ed traffic. Until the migration commits, the source is still the authority; serving here would let two nodes answer for one slot with no ordering between them. |
| Invariant | The importing arms of `route_with_snapshot` are all gated on the `asking` flag (or on `is_restore`, FM-CLUSTER-027); nothing else opens them. |
| Outcome variant | `RouteDecision::{AcceptImporting, Moved, Unassigned}` |
| Forced by | `route_accept_importing_when_self_is_target_and_asking`, `route_moved_when_self_is_target_but_no_asking`, `route_unassigned_when_self_is_target_but_no_asking_or_restore`, `route_accept_importing_when_unassigned_self_target_with_asking`, `batch_on_import_target_with_asking_probes_importing`, `batch_on_import_target_without_asking_is_moved` |
| Bug refs | — |

## FM-CLUSTER-027 — `RESTORE` crosses into an importing slot without `ASKING`

| Field | Value |
|---|---|
| Trigger | The `RESTORE` half of a `MIGRATE`, landing on the migration target. |
| Observable | Accepted as importing even though the connection never sent `ASKING`. |
| NOT observable | The data-moving half of a migration being redirected back to the source — the migration would never make progress and `MIGRATE` would loop. |
| Invariant | `is_restore` is a distinct input to `route_with_snapshot`, so the exemption is one named flag on one command rather than a general relaxation of FM-CLUSTER-026. |
| Outcome variant | `RouteDecision::AcceptImporting` |
| Forced by | `route_accept_importing_for_restore_without_asking` |
| Bug refs | — |

## FM-CLUSTER-028 — the migrating source serves what it still holds and `ASK`s what has moved

| Field | Value |
|---|---|
| Trigger | A keyed command on a slot this node still owns while a migration off it is open — at **any arity**, plain or scripted, inside or outside `MULTI`. `MIGRATE` deletes each key as it hands it over, so "we own the slot" and "we hold the key" are different questions for the whole duration of the window. |
| Observable | Key presence decides, and it is consulted **before the command executes**: every key still here → served locally; every key already gone → `-ASK <slot> <importing node>`; some here and some gone → `-TRYAGAIN`. A probe the shard does not answer → `ERR shard unavailable`. The single-key path, the scripting family, and the EXEC/batch path give the same answer for the same key set. If the importing node's address is unknown, the source serves locally rather than emitting a redirect it cannot address. A node that is the migration *source* but not the current owner takes the ordinary `MOVED`. Node-scoped commands (`SCAN`, `MIGRATE`, `CLUSTER`, the connection-level families) are never probed — no slot owns them, so no migration can redirect them. |
| NOT observable | A write to an already-migrated key answered `+OK` on the source. That re-creates the key behind the migration, and `CLUSTER SETSLOT <slot> NODE` then hands the slot away and destroys the acknowledged write — 11 acked writes lost in a fault-free cluster in the run issue 40 was filed from. Nor a reply-shape stand-in for presence: a nil reply does not mean the key is gone (`HGET` on a live hash, `LPOS` on a live list, a `BLPOP` timeout are all nil) and a non-nil reply does not mean it is here (`SET` answers `+OK` on the key it just re-created), so "convert nil replies to `ASK` after execution" is both over- and under-inclusive and is unsound for writes by construction. Nor the mirror-image over-correction — a blanket refusal of a migrating slot: Redis serves what it still holds, and a blanket `TRYAGAIN` would stall the slot for the migration's whole duration. |
| Invariant | One decision, one site. `route_migrating_source` (`server/src/slot_migration/routing.rs`) turns "this node owns the slot, a migration off it is open, and the ASK address renders" into a probe instruction, reusing `route_with_snapshot`'s `LocalServeMigrating` arm and the batch path's `migration_target_addr`; both callers — the per-command `MigratingSourceProbe` stage and `validate_queued_batch` — map the shared `probe_key_presence` verdict through the single `KeyPresence::migrating_source_reply`. Arity is not a gate anywhere on that path. The probe is ordered after `ClusterSlotValidation` and before `ConnectionCommand` (`MUST_PRECEDE`), so the scripting family cannot outrun it the way FM-CLUSTER-030 describes; it reads slot state first, so a slot with no open migration costs one snapshot read and no keyspace lookup. |
| Outcome variant | `RouteDecision::LocalServeMigrating` → `KeyPresence` → `None` / `-ASK` / `-TRYAGAIN` / `ERR shard unavailable` |
| Forced by | `route_local_serve_migrating_when_self_is_source`, `route_moved_when_self_is_source_but_other_owns`, `migrating_source_probe_names_the_importing_node`, `migrating_source_probe_is_none_without_an_open_migration`, `migrating_source_probe_is_none_when_the_ask_address_is_unknown`, `batch_on_migrating_source_probes_with_the_ask_target`, `batch_on_migrating_source_with_unknown_target_serves_locally`, `migrating_source_asks_a_single_key_write_whose_key_moved`, `migrating_source_serves_a_single_key_command_still_held_here`, `migrating_source_tryagain_when_the_keys_straddle_the_slot`, `migrating_source_probes_the_scripting_family`, `migrating_source_refuses_when_the_shard_does_not_answer`, `migrating_source_probe_is_skipped_when_the_slot_is_not_migrating`, `migrating_source_probe_is_skipped_for_node_scoped_commands`, `test_single_key_write_on_migrating_source_asks_and_never_orphans` |
| Bug refs | `.scratch/hardening/issues/done/40-single-key-write-on-migrating-source-acks-then-orphans.md` (fixed); issue 31 §"Bug X" (the jepsen forensics it was split out of) |

## FM-CLUSTER-029 — `WATCH` is routed when it is registered, not when `EXEC` runs

| Field | Value |
|---|---|
| Trigger | `WATCH` naming a key on a foreign slot, on two slots, on a slot with an open migration, and on an unassigned slot. |
| Observable | A foreign slot is refused at `WATCH` time with `MOVED`; two slots with `CROSSSLOT`; an unassigned slot with `CLUSTERDOWN`; a slot with an open migration is accepted. |
| NOT observable | A `WATCH` silently accepted on a slot this node does not serve, only for the `EXEC` to fail much later — the client would have built a whole transaction on a watch that was never valid. Nor a `READONLY` rescue: watch routing hard-codes `readonly_eligible = false`, because a watch exists to guard a subsequent write. |
| Invariant | `route_watched_keys` and `watch_slot_is_locally_served` share the routing verdicts with the command path and pass `readonly_eligible = false` explicitly (`routing.rs:317-350`). |
| Outcome variant | `Option<Response>` |
| Forced by | `watch_on_a_foreign_slot_is_refused_with_moved`, `watch_on_an_open_migration_is_accepted`, `watch_slot_locally_served_accepts_an_open_migration`, `watch_slot_locally_served_rejects_a_slot_owned_elsewhere` |
| Bug refs | — |

## FM-CLUSTER-030 — A bare script is redirected, never executed on a non-owner

| Field | Value |
|---|---|
| Trigger | A bare `EVAL` / `EVAL_RO` / `EVALSHA` / `EVALSHA_RO` / `FCALL` / `FCALL_RO` (outside `MULTI`) naming a key owned by another node, sent to the non-owner. |
| Observable | `MOVED <slot> <owner>`, identical to the plain command on the same key — for every member of the family, and before the script body or function ever runs. A script that declares `numkeys 0` (and the keyless `SCRIPT LOAD`) still runs on any node. |
| NOT observable | Any script side effect on a non-owner. Before the fix the non-owner answered `Integer(1)` to a script whose body was `redis.call('SET', KEYS[1], …); return 1` — an acked write on a node that does not own the slot — while the control `GET` on the same key and connection was correctly `MOVED`. That is strictly wider than the finalization window of rework 02/03: not a race, but unconditional for every bare script on a non-owner. Nor the inverse over-correction: a keyless script being assigned a slot and redirected. |
| Invariant | `is_cluster_exempt` refuses to exempt `ConnectionLevelOp::Scripting` (`guards.rs:696-713`), and `PRE_DISPATCH_ORDER` runs `ClusterSlotValidation` *before* `ConnectionCommand`, the stage that dispatches the scripting family. The ordering is pinned structurally by the `MUST_PRECEDE` pair `(ClusterSlotValidation, ConnectionCommand)` (`dispatch.rs`), validated by `load_bearing_ordering_invariants` — so a reorder that compiles but re-opens the hole fails the unit test. The verdict itself has one source: `validate_cluster_slots` → `coordinator.route`, the same seam every keyed command uses. |
| Outcome variant | `RouteDecision::Moved` → `RouteOutcome::Reply(MOVED)` |
| Forced by | `test_bare_eval_on_non_owner_returns_moved`, `test_bare_script_family_on_non_owner_returns_moved`, `test_keyless_script_is_never_redirected`, `load_bearing_ordering_invariants` |
| Bug refs | `.scratch/replication-cluster-rework/issues/done/11-bare-eval-may-skip-cluster-slot-validation.md` (fixed); rework issues 02 and 03 (the narrower finalization-window form) |

Residual, deliberately out of scope here: a script's *undeclared* runtime writes (`redis.call` on a
key the invocation never named) are still unvalidated — Redis re-checks on every call via
`scriptVerifyClusterState`, and that is rework issue 03.

The sibling gap this section used to record — the migrating-source probe sitting *after*
`ConnectionCommand`, so a script against a half-migrated slot was served locally instead of
redirected — is closed. Issue 40 hoisted the stage (now `MigratingSourceProbe`) to the same
position for the same reason, pinned by the `MUST_PRECEDE` pair
`(MigratingSourceProbe, ConnectionCommand)`. The concern that blocked the hoist —
newly subjecting `MIGRATE` (server-wide, `KeySpec::Dynamic`) to the probe — is answered by the
stage sharing `is_cluster_exempt` with slot validation, so every node-scoped command keeps
skipping it. See FM-CLUSTER-028.

---

## FM-CLUSTER-031 — `IMPORTING` and `MIGRATING` converge on one idempotent begin

| Field | Value |
|---|---|
| Trigger | The Redis migration handshake: `SETSLOT <slot> IMPORTING` on the target and `SETSLOT <slot> MIGRATING` on the source, in either order, both replicated through the leader. |
| Observable | Both lower to the same `BeginSlotMigration{slot, source, target}`. The second one finds an identical migration and answers `Ok`. A *different* migration proposed for the same slot is `MigrationInProgress(slot)`. The slot then reports as migrating with the recorded source and target. |
| NOT observable | The second half of the handshake failing merely because the first half already ran — the operator's script would abort a migration that is in fact healthy. Nor a second, conflicting migration silently replacing the first. |
| Invariant | The idempotency check compares source *and* target and runs before the node-existence checks (`commands.rs:367-372`), so the converging pair is recognised as one intent rather than two. |
| Outcome variant | `ClusterResponse::Ok` / `ClusterError::MigrationInProgress` |
| Forced by | `test_slot_migration`, `test_begin_migration_idempotent`, `test_begin_migration_already_in_progress` |
| Bug refs | — |

## FM-CLUSTER-032 — a begin validates both endpoints, and the owner only when the slot is assigned

| Field | Value |
|---|---|
| Trigger | `SETSLOT … IMPORTING/MIGRATING` naming an unknown node, naming a source that does not own the slot, or naming a slot nobody owns yet. |
| Observable | An unknown source or target is `NodeNotFound`. A source that is not the recorded owner is `InvalidOperation("slot N is owned by X, not Y")`. A slot with no recorded owner is accepted. |
| NOT observable | A migration recorded against a node that is not a member — it could never complete, and it would block the slot until someone cancelled it. |
| Invariant | The owner check is conditional on the slot being present in `slot_assignment` (`commands.rs:381-390`), because a follower's slot map may legitimately be empty: bootstrap assigns slots locally, not through Raft. |
| Outcome variant | `ClusterError::{NodeNotFound, InvalidOperation}` |
| Forced by | `test_begin_migration_source_not_found`, `test_begin_migration_target_not_found`, `test_begin_migration_wrong_owner`, `begin_migration_accepts_an_unassigned_slot` |
| Bug refs | — |

## FM-CLUSTER-033 — completing a migration is the one ownership transfer that does not consult the slot map

| Field | Value |
|---|---|
| Trigger | `SETSLOT <slot> NODE <target>` closing an open migration, including onto a slot the map still shows as owned by the source. |
| Observable | Ownership moves to the target and the migration record is removed. With no migration open the command is `InvalidOperation("no migration in progress for slot N")`; with mismatched source or target it is `InvalidOperation("migration parameters don't match")`. |
| NOT observable | A `SlotAlreadyAssigned` refusal at the moment of completion — that guard exists to stop *concurrent claims*, and a completion is the resolution of a claim that was already accepted at begin time. Nor an epoch bump: the ownership swap is authorized by the migration record, not by a new epoch. |
| Invariant | The parameter match against the recorded migration is what makes the unguarded `slot_assignment.insert` safe (`commands.rs:404-436`): only the exact pair that opened the migration may close it. |
| Outcome variant | `ClusterResponse::Ok` / `ClusterError::InvalidOperation` |
| Forced by | `test_complete_migration_no_active`, `test_complete_migration_params_mismatch`, `complete_migration_transfers_ownership_over_an_existing_owner` |
| Bug refs | — |

## FM-CLUSTER-034 — a completed migration emits exactly one event, and a failed one emits none

| Field | Value |
|---|---|
| Trigger | `CompleteSlotMigration`, succeeding and failing; and any non-event command. |
| Observable | Success emits exactly one `SlotMigrationCompleted{slot, source, target}`, delivered on *every* node (unlike role events, which are filtered to self). Failure emits nothing. A begin emits nothing. |
| NOT observable | An event emitted on a rejected command — a consumer would wake blocked clients and redirect them on a migration that never happened. |
| Invariant | Events are pushed only on the `Ok` path of the mutating arm, so emit-on-failure is structurally impossible (`commands.rs:10-17`); the migration event is forwarded without the self-filter because every node's blocked clients need to learn the slot moved. |
| Outcome variant | `ClusterEvent::SlotMigrationCompleted` |
| Forced by | `test_migration_complete_event_fires`, `complete_migration_emits_event_on_success`, `complete_migration_emits_no_event_on_error` |
| Bug refs | — |

## FM-CLUSTER-035 — `SETSLOT … STABLE` cancels whether or not a migration is open

| Field | Value |
|---|---|
| Trigger | `SETSLOT <slot> STABLE`, on a slot with an open migration and on one without. |
| Observable | The slot stops reporting as migrating. Both cases answer `Ok`. |
| NOT observable | An error for cancelling a migration that is not open. `STABLE` is the operator's recovery verb — for a half-finished handshake, for a migration stranded by a `FORGET` (FM-CLUSTER-002), for an aborted script — and a recovery verb that fails when the state is already clean cannot be used blindly. |
| Invariant | The arm is an unconditional `migrations.remove(&slot)` returning `Ok` (`commands.rs:438-442`), so it is idempotent by construction rather than by a presence check. |
| Outcome variant | `ClusterResponse::Ok` |
| Forced by | `test_cancel_slot_migration`, `test_cancel_slot_migration_nonexistent` |
| Bug refs | — |

## FM-CLUSTER-036 — a force failover cancels the migrations of the node it removes

| Field | Value |
|---|---|
| Trigger | An automatic (force) failover of a primary that is the source or target of an open migration, and a graceful failover alongside an unrelated migration. |
| Observable | Migrations naming the removed node are gone. Migrations between two other nodes survive both forms of failover untouched. |
| NOT observable | A migration left pointing at a node that is no longer a member — it can never complete, and its slot stays in a migrating state that nothing will clear except manual intervention. |
| Invariant | The force branch applies `migrations.retain(source != old && target != old)` in the same transition that removes the node (`commands.rs:271-280`), so no window exists in which the node is gone and its migrations are not. |
| Outcome variant | `ClusterResponse::Epoch` |
| Forced by | `test_failover_force_cancels_migrations_of_removed_node`, `test_failover_graceful_keeps_unrelated_migrations`, `force_failover_reports_moved_slots_and_prunes_only_related_migrations` |
| Bug refs | — |

## FM-CLUSTER-037 — the commit-to-apply window is a known, bounded, accepted mode

| Field | Value |
|---|---|
| Trigger | A `CompleteSlotMigration` that commits through Raft microseconds after a batch has already been validated against the cluster snapshot read at `EXEC` entry. |
| Observable | That already-validated batch executes against the *pre-completion* ownership: it serves locally a slot whose ownership has just moved. The next batch, reading a fresh snapshot, is redirected. |
| NOT observable | An unbounded window. The exposure is bounded by Raft apply latency, not by the client's think time — the snapshot is taken at `EXEC` entry, not at `MULTI` entry, so a client that sits in an open transaction for minutes does not widen it. |
| Invariant | Routing is a pure function of an explicitly supplied `ClusterSnapshot` (`route_with_snapshot`, `route_queued_batch`), which is what makes the window a single named read rather than a scatter of live lookups that could each disagree. This is the same window every Redis Cluster node has, and the two-point ownership swap (no intermediate migration state machine) is the deliberate design — see `website/src/content/docs/architecture/clustering.md`. **This row is a documented accepted mode, not a bug.** |
| Outcome variant | n/a |
| Forced by | `batch_on_owned_stable_slot_serves_locally`, `batch_on_foreign_slot_is_moved_to_the_owner` |
| Bug refs | `.scratch/replication-cluster-rework/issues/open/02`, `.scratch/replication-cluster-rework/issues/open/03` (the finalization-window pair — narrowing this window is their subject) |

## FM-CLUSTER-038 — clients blocked on a migrated slot are woken and redirected

| Field | Value |
|---|---|
| Trigger | A `SlotMigrationCompleteEvent` reaching the per-node dispatcher while clients are blocked (`BLPOP` and friends) on keys in that slot. |
| Observable | Blocked clients are woken with `MOVED` to the migration target — or, when this node cannot name the target, with `CLUSTERDOWN Hash slot <n> not served` (the rendering routing already uses for "owner known, address unknown", FM-CLUSTER-023). Both losses of contact — a shard index that does not exist, a shard channel that is closed — are logged at `error`. |
| NOT observable | A client left blocked forever on a slot this node no longer owns — its key can never be written here again, so nothing will ever wake it. In particular an event is never `continue`d past because its target is unresolvable. |
| Invariant | `plan_migration_notice` maps the event to a `MigrationNotice{shard, slot, target_addr: Option<SocketAddr>}` and `run_slot_migration_event_dispatcher` delivers *every* notice (`cluster-runtime/src/migration_events.rs`); the shard renders `MOVED` for `Some` and `CLUSTERDOWN` for `None` (`core/src/shard/blocking.rs:118`). |
| Outcome variant | `MigrationNotice` / `ClusterMsg::SlotMigrated{target_addr: Option<SocketAddr>}` |
| Forced by | `migration_event_with_a_known_target_notifies_the_owning_shard`, `migration_event_with_an_unknown_target_still_wakes_blocked_clients`, `migration_event_routes_to_slot_modulo_num_shards`, `migration_event_reports_a_closed_shard_channel`, `slot_migrated_without_a_known_target_replies_clusterdown` |
| Bug refs | fixed: [35-migration-complete-event-dropped-when-target-unknown.md](../issues/done/35-migration-complete-event-dropped-when-target-unknown.md) |

The dispatcher moved out of `frogdb-server` (`slot_migration/events.rs`, deleted) into the
mutation-gated `frogdb-cluster-runtime`, split into a pure planning function and a delivery step that
reports its own failures. `BlockedMigrationMoved` counts only the `MOVED` wake-ups, so the counter
keeps meaning exactly what its name says; the `CLUSTERDOWN` path is visible in the logs instead.

---

## FM-CLUSTER-039 — a failover validates completely before it mutates anything

| Field | Value |
|---|---|
| Trigger | `Failover` naming an unknown successor, naming the same node twice, or gracefully failing over a primary that is no longer a member. |
| Observable | `NodeNotFound(new)` / `InvalidOperation("failover source and target are the same node")` / `NodeNotFound(old)`. Membership, roles, slot ownership, epochs, and parenting are all exactly as they were. |
| NOT observable | A partially applied failover — slots transferred but no successor promoted, or a promotion with no slots. That is the ownerless-slot window the atomic `Failover` entry exists to close; it is what the old multi-entry `RemoveNode`/`SetRole`/`AssignSlots`/`IncrementEpoch` sequence exposed on a leader crash. |
| Invariant | The arm is strictly two-phase: every check runs first, and from `commands.rs:247` onward the mutation is infallible. A *force* failover of an already-removed node is deliberately allowed, because that is precisely the situation an automatic failover fires in. |
| Outcome variant | `ClusterError::{NodeNotFound, InvalidOperation}` |
| Forced by | `test_failover_validation_failure_mutates_nothing`, `test_failover_same_node_rejected`, `test_failover_graceful_requires_old_node` |
| Bug refs | — |

## FM-CLUSTER-040 — a force failover removes the old primary and transfers everything in one entry

| Field | Value |
|---|---|
| Trigger | An automatic failover (`force: true`) of a failed primary, and the primary-absorbs-primary case. |
| Observable | The old primary is gone from `CLUSTER NODES`; every slot it owned is owned by the successor; the successor is a primary with no parent; the old primary's other replicas are re-parented to the successor; the epoch is bumped once and the successor's `config_epoch` is stamped with it. |
| NOT observable | Any intermediate state in which the failed primary's slots are ownerless, or in which the successor owns slots at a stale epoch. Slot ownership and the epoch that authorizes it propagate together, as in Redis. |
| Invariant | One replicated log entry performs remove, transfer, promote, re-parent, and bump; there is no sequence of entries a crash can land between (`commands.rs:226-333`). |
| Outcome variant | `ClusterResponse::Epoch` |
| Forced by | `test_failover_force_removes_old_and_transfers_everything`, `test_failover_absorb_between_primaries` |
| Bug refs | — |

## FM-CLUSTER-041 — a graceful failover demotes the old primary rather than removing it

| Field | Value |
|---|---|
| Trigger | An operator-initiated `CLUSTER FAILOVER` with the old primary still healthy and reachable. |
| Observable | The old primary stays a member, recorded as a replica of the successor; its slots move to the successor; sibling replicas are re-parented; the epoch is bumped once. |
| NOT observable | A healthy node being evicted from the cluster by a planned failover — it is the new replica, and removing it would force a full re-`MEET` and a full resync. |
| Invariant | The `force` flag selects between remove and demote inside the one atomic entry, so both shapes share the transfer/promote/bump path and cannot drift. |
| Outcome variant | `ClusterResponse::Epoch` |
| Forced by | `test_failover_graceful_demotes_old_primary`, `failover_re_parents_only_the_old_primarys_replicas` |
| Bug refs | — |

## FM-CLUSTER-042 — a replayed failover is safe

| Field | Value |
|---|---|
| Trigger | Log replay, or a retry of the same `Failover` entry after a leader change. |
| Observable | The second application finds the old primary already gone, transfers zero slots, and no-ops the promotion; the end state is coherent (old removed, successor primary, slots owned by the successor). The command and the resulting state both survive a JSON round trip. |
| NOT observable | A replay corrupting the topology — replay is not an exceptional path, it is how a restarting node rebuilds state. |
| Invariant | `force` allows an absent old primary, and promotion is idempotent because it sets rather than toggles. The one non-idempotent effect is the epoch, which is bumped again; that is safe under FM-CLUSTER-010's dominance invariant (bumping is always allowed) and is noted here so a future reader does not mistake it for a bug. |
| Outcome variant | `ClusterResponse::Epoch` |
| Forced by | `test_failover_force_replay_is_safe`, `test_failover_command_serde_roundtrip`, `test_state_snapshot_roundtrip_after_failover` |
| Bug refs | — |

## FM-CLUSTER-043 — role events fire only for this node, and only on a real transition

| Field | Value |
|---|---|
| Trigger | `SetRole` naming this node, naming another node, being rejected, and re-asserting a role the node already holds. |
| Observable | A demotion of self delivers exactly one `Demoted{self, new_primary}`. A role change naming another node delivers nothing here. A rejected `SetRole` delivers nothing. Re-asserting `Primary` on a node that is already primary delivers nothing; re-asserting `Replica` *does* deliver a demotion, because re-parenting must re-point the replication stream even when the role word is unchanged. |
| NOT observable | A promotion event on a replay that changed nothing — the data path would tear down and rebuild its replication link for no reason, and on a restarting node that is a spurious full resync. |
| Invariant | `apply` filters role events against `self_node_id` before forwarding them, and the `Primary` arm emits only when `!was_primary` (`commands.rs:204-216`, `state.rs:688-728`). |
| Outcome variant | `RoleChangeEvent::{Demoted, Promoted}` |
| Forced by | `test_demotion_detection_fires_for_self`, `test_demotion_detection_ignores_other_nodes`, `test_demotion_detection_not_fired_for_rejected_set_role`, `test_promotion_detection_silent_when_already_primary`, `set_role_replica_emits_node_demoted`, `set_role_primary_emits_no_event`, `set_role_self_demotion_emits_no_event_on_error` |
| Bug refs | — |

## FM-CLUSTER-044 — a failover's role events arrive in apply order on one channel

| Field | Value |
|---|---|
| Trigger | A failover round that demotes and promotes within a few entries, on both the old primary and the successor. |
| Observable | A graceful failover emits demotion then promotion, in that order. A force failover emits only the promotion (the old primary was removed, not demoted). A successor that was already a primary is not "promoted" again. A failed failover emits nothing. Each node sees only its own transitions. |
| NOT observable | The two events reordered or split across channels. A promotion applied before the demotion it supersedes leaves the data path a replica of a node that no longer leads; the transitions are not commutative. |
| Invariant | One unbounded channel with one consumer, fed in `apply` order (`state.rs:333-347`), so ordering is a property of the queue rather than of consumer discipline. |
| Outcome variant | `RoleChangeEvent` |
| Forced by | `graceful_failover_emits_node_demoted_for_old_primary`, `force_failover_emits_promotion_only`, `test_role_changes_preserve_apply_order`, `test_promotion_detection_fires_for_failover_successor`, `test_force_failover_still_promotes_the_successor`, `test_demotion_detection_fires_for_graceful_failover_of_self`, `test_demotion_detection_not_fired_for_failed_failover`, `test_demotion_detection_not_fired_for_force_failover`, `test_promotion_detection_fires_for_self_set_role` |
| Bug refs | — |

## FM-CLUSTER-045 — installing a snapshot synthesizes the role change it skipped

| Field | Value |
|---|---|
| Trigger | A node that fell far enough behind to receive a snapshot rather than log entries, where the snapshot's topology gives it a different role than it had. |
| Observable | It emits the promotion or demotion the skipped entries would have produced. A snapshot that does not change this node's role emits nothing. |
| NOT observable | A restored replica booting with `role:master` on its data path. A role folded into a snapshot produces no entry to replay, so without this the transition would be lost silently — the node would serve reads as a primary and never attach to its actual primary. |
| Invariant | `install_snapshot` samples `self_role()` before and after the restore and emits on a difference (`state.rs:787-797`); the persist happens before the in-memory restore, so a snapshot visible in memory is always already on disk. |
| Outcome variant | `RoleChangeEvent` |
| Forced by | `test_install_snapshot_emits_promotion_when_self_role_flipped`, `test_install_snapshot_emits_demotion_when_self_role_flipped`, `test_install_snapshot_silent_when_self_role_unchanged` |
| Bug refs | — |

## FM-CLUSTER-046 — the reconciler re-drives a disagreeing data path, and detaches when nobody is listening

| Field | Value |
|---|---|
| Trigger | The replicated role and the data path's role disagree — after a restore, after a missed event — and the disagreement persists across ticks. Separately: the consumer is dropped at shutdown. |
| Observable | Every tick that finds a disagreement re-emits the correct role and reports `ReDriven(role)`; once the data path agrees it reports `Agreed` and goes silent. With the consumer gone it reports `Detached`. A node that is not a member reports `Agreed` and emits nothing. The reconciler does not exist at all until role-change detection is enabled. |
| NOT observable | A single-shot correction that gives up while the views still disagree, leaving a replica advertising `role:master` indefinitely. Nor the reconciler keeping the consumer — and through it the storage engine and its RocksDB lock — alive past shutdown, which breaks restart-in-process. |
| Invariant | The reconciler holds a *weak* sender, so `Detached` is a real state rather than a send that silently succeeds into a leaked consumer (`state.rs:517-527`). |
| Outcome variant | `RoleReconcile::{Agreed, ReDriven, Detached}` |
| Forced by | `test_reconcile_self_role_demotes_a_restored_replica`, `test_reconcile_self_role_promotes_a_restored_primary`, `test_reconcile_self_role_silent_when_views_agree`, `test_reconcile_self_role_noop_when_self_absent`, `test_reconciler_re_emits_while_the_views_disagree`, `test_reconciler_detaches_once_the_consumer_is_gone`, `test_self_role_reconciler_absent_until_detection_is_enabled` |
| Bug refs | — |

## FM-CLUSTER-047 — a committed proposal may still carry a state-machine rejection

| Field | Value |
|---|---|
| Trigger | A `ClusterCommand` the leader commits successfully but the state machine refuses — `SlotAlreadyAssigned`, `NodeNotFound`, and every other validated arm. |
| Observable | `propose` returns `Ok(Proposed::Committed(ClusterResponse::Error(e)))`. The caller renders `ERR <message>`. A command the state machine accepts returns `Committed(Ok)` and the forwarder is never consulted. |
| NOT observable | A state-machine rejection surfacing as a *transport* error. The entry really was replicated and applied; reporting it as a Raft failure would invite a retry that must fail identically, and would hide a durable decision behind a transient-looking error. |
| Invariant | `ClusterResponse` is openraft's response type, so a rejection rides back through the success channel by construction (`writer.rs`). |
| Outcome variant | `Proposed::Committed(ClusterResponse::Error)` |
| Forced by | `leader_commit_ok_is_committed`, `leader_commit_state_machine_error_is_committed_error` |
| Bug refs | — |

## FM-CLUSTER-048 — a follower forwards to the leader, and falls back to a redirect it can name

| Field | Value |
|---|---|
| Trigger | An administrative `CLUSTER` command received by a follower. |
| Observable | The follower forwards the write over the cluster bus and answers `Proposed::Forwarded` — an `+OK` to the client, with no redirect. If the forward fails, the client gets `REDIRECT <leader-id> <leader-client-addr>`. |
| NOT observable | The follower performing the leader's *side effects* on a forwarded write — the leader already added the Raft voter, and doing it twice on an existing voter would demote it to a learner and cost the cluster a quorum member. |
| Invariant | `Forwarded` is a distinct variant from `Committed`, so the caller can tell "the leader did it" from "I did it" and skip the local side effects; `resolve_redirect` is a pure function of `(leader_id, cluster_state)`. |
| Outcome variant | `Proposed::Forwarded` / `ProposeError::Redirect` |
| Forced by | `follower_forward_success_is_forwarded`, `follower_forward_failure_is_redirect`, `resolve_redirect_known_leader_yields_redirect`, `a_forwarded_write_reports_the_leaders_verdict`, `the_bus_forwarder_reports_whether_the_leader_took_the_write` |
| Bug refs | — |

## FM-CLUSTER-049 — no addressable leader degrades to `CLUSTERDOWN`

| Field | Value |
|---|---|
| Trigger | A forward failing with no leader known, or with a leader id that is not in this node's cluster state (so it has no client address). |
| Observable | `CLUSTERDOWN No leader available: <id-or-None>`. The wire strings for both this and the `REDIRECT` form are pinned exactly. |
| NOT observable | A `REDIRECT` naming an address the node does not have — the same class of malformed redirect as FM-CLUSTER-023. |
| Invariant | `LeaderRedirect` carries `Option<NodeId>` and `Option<SocketAddr>` independently, so "leader known, address unknown" is representable and renders as `CLUSTERDOWN` rather than a hole in a `REDIRECT`. |
| Outcome variant | `CLUSTERDOWN` |
| Forced by | `follower_forward_failure_unknown_leader_is_clusterdown_redirect`, `resolve_redirect_leader_not_in_state_yields_clusterdown`, `resolve_redirect_no_leader_yields_clusterdown`, `redirect_to_response_renders_wire_strings` |
| Bug refs | `.scratch/hardening/issues/done/30-self-fence-flake-and-misleading-clusterdown.md` |

## FM-CLUSTER-050 — any other Raft failure is reported as itself

| Field | Value |
|---|---|
| Trigger | A Raft error that is not `ForwardToLeader` — a fatal storage error, a panicked state machine, an election timeout. |
| Observable | `ProposeError::Raft(message)`, rendered as `ERR Raft error: …` with the underlying message preserved. |
| NOT observable | A fatal error being laundered into a redirect. A client that follows a redirect after a storage failure retries against a node that will fail identically, and the operator loses the only signal that the failure was local and permanent. |
| Invariant | The `ForwardToLeader` match is on the specific `ClientWriteError` variant, so everything else falls through to the `Raft` arm rather than being classified by string matching. |
| Outcome variant | `ProposeError::Raft` |
| Forced by | `non_forward_raft_error_is_raft_error` |
| Bug refs | — |

`ClusterWriter::propose` has exactly these two error variants and no quorum-loss variant. Quorum loss
reaches a client one of three ways: the write-path self-fence rejects it before it ever proposes
(FM-CLUSTER-059), a redirect cannot be resolved (FM-CLUSTER-049), or it surfaces here as
`Raft(String)`. That is a deliberate consequence of quorum being a *detector* concept rather than a
propose-time one; it is recorded so a reader does not go looking for the missing variant.

## FM-CLUSTER-051 — the bus envelope cannot confuse a Raft RPC with a bus RPC

| Field | Value |
|---|---|
| Trigger | Any inter-node message: `AppendEntries`, `Vote`, `InstallSnapshot`, `ForwardedWrite`, `PubSubBroadcast`, `PubSubForward`, `HealthProbe`. |
| Observable | Every variant round-trips through the postcard framing unchanged, and each subset's `From` shim places it on the matching envelope arm. The address registry resolves, updates and removes node addresses. |
| NOT observable | A bus RPC being dispatched into the Raft handler or vice versa — the handler for one would treat the other's payload as a consensus message. |
| Invariant | `ClusterRpcRequest::{Raft, Bus}` is a typed split and `BusRpc` names only the serviceable variants, so `handle_bus_rpc` is exhaustive by construction and cannot receive a Raft RPC. |
| Outcome variant | `ClusterRpcRequest` / `ClusterRpcResponse` |
| Forced by | `test_from_shims_wrap_correct_arm`, `test_all_rpc_variants_roundtrip`, `test_rpc_request_serialization`, `test_network_factory_node_registration`, `the_bus_serves_probes_and_raft_rpcs_on_one_connection`, `the_pool_keeps_one_slot_per_peer_until_the_peer_is_removed`, `the_factory_reports_every_registered_address`, `the_voter_retry_schedule_backs_off_and_then_stops`, `adding_a_voter_runs_for_a_stranger_and_skips_an_existing_member` |
| Bug refs | — |

---

## FM-CLUSTER-052 — failure latching is symmetric and hysteretic

| Field | Value |
|---|---|
| Trigger | A peer failing probes up to and past the threshold, then answering them again; and a peer that answers roughly every other probe. |
| Observable | `fail_threshold - 1` consecutive failures do not latch; the Nth does. Once latched, `fail_threshold - 1` consecutive successes do not clear it; the Nth does. Any single failure resets an in-progress recovery run, so a peer alternating success and failure never clears. A single success resets the failure run of a peer that has not yet latched. |
| NOT observable | A flapping peer churning `MarkNodeFailed`/`MarkNodeRecovered` pairs — each one is a replicated entry and each `MarkNodeFailed` bumps the config epoch (FM-CLUSTER-013), so an unhysteretic detector turns a marginal link into unbounded epoch churn across the whole cluster. |
| Invariant | `record_failure` zeroes `success_count` and `record_success` zeroes `failure_count`, and the un-latch branch runs only while latched (`failure_detector.rs:162-188`) — the hysteresis is symmetric because both counters are consecutive-run counters, not totals. |
| Outcome variant | `NodeHealth::is_marked_fail` |
| Forced by | `test_health_table_threshold_latching_is_symmetric`, `test_health_table_latch_survives_flapping_recovery`, `test_health_table_success_resets_failure_run`, `test_node_health_default`, `local_probe_results_land_in_the_health_table` |
| Bug refs | — |

## FM-CLUSTER-053 — the verdict is level-triggered and a latch never decays

| Field | Value |
|---|---|
| Trigger | Polling a peer's verdict: never probed, healthy, failing below threshold, latched, latched an hour ago, recovered, and then gone quiet. |
| Observable | Never probed is `Unknown`; a success is `Healthy`; a sub-threshold failure is still `Healthy`; a latch is `Failed` on every subsequent poll including an hour later; after recovery it is `Healthy`, and it decays to `Unknown` once it goes stale. |
| NOT observable | A latched peer silently decaying to `Unknown` through staleness, which would make the leader stop reconciling it and leave a dead node unflagged in the replicated topology forever. That is why the `is_marked_fail` check precedes the staleness check. |
| Invariant | The verdict is recomputed from state on every poll rather than latched at a transition; that is what lets a *new* leader converge on a peer whose threshold was crossed while it was still a follower — the transition-triggered version drops that write and never retries it. |
| Outcome variant | `LocalVerdict::{Failed, Healthy, Unknown}` |
| Forced by | `test_health_table_verdict_is_level_triggered` |
| Bug refs | — |

## FM-CLUSTER-054 — staleness has a closed, threshold-derived boundary

| Field | Value |
|---|---|
| Trigger | A peer last seen exactly at, just inside, and past the staleness window. |
| Observable | The window is `check_interval * (fail_threshold + 2)`. Just inside it the peer counts as reachable; exactly at the boundary and beyond it does not (the comparison is strict `<`). The defaults are `check_interval_ms = 1000`, `connect_timeout_ms = 500`, `fail_threshold = 5`. |
| NOT observable | A staleness window unrelated to the probe cadence. Deriving it from `check_interval` and `fail_threshold` means retuning either one keeps the two consistent; a hard-coded window would silently become either trigger-happy or useless. |
| Invariant | `stale_threshold()` is the single derivation, and every time-dependent method takes `now` as a parameter, so the whole table is a pure state machine testable without a clock. |
| Outcome variant | `Duration` |
| Forced by | `test_health_table_staleness_boundary`, `test_failure_detector_config_default` |
| Bug refs | — |

## FM-CLUSTER-055 — quorum counts conservatively

| Field | Value |
|---|---|
| Trigger | Evaluating quorum at startup, with peers latched failed, with peers never probed, and across cluster sizes 1..5. |
| Observable | Quorum is `total/2 + 1`. This node always counts itself. A peer that is latched failed does not count. A peer that has never been probed does not count. So a freshly started node reports *no* quorum until its probes land. |
| NOT observable | An unprobed peer counted as reachable. That would let a node that has just booted — or one whose detector task is wedged — believe it has quorum and accept writes it cannot replicate. Optimism here is exactly the split-brain failure the self-fence exists to prevent. |
| Invariant | The "no entry" arm of `reachable_count` returns false explicitly rather than falling through to a default (`failure_detector.rs:245-249`). |
| Outcome variant | `bool` |
| Forced by | `test_health_table_quorum_arithmetic`, `test_health_table_reachable_never_seen_and_failed`, `quorum_follows_the_locally_probed_peers` |
| Bug refs | — |

## FM-CLUSTER-056 — the promotion score is priority first, replication lag second

| Field | Value |
|---|---|
| Trigger | Scoring candidate replicas for an automatic failover. |
| Observable | The score is `priority * 100_000 + (max_offset - replica_offset) * 1_000`, lower is better. At equal offsets the lower priority wins; at equal priorities the higher offset (less lag) wins. A candidate whose offset could not be determined is scored as if it had the worst offset, not excluded. |
| NOT observable | A more-lagged replica outscoring a less-lagged one at the same priority — that is data loss chosen deliberately. The lag term uses `saturating_sub`, so a stale or absent offset can only make a candidate look worse. |
| Invariant | `compute_replica_score` is a pure function of three numbers, so the weighting is auditable in one place rather than distributed across the selection loop. |
| Outcome variant | `u64` score |
| Forced by | `test_compute_replica_score_formula`, `test_compute_replica_score_lower_priority_is_better`, `test_compute_replica_score_higher_offset_is_better`, `test_compute_replica_score_no_offset_found`, `auto_failover_promotes_the_replica_with_the_freshest_offset` |
| Bug refs | — |

## FM-CLUSTER-057 — priority 0 is never promoted

| Field | Value |
|---|---|
| Trigger | A failover where some or all candidates carry `replica_priority = 0`. |
| Observable | A priority-0 candidate scores `u64::MAX` and is filtered out of selection entirely. If every candidate is priority 0, selection returns `None` and the failover is abandoned with a warning rather than promoting someone. |
| NOT observable | A node the operator marked never-promote being promoted anyway — that setting is how a cross-region or backup-only replica is kept out of the primary role, and violating it is a latency or durability regression the operator explicitly ruled out. |
| Invariant | Belt and braces: an explicit `filter(priority != 0)` *and* the `u64::MAX` score, so removing either one alone still refuses. |
| Outcome variant | `Option<&NodeInfo>` = `None` |
| Forced by | `test_compute_replica_score_priority_zero_excluded`, `test_select_failover_target_all_never_promote`, `test_missing_replica_priority_defaults_to_the_neutral_value` |
| Bug refs | — |

## FM-CLUSTER-058 — failover target selection is deterministic and live-tunable

| Field | Value |
|---|---|
| Trigger | Two candidates with identical scores; and `CONFIG SET cluster-replica-priority` on a running node. |
| Observable | The tie is broken by the lower node id, so every node computing the selection independently reaches the same answer. Changing this node's priority at runtime changes the selection it computes immediately — no restart, no re-published `NodeInfo`. Setting it to 0 removes this node from its own candidate set. |
| NOT observable | Two nodes disagreeing about the winner. Selection is not itself replicated — the decision is made locally and then proposed — so a nondeterministic tiebreak is a route to two competing `Failover` proposals. |
| Invariant | `min_by(score).then_with(id)` makes the ordering total; the priority read goes through `ClusterRuntimeFlags`, which is atomics behind an `Arc` read at decision time. |
| Outcome variant | `Option<&NodeInfo>` |
| Forced by | `test_compute_replica_score_equal_scores_tiebreak_by_node_id`, `test_replica_priority_store_changes_failover_target`, `effective_priority_is_live_for_this_node_and_published_for_peers` |
| Bug refs | — |

Documented asymmetry: a live priority change is authoritative only for *this* node's own score.
Peers keep scoring this node from the Raft-replicated `NodeInfo.replica_priority` until it
re-registers, so two nodes running selection concurrently mid-change can pick different targets.

## FM-CLUSTER-059 — the self-fence knob is live, and its reported reason matches the verdict it gates on

| Field | Value |
|---|---|
| Trigger | `CONFIG SET cluster-self-fence-on-quorum-loss` and `cluster-auto-failover` on a running node with quorum already lost. |
| Observable | Disabling the fence makes writes acceptable again on the same gate instance; re-enabling it fences again. With healthy quorum the flag makes no difference. The write-fence reason reported to observability is `"cluster quorum lost"` exactly when the gate fences, and absent when it does not. |
| NOT observable | A knob that only takes effect at startup. Gating *installation* of the checker rather than its verdict is what made this startup-only before, and an operator riding out a partition cannot restart the node to change it. Nor `/status` claiming a fence reason the write path is not enforcing. |
| Invariant | `SelfFenceGate::has_quorum` is `!flag || inner.has_quorum()` and `write_fence_reason` is derived from that same call, so the report cannot disagree with the gate (`flags.rs:126-139`). The wire wording is the *inner* checker's: `quorum_lost_error()` forwards to `inner.quorum_lost_error()`, whose `QuorumChecker` default is `CLUSTER_DOWN_QUORUM_LOST`, so a cluster fence keeps answering `CLUSTERDOWN` while the replication self-fence behind the same write gate answers its own `SELFFENCE` code (FM-REPLICATION-041). The gate never spells the string itself — a wrapper that hardcoded one would re-label whatever it wraps. |
| Outcome variant | `Option<&'static str>` = `Some("cluster quorum lost")`; `CLUSTERDOWN The cluster is down (quorum lost, writes rejected)` |
| Forced by | `auto_failover_flag_is_live`, `self_fence_gate_follows_live_flag`, `self_fence_gate_passes_through_healthy_quorum`, `self_fence_gate_reports_the_reason_it_gates_on`, `self_fence_gate_delegates_the_quorum_lost_wording` |
| Bug refs | `.scratch/hardening/issues/done/30-self-fence-flake-and-misleading-clusterdown.md` |

## FM-CLUSTER-060 — runtime flags derive from config with no field drift

| Field | Value |
|---|---|
| Trigger | Building the runtime flags from a cluster config section, and constructing them with no config at all. |
| Observable | Each of the three fields lands on its own flag — auto-failover, self-fence, replica priority — with no transposition. The defaults are auto-failover **off**, self-fence **on**, priority 100, matching `ClusterConfigSection::default()`. |
| NOT observable | Two same-typed flags swapped. `auto_failover` and `self_fence_on_quorum_loss` are both `bool` with *different* defaults, so a transposition silently inverts both policies: failover fires unbidden and writes are never fenced. Nor the runtime defaults drifting from the config defaults, which would make the same deployment behave differently depending on whether a config file was supplied. |
| Invariant | `Default` is implemented by delegating to `ClusterConfigSection::default()` and then `from_config`, so the two cannot diverge by construction. |
| Outcome variant | `Arc<ClusterRuntimeFlags>` |
| Forced by | `runtime_flags_from_config_carry_each_field`, `runtime_flags_default_matches_the_config_default` |
| Bug refs | — |

---

## FM-CLUSTER-061 — `CLUSTER`'s discovery subcommands stay reachable from the client port

| Field | Value |
|---|---|
| Trigger | A non-admin connection on a server with `admin_enabled` issuing `CLUSTER INFO`/`NODES`/`MYID`/`SLOTS`/`SHARDS`/`KEYSLOT`/`COUNTKEYSINSLOT`/`GETKEYSINSLOT`/`HELP`. |
| Observable | All nine are answered. Every other subcommand — `SETSLOT`, `FORGET`, `MEET`, `RESET`, `FAILOVER`, `ADDSLOTS`, … — is `NOADMIN Admin commands are disabled on this port. Use the admin port.` Subcommand matching is case-insensitive, as is the command name. |
| NOT observable | Topology discovery being admin-gated. Every cluster-aware client bootstraps with `CLUSTER SLOTS` or `CLUSTER SHARDS`; gating those makes the client port unusable for its intended purpose. |
| Invariant | `AdminSurface::SplitBySubcommand{public}` is consulted per subcommand rather than per command, so the split is a data table rather than a branch in the guard. |
| Outcome variant | `AdminSurface::SplitBySubcommand` / `NOADMIN` |
| Forced by | `admin_surface_split_allows_public_subcommands`, `admin_surface_matches_names_case_insensitively` |
| Bug refs | — |

## FM-CLUSTER-062 — a bare container command fails closed

| Field | Value |
|---|---|
| Trigger | `CLUSTER` with no subcommand at all, from a non-admin connection. Likewise `CONFIG`, `ACL`, `CLIENT`, `MEMORY`. |
| Observable | `NOADMIN`. |
| NOT observable | A bare container command being treated as public because there is no subcommand to look up. `requires_admin(None)` returning false would open every split command's arity-error path to unauthenticated probing. |
| Invariant | The `None` arm of `requires_admin` returns `true` explicitly (`command_spec.rs`), and `extract_subcommand` returns `None` exactly for the bare form. |
| Outcome variant | `NOADMIN` |
| Forced by | `admin_surface_split_gates_missing_subcommand` |
| Bug refs | — |

## FM-CLUSTER-063 — an unknown subcommand fails closed

| Field | Value |
|---|---|
| Trigger | `CLUSTER NOT-A-SUBCOMMAND` from a non-admin connection, and any subcommand added to the dispatcher but not to the public list. |
| Observable | `NOADMIN`, before the dispatcher gets a chance to report an unknown subcommand. |
| NOT observable | A newly added subcommand being public by default. This is the property that makes the split table safe to extend: a `CLUSTER` subcommand added next year is admin-only until someone deliberately declares it public. |
| Invariant | The public list is an allow-list matched with `any(..)`, so anything absent is gated — there is no deny-list to keep in sync. |
| Outcome variant | `NOADMIN` |
| Forced by | `admin_surface_split_gates_unknown_subcommand` |
| Bug refs | — |

## FM-CLUSTER-064 — the split table is the whole admin contract, and the registry agrees with it

| Field | Value |
|---|---|
| Trigger | Resolving the admin surface for a split command, a whole-command admin command, and an ungated command. |
| Observable | The per-command classification of all five split commands is pinned, so a change to the admin-port contract shows up in a diff. A command carrying `ADMIN` with no split entry is gated in every form. A command with neither is `Open`. Live registry entries never carry `ADMIN` *and* a split entry. |
| NOT observable | A command gated by two mechanisms that disagree — the split table wins over the `ADMIN` flag, so a command carrying both would have its flag silently ignored, and a reader auditing flags would reach the wrong conclusion. The registry coherence test makes that combination impossible rather than merely discouraged. |
| Invariant | `admin_surface(command, flags)` consults the split table first and falls back to the flag; `split_admin_surfaces_agree_with_command_flags` walks the *live* registry rather than a copy of the table. |
| Outcome variant | `AdminSurface::{Open, Whole, SplitBySubcommand}` |
| Forced by | `admin_surface_split_table_classification`, `admin_surface_open_when_no_admin_flag_and_no_split`, `admin_surface_whole_command_gates_every_form`, `split_admin_surfaces_agree_with_command_flags` |
| Bug refs | — |

---

## FM-CLUSTER-065 — strict mode on the cluster bus never downgrades

| Field | Value |
|---|---|
| Trigger | An inbound bus connection while dual-accept is off, whatever its first byte — a TLS `ClientHello` (`0x16`), a plaintext frame, or an immediate EOF. |
| Observable | The connection is always handed to the TLS acceptor. A plaintext peer fails the handshake. |
| NOT observable | A plaintext bus connection accepted while strict mode is on. The bus carries Raft RPCs, forwarded writes, and pub/sub payloads with no authentication of its own — anything that can dial it in plaintext can propose cluster state. |
| Invariant | `choose_transport` returns early on `!migration` before it inspects the sniffed byte at all (`bus.rs:129-130`), so no byte value can reach the plaintext arm. |
| Outcome variant | `BusTransport::Tls` |
| Forced by | `strict_mode_always_requires_tls` |
| Bug refs | — |

## FM-CLUSTER-066 — dual-accept sniffs the ClientHello, and the decision is re-read per connection

| Field | Value |
|---|---|
| Trigger | A rolling TLS migration with dual-accept enabled, receiving both TLS and plaintext peers; and the flag being flipped between connections. |
| Observable | A first byte of `0x16` goes to TLS; anything else, including an immediate EOF, is treated as plaintext and fails downstream on its own terms. Flipping the flag changes the next connection's decision with no restart and no listener rebind. |
| NOT observable | The transport decision being cached for the lifetime of the listener — a migration would then need a restart at exactly the moment the operator is trying to avoid one. |
| Invariant | `dual_accept()` and `handshake_timeout()` are read inside `handle_connection`, once per connection; the listener itself is pre-bound and never re-bound, so there is no port TOCTOU. |
| Outcome variant | `BusTransport::{Tls, Plaintext}` |
| Forced by | `dual_accept_sniffs_the_client_hello`, `flipping_the_runtime_flag_changes_the_next_connection`, `test_cluster_bus_bind_fails_on_invalid_addr`, `a_peek_of_zero_bytes_is_not_a_first_byte` |
| Bug refs | — |

## FM-CLUSTER-067 — no cross-node pub/sub RPC can hang the publisher

| Field | Value |
|---|---|
| Trigger | A peer that accepts the pub/sub RPC and never answers. |
| Observable | The call resolves as a timeout after `PUBSUB_RPC_TIMEOUT` (2s) and the publisher's `PUBLISH` returns. |
| NOT observable | A `PUBLISH` blocked indefinitely on one unresponsive peer. The fan-out awaits every peer, so an unbounded wait on one of them stalls the publishing *client*, not merely the delivery. |
| Invariant | `send_pubsub_rpc` is the single owner of the timeout and wraps the future rather than the transport, so every call site inherits it and none can opt out. |
| Outcome variant | `PubSubRpcError::Timeout` |
| Forced by | `test_rpc_timeout_maps_to_timeout_variant` |
| Bug refs | — |

## FM-CLUSTER-068 — a protocol-shape mismatch is never reported as a delivery count

| Field | Value |
|---|---|
| Trigger | A peer answering a broadcast request with a forward result, or with an explicit protocol error; and the ordinary success and transport-failure paths. |
| Observable | A well-shaped answer yields its subscriber count. A shape mismatch is `UnexpectedResponse` and logs the full response at `warn`. A transport failure is `Rpc`. The extractors match only their own result variant and reject an error response. |
| NOT observable | A peer bug rendered as `Ok(0)`. "The peer answered with the wrong message type" and "the peer has no subscribers" are different operational problems, and collapsing the first into the second makes it invisible. |
| Invariant | Extraction is a function pointer returning `Option`, and the `None` arm is the only path to `UnexpectedResponse` — so the distinction is structural rather than a log level. |
| Outcome variant | `PubSubRpcError::{UnexpectedResponse, Rpc}` |
| Forced by | `test_rpc_expected_shape_yields_count`, `test_rpc_shape_mismatch_is_distinguishable_not_zero`, `test_rpc_transport_error_maps_to_rpc_variant`, `test_forward_extractor_matches_only_forward_results` |
| Bug refs | — |

Accepted non-guarantee: all three error variants are folded to `0` by both call sites, so a client
cannot distinguish a total partition from a delivered-to-nobody broadcast. `PUBLISH` returns one
integer and Redis has the same property; the distinction lives in the logs.

## FM-CLUSTER-069 — pub/sub fan-out is a no-op when there is nobody to fan out to

| Field | Value |
|---|---|
| Trigger | `PUBLISH` on a standalone node, and on a clustered node whose address registry holds one entry or none. |
| Observable | The remote subscriber count is 0 and no RPC is attempted. `SPUBLISH` on a standalone node returns `None`, meaning "deliver locally". |
| NOT observable | A standalone node attempting cluster RPCs, or a single-node cluster broadcasting to itself and double-counting its own subscribers. |
| Invariant | Both methods `let-else` on the `Cluster` variant at the top, and the cluster path returns early on `all_nodes.len() <= 1` before building any task. |
| Outcome variant | `usize` = 0 / `Option<usize>` = `None` |
| Forced by | `test_local_forwarder_broadcast_is_noop`, `test_local_forwarder_forward_returns_none`, `cluster_broadcast_is_a_noop_below_two_nodes`, `cluster_broadcast_sums_every_other_reachable_peer_once`, `cluster_broadcast_folds_an_unreachable_peer_into_zero` |
| Bug refs | — |

## FM-CLUSTER-070 — `SPUBLISH` falls back to local delivery when it cannot name a remote owner

| Field | Value |
|---|---|
| Trigger | `SPUBLISH` to a shard channel whose slot this node owns, whose slot a reachable peer owns, whose slot nobody owns, and whose owner has no address in the registry. |
| Observable | Four distinct outcomes. Only a reachable remote owner forwards (`Forwarded(count)`); the other three deliver locally and say which one they are — `Local(Local)`, `Local(Unowned{slot})`, `Local(OwnerUnaddressable{owner,slot})`. Both fallbacks warn-log naming the slot. The client-visible reply is identical across all three local outcomes. |
| NOT observable | The message being dropped — the local path still runs in every non-`Remote` case, so a subscriber attached to this node still receives it. Nor a fallback delivery reported identically to a correct one: a slot-assignment gap that silently delivers shard messages on the wrong node used to be indistinguishable from correct ownership. |
| Invariant | `route_shard_channel_in` is a pure function of the slot map plus the address registry and returns a four-variant `ShardRoute`; `forward_spublish` matches it exhaustively, so a new unresolvable case cannot be added as another silent `None`. |
| Outcome variant | `ShardRoute::{Local, Remote, Unowned, OwnerUnaddressable}` / `SpublishOutcome::{Forwarded, Local}` |
| Forced by | `cluster_forward_returns_none_when_this_node_owns_the_slot`, `cluster_forward_distinguishes_an_unowned_slot_from_local_ownership`, `cluster_forward_distinguishes_an_unaddressable_owner_from_local_ownership`, `cluster_route_names_a_reachable_remote_owner`, `test_local_forwarder_forward_returns_none`, `cluster_forward_reports_the_owners_subscriber_count` |
| Bug refs | fixed: [36-spublish-conflates-unowned-slot-with-local-ownership.md](../issues/done/36-spublish-conflates-unowned-slot-with-local-ownership.md) |

The local fallback itself stays deliberate: delivering locally is strictly better than dropping,
because FrogDB's `SSUBSCRIBE` does not slot-route subscribers, so no other node would deliver the
message either. Redis answers `MOVED`/`CLUSTERDOWN` for a shard channel it does not serve; matching
that is a client-visible change gated on the broader "should shard pub/sub slot-route at all"
question, and is not this row's subject. What this row pins is that the fallback is *named* rather
than disguised as correct ownership.

## FM-CLUSTER-071 — `CLUSTER NODES` renders one exact line per node

| Field | Value |
|---|---|
| Trigger | `CLUSTER NODES` on a clustered node and on a standalone one. |
| Observable | Node ids are 40 hex characters (a `u64` zero-padded to Redis' width). Flags render in the order `myself`, `master`/`slave`, `fail`, `fail?`, `handshake`, `noaddr`. Health is `fail` > `loading` (pfail) > `online`. `ping-sent`/`pong-recv` are `0 0` and the link state is `connected`, always. Migration markers are appended as `[slot->-<target>]` / `[slot-<-<source>]`. A slotless node yields a trailing double space. Standalone renders one `myself,master` line owning `0-16383`. |
| NOT observable | A node id that is not 40 hex characters — every Redis client parses this field positionally and by width. Nor fabricated gossip timings: FrogDB has no gossip layer, so `0 0` is the honest value rather than a synthesized one. |
| Invariant | One renderer serves both the clustered and standalone paths (`standalone_snapshot` builds a snapshot rather than a second formatter), and the golden test pins the whole line including the whitespace artifact. |
| Outcome variant | `CLUSTER NODES` body |
| Forced by | `test_render_cluster_nodes_golden`, `test_render_cluster_nodes_standalone`, `test_format_node_id_is_40_hex`, `test_node_flags_label`, `test_node_health`, `test_cluster_snapshot_partitions_nodes_by_role` |
| Bug refs | — |

## FM-CLUSTER-072 — `SLOTS` drops slotless primaries, `SHARDS` keeps them

| Field | Value |
|---|---|
| Trigger | A cluster containing a primary that owns no slots, and a `CLUSTER SHARDS` on a node with replicas. |
| Observable | `CLUSTER SLOTS` omits the slotless primary entirely; `CLUSTER SHARDS` includes it with an empty slot list. Shards are ordered by primary id whatever the insertion order, replicas nested in id order. A node's slots are compacted into contiguous ranges. `replication-offset` is reported only for the local node; peers omit the field. |
| NOT observable | A `CLUSTER SLOTS` entry with an empty slot range — a client would parse it as a mapping and route nothing to it. Nor a fabricated replication offset for a peer this node cannot measure. |
| Invariant | `shard_views` deliberately keeps zero-slot primaries and documents that the SLOTS mapper must filter them itself, so the two commands share one grouping with one explicit difference. |
| Outcome variant | `CLUSTER SLOTS` / `CLUSTER SHARDS` replies |
| Forced by | `test_shard_views_grouping_and_order`, `test_shard_views_includes_zero_slot_primary`, `test_map_slots_response_skips_zero_slot_primary`, `test_map_shards_response_reports_offset_for_local_node_only`, `test_cluster_snapshot_get_node_slots` |
| Bug refs | — |

## FM-CLUSTER-073 — slot health counts every assigned slot exactly once

| Field | Value |
|---|---|
| Trigger | `CLUSTER INFO` with owners flagged `fail`, flagged `pfail`, flagged both, recovered, and with slots whose owner is not a member. |
| Observable | `cluster_slots_ok + cluster_slots_pfail + cluster_slots_fail == cluster_slots_assigned`, always. `fail` beats `pfail` for a doubly flagged owner. Flagging one owner leaves other owners' slots untouched. A flagged owner with no slots moves no counts. A slot whose owner is unknown counts as `ok` rather than being dropped. Recovery restores the full `ok` count. |
| NOT observable | Counters that do not sum to the assigned total. An operator sizing an incident from these numbers needs them to partition the slot space; a dropped slot understates the outage. |
| Invariant | `count_slot_health` walks the assignment map once and assigns each slot to exactly one bucket, with the unknown-owner case landing in `ok` explicitly rather than falling out of the loop. |
| Outcome variant | `SlotHealthCounts` |
| Forced by | `test_count_slot_health_totals_always_equal_slots_assigned`, `test_count_slot_health_all_ok_when_no_node_flagged`, `test_count_slot_health_fail_flagged_owner_counts_as_fail_not_ok`, `test_count_slot_health_pfail_flagged_owner_counts_distinct_from_fail`, `test_count_slot_health_fail_takes_precedence_over_pfail`, `test_count_slot_health_recovery_restores_full_ok`, `test_count_slot_health_only_counts_flagged_owners_slots_others_unaffected`, `test_count_slot_health_fail_flagged_slotless_primary_leaves_slots_ok`, `test_count_slot_health_unknown_owner_counted_ok_not_dropped`, `test_cluster_snapshot_slot_coverage_readers_agree` |
| Bug refs | fixed: [37-cluster-stats-messages-hardcoded-zero.md](../issues/done/37-cluster-stats-messages-hardcoded-zero.md) |

## FM-CLUSTER-074 — `CLUSTER INFO` is CRLF-framed `key:value` lines

| Field | Value |
|---|---|
| Trigger | `CLUSTER INFO` on any node. |
| Observable | Every line is `key:value` terminated by CRLF, the framing every Redis client's INFO parser expects. |
| NOT observable | LF-only framing, which some clients accept and others silently truncate on. |
| Invariant | One `render` builds the whole body, so a field added later inherits the framing rather than choosing its own. |
| Outcome variant | `CLUSTER INFO` body |
| Forced by | `test_cluster_info_render_is_crlf_framed_key_value_lines` |
| Bug refs | fixed: [37-cluster-stats-messages-hardcoded-zero.md](../issues/done/37-cluster-stats-messages-hardcoded-zero.md) |

## FM-CLUSTER-075 — slot ranges parse and render Redis-style

| Field | Value |
|---|---|
| Trigger | Parsing and rendering slot ranges from `CLUSTER NODES` and from operator input. |
| Observable | `"0-100"` round-trips; a single slot renders bare (`"42"`) and parses back. `"invalid"`, an inverted range (`"100-50"`), and an out-of-range slot (`"20000"`) are all `InvalidSlot`. Containment is inclusive at both ends. |
| NOT observable | An inverted or out-of-range range being accepted from a string — every downstream length computation assumes `start <= end < 16384`, and an inverted range underflows it. |
| Invariant | `FromStr` validates the ordering and the upper bound before constructing, so the parse boundary is where the invariant is established. |
| Outcome variant | `ClusterError::InvalidSlot` |
| Forced by | `test_slot_range_contains`, `test_slot_range_display`, `test_slot_range_parse`, `test_slot_range_len_counts_both_endpoints`, `test_slot_range_parse_accepts_a_degenerate_range` |
| Bug refs | — |

## FM-CLUSTER-077 — `CLUSTER INFO`'s bus counters are measured or absent, never fabricated

| Field | Value |
|---|---|
| Trigger | `CLUSTER INFO` on a node whose bus has carried traffic, on a node whose bus has carried none, and on a node with no handle on a bus at all. |
| Observable | `cluster_stats_messages_sent`/`_received` are live counts of frames that crossed the cluster bus — requests written and responses read by the client side, requests read and responses written by the bus loop, both directions accumulating into one node-wide pair. A node that cannot read them omits both lines. `total_cluster_links_buffer_limit_exceeded` is `0` because there is no per-link output buffer limit to exceed. |
| NOT observable | A `ping`/`pong` per-type line: FrogDB runs no gossip protocol, and Redis omits a per-type line whose counter is zero. Nor a counted frame that never crossed the wire — a request that failed to serialize, or a connection that never opened, is not traffic. |
| Invariant | Counting happens at the four wire seams and nowhere else: `try_send_on_framed` after the write and after the response frame (`network.rs`), `parse_rpc_message` after a frame arrives, `send_rpc_response` after the write. The rendering treats an unreadable counter as `None` and omits the line, the way `cluster_raft_term` already does. |
| Outcome variant | `ClusterBusStatsSnapshot` / `CLUSTER INFO` body |
| Forced by | `a_fresh_counter_pair_reads_zero`, `the_two_directions_are_counted_independently`, `counters_accumulate_across_threads`, `cluster_stats_messages_sent_grows_with_bus_traffic`, `cluster_stats_messages_received_grows_on_the_receiving_node`, `a_connection_that_never_opens_counts_nothing`, `cluster_info_omits_gossip_counters_that_have_no_source`, `cluster_info_reports_the_live_bus_counters`, `cluster_info_omits_the_bus_totals_when_they_cannot_be_read`, `a_peer_handle_shares_the_factorys_counters` |
| Bug refs | fixed: [37-cluster-stats-messages-hardcoded-zero.md](../issues/done/37-cluster-stats-messages-hardcoded-zero.md) |

## FM-CLUSTER-078 — the published snapshot is never older than an applied mutation

| Field | Value |
|---|---|
| Trigger | Any topology mutation the state machine applies — a `ClusterCommand` through `apply_command` (including one it rejects), or a whole-state replacement through `restore_from_snapshot` — followed by a `ClusterState::snapshot()`. |
| Observable | The next `snapshot()` observes the mutation. Two `snapshot()` calls with no intervening mutation return the *same* allocation (`Arc::ptr_eq`), and read-only accessors do not invalidate it. A snapshot a reader already holds is immutable: a later mutation cannot reach back into a decision made against it. |
| NOT observable | A reader routing against a topology the state machine has already moved past — a stale slot table is a `MOVED` to the wrong node or a local serve of a slot this node no longer owns. Nor a per-read copy of the 16384-entry slot table: `snapshot()` is on the keyed-command path (`SlotMigrationCoordinator::route`), so a copy there is per-command cost. |
| Invariant | `StateCell` holds the authoritative `ClusterStateInner` and the published `Arc<ClusterSnapshot>` under one lock, and `PublishOnDrop` — the only handle that hands out a `&mut ClusterStateInner` — rebuilds the published value in its `Drop`, inside the same critical section. An early `return`, a `?`, or an unwinding panic all run that `Drop`. The openraft bookkeeping fields (`last_applied_log`, `last_membership`) are deliberately not snapshot-visible and have their own no-republish setters (`state.rs`). |
| Outcome variant | `Arc<ClusterSnapshot>` |
| Forced by | `test_snapshot_observes_topology_applied_since_the_last_read`, `test_repeated_snapshots_without_mutation_share_one_allocation`, `test_rejected_command_leaves_snapshot_agreeing_with_state`, `test_snapshot_install_republishes_the_reader_view`, `state_readers_report_the_applied_table` |
| Bug refs | fixed: [10-cluster-snapshot-clone-cost.md](../../replication-cluster-rework/issues/done/10-cluster-snapshot-clone-cost.md) |

---

## GAPS — behavior nothing forces

Listed so the mutation run's survivors can be triaged against a known list rather than
rediscovered. Each is a candidate row for the gap-filling step of this phase.

1. **`reconcile_topology`'s 2×3 decision matrix** (`failure_detector.rs:367-390`) — the whole
   leader-side reconciliation, including the "converged cluster performs zero Raft writes" property
   that FM-CLUSTER-014 leans on, has no unit test. It needs a `ClusterRaft`, so forcing it means
   extracting the matrix into a pure function first.
2. **`InflightGuard` dedup and drop** (`failure_detector.rs:107-123, 394-414`) — the at-most-one-
   write-per-peer property and the poison-recovering `Drop` are untested. Same blocker.
3. **`trigger_auto_failover` end to end** (`failure_detector.rs:487-629`) — the primary check, the
   empty-replica abandon, sequential offset probing, "unreachable scores 0 but stays a candidate",
   the 3-attempt retry loop, and the permanent give-up. The give-up is the highest-consequence
   untested path in the area: after it, the FAIL flag is already set, so level-triggered
   reconciliation never calls `mark_node_failed` again and no further automatic attempt is made.
4. **`check_node_reachable`** — liveness is a bare TCP connect, so a wedged-but-listening node reads
   as healthy. Nothing pins that, and nothing pins that the bus `HealthProbe` answers
   unconditionally without consulting quorum, fence, or loading state.
5. **The `Cluster` arm of `broadcast_publish`** — self-skip, the `flags.fail` skip, `JoinSet`
   aggregation, and the silently dropped `JoinError` from a panicked fan-out task. The new tests
   added for FM-CLUSTER-069/070 cover the early returns and the forward path, not the fan-out.
6. **`ClusterState::from_snapshot` zeroing `last_applied_log`**, and `apply` advancing
   `last_applied_log` even for entries whose command errored (`state.rs:65, 742`).
7. **Storage corruption paths** — `try_get_log_entries` with an `Excluded(0)` end bound silently
   reads to the end of the log; `purge` writes `last_purged` before the delete batch; the CF
   `.expect`s and `decode_log_key`'s fixed-width copy panic on a tampered DB.
8. **Unvalidated detector config** — `check_interval_ms = 0` panics `tokio::time::interval`, and a
    huge `fail_threshold` panics the staleness multiply. Nothing rejects either at load.

## Tagging notes for whoever lands these

* Rows 018-030 (routing) and 061-064 (admin gating) are forced by unit tests that live **outside**
  the two crates this phase's mutation gate measures — `frogdb-server` and `frogdb-core`
  respectively. They are real unit tests, not integration tests, and they run in seconds; but
  `cargo mutants -p frogdb-cluster` will not execute them, so they contribute nothing to this
  area's score. The routing seam physically lives in `frogdb-server/src/slot_migration/`; moving it
  would be a Phase 5 extraction, not a spec edit.
* Row 030's witness is `#[ignore]`d and therefore absent from `cargo nextest list`, so the row
  cites its issue as a gap instead. Re-point it when the bug is fixed.
* Row 038's witness does not exist; the dispatcher needs a seam first.
* The `cluster_*` integration binaries (`frogdb-server/crates/server/tests/cluster_{topology,
  slots,migration,failover,misc}.rs`, 185 tests) are end-to-end witnesses for most of these rows
  and may be added to `Forced by` cells, but no row above relies on one as its *only* witness.

## Redis deviations

| Mode | FrogDB | Redis | Rationale |
|---|---|---|---|
| Topology consensus | Raft (openraft) over a dedicated bus, metadata only | Gossip with per-node config epochs and majority agreement on failover | A replicated log gives an ordered, atomic `Failover` entry (FM-CLUSTER-040); gossip needs a multi-step protocol to reach the same place. Data replication is still PSYNC, so this is a control-plane change only. |
| Slot migration states | Two-point ownership swap; no intermediate migration state machine | `MIGRATING`/`IMPORTING` per-slot flags with the same two endpoints | The intermediate enum existed and only ever had one variant constructed. The importing/migrating *roles* are still derived, by comparing the local id against the migration's source and target. |
| `CLUSTER SETSLOT IMPORTING`/`MIGRATING` | Both lower to the same replicated `BeginSlotMigration` | Two independent local flags on two different nodes | Replication makes the pair converge on one record, which is why idempotency is load-bearing (FM-CLUSTER-031). |
| Gossip counters in `CLUSTER INFO` | No per-type lines at all; `cluster_stats_messages_sent`/`_received` count real bus frames | Per-type lines for every non-zero counter, then the same two totals | Not a deviation any more. Redis skips a per-type line whose counter is zero, and FrogDB's per-type counters are *structurally* zero (no gossip protocol), so the lines are absent for the same reason they would be absent on an idle Redis node. `total_cluster_links_buffer_limit_exceeded` stays `0`: there is no per-link output buffer limit to exceed, so zero is measured, not fabricated. See FM-CLUSTER-077. |
| `ping-sent`/`pong-recv` in `CLUSTER NODES` | Always `0 0` | Real timestamps | Same reason; positional fields cannot be omitted without breaking every client's parser, so `0` is the honest placeholder here. |
| `PFAIL` | Structurally supported, never produced | Set by gossip suspicion before a majority confirms `FAIL` | Leader-only detection has no suspicion phase: the leader either has enough consecutive failures to latch or it does not. |
| Shard pub/sub slot routing | `SPUBLISH` forwards to the slot owner when it can name one, and otherwise delivers locally; `SSUBSCRIBE` does not slot-route subscribers | Both are slot-routed like keyed commands, answering `MOVED`/`CLUSTERDOWN` | Since subscribers are not pinned to the owner, refusing a publish would drop a message no other node would deliver. The fallback is named rather than silent (FM-CLUSTER-070); adopting Redis' refusal means routing subscribers first. |
| `CLUSTER BUMPEPOCH` | Not supported | Supported | Epoch changes are authorized by the replicated log; a manual bump would create a claim no entry justifies. |
| `CLUSTER SET-CONFIG-EPOCH` | Sets the exact value; refused unless the node knows no peer and holds epoch 0 | Same two guards, same exact assignment | Identical (FM-CLUSTER-076). The command is replicated through Raft rather than applied locally, so the assignment is durable on the log; the guards are Redis' verbatim. |
| `WAIT` in cluster mode | Per-node, keyless, never redirects | Per-node, keyless, never redirects | Identical. Cluster replicas attach over the same PSYNC link and ACK into the same tracker, so there is no cluster-mode branch to deviate in. |
| Script slot validation | Currently absent for bare scripts (FM-CLUSTER-030) | Re-validated per `redis.call` (`scriptVerifyClusterState`) | A confirmed bug, not a design choice. |
