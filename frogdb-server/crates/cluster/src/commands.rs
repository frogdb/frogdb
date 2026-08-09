//! Command handlers for cluster state mutations.

use crate::types::{
    ClusterCommand, ClusterError, ClusterEvent, ClusterResponse, NodeId, NodeRole, SlotHandoff,
    SlotMigration,
};

use super::state::{ClusterState, ClusterStateInner, EpochReconciliation};

/// The release events a removed [`SlotMigration`] owes.
///
/// Every arm that drops a migration record funnels through here so the
/// invariant holds unconditionally: **a prepared handoff never disappears
/// without a [`ClusterEvent::SlotHandoffReleased`]**. Without it a source that
/// armed its barrier would depend on the barrier timeout to recover from a
/// cancel or a failover prune, parking that slot's writes for the rest of the
/// window for no reason.
fn release_events(migration: SlotMigration) -> Vec<ClusterEvent> {
    let SlotMigration {
        slot,
        source_node,
        handoff,
        ..
    } = migration;
    match handoff {
        Some(h) => vec![ClusterEvent::SlotHandoffReleased {
            slot,
            source_node,
            seq: h.seq,
        }],
        None => Vec::new(),
    }
}

/// Cancel every migration that names `node_id` as source or target, returning
/// the [`release_events`] they owe.
///
/// A migration referencing a node the cluster no longer has can never complete
/// and blocks its slot until someone cancels it, so both arms that drop a node
/// — `RemoveNode` (FM-CLUSTER-002) and `Failover { force: true }`
/// (FM-CLUSTER-036) — call this in the same transition that removes it. Sharing
/// the helper is what keeps the two removal paths from drifting apart: the
/// asymmetry between them *was* the defect.
fn prune_migrations_naming(inner: &mut ClusterStateInner, node_id: NodeId) -> Vec<ClusterEvent> {
    let doomed: Vec<u16> = inner
        .migrations
        .iter()
        .filter(|(_, m)| m.source_node == node_id || m.target_node == node_id)
        .map(|(slot, _)| *slot)
        .collect();
    doomed
        .into_iter()
        .filter_map(|slot| inner.migrations.remove(&slot))
        .flat_map(release_events)
        .collect()
}

/// Point every replica of `old_primary_id` at `new_parent`, skipping the new
/// parent itself so a promoted node is never made its own replica.
///
/// `Failover` passes the successor it validated; `RemoveNode` passes `None`,
/// which *detaches* the replicas — `FORGET` names no successor, and inventing
/// one would hand a replication stream to a node that never held the data, the
/// same reason the departing node's slots are orphaned rather than transferred.
/// A detached replica keeps its role: minting a replication identity is a role
/// transition, and those belong to `SetRole` and `Failover`. Redis does exactly
/// this in `freeClusterNode`, which nulls its slaves' `slaveof` and promotes
/// nobody.
fn reparent_children(
    inner: &mut ClusterStateInner,
    old_primary_id: NodeId,
    new_parent: Option<NodeId>,
) {
    for node in inner.nodes.values_mut() {
        if node.primary_id == Some(old_primary_id) && Some(node.id) != new_parent {
            node.primary_id = new_parent;
        }
    }
}

impl ClusterState {
    /// Apply a command to the state, returning the response and any
    /// [`ClusterEvent`]s the mutation produced.
    ///
    /// Events are pushed only on the `Ok` path of the arm that performs the
    /// corresponding mutation, so a rejected command (an `Err` return) carries
    /// no events at all — emit-on-failure is structurally impossible. The
    /// events are node-agnostic; the node-local self-filter and channel routing
    /// live in [`crate::state::ClusterStateMachine`]'s `apply`.
    ///
    /// The guard republishes the reader snapshot when it drops, so every arm —
    /// including the ones that `return Err` before mutating — leaves
    /// [`ClusterState::snapshot`] agreeing with the state this call produced.
    pub(crate) fn apply_command(
        &self,
        cmd: ClusterCommand,
    ) -> Result<(ClusterResponse, Vec<ClusterEvent>), ClusterError> {
        let mut inner = self.write_inner();
        let outcome = Self::apply_to(&mut inner, cmd);

        // Self-check seam. Every transition — accepted *or* rejected — has to
        // leave the state machine satisfying the HARD invariant catalog, so
        // every test in the crate that applies a command is also an invariant
        // test. The rejected path is checked too: an arm that mutated before
        // deciding to fail would show up here rather than at whatever later
        // command tripped over the debris.
        #[cfg(any(test, debug_assertions))]
        crate::invariants::debug_assert_clean(&inner, "apply_command");

        outcome
    }

    /// The transition function itself, split out so [`Self::apply_command`]
    /// owns the one place the invariant hook can be attached. Arms `return`
    /// freely; the hook still runs, which it could not if the match were
    /// inlined into the caller.
    fn apply_to(
        inner: &mut ClusterStateInner,
        cmd: ClusterCommand,
    ) -> Result<(ClusterResponse, Vec<ClusterEvent>), ClusterError> {
        match cmd {
            ClusterCommand::AddNode { mut node } => {
                let existed = inner.nodes.contains_key(&node.id);
                if existed {
                    tracing::info!(node_id = node.id, addr = %node.addr, "Updated node in cluster");
                } else {
                    tracing::info!(node_id = node.id, addr = %node.addr, "Adding node to cluster");
                }

                // Re-registration refreshes a node's *address*, never its role.
                // A node re-registers itself on every boot (`cluster_init`) with
                // the role it can know without the cluster's help — primary — so
                // taking the incoming role would demote-by-restart: a replica
                // that restarted would silently reappear as a slotless primary,
                // its primary would lose a replica, and its data path would keep
                // streaming from a node the topology no longer links it to.
                // Role transitions belong to `SetRole` and `Failover`; this arm
                // keeps the recorded role for the same reason it keeps the
                // recorded epoch below.
                if let Some(existing) = inner.nodes.get(&node.id) {
                    if existing.role != node.role || existing.primary_id != node.primary_id {
                        tracing::debug!(
                            node_id = node.id,
                            recorded_role = %existing.role,
                            claimed_role = %node.role,
                            "Node re-registered with a different role; kept its recorded role"
                        );
                    }
                    node.role = existing.role;
                    node.primary_id = existing.primary_id;
                }

                // Warn on version mismatch when a node joins or updates
                if !node.version.is_empty() {
                    // Compare against the highest version any other versioned
                    // peer reports. This is *not* a majority or consensus
                    // version — with only two distinct versions present it is
                    // whichever one happens to sort higher — so the log field
                    // below is named for what it actually is.
                    let max_peer_version = inner
                        .nodes
                        .values()
                        .filter(|n| !n.version.is_empty() && n.id != node.id)
                        .map(|n| n.version.as_str())
                        .max();
                    if let Some(max_peer_version) = max_peer_version
                        && node.version != max_peer_version
                    {
                        tracing::warn!(
                            node_id = node.id,
                            node_version = %node.version,
                            max_peer_version = %max_peer_version,
                            "Node joining with different binary version (mixed-version cluster)"
                        );
                    }
                }

                // Resolve the epoch the incoming node claims before it lands in
                // the table: `AddNode` is the one path that carries an epoch the
                // cluster-wide counter did not mint, so it is the one path that
                // can introduce a collision.
                match inner.reconcile_incoming_epoch(&mut node) {
                    EpochReconciliation::Accepted => {}
                    EpochReconciliation::Preserved(epoch) => {
                        tracing::debug!(
                            node_id = node.id,
                            epoch,
                            "Node re-registered without an epoch; kept its recorded config_epoch"
                        );
                    }
                    EpochReconciliation::Reassigned { claimed, assigned } => {
                        tracing::warn!(
                            node_id = node.id,
                            claimed_epoch = claimed,
                            assigned_epoch = assigned,
                            "config_epoch collision on join: claimed epoch is already held by \
                             another primary, assigned a fresh epoch"
                        );
                    }
                }

                inner.nodes.insert(node.id, node);
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::RemoveNode { node_id } => {
                if !inner.nodes.contains_key(&node_id) {
                    return Err(ClusterError::NodeNotFound(node_id));
                }
                // Remove slot assignments for this node. They stay *unassigned*:
                // retargeting them would hand a keyspace to a node that never
                // received its data (FM-CLUSTER-002).
                inner
                    .slot_assignment
                    .retain(|_, &mut owner| owner != node_id);

                // Everything that *references* the departing node goes with it,
                // through the same two helpers the force-failover removal path
                // uses. A retired node must not leave a migration that can never
                // complete or a parent pointer into a hole in the topology.
                let events = prune_migrations_naming(inner, node_id);
                reparent_children(inner, node_id, None);

                inner.nodes.remove(&node_id);
                tracing::info!(node_id, "Removed node from cluster");
                Ok((ClusterResponse::Ok, events))
            }

            ClusterCommand::AssignSlots { node_id, slots } => {
                if !inner.nodes.contains_key(&node_id) {
                    return Err(ClusterError::NodeNotFound(node_id));
                }

                for range in &slots {
                    for slot in range.iter() {
                        if let Some(&existing_owner) = inner.slot_assignment.get(&slot)
                            && existing_owner != node_id
                        {
                            return Err(ClusterError::SlotAlreadyAssigned(slot, existing_owner));
                        }
                    }
                }

                for range in slots {
                    for slot in range.iter() {
                        inner.slot_assignment.insert(slot, node_id);
                    }
                }
                tracing::debug!(node_id, "Assigned slots to node");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::RemoveSlots { node_id, slots } => {
                // Validate every slot is assigned *to the named node* before
                // removing any. Redis' `CLUSTER DELSLOTS` is a local
                // unassignment with no node argument, so ownership is
                // structural there; FrogDB's is replicated and carries an
                // explicit `node_id`, so the ownership assertion has to be
                // made explicitly or a stale id strips another node's slots.
                for range in &slots {
                    for slot in range.iter() {
                        match inner.slot_assignment.get(&slot) {
                            None => return Err(ClusterError::SlotNotAssigned(slot)),
                            Some(&owner) if owner != node_id => {
                                return Err(ClusterError::InvalidOperation(format!(
                                    "slot {} is owned by {}, not {}",
                                    slot, owner, node_id
                                )));
                            }
                            Some(_) => {}
                        }
                    }
                }
                // Remove validated slots
                for range in slots {
                    for slot in range.iter() {
                        inner.slot_assignment.remove(&slot);
                    }
                }
                tracing::debug!(node_id, "Removed slots from node");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::SetRole {
                node_id,
                role,
                primary_id,
            } => {
                // Validate node exists first
                if !inner.nodes.contains_key(&node_id) {
                    return Err(ClusterError::NodeNotFound(node_id));
                }

                // Validate primary_id if setting replica role
                if role == NodeRole::Replica {
                    if let Some(pid) = primary_id {
                        if !inner.nodes.contains_key(&pid) {
                            return Err(ClusterError::NodeNotFound(pid));
                        }
                    } else {
                        return Err(ClusterError::InvalidOperation(
                            "replica requires a primary_id".to_string(),
                        ));
                    }
                }

                // Now we can safely modify
                let node = inner.nodes.get_mut(&node_id).unwrap();
                let was_primary = node.role == NodeRole::Primary;
                node.role = role;
                node.primary_id = primary_id;
                tracing::info!(node_id, ?role, "Set node role");

                // Both events are node-agnostic; the state machine decides
                // whether they apply to *this* node.
                //
                // Replica: emit unconditionally. Re-issuing `SetRole { Replica }`
                // on a node that is already a replica is how a re-parent is
                // expressed, and the data path has to re-point its replication
                // stream at the new primary — so this is not a no-op.
                //
                // Primary: emit only on a real replica→primary transition. A
                // re-applied `SetRole { Primary }` on a node that is already a
                // primary changes nothing, and promoting the data path again
                // would mint a fresh replication ID and force every attached
                // replica into a full resync for no reason.
                let mut events = Vec::new();
                match role {
                    NodeRole::Replica => events.push(ClusterEvent::NodeDemoted {
                        demoted_node_id: node_id,
                        new_primary_id: primary_id,
                        epoch: inner.config_epoch,
                    }),
                    NodeRole::Primary if !was_primary => events.push(ClusterEvent::NodePromoted {
                        promoted_node_id: node_id,
                        epoch: inner.config_epoch,
                    }),
                    NodeRole::Primary => {}
                }
                Ok((ClusterResponse::Ok, events))
            }

            ClusterCommand::IncrementEpoch => {
                inner.config_epoch += 1;
                tracing::debug!(epoch = inner.config_epoch, "Incremented config epoch");
                Ok((ClusterResponse::Epoch(inner.config_epoch), Vec::new()))
            }

            ClusterCommand::SetConfigEpoch { node_id, epoch } => {
                // Redis' two guards, verbatim: a manual epoch assignment is
                // only safe before the node has met anyone and before the
                // cluster has assigned it an authority of its own. Under
                // FrogDB's Raft plane collisions are resolved automatically
                // (FM-CLUSTER-011), so this exists for bootstrap tooling
                // parity rather than as a recovery lever — which is exactly
                // why it stays as narrow as Redis'.
                if !inner.nodes.contains_key(&node_id) {
                    return Err(ClusterError::NodeNotFound(node_id));
                }
                if inner.nodes.len() > 1 {
                    return Err(ClusterError::InvalidOperation(
                        "config epoch can only be set while the node knows no other node"
                            .to_string(),
                    ));
                }
                let node = inner
                    .nodes
                    .get_mut(&node_id)
                    .expect("membership checked above");
                if node.config_epoch != 0 {
                    return Err(ClusterError::InvalidOperation(format!(
                        "node {} config epoch is already non-zero ({})",
                        node_id, node.config_epoch
                    )));
                }
                node.config_epoch = epoch;
                // Keep FM-CLUSTER-010: the cluster counter never sits below
                // any node's epoch.
                inner.config_epoch = inner.config_epoch.max(epoch);
                tracing::info!(node_id, epoch, "Set node config epoch");
                Ok((ClusterResponse::Epoch(epoch), Vec::new()))
            }

            ClusterCommand::Failover {
                old_primary_id,
                new_primary_id,
                force,
            } => {
                // ---- Validation phase: no mutation until every check passes,
                // so the transition is all-or-nothing.
                if old_primary_id == new_primary_id {
                    return Err(ClusterError::InvalidOperation(
                        "failover source and target are the same node".to_string(),
                    ));
                }
                if !inner.nodes.contains_key(&new_primary_id) {
                    return Err(ClusterError::NodeNotFound(new_primary_id));
                }
                let old_exists = inner.nodes.contains_key(&old_primary_id);
                if !old_exists && !force {
                    // Graceful failover demotes the old primary, so it must exist.
                    return Err(ClusterError::NodeNotFound(old_primary_id));
                }

                // ---- Mutation phase (infallible from here).

                // 1. Transfer every slot owned by the old primary to the successor.
                let mut transferred = 0usize;
                for owner in inner.slot_assignment.values_mut() {
                    if *owner == old_primary_id {
                        *owner = new_primary_id;
                        transferred += 1;
                    }
                }

                // 2. Promote the successor (no-op if it is already a primary,
                //    e.g. the absorb path or a replayed retry).
                let successor_was_replica = {
                    let new_node = inner.nodes.get_mut(&new_primary_id).unwrap();
                    let was_replica = new_node.role == NodeRole::Replica;
                    new_node.role = NodeRole::Primary;
                    new_node.primary_id = None;
                    was_replica
                };

                // 3. Apply the old primary's fate. A graceful failover demotes
                //    the old primary (a NodeDemoted event); a force failover
                //    *removes* it, which is not a demotion.
                let graceful_demotion = !force;
                let mut pruned_releases = Vec::new();
                if force {
                    if old_exists {
                        inner.nodes.remove(&old_primary_id);
                    }
                    // Migrations referencing a removed node can never complete
                    // and would block future migrations of those slots. A pruned
                    // migration still owes its release event: a surviving source
                    // that had armed a barrier for a handoff to the removed node
                    // must be told to drop it. `RemoveNode` prunes through the
                    // same helper (FM-CLUSTER-002).
                    pruned_releases = prune_migrations_naming(inner, old_primary_id);
                } else {
                    let old_node = inner.nodes.get_mut(&old_primary_id).unwrap();
                    old_node.role = NodeRole::Replica;
                    old_node.primary_id = Some(new_primary_id);
                }

                // 4. Re-parent the old primary's remaining replicas so they
                //    follow the successor instead of a demoted/removed node.
                reparent_children(inner, old_primary_id, Some(new_primary_id));

                // 5. Bump the config epoch in the same transition and let the
                //    successor claim it, so the new slot ownership can never be
                //    observed at a stale epoch (Redis parity: the promoted
                //    replica claims a new configEpoch and the slot bitmap and
                //    epoch propagate together in one cluster message).
                inner.config_epoch += 1;
                let epoch = inner.config_epoch;
                if let Some(new_node) = inner.nodes.get_mut(&new_primary_id) {
                    new_node.config_epoch = epoch;
                }

                tracing::info!(
                    old_primary = old_primary_id,
                    new_primary = new_primary_id,
                    force,
                    slots_transferred = transferred,
                    epoch,
                    "Applied atomic failover"
                );

                // Order matters on a node that is both: the demotion is the old
                // primary's, the promotion the successor's, and they can never
                // name the same node (the two ids are validated distinct above).
                let mut events: Vec<ClusterEvent> = pruned_releases;
                if graceful_demotion {
                    events.push(ClusterEvent::NodeDemoted {
                        demoted_node_id: old_primary_id,
                        new_primary_id: Some(new_primary_id),
                        epoch,
                    });
                }
                if successor_was_replica {
                    events.push(ClusterEvent::NodePromoted {
                        promoted_node_id: new_primary_id,
                        epoch,
                    });
                }
                Ok((ClusterResponse::Epoch(epoch), events))
            }

            ClusterCommand::MarkNodeFailed { node_id } => {
                let node = inner
                    .nodes
                    .get_mut(&node_id)
                    .ok_or(ClusterError::NodeNotFound(node_id))?;
                node.flags.fail = true;
                // Topology-visibility change: bump the epoch in the same
                // transition so other nodes never observe the FAIL flag at a
                // stale epoch (previously a separate IncrementEpoch entry that
                // could be lost on leader crash).
                inner.config_epoch += 1;
                tracing::warn!(node_id, epoch = inner.config_epoch, "Marked node as failed");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::MarkNodeRecovered { node_id } => {
                let node = inner
                    .nodes
                    .get_mut(&node_id)
                    .ok_or(ClusterError::NodeNotFound(node_id))?;
                node.flags.fail = false;
                node.flags.pfail = false;
                tracing::info!(node_id, "Marked node as recovered");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::BeginSlotMigration {
                slot,
                source_node,
                target_node,
            } => {
                // Idempotent: if the exact same migration is already in progress, succeed.
                if let Some(existing) = inner.migrations.get(&slot) {
                    if existing.source_node == source_node && existing.target_node == target_node {
                        return Ok((ClusterResponse::Ok, Vec::new()));
                    }
                    return Err(ClusterError::MigrationInProgress(slot));
                }

                if !inner.nodes.contains_key(&source_node) {
                    return Err(ClusterError::NodeNotFound(source_node));
                }
                if !inner.nodes.contains_key(&target_node) {
                    return Err(ClusterError::NodeNotFound(target_node));
                }

                // Slot assignment may be empty on follower nodes (assigned locally
                // on bootstrap, not via Raft), so only validate if present.
                if let Some(&owner) = inner.slot_assignment.get(&slot)
                    && owner != source_node
                {
                    return Err(ClusterError::InvalidOperation(format!(
                        "slot {} is owned by {}, not {}",
                        slot, owner, source_node
                    )));
                }

                inner
                    .migrations
                    .insert(slot, SlotMigration::new(slot, source_node, target_node));
                tracing::info!(slot, source_node, target_node, "Started slot migration");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::PrepareSlotHandoff {
                slot,
                source_node,
                target_node,
                barrier_ms,
                lease_ms,
                proposed_at_ms,
            } => {
                let migration = inner.migrations.get(&slot).ok_or_else(|| {
                    ClusterError::HandoffNotReady(slot, "no migration in progress".to_string())
                })?;

                if migration.source_node != source_node || migration.target_node != target_node {
                    return Err(ClusterError::InvalidOperation(
                        "migration parameters don't match".to_string(),
                    ));
                }

                // One live prepare at a time per slot. A prepare whose finalizer
                // died is *not* live — the lease is exactly what lets the next
                // attempt through without an operator having to clear anything.
                if let Some(live) = migration.live_handoff_at(proposed_at_ms) {
                    return Err(ClusterError::HandoffNotReady(
                        slot,
                        format!("handoff seq {} already prepared", live.seq),
                    ));
                }

                inner.handoff_seq += 1;
                let seq = inner.handoff_seq;
                let migration = inner
                    .migrations
                    .get_mut(&slot)
                    .expect("migration presence checked above");
                migration.handoff = Some(SlotHandoff {
                    seq,
                    prepared_at_ms: proposed_at_ms,
                    barrier_ms,
                    lease_ms,
                    drained: false,
                });
                tracing::info!(
                    slot,
                    source_node,
                    target_node,
                    seq,
                    barrier_ms,
                    lease_ms,
                    "Prepared slot handoff"
                );
                Ok((
                    ClusterResponse::Ok,
                    vec![ClusterEvent::SlotHandoffPrepared {
                        slot,
                        source_node,
                        target_node,
                        seq,
                        barrier_ms,
                    }],
                ))
            }

            ClusterCommand::ConfirmSlotHandoffDrained { slot, seq } => {
                // Deliberately no expiry check: the confirmation carries no
                // timestamp because it does not need one. `CompleteSlotMigration`
                // re-checks both the barrier window and the lease against its own
                // proposer timestamp, so marking a lapsed handoff drained can
                // never let ownership move.
                let handoff = inner
                    .migrations
                    .get_mut(&slot)
                    .and_then(|m| m.handoff.as_mut())
                    .filter(|h| h.seq == seq)
                    .ok_or_else(|| {
                        ClusterError::HandoffNotReady(
                            slot,
                            format!("no prepared handoff with seq {}", seq),
                        )
                    })?;

                handoff.drained = true;
                tracing::debug!(slot, seq, "Slot handoff drained");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::AbortSlotHandoff { slot, seq } => {
                // Idempotent: aborting a handoff that is already gone succeeds
                // silently, so a finalizer that crashes after proposing the abort
                // can safely re-propose it on restart.
                let released = inner
                    .migrations
                    .get_mut(&slot)
                    .filter(|m| m.handoff.as_ref().is_some_and(|h| h.seq == seq))
                    .map(|m| {
                        let source_node = m.source_node;
                        m.handoff = None;
                        source_node
                    });

                let events = match released {
                    Some(source_node) => {
                        tracing::info!(slot, seq, "Aborted slot handoff; migration left intact");
                        vec![ClusterEvent::SlotHandoffReleased {
                            slot,
                            source_node,
                            seq,
                        }]
                    }
                    None => Vec::new(),
                };
                Ok((ClusterResponse::Ok, events))
            }

            ClusterCommand::CompleteSlotMigration {
                slot,
                source_node,
                target_node,
                proposed_at_ms,
            } => {
                let migration =
                    inner
                        .migrations
                        .get(&slot)
                        .ok_or(ClusterError::InvalidOperation(format!(
                            "no migration in progress for slot {}",
                            slot
                        )))?;

                if migration.source_node != source_node || migration.target_node != target_node {
                    return Err(ClusterError::InvalidOperation(
                        "migration parameters don't match".to_string(),
                    ));
                }

                // The slot map is not consulted here — the migration record
                // authorizes the transfer — but the node table is. Membership was
                // checked at begin time (FM-CLUSTER-032) and a record can outlive
                // it: a snapshot installed from a leader on an older binary
                // carries migrations whose endpoints that binary never pruned. An
                // owner that is not a member is a slot nothing can be redirected
                // to, and the coverage readers count it as healthy
                // (FM-CLUSTER-073). Only the target is checked: it is the id this
                // arm writes into the slot map.
                if !inner.nodes.contains_key(&target_node) {
                    return Err(ClusterError::NodeNotFound(target_node));
                }

                // Ownership moves only under a prepared, drained, still-armed
                // handoff. A `Complete` that raced past the barrier deadline is
                // refused outright rather than half-applied: by then the source
                // has resumed serving the slot, so moving ownership would strand
                // exactly the writes this barrier exists to fence.
                let seq = match &migration.handoff {
                    Some(h) if h.admits_complete_at(proposed_at_ms) => h.seq,
                    Some(h) => {
                        let why = if !h.drained {
                            "handoff not drained"
                        } else if h.lease_expired_at(proposed_at_ms) {
                            "handoff lease expired"
                        } else {
                            "handoff barrier window elapsed"
                        };
                        return Err(ClusterError::HandoffNotReady(slot, why.to_string()));
                    }
                    None => {
                        return Err(ClusterError::HandoffNotReady(
                            slot,
                            "no prepared handoff".to_string(),
                        ));
                    }
                };

                // Transfer slot ownership
                inner.slot_assignment.insert(slot, target_node);
                inner.migrations.remove(&slot);
                tracing::info!(
                    slot,
                    source_node,
                    target_node,
                    seq,
                    "Completed slot migration"
                );
                Ok((
                    ClusterResponse::Ok,
                    vec![
                        ClusterEvent::SlotMigrationCompleted {
                            slot,
                            source_node,
                            target_node,
                        },
                        ClusterEvent::SlotHandoffReleased {
                            slot,
                            source_node,
                            seq,
                        },
                    ],
                ))
            }

            ClusterCommand::CancelSlotMigration { slot } => {
                let events = inner
                    .migrations
                    .remove(&slot)
                    .map(release_events)
                    .unwrap_or_default();
                tracing::info!(slot, "Cancelled slot migration");
                Ok((ClusterResponse::Ok, events))
            }

            ClusterCommand::FinalizeUpgrade { version } => {
                // Parse the target *before* touching state, and outside the
                // per-node loop it is invariant over. Inside the loop it is
                // never reached on an empty cluster, which stored an
                // unparseable string as `active_version` — and since
                // `is_gate_active_in` treats an unparseable active version as
                // inactive (FM-CLUSTER-009), every gate would be silently off
                // with no way back short of `CLUSTER RESET HARD`.
                let target_ver = semver::Version::parse(&version).map_err(|e| {
                    ClusterError::InvalidOperation(format!(
                        "invalid target version '{}': {}",
                        version, e
                    ))
                })?;

                // Validate all nodes report a version >= target
                for (node_id, node) in &inner.nodes {
                    if node.version.is_empty() {
                        return Err(ClusterError::InvalidOperation(format!(
                            "node {} has no version info (pre-versioning binary)",
                            node_id
                        )));
                    }
                    let node_ver = semver::Version::parse(&node.version).map_err(|e| {
                        ClusterError::InvalidOperation(format!(
                            "node {} has invalid version '{}': {}",
                            node_id, node.version, e
                        ))
                    })?;
                    if node_ver < target_ver {
                        return Err(ClusterError::InvalidOperation(format!(
                            "node {} is at version {} but finalization requires {}",
                            node_id, node.version, version
                        )));
                    }
                }

                tracing::info!(version = %version, "Finalizing upgrade — active version advanced");
                inner.active_version = Some(version);
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::ResetCluster {
                node_id,
                new_node_id,
            } => {
                // Clear all slot assignments
                inner.slot_assignment.clear();

                // Clear all migrations, still paying the release events any
                // prepared handoffs owe so a reset cannot leave this node's
                // slot barriers armed until they time out.
                let reset_events: Vec<ClusterEvent> = std::mem::take(&mut inner.migrations)
                    .into_values()
                    .flat_map(release_events)
                    .collect();
                inner.handoff_seq = 0;

                // Remove all nodes except this one, and ensure it's a primary
                if let Some(mut this_node) = inner.nodes.remove(&node_id) {
                    this_node.role = NodeRole::Primary;
                    this_node.primary_id = None;

                    inner.nodes.clear();

                    if let Some(new_id) = new_node_id {
                        // HARD: reset epoch and assign new node ID
                        inner.config_epoch = 0;
                        this_node.id = new_id;
                        this_node.config_epoch = 0;
                        inner.nodes.insert(new_id, this_node);
                        tracing::info!(
                            old_node_id = node_id,
                            new_node_id = new_id,
                            "HARD cluster reset"
                        );
                    } else {
                        // SOFT: keep same node ID and epoch
                        inner.nodes.insert(node_id, this_node);
                        tracing::info!(node_id, "SOFT cluster reset");
                    }
                } else {
                    // Node not found - clear everything anyway
                    inner.nodes.clear();
                    tracing::warn!(node_id, "Cluster reset: node not found in state");
                }

                Ok((ClusterResponse::Ok, reset_events))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NodeInfo, SlotRange};
    use std::net::SocketAddr;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    /// Seed `count` primaries with ids `1..=count`.
    fn state_with_primaries(count: u64) -> ClusterState {
        let state = ClusterState::new();
        for id in 1..=count {
            let port = 6379 + id as u16;
            state
                .apply_local(ClusterCommand::AddNode {
                    node: NodeInfo::new_primary(id, test_addr(port), test_addr(port + 10_000)),
                })
                .expect("seeding a primary must succeed");
        }
        state
    }

    fn assign(state: &ClusterState, node_id: u64, start: u16, end: u16) {
        state
            .apply_local(ClusterCommand::AssignSlots {
                node_id,
                slots: vec![SlotRange::new(start, end)],
            })
            .expect("seeding slots must succeed");
    }

    // FM-CLUSTER-002
    #[test]
    fn remove_node_prunes_migrations_and_detaches_replicas() {
        let state = state_with_primaries(2);
        // A replica parented to the node that is about to be forgotten.
        state
            .apply_local(ClusterCommand::AddNode {
                node: NodeInfo::new_replica(3, test_addr(6382), test_addr(16382), 1),
            })
            .unwrap();
        assign(&state, 1, 0, 10);
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();

        state
            .apply_command(ClusterCommand::RemoveNode { node_id: 1 })
            .expect("forgetting a member must succeed");

        // Specced: the slots are orphaned rather than transferred.
        assert_eq!(state.get_slot_owner(0), None, "slots must be orphaned");
        assert_eq!(state.get_slot_owner(10), None);
        assert!(state.get_node(1).is_none());

        // Every reference to the departed node goes with it: the migration it
        // sourced could never complete, and its child's parent pointer would
        // render a dangling master id.
        assert!(
            state.get_slot_migration(5).is_none(),
            "the migration naming the removed source must be pruned"
        );
        assert!(!state.is_slot_migrating(5));
        let child = state.get_node(3).expect("the replica itself survives");
        assert_eq!(child.primary_id, None, "the replica is detached");
        assert_eq!(
            child.role,
            NodeRole::Replica,
            "detaching is not a promotion — FORGET is not a role transition"
        );
    }

    /// The target side of the same prune, and the release the pruned handoff
    /// owes: a source that armed its barrier for a target that has just been
    /// forgotten must be told to drop it rather than wait out the window.
    // FM-CLUSTER-002
    #[test]
    fn remove_node_prunes_a_migration_that_only_targets_it_and_releases_its_barrier() {
        let state = state_with_primaries(2);
        assign(&state, 1, 0, 10);
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();
        state.arm_handoff_for_test(5, 1, 2);
        let seq = state
            .get_slot_migration(5)
            .and_then(|m| m.handoff)
            .expect("the handoff must be armed")
            .seq;

        // Node 2 is only the *target* of the migration, and owns no slots.
        let (_, events) = state
            .apply_command(ClusterCommand::RemoveNode { node_id: 2 })
            .expect("forgetting a member must succeed");

        assert!(
            state.get_slot_migration(5).is_none(),
            "the migration naming the removed target must be pruned"
        );
        assert_eq!(
            events,
            vec![ClusterEvent::SlotHandoffReleased {
                slot: 5,
                source_node: 1,
                seq,
            }],
            "the surviving source is told to drop the barrier it armed"
        );
        assert_eq!(
            state.get_slot_owner(5),
            Some(1),
            "the source keeps the slot it never handed over"
        );
    }

    /// The prune is targeted, not a sweep: state that does not name the
    /// departing node survives it untouched.
    // FM-CLUSTER-002
    #[test]
    fn remove_node_keeps_migrations_and_parents_that_do_not_name_it() {
        let state = state_with_primaries(3);
        state
            .apply_local(ClusterCommand::AddNode {
                node: NodeInfo::new_replica(4, test_addr(6383), test_addr(16383), 2),
            })
            .unwrap();
        assign(&state, 2, 0, 10);
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 2,
                target_node: 3,
            })
            .unwrap();

        let (_, events) = state
            .apply_command(ClusterCommand::RemoveNode { node_id: 1 })
            .expect("forgetting a member must succeed");

        assert!(events.is_empty(), "nothing was pruned, so nothing released");
        let migration = state
            .get_slot_migration(5)
            .expect("a migration between two survivors is none of the removal's business");
        assert_eq!((migration.source_node, migration.target_node), (2, 3));
        assert_eq!(
            state.get_node(4).unwrap().primary_id,
            Some(2),
            "a replica of another primary keeps its parent"
        );
    }

    // FM-CLUSTER-003
    #[test]
    fn assign_slots_rejects_the_whole_batch_on_one_conflict() {
        let state = state_with_primaries(2);
        assign(&state, 1, 5, 6);

        // Node 2 asks for a clean range *and* a conflicting one, clean range
        // first so a mutate-as-you-validate implementation would have already
        // committed it by the time it hit the conflict.
        let err = state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 2,
                slots: vec![SlotRange::new(20, 30), SlotRange::new(5, 6)],
            })
            .expect_err("a batch containing a conflict must be refused");
        assert!(
            matches!(err, ClusterError::SlotAlreadyAssigned(5, 1)),
            "expected SlotAlreadyAssigned(5, 1), got {err:?}"
        );

        assert_eq!(
            state.get_slot_owner(20),
            None,
            "the clean prefix of the batch must not have been applied"
        );
        assert_eq!(state.get_slot_owner(30), None);
        assert_eq!(state.get_slot_owner(5), Some(1), "the incumbent keeps it");
    }

    // FM-CLUSTER-004
    #[test]
    fn remove_slots_rejects_a_slot_owned_by_another_node() {
        let state = state_with_primaries(2);
        assign(&state, 1, 0, 10);

        // Node 2 asks to remove slots node 1 owns. The ownership assertion
        // refuses it; the slots stay with their incumbent.
        let err = state
            .apply_command(ClusterCommand::RemoveSlots {
                node_id: 2,
                slots: vec![SlotRange::new(0, 10)],
            })
            .expect_err("removing another node's slots must be refused");
        match err {
            ClusterError::InvalidOperation(msg) => {
                assert!(msg.contains("owned by 1"), "{msg}");
                assert!(msg.contains("not 2"), "{msg}");
            }
            other => panic!("expected InvalidOperation, got {other:?}"),
        }
        assert_eq!(state.get_slot_owner(5), Some(1), "the incumbent keeps it");

        // The owner itself is still allowed to remove them.
        state
            .apply_command(ClusterCommand::RemoveSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 10)],
            })
            .expect("the owner may unassign its own slots");
        assert_eq!(state.get_slot_owner(5), None);
    }

    // FM-CLUSTER-004
    #[test]
    fn remove_slots_rejects_the_whole_batch_when_one_slot_is_foreign() {
        let state = state_with_primaries(2);
        assign(&state, 1, 0, 2);
        assign(&state, 2, 3, 3);

        // Node 1 owns 0..=2 and node 2 owns slot 3. A batch spanning both is
        // refused as a whole — the validate-all-then-apply shape of
        // FM-CLUSTER-003 must survive the ownership check.
        let err = state
            .apply_command(ClusterCommand::RemoveSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 3)],
            })
            .expect_err("a foreign slot in the batch must refuse the batch");
        assert!(matches!(err, ClusterError::InvalidOperation(_)), "{err:?}");
        assert_eq!(
            state.get_slot_owner(0),
            Some(1),
            "the refused batch must not have removed its valid prefix"
        );
        assert_eq!(state.get_slot_owner(3), Some(2));

        // Unassigned slots are still the `SlotNotAssigned` refusal, and still
        // all-or-nothing.
        let err = state
            .apply_command(ClusterCommand::RemoveSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 2), SlotRange::single(9)],
            })
            .expect_err("an unassigned slot in the batch must refuse the batch");
        assert!(matches!(err, ClusterError::SlotNotAssigned(9)), "{err:?}");
        assert_eq!(state.get_slot_owner(0), Some(1));
    }

    // FM-CLUSTER-076
    #[test]
    fn set_config_epoch_assigns_the_exact_value_requested() {
        let state = state_with_primaries(1);
        assert_eq!(state.get_node(1).unwrap().config_epoch, 0);

        let (resp, events) = state
            .apply_command(ClusterCommand::SetConfigEpoch {
                node_id: 1,
                epoch: 100,
            })
            .expect("a lone node at epoch 0 must accept the assignment");

        // Exactly 100 — not 1, which is what a bump would have produced.
        assert!(matches!(resp, ClusterResponse::Epoch(100)), "{resp:?}");
        assert!(events.is_empty(), "an epoch assignment emits no events");
        assert_eq!(state.get_node(1).unwrap().config_epoch, 100);
        // FM-CLUSTER-010: the counter is ratcheted up to the assignment.
        assert_eq!(state.config_epoch(), 100);
    }

    // FM-CLUSTER-076
    #[test]
    fn set_config_epoch_never_lowers_the_cluster_counter() {
        let state = state_with_primaries(1);
        state.apply_local(ClusterCommand::IncrementEpoch).unwrap();
        state.apply_local(ClusterCommand::IncrementEpoch).unwrap();
        assert_eq!(state.config_epoch(), 2);

        state
            .apply_command(ClusterCommand::SetConfigEpoch {
                node_id: 1,
                epoch: 1,
            })
            .expect("assigning below the counter is still a valid assignment");

        assert_eq!(state.get_node(1).unwrap().config_epoch, 1);
        assert_eq!(
            state.config_epoch(),
            2,
            "the counter must not follow an assignment downwards"
        );
    }

    // FM-CLUSTER-076
    #[test]
    fn set_config_epoch_refused_once_the_node_knows_another_node() {
        let state = state_with_primaries(2);

        let err = state
            .apply_command(ClusterCommand::SetConfigEpoch {
                node_id: 1,
                epoch: 100,
            })
            .expect_err("a node that knows a peer must refuse a manual assignment");
        assert!(
            matches!(&err, ClusterError::InvalidOperation(m) if m.contains("knows no other node")),
            "{err:?}"
        );
        assert_eq!(state.get_node(1).unwrap().config_epoch, 0);
        assert_eq!(state.config_epoch(), 0);
    }

    // FM-CLUSTER-076
    #[test]
    fn set_config_epoch_refused_once_the_node_holds_an_epoch() {
        let state = state_with_primaries(1);
        state
            .apply_local(ClusterCommand::SetConfigEpoch {
                node_id: 1,
                epoch: 7,
            })
            .unwrap();

        let err = state
            .apply_command(ClusterCommand::SetConfigEpoch {
                node_id: 1,
                epoch: 100,
            })
            .expect_err("a node already holding an epoch must refuse a reassignment");
        assert!(
            matches!(&err, ClusterError::InvalidOperation(m) if m.contains("already non-zero")),
            "{err:?}"
        );
        assert_eq!(
            state.get_node(1).unwrap().config_epoch,
            7,
            "the refusal must leave the held epoch alone"
        );
    }

    // FM-CLUSTER-076
    #[test]
    fn set_config_epoch_on_an_unknown_node_is_not_found() {
        let state = state_with_primaries(1);

        let err = state
            .apply_command(ClusterCommand::SetConfigEpoch {
                node_id: 42,
                epoch: 100,
            })
            .expect_err("an unknown node id must not be silently created");
        assert!(matches!(err, ClusterError::NodeNotFound(42)), "{err:?}");
        assert_eq!(state.get_node(1).unwrap().config_epoch, 0);
        assert_eq!(state.config_epoch(), 0);
    }

    // FM-CLUSTER-032
    #[test]
    fn begin_migration_accepts_an_unassigned_slot() {
        let state = state_with_primaries(2);
        // No AssignSlots at all: a follower's slot map is legitimately empty,
        // because bootstrap seeds slots locally rather than through Raft.
        assert_eq!(state.get_slot_owner(100), None);

        state
            .apply_command(ClusterCommand::BeginSlotMigration {
                slot: 100,
                source_node: 1,
                target_node: 2,
            })
            .expect("the owner check must be skipped when the slot has no owner");

        assert!(state.is_slot_migrating(100));
        let migration = state.get_slot_migration(100).unwrap();
        assert_eq!((migration.source_node, migration.target_node), (1, 2));
    }

    // FM-CLUSTER-033
    #[test]
    fn complete_migration_transfers_ownership_over_an_existing_owner() {
        let state = state_with_primaries(2);
        assign(&state, 1, 0, 10);
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();
        let epoch_before = state.config_epoch();

        // Slot 5 is still recorded as owned by node 1. Completion inserts over
        // that owner without consulting the map — the parameter match against
        // the recorded migration is what authorizes it.
        let proposed_at_ms = state.arm_handoff_for_test(5, 1, 2);
        let (response, events) = state
            .apply_command(ClusterCommand::CompleteSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
                proposed_at_ms,
            })
            .expect("a matching completion must not hit SlotAlreadyAssigned");
        assert!(matches!(response, ClusterResponse::Ok));
        assert_eq!(
            events.len(),
            2,
            "one completion event plus the handoff release"
        );

        assert_eq!(state.get_slot_owner(5), Some(2), "ownership moved");
        assert_eq!(state.get_slot_owner(4), Some(1), "neighbours untouched");
        assert!(!state.is_slot_migrating(5), "the migration record is gone");
        assert_eq!(
            state.config_epoch(),
            epoch_before,
            "completion is authorized by the migration record, not a new epoch"
        );
    }

    /// The slot map is not consulted, but the node table is: an owner that is
    /// not a member is a ghost the coverage readers would count as healthy
    /// (FM-CLUSTER-073) while no client could ever be redirected to it.
    ///
    /// The state is built by installing a doctored snapshot rather than by
    /// `RemoveNode`, which prunes the migration itself (FM-CLUSTER-002) — this
    /// guard exists for the shape a snapshot from a leader on an older binary
    /// still presents.
    // FM-CLUSTER-033
    #[test]
    fn complete_migration_refuses_a_target_that_is_no_longer_a_member() {
        let state = state_with_primaries(2);
        assign(&state, 1, 0, 10);
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();
        let proposed_at_ms = state.arm_handoff_for_test(5, 1, 2);

        // A snapshot carrying the migration but not its target. The catalog
        // calls that shape malformed (INV-REF-2) and both restore vehicles
        // assert against it, so the fixture is handed straight to the
        // transition function instead of being installed through a
        // `ClusterState`: the guard under test is a property of the
        // transition, and a negative fixture is the only way to reach it.
        let mut inner = (*state.read_inner()).clone();
        inner.nodes.remove(&2);

        let err = ClusterState::apply_to(
            &mut inner,
            ClusterCommand::CompleteSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
                proposed_at_ms,
            },
        )
        .expect_err("ownership must not move onto a node that is not a member");
        assert!(matches!(err, ClusterError::NodeNotFound(2)), "{err:?}");
        assert_eq!(
            inner.slot_assignment.get(&5),
            Some(&1),
            "the slot map must not name a ghost"
        );
        assert!(
            inner.migrations.contains_key(&5),
            "a refused completion mutates nothing at all"
        );
    }

    // ---- Two-phase slot handoff (rework issue 02) --------------------------

    /// Wall-clock instant every handoff test mints its deadlines from. Any
    /// value works — the state machine only ever compares two data values.
    const T0: u64 = 1_000_000;

    /// Seed two primaries, give node 1 slots 0..=10, and open a migration of
    /// `slot` from node 1 to node 2.
    fn migrating_state(slot: u16) -> ClusterState {
        let state = state_with_primaries(2);
        assign(&state, 1, 0, 10);
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot,
                source_node: 1,
                target_node: 2,
            })
            .expect("begin must succeed");
        state
    }

    fn prepare(state: &ClusterState, slot: u16, at: u64) -> Vec<ClusterEvent> {
        state
            .apply_command(ClusterCommand::PrepareSlotHandoff {
                slot,
                source_node: 1,
                target_node: 2,
                barrier_ms: crate::types::HANDOFF_BARRIER_MS,
                lease_ms: crate::types::HANDOFF_LEASE_MS,
                proposed_at_ms: at,
            })
            .expect("prepare must succeed")
            .1
    }

    fn complete_at(
        state: &ClusterState,
        slot: u16,
        at: u64,
    ) -> Result<(ClusterResponse, Vec<ClusterEvent>), ClusterError> {
        state.apply_command(ClusterCommand::CompleteSlotMigration {
            slot,
            source_node: 1,
            target_node: 2,
            proposed_at_ms: at,
        })
    }

    /// The whole point of phase one: ownership does not move until the source
    /// has been told to quiesce and has said it did.
    // FM-CLUSTER-084
    #[test]
    fn complete_is_refused_without_a_prepared_handoff() {
        let state = migrating_state(5);
        let err = complete_at(&state, 5, T0).expect_err("no prepare, no completion");
        assert!(
            matches!(&err, ClusterError::HandoffNotReady(5, why) if why == "no prepared handoff"),
            "{err:?}"
        );
        assert_eq!(state.get_slot_owner(5), Some(1), "ownership stayed put");
        assert!(state.is_slot_migrating(5), "the migration record survived");
    }

    // FM-CLUSTER-084
    #[test]
    fn complete_is_refused_while_the_handoff_is_undrained() {
        let state = migrating_state(5);
        prepare(&state, 5, T0);
        let err = complete_at(&state, 5, T0 + 1).expect_err("undrained handoff must not complete");
        assert!(
            matches!(&err, ClusterError::HandoffNotReady(5, why) if why == "handoff not drained"),
            "{err:?}"
        );
        assert_eq!(state.get_slot_owner(5), Some(1));
    }

    // FM-CLUSTER-084
    #[test]
    fn prepare_then_drain_then_complete_moves_ownership() {
        let state = migrating_state(5);
        let events = prepare(&state, 5, T0);
        assert_eq!(
            events,
            vec![ClusterEvent::SlotHandoffPrepared {
                slot: 5,
                source_node: 1,
                target_node: 2,
                seq: 1,
                barrier_ms: crate::types::HANDOFF_BARRIER_MS,
            }]
        );
        state
            .apply_command(ClusterCommand::ConfirmSlotHandoffDrained { slot: 5, seq: 1 })
            .expect("confirm must succeed");

        let (_, events) = complete_at(&state, 5, T0 + 10).expect("a drained handoff completes");
        assert_eq!(
            events,
            vec![
                ClusterEvent::SlotMigrationCompleted {
                    slot: 5,
                    source_node: 1,
                    target_node: 2,
                },
                ClusterEvent::SlotHandoffReleased {
                    slot: 5,
                    source_node: 1,
                    seq: 1,
                },
            ],
            "completion always pays the release the source is waiting for"
        );
        assert_eq!(state.get_slot_owner(5), Some(2));
    }

    /// The hazard the measurement exposed: a `Complete` that lands after the
    /// source's barrier lapsed would move ownership out from under writes the
    /// source has already resumed serving.
    // FM-CLUSTER-084
    #[test]
    fn complete_is_refused_once_the_barrier_window_elapsed() {
        let state = migrating_state(5);
        prepare(&state, 5, T0);
        state
            .apply_command(ClusterCommand::ConfirmSlotHandoffDrained { slot: 5, seq: 1 })
            .unwrap();

        let late = T0 + crate::types::HANDOFF_BARRIER_MS;
        let err = complete_at(&state, 5, late).expect_err("a late completion must be refused");
        assert!(
            matches!(&err, ClusterError::HandoffNotReady(5, why)
                if why == "handoff barrier window elapsed"),
            "{err:?}"
        );
        assert_eq!(state.get_slot_owner(5), Some(1), "ownership stayed put");
        assert!(
            state.is_slot_migrating(5),
            "and the migration is still retryable"
        );
        assert!(err.is_retryable());
    }

    // FM-CLUSTER-085
    #[test]
    fn complete_is_refused_once_the_lease_expired() {
        let state = migrating_state(5);
        prepare(&state, 5, T0);
        state
            .apply_command(ClusterCommand::ConfirmSlotHandoffDrained { slot: 5, seq: 1 })
            .unwrap();

        // Past the lease is also past the barrier; the lease is reported
        // because it is the stronger statement (the record itself is dead).
        let err = complete_at(&state, 5, T0 + crate::types::HANDOFF_LEASE_MS)
            .expect_err("an expired lease must not complete");
        assert!(
            matches!(&err, ClusterError::HandoffNotReady(5, why) if why == "handoff lease expired"),
            "{err:?}"
        );
    }

    /// A second finalizer cannot barge in on a live prepare, but once the lease
    /// has run out — the finalizer died — the next attempt goes through with no
    /// operator intervention.
    // FM-CLUSTER-085
    #[test]
    fn a_second_prepare_waits_for_the_lease_but_not_forever() {
        let state = migrating_state(5);
        prepare(&state, 5, T0);

        let err = state
            .apply_command(ClusterCommand::PrepareSlotHandoff {
                slot: 5,
                source_node: 1,
                target_node: 2,
                barrier_ms: crate::types::HANDOFF_BARRIER_MS,
                lease_ms: crate::types::HANDOFF_LEASE_MS,
                proposed_at_ms: T0 + 1,
            })
            .expect_err("a live prepare owns the slot");
        assert!(
            matches!(&err, ClusterError::HandoffNotReady(5, why)
                if why == "handoff seq 1 already prepared"),
            "{err:?}"
        );

        let events = prepare(&state, 5, T0 + crate::types::HANDOFF_LEASE_MS);
        assert!(
            matches!(
                events.as_slice(),
                [ClusterEvent::SlotHandoffPrepared { seq: 2, .. }]
            ),
            "the expired record is superseded, not cleared by hand: {events:?}"
        );
    }

    /// The stale-ack hazard: attempt #1's drain confirmation must not vouch for
    /// attempt #2, which was never drained.
    // FM-CLUSTER-086
    #[test]
    fn a_stale_drain_ack_cannot_vouch_for_the_next_attempt() {
        let state = migrating_state(5);
        prepare(&state, 5, T0);
        state
            .apply_command(ClusterCommand::AbortSlotHandoff { slot: 5, seq: 1 })
            .expect("abort must succeed");
        prepare(&state, 5, T0 + 1);

        let err = state
            .apply_command(ClusterCommand::ConfirmSlotHandoffDrained { slot: 5, seq: 1 })
            .expect_err("attempt 1's ack must not confirm attempt 2");
        assert!(
            matches!(&err, ClusterError::HandoffNotReady(5, why)
                if why == "no prepared handoff with seq 1"),
            "{err:?}"
        );
        let err = complete_at(&state, 5, T0 + 2).expect_err("attempt 2 is still undrained");
        assert!(
            matches!(&err, ClusterError::HandoffNotReady(5, why) if why == "handoff not drained"),
            "{err:?}"
        );
    }

    /// Abort is the drain-timeout path: it drops the barrier and leaves the
    /// migration exactly where it was, so the operator retries rather than
    /// re-opening the migration.
    // FM-CLUSTER-087
    #[test]
    fn abort_releases_the_barrier_and_keeps_the_migration() {
        let state = migrating_state(5);
        prepare(&state, 5, T0);

        let (_, events) = state
            .apply_command(ClusterCommand::AbortSlotHandoff { slot: 5, seq: 1 })
            .expect("abort must succeed");
        assert_eq!(
            events,
            vec![ClusterEvent::SlotHandoffReleased {
                slot: 5,
                source_node: 1,
                seq: 1,
            }]
        );
        let migration = state.get_slot_migration(5).expect("migration intact");
        assert_eq!((migration.source_node, migration.target_node), (1, 2));
        assert!(migration.handoff.is_none());
        assert_eq!(state.get_slot_owner(5), Some(1));

        // Idempotent: a finalizer that crashed after proposing may re-propose.
        let (_, events) = state
            .apply_command(ClusterCommand::AbortSlotHandoff { slot: 5, seq: 1 })
            .expect("a repeated abort must succeed");
        assert!(events.is_empty(), "and emits no second release");
    }

    /// `CLUSTER SETSLOT STABLE` mid-handoff. FM-006/FM-010: the prepared record
    /// goes away with the migration, and the source is told to drop its barrier
    /// rather than waiting out the timeout.
    // FM-CLUSTER-087
    #[test]
    fn cancel_releases_a_prepared_handoff() {
        let state = migrating_state(5);
        prepare(&state, 5, T0);

        let (_, events) = state
            .apply_command(ClusterCommand::CancelSlotMigration { slot: 5 })
            .expect("cancel must succeed");
        assert_eq!(
            events,
            vec![ClusterEvent::SlotHandoffReleased {
                slot: 5,
                source_node: 1,
                seq: 1,
            }]
        );
        assert!(!state.is_slot_migrating(5));
        assert_eq!(state.get_slot_owner(5), Some(1), "ownership never moved");
    }

    // FM-CLUSTER-087
    #[test]
    fn cancel_without_a_handoff_emits_nothing() {
        let state = migrating_state(5);
        let (_, events) = state
            .apply_command(ClusterCommand::CancelSlotMigration { slot: 5 })
            .expect("cancel must succeed");
        assert!(events.is_empty());
    }

    /// A force failover that prunes a migration owes the surviving source its
    /// release: node 2 disappears, but node 1 is the one holding the barrier.
    // FM-CLUSTER-087
    #[test]
    fn force_failover_releases_the_handoffs_it_prunes() {
        let state = state_with_primaries(3);
        assign(&state, 1, 0, 10);
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();
        prepare(&state, 5, T0);

        let (_, events) = state
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 2,
                new_primary_id: 3,
                force: true,
            })
            .expect("force failover must succeed");
        assert!(
            events.contains(&ClusterEvent::SlotHandoffReleased {
                slot: 5,
                source_node: 1,
                seq: 1,
            }),
            "the pruned migration owed a release: {events:?}"
        );
        assert!(!state.is_slot_migrating(5));
    }

    // FM-CLUSTER-087
    #[test]
    fn reset_releases_prepared_handoffs() {
        let state = migrating_state(5);
        prepare(&state, 5, T0);

        let (_, events) = state
            .apply_command(ClusterCommand::ResetCluster {
                node_id: 1,
                new_node_id: None,
            })
            .expect("reset must succeed");
        assert_eq!(
            events,
            vec![ClusterEvent::SlotHandoffReleased {
                slot: 5,
                source_node: 1,
                seq: 1,
            }]
        );
    }

    // FM-CLUSTER-086
    #[test]
    fn prepare_requires_a_migration_and_matching_parameters() {
        let state = state_with_primaries(3);
        assign(&state, 1, 0, 10);

        let err = state
            .apply_command(ClusterCommand::PrepareSlotHandoff {
                slot: 5,
                source_node: 1,
                target_node: 2,
                barrier_ms: crate::types::HANDOFF_BARRIER_MS,
                lease_ms: crate::types::HANDOFF_LEASE_MS,
                proposed_at_ms: T0,
            })
            .expect_err("no migration to prepare");
        assert!(
            matches!(&err, ClusterError::HandoffNotReady(5, why)
                if why == "no migration in progress"),
            "{err:?}"
        );

        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();
        let err = state
            .apply_command(ClusterCommand::PrepareSlotHandoff {
                slot: 5,
                source_node: 1,
                target_node: 3,
                barrier_ms: crate::types::HANDOFF_BARRIER_MS,
                lease_ms: crate::types::HANDOFF_LEASE_MS,
                proposed_at_ms: T0,
            })
            .expect_err("a prepare naming a different target must not be honoured");
        assert!(matches!(err, ClusterError::InvalidOperation(_)), "{err:?}");
    }

    /// Confirming a handoff whose lease already ran out is allowed (the entry
    /// carries no timestamp) but buys nothing: `Complete` re-checks.
    // FM-CLUSTER-085
    #[test]
    fn a_late_confirm_cannot_resurrect_an_expired_handoff() {
        let state = migrating_state(5);
        prepare(&state, 5, T0);
        state
            .apply_command(ClusterCommand::ConfirmSlotHandoffDrained { slot: 5, seq: 1 })
            .expect("confirm is accepted regardless of expiry");

        let err = complete_at(&state, 5, T0 + crate::types::HANDOFF_LEASE_MS + 1)
            .expect_err("but completion still re-checks");
        assert!(
            matches!(&err, ClusterError::HandoffNotReady(5, why) if why == "handoff lease expired"),
            "{err:?}"
        );
        assert_eq!(state.get_slot_owner(5), Some(1));
    }

    /// The seq counter is global, not per-slot, so two slots finalizing at once
    /// get distinct attempt identifiers and neither can confirm the other's.
    // FM-CLUSTER-088
    #[test]
    fn concurrent_handoffs_on_two_slots_do_not_interfere() {
        let state = migrating_state(5);
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 6,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();

        prepare(&state, 5, T0);
        prepare(&state, 6, T0);
        assert_eq!(state.get_slot_migration(5).unwrap().handoff.unwrap().seq, 1);
        assert_eq!(state.get_slot_migration(6).unwrap().handoff.unwrap().seq, 2);

        // Slot 5's ack must not drain slot 6.
        state
            .apply_command(ClusterCommand::ConfirmSlotHandoffDrained { slot: 5, seq: 1 })
            .unwrap();
        assert!(
            !state
                .get_slot_migration(6)
                .unwrap()
                .handoff
                .unwrap()
                .drained
        );

        complete_at(&state, 5, T0 + 1).expect("slot 5 completes");
        assert!(
            state.is_slot_migrating(6),
            "slot 6's finalization is untouched"
        );
    }

    /// Deadline arithmetic, pinned directly: the barrier is the shorter bound
    /// and the lease the longer one, and both are computed from replicated data
    /// rather than a clock read inside `apply`.
    // FM-CLUSTER-089
    #[test]
    fn handoff_deadlines_are_pure_functions_of_replicated_data() {
        let h = SlotHandoff {
            seq: 7,
            prepared_at_ms: 1_000,
            barrier_ms: 100,
            lease_ms: 10_000,
            drained: true,
        };
        assert_eq!(h.barrier_expires_at_ms(), 1_100);
        assert_eq!(h.lease_expires_at_ms(), 11_000);
        assert!(!h.barrier_expired_at(1_099));
        assert!(h.barrier_expired_at(1_100));
        assert!(!h.lease_expired_at(10_999));
        assert!(h.lease_expired_at(11_000));
        assert!(h.admits_complete_at(1_099));
        assert!(!h.admits_complete_at(1_100));

        let undrained = SlotHandoff {
            drained: false,
            ..h.clone()
        };
        assert!(!undrained.admits_complete_at(1_001));

        // Saturating, so an absurd lease cannot wrap into "already expired".
        let huge = SlotHandoff {
            prepared_at_ms: u64::MAX - 1,
            lease_ms: 10,
            ..h
        };
        assert_eq!(huge.lease_expires_at_ms(), u64::MAX);
    }

    // ---- AddNode: what a re-registration reports ---------------------------

    /// A node re-registering with a role or parent that disagrees with the
    /// recorded one keeps the recorded one — silently dropping the claim would
    /// leave an operator staring at a node that reports one role and behaves as
    /// another, so the disagreement is logged. Either half of the disagreement
    /// is enough to report it.
    // FM-CLUSTER-001
    #[test]
    fn a_disagreeing_re_registration_is_reported_on_either_half() {
        let state = state_with_primaries(2);
        state
            .apply_local(ClusterCommand::AddNode {
                node: NodeInfo::new_replica(3, test_addr(6382), test_addr(16382), 1),
            })
            .unwrap();

        // Role differs, parent agrees (both `None`).
        let demoted_claim = NodeInfo {
            role: NodeRole::Replica,
            ..NodeInfo::new_primary(1, test_addr(6380), test_addr(16380))
        };
        let (_, capture) = crate::test_tracing::capture_events(|| {
            state
                .apply_command(ClusterCommand::AddNode {
                    node: demoted_claim,
                })
                .unwrap()
        });
        let event = capture.only("re-registered with a different role");
        assert_eq!(event.field("node_id"), Some("1"));
        assert_eq!(event.field("recorded_role"), Some("master"));
        assert_eq!(event.field("claimed_role"), Some("slave"));
        assert_eq!(
            state.get_node(1).unwrap().role,
            NodeRole::Primary,
            "the claim is reported, never applied"
        );

        // Role agrees, parent differs.
        let (_, capture) = crate::test_tracing::capture_events(|| {
            state
                .apply_command(ClusterCommand::AddNode {
                    node: NodeInfo::new_replica(3, test_addr(6382), test_addr(16382), 2),
                })
                .unwrap()
        });
        assert_eq!(
            capture
                .only("re-registered with a different role")
                .field("node_id"),
            Some("3")
        );
        assert_eq!(
            state.get_node(3).unwrap().primary_id,
            Some(1),
            "the recorded parent survives the claim too"
        );

        // Both agree: nothing to report.
        let (_, capture) = crate::test_tracing::capture_events(|| {
            state
                .apply_command(ClusterCommand::AddNode {
                    node: NodeInfo::new_replica(3, test_addr(6382), test_addr(16382), 1),
                })
                .unwrap()
        });
        assert!(
            capture
                .matching("re-registered with a different role")
                .is_empty(),
            "an agreeing re-registration is not a disagreement: {:?}",
            capture.events()
        );
    }

    /// Seed a node carrying a specific binary version.
    fn add_versioned(state: &ClusterState, id: crate::NodeId, version: &str) {
        let port = 6379 + id as u16;
        state
            .apply_local(ClusterCommand::AddNode {
                node: NodeInfo {
                    version: version.to_string(),
                    ..NodeInfo::new_primary(id, test_addr(port), test_addr(port + 10_000))
                },
            })
            .expect("seeding a versioned node must succeed");
    }

    /// The mixed-version warning compares against the highest version any
    /// *other* versioned node reports — the joining node's own recorded entry
    /// and versionless peers are not part of that comparison, or the warning
    /// would tell an operator to chase a mismatch against the node itself.
    /// The field carrying that value is named `max_peer_version`, not
    /// `cluster_version` — it is the maximum of whatever versions happen to
    /// be present, not the cluster's majority or consensus version, and a
    /// field name that implied otherwise would mislead an operator reading
    /// the log (issue 41).
    // FM-CLUSTER-001
    #[test]
    fn mixed_version_warning_compares_against_the_other_versioned_nodes() {
        let state = ClusterState::new();
        add_versioned(&state, 1, "1.0.0");
        add_versioned(&state, 2, ""); // pre-version-tracking peer
        add_versioned(&state, 3, "9.9.9"); // the joiner's own recorded entry

        let (_, capture) = crate::test_tracing::capture_events(|| {
            state
                .apply_command(ClusterCommand::AddNode {
                    node: NodeInfo {
                        version: "2.0.0".to_string(),
                        ..NodeInfo::new_primary(3, test_addr(6382), test_addr(16382))
                    },
                })
                .unwrap()
        });
        let event = capture.only("mixed-version cluster");
        assert_eq!(event.level, tracing::Level::WARN);
        assert_eq!(event.field("node_id"), Some("3"));
        assert_eq!(event.field("node_version"), Some("2.0.0"));
        assert_eq!(
            event.field("max_peer_version"),
            Some("1.0.0"),
            "only other nodes that report a version count"
        );
        assert_eq!(
            event.field("cluster_version"),
            None,
            "the field must not be named as if it were the cluster's consensus version"
        );

        // A node that reports no version at all cannot be mismatched.
        let (_, capture) = crate::test_tracing::capture_events(|| {
            state
                .apply_command(ClusterCommand::AddNode {
                    node: NodeInfo {
                        version: String::new(),
                        ..NodeInfo::new_primary(4, test_addr(6383), test_addr(16383))
                    },
                })
                .unwrap()
        });
        assert!(
            capture.matching("mixed-version cluster").is_empty(),
            "a versionless node has nothing to compare: {:?}",
            capture.events()
        );

        // Agreement is not a mismatch either. Node 3 now reports 2.0.0, so
        // that is the version a joiner has to match.
        let (_, capture) = crate::test_tracing::capture_events(|| {
            state
                .apply_command(ClusterCommand::AddNode {
                    node: NodeInfo {
                        version: "2.0.0".to_string(),
                        ..NodeInfo::new_primary(5, test_addr(6384), test_addr(16384))
                    },
                })
                .unwrap()
        });
        assert!(
            capture.matching("mixed-version cluster").is_empty(),
            "matching the cluster version is the normal case: {:?}",
            capture.events()
        );
    }

    // ---- Failover: transferred slots, migration pruning, re-parenting ------

    /// A force failover reports how many slots it moved and cancels exactly the
    /// migrations that name the removed node — on either leg. Cancelling an
    /// unrelated migration would abort a slot move nobody asked to stop.
    // FM-CLUSTER-036
    #[test]
    fn force_failover_reports_moved_slots_and_prunes_only_related_migrations() {
        let state = state_with_primaries(4);
        assign(&state, 1, 0, 2);
        assign(&state, 2, 3, 5);
        assign(&state, 3, 6, 8);
        for (slot, source_node, target_node) in [(0u16, 1u64, 2u64), (3, 2, 1), (6, 3, 4)] {
            state
                .apply_local(ClusterCommand::BeginSlotMigration {
                    slot,
                    source_node,
                    target_node,
                })
                .unwrap();
        }

        let (_, capture) = crate::test_tracing::capture_events(|| {
            state
                .apply_command(ClusterCommand::Failover {
                    old_primary_id: 1,
                    new_primary_id: 2,
                    force: true,
                })
                .unwrap()
        });

        let event = capture.only("Applied atomic failover");
        assert_eq!(
            event.field("slots_transferred"),
            Some("3"),
            "the old primary owned slots 0..=2"
        );
        assert_eq!(event.field("old_primary"), Some("1"));
        assert_eq!(event.field("new_primary"), Some("2"));
        assert_eq!(state.get_slot_owner(0), Some(2));
        assert_eq!(state.get_slot_owner(2), Some(2));

        assert!(
            state.get_slot_migration(0).is_none(),
            "a migration out of the removed node cannot complete"
        );
        assert!(state.get_slot_migration(3).is_none(), "nor can one into it");
        let survivor = state
            .get_slot_migration(6)
            .expect("a migration between two live nodes must survive the failover");
        assert_eq!((survivor.source_node, survivor.target_node), (3, 4));
    }

    /// Failover re-parents the old primary's replicas onto the successor —
    /// *only* those. Re-parenting a bystander would hand the successor replicas
    /// that belong to another shard, and hand a primary a parent of its own.
    // FM-CLUSTER-041
    #[test]
    fn failover_re_parents_only_the_old_primarys_replicas() {
        let state = state_with_primaries(3);
        state
            .apply_local(ClusterCommand::AddNode {
                node: NodeInfo::new_replica(4, test_addr(6383), test_addr(16383), 1),
            })
            .unwrap();
        state
            .apply_local(ClusterCommand::AddNode {
                node: NodeInfo::new_replica(5, test_addr(6384), test_addr(16384), 3),
            })
            .unwrap();

        state
            .apply_command(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 2,
                force: false,
            })
            .unwrap();

        assert_eq!(
            state.get_node(4).unwrap().primary_id,
            Some(2),
            "the old primary's replica follows the successor"
        );
        assert_eq!(
            state.get_node(5).unwrap().primary_id,
            Some(3),
            "another shard's replica keeps its own primary"
        );
        assert_eq!(
            state.get_node(3).unwrap().primary_id,
            None,
            "an uninvolved primary is nobody's replica"
        );
        assert_eq!(
            state.get_node(2).unwrap().primary_id,
            None,
            "the successor is not re-parented onto itself"
        );
    }
}
