// cluster_migration_failover_types — types and constants for the migration x failover
// composite model (formal-spec phase 2, task 2). The state-shape half of the model: the
// "Node role"/"Node parent pointer"/"Node membership"/"Node FAIL flag" state-space rows
// behind TR-CLUSTER-003/004, the handoff record behind TR-CLUSTER-011/012 (INV-HANDOFF-1,
// INV-HANDOFF-2), and the migration record behind TR-CLUSTER-010/015 (INV-MIG-1).
//
// No state variables and no logic live here — see cluster_migration_failover.qnt (the
// runnable main model) for the full "Rows modeled" header, the deliberately-not-modeled
// list, and the four-file layout note.
module cluster_migration_failover_types {
  type NodeId = int
  type SlotId = int

  // Local Option — see cluster_admission.qnt's header for why this isn't basicSpells'
  // (kept model-local rather than pulled from the standard spells library).
  type Option[a] = Some(a) | None

  type RoleTag = Primary | Replica

  type NodeState = {
    role: RoleTag,
    // "Node parent pointer" — Some(p) only meaningful when role == Replica
    // (TR-CLUSTER-004's ruled precondition, issue 14: p must exist and be a Primary).
    parent: Option[NodeId],
    // "Node membership" — a genuinely two-way field: TR-CLUSTER-003 (`removeNode`) can
    // clear it, gated by issue 20's amendment; no other action ever does.
    member: bool,
    // "Node FAIL flag".
    fail: bool,
  }

  type Handoff = {
    // "A handoff's attempt number" — minted from the monotone handoff_seq counter.
    seq: int,
    drained: bool,
  }

  type Migration = {
    source: NodeId,
    target: NodeId,
    // "A migration's prepared handoff".
    handoff: Option[Handoff],
    // "This migration is mid-Cancel, awaiting repatriation" — set by `cancelMigration`
    // when there is residue to stream back; distinguishes a cancelling-and-pending
    // migration record (which `completeRepatriation` must remove once the stream
    // finishes) from an abort-and-pending one (whose migration record survives
    // Abort per TR-CLUSTER-014, so `completeRepatriation` must leave it alone).
    cancelling: bool,
  }

  pure val NODES: Set[NodeId] = Set(1, 2, 3, 4)
  pure val SLOTS: Set[SlotId] = Set(1, 2, 3, 4)
  pure val HOLD_BYTE_CAP: int = 6
}
