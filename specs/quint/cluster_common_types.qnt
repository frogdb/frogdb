// cluster_common_types — the type vocabulary shared by every cluster design model under
// specs/quint/ (formal-spec phase 2). Single responsibility: the handful of declarations
// that are *genuinely identical* in more than one model, so neither model owns them and
// neither can drift.
//
// Belongs here:
//   - `NodeId`, the abstract node identifier every cluster model keys its per-node state
//     by. Same meaning in both models: an opaque, stable identity (see
//     cluster_admission.qnt's "This node's own id" assumption note).
//   - `Option[a]`, the model-local sum type both models use instead of basicSpells'. It is
//     defined once here so the "why not basicSpells" deviation is argued once, and so the
//     two models cannot end up with structurally-equal-but-distinct `Option` types.
//
// Does NOT belong here:
//   - Model *sizing* constants (`NODES`, `SLOTS`, `HOLD_BYTE_CAP`, ...). They are per-model
//     bounds on the state space a bounded checker explores, not shared vocabulary: the two
//     models happen to agree on a 4-node universe today, and a model that later needs a
//     different bound must be free to change its own without silently rebounding the other.
//     Each model's `_types` module declares its own.
//   - Any per-model state shape (`NodeState`, `Migration`, `RaftState`, ...). The two
//     models' `NodeState` records share a name and nothing else: admission's tracks
//     bootstrap-vs-join intent and Raft provenance, migration's tracks role/parent/keyspace
//     and the run-identity machinery. Unifying them would be forcing a merge on two
//     genuinely different state spaces.
//   - Any logic. This module declares no `var`, no `def`, no guard.
//
// Import it directly wherever it is needed: Quint's `import M.*` is NOT transitive, so a
// module that uses `NodeId`/`Option` imports this file itself rather than inheriting it
// through a sibling.
//
// This module models no transition itself — it supplies the vocabulary both consuming
// models key their per-node state by: `NodeId` anchors cluster_admission's `AddNode`
// (TR-CLUSTER-005) and cluster_migration_failover's `BeginSlotMigration`
// (TR-CLUSTER-010), and `Option` is the sum type both machines use throughout.
module cluster_common_types {
  // The abstract node identifier. Uninterpreted beyond equality and the per-model `NODES`
  // universe that enumerates it.
  type NodeId = int

  // Quint has no built-in Option (see quint-lang's Basic spells section). Both models use
  // this local declaration rather than `basicSpells.qnt`'s, so no model depends on an
  // external spells library. Note the knock-on naming deviation it forces in
  // cluster_admission_types: `Intent`'s undecided case is `Undecided`, not a bare `None`,
  // because Quint sum-type constructors share one flat namespace with `None` below.
  type Option[a] = Some(a) | None
}
