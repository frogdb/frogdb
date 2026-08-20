// lib_monotone — the "only ever moves one way" primitives every design model under
// specs/quint/ repeats (formal-spec issue 01, the shared-Quint-library extraction;
// .scratch/formal-spec/issues/open/01-quint-shared-library.md).
//
// One responsibility: monotone advance. A watermark that never retreats (`max2`), an
// ordering on the (generation, position) pairs the protocols advance lexicographically
// (`lexGt`), and the boolean latch the defect/coverage ghosts are built out of (`latch`).
// The three are the same discipline at three types, which is why they share a file rather
// than each getting one.
//
// Like specs/quint/lib_option.qnt this is a helper module, not a model: no `var`, no
// `action`, no `init`/`step`, and deliberately **no `val inv_*`** — scripts/quint-models.sh
// derives the runnable main models from "declares at least one `val inv_*`", so this file
// stays out of `just quint-run`/`just quint-verify-*` by construction while `just
// quint-check` still typechecks it. specs/quint/lib_selftest.qnt exercises every
// definition below under `just quint-run`.
//
// Rows the hosted helpers serve, via their consumers:
//   TR-CLUSTER-010 — the slot-migration lifecycle. `max2` is how the migration model
//     advances the copy/ack watermarks a handoff is judged on (`target_copy.pos`,
//     `target_replica_ack` in cluster_migration_failover_machine.qnt) without ever
//     letting an out-of-order observation move them backward.
//   TR-CLUSTER-018 / TR-CLUSTER-019 — failover application and the automatic proposal.
//     `max2` also folds the per-node `epoch_own` into the control-plane epoch watermark
//     (cluster_migration_failover.qnt's epoch invariant), and `lexGt` is the shape of
//     `identityOrderOk`'s ruled ordering conjunct: a run-identity report is admitted only
//     if its `(incarnation, identity_seq)` pair strictly dominates the stored one, so a
//     replayed or reordered report is refused rather than acted on.
//   TR-REPLICATION-018 / TR-REPLICATION-033 — ACK ingest and "a replica ACKs the landed
//     offset, never the claimed or received one": the same never-retreating watermark at
//     the offset type. Imminent consumer: the phase-3 feed-gate/barrier session model
//     (campaign W3, .scratch/formal-spec/2026-08-19-quint-completeness-campaign.md).
//   TR-REPLICATION-015 / TR-REPLICATION-019 — a full resync installing an absolute offset
//     pair, and a granted `+CONTINUE` shifting the inherited id at a *validated* id. The
//     replication-issue-24 rulings (replid2 shift + atomic offset pairing) make that a
//     lexicographic comparison on a (generation, offset) pair — `lexGt` at a second type.
//     Imminent consumer: the phase-3 full-sync/PSYNC handoff model (campaign W3).
//   TR-CLUSTER-018 / TR-REPLICATION-013 — both phase-3 models carry the mandatory
//     defect/coverage ghost battery the cluster models established (the `defects`/
//     `coverage` records in cluster_migration_failover_machine.qnt), whose every field is
//     a `latch`.
//
// Deliberately NOT hosted here (the bar is two real consumers, existing or imminent):
//   - `min2`, `lexGe`, `lexLt`, `clamp`, `abs` — no consumer. `max2`'s counterparts are
//     the obvious next thing to reach for, which is exactly why they are not pre-placed:
//     issue 01's brief is "extract only what the models actually use".
//   - A spread-preserving ghost *update* helper. `latch` is the value-level half; the
//     record-level idiom `defects' = { ...defects, f: latch(defects.f, cond) }` cannot be
//     abstracted further, because Quint has no way to parameterize over a field name.
//     The convention is documented at `latch` instead.
module lib_monotone {
  // The larger of two integers — a watermark advance. Written out rather than imported
  // from `basicSpells.qnt`'s `max`, for the same reason cluster_common_types.qnt declares
  // its own `Option`: no model here depends on an external spells library (issue 01: "do
  // NOT vendor basicSpells wholesale").
  pure def max2(a: int, b: int): int = if (a > b) a else b

  // Strict lexicographic domination on a `(generation, position)` pair: `x` is admitted
  // over `y` only if it advances the generation, or holds the generation and advances the
  // position. Equal pairs do NOT dominate — a replay of the pair already recorded is
  // refused, which is the property (a replayed report is a no-op, never an action) the
  // consumers below are built on.
  pure def lexGt(x: (int, int), y: (int, int)): bool =
    x._1 > y._1 or (x._1 == y._1 and x._2 > y._2)

  // A ghost latch: once a behaviour has been observed, the ghost stays observed. This is
  // the value-level half of the `defects`/`coverage` convention the cluster models
  // established — a ghost is recomputed from the step's *post-state* and OR-ed into its
  // previous value, so an invariant over it is a claim about the whole trace rather than
  // about the last step, and no later step can clear the evidence.
  //
  // Trivially `prev or seen`; it is a named definition because the name is the review
  // surface. `defects' = { ...defects, f: latch(defects.f, cond) }` states "this ghost is
  // monotone" where the bare `or` merely happens to be monotone, and a mutation that
  // drops the accumulator (`f: cond`) reads as a visible change to a documented rule.
  pure def latch(prev: bool, seen: bool): bool = prev or seen
}
