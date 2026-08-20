// lib_option — the Option-shaped *reads* every design model under specs/quint/ repeats
// (formal-spec issue 01, the shared-Quint-library extraction;
// .scratch/formal-spec/issues/open/01-quint-shared-library.md).
//
// This is a helper module, not a model: no `var`, no `action`, no `init`/`step`, and
// deliberately **no `val inv_*`** — scripts/quint-models.sh derives the runnable main
// models from "declares at least one `val inv_*`", so a helper module stays out of
// `just quint-run`/`just quint-verify-*` by construction. `just quint-check` globs every
// `.qnt`, so it is typechecked whether or not anything imports it, and
// specs/quint/lib_selftest.qnt exercises each definition below under `just quint-run`.
//
// It hosts helpers, not the type. `Option[a]` itself stays declared exactly once, in
// cluster_common_types.qnt, and is imported here: Quint sum-type constructors are
// nominal, so a second `type Option[a] = Some(a) | None` anywhere in this directory would
// be a *different* type whose `Some`/`None` clash with the first on import — the very
// drift cluster_common_types.qnt's header exists to prevent. Consumers therefore import
// both files (`import cluster_common_types.* from "./cluster_common_types"` for the type
// and the `NodeId` vocabulary, `import lib_option.* from "./lib_option"` for these
// reads); Quint's `import M.*` is not transitive, so neither import hides the other.
//
// Rows the hosted helpers serve, via their consumers:
//   TR-CLUSTER-010 — the slot-migration lifecycle. `migs.get(s)` is `Option[Migration]`,
//     and every guard that asks "is there a migration on this slot, and does it satisfy
//     P?" is `optExists` written out longhand (the dominant form in
//     cluster_migration_failover_logic.qnt / _machine.qnt: 83 `| None => false` arms).
//   TR-CLUSTER-018 / TR-CLUSTER-019 — failover application and the automatic proposal.
//     The per-slot residue (`res.get(s)`) and the staged transition record (`ns.record`)
//     are read the same way, including the 16 `| None => true` arms `optForall` names
//     (e.g. cluster_migration_failover_logic.qnt's `identityOrderOk`, where "no stored
//     identity yet" must *pass* the ordering check rather than fail it).
//   TR-REPLICATION-019 / TR-REPLICATION-020 — the inherited-id shift and the full-sync
//     trailer adoption, both gated on a *validated* id: an `Option`-shaped stored
//     identity read exactly like `identityOrderOk` reads its own. Imminent consumer: the
//     phase-3 full-sync/PSYNC handoff model (campaign W3,
//     .scratch/formal-spec/2026-08-19-quint-completeness-campaign.md).
//   TR-REPLICATION-013 — the feed-gate deadline a barrier arm/release publishes: an
//     `Option`-shaped "is a hold in force, and does it still cover this session?" read.
//     Imminent consumer: the phase-3 feed-gate/barrier session model (campaign W3).
//
// Deliberately NOT hosted here (the bar is two real consumers, existing or imminent —
// issue 01's "do NOT vendor basicSpells wholesale"):
//   - `optGetOr(o, dflt)` — exactly one existing consumer today
//     (cluster_migration_failover_machine.qnt's `registration_seq` read, defaulting to
//     -1). Add it when the second lands.
//   - `optMap`, `optFilter`, `optIsSome`/`optIsNone` — no consumer at all: models
//     compare against `None` directly (`ns.parent != None`), which is legal on this
//     type and reads better than a wrapper.
//   - Anything that would need an `action`-valued arm. The 22 `| None => nope` arms in
//     cluster_migration_failover_machine.qnt dispatch to a *disabled action*, and Quint
//     has no first-class actions, so that shape cannot be a `pure def` at all.
module lib_option {
  import cluster_common_types.* from "./cluster_common_types"

  // "The option is present *and* its contents satisfy `f`." Absent ⇒ false, which is the
  // conservative reading every guard wants: a slot with no migration on it does not
  // satisfy a claim about its migration.
  pure def optExists(o: Option[a], f: (a) => bool): bool =
    match o {
      | Some(v) => f(v)
      | None => false
    }

  // "If the option is present, its contents satisfy `f`." Absent ⇒ true — the vacuous
  // reading, which is what an ordering/fence check needs for a field that has not been
  // written yet (nothing stored cannot contradict what is being proposed).
  //
  // De Morgan makes this expressible as `not(optExists(o, x => not(f(x))))`; it is a
  // separate name anyway because the only thing that distinguishes the two is what an
  // absent option means, and that choice — vacuously satisfied, or conservatively
  // refused — is exactly what a reviewer must be able to see at the call site.
  pure def optForall(o: Option[a], f: (a) => bool): bool =
    match o {
      | Some(v) => f(v)
      | None => true
    }
}
